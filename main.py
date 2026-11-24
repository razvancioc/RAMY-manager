import json
import math
import asyncio
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, Request, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func, case, text
from pydantic import BaseModel

import models
from database import engine, SessionLocal
from bambu_client import bambu_printer
from camera_client import bambu_camera
from importer import import_model_from_url

# Ensure DB tables exist
models.Base.metadata.create_all(bind=engine)
with engine.connect() as conn:
    for col, col_type in [
        ("material_id", "INTEGER"),
        ("culoare_dorita", "TEXT DEFAULT ''"),
        ("order_id", "INTEGER"),
        ("order_item_id", "INTEGER")
    ]:
        try:
            conn.execute(text(f"ALTER TABLE print_queue ADD COLUMN {col} {col_type}"))
            conn.commit()
        except Exception:
            pass

# Pre-populare materiale implicite la prima initializare daca baza este goala
with SessionLocal() as db_init:
    if db_init.query(models.Material).count() == 0:
        db_init.add_all([
            models.Material(tip="PLA Basic", culoare="Negru", culoare_hex="#1a1a1a", greutate_ramasa=1000.0, pret_per_kg=90.0, brand="Bambu Lab", temperatura_duza=220, temperatura_pat=60, locatie_slot="A1"),
            models.Material(tip="PLA Basic", culoare="Alb", culoare_hex="#f9fafb", greutate_ramasa=1000.0, pret_per_kg=90.0, brand="Bambu Lab", temperatura_duza=220, temperatura_pat=60, locatie_slot="A2"),
            models.Material(tip="PLA Basic", culoare="Roșu", culoare_hex="#e02424", greutate_ramasa=1000.0, pret_per_kg=90.0, brand="Bambu Lab", temperatura_duza=220, temperatura_pat=60, locatie_slot="A3"),
            models.Material(tip="PLA Basic", culoare="Galben", culoare_hex="#faca15", greutate_ramasa=1000.0, pret_per_kg=90.0, brand="Bambu Lab", temperatura_duza=220, temperatura_pat=60, locatie_slot="A4")
        ])
        db_init.commit()

# WebSockets Connection Manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: Dict[str, Any]):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

ws_manager = ConnectionManager()

# Background async bridge for MQTT thread
main_loop = None

def on_bambu_state_change(state: Dict[str, Any]):
    global main_loop
    if main_loop and main_loop.is_running():
        asyncio.run_coroutine_threadsafe(
            ws_manager.broadcast({"type": "telemetry", "data": state}),
            main_loop
        )

bambu_printer.on_state_change = on_bambu_state_change

@asynccontextmanager
async def lifespan(app: FastAPI):
    global main_loop
    main_loop = asyncio.get_running_loop()
    
    # Initialize printer config from DB
    db = SessionLocal()
    try:
        printer = db.query(models.Printer).filter(models.Printer.activ == 1).first()
        if printer:
            bambu_printer.update_config(
                ip=printer.ip_address,
                access_code=printer.access_code or "",
                serial=printer.serial_number or ""
            )
            bambu_camera.update_config(
                ip=printer.ip_address,
                access_code=printer.access_code or ""
            )
    except Exception as e:
        print(f"Eroare încărcare config imprimantă: {e}")
    finally:
        db.close()
        
    bambu_printer.start()
    bambu_camera.start()
    yield
    bambu_camera.stop()
    bambu_printer.stop()

from fastapi.staticfiles import StaticFiles
app = FastAPI(title="Ramy Manager - 3D Printer Hub", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Dependency DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- WEBSOCKET ROUTE ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    # Send initial telemetry immediately
    await websocket.send_json({"type": "telemetry", "data": bambu_printer.get_state()})
    try:
        while True:
            data = await websocket.receive_text()
            # Handle client ping or requests
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)

# --- HOME VIEW ---
@app.get("/", response_class=HTMLResponse)
def home_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate, max-age=0"}
    )

# --- SCHEMAS ---
class PrinterConfig(BaseModel):
    nume: str = "Bambu Lab A1"
    ip_address: str
    access_code: str
    serial_number: Optional[str] = ""

class PrinterControl(BaseModel):
    action: str # pause, resume, stop, light_on, light_off, speed
    value: Optional[Any] = None

class MaterialCreate(BaseModel):
    tip: str
    culoare: str
    culoare_hex: Optional[str] = "#00d26a"
    greutate_ramasa: float = 1000.0
    pret_per_kg: Optional[float] = 90.0
    brand: Optional[str] = "Bambu Lab"
    temperatura_duza: Optional[int] = 220
    temperatura_pat: Optional[int] = 60
    locatie_slot: Optional[str] = ""

class MaterialUpdate(BaseModel):
    tip: Optional[str] = None
    culoare: Optional[str] = None
    culoare_hex: Optional[str] = None
    greutate_ramasa: Optional[float] = None
    pret_per_kg: Optional[float] = None
    brand: Optional[str] = None
    locatie_slot: Optional[str] = None

class AMSSyncRequest(BaseModel):
    slot: int
    tip: Optional[str] = "PLA"
    culoare: Optional[str] = None
    culoare_hex: Optional[str] = "#00d26a"
    greutate_ramasa: Optional[float] = 1000.0
    pret_per_kg: Optional[float] = 90.0
    brand: Optional[str] = "Bambu Lab"
    force_new: Optional[bool] = False

class ComponentCreate(BaseModel):
    material_id: int
    greutate: float

class Model3DCreate(BaseModel):
    nume_proiect: str
    timp_estimat_minute: int = 0
    link_imagine: Optional[str] = None
    link_proiect: Optional[str] = None
    componente: List[ComponentCreate] = []

class Model3DUpdate(BaseModel):
    nume_proiect: Optional[str] = None
    timp_estimat_minute: Optional[int] = None
    link_imagine: Optional[str] = None
    link_proiect: Optional[str] = None
    componente: Optional[List[ComponentCreate]] = None

class PrintQueueCreate(BaseModel):
    model_id: int
    order_id: Optional[int] = None
    order_item_id: Optional[int] = None
    prioritate: int = 2 # 1=Urgent, 2=Normal, 3=Stoc
    nume_client: Optional[str] = ""
    pret_vanzare: Optional[float] = 0.0
    note: Optional[str] = ""
    ordine: Optional[int] = 0
    data_programata: Optional[str] = ""
    material_id: Optional[int] = None
    culoare_dorita: Optional[str] = ""

class PrintQueueUpdate(BaseModel):
    model_id: Optional[int] = None
    order_id: Optional[int] = None
    order_item_id: Optional[int] = None
    prioritate: Optional[int] = None
    status: Optional[str] = None
    nume_client: Optional[str] = None
    pret_vanzare: Optional[float] = None
    note: Optional[str] = None
    ordine: Optional[int] = None
    data_programata: Optional[str] = None
    material_id: Optional[int] = None
    culoare_dorita: Optional[str] = None

class OrderItemCreate(BaseModel):
    id: Optional[int] = None
    model_id: Optional[int] = None
    nume_produs: Optional[str] = ""
    cantitate: int = 1
    material_id: Optional[int] = None
    culoare_dorita: Optional[str] = ""
    pret_unitar: float = 0.0
    cost_unitar: Optional[float] = 0.0
    note: Optional[str] = ""

class OrderCreate(BaseModel):
    nume_client: str
    telefon: Optional[str] = ""
    status: Optional[str] = "În așteptare"
    pret_total: Optional[float] = 0.0
    incasat: Optional[int] = 0
    metoda_plata: Optional[str] = "Cash"
    note: Optional[str] = ""
    trimite_in_coada: Optional[bool] = True
    prioritate_coada: Optional[int] = 2
    produse: List[OrderItemCreate] = []

class OrderUpdate(BaseModel):
    nume_client: Optional[str] = None
    telefon: Optional[str] = None
    status: Optional[str] = None
    pret_total: Optional[float] = None
    cost_total_estimat: Optional[float] = None
    incasat: Optional[int] = None
    data_incasare: Optional[str] = None
    metoda_plata: Optional[str] = None
    note: Optional[str] = None
    trimite_in_coada: Optional[bool] = True
    prioritate_coada: Optional[int] = 2
    produse: Optional[List[OrderItemCreate]] = None

class ToggleItemStatusRequest(BaseModel):
    finalizat: Optional[bool] = None

class BatchCompleteRequest(BaseModel):
    model_id: Optional[int] = None
    nume_produs: Optional[str] = None
    bucati: int = 1

class BatchQueueCreate(BaseModel):
    model_id: int
    nume_lot: Optional[str] = ""
    cantitate: int = 1
    prioritate: Optional[int] = 2
    note: Optional[str] = ""

class FilamentPurchaseCreate(BaseModel):
    furnizor: Optional[str] = "Bambu Lab EU"
    brand: Optional[str] = "Bambu Lab"
    tip: Optional[str] = "PLA Basic"
    culoare: Optional[str] = ""
    culoare_hex: Optional[str] = "#00e575"
    cantitate_role: int = 1
    greutate_per_rola_g: Optional[float] = 1000.0
    pret_per_rola: float = 90.0
    cost_transport: Optional[float] = 0.0
    data_achizitie: Optional[str] = ""
    status: Optional[str] = "Livrat / În stoc"
    adaugat_in_stoc: Optional[bool] = True
    note: Optional[str] = ""

class FilamentPurchaseUpdate(BaseModel):
    furnizor: Optional[str] = None
    brand: Optional[str] = None
    tip: Optional[str] = None
    culoare: Optional[str] = None
    culoare_hex: Optional[str] = None
    cantitate_role: Optional[int] = None
    greutate_per_rola_g: Optional[float] = None
    pret_per_rola: Optional[float] = None
    cost_transport: Optional[float] = None
    status: Optional[str] = None
    adaugat_in_stoc: Optional[int] = None
    note: Optional[str] = None

class QueueReorderRequest(BaseModel):
    item_ids: List[int]

class QueueMoveRequest(BaseModel):
    direction: str # "up", "down", "top", "bottom"

class QueueScheduleDateRequest(BaseModel):
    data_programata: str

class ImportUrlRequest(BaseModel):
    url: str

class PlateFilament(BaseModel):
    id: Optional[str] = None
    type: Optional[str] = "PLA"
    color: Optional[str] = "#FFFFFF"
    color_name: Optional[str] = ""
    used_g: Optional[float] = 0.0
    used_m: Optional[float] = 0.0

class PlateItem(BaseModel):
    index: int = 1
    nume: str
    thumbnail_url: Optional[str] = None
    timp_minute: int = 0
    greutate_g: float = 0.0
    filamente: List[PlateFilament] = []

class SavePlateRequest(BaseModel):
    nume_model: str
    varianta_titlu: Optional[str] = ""
    placa: PlateItem
    link_proiect: Optional[str] = None
    adauga_in_coada: bool = False
    prioritate: int = 2
    nume_client: Optional[str] = ""
    pret_vanzare: Optional[float] = 0.0
    note: Optional[str] = ""

class BatchQueueVariantRequest(BaseModel):
    nume_model: str
    varianta_titlu: Optional[str] = ""
    link_proiect: Optional[str] = None
    plates: List[PlateItem]
    prioritate: int = 2
    nume_client: Optional[str] = ""
    pret_vanzare: Optional[float] = 0.0
    note: Optional[str] = ""

class SaveVariantModelRequest(BaseModel):
    nume_model: str
    varianta_titlu: Optional[str] = ""
    link_imagine: Optional[str] = None
    link_proiect: Optional[str] = None
    timp_minute: int = 0
    greutate_g: float = 0.0
    filamente: List[PlateFilament] = []

class CostCalcRequest(BaseModel):
    greutate_g: float
    timp_minute: int
    pret_kg_filament: Optional[float] = 90.0
    cost_kwh: Optional[float] = 1.30 # RON/kWh
    putere_w: Optional[float] = 140.0 # W consum mediu Bambu A1
    uzura_ron_ora: Optional[float] = 1.50 # amortizare / uzura duza/curea
    marja_profit: Optional[float] = 60.0 # % marja

class ExcelCostRequest(BaseModel):
    model_id: Optional[int] = None
    gramaj_g: Optional[float] = 0.0
    timp_minute: Optional[float] = 0.0
    timp_ore: Optional[float] = None
    pret_kg_filament: Optional[float] = 100.0 # 0.10 RON/g implicit PLA (100 RON/kg)
    cost_curent_ora: Optional[float] = 1.36 # RON/oră rotunjit în sus CEIL
    diverse: Optional[float] = 0.0 # Accesorii, kit lampă, etc.
    extra: Optional[float] = 2.96 # Pachet ambalare standard (1.25 cutie + 0.24 hartie + 0.05 sticker + 0.12 TY + 1.0 lipici + 0.3 folie bule)
    pret_vanzare_manual: Optional[float] = None

def calcul_cost_productie_excel(
    gramaj_g: float,
    timp_minute: float = 0.0,
    timp_ore: Optional[float] = None,
    pret_kg_filament: float = 100.0,
    cost_curent_ora: float = 1.36,
    diverse: float = 0.0,
    extra: float = 2.96,
    pret_vanzare_manual: Optional[float] = None
) -> Dict[str, Any]:
    """
    Formulele exacte din Excel sheetul CRACIUN:
    - Cost Filament = Gramaj * (Pret_per_kg / 1000) (ex. 0.10 RON/g PLA, 0.125 RON/g PETG)
    - Cost Curent = CEIL(Ore_print * 1.36) rotunjit în sus la număr întreg
    - Diverse = Componente speciale, accesorii opționale
    - Extra = Consumabile ambalare standard (2.96 RON)
    - Cost Producție = Cost Filament + Cost Curent + Diverse + Extra
    - Profit Formula Excel = (Cost Filament + Cost Curent) / 2.0 (50% adaos peste producția brută)
    - Total Fără TVA = Cost Producție + Profit Excel
    - Total Cu TVA = ROUND(Total Fără TVA * 1.19)
    - Preț Uniformizat Sugerat = Rotunjire comercială la cel mai apropiat prag de 5 sau 10 RON
    """
    if timp_ore is None or timp_ore <= 0:
        ore = max(0.0, float(timp_minute or 0.0) / 60.0)
    else:
        ore = float(timp_ore)

    gr = max(0.0, float(gramaj_g or 0.0))
    pkg = float(pret_kg_filament or 100.0)
    cost_fil = round((gr / 1000.0) * pkg, 3)

    cur_rate = float(cost_curent_ora or 1.36)
    cost_cur = int(math.ceil(ore * cur_rate)) if ore > 0 else 0

    div = round(float(diverse or 0.0), 2)
    ext = round(float(extra if extra is not None else 2.96), 2)

    cost_prod = round(cost_fil + cost_cur + div + ext, 2)
    profit_excel = round((cost_fil + cost_cur) / 2.0, 2)
    total_fara_tva = round(cost_prod + profit_excel, 2)
    total_cu_tva = int(round(total_fara_tva * 1.19))

    step = 10 if total_cu_tva >= 100 else (5 if total_cu_tva >= 15 else 2)
    pret_unif = int(math.ceil(total_cu_tva / step) * step) if total_cu_tva > 0 else 10
    if pret_unif == total_cu_tva and total_cu_tva >= 15:
        pret_unif += step

    pret_ales = float(pret_vanzare_manual) if (pret_vanzare_manual is not None and pret_vanzare_manual > 0) else float(total_cu_tva)
    profit_real = round(pret_ales - cost_prod, 2)
    marja_pct = round((profit_real / pret_ales * 100.0), 1) if pret_ales > 0 else 0.0

    return {
        "gramaj_g": round(gr, 1),
        "timp_minute": int(ore * 60),
        "timp_ore": round(ore, 2),
        "pret_kg_filament": pkg,
        "cost_filament": round(cost_fil, 2),
        "cost_curent": cost_cur,
        "diverse": div,
        "extra": ext,
        "cost_productie": cost_prod,
        "profit_excel": profit_excel,
        "total_fara_tva": total_fara_tva,
        "total_cu_tva": total_cu_tva,
        "pret_uniformizat": pret_unif,
        "pret_ales": pret_ales,
        "profit_real": profit_real,
        "marja_pct": marja_pct
    }

class VolumetricProjectCreate(BaseModel):
    nume_client: str
    text_comanda: str
    font_family: Optional[str] = "Standard Bold"
    inaltime_cm: Optional[float] = 15.0
    adancime_mm: Optional[float] = 30.0
    tip_constructie: Optional[str] = "Fațetă + Casetă"
    material_id: Optional[int] = None
    culoare_dorita: Optional[str] = "Alb / Negru"
    pret_total: Optional[float] = 0.0
    timp_per_litera_min: Optional[int] = 60
    greutate_per_litera_g: Optional[float] = 35.0
    note: Optional[str] = ""

class VolumetricLetterUpdate(BaseModel):
    status: Optional[str] = None # "pending", "printing", "completed", "failed"
    note: Optional[str] = None
    inaltime_cm: Optional[float] = None
    timp_estimat_min: Optional[int] = None
    greutate_est_g: Optional[float] = None

class VolumetricBatchStatus(BaseModel):
    action: str # "complete_all", "reset_failed", "reset_all", "set_height_all"
    value: Optional[float] = None

# --- PRINTER ROUTES ---
@app.get("/api/printer/status")
def get_printer_status():
    return bambu_printer.get_state()

@app.post("/api/printer/reconnect")
def reconnect_printer(db: Session = Depends(get_db)):
    printer = db.query(models.Printer).filter(models.Printer.activ == 1).first()
    if printer:
        bambu_printer.update_config(
            ip=printer.ip_address,
            access_code=printer.access_code or "",
            serial=printer.serial_number or ""
        )
        bambu_camera.update_config(
            ip=printer.ip_address,
            access_code=printer.access_code or ""
        )
    bambu_printer.reconnect()
    bambu_camera.start()
    return {"success": True, "state": bambu_printer.get_state(), "mesaj": "Reconectare inițializată cu succes!"}

@app.get("/api/printer/config")
def get_printer_config(db: Session = Depends(get_db)):
    printer = db.query(models.Printer).filter(models.Printer.activ == 1).first()
    if not printer:
        printer = models.Printer(
            nume="Bambu Lab A1",
            ip_address="",
            access_code="",
            serial_number="",
            status_curent="Neconfigurată",
            activ=1
        )
        db.add(printer)
        db.commit()
        db.refresh(printer)
    return printer

@app.post("/api/printer/config")
def update_printer_config(config: PrinterConfig, db: Session = Depends(get_db)):
    printer = db.query(models.Printer).filter(models.Printer.activ == 1).first()
    if not printer:
        printer = models.Printer(nume=config.nume, ip_address=config.ip_address, access_code=config.access_code, serial_number=config.serial_number or "")
        db.add(printer)
    else:
        printer.nume = config.nume
        printer.ip_address = config.ip_address
        printer.access_code = config.access_code
        if config.serial_number:
            printer.serial_number = config.serial_number
    db.commit()
    db.refresh(printer)

    bambu_printer.update_config(
        ip=printer.ip_address,
        access_code=printer.access_code or "",
        serial=printer.serial_number or ""
    )
    bambu_camera.update_config(
        ip=printer.ip_address,
        access_code=printer.access_code or ""
    )
    return {"mesaj": "Configurație salvată și conexiune reinițializată!", "printer": printer}

# --- CAMERA STREAMING & SNAPSHOT ROUTES ---
@app.get("/api/camera/stream")
def get_camera_stream():
    """
    Returns an MJPEG live multipart video stream from Bambu A1 port 6000.
    Viewable directly in <img src="/api/camera/stream"> in any browser.
    """
    return StreamingResponse(
        bambu_camera.get_mjpeg_stream(),
        media_type="multipart/x-mixed-replace; boundary=frame"
    )

@app.get("/api/camera/snapshot")
def get_camera_snapshot():
    """
    Returns the latest static JPEG frame captured from the chamber camera.
    """
    frame = bambu_camera.get_latest_frame()
    if not frame:
        try:
            with open("static/camera_snapshot.jpg", "rb") as f:
                frame = f.read()
        except Exception:
            raise HTTPException(status_code=503, detail="Camera nu este disponibilă încă")
    return Response(
        content=frame,
        media_type="image/jpeg",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"}
    )

@app.post("/api/printer/control")
def printer_control(ctrl: PrinterControl):
    act = ctrl.action.lower()
    success = False
    msg = ""

    if act == "pause":
        success = bambu_printer.pause_print()
        msg = "Comandă trimisă: Pauză imprimare"
    elif act == "resume":
        success = bambu_printer.resume_print()
        msg = "Comandă trimisă: Reluare imprimare"
    elif act == "stop":
        success = bambu_printer.stop_print()
        msg = "Comandă trimisă: Stop imprimare"
    elif act == "light_on":
        success = bambu_printer.toggle_light(True)
        msg = "Comandă trimisă: Lumină aprinsă"
    elif act == "light_off":
        success = bambu_printer.toggle_light(False)
        msg = "Comandă trimisă: Lumină stinsă"
    elif act == "speed":
        level = int(ctrl.value or 2)
        success = bambu_printer.set_speed(level)
        msg = f"Comandă trimisă: Viteză profil {level}"
    else:
        return JSONResponse(status_code=400, content={"eroare": f"Acțiune necunoscută: {ctrl.action}"})

    return {"success": success, "mesaj": msg, "imprimanta_online": bambu_printer.is_connected}

# --- MATERIALS ROUTES ---
@app.get("/materials/")
def listeaza_materiale(db: Session = Depends(get_db)):
    return db.query(models.Material).order_by(models.Material.id.desc()).all()

@app.post("/materials/")
def adauga_material(mat: MaterialCreate, db: Session = Depends(get_db)):
    db_mat = models.Material(
        tip=mat.tip.strip(),
        culoare=mat.culoare.strip(),
        culoare_hex=mat.culoare_hex or "#00d26a",
        greutate_ramasa=float(mat.greutate_ramasa),
        pret_per_kg=float(mat.pret_per_kg or 90.0),
        brand=mat.brand or "Bambu Lab",
        temperatura_duza=mat.temperatura_duza or 220,
        temperatura_pat=mat.temperatura_pat or 60,
        locatie_slot=mat.locatie_slot or ""
    )
    db.add(db_mat)
    db.commit()
    db.refresh(db_mat)
    return db_mat

@app.put("/materials/{mat_id}")
def actualizeaza_material(mat_id: int, update: MaterialUpdate, db: Session = Depends(get_db)):
    mat = db.query(models.Material).filter(models.Material.id == mat_id).first()
    if not mat:
        raise HTTPException(status_code=404, detail="Material inexistent")
    if update.tip is not None: mat.tip = update.tip
    if update.culoare is not None: mat.culoare = update.culoare
    if update.culoare_hex is not None: mat.culoare_hex = update.culoare_hex
    if update.greutate_ramasa is not None: mat.greutate_ramasa = update.greutate_ramasa
    if update.pret_per_kg is not None: mat.pret_per_kg = update.pret_per_kg
    if update.brand is not None: mat.brand = update.brand
    if update.locatie_slot is not None: mat.locatie_slot = update.locatie_slot
    db.commit()
    db.refresh(mat)
    return mat

@app.post("/materials/{mat_id}/adjust")
def ajusteaza_greutate_material(mat_id: int, request: Request, db: Session = Depends(get_db)):
    mat = db.query(models.Material).filter(models.Material.id == mat_id).first()
    if not mat:
        raise HTTPException(status_code=404, detail="Material inexistent")
    # delta can be passed as json {"delta": -25.5}
    try:
        data = asyncio.run(request.json()) if asyncio.iscoroutinefunction(request.json) else None
    except Exception:
        data = {}
    delta = float(data.get("delta", 0)) if data else 0
    mat.greutate_ramasa = max(0.0, round(mat.greutate_ramasa + delta, 1))
    db.commit()
    db.refresh(mat)
    return mat

@app.delete("/materials/{mat_id}")
def sterge_material(mat_id: int, db: Session = Depends(get_db)):
    mat = db.query(models.Material).filter(models.Material.id == mat_id).first()
    if mat:
        db.delete(mat)
        db.commit()
        return {"mesaj": "Material șters cu succes!"}
    raise HTTPException(status_code=404, detail="Material inexistent")

# --- AMS TO STOCK SYNCHRONIZATION ---
ROMANIAN_COLOR_PALETTE = [
    ("#000000", "Negru"),
    ("#1A1A1A", "Negru"),
    ("#2B2B2B", "Negru Antracit"),
    ("#FFFFFF", "Alb"),
    ("#F9FAFB", "Alb"),
    ("#E5E7EB", "Alb / Gri Deschis"),
    ("#E02424", "Roșu"),
    ("#DC2626", "Roșu"),
    ("#FF0000", "Roșu"),
    ("#991B1B", "Roșu Închis / Bordeaux"),
    ("#00E575", "Verde Bambu"),
    ("#10B981", "Verde"),
    ("#059669", "Verde Smarald"),
    ("#008000", "Verde"),
    ("#1E40AF", "Albastru"),
    ("#2563EB", "Albastru"),
    ("#3B82F6", "Albastru Deschis"),
    ("#0000FF", "Albastru"),
    ("#0284C7", "Bleu"),
    ("#06B6D4", "Turcoaz"),
    ("#00FFFF", "Cyan"),
    ("#FACA15", "Galben"),
    ("#EAB308", "Galben"),
    ("#FFFF00", "Galben"),
    ("#FF7A00", "Portocaliu"),
    ("#F97316", "Portocaliu"),
    ("#FF8C00", "Portocaliu"),
    ("#7E3AF2", "Mov"),
    ("#8B5CF6", "Violet"),
    ("#9333EA", "Mov"),
    ("#800080", "Purpuriu"),
    ("#8B4513", "Maro"),
    ("#78350F", "Maro"),
    ("#A52A2A", "Maro"),
    ("#6B7280", "Gri"),
    ("#9CA3AF", "Gri Deschis"),
    ("#4B5563", "Gri"),
    ("#374151", "Gri Antracit"),
    ("#EC4899", "Roz"),
    ("#F43F5E", "Roz / Magenta"),
    ("#FFC0CB", "Roz Deschis"),
    ("#D97706", "Auriu / Chihlimbar"),
    ("#C0C0C0", "Argintiu Metalic"),
]

def hex_to_rgb(hex_str: str) -> tuple:
    if not hex_str:
        return (128, 128, 128)
    clean = hex_str.lstrip("#")
    if len(clean) >= 6:
        try:
            return int(clean[0:2], 16), int(clean[2:4], 16), int(clean[4:6], 16)
        except Exception:
            return (128, 128, 128)
    return (128, 128, 128)

def color_distance(hex1: str, hex2: str) -> float:
    r1, g1, b1 = hex_to_rgb(hex1)
    r2, g2, b2 = hex_to_rgb(hex2)
    return ((r1 - r2) ** 2 + (g1 - g2) ** 2 + (b1 - b2) ** 2) ** 0.5

def get_romanian_color_name(hex_code: str) -> str:
    r1, g1, b1 = hex_to_rgb(hex_code)
    best_name = "Colorat"
    best_dist = float("inf")
    for ref_hex, name in ROMANIAN_COLOR_PALETTE:
        r2, g2, b2 = hex_to_rgb(ref_hex)
        dist = ((r1 - r2) ** 2 + (g1 - g2) ** 2 + (b1 - b2) ** 2) ** 0.5
        if dist < best_dist:
            best_dist = dist
            best_name = name
    return best_name

@app.get("/api/ams/check-stock")
def check_ams_stock(db: Session = Depends(get_db)):
    ams_slots = bambu_printer.state.get("ams", [])
    materials = db.query(models.Material).all()
    
    results = []
    unregistered_count = 0
    
    for s in ams_slots:
        slot_num = s.get("slot")
        s_type = (s.get("type") or "PLA").strip()
        s_color = s.get("color") or "#FFFFFF"
        s_brand = s.get("brand") or "Bambu Lab"
        s_remain = s.get("remain", 100)
        s_empty = s.get("empty", False)
        
        color_name = get_romanian_color_name(s_color)
        
        matched_mat = None
        if not s_empty:
            for m in materials:
                # 1. Match by explicit slot location
                if m.locatie_slot and m.locatie_slot.strip().lower() == f"slot {slot_num}".lower():
                    matched_mat = m
                    break
                # 2. Match by type and close color
                if m.tip and s_type and m.tip.strip().lower() == s_type.lower():
                    if m.culoare_hex and color_distance(m.culoare_hex, s_color) <= 35:
                        matched_mat = m
                        break
                    if m.culoare and m.culoare.strip().lower() == color_name.lower():
                        matched_mat = m
                        break
        
        in_stock = (matched_mat is not None)
        if not in_stock and not s_empty:
            unregistered_count += 1
            
        results.append({
            "slot": slot_num,
            "type": s_type,
            "color": s_color,
            "color_name": color_name,
            "brand": s_brand,
            "remain": s_remain,
            "active": s.get("active", False),
            "empty": s_empty,
            "in_stock": in_stock,
            "material_id": matched_mat.id if matched_mat else None,
            "material_name": f"{matched_mat.tip} {matched_mat.culoare}" if matched_mat else None,
            "material_weight": matched_mat.greutate_ramasa if matched_mat else None
        })
        
    return {
        "slots": results,
        "unregistered_count": unregistered_count
    }

@app.post("/api/ams/sync-to-stock")
def sync_ams_slot_to_stock(req: AMSSyncRequest, db: Session = Depends(get_db)):
    hex_code = req.culoare_hex or "#00d26a"
    tip = (req.tip or "PLA").strip()
    color_name = req.culoare.strip() if req.culoare else get_romanian_color_name(hex_code)
    slot_label = f"Slot {req.slot}"
    
    # Check if a material already exists for this slot or exact type+color unless force_new
    existing = None
    if not req.force_new:
        existing = db.query(models.Material).filter(
            models.Material.locatie_slot == slot_label,
            models.Material.tip.ilike(tip)
        ).first()
        if not existing:
            for m in db.query(models.Material).filter(models.Material.tip.ilike(tip)).all():
                if color_distance(m.culoare_hex, hex_code) <= 25:
                    existing = m
                    break

    if existing:
        existing.locatie_slot = slot_label
        db.commit()
        db.refresh(existing)
        return {
            "success": True,
            "is_new": False,
            "mesaj": f"Filamentul existent '{existing.tip} {existing.culoare}' a fost asociat cu Slot {req.slot}!",
            "material": {
                "id": existing.id, "tip": existing.tip, "culoare": existing.culoare,
                "culoare_hex": existing.culoare_hex, "greutate_ramasa": existing.greutate_ramasa
            }
        }
        
    db_mat = models.Material(
        tip=tip,
        culoare=color_name,
        culoare_hex=hex_code,
        greutate_ramasa=float(req.greutate_ramasa or 1000.0),
        pret_per_kg=float(req.pret_per_kg or 90.0),
        brand=req.brand or "Bambu Lab",
        locatie_slot=slot_label
    )
    db.add(db_mat)
    db.commit()
    db.refresh(db_mat)
    
    return {
        "success": True,
        "is_new": True,
        "mesaj": f"Filamentul nou '{db_mat.tip} {db_mat.culoare}' din Slot {req.slot} a fost adăugat cu succes în stoc!",
        "material": {
            "id": db_mat.id, "tip": db_mat.tip, "culoare": db_mat.culoare,
            "culoare_hex": db_mat.culoare_hex, "greutate_ramasa": db_mat.greutate_ramasa
        }
    }

@app.post("/api/ams/sync-all-missing")
def sync_all_missing_ams(db: Session = Depends(get_db)):
    ams_slots = bambu_printer.state.get("ams", [])
    materials = db.query(models.Material).all()
    added_list = []
    
    for s in ams_slots:
        if s.get("empty", False):
            continue
        slot_num = s.get("slot")
        s_type = (s.get("type") or "PLA").strip()
        s_color = s.get("color") or "#FFFFFF"
        s_brand = s.get("brand") or "Bambu Lab"
        
        found = False
        color_name = get_romanian_color_name(s_color)
        for m in materials:
            if m.locatie_slot and m.locatie_slot.strip().lower() == f"slot {slot_num}".lower():
                found = True
                break
            if m.tip and m.tip.strip().lower() == s_type.lower():
                if color_distance(m.culoare_hex, s_color) <= 35 or (m.culoare and m.culoare.strip().lower() == color_name.lower()):
                    found = True
                    break
        
        if not found:
            new_mat = models.Material(
                tip=s_type,
                culoare=color_name,
                culoare_hex=s_color,
                greutate_ramasa=1000.0,
                pret_per_kg=90.0,
                brand=s_brand,
                locatie_slot=f"Slot {slot_num}"
            )
            db.add(new_mat)
            db.commit()
            db.refresh(new_mat)
            materials.append(new_mat)
            added_list.append({
                "slot": slot_num,
                "tip": new_mat.tip,
                "culoare": new_mat.culoare,
                "id": new_mat.id
            })
            
    return {
        "success": True,
        "count": len(added_list),
        "added": added_list,
        "mesaj": f"{len(added_list)} filament(e) din AMS au fost adăugate în stoc!" if added_list else "Toate filamentele din AMS sunt deja înregistrate în stoc."
    }

# --- MODELS ROUTES ---
@app.get("/models/")
def listeaza_modele(db: Session = Depends(get_db)):
    modele = db.query(models.Model3D).order_by(models.Model3D.id.desc()).all()
    rezultate = []
    
    # Pre-cache materials for quick lookup
    materiale_dict = {m.id: m for m in db.query(models.Material).all()}
    
    for m in modele:
        try:
            componente = json.loads(m.componente_json or "[]")
        except Exception:
            componente = []
            
        greutate_totala = sum(float(c.get("greutate", 0)) for c in componente)
        cost_total_material = 0.0
        componente_detaliate = []
        
        for c in componente:
            mid = c.get("material_id")
            gr = float(c.get("greutate", 0))
            mat = materiale_dict.get(mid)
            if mat:
                cost_part = (gr / 1000.0) * (mat.pret_per_kg or 90.0)
                cost_total_material += cost_part
                componente_detaliate.append({
                    "material_id": mid,
                    "tip": mat.tip,
                    "culoare": mat.culoare,
                    "culoare_hex": mat.culoare_hex,
                    "greutate": gr,
                    "cost": round(cost_part, 2)
                })
            else:
                c_tip = c.get("tip") or "Necunoscut"
                c_cul = c.get("culoare") or "N/A"
                c_hex = c.get("culoare_hex") or "#888888"
                c_cost = round((gr / 1000.0) * (c.get("pret_kg") or 90.0), 2)
                cost_total_material += c_cost
                componente_detaliate.append({
                    "material_id": mid,
                    "tip": c_tip,
                    "culoare": c_cul,
                    "culoare_hex": c_hex,
                    "greutate": gr,
                    "cost": c_cost
                })

        # Calcul cost productie conform formulelor din Excel
        mat_pkg_avg = (cost_total_material / (greutate_totala / 1000.0)) if greutate_totala > 0 else 100.0
        calc_ex = calcul_cost_productie_excel(
            gramaj_g=greutate_totala,
            timp_minute=m.timp_estimat_minute or 0,
            pret_kg_filament=mat_pkg_avg,
            diverse=0.0,
            extra=2.96
        )

        rezultate.append({
            "id": m.id,
            "nume_proiect": m.nume_proiect,
            "timp_estimat_minute": m.timp_estimat_minute,
            "link_imagine": m.link_imagine,
            "link_proiect": m.link_proiect,
            "greutate_totala": round(greutate_totala, 1),
            "cost_material_estimat": round(cost_total_material, 2),
            "cost_productie": calc_ex["cost_productie"],
            "cost_curent": calc_ex["cost_curent"],
            "cost_filament": calc_ex["cost_filament"],
            "extra_ambalare": calc_ex["extra"],
            "profit_excel": calc_ex["profit_excel"],
            "pret_fara_tva": calc_ex["total_fara_tva"],
            "pret_cu_tva": calc_ex["total_cu_tva"],
            "pret_uniformizat": calc_ex["pret_uniformizat"],
            "componente": componente_detaliate,
            "componente_raw": componente,
            "data_adaugare": m.data_adaugare or ""
        })
    return rezultate

@app.post("/models/")
def adauga_model(model: Model3DCreate, db: Session = Depends(get_db)):
    lista_comp = [{"material_id": c.material_id, "greutate": float(c.greutate)} for c in model.componente if c.material_id and c.greutate > 0]
    
    db_model = models.Model3D(
        nume_proiect=model.nume_proiect.strip(),
        timp_estimat_minute=int(model.timp_estimat_minute or 0),
        link_imagine=model.link_imagine.strip() if model.link_imagine else None,
        link_proiect=model.link_proiect.strip() if model.link_proiect else None,
        componente_json=json.dumps(lista_comp),
        data_adaugare=datetime.now().strftime("%Y-%m-%d %H:%M")
    )
    db.add(db_model)
    db.commit()
    db.refresh(db_model)
    return db_model

@app.put("/models/{model_id}")
def actualizeaza_model(model_id: int, update: Model3DUpdate, db: Session = Depends(get_db)):
    model = db.query(models.Model3D).filter(models.Model3D.id == model_id).first()
    if not model:
        raise HTTPException(status_code=404, detail="Model inexistent")
    if update.nume_proiect is not None: model.nume_proiect = update.nume_proiect
    if update.timp_estimat_minute is not None: model.timp_estimat_minute = update.timp_estimat_minute
    if update.link_imagine is not None: model.link_imagine = update.link_imagine
    if update.link_proiect is not None: model.link_proiect = update.link_proiect
    if update.componente is not None:
        lista_comp = [{"material_id": c.material_id, "greutate": float(c.greutate)} for c in update.componente if c.material_id and c.greutate > 0]
        model.componente_json = json.dumps(lista_comp)
    db.commit()
    db.refresh(model)
    return model

@app.delete("/models/{model_id}")
def sterge_model(model_id: int, db: Session = Depends(get_db)):
    model = db.query(models.Model3D).filter(models.Model3D.id == model_id).first()
    if model:
        db.delete(model)
        db.commit()
        return {"mesaj": "Model șters cu succes!"}
    raise HTTPException(status_code=404, detail="Model inexistent")

# --- QUICK IMPORTER ROUTES ---
def match_stock_material(db: Session, fil_type: str, fil_hex: str) -> Optional[models.Material]:
    fil_type = (fil_type or "PLA").strip().lower()
    fil_hex = (fil_hex or "").strip()
    materials = db.query(models.Material).all()
    # 1. Exact or very close color + type
    for m in materials:
        if (m.tip or "").strip().lower() == fil_type:
            if m.culoare_hex and fil_hex and color_distance(m.culoare_hex, fil_hex) <= 35:
                return m
    # 2. Any material of same type
    for m in materials:
        if (m.tip or "").strip().lower() == fil_type:
            return m
    return None

@app.post("/api/import/url")
def import_from_url(req: ImportUrlRequest):
    data = import_model_from_url(req.url)
    return data

@app.post("/api/import/save-plate")
def save_plate_endpoint(req: SavePlateRequest, db: Session = Depends(get_db)):
    pl = req.placa
    clean_model_title = req.nume_model.strip()
    plate_title = f"{clean_model_title} - Placa {pl.index}: {pl.nume.strip()}"
    
    componente = []
    total_cost = 0.0
    for f in pl.filamente:
        gr = float(f.used_g or 0.0)
        c_hex = f.color or "#FFFFFF"
        c_name = f.color_name or get_romanian_color_name(c_hex)
        c_type = f.type or "PLA"
        matched = match_stock_material(db, c_type, c_hex)
        mid = matched.id if matched else None
        pkg = matched.pret_per_kg if matched else 90.0
        c_cost = (gr / 1000.0) * pkg
        total_cost += c_cost
        componente.append({
            "material_id": mid,
            "greutate": gr,
            "tip": c_type,
            "culoare": c_name,
            "culoare_hex": c_hex,
            "pret_kg": pkg
        })

    db_model = models.Model3D(
        nume_proiect=plate_title,
        timp_estimat_minute=int(pl.timp_minute or 0),
        link_imagine=pl.thumbnail_url or None,
        link_proiect=req.link_proiect or None,
        componente_json=json.dumps(componente),
        data_adaugare=datetime.now().strftime("%Y-%m-%d %H:%M")
    )
    db.add(db_model)
    db.commit()
    db.refresh(db_model)

    queue_item = None
    if req.adauga_in_coada:
        queue_item = models.PrintQueue(
            model_id=db_model.id,
            prioritate=req.prioritate or 2,
            status="În așteptare",
            nume_client=req.nume_client or "",
            cost_estimat=round(total_cost, 2),
            pret_vanzare=float(req.pret_vanzare or 0.0),
            note=req.note or f"Importat din MakerWorld ({req.varianta_titlu or 'Variantă'})",
            data_adaugare=datetime.now().strftime("%Y-%m-%d %H:%M")
        )
        db.add(queue_item)
        db.commit()
        db.refresh(queue_item)

    return {
        "success": True,
        "model_id": db_model.id,
        "queue_id": queue_item.id if queue_item else None,
        "mesaj": f"Placa '{pl.nume}' a fost salvată" + (" și trimisă în coada de printare!" if queue_item else " în librărie!")
    }

@app.post("/api/import/batch-queue")
def batch_queue_endpoint(req: BatchQueueVariantRequest, db: Session = Depends(get_db)):
    clean_model_title = req.nume_model.strip()
    adaugate = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    for pl in req.plates:
        plate_title = f"{clean_model_title} - Placa {pl.index}: {pl.nume.strip()}"
        componente = []
        total_cost = 0.0
        for f in pl.filamente:
            gr = float(f.used_g or 0.0)
            c_hex = f.color or "#FFFFFF"
            c_name = f.color_name or get_romanian_color_name(c_hex)
            c_type = f.type or "PLA"
            matched = match_stock_material(db, c_type, c_hex)
            mid = matched.id if matched else None
            pkg = matched.pret_per_kg if matched else 90.0
            c_cost = (gr / 1000.0) * pkg
            total_cost += c_cost
            componente.append({
                "material_id": mid,
                "greutate": gr,
                "tip": c_type,
                "culoare": c_name,
                "culoare_hex": c_hex,
                "pret_kg": pkg
            })
            
        db_model = models.Model3D(
            nume_proiect=plate_title,
            timp_estimat_minute=int(pl.timp_minute or 0),
            link_imagine=pl.thumbnail_url or None,
            link_proiect=req.link_proiect or None,
            componente_json=json.dumps(componente),
            data_adaugare=now_str
        )
        db.add(db_model)
        db.flush() # get db_model.id
        
        queue_item = models.PrintQueue(
            model_id=db_model.id,
            prioritate=req.prioritate or 2,
            status="În așteptare",
            nume_client=req.nume_client or "",
            cost_estimat=round(total_cost, 2),
            pret_vanzare=float(req.pret_vanzare or 0.0),
            note=req.note or f"Importat din MakerWorld ({req.varianta_titlu or 'Variantă'})",
            data_adaugare=now_str
        )
        db.add(queue_item)
        adaugate += 1

    db.commit()
    return {
        "success": True,
        "adaugate": adaugate,
        "mesaj": f"Toate cele {adaugate} plăci au fost adăugate în coada de printare!"
    }

@app.post("/api/import/save-variant")
def save_variant_endpoint(req: SaveVariantModelRequest, db: Session = Depends(get_db)):
    clean_model_title = req.nume_model.strip()
    var_title = (req.varianta_titlu or "").strip()
    full_title = f"{clean_model_title} ({var_title})" if var_title and var_title not in clean_model_title else clean_model_title

    componente = []
    for f in req.filamente:
        gr = float(f.used_g or 0.0)
        c_hex = f.color or "#FFFFFF"
        c_name = f.color_name or get_romanian_color_name(c_hex)
        c_type = f.type or "PLA"
        matched = match_stock_material(db, c_type, c_hex)
        mid = matched.id if matched else None
        pkg = matched.pret_per_kg if matched else 90.0
        componente.append({
            "material_id": mid,
            "greutate": gr,
            "tip": c_type,
            "culoare": c_name,
            "culoare_hex": c_hex,
            "pret_kg": pkg
        })

    db_model = models.Model3D(
        nume_proiect=full_title,
        timp_estimat_minute=int(req.timp_minute or 0),
        link_imagine=req.link_imagine or None,
        link_proiect=req.link_proiect or None,
        componente_json=json.dumps(componente),
        data_adaugare=datetime.now().strftime("%Y-%m-%d %H:%M")
    )
    db.add(db_model)
    db.commit()
    db.refresh(db_model)

    return {
        "success": True,
        "model_id": db_model.id,
        "mesaj": f"Modelul complet '{full_title}' a fost salvat în librărie!"
    }

# --- PRINT QUEUE ROUTES ---
@app.get("/queue/")
def listeaza_coada(db: Session = Depends(get_db)):
    items = db.query(models.PrintQueue).order_by(
        case((models.PrintQueue.status == "Finalizat", 1), else_=0),
        models.PrintQueue.ordine.asc(),
        models.PrintQueue.prioritate.asc(),
        models.PrintQueue.id.desc()
    ).all()
    modele_dict = {m.id: m for m in db.query(models.Model3D).all()}
    materiale_dict = {m.id: m for m in db.query(models.Material).all()}

    rezultat = []
    for item in items:
        mod = modele_dict.get(item.model_id)
        model_info = None
        if mod:
            try:
                componente = json.loads(mod.componente_json or "[]")
            except Exception:
                componente = []
            greutate = sum(float(c.get("greutate", 0)) for c in componente)
            cost_mat = 0.0
            comp_list = []
            for c in componente:
                mat = materiale_dict.get(c.get("material_id"))
                if mat:
                    c_cost = (float(c.get("greutate", 0)) / 1000.0) * (mat.pret_per_kg or 90.0)
                    cost_mat += c_cost
                    comp_list.append(f"{mat.tip} {mat.culoare} ({c.get('greutate')}g)")
            model_info = {
                "id": mod.id,
                "nume_proiect": mod.nume_proiect,
                "timp_estimat_minute": mod.timp_estimat_minute,
                "link_imagine": mod.link_imagine,
                "greutate": round(greutate, 1),
                "cost_material": round(cost_mat, 2),
                "componente_sumar": ", ".join(comp_list)
            }

        custom_mat = materiale_dict.get(item.material_id) if item.material_id else None
        custom_mat_info = {
            "id": custom_mat.id,
            "tip": custom_mat.tip,
            "culoare": custom_mat.culoare,
            "culoare_hex": custom_mat.culoare_hex,
            "brand": custom_mat.brand
        } if custom_mat else None

        rezultat.append({
            "id": item.id,
            "model_id": item.model_id,
            "order_id": item.order_id,
            "order_item_id": item.order_item_id,
            "prioritate": item.prioritate, # 1=Urgent, 2=Normal, 3=Stoc
            "status": item.status,
            "nume_client": item.nume_client or "",
            "cost_estimat": item.cost_estimat or (model_info["cost_material"] if model_info else 0.0),
            "pret_vanzare": item.pret_vanzare or 0.0,
            "note": item.note or "",
            "ordine": item.ordine or 0,
            "data_programata": item.data_programata or "",
            "ora_start_estimat": item.ora_start_estimat or "",
            "ora_finish_estimat": item.ora_finish_estimat or "",
            "data_adaugare": item.data_adaugare or "",
            "data_finalizare": item.data_finalizare or "",
            "material_id": item.material_id,
            "culoare_dorita": item.culoare_dorita or "",
            "material_personalizat": custom_mat_info,
            "model": model_info
        })
    return rezultat

@app.post("/queue/")
def adauga_in_coada(item: PrintQueueCreate, db: Session = Depends(get_db)):
    model = db.query(models.Model3D).filter(models.Model3D.id == item.model_id).first()
    if not model:
        raise HTTPException(status_code=404, detail="Model inexistent")

    # Calculate production cost with Excel formula (Filament + Curent + Extra)
    cost_mat = 0.0
    total_gr = 0.0
    pkg_val = 100.0
    try:
        componente = json.loads(model.componente_json or "[]")
        total_gr = sum(float(c.get("greutate", 0)) for c in componente)
        if item.material_id:
            mat_custom = db.query(models.Material).filter(models.Material.id == item.material_id).first()
            if mat_custom:
                pkg_val = mat_custom.pret_per_kg or 100.0
                cost_mat = (total_gr / 1000.0) * pkg_val
        else:
            for c in componente:
                mat = db.query(models.Material).filter(models.Material.id == c.get("material_id")).first()
                if mat:
                    cost_mat += (float(c.get("greutate", 0)) / 1000.0) * (mat.pret_per_kg or 100.0)
            if total_gr > 0 and cost_mat > 0:
                pkg_val = (cost_mat / (total_gr / 1000.0))
    except Exception:
        pass

    calc_prod = calcul_cost_productie_excel(
        gramaj_g=total_gr,
        timp_minute=model.timp_estimat_minute or 0,
        pret_kg_filament=pkg_val,
        diverse=0.0,
        extra=2.96
    )
    cost_productie_final = calc_prod["cost_productie"]

    max_ord = db.query(func.max(models.PrintQueue.ordine)).scalar() or 0
    next_ord = item.ordine if (item.ordine and item.ordine > 0) else (max_ord + 1)

    db_queue = models.PrintQueue(
        model_id=item.model_id,
        order_id=item.order_id,
        order_item_id=item.order_item_id,
        prioritate=item.prioritate,
        status="În așteptare",
        nume_client=item.nume_client or "",
        cost_estimat=cost_productie_final,
        pret_vanzare=item.pret_vanzare or 0.0,
        note=item.note or "",
        ordine=next_ord,
        data_programata=item.data_programata or "",
        material_id=(item.material_id if item.material_id and item.material_id > 0 else None),
        culoare_dorita=item.culoare_dorita or "",
        data_adaugare=datetime.now().strftime("%Y-%m-%d %H:%M")
    )
    db.add(db_queue)
    db.commit()
    db.refresh(db_queue)
    return db_queue

@app.put("/queue/{item_id}")
def actualizeaza_coada(item_id: int, update: PrintQueueUpdate, db: Session = Depends(get_db)):
    job = db.query(models.PrintQueue).filter(models.PrintQueue.id == item_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job inexistent în coadă")
    
    if update.model_id is not None:
        job.model_id = update.model_id
    model = db.query(models.Model3D).filter(models.Model3D.id == job.model_id).first()

    if update.order_id is not None: job.order_id = update.order_id
    if update.order_item_id is not None: job.order_item_id = update.order_item_id
    if update.prioritate is not None: job.prioritate = update.prioritate
    if update.status is not None: job.status = update.status
    if update.nume_client is not None: job.nume_client = update.nume_client
    if update.pret_vanzare is not None: job.pret_vanzare = update.pret_vanzare
    if update.note is not None: job.note = update.note
    if update.ordine is not None: job.ordine = update.ordine
    if update.data_programata is not None: job.data_programata = update.data_programata
    if update.material_id is not None:
        job.material_id = update.material_id if update.material_id > 0 else None
    if update.culoare_dorita is not None:
        job.culoare_dorita = update.culoare_dorita

    # Recalculate cost if model or material changed
    if model and model.componente_json:
        try:
            componente = json.loads(model.componente_json or "[]")
            total_gr = sum(float(c.get("greutate", 0)) for c in componente)
            pkg_val = 100.0
            if job.material_id:
                mat_custom = db.query(models.Material).filter(models.Material.id == job.material_id).first()
                if mat_custom:
                    pkg_val = mat_custom.pret_per_kg or 100.0
            else:
                cost_mat = 0.0
                for c in componente:
                    mat = db.query(models.Material).filter(models.Material.id == c.get("material_id")).first()
                    pkg = mat.pret_per_kg if mat else 100.0
                    cost_mat += (float(c.get("greutate", 0)) / 1000.0) * pkg
                if total_gr > 0 and cost_mat > 0:
                    pkg_val = (cost_mat / (total_gr / 1000.0))

            calc_prod = calcul_cost_productie_excel(
                gramaj_g=total_gr,
                timp_minute=model.timp_estimat_minute or 0,
                pret_kg_filament=pkg_val,
                diverse=0.0,
                extra=2.96
            )
            job.cost_estimat = calc_prod["cost_productie"]
        except Exception:
            pass

    db.commit()
    db.refresh(job)
    return job

def scade_stoc_pentru_job(job: models.PrintQueue, db: Session):
    model_3d = db.query(models.Model3D).filter(models.Model3D.id == job.model_id).first()
    istoric = []
    if model_3d and model_3d.componente_json:
        try:
            componente = json.loads(model_3d.componente_json)
            total_greutate = sum(float(c.get("greutate", 0)) for c in componente)

            if job.material_id:
                custom_mat = db.query(models.Material).filter(models.Material.id == job.material_id).first()
                if custom_mat and total_greutate > 0:
                    noua_greutate = max(0.0, round(custom_mat.greutate_ramasa - total_greutate, 1))
                    custom_mat.greutate_ramasa = noua_greutate
                    extra = f" (Culoare solicitată: {job.culoare_dorita})" if job.culoare_dorita else ""
                    istoric.append(f"-{total_greutate}g din {custom_mat.tip} {custom_mat.culoare}{extra} (Rămas: {noua_greutate}g)")
            else:
                for comp in componente:
                    mat_id = comp.get("material_id")
                    greutate = float(comp.get("greutate", 0))
                    if mat_id and greutate > 0:
                        material = db.query(models.Material).filter(models.Material.id == mat_id).first()
                        if material:
                            noua_greutate = max(0.0, round(material.greutate_ramasa - greutate, 1))
                            material.greutate_ramasa = noua_greutate
                            istoric.append(f"-{greutate}g din {material.tip} {material.culoare} (Rămas: {noua_greutate}g)")
        except Exception as e:
            print(f"Eroare procesare componente la finalizare: {e}")
    return istoric

@app.put("/queue/{item_id}/complete")
def finalizeaza_print(item_id: int, db: Session = Depends(get_db)):
    job = db.query(models.PrintQueue).filter(models.PrintQueue.id == item_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Jobul nu a fost găsit în coada de așteptare.")
    if job.status == "Finalizat":
        return {"mesaj": "Acest print a fost deja marcat ca finalizat anterior.", "deja_finalizat": True}

    istoric_scaderi = scade_stoc_pentru_job(job, db)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    job.status = "Finalizat"
    job.data_finalizare = now_str

    if job.order_item_id:
        try:
            sibling_jobs = db.query(models.PrintQueue).filter(models.PrintQueue.order_item_id == job.order_item_id).all()
            item = db.query(models.OrderItem).filter(models.OrderItem.id == job.order_item_id).first()
            if item and sibling_jobs and all(sj.status in ["Finalizat", "completed"] for sj in sibling_jobs):
                item.status = "Finalizat"
        except Exception as e:
            print(f"Eroare actualizare status item comandă: {e}")

    # If linked to an order, check if all prints for this order are completed
    if job.order_id:
        try:
            all_q = db.query(models.PrintQueue).filter(models.PrintQueue.order_id == job.order_id).all()
            ord_rec = db.query(models.Order).filter(models.Order.id == job.order_id).first()
            if ord_rec:
                if all_q and all(q.status in ["Finalizat", "completed"] for q in all_q):
                    if ord_rec.status not in ["Livrată"]:
                        ord_rec.status = "Finalizată"
                        ord_rec.data_finalizare = now_str
                elif any(q.status in ["Finalizat", "completed"] for q in all_q):
                    if ord_rec.status == "În așteptare":
                        ord_rec.status = "În lucru"
        except Exception as e:
            print(f"Eroare actualizare status comanda asociata: {e}")

    db.commit()

    return {
        "success": True,
        "mesaj": "Print finalizat cu succes! Stocul a fost actualizat automat.",
        "scaderi": istoric_scaderi
    }

@app.delete("/queue/{item_id}")
def sterge_din_coada(item_id: int, db: Session = Depends(get_db)):
    job = db.query(models.PrintQueue).filter(models.PrintQueue.id == item_id).first()
    if job:
        db.delete(job)
        db.commit()
        return {"mesaj": "Element eliminat din coada de așteptare!"}
    raise HTTPException(status_code=404, detail="Job inexistent")

# ========================================================
# --- ORDERS & CUSTOMER MANAGEMENT API ROUTES ---
# ========================================================
@app.get("/api/orders/")
def listeaza_comenzi(db: Session = Depends(get_db)):
    comenzi = db.query(models.Order).order_by(models.Order.id.desc()).all()
    models_dict = {m.id: m for m in db.query(models.Model3D).all()}
    materials_dict = {m.id: m for m in db.query(models.Material).all()}
    queue_items = db.query(models.PrintQueue).filter(models.PrintQueue.order_id != None).all()
    queue_by_order = {}
    for q in queue_items:
        queue_by_order.setdefault(q.order_id, []).append(q)

    rezultate = []
    for c in comenzi:
        items = db.query(models.OrderItem).filter(models.OrderItem.order_id == c.id).all()
        q_jobs = queue_by_order.get(c.id, [])
        total_printuri = len(q_jobs)
        finalizate_printuri = sum(1 for q in q_jobs if q.status in ["Finalizat", "completed"])
        
        produse_list = []
        cost_calculat = 0.0
        valoare_calculata = 0.0
        for it in items:
            mod = models_dict.get(it.model_id) if it.model_id else None
            mat = materials_dict.get(it.material_id) if it.material_id else None
            cant = it.cantitate or 1
            pret_u = float(it.pret_unitar or 0.0)
            cost_u = float(it.cost_unitar or 0.0)
            subtotal = round(pret_u * cant, 2)
            cost_total_item = round(cost_u * cant, 2)
            cost_calculat += cost_total_item
            valoare_calculata += subtotal
            
            item_jobs = [q for q in q_jobs if q.order_item_id == it.id]
            item_jobs_fin = sum(1 for q in item_jobs if q.status in ["Finalizat", "completed"])
            este_finalizat = (it.status == "Finalizat") or (len(item_jobs) > 0 and item_jobs_fin == len(item_jobs))
            
            produse_list.append({
                "id": it.id,
                "order_id": it.order_id,
                "model_id": it.model_id,
                "nume_produs": it.nume_produs or (mod.nume_proiect if mod else "Produs Custom"),
                "cantitate": cant,
                "material_id": it.material_id,
                "material_nume": f"{mat.tip} {mat.culoare}" if mat else "",
                "material_hex": mat.culoare_hex if mat else "",
                "culoare_dorita": it.culoare_dorita or (mat.culoare if mat else ""),
                "pret_unitar": pret_u,
                "cost_unitar": cost_u,
                "subtotal": subtotal,
                "cost_total": cost_total_item,
                "status": "Finalizat" if este_finalizat else (it.status or "În așteptare"),
                "este_finalizat": este_finalizat,
                "printuri_total": len(item_jobs),
                "printuri_finalizate": item_jobs_fin,
                "note": it.note or "",
                "link_imagine": mod.link_imagine if mod else None,
                "timp_estimat_minute": (mod.timp_estimat_minute * cant) if mod else 0,
                "bucati_jobs": [
                    {
                        "id": q.id,
                        "nume": q.nume_client,
                        "status": q.status,
                        "finalizat": q.status in ["Finalizat", "completed"]
                    }
                    for q in item_jobs
                ]
            })
            
        pret_final = c.pret_total if (c.pret_total and c.pret_total > 0) else valoare_calculata
        cost_final = c.cost_total_estimat if (c.cost_total_estimat and c.cost_total_estimat > 0) else cost_calculat
        profit_estimat = round(pret_final - cost_final, 2)
        
        rezultate.append({
            "id": c.id,
            "numar_comanda": c.numar_comanda or f"CMD-{c.id:03d}",
            "nume_client": c.nume_client,
            "telefon": c.telefon or "",
            "status": c.status,
            "pret_total": pret_final,
            "cost_total_estimat": cost_final,
            "profit_estimat": profit_estimat,
            "incasat": c.incasat,
            "data_incasare": c.data_incasare or "",
            "metoda_plata": c.metoda_plata or "Cash",
            "note": c.note or "",
            "data_adaugare": c.data_adaugare or "",
            "data_finalizare": c.data_finalizare or "",
            "produse": produse_list,
            "produse_count": len(produse_list),
            "total_bucati": sum(p["cantitate"] for p in produse_list),
            "printuri_total": total_printuri,
            "printuri_finalizate": finalizate_printuri
        })
    return rezultate

@app.post("/api/orders/")
def creeaza_comanda(req: OrderCreate, db: Session = Depends(get_db)):
    if not req.nume_client.strip():
        raise HTTPException(status_code=400, detail="Numele clientului este obligatoriu.")
    
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # Calculate totals from products
    cost_total_est = 0.0
    pret_total_calc = 0.0
    
    models_dict = {m.id: m for m in db.query(models.Model3D).all()}
    materials_dict = {m.id: m for m in db.query(models.Material).all()}
    
    for p in req.produse:
        cant = max(1, p.cantitate)
        pret_u = float(p.pret_unitar or 0.0)
        pret_total_calc += (pret_u * cant)
        
        cost_u = float(p.cost_unitar or 0.0)
        if cost_u <= 0 and p.model_id and p.model_id in models_dict:
            mod = models_dict[p.model_id]
            try:
                comps = json.loads(mod.componente_json or "[]")
                tot_gr = sum(float(c.get("greutate", 0)) for c in comps)
                mat_pkg = 100.0
                if p.material_id and p.material_id in materials_dict:
                    mat_pkg = materials_dict[p.material_id].pret_per_kg or 100.0
                calc_ex = calcul_cost_productie_excel(
                    gramaj_g=tot_gr,
                    timp_minute=mod.timp_estimat_minute or 0,
                    pret_kg_filament=mat_pkg,
                    diverse=0.0,
                    extra=2.96
                )
                cost_u = calc_ex["cost_productie"]
            except Exception:
                cost_u = 0.0
        cost_total_est += (cost_u * cant)
        
    pret_final = float(req.pret_total) if (req.pret_total is not None and req.pret_total > 0) else pret_total_calc
    
    db_order = models.Order(
        nume_client=req.nume_client.strip(),
        telefon=req.telefon or "",
        status=req.status or "În așteptare",
        pret_total=round(pret_final, 2),
        cost_total_estimat=round(cost_total_est, 2),
        incasat=req.incasat or 0,
        data_incasare=now_str if req.incasat == 1 else "",
        metoda_plata=req.metoda_plata or "Cash",
        note=req.note or "",
        data_adaugare=now_str
    )
    db.add(db_order)
    db.flush()
    
    db_order.numar_comanda = f"CMD-{db_order.id:03d}"
    
    max_ord = db.query(func.max(models.PrintQueue.ordine)).scalar() or 0
    next_ord = max_ord + 1
    
    for p in req.produse:
        mod = models_dict.get(p.model_id) if p.model_id else None
        prod_nume = p.nume_produs or (mod.nume_proiect if mod else "Produs Custom")
        cant = max(1, p.cantitate)
        pret_u = float(p.pret_unitar or 0.0)
        cost_u = float(p.cost_unitar or 0.0)
        if cost_u <= 0 and mod:
            try:
                comps = json.loads(mod.componente_json or "[]")
                tot_gr = sum(float(c.get("greutate", 0)) for c in comps)
                mat_pkg = 100.0
                if p.material_id and p.material_id in materials_dict:
                    mat_pkg = materials_dict[p.material_id].pret_per_kg or 100.0
                calc_ex = calcul_cost_productie_excel(
                    gramaj_g=tot_gr,
                    timp_minute=mod.timp_estimat_minute or 0,
                    pret_kg_filament=mat_pkg,
                    diverse=0.0,
                    extra=2.96
                )
                cost_u = calc_ex["cost_productie"]
            except Exception:
                pass
                
        db_item = models.OrderItem(
            order_id=db_order.id,
            model_id=p.model_id,
            nume_produs=prod_nume,
            cantitate=cant,
            material_id=p.material_id,
            culoare_dorita=p.culoare_dorita or "",
            pret_unitar=pret_u,
            cost_unitar=cost_u,
            status="În așteptare",
            note=p.note or ""
        )
        db.add(db_item)
        db.flush()
        
        # Trimite în coada de printare dacă este bifat
        if req.trimite_in_coada and p.model_id:
            for b_idx in range(cant):
                buc_label = f" (Buc. {b_idx + 1}/{cant})" if cant > 1 else ""
                q_job = models.PrintQueue(
                    model_id=p.model_id,
                    order_id=db_order.id,
                    order_item_id=db_item.id,
                    prioritate=req.prioritate_coada or 2,
                    status="În așteptare",
                    nume_client=f"{db_order.nume_client}{buc_label}",
                    cost_estimat=cost_u,
                    pret_vanzare=pret_u,
                    note=f"Comandă #{db_order.numar_comanda}: {prod_nume}. {p.note or ''}".strip(),
                    ordine=next_ord,
                    material_id=p.material_id,
                    culoare_dorita=p.culoare_dorita or "",
                    data_adaugare=now_str
                )
                db.add(q_job)
                next_ord += 1
                
    db.commit()
    db.refresh(db_order)
    return {
        "success": True,
        "order_id": db_order.id,
        "numar_comanda": db_order.numar_comanda,
        "mesaj": f"Comanda '{db_order.numar_comanda}' pentru clientul {db_order.nume_client} a fost creată cu succes!"
    }

# ========================================================
# --- BATCH / LOTURI PRODUSE REPETATE API ROUTES ---
# ========================================================
@app.get("/api/orders/batch-summary")
def sumar_loturi_produse_repetate(doar_repetate: bool = False, db: Session = Depends(get_db)):
    """
    Identifică produsele care se repetă în comenzi nefinalizate și oferă
    situația cantităților necesare pentru printare comună pe loturi / plachete.
    """
    from collections import defaultdict

    active_orders = db.query(models.Order).filter(
        ~models.Order.status.in_(["Finalizată", "Livrată", "Anulată"])
    ).order_by(models.Order.id.asc()).all()

    models_dict = {m.id: m for m in db.query(models.Model3D).all()}
    materials_dict = {m.id: m for m in db.query(models.Material).all()}
    queue_jobs = db.query(models.PrintQueue).filter(models.PrintQueue.order_item_id != None).all()
    
    jobs_by_item = defaultdict(list)
    for q in queue_jobs:
        jobs_by_item[q.order_item_id].append(q)

    groups = {}
    total_bucati_active = 0
    total_bucati_ramase = 0

    for ord_rec in active_orders:
        items = db.query(models.OrderItem).filter(models.OrderItem.order_id == ord_rec.id).all()
        for it in items:
            q_list = jobs_by_item.get(it.id, [])
            fin_jobs = sum(1 for q in q_list if q.status in ["Finalizat", "completed"])
            cant = it.cantitate or 1
            total_bucati_active += cant

            if it.status == "Finalizat":
                fin_bucati = cant
                ramase_bucati = 0
            elif q_list:
                fin_bucati = fin_jobs
                ramase_bucati = max(0, cant - fin_bucati)
            else:
                fin_bucati = 0
                ramase_bucati = cant

            total_bucati_ramase += ramase_bucati

            norm_nume = (it.nume_produs or "").strip()
            key = (it.model_id, norm_nume)
            
            if key not in groups:
                mod = models_dict.get(it.model_id) if it.model_id else None
                mat = materials_dict.get(it.material_id) if it.material_id else None
                groups[key] = {
                    "model_id": it.model_id,
                    "nume_produs": norm_nume or (mod.nume_proiect if mod else "Produs Custom"),
                    "link_imagine": mod.link_imagine if mod else None,
                    "timp_estimat_bucata": mod.timp_estimat_minute if mod else 0,
                    "total_cantitate": 0,
                    "total_ramase": 0,
                    "total_finalizate": 0,
                    "comenzi_set": set(),
                    "culori_counts": defaultdict(int),
                    "comenzi": []
                }

            grp = groups[key]
            grp["total_cantitate"] += cant
            grp["total_ramase"] += ramase_bucati
            grp["total_finalizate"] += fin_bucati
            grp["comenzi_set"].add(ord_rec.id)

            culoare_label = (it.culoare_dorita or "").strip()
            if not culoare_label:
                mat = materials_dict.get(it.material_id) if it.material_id else None
                culoare_label = f"{mat.tip} {mat.culoare}".strip() if mat else "Implicită"
            grp["culori_counts"][culoare_label] += cant

            mat = materials_dict.get(it.material_id) if it.material_id else None
            grp["comenzi"].append({
                "order_id": ord_rec.id,
                "numar_comanda": ord_rec.numar_comanda or f"CMD-{ord_rec.id:03d}",
                "nume_client": ord_rec.nume_client,
                "order_item_id": it.id,
                "cantitate": cant,
                "ramase": ramase_bucati,
                "finalizate": fin_bucati,
                "culoare_dorita": it.culoare_dorita or "",
                "material_hex": mat.culoare_hex if mat else "",
                "status": it.status,
                "este_finalizat": (ramase_bucati == 0),
                "bucati_jobs": [
                    {
                        "id": q.id,
                        "nume": q.nume_client,
                        "status": q.status,
                        "finalizat": q.status in ["Finalizat", "completed"]
                    }
                    for q in q_list
                ]
            })

    lista_grupuri = []
    total_modele_repetate = 0
    total_bucati_repetate = 0

    for k, g in groups.items():
        nr_comenzi = len(g["comenzi_set"])
        se_repeta_comenzi = nr_comenzi > 1
        are_multiple = se_repeta_comenzi or (g["total_cantitate"] > 1)

        if se_repeta_comenzi:
            total_modele_repetate += 1
            total_bucati_repetate += g["total_ramase"]

        if doar_repetate and not se_repeta_comenzi:
            continue

        culori_list = [{"culoare": c, "cantitate": cnt} for c, cnt in g["culori_counts"].items()]
        culori_list.sort(key=lambda x: x["cantitate"], reverse=True)

        lista_grupuri.append({
            "model_id": g["model_id"],
            "nume_produs": g["nume_produs"],
            "link_imagine": g["link_imagine"],
            "timp_estimat_bucata": g["timp_estimat_bucata"],
            "total_cantitate": g["total_cantitate"],
            "total_ramase": g["total_ramase"],
            "total_finalizate": g["total_finalizate"],
            "numar_comenzi": nr_comenzi,
            "se_repeta_comenzi": se_repeta_comenzi,
            "are_multiple": are_multiple,
            "culori": culori_list,
            "comenzi": g["comenzi"]
        })

    # Sortare: cele cu piese rămase primele, descrescător după bucăți rămase și număr comenzi
    lista_grupuri.sort(key=lambda x: (x["total_ramase"] > 0, x["total_ramase"], x["numar_comenzi"]), reverse=True)

    return {
        "success": True,
        "sumar": {
            "total_comenzi_active": len(active_orders),
            "total_bucati_active": total_bucati_active,
            "total_bucati_ramase": total_bucati_ramase,
            "total_modele_grupate": len(lista_grupuri),
            "total_modele_repetate": total_modele_repetate,
            "total_bucati_repetate": total_bucati_repetate
        },
        "grupuri": lista_grupuri
    }

@app.post("/api/orders/batch-complete")
def finalizeaza_lot_produse(req: BatchCompleteRequest, db: Session = Depends(get_db)):
    """
    Marchează un număr de bucăți din lot ca printate, distribuindu-le
    automat în comenzile active în ordinea vechimii comenzilor (FIFO).
    """
    if req.bucati <= 0:
        raise HTTPException(status_code=400, detail="Numărul de bucăți trebuie să fie minim 1.")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # Găsim comenzile active în ordine FIFO
    active_orders = db.query(models.Order).filter(
        ~models.Order.status.in_(["Finalizată", "Livrată", "Anulată"])
    ).order_by(models.Order.id.asc()).all()

    bucati_ramase_de_marcat = req.bucati
    bucati_marcate = 0
    comenzi_afectate = set()

    for ord_rec in active_orders:
        if bucati_ramase_de_marcat <= 0:
            break

        query = db.query(models.OrderItem).filter(models.OrderItem.order_id == ord_rec.id)
        if req.model_id:
            query = query.filter(models.OrderItem.model_id == req.model_id)
        elif req.nume_produs:
            query = query.filter(models.OrderItem.nume_produs == req.nume_produs.strip())
        else:
            continue

        items = query.all()
        for it in items:
            if bucati_ramase_de_marcat <= 0:
                break

            # Găsim joburile nefinalizate din coadă asociate acestui produs
            q_jobs = db.query(models.PrintQueue).filter(
                models.PrintQueue.order_item_id == it.id
            ).order_by(models.PrintQueue.id.asc()).all()

            pending_jobs = [q for q in q_jobs if q.status not in ["Finalizat", "completed"]]

            if pending_jobs:
                for qj in pending_jobs:
                    if bucati_ramase_de_marcat <= 0:
                        break
                    qj.status = "Finalizat"
                    qj.data_finalizare = now_str
                    scade_stoc_pentru_job(qj, db)
                    bucati_marcate += 1
                    bucati_ramase_de_marcat -= 1
                    comenzi_afectate.add(ord_rec.id)

                # Reevaluare status produs
                all_fin = all(q.status in ["Finalizat", "completed"] for q in q_jobs)
                if all_fin:
                    it.status = "Finalizat"
            else:
                # Produs fără joburi individuale în coadă
                if it.status != "Finalizat":
                    marcare_aici = min(it.cantitate, bucati_ramase_de_marcat)
                    it.status = "Finalizat"
                    bucati_marcate += marcare_aici
                    bucati_ramase_de_marcat -= marcare_aici
                    comenzi_afectate.add(ord_rec.id)

        # Reevaluare stare comandă
        toate_itemele = db.query(models.OrderItem).filter(models.OrderItem.order_id == ord_rec.id).all()
        if toate_itemele:
            if all(item_rec.status == "Finalizat" for item_rec in toate_itemele):
                ord_rec.status = "Finalizată"
                if not ord_rec.data_finalizare:
                    ord_rec.data_finalizare = now_str
            elif any(item_rec.status == "Finalizat" for item_rec in toate_itemele):
                if ord_rec.status not in ["Livrată"]:
                    ord_rec.status = "În lucru"

    db.commit()

    if bucati_marcate == 0:
        return {
            "success": False,
            "bucati_marcate": 0,
            "mesaj": "Nu au fost găsite bucăți nefinalizate pentru acest produs în comenzile active."
        }

    return {
        "success": True,
        "bucati_marcate": bucati_marcate,
        "comenzi_afectate_count": len(comenzi_afectate),
        "mesaj": f"Au fost marcate cu succes {bucati_marcate} bucăți ca printate în {len(comenzi_afectate)} comenzi active!"
    }

@app.post("/api/orders/batch-send-to-queue")
def adauga_lot_in_coada(req: BatchQueueCreate, db: Session = Depends(get_db)):
    """
    Adaugă un job de lot/plachetă comună direct în coada de imprimare
    pentru a printa mai multe bucăți deodată pe patul imprimantei.
    """
    model = db.query(models.Model3D).filter(models.Model3D.id == req.model_id).first()
    if not model:
        raise HTTPException(status_code=404, detail="Model 3D inexistent")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    max_ord = db.query(func.max(models.PrintQueue.ordine)).scalar() or 0
    next_ord = max_ord + 1

    titlu_lot = req.nume_lot or f"Lot Plachetă {req.cantitate}x {model.nume_proiect}"
    cost_total_lot = 0.0
    try:
        comps = json.loads(model.componente_json or "[]")
        tot_gr = sum(float(c.get("greutate", 0)) for c in comps) * req.cantitate
        calc_ex = calcul_cost_productie_excel(
            gramaj_g=tot_gr,
            timp_minute=(model.timp_estimat_minute or 0) * req.cantitate,
            pret_kg_filament=100.0,
            diverse=0.0,
            extra=2.96
        )
        cost_total_lot = calc_ex["cost_productie"]
    except Exception:
        pass

    q_job = models.PrintQueue(
        model_id=model.id,
        order_id=None,
        order_item_id=None,
        prioritate=req.prioritate or 1,
        status="În așteptare",
        nume_client=f"Plachetă Comună ({req.cantitate} buc)",
        cost_estimat=cost_total_lot,
        pret_vanzare=0.0,
        note=f"{titlu_lot}. {req.note or ''}".strip(),
        ordine=next_ord,
        data_adaugare=now_str
    )
    db.add(q_job)
    db.commit()
    db.refresh(q_job)

    return {
        "success": True,
        "job_id": q_job.id,
        "mesaj": f"Lotul de {req.cantitate} bucăți pentru '{model.nume_proiect}' a fost adăugat în coada de printare!"
    }

@app.get("/api/orders/{order_id}")
def obtine_comanda(order_id: int, db: Session = Depends(get_db)):
    c = db.query(models.Order).filter(models.Order.id == order_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Comandă inexistentă")
    items = db.query(models.OrderItem).filter(models.OrderItem.order_id == c.id).all()
    queue_jobs = db.query(models.PrintQueue).filter(models.PrintQueue.order_id == c.id).all()
    
    models_dict = {m.id: m for m in db.query(models.Model3D).all()}
    materials_dict = {m.id: m for m in db.query(models.Material).all()}
    
    produse_list = []
    cost_calculat = 0.0
    valoare_calculata = 0.0
    for it in items:
        mod = models_dict.get(it.model_id) if it.model_id else None
        mat = materials_dict.get(it.material_id) if it.material_id else None
        cant = it.cantitate or 1
        pret_u = float(it.pret_unitar or 0.0)
        cost_u = float(it.cost_unitar or 0.0)
        subtotal = round(pret_u * cant, 2)
        cost_total_item = round(cost_u * cant, 2)
        cost_calculat += cost_total_item
        valoare_calculata += subtotal
        profit_item = round(subtotal - cost_total_item, 2)
        marja_item = round((profit_item / subtotal * 100), 1) if subtotal > 0 else 0
        
        item_jobs = [q for q in queue_jobs if q.order_item_id == it.id]
        item_jobs_fin = sum(1 for q in item_jobs if q.status in ["Finalizat", "completed"])
        este_finalizat = (it.status == "Finalizat") or (len(item_jobs) > 0 and item_jobs_fin == len(item_jobs))
        
        produse_list.append({
            "id": it.id,
            "order_id": it.order_id,
            "model_id": it.model_id,
            "nume_produs": it.nume_produs or (mod.nume_proiect if mod else "Produs Custom"),
            "cantitate": cant,
            "material_id": it.material_id,
            "material_nume": f"{mat.tip} {mat.culoare}" if mat else "",
            "material_hex": mat.culoare_hex if mat else "",
            "culoare_dorita": it.culoare_dorita or (mat.culoare if mat else ""),
            "pret_unitar": pret_u,
            "cost_unitar": cost_u,
            "subtotal": subtotal,
            "cost_total": cost_total_item,
            "profit": profit_item,
            "marja": marja_item,
            "status": "Finalizat" if este_finalizat else (it.status or "În așteptare"),
            "este_finalizat": este_finalizat,
            "printuri_total": len(item_jobs),
            "printuri_finalizate": item_jobs_fin,
            "note": it.note or "",
            "link_imagine": mod.link_imagine if mod else None,
            "timp_estimat_minute": (mod.timp_estimat_minute * cant) if mod else 0,
            "bucati_jobs": [
                {
                    "id": q.id,
                    "nume": q.nume_client,
                    "status": q.status,
                    "finalizat": q.status in ["Finalizat", "completed"]
                }
                for q in item_jobs
            ]
        })
        
    pret_final = c.pret_total if (c.pret_total and c.pret_total > 0) else valoare_calculata
    cost_final = c.cost_total_estimat if (c.cost_total_estimat and c.cost_total_estimat > 0) else cost_calculat
    profit_estimat = round(pret_final - cost_final, 2)
    marja_globala = round((profit_estimat / pret_final * 100), 1) if pret_final > 0 else 0
    
    queue_list = []
    finalizate_count = 0
    for q in queue_jobs:
        mod = models_dict.get(q.model_id) if q.model_id else None
        mat = materials_dict.get(q.material_id) if q.material_id else None
        if q.status in ["Finalizat", "completed"]:
            finalizate_count += 1
        queue_list.append({
            "id": q.id,
            "model_id": q.model_id,
            "model_nume": mod.nume_proiect if mod else "Print 3D",
            "link_imagine": mod.link_imagine if mod else None,
            "timp_estimat_minute": mod.timp_estimat_minute if mod else 0,
            "status": q.status,
            "prioritate": q.prioritate,
            "cost_estimat": q.cost_estimat,
            "pret_vanzare": q.pret_vanzare,
            "material_nume": f"{mat.tip} {mat.culoare}" if mat else "",
            "material_hex": mat.culoare_hex if mat else "",
            "culoare_dorita": q.culoare_dorita or "",
            "data_programata": q.data_programata or "",
            "ora_start_estimat": q.ora_start_estimat or "",
            "ora_finish_estimat": q.ora_finish_estimat or "",
            "data_adaugare": q.data_adaugare or "",
            "data_finalizare": q.data_finalizare or "",
            "note": q.note or ""
        })
        
    order_dict = {
        "id": c.id,
        "numar_comanda": c.numar_comanda or f"CMD-{c.id:03d}",
        "nume_client": c.nume_client,
        "telefon": c.telefon or "",
        "status": c.status,
        "pret_total": pret_final,
        "cost_total_estimat": cost_final,
        "profit_estimat": profit_estimat,
        "marja_procent": marja_globala,
        "incasat": c.incasat,
        "data_incasare": c.data_incasare or "",
        "metoda_plata": c.metoda_plata or "Cash",
        "note": c.note or "",
        "data_adaugare": c.data_adaugare or "",
        "data_finalizare": c.data_finalizare or "",
        "total_bucati": sum(p["cantitate"] for p in produse_list),
        "printuri_total": len(queue_jobs),
        "printuri_finalizate": finalizate_count
    }
    
    return {
        "order": order_dict,
        "produse": produse_list,
        "printuri_asociate": queue_list
    }

@app.put("/api/orders/{order_id}")
def actualizeaza_comanda(order_id: int, update: OrderUpdate, db: Session = Depends(get_db)):
    c = db.query(models.Order).filter(models.Order.id == order_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Comandă inexistentă")
    
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    if update.nume_client is not None and update.nume_client.strip():
        c.nume_client = update.nume_client.strip()
    if update.telefon is not None:
        c.telefon = update.telefon
    if update.status is not None:
        c.status = update.status
        if update.status in ["Finalizată", "Livrată"] and not c.data_finalizare:
            c.data_finalizare = now_str
        elif update.status not in ["Finalizată", "Livrată"]:
            c.data_finalizare = ""
    if update.incasat is not None:
        c.incasat = update.incasat
        if update.incasat == 1 and not c.data_incasare:
            c.data_incasare = now_str
        elif update.incasat == 0:
            c.data_incasare = ""
    if update.data_incasare is not None:
        c.data_incasare = update.data_incasare
    if update.metoda_plata is not None:
        c.metoda_plata = update.metoda_plata
    if update.note is not None:
        c.note = update.note

    if update.produse is not None:
        existing_items = db.query(models.OrderItem).filter(models.OrderItem.order_id == c.id).all()
        existing_items_dict = {it.id: it for it in existing_items}
        existing_queue_jobs = db.query(models.PrintQueue).filter(models.PrintQueue.order_id == c.id).all()
        
        models_dict = {m.id: m for m in db.query(models.Model3D).all()}
        materials_dict = {m.id: m for m in db.query(models.Material).all()}
        
        max_ord = db.query(func.max(models.PrintQueue.ordine)).scalar() or 0
        next_ord = max_ord + 1
        
        seen_item_ids = set()
        cost_total_est = 0.0
        pret_total_calc = 0.0
        
        for p in update.produse:
            cant = max(1, p.cantitate)
            pret_u = float(p.pret_unitar or 0.0)
            cost_u = float(p.cost_unitar or 0.0)
            mod = models_dict.get(p.model_id) if p.model_id else None
            prod_nume = p.nume_produs or (mod.nume_proiect if mod else "Produs Custom")
            
            if cost_u <= 0 and mod:
                try:
                    comps = json.loads(mod.componente_json or "[]")
                    tot_gr = sum(float(comp.get("greutate", 0)) for comp in comps)
                    mat_pkg = 100.0
                    if p.material_id and p.material_id in materials_dict:
                        mat_pkg = materials_dict[p.material_id].pret_per_kg or 100.0
                    calc_ex = calcul_cost_productie_excel(
                        gramaj_g=tot_gr,
                        timp_minute=mod.timp_estimat_minute or 0,
                        pret_kg_filament=mat_pkg,
                        diverse=0.0,
                        extra=2.96
                    )
                    cost_u = calc_ex["cost_productie"]
                except Exception:
                    cost_u = 0.0
                    
            cost_total_est += (cost_u * cant)
            pret_total_calc += (pret_u * cant)
            
            # Verificam daca este un produs deja existent sau nou
            if p.id and p.id in existing_items_dict:
                db_item = existing_items_dict[p.id]
                seen_item_ids.add(db_item.id)
                db_item.model_id = p.model_id
                db_item.nume_produs = prod_nume
                db_item.cantitate = cant
                db_item.material_id = p.material_id
                db_item.culoare_dorita = p.culoare_dorita or ""
                db_item.pret_unitar = pret_u
                db_item.cost_unitar = cost_u
                if p.note:
                    db_item.note = p.note
                
                # Gestionare joburi de printare pentru produs existent
                item_jobs = [q for q in existing_queue_jobs if q.order_item_id == db_item.id]
                if cant > len(item_jobs) and update.trimite_in_coada and p.model_id:
                    extra_needed = cant - len(item_jobs)
                    for idx in range(extra_needed):
                        piece_no = len(item_jobs) + idx + 1
                        buc_label = f" (Buc. {piece_no}/{cant})" if cant > 1 else ""
                        q_job = models.PrintQueue(
                            model_id=p.model_id,
                            order_id=c.id,
                            order_item_id=db_item.id,
                            prioritate=update.prioritate_coada or 2,
                            status="În așteptare",
                            nume_client=f"{c.nume_client}{buc_label}",
                            cost_estimat=cost_u,
                            pret_vanzare=pret_u,
                            note=f"Comandă #{c.numar_comanda}: {prod_nume}. {p.note or ''}".strip(),
                            ordine=next_ord,
                            material_id=p.material_id,
                            culoare_dorita=p.culoare_dorita or "",
                            data_adaugare=now_str
                        )
                        db.add(q_job)
                        next_ord += 1
                elif cant < len(item_jobs):
                    pending_jobs = [q for q in item_jobs if q.status == "În așteptare"]
                    to_remove_count = len(item_jobs) - cant
                    for q in reversed(pending_jobs):
                        if to_remove_count <= 0:
                            break
                        db.delete(q)
                        to_remove_count -= 1
                
                for q in item_jobs:
                    if q.status == "În așteptare":
                        q.cost_estimat = cost_u
                        q.pret_vanzare = pret_u
                        q.material_id = p.material_id
                        q.culoare_dorita = p.culoare_dorita or ""
            else:
                # Produs NOU adăugat în comandă
                db_item = models.OrderItem(
                    order_id=c.id,
                    model_id=p.model_id,
                    nume_produs=prod_nume,
                    cantitate=cant,
                    material_id=p.material_id,
                    culoare_dorita=p.culoare_dorita or "",
                    pret_unitar=pret_u,
                    cost_unitar=cost_u,
                    status="În așteptare",
                    note=p.note or ""
                )
                db.add(db_item)
                db.flush()
                seen_item_ids.add(db_item.id)
                
                if update.trimite_in_coada and p.model_id:
                    for b_idx in range(cant):
                        buc_label = f" (Buc. {b_idx + 1}/{cant})" if cant > 1 else ""
                        q_job = models.PrintQueue(
                            model_id=p.model_id,
                            order_id=c.id,
                            order_item_id=db_item.id,
                            prioritate=update.prioritate_coada or 2,
                            status="În așteptare",
                            nume_client=f"{c.nume_client}{buc_label}",
                            cost_estimat=cost_u,
                            pret_vanzare=pret_u,
                            note=f"Comandă #{c.numar_comanda}: {prod_nume}. {p.note or ''}".strip(),
                            ordine=next_ord,
                            material_id=p.material_id,
                            culoare_dorita=p.culoare_dorita or "",
                            data_adaugare=now_str
                        )
                        db.add(q_job)
                        next_ord += 1
                        
        # Ștergem produsele care au fost eliminate din formular
        for old_item_id, old_item in existing_items_dict.items():
            if old_item_id not in seen_item_ids:
                jobs_to_delete = db.query(models.PrintQueue).filter(
                    models.PrintQueue.order_item_id == old_item_id,
                    models.PrintQueue.status == "În așteptare"
                ).all()
                for qj in jobs_to_delete:
                    db.delete(qj)
                finished_jobs = db.query(models.PrintQueue).filter(
                    models.PrintQueue.order_item_id == old_item_id
                ).all()
                for fj in finished_jobs:
                    fj.order_item_id = None
                db.delete(old_item)
                
        # Actualizare cost total estimat și preț de vânzare
        c.cost_total_estimat = round(cost_total_est, 2)
        if update.pret_total is not None and update.pret_total > 0:
            c.pret_total = round(update.pret_total, 2)
        else:
            c.pret_total = round(pret_total_calc, 2)
    elif update.pret_total is not None:
        c.pret_total = round(update.pret_total, 2)
        
    if update.nume_client:
        for qj in db.query(models.PrintQueue).filter(models.PrintQueue.order_id == c.id).all():
            if qj.status == "În așteptare":
                if "(" in qj.nume_client and ")" in qj.nume_client:
                    suffix = qj.nume_client[qj.nume_client.rfind("("):]
                    qj.nume_client = f"{c.nume_client} {suffix}".strip()
                else:
                    qj.nume_client = c.nume_client

    db.commit()
    db.refresh(c)
    return {"success": True, "mesaj": f"Comanda {c.numar_comanda} a fost actualizată cu succes!", "order": c}

@app.post("/api/orders/{order_id}/toggle-payment")
def comuta_plata_comanda(order_id: int, db: Session = Depends(get_db)):
    order = db.query(models.Order).filter(models.Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Comandă inexistentă")
    
    if order.incasat == 1:
        order.incasat = 0
        order.data_incasare = ""
        mesaj = f"Comanda {order.numar_comanda} a fost marcată ca NEÎNCASATĂ."
    else:
        order.incasat = 1
        order.data_incasare = datetime.now().strftime("%Y-%m-%d %H:%M")
        mesaj = f"Comanda {order.numar_comanda} ({order.pret_total} RON) a fost marcată ca ÎNCASATĂ!"
        
    db.commit()
    return {"success": True, "incasat": order.incasat, "data_incasare": order.data_incasare, "mesaj": mesaj}

@app.post("/api/orders/{order_id}/items/{item_id}/toggle-status")
def comuta_status_produs_comanda(order_id: int, item_id: int, req: Optional[ToggleItemStatusRequest] = None, db: Session = Depends(get_db)):
    c = db.query(models.Order).filter(models.Order.id == order_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Comandă inexistentă")
        
    item = db.query(models.OrderItem).filter(
        models.OrderItem.id == item_id,
        models.OrderItem.order_id == order_id
    ).first()
    if not item:
        raise HTTPException(status_code=404, detail="Produs inexistent în comandă")
        
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    if req and req.finalizat is not None:
        este_finalizat = req.finalizat
    else:
        este_finalizat = (item.status != "Finalizat")
        
    nou_status = "Finalizat" if este_finalizat else "În așteptare"
    item.status = nou_status
    
    # Sincronizăm toate printurile asociate acestui produs
    queue_jobs = db.query(models.PrintQueue).filter(
        models.PrintQueue.order_item_id == item.id
    ).all()
    
    for q in queue_jobs:
        if este_finalizat:
            if q.status != "Finalizat":
                q.status = "Finalizat"
                q.data_finalizare = now_str
                scade_stoc_pentru_job(q, db)
        else:
            q.status = "În așteptare"
            q.data_finalizare = ""
            
    # Evaluăm starea globală a comenzii pe baza produselor
    toate_itemele = db.query(models.OrderItem).filter(models.OrderItem.order_id == order_id).all()
    if toate_itemele:
        toate_gata = all(it.status == "Finalizat" for it in toate_itemele)
        vreunul_gata = any(it.status == "Finalizat" for it in toate_itemele)
        
        if toate_gata:
            c.status = "Finalizată"
            if not c.data_finalizare:
                c.data_finalizare = now_str
        elif vreunul_gata:
            if c.status not in ["Livrată"]:
                c.status = "În lucru"
        else:
            if c.status == "Finalizată":
                c.status = "În așteptare"
                c.data_finalizare = ""
                
    db.commit()
    db.refresh(item)
    db.refresh(c)
    
    stare_text = "printat" if este_finalizat else "în așteptare"
    return {
        "success": True,
        "item_id": item.id,
        "status": item.status,
        "este_finalizat": este_finalizat,
        "order_status": c.status,
        "mesaj": f"'{item.nume_produs}' marcat ca {stare_text}!"
    }

@app.post("/api/queue/{job_id}/toggle-status")
def comuta_status_job_coada(job_id: int, req: Optional[ToggleItemStatusRequest] = None, db: Session = Depends(get_db)):
    job = db.query(models.PrintQueue).filter(models.PrintQueue.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job de printare inexistent")
        
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    if req and req.finalizat is not None:
        este_finalizat = req.finalizat
    else:
        este_finalizat = (job.status not in ["Finalizat", "completed"])
        
    if este_finalizat:
        if job.status not in ["Finalizat", "completed"]:
            scade_stoc_pentru_job(job, db)
        job.status = "Finalizat"
        job.data_finalizare = now_str
    else:
        job.status = "În așteptare"
        job.data_finalizare = ""
        
    # Dacă ține de un order_item, verificăm dacă toate bucățile acelui produs sunt gata
    if job.order_item_id:
        sibling_jobs = db.query(models.PrintQueue).filter(models.PrintQueue.order_item_id == job.order_item_id).all()
        item = db.query(models.OrderItem).filter(models.OrderItem.id == job.order_item_id).first()
        if item and sibling_jobs:
            if all(sj.status in ["Finalizat", "completed"] for sj in sibling_jobs):
                item.status = "Finalizat"
            else:
                item.status = "În așteptare"
                
    # Dacă ține de o comandă, sincronizăm și statusul comenzii
    if job.order_id:
        c = db.query(models.Order).filter(models.Order.id == job.order_id).first()
        if c:
            all_q = db.query(models.PrintQueue).filter(models.PrintQueue.order_id == job.order_id).all()
            if all_q:
                toate_q_gata = all(qj.status in ["Finalizat", "completed"] for qj in all_q)
                vreunul_q_gata = any(qj.status in ["Finalizat", "completed"] for qj in all_q)
                if toate_q_gata:
                    c.status = "Finalizată"
                    if not c.data_finalizare:
                        c.data_finalizare = now_str
                elif vreunul_q_gata:
                    if c.status not in ["Livrată"]:
                        c.status = "În lucru"
                else:
                    if c.status == "Finalizată":
                        c.status = "În așteptare"
                        c.data_finalizare = ""
                        
    db.commit()
    db.refresh(job)
    return {
        "success": True,
        "job_id": job.id,
        "status": job.status,
        "este_finalizat": este_finalizat,
        "mesaj": f"Piesă printată ({job.nume_client}): {job.status}!"
    }

@app.delete("/api/orders/{order_id}")
def sterge_comanda(order_id: int, db: Session = Depends(get_db)):
    c = db.query(models.Order).filter(models.Order.id == order_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Comandă inexistentă")
    
    # Ștergem produsele din comandă
    db.query(models.OrderItem).filter(models.OrderItem.order_id == order_id).delete()
    # Ștergem printurile asociate din coadă
    db.query(models.PrintQueue).filter(models.PrintQueue.order_id == order_id).delete()
    db.delete(c)
    db.commit()
    return {"success": True, "mesaj": f"Comanda #{c.numar_comanda} a fost ștearsă cu succes!"}

# ========================================================
# --- FILAMENT PURCHASES & EXPENSES API ROUTES ---
# ========================================================
@app.get("/api/filament-purchases/")
def listeaza_achizitii_filamente(db: Session = Depends(get_db)):
    return db.query(models.FilamentPurchase).order_by(models.FilamentPurchase.id.desc()).all()

@app.post("/api/filament-purchases/")
def creeaza_achizitie_filament(req: FilamentPurchaseCreate, db: Session = Depends(get_db)):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    cost_transport = float(req.cost_transport or 0.0)
    role = max(1, req.cantitate_role)
    pret_rola = float(req.pret_per_rola or 90.0)
    cost_total = (role * pret_rola) + cost_transport
    
    furnizor = req.furnizor.strip() if req.furnizor else "Bambu Lab EU"
    brand = req.brand.strip() if req.brand else "Bambu Lab"
    tip = req.tip.strip() if req.tip else "PLA Basic"
    hex_code = req.culoare_hex or "#00e575"
    culoare_nume = req.culoare.strip() if req.culoare else get_romanian_color_name(hex_code)
    greutate_rola = float(req.greutate_per_rola_g or 1000.0)
    
    achizitie = models.FilamentPurchase(
        furnizor=furnizor,
        brand=brand,
        tip=tip,
        culoare=culoare_nume,
        culoare_hex=hex_code,
        cantitate_role=role,
        greutate_per_rola_g=greutate_rola,
        pret_per_rola=pret_rola,
        cost_transport=cost_transport,
        cost_total=round(cost_total, 2),
        data_achizitie=req.data_achizitie or now_str.split(" ")[0],
        status=req.status or "Livrat / În stoc",
        adaugat_in_stoc=1 if req.adaugat_in_stoc else 0,
        note=req.note or ""
    )
    db.add(achizitie)
    db.flush()
    
    mesaj_stoc = ""
    if req.adaugat_in_stoc and achizitie.status == "Livrat / În stoc":
        total_grame_noi = role * greutate_rola
        pret_kg = round((pret_rola / (greutate_rola / 1000.0)), 2)
        
        mat_existent = None
        for m in db.query(models.Material).filter(models.Material.tip.ilike(tip)).all():
            if color_distance(m.culoare_hex, hex_code) <= 25 or (m.culoare and m.culoare.strip().lower() == culoare_nume.lower()):
                mat_existent = m
                break
                
        if mat_existent:
            mat_existent.greutate_ramasa = round(mat_existent.greutate_ramasa + total_grame_noi, 1)
            mat_existent.pret_per_kg = pret_kg
            mesaj_stoc = f"Stocul existent pentru '{mat_existent.tip} {mat_existent.culoare}' a fost suplimentat cu +{total_grame_noi}g (Total: {mat_existent.greutate_ramasa}g)!"
        else:
            nou_mat = models.Material(
                tip=tip,
                culoare=culoare_nume,
                culoare_hex=hex_code,
                greutate_ramasa=total_grame_noi,
                pret_per_kg=pret_kg,
                brand=brand,
                locatie_slot=""
            )
            db.add(nou_mat)
            mesaj_stoc = f"Material nou '{nou_mat.tip} {nou_mat.culoare}' (+{total_grame_noi}g) a fost înregistrat în stoc!"
            
    db.commit()
    db.refresh(achizitie)
    return {
        "success": True,
        "id": achizitie.id,
        "cost_total": achizitie.cost_total,
        "mesaj": f"Achiziția de filament ({role} role {tip} {culoare_nume}) a fost salvată! {mesaj_stoc}".strip()
    }

@app.post("/api/filament-purchases/{purchase_id}/add-to-stock")
def adauga_achizitie_in_stoc(purchase_id: int, db: Session = Depends(get_db)):
    achizitie = db.query(models.FilamentPurchase).filter(models.FilamentPurchase.id == purchase_id).first()
    if not achizitie:
        raise HTTPException(status_code=404, detail="Achiziție inexistentă")
    if achizitie.adaugat_in_stoc == 1:
        return {"success": True, "mesaj": "Acest filament a fost deja adăugat în stoc!"}
        
    total_grame = achizitie.cantitate_role * achizitie.greutate_per_rola_g
    pret_kg = round(achizitie.pret_per_rola / (achizitie.greutate_per_rola_g / 1000.0), 2)
    
    mat_existent = None
    for m in db.query(models.Material).filter(models.Material.tip.ilike(achizitie.tip)).all():
        if color_distance(m.culoare_hex, achizitie.culoare_hex) <= 25 or (m.culoare and m.culoare.strip().lower() == achizitie.culoare.lower()):
            mat_existent = m
            break
            
    if mat_existent:
        mat_existent.greutate_ramasa = round(mat_existent.greutate_ramasa + total_grame, 1)
        mat_existent.pret_per_kg = pret_kg
        mesaj = f"Stocul existent a fost suplimentat cu +{total_grame}g!"
    else:
        nou_mat = models.Material(
            tip=achizitie.tip,
            culoare=achizitie.culoare,
            culoare_hex=achizitie.culoare_hex,
            greutate_ramasa=total_grame,
            pret_per_kg=pret_kg,
            brand=achizitie.brand,
            locatie_slot=""
        )
        db.add(nou_mat)
        mesaj = f"Filamentul '{nou_mat.tip} {nou_mat.culoare}' a fost adăugat în stoc!"
        
    achizitie.adaugat_in_stoc = 1
    achizitie.status = "Livrat / În stoc"
    db.commit()
    return {"success": True, "mesaj": mesaj}

@app.delete("/api/filament-purchases/{purchase_id}")
def sterge_achizitie_filament(purchase_id: int, db: Session = Depends(get_db)):
    achizitie = db.query(models.FilamentPurchase).filter(models.FilamentPurchase.id == purchase_id).first()
    if not achizitie:
        raise HTTPException(status_code=404, detail="Achiziție inexistentă")
    db.delete(achizitie)
    db.commit()
    return {"success": True, "mesaj": "Înregistrarea achiziției a fost ștearsă!"}

# ========================================================
# --- FINANCE SUMMARY & PROFIT ENGINE API ROUTE ---
# ========================================================
@app.get("/api/finance/summary")
def sumar_financiar(db: Session = Depends(get_db)):
    comenzi = db.query(models.Order).all()
    achizitii = db.query(models.FilamentPurchase).all()
    queue = db.query(models.PrintQueue).all()
    
    # Total încasat din comenzi marcate incasat == 1
    incasari_comenzi = sum(c.pret_total for c in comenzi if c.incasat == 1)
    de_incasat_comenzi = sum(c.pret_total for c in comenzi if c.incasat == 0)
    
    # De asemenea, includem printurile finalizate din coadă fără order_id ca vânzări istorice dacă au preț
    legacy_incasari = sum(q.pret_vanzare for q in queue if q.status == "Finalizat" and not q.order_id and (q.pret_vanzare or 0) > 0)
    
    total_incasari_reale = round(incasari_comenzi + legacy_incasari, 2)
    total_potential_valoare = round(incasari_comenzi + de_incasat_comenzi + legacy_incasari, 2)
    
    # Cheltuieli achiziții filamente
    total_cheltuieli_filamente = round(sum(a.cost_total for a in achizitii), 2)
    total_role_achizitionate = sum(a.cantitate_role for a in achizitii)
    total_kg_achizitionate = round(sum(a.cantitate_role * (a.greutate_per_rola_g / 1000.0) for a in achizitii), 2)
    
    # Cost material consumat în printuri finalizate
    cost_material_consumat = round(sum(q.cost_estimat for q in queue if q.status == "Finalizat"), 2)
    
    # Calcul profit
    # 1. Profit Net Cashflow: Încasări reale minus cheltuieli totale pe filamente cumpărate
    profit_net_cashflow = round(total_incasari_reale - total_cheltuieli_filamente, 2)
    # 2. Profit Operațional: Încasări reale minus costul materialelor consumate în printurile finalizate
    profit_operational = round(total_incasari_reale - cost_material_consumat, 2)
    
    marja_profit = round((profit_operational / total_incasari_reale * 100), 1) if total_incasari_reale > 0 else 0.0
    
    return {
        "total_incasari_reale": total_incasari_reale,
        "total_de_incasat": round(de_incasat_comenzi, 2),
        "total_potential_valoare": total_potential_valoare,
        "numar_comenzi_totale": len(comenzi),
        "numar_comenzi_incasate": sum(1 for c in comenzi if c.incasat == 1),
        "numar_comenzi_in_asteptare": sum(1 for c in comenzi if c.incasat == 0),
        "total_cheltuieli_filamente": total_cheltuieli_filamente,
        "total_role_achizitionate": total_role_achizitionate,
        "total_kg_achizitionate": total_kg_achizitionate,
        "cost_material_consumat": cost_material_consumat,
        "profit_net_cashflow": profit_net_cashflow,
        "profit_operational": profit_operational,
        "marja_profit_procent": marja_profit
    }


# --- CALENDAR & SCHEDULE ROUTES ---
RO_DAYS = ["Luni", "Marți", "Miercuri", "Joi", "Vineri", "Sâmbătă", "Duminică"]
RO_MONTHS = ["Ian", "Feb", "Mar", "Apr", "Mai", "Iun", "Iul", "Aug", "Sep", "Oct", "Noi", "Dec"]

def format_ro_time(dt: datetime, base_dt: datetime) -> str:
    time_str = dt.strftime("%H:%M")
    if dt.date() == base_dt.date():
        return f"Azi la {time_str}"
    elif dt.date() == (base_dt + timedelta(days=1)).date():
        return f"Mâine la {time_str}"
    else:
        return f"{RO_DAYS[dt.weekday()]}, {dt.day} {RO_MONTHS[dt.month-1]} {time_str}"

@app.get("/api/schedule/data")
def get_schedule_data(db: Session = Depends(get_db)):
    modele_dict = {m.id: m for m in db.query(models.Model3D).all()}
    materiale_dict = {m.id: m for m in db.query(models.Material).all()}

    all_jobs = db.query(models.PrintQueue).order_by(
        case((models.PrintQueue.status == "Finalizat", 1), else_=0),
        models.PrintQueue.ordine.asc(),
        models.PrintQueue.prioritate.asc(),
        models.PrintQueue.id.asc()
    ).all()

    active_jobs = [j for j in all_jobs if j.status != "Finalizat"]
    finished_jobs = [j for j in all_jobs if j.status == "Finalizat"]

    now = datetime.now()
    printer_state = bambu_printer.get_state()
    is_printer_running = printer_state.get("print_status") in ["RUNNING", "PREPARE"]
    current_remaining_min = int(printer_state.get("remaining_time_minutes", 0) or printer_state.get("mc_remaining_time", 0) or 0)

    # Base start cursor: if printer is printing right now, start after it finishes + 10m buffer
    if is_printer_running and current_remaining_min > 0:
        cursor_time = now + timedelta(minutes=current_remaining_min + 10)
    else:
        cursor_time = now + timedelta(minutes=2)

    timeline = []
    total_scheduled_minutes = 0
    total_weight_g = 0.0
    total_cost_ron = 0.0
    calendar_events: Dict[str, List[Any]] = {}

    for idx, item in enumerate(active_jobs, start=1):
        mod = modele_dict.get(item.model_id)
        mod_time = mod.timp_estimat_minute if (mod and mod.timp_estimat_minute and mod.timp_estimat_minute > 0) else 60
        
        greutate = 0.0
        cost_mat = 0.0
        comp_details = []
        if mod:
            try:
                comps = json.loads(mod.componente_json or "[]")
                greutate = sum(float(c.get("greutate", 0)) for c in comps)
                for c in comps:
                    mat = materiale_dict.get(c.get("material_id"))
                    pkg = mat.pret_per_kg if mat else 90.0
                    c_cost = (float(c.get("greutate", 0)) / 1000.0) * pkg
                    cost_mat += c_cost
                    comp_details.append({
                        "tip": mat.tip if mat else (c.get("tip") or "PLA"),
                        "culoare": mat.culoare if mat else (c.get("culoare") or "N/A"),
                        "culoare_hex": mat.culoare_hex if mat else (c.get("culoare_hex") or "#888888"),
                        "greutate": float(c.get("greutate", 0))
                    })
            except Exception:
                pass

        total_scheduled_minutes += mod_time
        total_weight_g += greutate
        total_cost_ron += (item.cost_estimat or cost_mat)

        job_start_time = cursor_time
        if item.data_programata and len(item.data_programata) >= 10:
            try:
                if len(item.data_programata) == 10:
                    prog_dt = datetime.strptime(item.data_programata, "%Y-%m-%d").replace(hour=9, minute=0)
                else:
                    prog_dt = datetime.strptime(item.data_programata[:16], "%Y-%m-%d %H:%M")
                if prog_dt > job_start_time:
                    job_start_time = prog_dt
            except Exception:
                pass

        job_end_time = job_start_time + timedelta(minutes=mod_time)
        cursor_time = job_end_time + timedelta(minutes=10) # 10m buffer for bed cleaning

        item.ora_start_estimat = job_start_time.strftime("%Y-%m-%d %H:%M")
        item.ora_finish_estimat = job_end_time.strftime("%Y-%m-%d %H:%M")
        if item.ordine != idx:
            item.ordine = idx

        ore = mod_time // 60
        minute = mod_time % 60
        durata_str = f"{ore}h {minute}m" if ore > 0 else f"{minute}m"

        custom_mat = materiale_dict.get(item.material_id) if item.material_id else None
        custom_mat_info = {
            "id": custom_mat.id,
            "tip": custom_mat.tip,
            "culoare": custom_mat.culoare,
            "culoare_hex": custom_mat.culoare_hex,
            "brand": custom_mat.brand
        } if custom_mat else None

        display_comps = [{
            "tip": custom_mat.tip,
            "culoare": custom_mat.culoare,
            "culoare_hex": custom_mat.culoare_hex,
            "greutate": round(greutate, 1),
            "is_custom": True
        }] if custom_mat else comp_details

        item_data = {
            "id": item.id,
            "model_id": item.model_id,
            "material_id": item.material_id,
            "culoare_dorita": item.culoare_dorita or "",
            "material_personalizat": custom_mat_info,
            "ordine": idx,
            "nume_proiect": mod.nume_proiect if mod else f"Job #{item.id}",
            "link_imagine": mod.link_imagine if mod else None,
            "link_proiect": mod.link_proiect if mod else None,
            "prioritate": item.prioritate,
            "status": item.status,
            "nume_client": item.nume_client or "",
            "timp_minute": mod_time,
            "durata_formatted": durata_str,
            "greutate_g": round(greutate, 1),
            "cost_estimat": round(item.cost_estimat or cost_mat, 2),
            "pret_vanzare": item.pret_vanzare or 0.0,
            "note": item.note or "",
            "data_programata": item.data_programata or "",
            "start_time_iso": job_start_time.isoformat(),
            "finish_time_iso": job_end_time.isoformat(),
            "start_time_formatted": format_ro_time(job_start_time, now),
            "finish_time_formatted": format_ro_time(job_end_time, now),
            "date_key": job_start_time.strftime("%Y-%m-%d"),
            "componente": display_comps,
            "este_primul": (idx == 1)
        }
        timeline.append(item_data)

        date_key = job_start_time.strftime("%Y-%m-%d")
        if date_key not in calendar_events:
            calendar_events[date_key] = []
        calendar_events[date_key].append(item_data)

    db.commit()

    total_ore = total_scheduled_minutes // 60
    total_min = total_scheduled_minutes % 60
    total_durata_str = f"{total_ore}h {total_min}m" if total_ore > 0 else f"{total_min}m"
    finish_all_str = format_ro_time(cursor_time - timedelta(minutes=10), now) if active_jobs else "Niciun print în așteptare"

    istoric = []
    for f in finished_jobs[:15]:
        m = modele_dict.get(f.model_id)
        istoric.append({
            "id": f.id,
            "nume_proiect": m.nume_proiect if m else "Print",
            "link_imagine": m.link_imagine if m else None,
            "data_finalizare": f.data_finalizare or "",
            "nume_client": f.nume_client or "",
            "cost_estimat": f.cost_estimat or 0.0
        })

    return {
        "summary": {
            "total_jobs": len(active_jobs),
            "total_minutes": total_scheduled_minutes,
            "total_durata_str": total_durata_str,
            "finalizare_estimata": finish_all_str,
            "total_greutate_g": round(total_weight_g, 1),
            "total_cost_ron": round(total_cost_ron, 2),
            "is_printing_now": is_printer_running
        },
        "timeline": timeline,
        "calendar_events": calendar_events,
        "istoric": istoric
    }

@app.post("/api/schedule/reorder")
def reorder_queue(req: QueueReorderRequest, db: Session = Depends(get_db)):
    for idx, item_id in enumerate(req.item_ids, start=1):
        job = db.query(models.PrintQueue).filter(models.PrintQueue.id == item_id).first()
        if job:
            job.ordine = idx
    db.commit()
    return {"success": True, "mesaj": "Ordinea din orar a fost actualizată!"}

@app.put("/api/schedule/{item_id}/move")
def move_queue_item(item_id: int, req: QueueMoveRequest, db: Session = Depends(get_db)):
    jobs = db.query(models.PrintQueue).filter(models.PrintQueue.status != "Finalizat").order_by(models.PrintQueue.ordine.asc(), models.PrintQueue.prioritate.asc(), models.PrintQueue.id.asc()).all()
    ids = [j.id for j in jobs]
    if item_id not in ids:
        raise HTTPException(status_code=404, detail="Printul nu a fost găsit în orar.")
    
    idx = ids.index(item_id)
    direction = req.direction.lower().strip()
    if direction == "up" and idx > 0:
        ids[idx], ids[idx-1] = ids[idx-1], ids[idx]
    elif direction == "down" and idx < len(ids) - 1:
        ids[idx], ids[idx+1] = ids[idx+1], ids[idx]
    elif direction == "top":
        ids.remove(item_id)
        ids.insert(0, item_id)
    elif direction == "bottom":
        ids.remove(item_id)
        ids.append(item_id)
        
    for i, j_id in enumerate(ids, start=1):
        j = next(item for item in jobs if item.id == j_id)
        j.ordine = i
    db.commit()
    return {"success": True, "mesaj": "Poziția printului a fost actualizată!"}

@app.put("/api/schedule/{item_id}/date")
def schedule_queue_date(item_id: int, req: QueueScheduleDateRequest, db: Session = Depends(get_db)):
    job = db.query(models.PrintQueue).filter(models.PrintQueue.id == item_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Printul nu a fost găsit.")
    job.data_programata = req.data_programata.strip()
    db.commit()
    return {"success": True, "mesaj": f"Data programată a fost setată la {job.data_programata}!" if job.data_programata else "Programarea a fost resetată la succesiune automată!"}

# --- COST & PROFIT CALCULATOR ROUTES ---
@app.post("/api/calculator/excel-cost")
def calculeaza_cost_excel(req: ExcelCostRequest, db: Session = Depends(get_db)):
    gr = float(req.gramaj_g or 0.0)
    timp_m = float(req.timp_minute or 0.0)
    pkg = float(req.pret_kg_filament or 100.0)

    if req.model_id and req.model_id > 0:
        mod = db.query(models.Model3D).filter(models.Model3D.id == req.model_id).first()
        if mod:
            if timp_m <= 0:
                timp_m = float(mod.timp_estimat_minute or 0)
            try:
                comps = json.loads(mod.componente_json or "[]")
                tot_gr = sum(float(c.get("greutate", 0)) for c in comps)
                if gr <= 0 and tot_gr > 0:
                    gr = tot_gr
            except Exception:
                pass

    return calcul_cost_productie_excel(
        gramaj_g=gr,
        timp_minute=timp_m,
        timp_ore=req.timp_ore,
        pret_kg_filament=pkg,
        cost_curent_ora=req.cost_curent_ora or 1.36,
        diverse=req.diverse or 0.0,
        extra=req.extra if req.extra is not None else 2.96,
        pret_vanzare_manual=req.pret_vanzare_manual
    )

@app.post("/api/calculator/cost")
def calculeaza_cost(req: CostCalcRequest):
    # Filament cost
    cost_filament = (req.greutate_g / 1000.0) * req.pret_kg_filament
    
    # Electricity cost: (minutes/60) * (Watts/1000) * RON/kWh
    ore_print = req.timp_minute / 60.0
    kwh_consumat = ore_print * (req.putere_w / 1000.0)
    cost_electricitate = kwh_consumat * req.cost_kwh

    # Printer wear and tear
    cost_uzura = ore_print * req.uzura_ron_ora

    # Subtotal direct production
    cost_productie = cost_filament + cost_electricitate + cost_uzura

    # Failure risk buffer (10%)
    cost_risc = cost_productie * 0.10

    # Total cost per piece
    cost_total = cost_productie + cost_risc

    # Suggested selling price with desired profit margin
    marja_factor = (req.marja_profit or 50.0) / 100.0
    pret_sugerat = cost_total * (1.0 + marja_factor)
    profit_net = pret_sugerat - cost_total

    # Calcul paralel conform Formulei Excel CRACIUN
    excel_calc = calcul_cost_productie_excel(
        gramaj_g=req.greutate_g,
        timp_minute=req.timp_minute,
        pret_kg_filament=req.pret_kg_filament or 100.0
    )

    return {
        "greutate_g": req.greutate_g,
        "timp_ore": round(ore_print, 1),
        "cost_filament": round(cost_filament, 2),
        "cost_electricitate": round(cost_electricitate, 2),
        "cost_uzura": round(cost_uzura, 2),
        "cost_risc": round(cost_risc, 2),
        "cost_total_productie": round(cost_total, 2),
        "pret_minim_recomandat": round(cost_total * 1.25, 2),
        "pret_vanzare_sugerat": round(pret_sugerat, 2),
        "profit_net_estimat": round(profit_net, 2),
        # Excel CRACIUN fields
        "excel_cost_productie": excel_calc["cost_productie"],
        "excel_cost_curent": excel_calc["cost_curent"],
        "excel_profit": excel_calc["profit_excel"],
        "excel_pret_fara_tva": excel_calc["total_fara_tva"],
        "excel_pret_cu_tva": excel_calc["total_cu_tva"],
        "excel_pret_uniformizat": excel_calc["pret_uniformizat"]
    }

# --- STATS ROUTE ---
@app.get("/api/stats")
def statistici_generale(db: Session = Depends(get_db)):
    materiale = db.query(models.Material).all()
    modele = db.query(models.Model3D).all()
    queue = db.query(models.PrintQueue).all()

    total_role = len(materiale)
    total_kg_stoc = sum(m.greutate_ramasa for m in materiale) / 1000.0
    valoare_stoc = sum((m.greutate_ramasa / 1000.0) * (m.pret_per_kg or 90.0) for m in materiale)
    
    in_asteptare = sum(1 for q in queue if q.status == "În așteptare")
    finalizate = sum(1 for q in queue if q.status == "Finalizat")
    
    # Calculate low stock count (< 150g)
    role_pe_terminate = sum(1 for m in materiale if m.greutate_ramasa < 150.0)

    return {
        "total_role": total_role,
        "total_kg_stoc": round(total_kg_stoc, 2),
        "valoare_stoc_ron": round(valoare_stoc, 2),
        "total_modele": len(modele),
        "queue_in_asteptare": in_asteptare,
        "queue_finalizate": finalizate,
        "role_pe_terminate": role_pe_terminate,
        "imprimanta_online": bambu_printer.is_connected
    }

# --- VOLUMETRIC CHANNEL LETTERS MODULE ROUTES ---
def _format_volumetric_project(proj: models.VolumetricProject, db: Session):
    letters = db.query(models.VolumetricLetter).filter(
        models.VolumetricLetter.project_id == proj.id
    ).order_by(models.VolumetricLetter.index_pozitie.asc()).all()

    total = len(letters)
    completed = sum(1 for l in letters if l.status == "completed")
    printing = sum(1 for l in letters if l.status == "printing")
    failed = sum(1 for l in letters if l.status == "failed")
    pending = sum(1 for l in letters if l.status == "pending")
    procent = round((completed / total * 100), 1) if total > 0 else 0.0

    return {
        "id": proj.id,
        "nume_client": proj.nume_client,
        "text_comanda": proj.text_comanda,
        "font_family": proj.font_family,
        "inaltime_cm": proj.inaltime_cm,
        "adancime_mm": proj.adancime_mm,
        "tip_constructie": proj.tip_constructie,
        "material_id": proj.material_id,
        "culoare_dorita": proj.culoare_dorita,
        "pret_total": proj.pret_total,
        "status": proj.status,
        "note": proj.note or "",
        "data_adaugare": proj.data_adaugare or "",
        "stats": {
            "total": total,
            "completed": completed,
            "printing": printing,
            "failed": failed,
            "pending": pending,
            "procent": procent
        },
        "litere": [
            {
                "id": l.id,
                "project_id": l.project_id,
                "caracter": l.caracter,
                "index_pozitie": l.index_pozitie,
                "status": l.status,
                "inaltime_cm": l.inaltime_cm or proj.inaltime_cm or 15.0,
                "timp_estimat_min": l.timp_estimat_min,
                "greutate_est_g": l.greutate_est_g,
                "note": l.note or "",
                "data_actualizare": l.data_actualizare or ""
            }
            for l in letters
        ]
    }

@app.get("/api/volumetric/projects")
def get_volumetric_projects(db: Session = Depends(get_db)):
    projects = db.query(models.VolumetricProject).order_by(models.VolumetricProject.id.desc()).all()
    return [_format_volumetric_project(p, db) for p in projects]

@app.post("/api/volumetric/projects")
def create_volumetric_project(data: VolumetricProjectCreate, db: Session = Depends(get_db)):
    text_curat = data.text_comanda.strip()
    if not text_curat:
        raise HTTPException(status_code=400, detail="Textul comenzii nu poate fi gol.")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    base_height = data.inaltime_cm or 15.0
    proj = models.VolumetricProject(
        nume_client=data.nume_client.strip() or "Client Nenumit",
        text_comanda=text_curat,
        font_family=data.font_family or "Standard Bold",
        inaltime_cm=base_height,
        adancime_mm=data.adancime_mm or 30.0,
        tip_constructie=data.tip_constructie or "Fațetă + Casetă",
        material_id=data.material_id,
        culoare_dorita=data.culoare_dorita or "Alb / Negru",
        pret_total=data.pret_total or 0.0,
        status="În lucru",
        note=data.note or "",
        data_adaugare=now_str
    )
    db.add(proj)
    db.commit()
    db.refresh(proj)

    # Generăm fiecare literă individuală din text
    pos_idx = 0
    for char in text_curat:
        if char == " ":
            db.add(models.VolumetricLetter(
                project_id=proj.id,
                caracter=" ",
                index_pozitie=pos_idx,
                status="space",
                inaltime_cm=0,
                timp_estimat_min=0,
                greutate_est_g=0,
                note="Spațiu cuvânt",
                data_actualizare=now_str
            ))
        else:
            db.add(models.VolumetricLetter(
                project_id=proj.id,
                caracter=char,
                index_pozitie=pos_idx,
                status="pending",
                inaltime_cm=base_height,
                timp_estimat_min=data.timp_per_litera_min or 60,
                greutate_est_g=data.greutate_per_litera_g or 35.0,
                note="",
                data_actualizare=now_str
            ))
        pos_idx += 1

    db.commit()
    return {
        "success": True,
        "mesaj": f"Proiectul '{proj.nume_client}' cu textul '{proj.text_comanda}' a fost creat cu succes!",
        "project": _format_volumetric_project(proj, db)
    }

@app.get("/api/volumetric/projects/{proj_id}")
def get_single_volumetric_project(proj_id: int, db: Session = Depends(get_db)):
    proj = db.query(models.VolumetricProject).filter(models.VolumetricProject.id == proj_id).first()
    if not proj:
        raise HTTPException(status_code=404, detail="Proiect inexistent")
    return _format_volumetric_project(proj, db)

@app.patch("/api/volumetric/letters/{letter_id}")
def update_volumetric_letter(letter_id: int, update: VolumetricLetterUpdate, db: Session = Depends(get_db)):
    letter = db.query(models.VolumetricLetter).filter(models.VolumetricLetter.id == letter_id).first()
    if not letter:
        raise HTTPException(status_code=404, detail="Litera nu a fost găsită")

    if update.status is not None:
        valid_statuses = ["pending", "printing", "completed", "failed", "space"]
        if update.status not in valid_statuses:
            raise HTTPException(status_code=400, detail="Status invalid")
        letter.status = update.status

    if update.inaltime_cm is not None:
        letter.inaltime_cm = update.inaltime_cm
    if update.note is not None:
        letter.note = update.note
    if update.timp_estimat_min is not None:
        letter.timp_estimat_min = update.timp_estimat_min
    if update.greutate_est_g is not None:
        letter.greutate_est_g = update.greutate_est_g

    letter.data_actualizare = datetime.now().strftime("%Y-%m-%d %H:%M")
    db.commit()

    proj = db.query(models.VolumetricProject).filter(models.VolumetricProject.id == letter.project_id).first()
    return {
        "success": True,
        "letter": {
            "id": letter.id,
            "project_id": letter.project_id,
            "caracter": letter.caracter,
            "index_pozitie": letter.index_pozitie,
            "status": letter.status,
            "inaltime_cm": letter.inaltime_cm,
            "note": letter.note,
            "data_actualizare": letter.data_actualizare
        },
        "project": _format_volumetric_project(proj, db) if proj else None
    }

@app.post("/api/volumetric/letters/{letter_id}/set-printing")
def set_letter_printing(letter_id: int, db: Session = Depends(get_db)):
    target_letter = db.query(models.VolumetricLetter).filter(models.VolumetricLetter.id == letter_id).first()
    if not target_letter:
        raise HTTPException(status_code=404, detail="Litera nu a fost găsită")

    # Setăm litera ca fiind în imprimare acum
    target_letter.status = "printing"
    target_letter.data_actualizare = datetime.now().strftime("%Y-%m-%d %H:%M")
    db.commit()

    proj = db.query(models.VolumetricProject).filter(models.VolumetricProject.id == target_letter.project_id).first()
    return {
        "success": True,
        "mesaj": f"Litera '{target_letter.caracter}' a fost marcată ca fiind în imprimare!",
        "project": _format_volumetric_project(proj, db)
    }

@app.post("/api/volumetric/projects/{proj_id}/batch")
def batch_update_letters(proj_id: int, req: VolumetricBatchStatus, db: Session = Depends(get_db)):
    proj = db.query(models.VolumetricProject).filter(models.VolumetricProject.id == proj_id).first()
    if not proj:
        raise HTTPException(status_code=404, detail="Proiect inexistent")

    letters = db.query(models.VolumetricLetter).filter(
        models.VolumetricLetter.project_id == proj_id,
        models.VolumetricLetter.status != "space"
    ).all()

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    if req.action == "complete_all":
        for l in letters:
            l.status = "completed"
            l.data_actualizare = now_str
        proj.status = "Finalizat"
    elif req.action == "reset_failed":
        for l in letters:
            if l.status == "failed":
                l.status = "pending"
                l.data_actualizare = now_str
    elif req.action == "reset_all":
        for l in letters:
            l.status = "pending"
            l.data_actualizare = now_str
        proj.status = "În lucru"
    elif req.action == "set_height_all" and req.value is not None:
        for l in letters:
            l.inaltime_cm = float(req.value)
            l.data_actualizare = now_str
        proj.inaltime_cm = float(req.value)

    db.commit()
    return {
        "success": True,
        "mesaj": f"Acțiune '{req.action}' executată cu succes!",
        "project": _format_volumetric_project(proj, db)
    }

@app.delete("/api/volumetric/projects/{proj_id}")
def delete_volumetric_project(proj_id: int, db: Session = Depends(get_db)):
    proj = db.query(models.VolumetricProject).filter(models.VolumetricProject.id == proj_id).first()
    if not proj:
        raise HTTPException(status_code=404, detail="Proiect inexistent")

    # Ștergem literele asociate
    db.query(models.VolumetricLetter).filter(models.VolumetricLetter.project_id == proj_id).delete()
    db.delete(proj)
    db.commit()
    return {"success": True, "mesaj": "Proiectul de litere volumetrice a fost șters cu succes!"}
