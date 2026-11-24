from sqlalchemy import Column, Integer, String, Float, ForeignKey
from database import Base

class Printer(Base):
    __tablename__ = 'printers'
    id = Column(Integer, primary_key=True, index=True)
    nume = Column(String, default="Bambu Lab A1")
    ip_address = Column(String, default="")
    access_code = Column(String, default="")
    serial_number = Column(String, default="")
    status_curent = Column(String, default="Neconfigurată")
    port = Column(Integer, default=8883)
    model_type = Column(String, default="Bambu Lab A1")
    activ = Column(Integer, default=1)

class Material(Base):
    __tablename__ = 'materials'
    id = Column(Integer, primary_key=True, index=True)
    tip = Column(String)
    culoare = Column(String)
    culoare_hex = Column(String, default="#00d26a")
    greutate_ramasa = Column(Float, default=1000.0)
    pret_per_kg = Column(Float, default=90.0)
    brand = Column(String, default="Bambu Lab")
    temperatura_duza = Column(Integer, default=220)
    temperatura_pat = Column(Integer, default=60)
    locatie_slot = Column(String, default="")

class Model3D(Base):
    __tablename__ = 'models'
    id = Column(Integer, primary_key=True, index=True)
    nume_proiect = Column(String)
    timp_estimat_minute = Column(Integer, default=0)
    link_imagine = Column(String, nullable=True)
    link_proiect = Column(String, nullable=True)
    componente_json = Column(String, default="[]")
    data_adaugare = Column(String, default="")

class PrintQueue(Base):
    __tablename__ = 'print_queue'
    id = Column(Integer, primary_key=True, index=True)
    model_id = Column(Integer, ForeignKey('models.id'))
    order_id = Column(Integer, ForeignKey('orders.id'), nullable=True)
    order_item_id = Column(Integer, ForeignKey('order_items.id'), nullable=True)
    prioritate = Column(Integer, default=2) # 1=Urgent, 2=Normal, 3=Stoc
    status = Column(String, default="În așteptare") # În așteptare, În imprimare, Finalizat, Anulat
    nume_client = Column(String, default="")
    cost_estimat = Column(Float, default=0.0)
    pret_vanzare = Column(Float, default=0.0)
    note = Column(String, default="")
    ordine = Column(Integer, default=0)
    data_programata = Column(String, default="")
    ora_start_estimat = Column(String, default="")
    ora_finish_estimat = Column(String, default="")
    material_id = Column(Integer, ForeignKey('materials.id'), nullable=True)
    culoare_dorita = Column(String, default="")
    data_adaugare = Column(String, default="")
    data_finalizare = Column(String, default="")

class Order(Base):
    __tablename__ = 'orders'
    id = Column(Integer, primary_key=True, index=True)
    numar_comanda = Column(String, default="") # ex. CMD-101
    nume_client = Column(String, default="")
    telefon = Column(String, default="")
    status = Column(String, default="În așteptare") # "În așteptare", "În lucru", "Finalizată", "Livrată", "Anulată"
    pret_total = Column(Float, default=0.0)
    cost_total_estimat = Column(Float, default=0.0)
    incasat = Column(Integer, default=0) # 0 = Neîncasat, 1 = Încasat
    data_incasare = Column(String, default="")
    metoda_plata = Column(String, default="Cash") # "Cash", "Card / Transfer", "Ramburs"
    note = Column(String, default="")
    data_adaugare = Column(String, default="")
    data_finalizare = Column(String, default="")

class OrderItem(Base):
    __tablename__ = 'order_items'
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey('orders.id'), index=True)
    model_id = Column(Integer, ForeignKey('models.id'), nullable=True)
    nume_produs = Column(String, default="")
    cantitate = Column(Integer, default=1)
    material_id = Column(Integer, ForeignKey('materials.id'), nullable=True)
    culoare_dorita = Column(String, default="")
    pret_unitar = Column(Float, default=0.0)
    cost_unitar = Column(Float, default=0.0)
    status = Column(String, default="În așteptare") # "În așteptare", "În imprimare", "Finalizat"
    note = Column(String, default="")

class FilamentPurchase(Base):
    __tablename__ = 'filament_purchases'
    id = Column(Integer, primary_key=True, index=True)
    furnizor = Column(String, default="Bambu Lab EU")
    brand = Column(String, default="Bambu Lab")
    tip = Column(String, default="PLA Basic")
    culoare = Column(String, default="")
    culoare_hex = Column(String, default="#00e575")
    cantitate_role = Column(Integer, default=1)
    greutate_per_rola_g = Column(Float, default=1000.0)
    pret_per_rola = Column(Float, default=90.0)
    cost_transport = Column(Float, default=0.0)
    cost_total = Column(Float, default=90.0)
    data_achizitie = Column(String, default="")
    status = Column(String, default="Livrat / În stoc") # "Comandat", "Livrat / În stoc", "Anulat"
    adaugat_in_stoc = Column(Integer, default=1) # 1 = Da, 0 = Nu
    note = Column(String, default="")

class VolumetricProject(Base):
    __tablename__ = 'volumetric_projects'
    id = Column(Integer, primary_key=True, index=True)
    nume_client = Column(String, default="")
    text_comanda = Column(String, default="")
    font_family = Column(String, default="Standard Bold")
    inaltime_cm = Column(Float, default=15.0)
    adancime_mm = Column(Float, default=30.0)
    tip_constructie = Column(String, default="Fațetă + Casetă")
    material_id = Column(Integer, nullable=True)
    culoare_dorita = Column(String, default="Alb / Negru")
    pret_total = Column(Float, default=0.0)
    status = Column(String, default="În lucru") # "În lucru", "Finalizat", "Livrat"
    note = Column(String, default="")
    data_adaugare = Column(String, default="")

class VolumetricLetter(Base):
    __tablename__ = 'volumetric_letters'
    id = Column(Integer, primary_key=True, index=True)
    project_id = Column(Integer, ForeignKey('volumetric_projects.id'), index=True)
    caracter = Column(String(10), default="")
    index_pozitie = Column(Integer, default=0)
    status = Column(String, default="pending") # "pending", "printing", "completed", "failed"
    inaltime_cm = Column(Float, default=15.0)
    timp_estimat_min = Column(Integer, default=60)
    greutate_est_g = Column(Float, default=35.0)
    note = Column(String, default="")
    data_actualizare = Column(String, default="")
