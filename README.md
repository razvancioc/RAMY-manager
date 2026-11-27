# Ramy Manager - 3D Print Hub (Open Source Edition)

A self-hosted web application for monitoring, order management, production cost calculation, and local synchronization with Bambu Lab 3D printers (A1 / A1 mini / P1 / X1 / AMS).

> **Note:** The web interface is currently in Romanian.

---

## What Is Included in This Edition

This public release is completely decoupled from any personal printer credentials or workshop database:
- **No personal data:** Contains no real customer orders, phone numbers, pricing history, or private databases.
- **Universal setup:** Connect your own Bambu Lab printer directly from the web UI (**Setari** / Settings tab).
- **Auto-initialization:** On first startup, the app automatically creates a clean local SQLite database (`bambu_manager.db`) pre-populated with 4 sample filament spools (Bambu Lab PLA Basic Black, White, Red, Yellow).

---

## Features

- **Live Telemetry:** Local MQTT connection to your Bambu Lab printer (nozzle/bed temperatures, print progress, speed level, fan speed, layer progress, and AMS slots).
- **Live Camera Stream:** Built-in MJPEG video stream from the printer's local camera (port 6000).
- **Customer Order Management:**
  - Create multi-product orders per client with custom quantities, colors, and materials.
  - Automatically dispatch pieces into the print queue.
  - Track payment status (Cash, Card / Transfer, Cash on Delivery) and order lifecycle.
- **Smart Batching & Plate Grouping:**
  - Automatically detects identical products repeating across unfinished orders.
  - Supports batch completion (FIFO allocation across orders) and unified batch print jobs.
- **Cost & Profit Calculator:**
  - Calculates production costs based on filament weight, print time, electricity, and overhead.
  - Suggests selling prices (without VAT, with VAT, and rounded commercial pricing).
- **Filament Inventory & AMS Sync:**
  - Manage filament spools by brand, material type, color, and remaining weight.
  - Sync AMS slots with local filament stock and auto-deduct usage on completed prints.
- **3D Model Library & MakerWorld Import:**
  - Store local 3D model profiles with components, weights, and estimated print times.
  - Automatically fetch project details and thumbnails from MakerWorld URLs.

---

## Quick Start

### 1. Clone the repository
```bash
git clone https://github.com/razvancioc/RAMY-manager.git
cd RAMY-manager
```

### 2. Create and activate a Python virtual environment (Python 3.10+ recommended)
```bash
python3 -m venv venv
source venv/bin/activate
```
*(On Windows: `venv\Scripts\activate`)*

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Start the server
On macOS / Linux:
```bash
./start_manager.sh
```
Or manually with Uvicorn:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 5. Connect your printer
1. Open your browser at `http://localhost:8000`.
2. Go to the **Setari** (Settings) tab.
3. Enter your printer details:
   - **Printer IP Address:** Found on the printer screen under *Settings > Network / Wi-Fi*.
   - **Access Code:** The 8-character LAN access code shown on the printer screen.
   - **Serial Number:** Your printer's serial number (recommended for reliable MQTT topic subscription).
4. Click save to establish the local MQTT and camera connections.

---

## Project Structure

```text
├── main.py              # FastAPI server, API routes, cost/batch logic, WebSocket bridge
├── models.py            # SQLAlchemy database models (Printer, Material, Model3D, PrintQueue, Order, etc.)
├── database.py          # Local SQLite database connection
├── bambu_client.py      # Local MQTT client for Bambu Lab telemetry
├── camera_client.py     # Local TLS camera stream client
├── importer.py          # MakerWorld URL project importer
├── start_manager.sh     # Server startup script
├── requirements.txt     # Python dependencies
├── .gitignore           # Excludes local DBs, virtualenvs, and caches
├── templates/
│   └── index.html       # Single-page web UI (HTML, Bambu dark theme CSS, JavaScript)
└── static/              # Static visual assets
```

---

## License

Available for personal and commercial 3D printing workshop use.
