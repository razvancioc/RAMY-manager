# Ramy Manager - 3D Print Hub (Open Source Edition)

Platforma completa de monitorizare, gestiune a comenzilor de printare 3D, calcul de costuri si sincronizare cu imprimantele Bambu Lab (A1 / A1 mini / P1 / X1 / AMS).

---

## Ce include aceasta versiune

Aceasta distributie publica este complet curata si decuplata de orice dispozitiv sau baza de date privata:
- **Zero date personale**: Nu contine comenzi, preturi confidentiale, numere de telefon sau baze de date interne.
- **Configurare universala**: Oricine isi poate conecta propria imprimanta Bambu Lab direct din interfata web (tabul Setari) sau prin variabile de mediu.
- **Auto-initializare**: La prima pornire, aplicatia creeaza automat o baza de date locala SQLite curata, cu preset-uri utile de filamente (PLA Basic Bambu Lab negru, alb, rosu, galben).

---

## Functionalitati

- **Telemetrie Live**: Conexiune MQTT locala si sigura cu imprimanta (temperaturi duza/pat, procente, viteza, ventilatoare, stare AMS).
- **Flux Video in Timp Real**: Streaming video local de la camera imprimantei pe portul dedicat.
- **Gestiune Comenzi Clienti**:
  - Creare comenzi compuse (mai multe produse/modele per client).
  - Trimitere automata a pieselor in coada de asteptare.
  - Urmarire plati si incasari (cash, card/transfer, ramburs).
- **Lotizare Inteligenta & Plachete Comune (Batch Printing)**:
  - Detecteaza automat produsele identice care se repeta in comenzi active diferite.
  - Permite printarea mai multor piese pe acelasi pat si marcarea automata FIFO a comenzilor finalizate.
- **Calculator Costuri & Profit**:
  - Calculeaza costul de productie (filament consumat, energie electrica, uzura).
  - Recomanda preturi de vanzare (fara TVA, cu TVA, uniformizate comercial).
- **Stoc Filamente & Sincronizare AMS**:
  - Gestiune role pe branduri, culori si greutate ramasa.
  - Sincronizare automata a bobinelor din AMS cu stocul local.
- **Librarie Modele 3D & Import MakerWorld**:
  - Catalog local de modele cu piese, gramaje si timpi estimati.
  - Preluare automata specificatii direct din link MakerWorld.

---

## Ghid Rapid de Instalare

### 1. Clonare repository
```bash
git clone https://github.com/UTILIZATOR/ramy-manager.git
cd ramy-manager
```

### 2. Creare si activare mediu virtual Python (recomandat Python 3.10+)
```bash
python3 -m venv venv
source venv/bin/activate
```
*(Pe Windows: `venv\Scripts\activate`)*

### 3. Instalare dependinte
```bash
pip install -r requirements.txt
```

### 4. Pornire server
Pe macOS / Linux:
```bash
./start_manager.sh
```
Sau manual:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 5. Conectare la imprimanta proprie
1. Deschideti browserul la adresa: `http://localhost:8000`
2. Navigati la tab-ul **Setari**.
3. Introduceti:
   - **Adresa IP a imprimantei**: o gasiti pe ecranul imprimantei la *Settings > Network / Wi-Fi*.
   - **Access Code**: codul de 8 caractere afisat pe ecranul imprimantei in meniul de retea.
   - **Serial Number**: numarul de serie al imprimantei (optional).
4. Apasati **Salveaza Configurarea**. Telemetria si camera se vor conecta instantaneu.

---

## Structura Proiectului

```text
├── main.py              # Server FastAPI, rute API, logica de calcul si WebSocket
├── models.py            # Modele tabele SQLite (Comenzi, Produse, Coada, Filamente)
├── database.py          # Conexiune baza de date locala SQLite
├── bambu_client.py      # Client MQTT local pentru telemetrie Bambu Lab
├── camera_client.py     # Client stream video camera locala
├── importer.py          # Import automat modele 3D din MakerWorld
├── start_manager.sh     # Script pornire server
├── requirements.txt     # Dependinte Python
├── .gitignore           # Exclude baze de date locale, medii virtuale si cache-uri
├── .env.example         # Exemplu configurare mediu
├── templates/
│   └── index.html       # Interfata web completa (HTML, CSS Bambu dark, JS)
└── static/              # Resurse grafice
```

---

## Licenta
Distribuit sub licenta MIT. Liber pentru utilizare personala si comerciala in ateliere de printare 3D.
