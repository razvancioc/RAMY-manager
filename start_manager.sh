#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate
echo "=========================================="
echo "   [RAMY MANAGER] - 3D PRINT HUB          "
echo "=========================================="
echo "Serverul pornește la: http://localhost:8000"
echo "Apasă Ctrl+C pentru a opri."
echo "=========================================="
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
