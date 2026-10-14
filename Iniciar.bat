@echo off
cd /d "%~dp0"
if not exist .venv (
  echo Creando entorno virtual...
  py -3.12 -m venv .venv || py -3.11 -m venv .venv || python -m venv .venv
  .venv\Scripts\python -m pip install -r requirements.txt
)
.venv\Scripts\python run.py
