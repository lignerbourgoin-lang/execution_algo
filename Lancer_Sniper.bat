@echo off
title Billetterie Sniper - Lancement
cd /d "%~dp0"

echo =======================================================
echo    BILLETTERIE SNIPER & TOMBOLA 20 IPs - LANCEMENT
echo =======================================================
echo.

REM Detection de l'environnement virtuel actif
if exist "..\154\.venv\Scripts\python.exe" (
    set "PYTHON_EXE=..\154\.venv\Scripts\python.exe"
) else if exist ".venv\Scripts\python.exe" (
    set "PYTHON_EXE=.venv\Scripts\python.exe"
) else (
    set "PYTHON_EXE=python"
)

echo [*] Utilisation de : %PYTHON_EXE%
echo [*] Lancement de l'interface graphique...
echo.

"%PYTHON_EXE%" gui\app.py

if %ERRORLEVEL% neq 0 (
    echo.
    echo [!] Fermeture avec code d'erreur %ERRORLEVEL%.
    pause
)
