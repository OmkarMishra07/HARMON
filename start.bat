@echo off
echo ==========================================
echo   Velox Music Server
echo ==========================================
echo.

:: Check if pip packages are installed
python -c "import flask, flask_cors, requests, boto3, dotenv" 2>nul
if errorlevel 1 (
    echo Installing dependencies...
    pip install -r requirements.txt
    echo.
)

echo Starting server on http://localhost:5000
echo Open your browser at http://localhost:5000
echo Press Ctrl+C to stop.
echo.
start "" "http://localhost:5000"
python server.py
