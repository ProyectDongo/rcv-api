from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
import os
import subprocess
import uuid
import logging

app = FastAPI(title="SII RCV API - PythonAnywhere")

# Configuración de logs
logging.basicConfig(
    filename=f"/home/{os.getenv('USER')}/logs/sii_api.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

API_KEY = os.getenv("SII_API_KEY", "sii2025facilmasterkey")

class RCVRequest(BaseModel):
    periodo: str    # ej: "202501"
    tipo: str       # "compra" o "venta"
    rut: str
    password: str

@app.get("/")
async def root():
    return {"message": "SII RCV API viva - PythonAnywhere GRATIS"}

@app.post("/api/rcv")
async def rcv(request: RCVRequest, x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        logging.warning(f"API Key inválida desde {request.client.host}")
        raise HTTPException(401, "API Key inválida")
    
    task_id = str(uuid.uuid4())
    log_file = f"/home/{os.getenv('USER')}/logs/rcv_{task_id}.log"
    
    # Lanzamos la tarea en segundo plano
    cmd = f"python /home/{os.getenv('USER')}/sii-rcv-api/run_worker.py {task_id} {request.periodo} {request.tipo} \"{request.rut}\" \"{request.password}\" > {log_file} 2>&1"
    subprocess.Popen(["bash", "-c", cmd])
    
    logging.info(f"Nueva solicitud RCV - Task ID: {task_id} - {request.tipo} {request.periodo}")
    
    return {
        "task_id": task_id,
        "status": "iniciado",
        "message": "RCV en proceso (3-5 minutos)",
        "log_url": f"https://{os.getenv('USER')}.pythonanywhere.com/logs/rcv_{task_id}.log",
        "docs": f"https://{os.getenv('USER')}.pythonanywhere.com/docs"
    }