from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import FileResponse
from pydantic import BaseModel
from tasks import fetch_rcv_task
from celery.result import AsyncResult
import os
import uuid

app = FastAPI(title="SII RCV Worker")

API_KEY = os.getenv("SII_API_KEY", "tu_clave_secreta")

class RCVRequest(BaseModel):
    periodo: str  # "202501"
    tipo: str     # "compra" o "venta"
    rut: str
    password: str

@app.post("/rcv")
async def iniciar_rcv(request: RCVRequest, x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(401, "API Key inválida")
    
    task = fetch_rcv_task.delay(
        periodo=request.periodo,
        tipo_rcv=request.tipo,
        rut=request.rut,
        password=request.password
    )
    
    return {"task_id": task.id, "status": "iniciado"}

@app.get("/rcv/status/{task_id}")
async def estado(task_id: str, x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(401, "API Key inválida")
    
    result = AsyncResult(task_id)
    if result.ready():
        return {"status": "completado", "data": result.get()}
    return {"status": result.state}

@app.get("/rcv/download/{task_id}")
async def download(task_id: str, x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(401, "API Key inválida")
    
    file_path = f"/downloads/rcv_{task_id}.csv"
    if not os.path.exists(file_path):
        raise HTTPException(404, "Archivo no listo")
    
    return FileResponse(file_path, filename=f"rcv_{task_id}.csv")