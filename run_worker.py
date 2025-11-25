import sys
import os
from tasks import fetch_rcv_task

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Uso: python run_worker.py <task_id> <periodo> <tipo> <rut> <password>")
        sys.exit(1)
    
    task_id = sys.argv[1]
    periodo = sys.argv[2]
    tipo_rcv = sys.argv[3]
    rut = sys.argv[4]
    password = sys.argv[5]
    
    fetch_rcv_task(task_id, periodo, tipo_rcv, rut, password)