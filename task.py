import os
import csv
import time
import random
import logging
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from twocaptcha import TwoCaptcha

# Configuración de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Clave de 2Captcha (configurada en variables de entorno)
TWOCAPTCHA_API_KEY = os.getenv("TWOCAPTCHA_API_KEY")

def fetch_rcv_task(task_id, periodo, tipo_rcv, rut, password):
    """
    Descarga RCV del SII usando Playwright
    task_id: ID único para logs
    periodo: formato AAAAMM (ej: 202501)
    tipo_rcv: "compra" o "venta"
    rut: RUT con DV y guión (ej: 76123456-7)
    password: Clave SII
    """
    log_file = f"/home/{os.getenv('USER')}/logs/rcv_{task_id}.log"
    download_dir = f"/home/{os.getenv('USER')}/downloads"
    os.makedirs(download_dir, exist_ok=True)
    
    logger.info(f"INICIO RCV - Task: {task_id} | {tipo_rcv.upper()} | {periodo} | RUT: {rut}")
    
    file_path = None
    try:
        with sync_playwright() as p:
            # Lanzar navegador
            browser = p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
            )
            context = browser.new_context(
                accept_downloads=True,
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()
            
            # === LOGIN SII ===
            login_url = 'https://zeus.sii.cl/AUT2000/InicioAutenticacion/IngresoRutClave.html?https://misiir.sii.cl/cgi_misii/siihome.cgi'
            logger.info("Accediendo al portal SII...")
            page.goto(login_url, timeout=60000)
            page.wait_for_load_state('networkidle', timeout=30000)
            
            page.wait_for_selector('input#rutcntr', timeout=30000)
            page.wait_for_selector('input#clave', timeout=30000)
            page.wait_for_selector('button#bt_ingresar', timeout=30000)
            
            # Resolver CAPTCHA si aparece
            if page.is_visible('.g-recaptcha') or page.is_visible('#captcha'):
                logger.info("CAPTCHA detectado, resolviendo con 2Captcha...")
                solver = TwoCaptcha(TWOCAPTCHA_API_KEY)
                try:
                    result = solver.recaptcha(sitekey='6Lc-9QMTAAAAAKr0r1v4GNSiZJ4-5p6K7s3QJ8L9', url=page.url)
                    page.evaluate(f'document.getElementById("g-recaptcha-response").innerHTML = "{result["code"]}";')
                    time.sleep(3)
                except Exception as e:
                    logger.error(f"Error resolviendo CAPTCHA: {e}")
            
            # Llenar credenciales
            page.fill('input#rutcntr', rut.replace('-', '').replace('.', ''))
            time.sleep(1)
            page.fill('input#clave', password)
            time.sleep(random.uniform(2, 4))
            page.click('button#bt_ingresar')
            
            page.wait_for_load_state('networkidle', timeout=30000)
            time.sleep(8)
            
            if "error" in page.url.lower() or "incorrecta" in page.content().lower():
                raise Exception("Login fallido: RUT o contraseña incorrectos")
            
            logger.info("Login exitoso")
            
            # === IR A RCV ===
            rcv_url = 'https://www.sii.cl/servicios_online/1039-3256.html'
            page.goto(rcv_url, timeout=60000)
            page.wait_for_load_state('networkidle')
            
            ingresar_link = page.query_selector('a[href*="consdcvinternetui"]')
            if ingresar_link:
                ingresar_link.click()
                page.wait_for_load_state('networkidle')
                time.sleep(5)
            else:
                raise Exception("No se encontró enlace al RCV")
            
            # === CONSULTA RCV ===
            page.wait_for_selector('select[name="rut"]', timeout=30000)
            page.wait_for_selector('select#periodoMes', timeout=30000)
            page.wait_for_selector('select[ng-model="periodoAnho"]', timeout=30000)
            
            page.select_option('select[name="rut"]', value=rut)
            mes = periodo[-2:]
            anio = periodo[:4]
            page.select_option('select#periodoMes', value=mes)
            page.select_option('select[ng-model="periodoAnho"]', value=anio)
            
            page.click('button[type="submit"]')
            page.wait_for_load_state('networkidle')
            time.sleep(8)
            
            # === SELECCIONAR PESTAÑA ===
            tab = 'compra' if tipo_rcv == 'compra' else 'venta'
            tab_link = page.query_selector(f'a[ui-sref="{tab}"]')
            if tab_link:
                tab_link.click()
                time.sleep(6)
            else:
                raise Exception(f"No se encontró pestaña {tab}")
            
            # === DESCARGAR ===
            descargar_btn = page.query_selector('button[ng-click*="descargaDetalle"]')
            if not descargar_btn:
                logger.info("No hay documentos para este período")
                return {"status": "sin_datos", "message": "No hay RCV para este período"}
            
            with page.expect_download(timeout=120000) as download_info:
                descargar_btn.click()
                time.sleep(3)
            download = download_info.value
            
            file_path = os.path.join(download_dir, f"rcv_{periodo}_{tipo_rcv}_{task_id}.csv")
            download.save_as(file_path)
            logger.info(f"Archivo descargado: {file_path}")
            
            # === PROCESAR CSV ===
            num_registros = 0
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.DictReader(f, delimiter=';')
                for row in reader:
                    if not row or not row.get('Tipo Doc'):
                        continue
                    num_registros += 1
            
            logger.info(f"RCV completado: {num_registros} registros procesados")
            
            with open(log_file, "a") as f:
                f.write(f"\nSUCCESS: {num_registros} registros - Archivo: {os.path.basename(file_path)}\n")
                
            return {
                "status": "completado",
                "registros": num_registros,
                "archivo": os.path.basename(file_path),
                "url_descarga": f"https://{os.getenv('USER')}.pythonanywhere.com/downloads/{os.path.basename(file_path)}"
            }
            
    except Exception as e:
        error_msg = f"ERROR: {str(e)}"
        logger.error(error_msg)
        with open(log_file, "a") as f:
            f.write(f"\n{error_msg}\n")
        raise
    finally:
        if 'browser' in locals():
            browser.close()