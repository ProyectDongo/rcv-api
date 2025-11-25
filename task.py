from celery import Celery
from playwright.sync_api import sync_playwright
import os
import csv
import logging

celery = Celery("worker")
celery.conf.broker_url = os.getenv("REDIS_URL", "redis://localhost:6379")
celery.conf.result_backend = "rpc://"

@celery.task
def fetch_rcv_task(periodo, tipo_rcv, rut, password):
    os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
    logger.debug(f"Iniciando RPA fetch para período {period} y tipo {tipo_rcv}")
    download_dir = '/app/downloads'
    os.makedirs(download_dir, exist_ok=True)
   
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
        context = browser.new_context(
            accept_downloads=True,
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
       
        file_path = None
        try:
            # Paso 1: Acceder a la página de login
            login_url = 'https://zeus.sii.cl/AUT2000/InicioAutenticacion/IngresoRutClave.html?https://misiir.sii.cl/cgi_misii/siihome.cgi'
            logger.debug(f"Accediendo a {login_url}")
            page.goto(login_url, timeout=60000)
            page.wait_for_load_state('networkidle')
           
            # Esperar por los campos de login
            page.wait_for_selector('input#rutcntr', state='visible', timeout=60000)
            page.wait_for_selector('input#clave', state='visible', timeout=60000)
            page.wait_for_selector('button#bt_ingresar', state='visible', timeout=60000)
           
            # Manejar posible CAPTCHA
            if page.is_visible('.g-recaptcha') or page.is_visible('#captcha') or page.is_visible('img[src*="captcha"]'):
                logger.debug("CAPTCHA detectado - intentando resolver...")
                solver = TwoCaptcha(TWOCAPTCHA_API_KEY)
                captcha_element = page.query_selector('.g-recaptcha[data-sitekey]')
                if captcha_element:
                    site_key = captcha_element.get_attribute('data-sitekey')
                    result = solver.recaptcha(sitekey=site_key, url=page.url)
                    page.evaluate(f'document.getElementById("g-recaptcha-response").innerHTML = "{result["code"]}";')
                else:
                    captcha_img = page.query_selector('img[src*="captcha"]')
                    if captcha_img:
                        captcha_img_src = captcha_img.get_attribute('src')
                        result = solver.normal(captcha_img_src)
                        page.fill('input[name="captcha"]', result['code']) # Ajusta si necesario
               
                time.sleep(random.uniform(2, 5)) # Espera humana
            try:
                    credenciales = RCVUsuario.objects.get(empresa_id=empresa_id)
                    SII_RUT = credenciales.rut.strip()
                    SII_PASSWORD = credenciales.password.strip()
            except RCVUsuario.DoesNotExist:
                logger.error(f"No hay credenciales SII configuradas para la empresa ID {empresa_id}")
                raise Exception("❌ Credenciales SII no configuradas. Ve al módulo contable → 'Parámetros SII' y configúralas.")
            if not SII_RUT or not SII_PASSWORD:
                raise Exception("❌ RUT o contraseña SII vacíos. Configúralos en 'Parámetros SII'.")
           
            # Paso 2: Llenar formulario de login
            logger.debug("Llenando campo RUT")
            page.fill('input#rutcntr', SII_RUT.replace('-', ''), timeout=60000) # Sin guión, como placeholder sugiere
            time.sleep(1) # Espera para onblur formatoRut
           
            logger.debug("Llenando campo clave")
            page.fill('input#clave', SII_PASSWORD, timeout=60000)
            time.sleep(random.uniform(1, 3)) # Delay humano
           
            logger.debug("Click en botón de ingreso")
            page.click('button#bt_ingresar', timeout=60000)
           
            page.wait_for_load_state('networkidle')
            time.sleep(5) # Espera redirección post-login
           
            if "error" in page.url.lower() or "clave incorrecta" in page.content().lower():
                raise Exception("Login fallido: Verifica RUT y contraseña")
           
            # Paso 3: Navegar a "Registro de Compras y Ventas"
            rcv_entry_url = 'https://www.sii.cl/servicios_online/1039-3256.html'
            logger.debug(f"Accediendo a página de RCV: {rcv_entry_url}")
            page.goto(rcv_entry_url, timeout=60000)
            page.wait_for_load_state('networkidle')
           
            # Click en "Ingresar al Registro de Compras y Ventas"
            ingresar_link = page.query_selector('a[href="https://www4.sii.cl/consdcvinternetui"]')
            if ingresar_link:
                logger.debug("Click en 'Ingresar al Registro de Compras y Ventas'")
                ingresar_link.click(timeout=60000)
                page.wait_for_load_state('networkidle')
                time.sleep(3)
            else:
                raise Exception("No se encontró el link para ingresar al RCV")
           
            # Paso 4: En la página de consulta, llenar filtro
            page.wait_for_selector('select[name="rut"]', state='visible', timeout=60000) # Select RUT
            page.wait_for_selector('select#periodoMes', state='visible', timeout=60000)
            page.wait_for_selector('select[ng-model="periodoAnho"]', state='visible', timeout=60000)
            page.wait_for_selector('button[type="submit"]', state='visible', timeout=60000) # Botón Consultar
           
            # Seleccionar RUT (asumiendo es el primero o único)
            page.select_option('select[name="rut"]', value=SII_RUT) # Usa el valor real de RUT
           
            # Seleccionar Mes y Año
            mes = period[-2:] # e.g., '12'
            anio = period[:4] # e.g., '2017'
            page.select_option('select#periodoMes', value=mes)
            page.select_option('select[ng-model="periodoAnho"]', value=anio)
           
            # Click en Consultar
            logger.debug("Click en Consultar")
            page.click('button[type="submit"]', timeout=60000)
            page.wait_for_load_state('networkidle')
            time.sleep(5) # Espera resultados
           
            # Paso 5: Seleccionar tab Compra o Venta
            ui_sref = 'compra' if tipo_rcv == 'compra' else 'venta'
            tab_selector = f'a[ui-sref="{ui_sref}"]'
            logger.debug(f"Trying to select tab with selector: {tab_selector}")
            tab = page.query_selector(tab_selector)
            if tab:
                logger.debug("Tab found, clicking")
                tab.click(timeout=60000)
                time.sleep(5) # Aumentado para asegurar carga
                logger.debug("Waiting for active tab")
                page.wait_for_function(f'() => {{ const activeLi = document.querySelector("ul.nav-tabs li.active"); return activeLi && activeLi.querySelector("a").getAttribute("ui-sref") === "{ui_sref}"; }}', timeout=60000)
                logger.debug("Tab active confirmed")
            else:
                raise Exception(f"Tab not found for {tipo_rcv}")
           
            # Paso 6: En la sección de Resumen, check for "Descargar Detalles"
            descargar_btn_selector = 'button[ng-click*="descargaDetalle"]'
            try:
                page.wait_for_selector(descargar_btn_selector, state='visible', timeout=10000)
            except PlaywrightTimeoutError:
                logger.info(f"No data for {tipo_rcv} in period {period}, no download button found.")
                return None
           
            descargar_btn = page.query_selector(descargar_btn_selector)
            if descargar_btn:
                logger.debug("Click en Descargar Detalles")
                with page.expect_download(timeout=60000) as download_info:
                    descargar_btn.click(timeout=60000)
                    time.sleep(2)
                download = download_info.value
                file_path = os.path.join(download_dir, download.suggested_filename)
                download.save_as(file_path)
                logger.debug(f"Archivo descargado: {file_path}")
            else:
                raise Exception("No se encontró botón de descargar detalles")
           
            # Paso 7: Procesar CSV descargado
            num_registros = 0
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=';')
                for row in reader:
                    logger.debug(f"Raw row: {row}")
                    if not row or not row.get('Tipo Doc'): # Saltar líneas vacías o inválidas
                        continue
                   
                    tipo_doc = row.get('Tipo Doc', '').strip()
                    fecha_docto_str = row.get('Fecha Docto', '').strip()
                   
                    if not fecha_docto_str:
                        logger.warning(f"Registro sin fecha_docto para folio {row.get('Folio')} - Saltando")
                        continue
                   
                    try:
                        fecha_docto = datetime.strptime(fecha_docto_str, '%d/%m/%Y').date()
                    except ValueError:
                        logger.warning(f"Fecha inválida en documento {row.get('Folio')}: {fecha_docto_str} - Saltando registro")
                        continue
                   
                    monto_exento_str = row.get('Monto Exento', '0').replace('.', '').strip()
                    monto_exento = int(monto_exento_str) if monto_exento_str else 0
                   
                    monto_neto_str = row.get('Monto Neto', '0').replace('.', '').strip()
                    monto_neto = int(monto_neto_str) if monto_neto_str else 0
                   
                    if tipo_rcv == 'compra':
                        model = RCVCompra
                        tipo_compra = row.get('Tipo Compra', '').strip()
                        rut_proveedor = row.get('RUT Proveedor', '').strip()
                        razon_social = row.get('Razon Social', '').strip()
                        folio = row.get('Folio', '').strip()
                        fecha_recepcion_str = row.get('Fecha Recepcion', '').strip()
                        monto_iva_recuperable_str = row.get('Monto IVA Recuperable', '0').replace('.', '').strip() or row.get('', '0').replace('.', '').strip() # Handle empty
                        monto_iva_recuperable = int(monto_iva_recuperable_str) if monto_iva_recuperable_str and monto_iva_recuperable_str != '' else 0
                        monto_iva_no_recuperable_str = row.get('Monto Iva No Recuperable', '0').replace('.', '').strip()
                        monto_iva_no_recuperable = int(monto_iva_no_recuperable_str) if monto_iva_no_recuperable_str else 0
                        codigo_iva_no_rec = row.get('Codigo IVA No Rec.', '').strip()
                        monto_total_str = row.get('Monto Total', '0').replace('.', '').strip()
                        monto_total = int(monto_total_str) if monto_total_str else 0
                       
                        fecha_recepcion = None
                        if fecha_recepcion_str:
                            try:
                                fecha_recepcion = datetime.strptime(fecha_recepcion_str, '%d/%m/%Y %H:%M:%S').date()
                            except ValueError:
                                try:
                                    fecha_recepcion = datetime.strptime(fecha_recepcion_str, '%d/%m/%Y').date()
                                except ValueError:
                                    pass
                       
                        logger.debug(f"Procesando folio: {folio} - RUT: {rut_proveedor} - Nombre: {razon_social}")
                       
                        defaults = {
                            'tipo_compra': tipo_compra,
                            'razon_social': razon_social,
                            'fecha_docto': fecha_docto,
                            'fecha_recepcion': fecha_recepcion,
                            'monto_exento': monto_exento,
                            'monto_neto': monto_neto,
                            'monto_iva_recuperable': monto_iva_recuperable,
                            'monto_iva_no_recuperable': monto_iva_no_recuperable,
                            'codigo_iva_no_rec': codigo_iva_no_rec,
                            'monto_total': monto_total,
                        }
                       
                        rcv, created = model.objects.update_or_create(
                            empresa_id=empresa_id,
                            tipo_doc=tipo_doc,
                            rut_proveedor=rut_proveedor,
                            folio=folio,
                            defaults=defaults
                        )
                    else: # venta
                        model = RCVVenta
                        tipo_venta = row.get('Tipo Venta', '').strip()
                        rut_cliente = row.get('RUT Cliente', '').strip() # Cambiado a 'RUT Cliente' si es mayúscula
                        nombre_cliente = row.get('Razon Social', '').strip()
                        folio = row.get('Folio', '').strip()
                        monto_iva_str = row.get('Monto IVA', '0').replace('.', '').strip()
                        monto_iva = int(monto_iva_str) if monto_iva_str else 0
                        monto_total_str = row.get('Monto total', '0').replace('.', '').strip()
                        monto_total = int(monto_total_str) if monto_total_str else 0
                       
                        logger.debug(f"Procesando folio: {folio} - RUT: {rut_cliente} - Nombre: {nombre_cliente}")
                       
                        defaults = {
                            'tipo_venta': tipo_venta,
                            'nombre_cliente': nombre_cliente,
                            'fecha_docto': fecha_docto,
                            'monto_exento': monto_exento,
                            'monto_neto': monto_neto,
                            'monto_iva': monto_iva,
                            'monto_total': monto_total,
                        }
                       
                        rcv, created = model.objects.update_or_create(
                            empresa_id=empresa_id,
                            tipo_doc=tipo_doc,
                            rut_cliente=rut_cliente,
                            folio=folio,
                            defaults=defaults
                        )
                    num_registros += 1
           
            logger.debug(f"Datos fetched: {num_registros} registros")
           
        except Exception as e:
            logger.error(f"Error en RPA: {str(e)}")
            raise # Propaga el error para que Celery marque como failed
        finally:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
            browser.close()
   
    return None
def asegurar_conceptos_y_centro_rcv(empresa):
    """Crea automáticamente los conceptos y centro de costo para RCV si no existen"""
    centro, _ = CentroCosto.objects.get_or_create(
        empresa=empresa,
        nombre="RCV-SII",
        defaults={'descripcion': 'Centro de costo automático para documentos RCV del SII'}
    )
    ConceptoEgreso.objects.get_or_create(
        empresa=empresa,
        nombre="RCV - COMPRA",
        defaults={'nombre': "RCV - COMPRA"}
    )
    ConceptoIngreso.objects.get_or_create(
        empresa=empresa,
        nombre="RCV - VENTA",
        defaults={'nombre': "RCV - VENTA"}
    )
    
    return {"archivo": f"rcv_{periodo}_{tipo_rcv}.csv", "registros": 150}