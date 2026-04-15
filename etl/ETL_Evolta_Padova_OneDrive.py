import time
import os
import glob
import shutil
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials as ServiceCredentials
import smtplib
import traceback
import random
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ============================================================
# TIPO DE CAMBIO - BCRP
# ============================================================

# Cache de TCs para no consultar la API múltiples veces por la misma fecha
_TC_CACHE = {}

def get_tipo_cambio(fecha=None):
    """
    Obtiene TC USD/PEN del BCRP para una fecha específica.
    Si fecha=None usa la fecha de hoy.
    Usa cache para evitar múltiples consultas a la misma fecha.
    Usa 3.75 como respaldo si la API falla.
    """
    TC_RESPALDO = 3.75

    # Determinar fecha a consultar
    import pandas as _pd
    if fecha is None or (hasattr(_pd, 'isnull') and _pd.isnull(fecha)):
        fecha_dt = datetime.now()
    elif isinstance(fecha, str):
        try:
            fecha_dt = datetime.strptime(fecha[:10], "%Y-%m-%d")
        except:
            fecha_dt = datetime.now()
    elif hasattr(fecha, 'strftime'):
        try:
            # Pandas NaT lanza excepción al hacer strftime
            fecha_dt = fecha.to_pydatetime() if hasattr(fecha, 'to_pydatetime') else fecha
        except:
            fecha_dt = datetime.now()
    else:
        fecha_dt = datetime.now()

    fecha_str = fecha_dt.strftime("%Y-%m-%d")

    # Revisar cache primero
    if fecha_str in _TC_CACHE:
        return _TC_CACHE[fecha_str]

    try:
        # Consultar BCRP para esa fecha
        url = f"https://estadisticas.bcrp.gob.pe/estadisticas/series/api/PD04637PD/json/{fecha_str}/{fecha_str}/ing"
        r = requests.get(url, timeout=10)
        if not r.text.strip():
            raise ValueError("Respuesta vacía del BCRP")
        data = r.json()
        periodos = data.get("periods", [])
        if periodos and periodos[0].get("values"):
            tc = float(periodos[0]["values"][0])
            _TC_CACHE[fecha_str] = tc
            return tc

        # Si no hay dato (feriado/fin de semana), buscar hasta 7 días antes
        for dias_atras in range(1, 8):
            fecha_anterior = (fecha_dt - timedelta(days=dias_atras)).strftime("%Y-%m-%d")
            if fecha_anterior in _TC_CACHE:
                tc = _TC_CACHE[fecha_anterior]
                _TC_CACHE[fecha_str] = tc
                return tc
            url2 = f"https://estadisticas.bcrp.gob.pe/estadisticas/series/api/PD04637PD/json/{fecha_anterior}/{fecha_anterior}/ing"
            r2 = requests.get(url2, timeout=5)
            if not r2.text.strip():
                continue
            data2 = r2.json()
            periodos2 = data2.get("periods", [])
            if periodos2 and periodos2[0].get("values"):
                tc = float(periodos2[0]["values"][0])
                _TC_CACHE[fecha_str] = tc
                _TC_CACHE[fecha_anterior] = tc
                return tc

        print(f"   -> [TC] Sin datos BCRP para {fecha_str}, usando respaldo: S/ {TC_RESPALDO}")
        _TC_CACHE[fecha_str] = TC_RESPALDO
        return TC_RESPALDO

    except Exception as e:
        print(f"   -> [TC] Error API BCRP ({e}), usando respaldo: S/ {TC_RESPALDO}")
        _TC_CACHE[fecha_str] = TC_RESPALDO
        return TC_RESPALDO


def convertir_precios_a_soles(df, col_precio, col_moneda, tc=None, col_fecha=None):
    """
    Agrega columna PrecioVentaSoles convirtiendo USD a PEN.
    Si col_fecha está definida, usa el TC histórico de la fecha de cada registro.
    Si tc está definido (sin col_fecha), usa ese TC fijo para todos.
    """
    df = df.copy()
    convertidos = 0
    precios_soles = []

    for idx, row in df.iterrows():
        try:
            precio = float(str(row[col_precio]).replace(",", "")) if row[col_precio] else 0
        except:
            precio = 0

        moneda = str(row[col_moneda]).upper().strip()
        es_usd = "DOLAR" in moneda or "USD" in moneda

        if es_usd:
            # Determinar TC a usar
            if col_fecha and col_fecha in df.columns:
                fecha_registro = row[col_fecha]
                tc_usar = get_tipo_cambio(fecha_registro)
            elif tc is not None:
                tc_usar = tc
            else:
                tc_usar = get_tipo_cambio()
            precios_soles.append(round(precio * tc_usar, 2))
            convertidos += 1
        else:
            precios_soles.append(round(precio, 2))

    df["PrecioVentaSoles"] = precios_soles
    en_soles = len(df) - convertidos
    print(f"   -> [TC] {en_soles} en soles + {convertidos} en dólares convertidos con TC histórico por fecha")
    return df



# --- CONFIGURACIÓN DE CREDENCIALES ---
# En nube (GitHub Actions): se leen desde variables de entorno (Secrets)
# En local (Windows): se pueden definir como variables de entorno o usar valores por defecto
USER_CRED = os.environ.get("EVOLTA_USER", "calopez")
PASS_CRED = os.environ.get("EVOLTA_PASS", "")

URL_LOGIN = "https://v4.evolta.pe/Login/Acceso/Index"
URL_REPORTE_STOCK = "https://v4.evolta.pe/Reportes/RepCargaStock/IndexNuevoRepStock"
URL_REPORTE_VENTAS = "https://v4.evolta.pe/Reportes/RepVenta/Index"

# Configuración SMTP
EMAIL_FROM = "sistema.padova@gmail.com"
EMAIL_TO = "yleon@padovasac.com, carrunategui@constructorapadova.pe"
EMAIL_PASS = os.environ.get("EMAIL_PASS", "")

# Reglas de Negocio
TARGET_PROJECTS = [
    'SUNNY',
    'LITORAL 900',
    'HELIO - SANTA BEATRIZ',
    'LOMAS DE CARABAYLLO'
]

# Detectar entorno: nube (Linux) o local (Windows)
IS_CLOUD = os.name != 'nt'

# Directorios de trabajo — /tmp en nube, rutas locales en Windows
if IS_CLOUD:
    DOWNLOAD_DIR       = "/tmp/evolta_stock"
    DOWNLOAD_DIR_VENTAS = "/tmp/evolta_ventas"
else:
    DOWNLOAD_DIR        = r"C:\Users\MKT\Documents\EVOLTA\descargas_stock"
    DOWNLOAD_DIR_VENTAS = r"C:\Users\MKT\Documents\EVOLTA\descargas_ventas"

# OneDrive — solo disponible en Windows local
ONEDRIVE_OUTPUT_DIR = None if IS_CLOUD else r"C:\Users\MKT\OneDrive - PADOVA SAC\PADOVA - MKT - MIRANO INMOBILIARIA - VENTAS\Dashboards"
ONEDRIVE_FILE_NAME  = "ReporteEvolta.xlsx"

# Google Sheets - Dashboard
GSHEETS_SPREADSHEET_ID = "15slphQ1xs7pkG4zH5mVwsnc1XKgGtLKah8-FHm7HMEM"

# Credenciales Google: en nube desde variable de entorno (base64), en local desde archivo .json
def _load_gsheets_credentials():
    """Carga credenciales de Google desde env var (nube) o archivo local."""
    import base64, json, tempfile
    b64 = os.environ.get("GSHEETS_CREDENTIALS_B64", "")
    if b64:
        creds_dict = json.loads(base64.b64decode(b64).decode("utf-8"))
        # Escribir a archivo temporal para que gspread lo pueda leer
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(creds_dict, tmp)
        tmp.flush()
        return tmp.name
    # Fallback: archivo local
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "evoltareportes-00ffe1b337be.json")
    if os.path.exists(local_path):
        return local_path
    raise FileNotFoundError("No se encontraron credenciales de Google. Define GSHEETS_CREDENTIALS_B64 o coloca el .json local.")

GSHEETS_CREDENTIALS_FILE = _load_gsheets_credentials()

# Crear directorios si no existen
for dir_path in [DOWNLOAD_DIR, DOWNLOAD_DIR_VENTAS]:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

# Configuración de años para reporte de ventas (máximo 1 año por descarga)
AÑOS_VENTAS = [2024, 2025, 2026]

# ============================================================================
# DEFINICIÓN DE COLUMNAS MAESTRAS PARA NORMALIZACIÓN
# ============================================================================

COLUMNAS_BASE = [
    'CorrelativoOC', 'FechaVenta', 'FechaPreminuta', 'FechaEntrega_Minuta',
    'Fecha_Registro_Sistema', 'Fecha_Primera_Visita', 'FechaProspecto', 'FechaDevolucion',
    'Estado', 'EstadoOC', 'TipoDocumentoTitular', 'NroDocumentoTitular', 'NombresTitular',
    'CorreoElectronico', 'CorreoElectronico2', 'TelefonoCasa', 'TelefonoCelular',
    'TelefonoCelular2', 'Genero', 'Estado_Civil', 'Provincia_Procedencia',
    'Distrito_Procedencia', 'Direccion', 'RangoEdad', 'NivelInteres', 'ComoSeEntero',
    'FormaContacto', 'PerfilCrediticio', 'Institucion', 'NivelIngresos', 'MotivoCompra',
    'Promocion', 'ContenidoPromocion', 'ValorTotalCombo', 'ReferidoPor'
]

def generar_columnas_inmueble(n):
    """Genera las 12 columnas para un inmueble n"""
    return [
        f'T/M_{n}', f'TipoInmueble_{n}', f'Modelo_{n}', f'NroInmueble_{n}',
        f'NroPiso_{n}', f'Vista_{n}', f'PrecioBase_{n}', f'PrecioLista_{n}',
        f'DescuentoLista_{n}', f'TotalLista_{n}', f'PrioridadOC_{n}', f'Orden_{n}'
    ]

COLUMNAS_INMUEBLES = []
for i in range(1, 9):
    COLUMNAS_INMUEBLES.extend(generar_columnas_inmueble(i))

COLUMNAS_FINALES = [
    'CargaFamiliar', 'Proyecto', 'Etapa', 'SubTotal', 'MontoDescuento', 'PrecioVenta',
    'MontoSeparacion', 'BonoVerde', 'TipodeBono', 'MontoBono', 'MontoPagadoBono',
    'PorcentajePagado', 'EstadoBono', 'MontoCuotaInicial', 'MontoPagadoCI',
    'PorcetanjePagadoCI', 'Estado_CI', 'MontoFinanciamiento', 'MontoDesembolsado',
    'PorcetanjePagado_SF', 'EstadoSF', 'TipoMoneda', 'TipoCambio', 'TipoFinanciamiento',
    'EntidadFinanciamiento', 'Vendedor', 'utm_medium', 'utm_source', 'utm_campaign',
    'utm_term', 'utm_content', 'Es_Cotizador_Evolta', 'Es_Formulario_Evolta',
    'Es_Cotizador_y_Formulario_Evolta', 'Ult_Comentario', 'MigracionMasiva',
    'TotalCuotaInicial', 'TotalCuotaFinanciar', 'Areaterreno', 'TasaInteres',
    'CallCenter', 'TipoProceso', 'Puesto', 'IdProforma', 'AÑO'
]

COLUMNAS_MAESTRAS = COLUMNAS_BASE + COLUMNAS_INMUEBLES + COLUMNAS_FINALES


def get_driver(download_dir):
    """Inicializa driver con configuración de descarga específica."""
    # Asegurar que el directorio existe
    os.makedirs(download_dir, exist_ok=True)
    options = webdriver.ChromeOptions()
    
    # --- FLAGS DE ESTABILIDAD ---
    options.add_argument("--headless=new") 
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu") 
    options.add_argument("--no-sandbox") 
    options.add_argument("--disable-dev-shm-usage") 
    options.add_argument("--disable-software-rasterizer") 
    options.add_argument("--disable-features=VizDisplayCompositor") 
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--log-level=3") 
    
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.automatic_downloads": 1
    }
    options.add_experimental_option("prefs", prefs)
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def clean_environment(directory, extension="*.xlsx"):
    """Limpia archivos previos de un directorio."""
    print(f">> [MAINTENANCE] Limpiando directorio: {directory}")
    files = glob.glob(os.path.join(directory, extension))
    for f in files:
        try: 
            os.remove(f)
        except: 
            pass


def dismiss_popup(driver):
    """Estrategia 'Anti-Propaganda'."""
    try:
        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
        time.sleep(1)
        driver.execute_script("document.body.click();")
        time.sleep(1)
    except Exception:
        pass


def robust_login(driver, wait):
    """Manejo de Login."""
    print(">> [LOGIN] Navegando al login...")
    try:
        driver.get(URL_LOGIN)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "input")))
        
        try: 
            user_field = driver.find_element(By.ID, "UserName")
        except:
            try: 
                user_field = driver.find_element(By.NAME, "Usuario")
            except: 
                user_field = driver.find_element(By.XPATH, "//input[@type='text']")
        
        user_field.clear()
        user_field.send_keys(USER_CRED)
        driver.find_element(By.XPATH, "//input[@type='password']").send_keys(PASS_CRED)

        try:
            driver.find_element(By.XPATH, "//button[@type='submit'] | //input[@type='submit']").click()
        except: 
            pass

        try:
            wait.until(EC.url_changes(URL_LOGIN))
            print(">> [LOGIN] Exitoso (URL Changed).")
        except:
            print(">> [LOGIN] Continuamos (Sin cambio URL detectado, asumiendo éxito).")
            
        time.sleep(2)
        dismiss_popup(driver)
            
    except Exception as e:
        driver.save_screenshot(os.path.join(DOWNLOAD_DIR, "error_login.png"))
        raise Exception(f"Error Login: {e}")


def execute_stock_extraction(driver):
    """Descarga reporte de Stock usando el ID fijo 'btnExportar'."""
    print(f">> [EXTRACTION STOCK] Navegando al módulo: {URL_REPORTE_STOCK}")
    driver.get(URL_REPORTE_STOCK)
    wait = WebDriverWait(driver, 30)
    time.sleep(3)
    dismiss_popup(driver)
    
    # 1. FILTRO PROYECTO: TODOS
    try:
        print("   -> Configurando filtro de Proyecto...")
        select_element = None
        try: 
            select_element = wait.until(EC.presence_of_element_located((By.ID, "ProyectoId")))
        except: 
            select_element = driver.find_element(By.TAG_NAME, "select")
            
        select = Select(select_element)
        try: 
            select.select_by_visible_text("Todos")
        except: 
            try: 
                select.select_by_visible_text("TODOS")
            except: 
                select.select_by_index(0)
        print("   -> Filtro establecido: TODOS")
        time.sleep(1)
    except Exception as e:
        print(f"   !! Warning UI: No se pudo manipular dropdown (Usando default): {e}")

    # 2. CLICK EN EXPORTAR
    try:
        print("   -> Buscando botón 'btnExportar'...")
        export_btn = wait.until(EC.element_to_be_clickable((By.ID, "btnExportar")))
        
        print("   -> Click en Exportar (JS)...")
        driver.execute_script("arguments[0].click();", export_btn)
        
        # 3. ESPERA DE DESCARGA
        timeout = 300 
        elapsed = 0
        file_downloaded = False
        
        print("   -> Esperando descarga (Max 300s)...")
        while elapsed < timeout:
            files = glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx"))
            valid_files = [f for f in files if not f.endswith('.crdownload') and not f.endswith('.tmp')]
            
            if valid_files:
                latest = max(valid_files, key=os.path.getctime)
                if (datetime.now().timestamp() - os.path.getctime(latest)) < 300:
                    print(f"   -> [OK] Archivo descargado: {os.path.basename(latest)}")
                    file_downloaded = True
                    break
            
            time.sleep(1)
            elapsed += 1
            
        if not file_downloaded:
            raise Exception("Tiempo de espera agotado. El archivo no se descargó.")
            
    except Exception as e:
        driver.save_screenshot(os.path.join(DOWNLOAD_DIR, "error_extraction.png"))
        raise Exception(f"Fallo crítico en botón exportar: {e}")


def set_date_field(driver, field_id, date_str):
    """Establece una fecha en un campo de fecha."""
    try:
        date_field = driver.find_element(By.ID, field_id)
        driver.execute_script("arguments[0].value = '';", date_field)
        date_field.clear()
        date_field.send_keys(date_str)
        time.sleep(0.5)
    except Exception as e:
        print(f"   !! Error estableciendo fecha en {field_id}: {e}")


def execute_ventas_extraction_year(driver, wait, año):
    """Descarga reporte de Ventas para un año específico en formato CSV."""
    print(f"\n>> [EXTRACTION VENTAS {año}] Procesando...")
    
    driver.get(URL_REPORTE_VENTAS)
    time.sleep(4)  # Esperar carga completa
    dismiss_popup(driver)
    
    # Calcular fechas del año
    fecha_inicio = f"01/01/{año}"
    fecha_fin = f"31/12/{año}"
    
    # Si es el año actual, usar la fecha de hoy como fin
    if año == datetime.now().year:
        fecha_fin = datetime.now().strftime("%d/%m/%Y")
    
    print(f"   -> Rango: {fecha_inicio} - {fecha_fin}")
    
    try:
        # 1. FILTRO PROYECTO: TODOS (el primero en la página)
        try:
            # Buscar todos los selects y tomar el primero (Proyecto)
            proyecto_select = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "select")))
            select = Select(proyecto_select)
            select.select_by_index(0)  # Primera opción suele ser "TODOS"
            print("   -> Proyecto: TODOS (index 0)")
            time.sleep(0.5)
        except Exception as e:
            print(f"   !! Warning Proyecto: {e}")
        
        # 2. ESTABLECER FECHAS - Buscar por label "Fecha de inicio" y "Fecha de fin"
        print("   -> Configurando fechas...")
        
        fecha_inicio_ok = False
        fecha_fin_ok = False
        
        # Estrategia 1: Buscar input después del label "Fecha de inicio"
        try:
            # Buscar el contenedor que tiene el label y el input
            fecha_inicio_input = driver.find_element(By.XPATH, 
                "//label[contains(text(),'Fecha de inicio')]/following::input[1] | " +
                "//span[contains(text(),'Fecha de inicio')]/following::input[1] | " +
                "//div[contains(text(),'Fecha de inicio')]/following::input[1]")
            driver.execute_script("arguments[0].value = '';", fecha_inicio_input)
            fecha_inicio_input.clear()
            time.sleep(0.2)
            fecha_inicio_input.send_keys(fecha_inicio)
            fecha_inicio_ok = True
            print(f"   -> Fecha inicio: {fecha_inicio} ✓")
        except Exception as e:
            print(f"   !! Fecha inicio (método 1): {e}")
        
        # Estrategia 2 para fecha inicio si la primera falló
        if not fecha_inicio_ok:
            try:
                # Buscar todos los inputs con valor que contenga fecha
                all_inputs = driver.find_elements(By.CSS_SELECTOR, "input")
                for inp in all_inputs:
                    val = inp.get_attribute("value") or ""
                    if "/2025" in val or "/2026" in val or "/2024" in val:
                        # Este podría ser un campo de fecha, verificar posición
                        rect = inp.rect
                        # El primero de izquierda a derecha debería ser fecha inicio
                        driver.execute_script("arguments[0].value = '';", inp)
                        inp.clear()
                        inp.send_keys(fecha_inicio)
                        fecha_inicio_ok = True
                        print(f"   -> Fecha inicio (alternativo): {fecha_inicio} ✓")
                        break
            except Exception as e:
                print(f"   !! Fecha inicio (método 2): {e}")
        
        time.sleep(0.3)
        
        # Estrategia 1: Buscar input después del label "Fecha de fin"
        try:
            fecha_fin_input = driver.find_element(By.XPATH, 
                "//label[contains(text(),'Fecha de fin')]/following::input[1] | " +
                "//span[contains(text(),'Fecha de fin')]/following::input[1] | " +
                "//div[contains(text(),'Fecha de fin')]/following::input[1]")
            driver.execute_script("arguments[0].value = '';", fecha_fin_input)
            fecha_fin_input.clear()
            time.sleep(0.2)
            fecha_fin_input.send_keys(fecha_fin)
            fecha_fin_ok = True
            print(f"   -> Fecha fin: {fecha_fin} ✓")
        except Exception as e:
            print(f"   !! Fecha fin (método 1): {e}")
        
        # Estrategia 2: Buscar el segundo input con formato fecha
        if not fecha_fin_ok:
            try:
                all_inputs = driver.find_elements(By.CSS_SELECTOR, "input")
                date_inputs_found = []
                for inp in all_inputs:
                    val = inp.get_attribute("value") or ""
                    if "/2025" in val or "/2026" in val or "/2024" in val or "/2021" in val:
                        date_inputs_found.append(inp)
                
                if len(date_inputs_found) >= 2:
                    # El segundo debería ser fecha fin
                    driver.execute_script("arguments[0].value = '';", date_inputs_found[1])
                    date_inputs_found[1].clear()
                    date_inputs_found[1].send_keys(fecha_fin)
                    fecha_fin_ok = True
                    print(f"   -> Fecha fin (alternativo): {fecha_fin} ✓")
            except Exception as e:
                print(f"   !! Fecha fin (método 2): {e}")
        
        # Estrategia 3: JavaScript directo si todo lo demás falla
        if not fecha_inicio_ok or not fecha_fin_ok:
            print("   -> Intentando establecer fechas por JavaScript...")
            try:
                result = driver.execute_script(f"""
                    var inputs = document.querySelectorAll('input');
                    var dateInputs = [];
                    for (var i = 0; i < inputs.length; i++) {{
                        var val = inputs[i].value || '';
                        if (val.match(/\\d{{2}}\\/\\d{{2}}\\/\\d{{4}}/)) {{
                            dateInputs.push(inputs[i]);
                        }}
                    }}
                    if (dateInputs.length >= 2) {{
                        dateInputs[0].value = '{fecha_inicio}';
                        dateInputs[0].dispatchEvent(new Event('change', {{ bubbles: true }}));
                        dateInputs[1].value = '{fecha_fin}';
                        dateInputs[1].dispatchEvent(new Event('change', {{ bubbles: true }}));
                        return 'OK: ' + dateInputs.length + ' campos encontrados';
                    }}
                    return 'FAIL: Solo ' + dateInputs.length + ' campos encontrados';
                """)
                print(f"   -> JS Result: {result}")
            except Exception as e:
                print(f"   !! Error JS fechas: {e}")
        
        time.sleep(1)
        
        # 3. SELECCIONAR FORMATO CSV
        try:
            # Buscar radio button de CSV
            csv_radio = driver.find_element(By.XPATH, 
                "//input[@type='radio'][following-sibling::text()[contains(.,'Csv')] or " +
                "following-sibling::label[contains(.,'Csv')] or " +
                "@value='Csv' or @value='csv' or @value='CSV']")
            driver.execute_script("arguments[0].click();", csv_radio)
            print("   -> Formato: CSV seleccionado")
        except:
            try:
                # Alternativa: buscar label con texto Csv y hacer click
                csv_label = driver.find_element(By.XPATH, "//label[contains(text(),'Csv')]")
                csv_label.click()
                print("   -> Formato: CSV (por label)")
            except:
                try:
                    # Buscar por el texto "Csv" en cualquier parte
                    csv_element = driver.find_element(By.XPATH, "//*[text()='Csv']")
                    csv_element.click()
                    print("   -> Formato: CSV (por texto)")
                except Exception as e:
                    print(f"   !! Warning CSV: usando formato por defecto")
        
        time.sleep(1)
        
        # 4. Registrar archivos ANTES de exportar (para detectar el nuevo)
        # Buscar CSV y XLSX en ambos directorios
        DOWNLOADS_DIR = os.path.join(os.path.expanduser("~"), "Downloads")
        existing_files_ventas_csv = set(glob.glob(os.path.join(DOWNLOAD_DIR_VENTAS, "*.csv")))
        existing_files_ventas_xlsx = set(glob.glob(os.path.join(DOWNLOAD_DIR_VENTAS, "*.xlsx")))
        existing_files_stock_csv = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.csv")))
        existing_files_stock_xlsx = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx")))
        existing_files_downloads = set(glob.glob(os.path.join(DOWNLOADS_DIR, "*.csv"))) | set(glob.glob(os.path.join(DOWNLOADS_DIR, "*.xlsx")))
        existing_files = existing_files_ventas_csv | existing_files_ventas_xlsx | existing_files_stock_csv | existing_files_stock_xlsx | existing_files_downloads
        
        print(f"   -> Archivos existentes antes de exportar: {len(existing_files)}")
        
        # 5. CLICK EN EXPORTAR
        print("   -> Buscando botón Exportar...")
        export_btn = None
        
        try:
            export_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Exportar')]")))
        except:
            try:
                export_btn = driver.find_element(By.XPATH, "//button[contains(@class,'btn-primary')]")
            except:
                try:
                    export_btn = driver.find_element(By.ID, "btnExportar")
                except:
                    try:
                        export_btn = driver.find_element(By.XPATH, "//button[@type='submit']")
                    except:
                        export_btn = driver.find_element(By.CSS_SELECTOR, "button.btn")
        
        if export_btn:
            print(f"   -> Botón encontrado: {export_btn.text if export_btn.text else 'Sin texto'}")
        
        print("   -> Click en Exportar...")
        driver.execute_script("arguments[0].click();", export_btn)
        
        # Esperar a que inicie la descarga
        time.sleep(5)
        
        # 6. ESPERA DE DESCARGA (buscar NUEVOS archivos en ambos directorios)
        timeout = 120  # Reducido a 2 minutos para no esperar tanto si falla
        elapsed = 0
        file_downloaded = False
        
        print("   -> Esperando descarga (Max 120s)...")
        while elapsed < timeout:
            # Buscar CSV y XLSX en ambos directorios
            files_ventas_csv = set(glob.glob(os.path.join(DOWNLOAD_DIR_VENTAS, "*.csv")))
            files_ventas_xlsx = set(glob.glob(os.path.join(DOWNLOAD_DIR_VENTAS, "*.xlsx")))
            files_stock_csv = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.csv")))
            files_stock_xlsx = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx")))
            
            # También buscar en Downloads del usuario por si acaso
            user_downloads = os.path.expanduser("~\\Downloads")
            files_user_csv = set(glob.glob(os.path.join(user_downloads, "*.csv")))
            files_user_xlsx = set(glob.glob(os.path.join(user_downloads, "Reporte*.xlsx")))
            
            all_current_files = files_ventas_csv | files_ventas_xlsx | files_stock_csv | files_stock_xlsx | files_user_csv | files_user_xlsx
            
            # Encontrar archivos NUEVOS (que no existían antes)
            new_files = all_current_files - existing_files
            
            # Filtrar archivos en proceso de descarga
            valid_new_files = [f for f in new_files 
                              if not f.endswith('.crdownload') 
                              and not f.endswith('.tmp')
                              and os.path.getsize(f) > 0]  # Verificar que tenga contenido
            
            if valid_new_files:
                new_file = valid_new_files[0]
                print(f"   -> Archivo detectado: {new_file}")
                
                # Determinar extensión
                file_ext = os.path.splitext(new_file)[1].lower()
                new_name = os.path.join(DOWNLOAD_DIR_VENTAS, f"ReporteVenta{año}{file_ext}")
                
                try:
                    if os.path.exists(new_name):
                        os.remove(new_name)
                    shutil.move(new_file, new_name)
                    print(f"   -> [OK] Archivo movido: ReporteVenta{año}{file_ext}")
                except Exception as e:
                    print(f"   -> [OK] Archivo en: {new_file} (Error moviendo: {e})")
                file_downloaded = True
                break
            
            # Log cada 30 segundos para saber que sigue buscando
            if elapsed > 0 and elapsed % 30 == 0:
                print(f"   -> Buscando... ({elapsed}s)")
            
            time.sleep(1)
            elapsed += 1
        
        if not file_downloaded:
            # Debug: mostrar qué archivos existen ahora
            print(f"   !! Warning: No se descargó archivo para {año}")
            print(f"   !! Archivos en stock: {list(glob.glob(os.path.join(DOWNLOAD_DIR, '*.*')))[-5:]}")
            print(f"   !! Archivos en ventas: {list(glob.glob(os.path.join(DOWNLOAD_DIR_VENTAS, '*.*')))}")
            
            # Guardar screenshot para debug
            driver.save_screenshot(os.path.join(DOWNLOAD_DIR_VENTAS, f"debug_ventas_{año}.png"))
            
    except Exception as e:
        driver.save_screenshot(os.path.join(DOWNLOAD_DIR_VENTAS, f"error_ventas_{año}.png"))
        print(f"   !! Error extrayendo ventas {año}: {e}")


def execute_ventas_extraction(driver):
    """Descarga todos los reportes de ventas por año."""
    print("\n" + "="*60)
    print(">> [EXTRACTION VENTAS] Iniciando descarga de reportes de ventas")
    print("="*60)
    
    wait = WebDriverWait(driver, 30)
    
    for año in AÑOS_VENTAS:
        try:
            execute_ventas_extraction_year(driver, wait, año)
            time.sleep(2)  # Pausa entre descargas
        except Exception as e:
            print(f"   !! Error procesando año {año}: {e}")
            continue
    
    print("\n>> [EXTRACTION VENTAS] Descarga completada")


def normalizar_dataframe(df, año):
    """Normaliza un DataFrame agregando columnas faltantes y reordenando."""
    df_norm = df.copy()
    
    for col in COLUMNAS_MAESTRAS:
        if col not in df_norm.columns:
            df_norm[col] = pd.NA
    
    df_norm['AÑO'] = int(año)
    df_norm = df_norm[COLUMNAS_MAESTRAS]
    
    return df_norm


def normalizar_ventas_unpivot(df):
    """
    Transforma la tabla de ventas de formato ANCHO a formato LARGO.
    Toma columnas como TipoInmueble_1, TipoInmueble_2 y las convierte en filas.
    Mantiene el IdProforma original para cada registro.
    """
    print("   > Iniciando normalización de columnas (Unpivot)...")
    
    # 1. Definimos los prefijos de las columnas repetitivas (Stubs)
    stubs = [
        'T/M', 'TipoInmueble', 'Modelo', 'NroInmueble', 'NroPiso', 'Vista', 
        'PrecioBase', 'PrecioLista', 'DescuentoLista', 'TotalLista', 'PrioridadOC', 'Orden'
    ]
    
    # Verificación de seguridad: asegurarnos que existe la columna llave
    if 'IdProforma' not in df.columns:
        print("   [Warning] No se encontró 'IdProforma'. Saltando transformación unpivot.")
        return df

    # 2. Crear un índice temporal único para la operación wide_to_long
    df = df.copy()
    df['_idx_temp'] = range(len(df))
    
    # 3. Aplicar wide_to_long (Transformación de formato ancho a largo)
    try:
        df_long = pd.wide_to_long(
            df, 
            stubnames=stubs, 
            i=['_idx_temp'], 
            j='Indice_Inmueble', 
            sep='_', 
            suffix=r'\d+'
        ).reset_index()
        
        # 4. Eliminar el índice temporal (ya no lo necesitamos)
        df_long = df_long.drop(columns=['_idx_temp'])
        
        # 5. Limpieza: Eliminar las filas generadas que están vacías
        filas_originales = len(df)
        filas_post_unpivot = len(df_long)
        df_long = df_long.dropna(subset=['TipoInmueble'])
        # También aseguramos que no sea string vacío si aplica
        df_long = df_long[df_long['TipoInmueble'] != '']
        filas_finales = len(df_long)
        
        print(f"   > Filas originales: {filas_originales}")
        print(f"   > Filas después de unpivot: {filas_post_unpivot} (incluye vacíos)")
        print(f"   > Filas finales (reales): {filas_finales}")
        print(f"   > IdProforma mantenido correctamente en cada fila.")
        return df_long
        
    except Exception as e:
        print(f"   [Error] Falló la normalización unpivot: {e}")
        return df


def process_ventas_data():
    """Normaliza y consolida los reportes de ventas. Retorna DataFrame consolidado."""
    print("\n>> [TRANSFORMATION VENTAS] Normalizando y consolidando datos de ventas...")
    
    dataframes = {}
    
    for año in AÑOS_VENTAS:
        # Buscar archivo CSV o XLSX
        ruta_csv = os.path.join(DOWNLOAD_DIR_VENTAS, f"ReporteVenta{año}.csv")
        ruta_xlsx = os.path.join(DOWNLOAD_DIR_VENTAS, f"ReporteVenta{año}.xlsx")
        
        ruta = None
        if os.path.exists(ruta_csv):
            ruta = ruta_csv
        elif os.path.exists(ruta_xlsx):
            ruta = ruta_xlsx
        
        if not ruta:
            print(f"   ️  Archivo no encontrado: ReporteVenta{año}.[csv/xlsx]")
            continue
        
        try:
            if ruta.endswith('.csv'):
                df = pd.read_csv(ruta, encoding='utf-8', low_memory=False)
            else:
                df = pd.read_excel(ruta)
            
            inmuebles = [col for col in df.columns if col.startswith('T/M_')]
            print(f"    {año}: {len(df):,} filas, {len(df.columns)} cols, {len(inmuebles)} inmuebles")
            dataframes[str(año)] = df
        except Exception as e:
            print(f"    Error al cargar {año}: {e}")
            continue
    
    if not dataframes:
        print("   !! No se encontraron archivos de ventas para procesar")
        return None
    
    print(f"\n    Archivos cargados: {len(dataframes)}/{len(AÑOS_VENTAS)}")
    
    # Normalizar
    print("\n    Normalizando datos...")
    dfs_normalizados = {}
    for año, df in dataframes.items():
        df_norm = normalizar_dataframe(df, año)
        dfs_normalizados[año] = df_norm
        print(f"    {año}: Normalizado ({len(df_norm)} filas, {len(df_norm.columns)} cols)")
    
    # Consolidar
    print("\n    Consolidando datos...")
    df_consolidado = pd.concat(dfs_normalizados.values(), ignore_index=True)
    print(f"    Total filas: {len(df_consolidado):,}")
    print(f"    Total columnas: {len(df_consolidado.columns)}")
    print(f"    Años: {sorted(df_consolidado['AÑO'].unique().tolist())}")
    
    # Aplicar transformación Unpivot (formato ancho a largo)
    print("\n    Aplicando transformación Unpivot (ancho -> largo)...")
    df_consolidado = normalizar_ventas_unpivot(df_consolidado)
    print(f"    Total filas después de unpivot: {len(df_consolidado):,}")

    # Usar TotalLista (precio individual por ítem) como PrecioVenta
    # El PrecioVenta global es el total del combo y se duplica en todas las filas
    # TotalLista_n es el precio correcto de cada ítem después del descuento
    if "TotalLista" in df_consolidado.columns:
        mask = df_consolidado["TotalLista"].notna() & (df_consolidado["TotalLista"] != 0) & (df_consolidado["TotalLista"] != "")
        try:
            df_consolidado.loc[mask, "PrecioVenta"] = pd.to_numeric(df_consolidado.loc[mask, "TotalLista"], errors="coerce")
            print("    Precios individuales por ítem aplicados desde TotalLista.")
        except Exception as e:
            print(f"    [Warning] No se pudo aplicar TotalLista a PrecioVenta: {e}")

    # Convertir precios a soles usando TC histórico por fecha de cada registro
    if "PrecioVenta" in df_consolidado.columns and "TipoMoneda" in df_consolidado.columns:
        print("\n    Convirtiendo precios a soles con TC histórico por fecha...")
        # Usar FechaVenta si existe, sino FechaEntrega_Minuta
        col_fecha_usar = None
        if "FechaVenta" in df_consolidado.columns:
            col_fecha_usar = "FechaVenta"
        elif "FechaEntrega_Minuta" in df_consolidado.columns:
            col_fecha_usar = "FechaEntrega_Minuta"
        df_consolidado = convertir_precios_a_soles(
            df_consolidado, "PrecioVenta", "TipoMoneda", col_fecha=col_fecha_usar
        )
    
    return df_consolidado


def process_stock_data(df_ventas=None):
    """ETL de Stock con FORMATO VISUAL MEJORADO. Incluye pestaña VENTAS si se proporciona."""
    print("\n>> [TRANSFORMATION STOCK] Procesando lógica de negocio y aplicando formatos...")
    
    list_of_files = glob.glob(os.path.join(DOWNLOAD_DIR, '*.xlsx'))
    if not list_of_files:
        raise Exception("No se encontró el archivo Excel en la carpeta de stock.")
        
    latest_file = max(list_of_files, key=os.path.getctime)
    
    try:
        df = pd.read_excel(latest_file)
        df.columns = df.columns.str.strip() 
        print(f"   -> Filas leídas: {len(df)}")
    except Exception as e:
        raise Exception(f"Error abriendo Excel descargado: {e}")
    
    # --- FILTROS DE NEGOCIO ---
    if 'Proyecto' in df.columns:
        df = df[df['Proyecto'].str.upper().isin(TARGET_PROJECTS)]
        
    print(f"   -> Filas tras filtros: {len(df)}")
    
    # Convertir precios a soles usando TC histórico por fecha
    if "Moneda" in df.columns:
        col_fecha_stock = None
        if "FechaSepDefinitiva" in df.columns:
            col_fecha_stock = "FechaSepDefinitiva"
        elif "FechaVenta" in df.columns:
            col_fecha_stock = "FechaVenta"
        if "PrecioVenta" in df.columns:
            df = convertir_precios_a_soles(df, "PrecioVenta", "Moneda", col_fecha=col_fecha_stock)
        elif "PrecioLista" in df.columns:
            df = convertir_precios_a_soles(df, "PrecioLista", "Moneda", col_fecha=col_fecha_stock)
    

    output_filename = os.path.join(DOWNLOAD_DIR, f"Reporte_Stock_BIEVO25_{datetime.now().strftime('%Y%m%d')}.xlsx")
    
    try:
        writer = pd.ExcelWriter(output_filename, engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name='Stock')
        
        workbook = writer.book
        worksheet = writer.sheets['Stock']
        (max_row, max_col) = df.shape
        
        fmt_base = workbook.add_format({'font_name': 'Arial', 'font_size': 9})
        fmt_header = workbook.add_format({
            'bold': True, 'font_name': 'Arial', 'font_size': 9,
            'bg_color': '#D9D9D9', 'border': 1, 'align': 'center', 'valign': 'vcenter'
        })
        fmt_currency = workbook.add_format({
            'num_format': '"S/" #,##0.00', 'font_name': 'Arial', 'font_size': 9
        })
        fmt_decimal = workbook.add_format({
            'num_format': '0.00', 'font_name': 'Arial', 'font_size': 9
        })

        for i, col in enumerate(df.columns):
            worksheet.write(0, i, col, fmt_header)
            col_upper = col.upper()
            
            if 'PRECIO' in col_upper or 'MONTO' in col_upper or 'CUOTA' in col_upper or 'IMPORTE' in col_upper:
                worksheet.set_column(i, i, 16, fmt_currency)
            elif 'AREA' in col_upper:
                worksheet.set_column(i, i, 12, fmt_decimal)
            elif 'PROYECTO' in col_upper:
                worksheet.set_column(i, i, 25, fmt_base)
            else:
                worksheet.set_column(i, i, 15, fmt_base)

        if max_row > 0:
            options = {
                'columns': [{'header': col} for col in df.columns],
                'style': 'Table Style Medium 2',
                'name': 'TablaStock'
            }
            worksheet.add_table(0, 0, max_row, max_col - 1, options)
        
        # --- AGREGAR PESTAÑA VENTAS SI EXISTE DATA ---
        if df_ventas is not None and len(df_ventas) > 0:
            print("\n   -> Agregando pestaña VENTAS al archivo...")
            df_ventas.to_excel(writer, index=False, sheet_name='VENTAS')
            
            worksheet_ventas = writer.sheets['VENTAS']
            (max_row_v, max_col_v) = df_ventas.shape
            
            # Formato numérico SIN símbolo de moneda para VENTAS (hay diferentes monedas)
            fmt_number_ventas = workbook.add_format({
                'num_format': '#,##0.00', 'font_name': 'Arial', 'font_size': 9
            })
            
            # Aplicar formatos a pestaña VENTAS
            for i, col in enumerate(df_ventas.columns):
                worksheet_ventas.write(0, i, col, fmt_header)
                col_upper = col.upper()
                
                # SIN símbolo S/. - solo formato numérico porque hay diferentes monedas
                if 'PRECIO' in col_upper or 'MONTO' in col_upper or 'CUOTA' in col_upper or 'IMPORTE' in col_upper or 'SUBTOTAL' in col_upper or 'DESCUENTO' in col_upper or 'TOTAL' in col_upper:
                    worksheet_ventas.set_column(i, i, 16, fmt_number_ventas)
                elif 'AREA' in col_upper:
                    worksheet_ventas.set_column(i, i, 12, fmt_decimal)
                elif 'PROYECTO' in col_upper:
                    worksheet_ventas.set_column(i, i, 25, fmt_base)
                elif 'AÑO' in col_upper:
                    worksheet_ventas.set_column(i, i, 8, fmt_base)
                else:
                    worksheet_ventas.set_column(i, i, 15, fmt_base)
            
            if max_row_v > 0:
                options_ventas = {
                    'columns': [{'header': col} for col in df_ventas.columns],
                    'style': 'Table Style Medium 2',
                    'name': 'TablaVentas'
                }
                worksheet_ventas.add_table(0, 0, max_row_v, max_col_v - 1, options_ventas)
            
            print(f"    Pestaña VENTAS agregada ({len(df_ventas):,} filas)")
        
        writer.close()
        return output_filename

    except Exception as e:
        print(f"!! Error aplicando formato (guardando simple): {e}")
        df.to_excel(output_filename, index=False)
        return output_filename


def dispatch_report(file_path):
    """Envío Correo con el reporte consolidado (Stock + Ventas)."""
    print("\n>> [DISTRIBUTION] Enviando correo...")
    
    msg = MIMEMultipart()
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_TO
    msg['Subject'] = f"REPORTE STOCK COMERCIAL - BIEVO25 - {datetime.now().strftime('%d/%m/%Y')}"
    
    body = f"""
    <html><body>
        <h3>Reporte Automatizado de Stock</h3>
        <p>Adjunto reporte actualizado al {datetime.now().strftime('%d/%m/%Y %H:%M')}.</p>
        <p>El archivo contiene las siguientes pestañas:</p>
        <ul>
            <li><b>Stock:</b> Información actualizada de inventario comercial</li>
            <li><b>VENTAS:</b> Histórico consolidado de ventas 2021-2026</li>
        </ul>
    </body></html>
    """
    msg.attach(MIMEText(body, 'html'))
    
    # Adjuntar reporte único
    with open(file_path, 'rb') as f:
        part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(file_path)}"')
        msg.attach(part)

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_FROM, EMAIL_PASS)
            smtp.send_message(msg)
        print(">> [SUCCESS] Correo enviado.")
    except Exception as e:
        print(f"!! Error SMTP: {e}")


def upload_to_gsheets(df_ventas, df_stock):
    """Sube los datos al Google Sheet para el dashboard."""
    print("\n>> [GOOGLE SHEETS] Actualizando dashboard...")
    
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceCredentials.from_service_account_file(GSHEETS_CREDENTIALS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(GSHEETS_SPREADSHEET_ID)
        
        # Pestaña VENTAS
        if df_ventas is not None and len(df_ventas) > 0:
            try:
                ws_ventas = spreadsheet.worksheet("VENTAS")
                ws_ventas.clear()
            except:
                ws_ventas = spreadsheet.add_worksheet(title="VENTAS", rows=10000, cols=200)
            
            def clean_df_for_sheets(df):
                import math
                def _clean(x):
                    if x is None:
                        return ""
                    try:
                        if pd.isna(x):
                            return ""
                    except (TypeError, ValueError):
                        pass
                    if isinstance(x, float) and (x != x or x == float('inf') or x == float('-inf')):
                        return ""
                    return str(x)
                result = []
                for col in df.columns:
                    result.append(df[col].apply(_clean))
                return pd.concat(result, axis=1)
            df_v = clean_df_for_sheets(df_ventas)
            data_ventas = [df_v.columns.tolist()] + df_v.values.tolist()
            ws_ventas.update(data_ventas, value_input_option="RAW")
            print(f"   -> [OK] Pestaña VENTAS actualizada ({len(df_ventas)} filas)")
        
        # Pestaña STOCK
        if df_stock is not None and len(df_stock) > 0:
            try:
                ws_stock = spreadsheet.worksheet("STOCK")
                ws_stock.clear()
            except:
                ws_stock = spreadsheet.add_worksheet(title="STOCK", rows=10000, cols=100)
            
            df_s = clean_df_for_sheets(df_stock)
            data_stock = [df_s.columns.tolist()] + df_s.values.tolist()
            ws_stock.update(data_stock, value_input_option="RAW")
            print(f"   -> [OK] Pestaña STOCK actualizada ({len(df_stock)} filas)")
        
        print(f"   -> Dashboard listo: https://docs.google.com/spreadsheets/d/{GSHEETS_SPREADSHEET_ID}")
        return True
        
    except Exception as e:
        print(f"!! GOOGLE SHEETS ERROR: {e}")
        traceback.print_exc()
        return False


def main():
    print("="*70)
    print("   PIPELINE ETL EVOLTA - STOCK Y VENTAS")
    print(f"   Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Limpiar directorios
    clean_environment(DOWNLOAD_DIR, "*.xlsx")
    clean_environment(DOWNLOAD_DIR_VENTAS, "*.csv")
    clean_environment(DOWNLOAD_DIR_VENTAS, "*.xlsx")
    
    # Inicializar driver (directorio de descarga principal para Stock)
    driver = get_driver(DOWNLOAD_DIR)
    wait = WebDriverWait(driver, 30)
    
    final_file = None
    df_ventas = None
    
    try:
        # 1. LOGIN (único)
        robust_login(driver, wait)
        
        # 2. EXTRAER STOCK
        execute_stock_extraction(driver)
        
        # 3. Cambiar directorio de descarga para Ventas
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": os.path.abspath(DOWNLOAD_DIR_VENTAS)
        })
        # Forzar preferencia de Chrome también
        driver.execute_script("""
            if (window.chrome && chrome.send) {
                chrome.send('setDownloadBehavior', {behavior: 'allow', downloadPath: arguments[0]});
            }
        """, os.path.abspath(DOWNLOAD_DIR_VENTAS))
        
        # 4. EXTRAER VENTAS (misma sesión)
        execute_ventas_extraction(driver)
        
    except Exception as e:
        print(f"!! CRITICAL ERROR: {e}")
    finally:
        driver.quit()
    
    # 5. PROCESAR DATOS DE VENTAS (consolidar)
    try:
        df_ventas = process_ventas_data()
    except Exception as e:
        print(f"!! DATA ERROR (Ventas): {e}")
        df_ventas = None
    
    # 6. PROCESAR DATOS DE STOCK (incluye pestaña VENTAS)
    try:
        final_file = process_stock_data(df_ventas)
    except Exception as e:
        print(f"!! DATA ERROR (Stock): {e}")
    
    # 7. ENVIAR CORREO (un solo archivo)
    if final_file:
        try:
            dispatch_report(final_file)
        except Exception as e:
            print(f"!! EMAIL ERROR: {e}")
        
        # 8. COPIAR A ONEDRIVE (solo en Windows local)
        if not IS_CLOUD and ONEDRIVE_OUTPUT_DIR:
            try:
                import shutil
                os.makedirs(ONEDRIVE_OUTPUT_DIR, exist_ok=True)
                onedrive_path = os.path.join(ONEDRIVE_OUTPUT_DIR, ONEDRIVE_FILE_NAME)
                shutil.copy2(final_file, onedrive_path)
                print(f"\n>> [ONEDRIVE] Reporte copiado exitosamente:")
                print(f"   -> {onedrive_path}")
            except Exception as e:
                print(f"!! ONEDRIVE ERROR: {e}")
        elif IS_CLOUD:
            print("\n>> [ONEDRIVE] Saltando copia OneDrive (entorno nube)")

        # 9. SUBIR A GOOGLE SHEETS para dashboard
        try:
            # Leer el stock desde el archivo generado (funciona en nube y local)
            df_stock_gs = None
            if final_file and os.path.exists(final_file):
                df_stock_gs = pd.read_excel(final_file, sheet_name='Stock')
            upload_to_gsheets(df_ventas, df_stock_gs)
        except Exception as e:
            print(f"!! GSHEETS ERROR: {e}")
    else:
        print("!! No hay reporte para enviar")
    
    print("\n" + "="*70)
    print("   PIPELINE COMPLETADO")
    print("="*70)


if __name__ == "__main__":
    main()