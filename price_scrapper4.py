# -*- coding: utf-8 -*-
"""
Scraper de precios online - VERSIÓN MÚLTIPLES PRODUCTOS
Busca una lista de EANs en 8 farmacias, manejando errores, múltiples resultados
y validaciones específicas por farmacia.
Adaptado 09-sep-2025
"""

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

import csv, time, re, os, unicodedata # <--- Se añade la librería unicodedata
from os.path import exists
from datetime import datetime

# --------------------------------------------------------------------
# --- PARÁMETROS DE BÚSQUEDA ---
# --------------------------------------------------------------------
# Diccionario con los productos a buscar. Formato: "EAN": "palabra_clave"
PRODUCTOS_A_BUSCAR = {
    "7702418006430": "anemidox", "7500435174527": "anemidox", "7702418002708": "anemidox", "7702418002715": "anemidox",
    "4054839106644": "bion 3", "4054839084621": "bion 3", "7500435227018": "bion 3",
    "7702418000100": "cebion", "7702418000117": "cebion", "7702418001640": "cebion", "7702418001657": "cebion",
    "7702418000742": "cebion", "7702418000810": "cebion", "7702418000834": "cebion", "7702418000926": "cebion",
    "7702418004795": "cebion", "7702418006140": "cebion", "7500435197397": "cebion", "7702418004528": "cebion",
    "7500435249232": "cebion", "7702418005754": "cebion", "7500435249249": "cebion", "7702418004672": "cebion",
    "7702418004696": "cebion", "7702418004702": "cebion",
    "7506339350890": "metamucil", "7506339350906": "metamucil", "7500435131377": "metamucil",
    "7702418006478": "nasivin", "7702418006485": "nasivin", "7702418006492": "nasivin",
    "7702418000414": "nenedent",
    "7501298217536": "neurobion", "7702418006089": "neurobion", "7702418004351": "neurobion", "7702418006249": "neurobion",
    "75916565": "vick", "7500435170857": "vick", "7500435107013": "vick", "7500435181068": "vick",
    "7500435151184": "vick", "7500435159012": "vick", "7501001153182": "vick", "7501001280031": "vick",
    "7500435204576": "vick", "7500435204583": "vick", "7500435225465": "vick", "7500435246408": "vick",
    "7500435246415": "vick", "7500435246453": "vick", "7500435243292": "vick",
    "4054839015915": "vivera"
}

FARMACIAS_A_BUSCAR = ["cruzverde_co", "farmatodo_co", "larebaja_co", "locatel_co", "colsubsidio_co", "cafam_co", "olimpica_co", "pasteur_co"]
# --------------------------------------------------------------------


# --------------------------------------------------------------------
# 1. UTILIDADES
# --------------------------------------------------------------------
def normalizar_precio(raw_price: str) -> int:
    """Función de normalización general para la mayoría de farmacias."""
    if not raw_price:
        return 0
    return int(re.sub(r'[^0-9]', '', raw_price) or 0)

def normalizar_precio_cafam(raw_price: str) -> int:
    """Normaliza el precio de Cafam, ignorando los centavos después de la coma."""
    main_price_part = raw_price.split(',')[0]
    return int(re.sub(r'[^0-9]', '', main_price_part) or 0)

def fecha_hoy_fmt() -> str:
    return datetime.now().strftime("%d/%m/%Y")

# --- FUNCIÓN DE NORMALIZACIÓN DE TEXTO MEJORADA ---
def normalizar_texto_para_comparacion(texto: str) -> str:
    """
    Normaliza un texto para hacerlo apto para comparaciones flexibles:
    - Convierte a minúsculas.
    - Elimina espacios en blanco.
    - Elimina tildes y diacríticos.
    """
    if not texto:
        return ""
    # Paso 1: Normalización a forma NFKD para separar caracteres base de diacríticos
    texto_nfkd = unicodedata.normalize('NFKD', texto)
    # Paso 2: Filtrar los caracteres que no son diacríticos (combining characters)
    texto_sin_tildes = "".join([c for c in texto_nfkd if not unicodedata.combining(c)])
    # Paso 3: Convertir a minúsculas y eliminar espacios
    return texto_sin_tildes.lower().replace(" ", "")

# --- FUNCIÓN DE VALIDACIÓN ACTUALIZADA ---
def validar_nombre_producto(nombre_producto: str, keyword: str) -> bool:
    """
    Verifica si la palabra clave está en el nombre del producto de forma flexible,
    utilizando la nueva función de normalización de texto.
    Ej: keyword 'Cebión' encontrará 'CEBION' o 'cebion' en el nombre_producto.
    """
    if not nombre_producto or not keyword:
        return False
    # Utiliza la nueva función de normalización para ambas cadenas
    keyword_normalizada = normalizar_texto_para_comparacion(keyword)
    nombre_normalizado = normalizar_texto_para_comparacion(nombre_producto)
    
    return keyword_normalizada in nombre_normalizado

# --------------------------------------------------------------------
# 2. CONFIGURACIÓN DE SITIOS
# --------------------------------------------------------------------
SITE_CONFIG = {
    "cruzverde_co": {
        "search_url": "https://www.cruzverde.com.co/search?query={q}",
        "container": {"by": By.TAG_NAME, "value": "ml-card-product"},
    },
    "farmatodo_co": {
        "search_url": "https://www.farmatodo.com.co/buscar?product={q}",
        "primary_container": {"by": By.CSS_SELECTOR, "value": "div[data-testid='product-card']"},
        "fallback_container": {"by": By.TAG_NAME, "value": "app-new-product-card"},
    },
    "larebaja_co": {
        "search_url": "https://www.larebajavirtual.com/{q}?_q={q}&map=ft",
        "selectors": {
            "nombre": {"by": By.CSS_SELECTOR, "value": "h3.vtex-product-summary-2-x-productNameContainer"},
            "marca": {"by": By.CSS_SELECTOR, "value": "span.vtex-store-components-3-x-productBrandName"},
            "p_online": {"by": By.CSS_SELECTOR, "value": "span.vtex-product-price-1-x-sellingPrice"},
            "p_normal": {"by": By.CSS_SELECTOR, "value": "span.vtex-product-price-1-x-listPrice"}
        }
    },
    "locatel_co": {
        "search_url": "https://www.locatelcolombia.com/{q}?_q={q}&map=ft",
        "selectors": {
            "nombre": {"by": By.CSS_SELECTOR, "value": "h2.vtex-product-summary-2-x-productNameContainer"},
            "p_online": {"by": By.CSS_SELECTOR, "value": "span.vtex-store-components-3-x-sellingPrice"},
            "p_normal": {"by": By.CSS_SELECTOR, "value": "div.vtex-store-components-3-x-listPrice"}
        }
    },
    "colsubsidio_co": {
        "search_url": "https://www.drogueriascolsubsidio.com/{q}",
        "container": {"by": By.CSS_SELECTOR, "value": "div.product-Vitrina-masVendidos"}
    },
    "cafam_co": {
        "search_url": "https://www.drogueriascafam.com.co/#2fce/fullscreen/m=and&q={q}",
        "container": {"by": By.CSS_SELECTOR, "value": "div.dfd-card"},
        "selectors": {
            "nombre": {"by": By.CSS_SELECTOR, "value": "div.dfd-card-title"},
            "p_online": {"by": By.CSS_SELECTOR, "value": "span.dfd-card-special-price"},
            "p_sale": {"by": By.CSS_SELECTOR, "value": "span.dfd-card-price--sale"},
            "p_normal": {"by": By.CSS_SELECTOR, "value": "span.dfd-card-price"}
        }
    },
    "olimpica_co": {
        "search_url": "https://www.olimpica.com/{q}?_q={q}&map=ft",
        "selectors": {
            "nombre": {"by": By.CSS_SELECTOR, "value": "h3.vtex-product-summary-2-x-productNameContainer"},
            "marca": {"by": By.CSS_SELECTOR, "value": "span.vtex-product-summary-2-x-productBrandName"},
            "p_online": {"by": By.CSS_SELECTOR, "value": "div.olimpica-dinamic-flags-0-x-listPrices"},
            "p_normal": {"by": By.CSS_SELECTOR, "value": "span.vtex-product-price-1-x-sellingPrice--summary"}
        }
    },
    "pasteur_co": {
        "search_url": "https://www.farmaciaspasteur.com.co/{q}?_q={q}&map=ft",
        "container": {"by": By.CSS_SELECTOR, "value": "section.vtex-product-summary-2-x-container"},
        "selectors": {
            "nombre": {"by": By.CSS_SELECTOR, "value": "h4.vtex-product-summary-2-x-productNameContainer"},
            "p_online": {"by": By.CSS_SELECTOR, "value": "span.vtex-product-price-1-x-sellingPriceValue"},
            "p_normal": {"by": By.CSS_SELECTOR, "value": "span.vtex-product-price-1-x-listPriceValue"}
        }
    }
}

# --------------------------------------------------------------------
# 3. FUNCIONES DE EXTRACCIÓN ESPECIALIZADAS
# --------------------------------------------------------------------
def _safe_find(element, by, value):
    try: 
        return element.find_element(by, value).text.strip()
    except NoSuchElementException: 
        return None

def extraer_datos_olimpica(driver, selectors):
    nombre = _safe_find(driver, selectors['nombre']['by'], selectors['nombre']['value']) or "N/A"
    marca = _safe_find(driver, selectors['marca']['by'], selectors['marca']['value']) or "N/A"
    p_online = _safe_find(driver, selectors['p_online']['by'], selectors['p_online']['value'])
    p_normal = _safe_find(driver, selectors['p_normal']['by'], selectors['p_normal']['value'])
    if not p_online: p_online = p_normal
    if not p_normal: p_normal = p_online
    p_online = p_online or "0"
    p_normal = p_normal or "0"
    return {"nombre": nombre, "marca": marca, "p_online": p_online, "p_normal": p_normal}

def extraer_datos_pasteur(item, selectors):
    nombre = _safe_find(item, selectors['nombre']['by'], selectors['nombre']['value']) or "N/A"
    marca = nombre.split()[0] if nombre != "N/A" else "N/A"
    p_online = _safe_find(item, selectors['p_online']['by'], selectors['p_online']['value'])
    p_normal = _safe_find(item, selectors['p_normal']['by'], selectors['p_normal']['value'])
    if not p_normal: p_normal = p_online
    p_online = p_online or "0"
    p_normal = p_normal or "0"
    return {"nombre": nombre, "marca": marca, "p_online": p_online, "p_normal": p_normal}

def extraer_datos_cruzverde(item):
    nombre_bruto, fabricante, marca, p_online, p_normal = "N/A", "N/A", "N/A", "0", "0"
    try:
        elements = item.find_elements(By.TAG_NAME, "div") + item.find_elements(By.TAG_NAME, "p")
        candidatos_nombre = [elem.text.strip() for elem in elements if elem.text.strip() and len(elem.text.strip()) > 10 and "$" not in elem.text]
        if candidatos_nombre: nombre_bruto = max(candidatos_nombre, key=len)
    except: pass
    try:
        fabricante = item.find_element(By.CSS_SELECTOR, "div.italic").text.strip()
    except: pass
    
    nombre_limpio = nombre_bruto.replace(fabricante, "").replace('\n', ' ').strip()
    if nombre_limpio != "N/A":
        marca = nombre_limpio.split()[0]

    try:
        elements = item.find_elements(By.TAG_NAME, "span") + item.find_elements(By.TAG_NAME, "p")
        for elem in elements:
            text = elem.text.strip()
            if "$" in text and len(re.sub(r'[^0-9]', '', text)) >= 4:
                if "Normal" in text: p_normal = text
                elif p_online == "0": p_online = text
    except: pass
    return {"nombre": nombre_limpio, "marca": marca, "p_online": p_online, "p_normal": p_normal}

def extraer_datos_farmatodo(item):
    marca = _safe_find(item, By.CSS_SELECTOR, "p.text-brand") or "N/A"
    nombre = _safe_find(item, By.CSS_SELECTOR, "p.text-title") or "N/A"
    p_online = _safe_find(item, By.CSS_SELECTOR, "span.price__text-price") or "0"
    p_normal = _safe_find(item, By.CSS_SELECTOR, "span.price__text-offer-price") or "0"
    return {"nombre": nombre, "marca": marca, "p_online": p_online, "p_normal": p_normal}

def extraer_datos_larebaja(driver, selectors):
    nombre = _safe_find(driver, selectors['nombre']['by'], selectors['nombre']['value']) or "N/A"
    marca = _safe_find(driver, selectors['marca']['by'], selectors['marca']['value']) or "N/A"
    p_online = _safe_find(driver, selectors['p_online']['by'], selectors['p_online']['value']) or "0"
    p_normal = _safe_find(driver, selectors['p_normal']['by'], selectors['p_normal']['value']) or p_online
    return {"nombre": nombre, "marca": marca, "p_online": p_online, "p_normal": p_normal}

def extraer_datos_locatel(driver, selectors):
    nombre = _safe_find(driver, selectors['nombre']['by'], selectors['nombre']['value']) or "N/A"
    marca = nombre.split()[0] if nombre != "N/A" else "N/A"
    p_online = _safe_find(driver, selectors['p_online']['by'], selectors['p_online']['value']) or "0"
    p_normal = _safe_find(driver, selectors['p_normal']['by'], selectors['p_normal']['value']) or p_online
    return {"nombre": nombre, "marca": marca, "p_online": p_online, "p_normal": p_normal}

def extraer_datos_colsubsidio(item):
    nombre = _safe_find(item, By.CSS_SELECTOR, "p.dataproducto-nameProduct") or "N/A"
    marca = nombre.split()[0] if nombre != "N/A" else "N/A"
    p_online = _safe_find(item, By.CSS_SELECTOR, "p.dataproducto-bestPrice") or "0"
    p_normal_raw = _safe_find(item, By.CSS_SELECTOR, "div.precioTachadoVitrina")
    p_normal = p_normal_raw if p_normal_raw else p_online
    return {"nombre": nombre, "marca": marca, "p_online": p_online, "p_normal": p_normal}

def extraer_datos_cafam(item):
    selectors = SITE_CONFIG["cafam_co"]["selectors"]
    nombre = _safe_find(item, selectors['nombre']['by'], selectors['nombre']['value']) or "N/A"
    marca = nombre.split()[0] if nombre != "N/A" else "N/A"
    p_online_raw = (_safe_find(item, selectors['p_online']['by'], selectors['p_online']['value']) or
                    _safe_find(item, selectors['p_sale']['by'], selectors['p_sale']['value']))
    p_normal_raw = _safe_find(item, selectors['p_normal']['by'], selectors['p_normal']['value'])
    p_online = p_online_raw if p_online_raw else p_normal_raw or "0"
    p_normal = p_normal_raw if p_normal_raw else p_online
    return {"nombre": nombre, "marca": marca, "p_online": p_online, "p_normal": p_normal}

# --------------------------------------------------------------------
# 4. MANEJO DE POP-UPS
# --------------------------------------------------------------------
def handle_popups(driver, pagina):
    print("  -> Buscando y cerrando pop-ups...")
    if pagina == "cruzverde_co":
        try:
            wait = WebDriverWait(driver, 3)
            bogota_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Bogot')]")))
            bogota_button.click()
            print("    -> Pop-up de ubicación de Cruz Verde cerrado.")
            time.sleep(1)
            return
        except: pass
    elif pagina == "cafam_co":
        try:
            wait = WebDriverWait(driver, 10)
            close_button = wait.until(EC.presence_of_element_located((By.ID, "popupbasic-close")))
            driver.execute_script("arguments[0].click();", close_button)
            print("    -> ✓ Pop-up de publicidad cerrado usando JavaScript.")
            time.sleep(1)
            return
        except:
            print("    -> No se encontró el popup inicial, continuando...")
            
    try:
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        print("    -> Intento de cierre con tecla ESC.")
        time.sleep(1)
    except: pass

# --------------------------------------------------------------------
# 5. ESCRITURA CSV
# --------------------------------------------------------------------
def write_to_csv(filename, fila):
    header = ['Farmacia', 'EAN', 'Product Name', 'Brand', 'Sale Price', 'Old Price', 'Stock', 'Fecha']
    file_exists = exists(filename) and os.path.getsize(filename) > 0
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        if not file_exists: w.writerow(header)
        w.writerow(fila)

# --------------------------------------------------------------------
# 6. SCRAPER GENERAL
# --------------------------------------------------------------------
def scrapper_general(pagina, ean_producto, keyword, driver):
    config = SITE_CONFIG[pagina]
    url = config["search_url"].format(q=ean_producto)
    datos = {}
    
    try:
        print(f"\n--- SCRAPEANDO {pagina.upper()} PARA EAN {ean_producto} ({keyword}) ---")
        print(f"-> Navegando a la URL...")
        driver.get(url)

        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        print("  -> Página cargada. Esperando scripts...")
        time.sleep(5)
        handle_popups(driver, pagina)

        wait = WebDriverWait(driver, 20)

        if pagina in ["larebaja_co", "locatel_co", "olimpica_co"]:
            print(f"  -> Estrategia: Página de producto directa. Esperando elemento clave...")
            nombre_selector = config['selectors']['nombre']
            wait.until(EC.presence_of_element_located((nombre_selector['by'], nombre_selector['value'])))
            print("  -> ¡Elemento clave encontrado! Extrayendo datos...")
            if pagina == "larebaja_co": datos = extraer_datos_larebaja(driver, config['selectors'])
            elif pagina == "locatel_co": datos = extraer_datos_locatel(driver, config['selectors'])
            elif pagina == "olimpica_co": datos = extraer_datos_olimpica(driver, config['selectors'])
        
        elif pagina == "pasteur_co":
            print("  -> Estrategia: Búsqueda en lista de resultados con verificación de palabra clave.")
            container_selector = config['container']
            wait.until(EC.presence_of_all_elements_located((container_selector['by'], container_selector['value'])))
            all_products = driver.find_elements(container_selector['by'], container_selector['value'])
            print(f"  -> Encontrados {len(all_products)} productos. Verificando con palabra clave: '{keyword}'...")
            
            for item in all_products:
                nombre_temp = _safe_find(item, config['selectors']['nombre']['by'], config['selectors']['nombre']['value'])
                # La lógica de validación ahora usa la función mejorada.
                if validar_nombre_producto(nombre_temp, keyword):
                    print(f"  -> ✓ Producto '{nombre_temp}' COINCIDE. Extrayendo sus datos...")
                    datos = extraer_datos_pasteur(item, config['selectors'])
                    break
                else:
                    print(f"  -> ✗ Producto '{nombre_temp}' no coincide con la palabra clave. Saltando...")
            
            if not datos:
                print(f"\n  -> INFO: No se encontró un producto que contenga '{keyword}' en los resultados de {pagina}.")

        else: # Lógica general para las demás farmacias
            print("  -> Estrategia: Búsqueda en lista de resultados. Buscando contenedor...")
            item = None
            if pagina == "farmatodo_co":
                try:
                    primary_selector = config["primary_container"]
                    item = wait.until(EC.presence_of_element_located((primary_selector['by'], primary_selector['value'])))
                except TimeoutException:
                    fallback_selector = config["fallback_container"]
                    print("    -> Selector primario falló. Intentando con selector de respaldo...")
                    item = wait.until(EC.presence_of_element_located((fallback_selector['by'], fallback_selector['value'])))
            elif pagina == "cafam_co":
                print("  -> Esperando a que el NOMBRE del producto sea visible...")
                nombre_selector = config['selectors']['nombre']
                wait.until(EC.presence_of_element_located((nombre_selector['by'], nombre_selector['value'])))
                container_selector = config["container"]
                item = driver.find_element(container_selector['by'], container_selector['value'])
            else: # Cruz Verde y Colsubsidio
                container_selector = config["container"]
                item = wait.until(EC.presence_of_element_located((container_selector['by'], container_selector['value'])))
            
            print("  -> ¡Contenedor de producto encontrado! Extrayendo datos...")
            if pagina == "cruzverde_co": datos = extraer_datos_cruzverde(item)
            elif pagina == "farmatodo_co": datos = extraer_datos_farmatodo(item)
            elif pagina == "colsubsidio_co": datos = extraer_datos_colsubsidio(item)
            elif pagina == "cafam_co": datos = extraer_datos_cafam(item)

        # -- PROCESAMIENTO Y ESCRITURA DE DATOS --
        if datos:
            nombre = datos.get("nombre", "N/A")
            
            # Verificación obligatoria con la función de validación mejorada.
            if validar_nombre_producto(nombre, keyword):
                print(f"    -> ✓ Verificación de nombre exitosa. El nombre '{nombre}' contiene '{keyword}'.")
                marca = datos.get("marca", "N/A")
                
                if pagina == "cafam_co":
                    p_online = normalizar_precio_cafam(datos.get("p_online", "0"))
                    p_normal = normalizar_precio_cafam(datos.get("p_normal", "0"))
                else:
                    p_online = normalizar_precio(datos.get("p_online", "0"))
                    p_normal = normalizar_precio(datos.get("p_normal", "0"))

                if p_online > 0 and p_normal == 0: p_normal = p_online
                if p_online > p_normal and p_normal > 0: p_online, p_normal = p_normal, p_online
                
                stock = "Disponible" if p_online > 0 else "No Disponible"

                print(f"    - Nombre: {nombre}")
                print(f"    - Marca: {marca}")
                print(f"    - Precio Online: {p_online}")
                print(f"    - Precio Normal: {p_normal}")
                print(f"    - Stock: {stock}")

                if p_online > 0:
                    write_to_csv(filename, [pagina, ean_producto, nombre, marca, p_online, p_normal, stock, fecha_hoy_fmt()])
                    print(f"    -> ✓ Producto guardado en CSV")
                else:
                    print(f"    -> ✗ Producto no guardado (precio es cero). Marcando como No disponible.")
                    write_to_csv(filename, [pagina, ean_producto, nombre, "N/A", 0, 0, "No Disponible", fecha_hoy_fmt()])
            
            else:
                # Si el nombre del producto no contiene la palabra clave, se descarta.
                print(f"    -> ✗ VERIFICACIÓN FALLIDA: El nombre '{nombre}' no contiene la palabra clave '{keyword}'.")
                print(f"    -> Se guardará como 'No disponible'.")
                write_to_csv(filename, [pagina, ean_producto, "No disponible (Nombre no coincide)", "N/A", 0, 0, "No Disponible", fecha_hoy_fmt()])
            
        else:
            print(f"  -> PRODUCTO NO ENCONTRADO en {pagina}. Se guardará como 'No disponible'.")
            write_to_csv(filename, [pagina, ean_producto, "No disponible", "N/A", 0, 0, "No Disponible", fecha_hoy_fmt()])

    except TimeoutException:
        print(f"\n  -> ERROR: Tiempo de espera agotado en {pagina}. El producto se considera 'No disponible'.")
        driver.save_screenshot(f'{pagina}_{ean_producto}_error.png')
        write_to_csv(filename, [pagina, ean_producto, "No disponible", "N/A", 0, 0, "No Disponible", fecha_hoy_fmt()])
        return

# --------------------------------------------------------------------
# 7. CONFIGURAR DRIVER Y EJECUCIÓN
# --------------------------------------------------------------------
if __name__ == "__main__":
    opts = Options()
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    prefs = {"profile.default_content_setting_values.notifications": 2, "profile.default_content_setting_values.geolocation": 2}
    opts.add_experimental_option("prefs", prefs)
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--start-maximized")
    # opts.add_argument("--headless") # Descomentar para ejecución sin interfaz gráfica
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--log-level=3")
    opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.page_load_strategy = 'eager'

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(40)

    filename = "Precios_Farmacias_Unificado_Final.csv"
    if exists(filename): os.remove(filename) # Limpia el archivo al iniciar

    print("--- INICIANDO SCRAPER UNIFICADO ---")
    try:
        for ean, keyword in PRODUCTOS_A_BUSCAR.items():
            for farmacia in FARMACIAS_A_BUSCAR:
                scrapper_general(farmacia, ean, keyword, driver)

    except Exception as e:
        print(f"\nERROR INESPERADO Y FATAL: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()
        print("\n--- SCRAPER FINALIZADO ---")

    if exists(filename) and os.path.getsize(filename) > 0:
        df = pd.read_csv(filename)
        print("\nContenido del archivo CSV generado:")
        print(df.to_string())
    else:
        print("\nNo se guardaron datos en el archivo CSV.")