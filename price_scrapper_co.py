# -*- coding: utf-8 -*-
"""
Scraper de precios online - VERSIÓN FINAL UNIFICADA
Busca un EAN en 8 farmacias, manejando errores de carga de página y múltiples resultados.
Adaptado 08-sep-2025
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

import csv, time, re, os
from os.path import exists
from datetime import datetime
from pathlib import Path

# --------------------------------------------------------------------
# --- PARÁMETROS DE BÚSQUEDA ---
# --------------------------------------------------------------------
EAN_A_BUSCAR = "7702418006430"
# Se añade "pasteur_co" a la lista final de farmacias.
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
def scrapper_general(pagina, ean_producto, driver):
    config = SITE_CONFIG[pagina]
    url = config["search_url"].format(q=ean_producto)
    
    try:
        print(f"\n--- SCRAPEANDO {pagina.upper()} ---")
        print(f"-> Navegando a la URL...")
        driver.get(url)

        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        print("  -> Página cargada. Esperando scripts...")
        time.sleep(5)
        handle_popups(driver, pagina)

        wait = WebDriverWait(driver, 20)
        datos = {}

        if pagina in ["larebaja_co", "locatel_co", "olimpica_co"]:
            print(f"  -> Estrategia: Página de producto directa. Esperando elemento clave...")
            nombre_selector = config['selectors']['nombre']
            wait.until(EC.presence_of_element_located((nombre_selector['by'], nombre_selector['value'])))
            print("  -> ¡Elemento clave encontrado! Extrayendo datos...")
            if pagina == "larebaja_co": datos = extraer_datos_larebaja(driver, config['selectors'])
            elif pagina == "locatel_co": datos = extraer_datos_locatel(driver, config['selectors'])
            elif pagina == "olimpica_co": datos = extraer_datos_olimpica(driver, config['selectors'])
        
        # Lógica de búsqueda en lista para Pasteur
        elif pagina == "pasteur_co":
            print("  -> Estrategia: Búsqueda en lista de resultados.")
            container_selector = config['container']
            wait.until(EC.presence_of_all_elements_located((container_selector['by'], container_selector['value'])))
            all_products = driver.find_elements(container_selector['by'], container_selector['value'])
            print(f"  -> Encontrados {len(all_products)} productos. Verificando...")
            
            for item in all_products:
                nombre_temp = _safe_find(item, config['selectors']['nombre']['by'], config['selectors']['nombre']['value'])
                if nombre_temp and nombre_temp.lower().startswith("anemidox"):
                    print(f"  -> ✓ Producto '{nombre_temp}' encontrado. Extrayendo sus datos...")
                    datos = extraer_datos_pasteur(item, config['selectors'])
                    break
                else:
                    print(f"  -> ✗ Producto '{nombre_temp}' no coincide. Saltando...")
            
            if not datos:
                print(f"\n  -> ERROR: No se encontró el producto 'Anemidox' en los resultados de {pagina}.")
                driver.save_screenshot(f'{pagina}_error_no_encontrado.png')
                return

        # Lógica general para las demás farmacias (lista de resultados)
        else:
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

        nombre = datos.get("nombre", "N/A")
        marca = datos.get("marca", "N/A")
        
        if pagina == "cafam_co":
            p_online = normalizar_precio_cafam(datos.get("p_online", "0"))
            p_normal = normalizar_precio_cafam(datos.get("p_normal", "0"))
        else:
            p_online = normalizar_precio(datos.get("p_online", "0"))
            p_normal = normalizar_precio(datos.get("p_normal", "0"))

        if p_online > 0 and p_normal == 0: p_normal = p_online
        if p_online > p_normal and p_normal > 0: p_online, p_normal = p_normal, p_online
        
        stock  = "Disponible" if p_online > 0 else "No Disponible"

        print(f"    - Nombre: {nombre}")
        print(f"    - Marca: {marca}")
        print(f"    - Precio Online: {p_online}")
        print(f"    - Precio Normal: {p_normal}")
        print(f"    - Stock: {stock}")

        if nombre and nombre != "N/A" and p_online > 0:
            write_to_csv(filename, [pagina, ean_producto, nombre, marca, p_online, p_normal, stock, fecha_hoy_fmt()])
            print(f"    -> ✓ Producto guardado en CSV")
        else:
            print(f"    -> ✗ Producto no guardado (nombre no encontrado o precio es cero)")

    except TimeoutException:
        print(f"\n  -> ERROR: Tiempo de espera agotado en {pagina}. Puede ser por carga lenta o porque no se encontró el producto.")
        driver.save_screenshot(f'{pagina}_error.png')
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
    # opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--log-level=3")
    opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.page_load_strategy = 'eager'

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(40)

    filename = "Precios_Farmacias_Unificado_Final.csv"
    if exists(filename): os.remove(filename)

    print("--- INICIANDO SCRAPER UNIFICADO ---")
    try:
        for farmacia in FARMACIAS_A_BUSCAR:
            scrapper_general(farmacia, EAN_A_BUSCAR, driver)
    except Exception as e:
        print(f"\nERROR INESPERADO: {e}")
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