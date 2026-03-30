"""
Pipeline completo SERCOP
========================
Lee configuración desde .env y ejecuta:
  Paso 1 — Scraping iterativo por rangos de 6 meses hacia atrás
  Paso 2 — Parseo de HTMLs → JSON
  Paso 3 — Visita cada proceso y descarga archivos

Uso:
    python 0_pipeline.py

Requiere:
    pip install selenium requests beautifulsoup4 python-dotenv
"""

import os, sys, time, base64, json, glob, re, requests, urllib3
from datetime import datetime, timedelta
from urllib.parse import unquote
from pathlib import Path
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

load_dotenv(Path(__file__).parent / ".env")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ═══ CONFIGURACIÓN (desde .env) ═══════════════════════════════════
ENTIDAD           = os.getenv("ENTIDAD")
API_KEY_2CAPTCHA  = os.getenv("API_KEY_2CAPTCHA")
URL_BUSQUEDA      = os.getenv("URL_BUSQUEDA")
BASE_URL          = os.getenv("BASE_URL")
CARPETA_HTML      = os.getenv("CARPETA_HTML",      "resultados_sercop")
ARCHIVO_JSON      = os.getenv("ARCHIVO_JSON",      "procesos_sercop.json")
CARPETA_DESCARGAS = os.getenv("CARPETA_DESCARGAS", "archivos_procesos")
INTERVALO_DIAS    = int(os.getenv("INTERVALO_DIAS", 180))
MAX_REINTENTOS_CAPTCHA = 5
# ══════════════════════════════════════════════════════════════════


# ── DRIVER ────────────────────────────────────────────────────────

def iniciar_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")
    return webdriver.Chrome(options=options)


def aceptar_cookies(driver, wait):
    try:
        btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.cc-dismiss")))
        btn.click()
        time.sleep(1)
    except Exception:
        pass


# ── PASO 1: SCRAPING ──────────────────────────────────────────────

def ingresar_entidad(driver, wait, nombre):
    campo = wait.until(EC.presence_of_element_located((By.ID, "txtEntidadContratante")))
    campo.clear()
    campo.send_keys(nombre)
    time.sleep(0.5)


def ingresar_fechas(driver, fecha_desde, fecha_hasta):
    for campo_id, valor in [("f_inicio", fecha_desde), ("f_fin", fecha_hasta)]:
        driver.execute_script(f"""
            var c = document.getElementById('{campo_id}');
            c.removeAttribute('readonly');
            c.value = '{valor}';
            c.dispatchEvent(new Event('change'));
            c.dispatchEvent(new Event('blur'));
        """)
        time.sleep(0.3)


def activar_filtro_estado(driver):
    driver.execute_script("""
        var sel = document.getElementById('txtCodigoTipoCompra');
        sel.value = '386';
        sel.dispatchEvent(new Event('change'));
    """)
    time.sleep(1)
    driver.execute_script("""
        var sel = document.getElementById('txtCodigoTipoCompra');
        sel.value = '';
        sel.dispatchEvent(new Event('change'));
    """)
    time.sleep(1)
    driver.execute_script("""
        var c = document.getElementById('cmbEstado');
        c.value = '476';
        c.dispatchEvent(new Event('change'));
    """)
    time.sleep(0.5)
    print("  ✅ Estado: Finalizada (476)")


def enviar_captcha_2captcha(img_b64):
    """Envía imagen a 2captcha y devuelve taskId."""
    resp = requests.post("https://api.2captcha.com/createTask", json={
        "clientKey": API_KEY_2CAPTCHA,
        "task": {"type": "ImageToTextTask", "body": img_b64, "case": False, "numeric": 0}
    }, verify=False)
    resp.raise_for_status()
    data = resp.json()
    if data.get("errorId") != 0:
        raise RuntimeError(f"2captcha createTask error: {data}")
    return data["taskId"]


def obtener_solucion_2captcha(task_id):
    """Espera y devuelve la solución del captcha."""
    for intento in range(24):
        time.sleep(5)
        res = requests.post("https://api.2captcha.com/getTaskResult", json={
            "clientKey": API_KEY_2CAPTCHA, "taskId": task_id
        }, verify=False)
        resultado = res.json()
        if resultado.get("status") == "ready":
            return resultado["solution"]["text"]
        print(f"     ⏳ Intento {intento+1}/24...")
    raise RuntimeError("Tiempo agotado esperando captcha")


def reportar_captcha_incorrecto(task_id):
    """Notifica a 2captcha que la solución fue incorrecta (mejora el servicio)."""
    try:
        requests.post("https://api.2captcha.com/reportIncorrect", json={
            "clientKey": API_KEY_2CAPTCHA, "taskId": task_id
        }, verify=False)
    except Exception:
        pass


def hay_resultados(driver):
    """Verifica si la búsqueda devolvió resultados."""
    try:
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        div  = soup.find("div", id="divProcesos")
        if not div:
            return False
        tabla = div.find("table")
        if not tabla:
            return False
        filas = tabla.find_all("tr")[1:]
        return any(len(f.find_all("td")) >= 7 for f in filas)
    except Exception:
        return False


def hay_error_captcha(driver):
    """Detecta si el sitio mostró un error de captcha."""
    try:
        texto = driver.page_source.lower()
        return any(p in texto for p in [
            "captcha incorrecto", "código incorrecto", "incorrect captcha",
            "wrong captcha", "captcha inválido", "error en captcha"
        ])
    except Exception:
        return False


def resolver_captcha_con_reintento(driver, wait):
    """
    Captura el captcha, lo envía a 2captcha y lo ingresa.
    Si tras buscar el captcha es incorrecto, reporta el error
    y reintenta hasta MAX_REINTENTOS_CAPTCHA veces.
    Devuelve el task_id de la última solución usada.
    """
    for intento in range(1, MAX_REINTENTOS_CAPTCHA + 1):
        print(f"  ⏳ Captcha intento {intento}/{MAX_REINTENTOS_CAPTCHA}...")

        # Recargar captcha si no es el primer intento
        if intento > 1:
            try:
                reload_btn = driver.find_element(By.ID, "linkReload")
                reload_btn.click()
                time.sleep(2)
            except Exception:
                pass

        img = wait.until(EC.presence_of_element_located((By.ID, "captcha_img")))
        img_b64  = base64.b64encode(img.screenshot_as_png).decode("utf-8")
        task_id  = enviar_captcha_2captcha(img_b64)
        solucion = obtener_solucion_2captcha(task_id)
        print(f"  ✅ Solución: '{solucion}'")

        campo = wait.until(EC.presence_of_element_located((By.ID, "image")))
        campo.clear()
        campo.send_keys(solucion)

        return task_id, solucion

    raise RuntimeError(f"Captcha falló {MAX_REINTENTOS_CAPTCHA} veces")


def hacer_busqueda_con_reintento(driver, wait):
    """
    Resuelve el captcha, hace clic en Buscar y verifica si hay resultados.
    Si el captcha era incorrecto, reporta y reintenta.
    """
    for intento in range(1, MAX_REINTENTOS_CAPTCHA + 1):
        task_id, solucion = resolver_captcha_con_reintento(driver, wait)

        # Buscar
        botones = wait.until(EC.presence_of_all_elements_located((By.ID, "btnBuscar")))
        botones[1].click()
        print("  🔍 Buscando...")
        time.sleep(4)

        # Verificar si el captcha fue rechazado
        if hay_error_captcha(driver):
            print(f"  ❌ Captcha incorrecto (intento {intento}) — reintentando...")
            reportar_captcha_incorrecto(task_id)
            continue

        # Captcha OK (aunque no haya resultados para este rango)
        print("  ✅ Captcha aceptado")
        return

    raise RuntimeError(f"Captcha rechazado {MAX_REINTENTOS_CAPTCHA} veces consecutivas")


def guardar_pagina(html, numero, carpeta, fecha_desde, fecha_hasta):
    os.makedirs(carpeta, exist_ok=True)
    nombre = f"{fecha_desde}_{fecha_hasta}_pagina_{numero:03d}.html"
    ruta   = os.path.join(carpeta, nombre)
    with open(ruta, "w", encoding="utf-8") as f:
        f.write(html)
    return ruta


def hay_siguiente(driver):
    try:
        return driver.find_element(By.XPATH, "//a[normalize-space(text())='Siguiente']")
    except Exception:
        return None


def contar_procesos(html):
    soup = BeautifulSoup(html, "html.parser")
    div  = soup.find("div", id="divProcesos")
    if not div:
        return 0
    tabla = div.find("table")
    if not tabla:
        return 0
    return sum(1 for f in tabla.find_all("tr")[1:] if len(f.find_all("td")) >= 7)


def extraer_rango(driver, wait, fecha_desde_str, fecha_hasta_str):
    print(f"\n  📅 Rango: {fecha_desde_str} → {fecha_hasta_str}")

    driver.get(URL_BUSQUEDA)
    time.sleep(2)
    aceptar_cookies(driver, wait)
    ingresar_entidad(driver, wait, ENTIDAD)
    ingresar_fechas(driver, fecha_desde_str, fecha_hasta_str)
    activar_filtro_estado(driver)
    hacer_busqueda_con_reintento(driver, wait)

    pagina, total = 1, 0
    while True:
        time.sleep(2)
        html = driver.page_source
        n    = contar_procesos(html)
        total += n
        ruta = guardar_pagina(html, pagina, CARPETA_HTML, fecha_desde_str, fecha_hasta_str)
        print(f"    💾 {os.path.basename(ruta)} ({n} procesos)")

        sig = hay_siguiente(driver)
        if sig is None:
            break
        sig.click()
        pagina += 1
        time.sleep(3)

    print(f"  ✅ {pagina} página(s), {total} proceso(s)")
    return total


FECHA_LIMITE = datetime(2020, 1, 1)   # no scrapeamos antes de esta fecha


def rangos_ya_scrapeados():
    """
    Lee los HTMLs existentes en CARPETA_HTML y devuelve un set de
    strings 'fecha_desde_fecha_hasta' ya scrapeados.
    """
    vistos = set()
    for archivo in glob.glob(os.path.join(CARPETA_HTML, "*.html")):
        nombre = os.path.basename(archivo)
        partes = nombre.split("_pagina_")
        if len(partes) == 2:
            vistos.add(partes[0])  # ej: "2025-09-28_2026-03-27"
    return vistos


def paso1_scraping(driver, wait):
    print("\n" + "═"*60)
    print("PASO 1 — SCRAPING ITERATIVO")
    print("═"*60)

    os.makedirs(CARPETA_HTML, exist_ok=True)

    # Detectar rangos ya scrapeados para poder resumir si se cortó
    ya_scrapeados = rangos_ya_scrapeados()
    if ya_scrapeados:
        print(f"  ℹ️  {len(ya_scrapeados)} rango(s) previo(s) detectado(s) — se omitirán")

    fecha_hasta = datetime.today()
    rango_num   = 1

    while fecha_hasta > FECHA_LIMITE:
        fecha_desde = max(
            fecha_hasta - timedelta(days=INTERVALO_DIAS),
            FECHA_LIMITE
        )
        fh_str = fecha_hasta.strftime("%Y-%m-%d")
        fd_str = fecha_desde.strftime("%Y-%m-%d")
        clave  = f"{fd_str}_{fh_str}"

        # Saltar si ya fue scrapeado en una ejecución anterior
        if clave in ya_scrapeados:
            print(f"\n⏭️  Rango #{rango_num} ya scrapeado: {fd_str} → {fh_str} — omitiendo")
            fecha_hasta = fecha_desde - timedelta(days=1)
            rango_num  += 1
            continue

        print(f"\n🔍 Rango #{rango_num}: {fd_str} → {fh_str}")
        try:
            total = extraer_rango(driver, wait, fd_str, fh_str)
            print(f"  ✅ {total} proceso(s) encontrados")
        except Exception as e:
            print(f"  ❌ Error en rango {fd_str}→{fh_str}: {e}")
            print("  ⚠️  El rango se saltará — los datos anteriores están guardados")

        fecha_hasta = fecha_desde - timedelta(days=1)
        rango_num  += 1

    print(f"\n✅ PASO 1 COMPLETADO — llegamos a {FECHA_LIMITE.strftime('%Y-%m-%d')}")


# ── PASO 2: PARSEO ────────────────────────────────────────────────

def parsear_archivo(ruta_html):
    with open(ruta_html, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")
    procesos = []
    div = soup.find("div", id="divProcesos")
    if not div:
        return procesos
    tabla = div.find("table")
    if not tabla:
        return procesos
    for fila in tabla.find_all("tr")[1:]:
        celdas = fila.find_all("td")
        if len(celdas) < 7:
            continue
        enlace = celdas[0].find("a")
        codigo = enlace.get_text(strip=True) if enlace else celdas[0].get_text(strip=True)
        href   = enlace["href"].strip() if enlace and enlace.get("href") else ""
        link   = BASE_URL + href if href and not href.startswith("http") else href
        procesos.append({
            "codigo":              codigo,
            "entidad_contratante": unquote(celdas[1].get_text(strip=True)),
            "objeto_proceso":      celdas[2].get_text(strip=True),
            "estado_proceso":      celdas[3].get_text(strip=True),
            "provincia_canton":    celdas[4].get_text(strip=True),
            "presupuesto":         celdas[5].get_text(strip=True),
            "fecha_publicacion":   celdas[6].get_text(strip=True),
            "link":                link,
        })
    return procesos


def paso2_parseo():
    print("\n" + "═"*60)
    print("PASO 2 — PARSEO DE HTMLs")
    print("═"*60)

    archivos = sorted(set(
        glob.glob(os.path.join(CARPETA_HTML, "**", "*.html"), recursive=True) +
        glob.glob(os.path.join(CARPETA_HTML, "*.html"))
    ))

    if not archivos:
        print(f"❌ No se encontraron HTMLs en '{CARPETA_HTML}/'")
        return 0

    print(f"📂 {len(archivos)} archivo(s)")
    todos = []
    for archivo in archivos:
        p = parsear_archivo(archivo)
        print(f"  {os.path.basename(archivo)} → {len(p)} proceso(s)")
        todos.extend(p)

    vistos, unicos = set(), []
    for p in todos:
        if p["codigo"] not in vistos:
            vistos.add(p["codigo"])
            unicos.append(p)

    print(f"\n📊 {len(todos)} total | {len(todos)-len(unicos)} duplicados | {len(unicos)} únicos")

    with open(ARCHIVO_JSON, "w", encoding="utf-8") as f:
        json.dump(unicos, f, ensure_ascii=False, indent=2)

    print(f"✅ PASO 2 COMPLETADO — {len(unicos)} procesos en '{ARCHIVO_JSON}'")
    return len(unicos)


# ── PASO 3: DESCARGA ──────────────────────────────────────────────

def ir_a_pestaña_archivos(driver, wait):
    pestaña = wait.until(EC.element_to_be_clickable((
        By.XPATH,
        "//ul[@id='tabmenu']//li//a[normalize-space(text())='Archivos' or normalize-space(text())='Archivos ']"
    )))
    driver.execute_script("arguments[0].click();", pestaña)
    time.sleep(2)


def obtener_links_descarga(driver):
    links = driver.find_elements(By.XPATH, "//a[contains(@href,'bajarArchivo.cpe')]")
    resultado = []
    for link in links:
        href = link.get_attribute("href") or ""
        if not href:
            continue
        try:
            fila    = link.find_element(By.XPATH, "./ancestor::tr[1]")
            fila_ant = driver.execute_script("return arguments[0].previousElementSibling;", fila)
            desc    = fila_ant.text.strip() if fila_ant else "archivo"
        except Exception:
            desc = "archivo"
        if href not in [r[1] for r in resultado]:
            resultado.append((desc or "archivo", href))
    return resultado


def descargar_archivo(url, carpeta, nombre_sug, cookies_selenium, i):
    session = requests.Session()
    for c in cookies_selenium:
        session.cookies.set(c["name"], c["value"])
    try:
        resp = session.get(url, timeout=30, stream=True, verify=False)
        resp.raise_for_status()
        cd = resp.headers.get("Content-Disposition", "")
        if "filename=" in cd:
            nombre = cd.split("filename=")[-1].strip().strip('"')
        else:
            ext    = ".pdf" if "pdf" in resp.headers.get("Content-Type", "").lower() else ""
            nombre = f"{i:02d}_{nombre_sug[:60]}{ext}"
        nombre = "".join(c for c in nombre if c not in r'\/:*?"<>|')
        ruta   = os.path.join(carpeta, nombre)
        with open(ruta, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        print(f"      💾 {nombre}")
        return True
    except Exception as e:
        print(f"      ❌ {e}")
        return False


def procesar_proceso_descarga(driver, wait, proceso, carpeta_raiz):
    codigo  = proceso["codigo"].strip().replace("/", "-")
    carpeta = os.path.join(carpeta_raiz, codigo)
    os.makedirs(carpeta, exist_ok=True)
    try:
        driver.get(proceso["link"])
        time.sleep(2)
        aceptar_cookies(driver, wait)
        ir_a_pestaña_archivos(driver, wait)
        links = obtener_links_descarga(driver)
        if not links:
            print("    ℹ️  Sin archivos")
            return 0
        print(f"    📎 {len(links)} archivo(s)")
        cookies = driver.get_cookies()
        n = 0
        for i, (desc, url) in enumerate(links, 1):
            if descargar_archivo(url, carpeta, desc, cookies, i):
                n += 1
            time.sleep(0.5)
        return n
    except Exception as e:
        print(f"    ❌ {e}")
        return 0


def paso3_descargas(driver, wait):
    print("\n" + "═"*60)
    print("PASO 3 — DESCARGA DE ARCHIVOS")
    print("═"*60)

    if not os.path.exists(ARCHIVO_JSON):
        print(f"❌ '{ARCHIVO_JSON}' no encontrado")
        return

    with open(ARCHIVO_JSON, "r", encoding="utf-8") as f:
        procesos = json.load(f)

    print(f"📂 {len(procesos)} procesos")
    os.makedirs(CARPETA_DESCARGAS, exist_ok=True)
    total = 0
    for i, p in enumerate(procesos, 1):
        print(f"\n  [{i}/{len(procesos)}] {p['codigo']} — {p['objeto_proceso'][:50]}...")
        total += procesar_proceso_descarga(driver, wait, p, CARPETA_DESCARGAS)
        time.sleep(1)

    print(f"\n✅ PASO 3 COMPLETADO — {total} archivo(s) descargados")


# ── MAIN ──────────────────────────────────────────────────────────

def main():
    print("\n" + "█"*60)
    print("  PIPELINE SERCOP")
    print("█"*60)

    driver = iniciar_driver()
    wait   = WebDriverWait(driver, 20)

    paso1_ok = False
    try:
        paso1_scraping(driver, wait)
        paso1_ok = True
    except KeyboardInterrupt:
        print("\n⚠️  Scraping interrumpido — continuando con los datos ya guardados...")
    except Exception as e:
        print(f"\n❌ Error en paso 1: {e}")
        import traceback; traceback.print_exc()
        print("⚠️  Continuando con los datos ya guardados...")

    # Pasos 2 y 3 siempre se ejecutan sobre lo que haya
    try:
        n = paso2_parseo()
        if n > 0:
            paso3_descargas(driver, wait)
        else:
            print("\n⚠️  Sin procesos para descargar")
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrumpido")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback; traceback.print_exc()
    finally:
        print("\n" + "█"*60)
        print("  FIN DEL PIPELINE")
        print("█"*60)
        input("\nPresiona ENTER para cerrar...")
        driver.quit()


if __name__ == "__main__":
    main()
