import os
import re
import json
import time
import requests
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────
ARCHIVO_JSON      = os.getenv("ARCHIVO_JSON",      "procesos_sercop.json")
CARPETA_DESCARGAS = os.getenv("CARPETA_DESCARGAS", "archivos_procesos")
NUM_WORKERS       = multiprocessing.cpu_count()   # un hilo por núcleo
# ──────────────────────────────────────────────────────────────────────────────


def iniciar_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless=new")        # headless para paralelizar sin conflictos de ventana
    options.add_argument("--window-size=1280,900")
    options.add_argument("--disable-gpu")
    return webdriver.Chrome(options=options)


def aceptar_cookies(driver, wait):
    try:
        btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.cc-dismiss")))
        btn.click()
        time.sleep(1)
    except Exception:
        pass


# ─── SKIP: proceso ya descargado ─────────────────────────────────────────────

def ya_descargado(codigo, carpeta_raiz):
    """
    Devuelve True si la carpeta del proceso ya existe Y contiene
    al menos un archivo (PDF u otro) además del datos_proceso.json.
    """
    carpeta = os.path.join(carpeta_raiz, codigo.strip().replace("/", "-"))
    if not os.path.isdir(carpeta):
        return False
    archivos = [
        f for f in os.listdir(carpeta)
        if f != "datos_proceso.json"
    ]
    return len(archivos) > 0


# ─── PESTAÑAS ─────────────────────────────────────────────────────────────────

def obtener_pestañas(driver):
    tabs = driver.find_elements(By.XPATH, "//ul[@id='tabmenu']//li//a")
    return [(tab.text.strip(), tab) for tab in tabs if tab.text.strip()]


def click_pestaña(driver, elemento):
    driver.execute_script("arguments[0].click();", elemento)
    time.sleep(2)


# ─── EXTRACTORES ──────────────────────────────────────────────────────────────

def extraer_one_column_emphasis(driver):
    resultado = {}
    try:
        filas = driver.find_elements(By.XPATH, "//table[@id='one-column-emphasis']//tr")
        for fila in filas:
            th = fila.find_elements(By.TAG_NAME, "th")
            td = fila.find_elements(By.TAG_NAME, "td")
            if th and td:
                clave = th[0].text.strip().rstrip(":")
                valor = td[0].text.strip()
                if clave:
                    resultado[clave] = valor
    except Exception as e:
        resultado["_error"] = str(e)
    return resultado


def extraer_rounded_corner(driver, xpath_tabla="//fieldset[@id='cuadro']//table[@id='rounded-corner']"):
    resultado = {"cabeceras": [], "filas": [], "total": None}
    try:
        tabla = driver.find_element(By.XPATH, xpath_tabla)
        cabeceras = []
        for tr in tabla.find_elements(By.XPATH, ".//tr"):
            ths = tr.find_elements(By.TAG_NAME, "th")
            if ths:
                textos = [th.text.strip() for th in ths if th.text.strip()]
                if textos:
                    cabeceras = textos
                    break
        resultado["cabeceras"] = cabeceras
        for tr in tabla.find_elements(By.XPATH, ".//tbody/tr"):
            celdas = tr.find_elements(By.TAG_NAME, "td")
            ths    = tr.find_elements(By.TAG_NAME, "th")
            if not celdas and not ths:
                continue
            texto_fila = tr.text.strip()
            primera = celdas[0] if celdas else ths[0]
            colspan = int(primera.get_attribute("colspan") or 1)
            if "TOTAL" in texto_fila.upper() and (colspan > 1 or len(celdas) + len(ths) < len(cabeceras)):
                resultado["total"] = (celdas or ths)[-1].text.strip()
                continue
            valores = [c.text.strip() for c in celdas]
            if cabeceras and len(valores) == len(cabeceras):
                resultado["filas"].append(dict(zip(cabeceras, valores)))
            elif valores:
                resultado["filas"].append(valores)
    except Exception as e:
        resultado["_error"] = str(e)
    return resultado


def extraer_criterios(driver):
    grupos = []
    try:
        fieldset = driver.find_element(By.XPATH, "//fieldset[@id='cuadro']")
        tablas   = fieldset.find_elements(By.XPATH, ".//table[@id='rounded-corner']")
        for tabla in tablas:
            tipo = ""
            ths_head = tabla.find_elements(By.XPATH, ".//thead//th")
            if ths_head:
                tipo = ths_head[0].text.strip()
            criterios, criterio_act = [], ""
            for tr in tabla.find_elements(By.XPATH, ".//tbody/tr"):
                celdas = tr.find_elements(By.TAG_NAME, "td")
                if len(celdas) == 3:
                    criterio_act = celdas[0].text.strip()
                    criterios.append({"criterio": criterio_act, "descripcion": celdas[1].text.strip(), "puntaje": celdas[2].text.strip()})
                elif len(celdas) == 2:
                    criterios.append({"criterio": criterio_act, "descripcion": celdas[0].text.strip(), "puntaje": celdas[1].text.strip()})
            puntaje_maximo = ""
            try:
                texto_div = driver.execute_script("""
                    var t = arguments[0], sib = t.nextElementSibling;
                    while (sib) {
                        if (sib.tagName === 'DIV' && /\\d/.test(sib.textContent.trim())) return sib.textContent;
                        if (sib.tagName === 'TABLE') break;
                        sib = sib.nextElementSibling;
                    }
                    return '';
                """, tabla)
                m = re.search(r"(\d+)", texto_div or "")
                puntaje_maximo = m.group(1) if m else ""
            except Exception:
                pass
            grupos.append({"tipo": tipo, "puntaje_maximo": puntaje_maximo, "criterios": criterios})
        try:
            div_total = fieldset.find_element(By.XPATH, ".//div[contains(text(),'TOTAL')]")
            m = re.search(r"(\d+)", div_total.text)
            if m:
                grupos.append({"tipo": "TOTAL", "puntaje_maximo": m.group(1), "criterios": []})
        except Exception:
            pass
    except Exception as e:
        grupos.append({"_error": str(e)})
    return grupos


def extraer_parametros_calificacion(driver):
    resultado = {"parametros": [], "total": ""}
    try:
        tabla = driver.find_element(By.XPATH, "//fieldset[@id='cuadro']//table[@id='rounded-corner']")
        for tr in tabla.find_elements(By.XPATH, ".//tbody/tr"):
            tds = tr.find_elements(By.TAG_NAME, "td")
            ths = tr.find_elements(By.TAG_NAME, "th")
            if ths and not tds:
                resultado["total"] = ths[-1].text.strip()
                continue
            if len(tds) >= 3:
                resultado["parametros"].append({
                    "parametro":   tds[0].text.strip(),
                    "descripcion": tds[1].text.strip(),
                    "porcentaje":  tds[2].text.strip(),
                })
    except Exception as e:
        resultado["_error"] = str(e)
    return resultado


def extraer_localidad(driver):
    resultado = {"cabeceras": [], "filas": []}
    try:
        tabla = driver.find_element(By.XPATH, "//div[@id='content']//table[@id='rounded-corner']")
        cabeceras = [th.text.strip() for th in tabla.find_elements(By.XPATH, ".//thead//th") if th.text.strip()]
        resultado["cabeceras"] = cabeceras
        for tr in tabla.find_elements(By.XPATH, ".//tbody/tr"):
            tds    = tr.find_elements(By.TAG_NAME, "td")
            valores = [td.text.strip() for td in tds]
            if cabeceras and len(valores) == len(cabeceras):
                resultado["filas"].append(dict(zip(cabeceras, valores)))
            elif valores:
                resultado["filas"].append(valores)
    except Exception as e:
        resultado["_error"] = str(e)
    return resultado


# ─── ARCHIVOS ─────────────────────────────────────────────────────────────────

def obtener_links_descarga(driver):
    links = driver.find_elements(By.XPATH, "//a[contains(@href,'bajarArchivo.cpe')]")
    resultado = []
    for link in links:
        href = link.get_attribute("href") or ""
        if not href:
            continue
        try:
            fila     = link.find_element(By.XPATH, "./ancestor::tr[1]")
            fila_ant = driver.execute_script("return arguments[0].previousElementSibling;", fila)
            desc     = fila_ant.text.strip() if fila_ant else "archivo"
        except Exception:
            desc = "archivo"
        if href not in [r[1] for r in resultado]:
            resultado.append((desc or "archivo", href))
    return resultado


def descargar_archivo(url, carpeta, nombre_sugerido, cookies_selenium, indice):
    session = requests.Session()
    for c in cookies_selenium:
        session.cookies.set(c["name"], c["value"])
    try:
        resp = session.get(url, timeout=30, stream=True)
        resp.raise_for_status()
        cd = resp.headers.get("Content-Disposition", "")
        if "filename=" in cd:
            nombre = cd.split("filename=")[-1].strip().strip('"')
        else:
            ext    = ".pdf" if "pdf" in resp.headers.get("Content-Type", "").lower() else ""
            nombre = f"{indice:02d}_{nombre_sugerido[:60]}{ext}"
        nombre = "".join(c for c in nombre if c not in r'\/:*?"<>|')
        ruta   = os.path.join(carpeta, nombre)
        with open(ruta, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        print(f"      💾 {nombre}")
        return True, nombre
    except Exception as e:
        print(f"      ❌ {e}")
        return False, None


# ─── DESPACHADOR ──────────────────────────────────────────────────────────────

PESTAÑAS_CRITERIOS  = {"criterios de inclusión"}
PESTAÑAS_ONE_COLUMN = {"descripción", "fechas"}
PESTAÑAS_ARCHIVOS   = {"archivos"}
PESTAÑAS_PARAMETROS = {"parámetros de calificación"}
PESTAÑAS_LOCALIDAD  = {"localidad"}


def extraer_pestaña(driver, nombre, carpeta_proceso, cookies_fn):
    nombre_lower = nombre.lower().strip()
    if nombre_lower in PESTAÑAS_ONE_COLUMN:
        return extraer_one_column_emphasis(driver)
    if nombre_lower in PESTAÑAS_CRITERIOS:
        return extraer_criterios(driver)
    if nombre_lower in PESTAÑAS_LOCALIDAD:
        return extraer_localidad(driver)
    if nombre_lower in PESTAÑAS_PARAMETROS:
        return extraer_parametros_calificacion(driver)
    if nombre_lower in PESTAÑAS_ARCHIVOS:
        links = obtener_links_descarga(driver)
        archivos_info = []
        if links:
            print(f"      📎 {len(links)} archivo(s)")
            cookies = cookies_fn()
            for i, (desc, url) in enumerate(links, 1):
                ok, fname = descargar_archivo(url, carpeta_proceso, desc, cookies, i)
                if ok:
                    archivos_info.append({"descripcion": desc, "archivo": fname, "url": url})
                time.sleep(0.5)
        else:
            print("      ℹ️  Sin archivos")
        return archivos_info
    return extraer_rounded_corner(driver)


# ─── PROCESAMIENTO DE UN PROCESO ──────────────────────────────────────────────

def procesar_proceso(driver, wait, proceso, carpeta_raiz):
    codigo = proceso["codigo"].strip().replace("/", "-")
    link   = proceso["link"]

    carpeta_proceso = os.path.join(carpeta_raiz, codigo)
    os.makedirs(carpeta_proceso, exist_ok=True)

    datos = {
        "codigo":              codigo,
        "entidad_contratante": proceso.get("entidad_contratante", ""),
        "objeto_proceso":      proceso.get("objeto_proceso", ""),
        "link":                link,
        "pestañas":            {"Parámetros de Calificación": {}},
    }

    try:
        driver.get(link)
        time.sleep(2)
        aceptar_cookies(driver, wait)

        pestañas = obtener_pestañas(driver)
        nombres  = [n for n, _ in pestañas]
        print(f"    📑 {nombres}")

        for nombre, elemento in pestañas:
            print(f"    🔍 {nombre}")
            try:
                click_pestaña(driver, elemento)
                contenido = extraer_pestaña(driver, nombre, carpeta_proceso, cookies_fn=driver.get_cookies)
                datos["pestañas"][nombre] = contenido
            except Exception as e:
                print(f"      ⚠️  Error: {e}")
                datos["pestañas"][nombre] = {"_error": str(e)}

    except Exception as e:
        print(f"  ❌ Error general: {e}")
        datos["_error"] = str(e)

    ruta_json = os.path.join(carpeta_proceso, "datos_proceso.json")
    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)

    n_arch = len(datos["pestañas"].get("Archivos", []))
    print(f"    ✅ JSON guardado | {n_arch} archivo(s) descargado(s)")
    return datos


# ─── WORKER (un hilo = un driver) ─────────────────────────────────────────────

def worker(procesos_chunk, carpeta_raiz, worker_id):
    """
    Cada worker tiene su propio driver Chrome y procesa su porción de la lista.
    """
    driver = iniciar_driver()
    wait   = WebDriverWait(driver, 15)
    resultados = []
    try:
        for i, proceso in enumerate(procesos_chunk, 1):
            codigo = proceso["codigo"].strip().replace("/", "-")
            print(f"\n[W{worker_id}] ({i}/{len(procesos_chunk)}) {codigo}")

            # ── SKIP si ya tiene archivos descargados ──
            if ya_descargado(codigo, carpeta_raiz):
                print(f"  ⏭️  Ya descargado — omitiendo")
                continue

            datos = procesar_proceso(driver, wait, proceso, carpeta_raiz)
            resultados.append(datos)
            time.sleep(1)
    except Exception as e:
        print(f"[W{worker_id}] ❌ Error: {e}")
    finally:
        driver.quit()
    return resultados


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    if not os.path.exists(ARCHIVO_JSON):
        print(f"❌ No se encontró '{ARCHIVO_JSON}'. Corre primero parsear_resultados.py")
        return

    with open(ARCHIVO_JSON, "r", encoding="utf-8") as f:
        procesos = json.load(f)

    os.makedirs(CARPETA_DESCARGAS, exist_ok=True)

    # Filtrar los que ya están completos
    pendientes = [p for p in procesos if not ya_descargado(p["codigo"].strip().replace("/", "-"), CARPETA_DESCARGAS)]
    omitidos   = len(procesos) - len(pendientes)

    print(f"📂 {len(procesos)} procesos totales")
    print(f"  ⏭️  {omitidos} ya descargados — omitidos")
    print(f"  🔄 {len(pendientes)} pendientes")
    print(f"  🖥️  {NUM_WORKERS} workers (núcleos detectados)")

    if not pendientes:
        print("✅ Todo ya está descargado.")
        return

    # Dividir en chunks iguales por worker
    chunks = [pendientes[i::NUM_WORKERS] for i in range(NUM_WORKERS)]
    chunks = [c for c in chunks if c]   # quitar chunks vacíos si hay menos procesos que workers

    todos = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futuros = {
            executor.submit(worker, chunk, CARPETA_DESCARGAS, idx + 1): idx
            for idx, chunk in enumerate(chunks)
        }
        for futuro in as_completed(futuros):
            try:
                resultado = futuro.result()
                todos.extend(resultado)
            except Exception as e:
                print(f"❌ Worker falló: {e}")

    # Guardar consolidado
    ruta = os.path.join(CARPETA_DESCARGAS, "consolidado.json")

    # Cargar consolidado existente y agregar nuevos (no sobreescribir lo anterior)
    existente = []
    if os.path.exists(ruta):
        try:
            with open(ruta, "r", encoding="utf-8") as f:
                existente = json.load(f)
        except Exception:
            existente = []

    codigos_existentes = {e["codigo"] for e in existente}
    nuevos = [d for d in todos if d["codigo"] not in codigos_existentes]
    consolidado = existente + nuevos

    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(consolidado, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"🎉 {len(todos)} proceso(s) nuevos procesados")
    print(f"   Consolidado total: {len(consolidado)} | '{ruta}'")


if __name__ == "__main__":
    main()
