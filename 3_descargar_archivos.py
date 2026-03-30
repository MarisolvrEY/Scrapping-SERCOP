import os
import re
import json
import time
import requests
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
# ──────────────────────────────────────────────────────────────────────────────


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


# ─── PESTAÑAS ─────────────────────────────────────────────────────────────────

def obtener_pestañas(driver):
    """
    Lee todas las pestañas del tabmenu y devuelve lista de (nombre, elemento).
    """
    tabs = driver.find_elements(By.XPATH, "//ul[@id='tabmenu']//li//a")
    resultado = []
    for tab in tabs:
        nombre = tab.text.strip()
        if nombre:
            resultado.append((nombre, tab))
    return resultado


def click_pestaña(driver, elemento):
    driver.execute_script("arguments[0].click();", elemento)
    time.sleep(2)


# ─── EXTRACTOR GENÉRICO: one-column-emphasis ──────────────────────────────────
# Usado por: Descripción, Fechas
# Estructura: <th> label | <td> valor (| <td> descripción ignorada)

def extraer_one_column_emphasis(driver):
    """
    Lee todas las filas th/td de table#one-column-emphasis.
    Devuelve dict {label: valor}.
    """
    resultado = {}
    try:
        filas = driver.find_elements(
            By.XPATH, "//table[@id='one-column-emphasis']//tr"
        )
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


# ─── EXTRACTOR GENÉRICO: rounded-corner SIMPLE ────────────────────────────────
# Usado por: Productos, Localidad, Parámetros de Calificación y cualquier tabla nueva
# Lee cabeceras dinámicamente del thead (o primera fila con th).
# Detecta fila TOTAL por colspan > 1 en primera celda o texto TOTAL.

def extraer_rounded_corner(driver, xpath_tabla="//fieldset[@id='cuadro']//table[@id='rounded-corner']"):
    """
    Extrae UNA tabla rounded-corner de forma genérica.
    Devuelve {cabeceras: [...], filas: [...], total: str|None}
    """
    resultado = {"cabeceras": [], "filas": [], "total": None}
    try:
        tabla = driver.find_element(By.XPATH, xpath_tabla)

        # Leer cabeceras: buscar primer tr con th
        cabeceras = []
        for tr in tabla.find_elements(By.XPATH, ".//tr"):
            ths = tr.find_elements(By.TAG_NAME, "th")
            if ths:
                textos = [th.text.strip() for th in ths if th.text.strip()]
                if textos:
                    cabeceras = textos
                    break
        resultado["cabeceras"] = cabeceras

        # Leer filas de datos
        for tr in tabla.find_elements(By.XPATH, ".//tbody/tr"):
            celdas = tr.find_elements(By.TAG_NAME, "td")
            ths    = tr.find_elements(By.TAG_NAME, "th")

            if not celdas and not ths:
                continue

            # Fila TOTAL: tiene th o td con colspan o texto TOTAL
            texto_fila = tr.text.strip()
            primera = celdas[0] if celdas else ths[0]
            colspan = int(primera.get_attribute("colspan") or 1)

            if "TOTAL" in texto_fila.upper() and (colspan > 1 or len(celdas) + len(ths) < len(cabeceras)):
                ultima = (celdas or ths)[-1]
                resultado["total"] = ultima.text.strip()
                continue

            # Fila de dato normal — mapear por cabeceras si hay, o lista plana
            valores = [c.text.strip() for c in celdas]
            if cabeceras and len(valores) == len(cabeceras):
                resultado["filas"].append(dict(zip(cabeceras, valores)))
            elif valores:
                resultado["filas"].append(valores)

    except Exception as e:
        resultado["_error"] = str(e)
    return resultado


# ─── EXTRACTOR: CRITERIOS DE INCLUSIÓN ───────────────────────────────────────
# Caso especial: múltiples tablas rounded-corner dentro de fieldset#cuadro,
# cada una seguida de un <div align="right"> con el puntaje máximo.

def extraer_criterios(driver):
    """
    Itera cada table#rounded-corner dentro de fieldset#cuadro.
    Por cada tabla:
      - tipo: primer <th> del thead (si existe)
      - puntaje_maximo: número del <div align=right> inmediatamente siguiente
      - criterios: filas de datos con {criterio, descripcion, puntaje}
        (maneja rowspan: fila con 2 td hereda el criterio de la fila anterior)
    Al final captura el TOTAL general si existe.
    """
    grupos = []
    try:
        fieldset = driver.find_element(By.XPATH, "//fieldset[@id='cuadro']")
        tablas   = fieldset.find_elements(By.XPATH, ".//table[@id='rounded-corner']")

        for tabla in tablas:
            # Tipo desde thead
            tipo = ""
            ths_head = tabla.find_elements(By.XPATH, ".//thead//th")
            if ths_head:
                tipo = ths_head[0].text.strip()

            # Criterios desde tbody
            criterios   = []
            criterio_act = ""
            for tr in tabla.find_elements(By.XPATH, ".//tbody/tr"):
                celdas = tr.find_elements(By.TAG_NAME, "td")
                if len(celdas) == 3:
                    criterio_act = celdas[0].text.strip()
                    criterios.append({
                        "criterio":    criterio_act,
                        "descripcion": celdas[1].text.strip(),
                        "puntaje":     celdas[2].text.strip(),
                    })
                elif len(celdas) == 2:
                    # Fila con rowspan: reutiliza criterio_act
                    criterios.append({
                        "criterio":    criterio_act,
                        "descripcion": celdas[0].text.strip(),
                        "puntaje":     celdas[1].text.strip(),
                    })

            # Puntaje máximo: primer <div> con número después de esta tabla
            puntaje_maximo = ""
            try:
                texto_div = driver.execute_script("""
                    var t = arguments[0];
                    var sib = t.nextElementSibling;
                    while (sib) {
                        var txt = sib.textContent.trim();
                        if (sib.tagName === 'DIV' && /\\d/.test(txt)) return txt;
                        if (sib.tagName === 'TABLE') break;
                        sib = sib.nextElementSibling;
                    }
                    return '';
                """, tabla)
                m = re.search(r"(\d+)", texto_div or "")
                puntaje_maximo = m.group(1) if m else ""
            except Exception:
                pass

            grupos.append({
                "tipo":           tipo,
                "puntaje_maximo": puntaje_maximo,
                "criterios":      criterios,
            })

        # TOTAL general
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


# ─── EXTRACTOR: ARCHIVOS ──────────────────────────────────────────────────────

def obtener_links_descarga(driver):
    links = driver.find_elements(By.XPATH, "//a[contains(@href,'bajarArchivo.cpe')]")
    resultado = []
    for link in links:
        href = link.get_attribute("href") or ""
        if not href:
            continue
        try:
            fila = link.find_element(By.XPATH, "./ancestor::tr[1]")
            fila_ant = driver.execute_script(
                "return arguments[0].previousElementSibling;", fila
            )
            descripcion = fila_ant.text.strip() if fila_ant else "archivo"
        except Exception:
            descripcion = "archivo"
        if not descripcion:
            descripcion = "archivo"
        if href not in [r[1] for r in resultado]:
            resultado.append((descripcion, href))
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
            ext = ".pdf" if "pdf" in resp.headers.get("Content-Type", "").lower() else ""
            nombre = f"{indice:02d}_{nombre_sugerido[:60]}{ext}"
        nombre = "".join(c for c in nombre if c not in r'\/:*?"<>|')
        ruta = os.path.join(carpeta, nombre)
        with open(ruta, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        print(f"      💾 {nombre}")
        return True, nombre
    except Exception as e:
        print(f"      ❌ {e}")
        return False, None


# ─── DESPACHADOR DE PESTAÑAS ──────────────────────────────────────────────────

PESTAÑAS_CRITERIOS    = {"criterios de inclusión"}
PESTAÑAS_ONE_COLUMN   = {"descripción", "fechas"}
PESTAÑAS_ARCHIVOS     = {"archivos"}
PESTAÑAS_PARAMETROS   = {"parámetros de calificación"}
PESTAÑAS_LOCALIDAD    = {"localidad"}

def extraer_parametros_calificacion(driver):
    """
    Extrae table#rounded-corner de Parámetros de Calificación.
    Estructura: 3 columnas (parametro | descripcion | porcentaje).
    Fila TOTAL: tiene <th> con colspan.
    """
    resultado = {"parametros": [], "total": ""}
    try:
        tabla = driver.find_element(
            By.XPATH, "//fieldset[@id='cuadro']//table[@id='rounded-corner']"
        )
        for tr in tabla.find_elements(By.XPATH, ".//tbody/tr"):
            tds = tr.find_elements(By.TAG_NAME, "td")
            ths = tr.find_elements(By.TAG_NAME, "th")

            # Fila TOTAL: solo <th>
            if ths and not tds:
                # Último <th> tiene el valor total
                resultado["total"] = ths[-1].text.strip()
                continue

            # Fila vacía de cabecera (th colspan=3 vacío)
            if ths and len(tds) == 0:
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
    """
    Extrae table#rounded-corner de Localidad (dentro de div#content, sin fieldset).
    Lee columnas dinámicamente del thead.
    """
    resultado = {"cabeceras": [], "filas": []}
    try:
        tabla = driver.find_element(
            By.XPATH, "//div[@id='content']//table[@id='rounded-corner']"
        )
        # Cabeceras dinámicas
        cabeceras = [
            th.text.strip()
            for th in tabla.find_elements(By.XPATH, ".//thead//th")
            if th.text.strip()
        ]
        resultado["cabeceras"] = cabeceras

        for tr in tabla.find_elements(By.XPATH, ".//tbody/tr"):
            tds = tr.find_elements(By.TAG_NAME, "td")
            valores = [td.text.strip() for td in tds]
            if cabeceras and len(valores) == len(cabeceras):
                resultado["filas"].append(dict(zip(cabeceras, valores)))
            elif valores:
                resultado["filas"].append(valores)
    except Exception as e:
        resultado["_error"] = str(e)
    return resultado

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

    # Cualquier otra pestaña desconocida → rounded-corner genérico
    return extraer_rounded_corner(driver)


# ─── PROCESAMIENTO POR PROCESO ────────────────────────────────────────────────

def procesar_proceso(driver, wait, proceso, carpeta_raiz):
    codigo = proceso["codigo"].strip().replace("/", "-")
    link   = proceso["link"]

    print(f"\n  [{codigo}] {proceso.get('objeto_proceso','')[:60]}...")

    carpeta_proceso = os.path.join(carpeta_raiz, codigo)
    os.makedirs(carpeta_proceso, exist_ok=True)

    datos = {
        "codigo":              codigo,
        "entidad_contratante": proceso.get("entidad_contratante", ""),
        "objeto_proceso":      proceso.get("objeto_proceso", ""),
        "link":                link,
        "pestañas":            {
            "Parámetros de Calificación": {}  # siempre presente, vacío si no existe en el proceso
        },
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
                contenido = extraer_pestaña(
                    driver, nombre, carpeta_proceso,
                    cookies_fn=driver.get_cookies
                )
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


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    if not os.path.exists(ARCHIVO_JSON):
        print(f"❌ No se encontró '{ARCHIVO_JSON}'. Corre primero parsear_resultados.py")
        return

    with open(ARCHIVO_JSON, "r", encoding="utf-8") as f:
        procesos = json.load(f)

    print(f"📂 {len(procesos)} procesos cargados")
    os.makedirs(CARPETA_DESCARGAS, exist_ok=True)

    driver = iniciar_driver()
    wait   = WebDriverWait(driver, 15)
    todos  = []

    try:
        for i, proceso in enumerate(procesos, 1):
            print(f"\n{'='*60}")
            print(f"Proceso {i}/{len(procesos)}")
            datos = procesar_proceso(driver, wait, proceso, CARPETA_DESCARGAS)
            todos.append(datos)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n⚠️  Interrumpido")

    finally:
        ruta = os.path.join(CARPETA_DESCARGAS, "consolidado.json")
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(todos, f, ensure_ascii=False, indent=2)
        print(f"\n{'='*60}")
        print(f"🎉 {len(todos)} procesos | Consolidado: '{ruta}'")
        driver.quit()


if __name__ == "__main__":
    main()
