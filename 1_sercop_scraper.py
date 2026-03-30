import time
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")
import base64
import requests
import urllib3
import os
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────
ENTIDAD          = os.getenv("ENTIDAD")
API_KEY_2CAPTCHA = os.getenv("API_KEY_2CAPTCHA")

FECHA_HASTA      = datetime.today()
FECHA_DESDE      = FECHA_HASTA - timedelta(days=180)

FECHA_DESDE_STR  = FECHA_DESDE.strftime("%Y-%m-%d")
FECHA_HASTA_STR  = FECHA_HASTA.strftime("%Y-%m-%d")

URL              = os.getenv("URL_BUSQUEDA")
CARPETA_SALIDA   = os.getenv("CARPETA_HTML", "resultados_sercop")
# ──────────────────────────────────────────────────────────────────────────────


def iniciar_driver():
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")  # descomenta para modo sin ventana
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")
    return webdriver.Chrome(options=options)


# ─── COOKIES ──────────────────────────────────────────────────────────────────

def aceptar_cookies(driver, wait):
    try:
        btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.cc-dismiss")))
        btn.click()
        print("Cookies aceptadas")
        time.sleep(1)
    except Exception:
        print("ℹBanner de cookies no apareció")


# ─── FORMULARIO ───────────────────────────────────────────────────────────────

def ingresar_entidad(driver, wait, nombre):
    campo = wait.until(EC.presence_of_element_located((By.ID, "txtEntidadContratante")))
    campo.clear()
    campo.send_keys(nombre)
    print(f"Entidad ingresada: {nombre}")
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
        print(f"✅ Fecha '{campo_id}': {valor}")
        time.sleep(0.3)


# ─── BUSCAR ───────────────────────────────────────────────────────────────────

def hacer_busqueda(driver, wait):
    """Clic en el segundo btnBuscar de la página."""
    botones = wait.until(EC.presence_of_all_elements_located((By.ID, "btnBuscar")))
    if len(botones) < 2:
        raise RuntimeError(f"Se esperaban 2 botones Buscar pero se encontraron {len(botones)}")
    botones[1].click()
    print("Clic en Buscar (2do elemento) — esperando resultados...")
    time.sleep(3)


# ─── PAGINACIÓN Y EXTRACCIÓN ──────────────────────────────────────────────────

def guardar_pagina(html, numero, carpeta, fecha_desde, fecha_hasta):
    """Guarda el HTML con prefijo de fechas de búsqueda."""
    os.makedirs(carpeta, exist_ok=True)
    nombre = f"{fecha_desde}_{fecha_hasta}_pagina_{numero:03d}.html"
    ruta = os.path.join(carpeta, nombre)
    with open(ruta, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Guardada: {ruta}")


def hay_siguiente(driver):
    """Devuelve el link 'Siguiente' si existe en la página, o None."""
    try:
        return driver.find_element(
            By.XPATH, "//a[normalize-space(text())='Siguiente']"
        )
    except Exception:
        return None


def extraer_todas_las_paginas(driver, wait, carpeta, fecha_desde, fecha_hasta):
    """
    Guarda el HTML de la página actual y navega por todas las páginas
    usando el link 'Siguiente' hasta que desaparezca.
    Devuelve (num_paginas, num_procesos_encontrados).
    """
    pagina = 1
    total_procesos = 0
    while True:
        time.sleep(2)
        html = driver.page_source

        # Contar procesos en la página para detectar si hay resultados
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        div = soup.find("div", id="divProcesos")
        if div:
            tabla = div.find("table")
            if tabla:
                filas = tabla.find_all("tr")[1:]  # saltar cabecera
                total_procesos += sum(1 for f in filas if len(f.find_all("td")) >= 7)

        guardar_pagina(html, pagina, carpeta, fecha_desde, fecha_hasta)
        print(f"Página {pagina} extraída ({len(html):,} caracteres)")

        siguiente = hay_siguiente(driver)
        if siguiente is None:
            print("No hay más páginas — extracción completa.")
            break

        siguiente.click()
        print(f"Navegando a página {pagina + 1}...")
        pagina += 1
        time.sleep(3)

    return pagina, total_procesos


# ─── 2CAPTCHA ─────────────────────────────────────────────────────────────────

MAX_REINTENTOS_CAPTCHA = 5


def enviar_captcha_2captcha(img_b64, api_key):
    resp = requests.post("https://api.2captcha.com/createTask", json={
        "clientKey": api_key,
        "task": {"type": "ImageToTextTask", "body": img_b64, "case": False, "numeric": 0}
    }, verify=False)
    resp.raise_for_status()
    data = resp.json()
    if data.get("errorId") != 0:
        raise RuntimeError(f"2captcha createTask error: {data}")
    return data["taskId"]


def obtener_solucion_2captcha(task_id, api_key):
    for intento in range(24):
        time.sleep(5)
        res = requests.post("https://api.2captcha.com/getTaskResult", json={
            "clientKey": api_key, "taskId": task_id
        }, verify=False)
        resultado = res.json()
        if resultado.get("status") == "ready":
            return resultado["solution"]["text"]
        print(f"   ⏳ Procesando... (intento {intento+1}/24)")
    raise RuntimeError("Tiempo agotado esperando captcha")


def reportar_captcha_incorrecto(task_id, api_key):
    try:
        requests.post("https://api.2captcha.com/reportIncorrect", json={
            "clientKey": api_key, "taskId": task_id
        }, verify=False)
    except Exception:
        pass


def hay_error_captcha(driver):
    try:
        texto = driver.page_source.lower()
        return any(p in texto for p in [
            "captcha incorrecto", "código incorrecto", "incorrect captcha",
            "wrong captcha", "captcha inválido", "error en captcha"
        ])
    except Exception:
        return False


def resolver_captcha(driver, wait, api_key):
    """
    Resuelve el captcha con reintento automático si la solución es incorrecta.
    Máximo MAX_REINTENTOS_CAPTCHA intentos.
    """
    for intento in range(1, MAX_REINTENTOS_CAPTCHA + 1):
        print(f"⏳ Captcha intento {intento}/{MAX_REINTENTOS_CAPTCHA}...")

        if intento > 1:
            try:
                driver.find_element(By.ID, "linkReload").click()
                time.sleep(2)
            except Exception:
                pass

        img     = wait.until(EC.presence_of_element_located((By.ID, "captcha_img")))
        img_b64 = base64.b64encode(img.screenshot_as_png).decode("utf-8")
        task_id = enviar_captcha_2captcha(img_b64, api_key)
        solucion = obtener_solucion_2captcha(task_id, api_key)
        print(f"✅ Solución: '{solucion}'")

        campo = wait.until(EC.presence_of_element_located((By.ID, "image")))
        campo.clear()
        campo.send_keys(solucion)
        return task_id

    raise RuntimeError(f"Captcha falló {MAX_REINTENTOS_CAPTCHA} veces")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    driver = iniciar_driver()
    wait   = WebDriverWait(driver, 20)

    try:
        # 1. Abrir página
        driver.get(URL)
        print("Página cargada")
        time.sleep(2)

        # 2. Aceptar cookies
        aceptar_cookies(driver, wait)

        # 3. Entidad
        ingresar_entidad(driver, wait, ENTIDAD)

        # 4. Fechas
        ingresar_fechas(driver, FECHA_DESDE_STR, FECHA_HASTA_STR)

        # 5. Activar dropdown de estado: seleccionar un tipo y volver a TODOS
        driver.execute_script("""
            var sel = document.getElementById('txtCodigoTipoCompra');
            sel.value = '386';
            sel.dispatchEvent(new Event('change'));
        """)
        print("✅ Tipo de contratación: Subasta Inversa Electrónica (trigger)")
        time.sleep(1)
        driver.execute_script("""
            var sel = document.getElementById('txtCodigoTipoCompra');
            sel.value = '';
            sel.dispatchEvent(new Event('change'));
        """)
        print("✅ Tipo de contratación: vuelto a TODOS")
        time.sleep(1)

        # 6. Inyectar estado Finalizada (ahora que el dropdown está activo)
        driver.execute_script("""
            var c = document.getElementById('cmbEstado');
            c.value = '476';
            c.dispatchEvent(new Event('change'));
        """)
        print("✅ Estado: Finalizada (476)")
        time.sleep(0.5)

        # 7. Resolver captcha + Buscar con reintento si captcha incorrecto
        for _cap_intento in range(MAX_REINTENTOS_CAPTCHA):
            task_id = resolver_captcha(driver, wait, API_KEY_2CAPTCHA)
            hacer_busqueda(driver, wait)
            if hay_error_captcha(driver):
                print(f"  ❌ Captcha rechazado — reintentando...")
                reportar_captcha_incorrecto(task_id, API_KEY_2CAPTCHA)
            else:
                print("  ✅ Captcha aceptado")
                break
        else:
            raise RuntimeError("Captcha rechazado demasiadas veces")

        # 9. Extraer HTML de todas las páginas
        paginas, procesos = extraer_todas_las_paginas(driver, wait, CARPETA_SALIDA, FECHA_DESDE_STR, FECHA_HASTA_STR)
        print(f"\nExtracción finalizada: {paginas} página(s), {procesos} proceso(s)")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

    finally:
        input("\nPresiona ENTER para cerrar el navegador...")
        driver.quit()


if __name__ == "__main__":
    main()
