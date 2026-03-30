import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")
import json
import glob
from urllib.parse import unquote
from bs4 import BeautifulSoup

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────
CARPETA_HTML  = os.getenv("CARPETA_HTML", "resultados_sercop")
ARCHIVO_JSON  = os.getenv("ARCHIVO_JSON", "procesos_sercop.json")
BASE_URL      = os.getenv("BASE_URL", "https://www.compraspublicas.gob.ec/ProcesoContratacion/compras/PC/")
# ──────────────────────────────────────────────────────────────────────────────


def parsear_archivo(ruta_html):
    """
    Lee un HTML guardado y extrae todos los procesos de la tabla de resultados.
    Devuelve una lista de diccionarios.
    """
    with open(ruta_html, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    procesos = []

    # Los resultados están dentro de <div id="divProcesos">
    div = soup.find("div", id="divProcesos")
    if div is None:
        print(f"  ⚠️  No se encontró #divProcesos en {ruta_html}")
        return procesos

    tabla = div.find("table")
    if tabla is None:
        return procesos

    filas = tabla.find_all("tr")
    # La primera fila es el encabezado, la saltamos
    for fila in filas[1:]:
        celdas = fila.find_all("td")
        if len(celdas) < 7:
            continue

        # Celda 0: Código + link
        enlace = celdas[0].find("a")
        codigo = enlace.get_text(strip=True) if enlace else celdas[0].get_text(strip=True)
        href   = enlace["href"].strip() if enlace and enlace.get("href") else ""
        link   = BASE_URL + href if href and not href.startswith("http") else href

        # Celda 1: Entidad Contratante (puede tener %20 de URL encoding)
        entidad = unquote(celdas[1].get_text(strip=True))

        # Celda 2: Objeto del Proceso
        objeto = celdas[2].get_text(strip=True)

        # Celda 3: Estado del Proceso
        estado = celdas[3].get_text(strip=True)

        # Celda 4: Provincia/Cantón
        provincia_canton = celdas[4].get_text(strip=True)

        # Celda 5: Presupuesto Referencial Total
        presupuesto = celdas[5].get_text(strip=True)

        # Celda 6: Fecha de Publicación
        fecha_publicacion = celdas[6].get_text(strip=True)

        procesos.append({
            "codigo":             codigo,
            "entidad_contratante": entidad,
            "objeto_proceso":     objeto,
            "estado_proceso":     estado,
            "provincia_canton":   provincia_canton,
            "presupuesto":        presupuesto,
            "fecha_publicacion":  fecha_publicacion,
            "link":               link,
        })

    return procesos


def main():
    # Busca tanto el formato nuevo (con fechas) como el formato antiguo
    archivos = sorted(set(
        glob.glob(os.path.join(CARPETA_HTML, "*.html")) +
        glob.glob(os.path.join(CARPETA_HTML, "**", "*.html"), recursive=True)
    ))

    if not archivos:
        print(f"No se encontraron archivos HTML en '{CARPETA_HTML}/'")
        print("Asegúrate de correr primero el sercop_scraper.py")
        return

    print(f"Encontrados {len(archivos)} archivo(s) HTML en '{CARPETA_HTML}/'")

    todos_los_procesos = []

    for archivo in archivos:
        nombre = os.path.basename(archivo)
        procesos = parsear_archivo(archivo)
        print(f" {nombre} → {len(procesos)} proceso(s) extraídos")
        todos_los_procesos.extend(procesos)

    # Eliminar duplicados por código de proceso
    vistos   = set()
    unicos   = []
    for p in todos_los_procesos:
        if p["codigo"] not in vistos:
            vistos.add(p["codigo"])
            unicos.append(p)

    duplicados = len(todos_los_procesos) - len(unicos)
    print(f"\nTotal: {len(todos_los_procesos)} registros | "
          f"{duplicados} duplicados eliminados | "
          f"{len(unicos)} únicos")

    # Guardar JSON
    with open(ARCHIVO_JSON, "w", encoding="utf-8") as f:
        json.dump(unicos, f, ensure_ascii=False, indent=2)

    print(f"\nJSON guardado en '{ARCHIVO_JSON}' con {len(unicos)} procesos.")
    print(f"\nEjemplo del primer registro:")
    if unicos:
        print(json.dumps(unicos[0], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
