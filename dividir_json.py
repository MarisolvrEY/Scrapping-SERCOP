import os
import json
import math
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────
ARCHIVO_JSON = os.getenv("ARCHIVO_JSON", "procesos_sercop.json")
CARPETA_PARTES = "partes_json"   # carpeta donde se guardan las partes
# ──────────────────────────────────────────────────────────────────────────────


def dividir_json(archivo_entrada, num_partes, carpeta_salida):
    # Cargar JSON
    with open(archivo_entrada, "r", encoding="utf-8") as f:
        datos = json.load(f)

    total = len(datos)
    if total == 0:
        print("❌ El archivo JSON está vacío.")
        return

    if num_partes > total:
        print(f"⚠️  Se pidieron {num_partes} partes pero solo hay {total} registros.")
        print(f"   Se crearán {total} partes de 1 registro cada una.")
        num_partes = total

    os.makedirs(carpeta_salida, exist_ok=True)

    tam_parte = math.ceil(total / num_partes)
    partes_creadas = 0

    for i in range(num_partes):
        inicio = i * tam_parte
        fin    = min(inicio + tam_parte, total)
        chunk  = datos[inicio:fin]

        if not chunk:
            break

        nombre = f"parte_{i+1:03d}_de_{num_partes:03d}.json"
        ruta   = os.path.join(carpeta_salida, nombre)
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(chunk, f, ensure_ascii=False, indent=2)

        print(f"  💾 {nombre} → {len(chunk)} registros (del {inicio+1} al {fin})")
        partes_creadas += 1

    print(f"\n✅ {total} registros divididos en {partes_creadas} parte(s)")
    print(f"   Guardados en: '{carpeta_salida}/'")


def main():
    if not os.path.exists(ARCHIVO_JSON):
        print(f"❌ No se encontró '{ARCHIVO_JSON}'.")
        return

    with open(ARCHIVO_JSON, "r", encoding="utf-8") as f:
        datos = json.load(f)

    print(f"📂 Archivo: '{ARCHIVO_JSON}'")
    print(f"📊 Total de registros: {len(datos)}")
    print()

    try:
        num_partes = int(input("¿En cuántas partes quieres dividir el JSON? → "))
        if num_partes < 1:
            print("❌ Debe ser al menos 1.")
            return
    except ValueError:
        print("❌ Ingresa un número entero válido.")
        return

    dividir_json(ARCHIVO_JSON, num_partes, CARPETA_PARTES)


if __name__ == "__main__":
    main()
