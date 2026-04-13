# -*- coding: utf-8 -*-
"""
01_generacion_datos.py
Versión adaptada para ejecutarse con GitHub Actions (sin Google Colab).
Guarda los CSVs localmente y los sube a Google Drive via pydrive2.
"""

# ── Librerías ─────────────────────────────────────────────────────────────────
import pandas as pd
import numpy as np
import random
import os
import glob
from datetime import datetime, timedelta
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# ── Rutas locales (dentro del runner de GitHub Actions) ───────────────────────
ruta      = "data_ventas/"
ruta_dim  = ruta + "dimensiones/"
ruta_fact = ruta + "hechos/"

for carpeta in [ruta, ruta_dim, ruta_fact]:
    os.makedirs(carpeta, exist_ok=True)

print("Carpetas creadas correctamente")

# ── Función autoincremental ───────────────────────────────────────────────────
def ultimo_id(nombre_archivo, columna_id):
    try:
        df = pd.read_csv(ruta_fact + nombre_archivo)
        return df[columna_id].max() + 1
    except:
        return 1

# ── Dimensión clientes ────────────────────────────────────────────────────────
num_clientes = 100
clientes = pd.DataFrame({
    "id_cliente": range(1, num_clientes + 1),
    "nombre"    : [f"Cliente_{i}" for i in range(1, num_clientes + 1)],
    "ciudad"    : np.random.choice(["bogota", "medellin", "cali", "barranquilla", "cartagena", "cucuta"], num_clientes),
    "segmento"  : np.random.choice(["nuevo", "frecuente", "vip"], num_clientes)
})
clientes.to_csv(ruta_dim + "dim_clientes.csv", index=False)

# ── Dimensión productos ───────────────────────────────────────────────────────
num_productos = 50
productos = pd.DataFrame({
    "id_producto"    : range(1, num_productos + 1),
    "nombre_producto": [f"Producto_{i}" for i in range(1, num_productos + 1)],
    "categoria"      : np.random.choice(["abarrotes", "lacteos", "aseo", "bebidas", "frutas_verduras", "snacks"], num_productos),
    "marca"          : np.random.choice(["alpina", "colanta", "postobon", "zenu", "nescafe", "diana", "doria"], num_productos)
})
productos.to_csv(ruta_dim + "dim_productos.csv", index=False)

# ── Dimensión tiendas ─────────────────────────────────────────────────────────
ciudades = ["bogota", "medellin", "cali", "barranquilla", "cartagena", "cucuta"]
tiendas = pd.DataFrame({
    "id_tienda"    : range(1, len(ciudades) + 2),
    "nombre_tienda": [f"tienda_{c}" for c in ciudades] + ["tienda_online"],
    "ciudad"       : ciudades + ["online"],
    "tipo_tienda"  : ["fisica"] * len(ciudades) + ["online"]
})
tiendas.to_csv(ruta_dim + "dim_tiendas.csv", index=False)

# ── Dimensión vendedores ──────────────────────────────────────────────────────
vendedores = pd.DataFrame({
    "id_vendedor": range(1, 11),
    "nombre"     : [f"Vendedor_{i}" for i in range(1, 11)],
    "zona"       : np.random.choice(["norte", "sur", "este", "oeste"], 10)
})
vendedores.to_csv(ruta_dim + "dim_vendedores.csv", index=False)

# ── Fact ventas ───────────────────────────────────────────────────────────────
nuevos_registros = 1000
inicio_id        = ultimo_id("fact_ventas.csv", "event_id")

num_clientes   = len(pd.read_csv(ruta_dim + "dim_clientes.csv"))
num_productos  = len(pd.read_csv(ruta_dim + "dim_productos.csv"))
num_tiendas    = len(pd.read_csv(ruta_dim + "dim_tiendas.csv"))
num_vendedores = len(pd.read_csv(ruta_dim + "dim_vendedores.csv"))

ventas = pd.DataFrame({
    "event_id"       : range(inicio_id, inicio_id + nuevos_registros),
    "fecha"          : [datetime.now() - timedelta(days=random.randint(0, 30)) for _ in range(nuevos_registros)],
    "id_cliente"     : np.random.randint(1, num_clientes + 1,   nuevos_registros),
    "id_producto"    : np.random.randint(1, num_productos + 1,  nuevos_registros),
    "id_tienda"      : np.random.randint(1, num_tiendas + 1,    nuevos_registros),
    "id_vendedor"    : np.random.randint(1, num_vendedores + 1, nuevos_registros),
    "cantidad"       : np.random.randint(1, 5,                  nuevos_registros),
    "precio_unitario": np.random.uniform(5000, 50000,           nuevos_registros)
})
ventas["total"] = ventas["cantidad"] * ventas["precio_unitario"]

try:
    existentes = pd.read_csv(ruta_fact + "fact_ventas.csv")
    ventas     = pd.concat([existentes, ventas])
except:
    pass

ventas.to_csv(ruta_fact + "fact_ventas.csv", index=False)

# ── Fact envios ───────────────────────────────────────────────────────────────
inicio_envio = ultimo_id("fact_envios.csv", "id_envio")

envios = pd.DataFrame({
    "id_envio"  : range(inicio_envio, inicio_envio + nuevos_registros),
    "id_venta"  : ventas["event_id"].tail(nuevos_registros).values,
    "fecha_envio": [datetime.now() for _ in range(nuevos_registros)]
})
envios["fecha_entrega"]  = envios["fecha_envio"] + pd.to_timedelta(np.random.randint(1, 5, nuevos_registros), unit="d")
envios["tiempo_entrega"] = (envios["fecha_entrega"] - envios["fecha_envio"]).dt.days
envios["estado_envio"]   = np.random.choice(["entregado", "retrasado", "cancelado"], nuevos_registros)
envios["costo_envio"]    = np.random.uniform(15000, 30000, nuevos_registros)

try:
    envios_existentes = pd.read_csv(ruta_fact + "fact_envios.csv")
    envios            = pd.concat([envios_existentes, envios])
except:
    pass

envios.to_csv(ruta_fact + "fact_envios.csv", index=False)

# ── Conteo rápido ─────────────────────────────────────────────────────────────
print(f"dim_clientes   : {len(pd.read_csv(ruta_dim  + 'dim_clientes.csv'))} filas")
print(f"dim_productos  : {len(pd.read_csv(ruta_dim  + 'dim_productos.csv'))} filas")
print(f"dim_tiendas    : {len(pd.read_csv(ruta_dim  + 'dim_tiendas.csv'))} filas")
print(f"dim_vendedores : {len(pd.read_csv(ruta_dim  + 'dim_vendedores.csv'))} filas")
print(f"fact_ventas    : {len(pd.read_csv(ruta_fact + 'fact_ventas.csv'))} filas")
print(f"fact_envios    : {len(pd.read_csv(ruta_fact + 'fact_envios.csv'))} filas")

# ── Subida a Google Drive ─────────────────────────────────────────────────────
import json

# Lee las credenciales desde la variable de entorno (GitHub Secret)
creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
with open("credenciales.json", "w") as f:
    json.dump(creds_json, f)

gauth = GoogleAuth()
gauth.settings["client_config_backend"] = "service"
gauth.settings["service_config"] = {
    "client_json_file_path": "credenciales.json",
    "client_user_email"    : ""
}
gauth.ServiceAuth()
drive_client = GoogleDrive(gauth)

# ⚠️ Reemplaza con el ID de tu carpeta de Drive
# Lo encuentras en la URL: drive.google.com/drive/folders/ESTE_ES_EL_ID
FOLDER_ID = "1T3w-o4CjGnRncs0bl37FDNy-9-1baaYJ"

# Solo subimos los archivos de hechos
archivos_a_subir = [
    ruta_fact + "fact_ventas.csv",
    ruta_fact + "fact_envios.csv"
]

for archivo in archivos_a_subir:
    file_drive = drive_client.CreateFile({
        "title"  : os.path.basename(archivo),
        "parents": [{"id": FOLDER_ID}]
    })
    file_drive.SetContentFile(archivo)
    file_drive.Upload()
    print(f"✅ Subido: {archivo}")
