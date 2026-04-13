# Librerías
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os
import json

# 🔥 GOOGLE DRIVE
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ================================
# 🔐 AUTENTICACIÓN DRIVE
# ================================
credenciales = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = service_account.Credentials.from_service_account_info(credenciales)
service = build('drive', 'v3', credentials=creds)

# ================================
# 📂 RUTA LOCAL (IMPORTANTE)
# ================================
ruta = "data_ventas/"

if not os.path.exists(ruta):
    os.makedirs(ruta)

# ================================
# 🔁 FUNCIÓN AUTOINCREMENTAL
# ================================
def ultimo_id(nombre_archivo, columna_id):
    try:
        df = pd.read_csv(ruta + nombre_archivo)
        return df[columna_id].max() + 1
    except:
        return 1

# ================================
# 🧱 DIMENSIONES (SE GENERAN NORMAL)
# ================================

num_clientes = 100
clientes = pd.DataFrame({
    "id_cliente": range(1, num_clientes + 1),
    "nombre": [f"Cliente_{i}" for i in range(1, num_clientes + 1)],
    "ciudad": np.random.choice(["bogota", "medellin", "cali", "barranquilla", "cartagena", "cucuta"], num_clientes),
    "segmento": np.random.choice(["nuevo", "frecuente", "vip"], num_clientes)
})
clientes.to_csv(ruta + "dim_clientes.csv", index=False)

num_productos = 50
productos = pd.DataFrame({
    "id_producto": range(1, num_productos + 1),
    "nombre_producto": [f"Producto_{i}" for i in range(1, num_productos + 1)],
    "categoria": np.random.choice(["abarrotes","lacteos","aseo","bebidas","frutas_verduras","snacks"], num_productos),
    "marca": np.random.choice(["alpina","colanta","postobon","zenu","nescafe","diana","doria"], num_productos)
})
productos.to_csv(ruta + "dim_productos.csv", index=False)

ciudades = ["bogota","medellin","cali","barranquilla","cartagena","cucuta"]

tiendas = pd.DataFrame({
    "id_tienda": range(1, len(ciudades) + 2),
    "nombre_tienda": [f"tienda_{c}" for c in ciudades] + ["tienda_online"],
    "ciudad": ciudades + ["online"],
    "tipo_tienda": ["fisica"] * len(ciudades) + ["online"]
})
tiendas.to_csv(ruta + "dim_tiendas.csv", index=False)

vendedores = pd.DataFrame({
    "id_vendedor": range(1, 11),
    "nombre": [f"Vendedor_{i}" for i in range(1, 11)],
    "zona": np.random.choice(["norte","sur","este","oeste"], 10)
})
vendedores.to_csv(ruta + "dim_vendedores.csv", index=False)

# ================================
# 📊 FACT VENTAS
# ================================

nuevos_registros = 1000
inicio_id = ultimo_id("fact_ventas.csv", "event_id")

num_clientes = len(clientes)
num_productos = len(productos)
num_tiendas = len(tiendas)
num_vendedores = len(vendedores)

ventas = pd.DataFrame({
    "event_id": range(inicio_id, inicio_id + nuevos_registros),
    "fecha": [datetime.now() - timedelta(days=random.randint(0,30)) for _ in range(nuevos_registros)],
    "id_cliente": np.random.randint(1, num_clientes + 1, nuevos_registros),
    "id_producto": np.random.randint(1, num_productos + 1, nuevos_registros),
    "id_tienda": np.random.randint(1, num_tiendas + 1, nuevos_registros),
    "id_vendedor": np.random.randint(1, num_vendedores + 1, nuevos_registros),
    "cantidad": np.random.randint(1, 5, nuevos_registros),
    "precio_unitario": np.random.uniform(5000, 50000, nuevos_registros)
})

ventas["total"] = ventas["cantidad"] * ventas["precio_unitario"]

try:
    existentes = pd.read_csv(ruta + "fact_ventas.csv")
    ventas = pd.concat([existentes, ventas])
except:
    pass

ventas.to_csv(ruta + "fact_ventas.csv", index=False)

# ================================
# 🚚 FACT ENVIOS
# ================================

inicio_envio = ultimo_id("fact_envios.csv", "id_envio")

envios = pd.DataFrame({
    "id_envio": range(inicio_envio, inicio_envio + nuevos_registros),
    "id_venta": ventas["event_id"].tail(nuevos_registros).values,
    "fecha_envio": [datetime.now() for _ in range(nuevos_registros)]
})

envios["fecha_entrega"] = envios["fecha_envio"] + pd.to_timedelta(
    np.random.randint(1,5,nuevos_registros), unit='d'
)

envios["tiempo_entrega"] = (envios["fecha_entrega"] - envios["fecha_envio"]).dt.days
envios["estado_envio"] = np.random.choice(["entregado","retrasado","cancelado"], nuevos_registros)
envios["costo_envio"] = np.random.uniform(15000,30000,nuevos_registros)

try:
    envios_existentes = pd.read_csv(ruta + "fact_envios.csv")
    envios = pd.concat([envios_existentes, envios])
except:
    pass

envios.to_csv(ruta + "fact_envios.csv", index=False)

# ================================
# ☁️ SUBIR SOLO FACT A DRIVE
# ================================

def subir_o_actualizar(ruta_archivo, carpeta_id):
    nombre_archivo = os.path.basename(ruta_archivo)

    query = f"name='{nombre_archivo}' and '{carpeta_id}' in parents and trashed=false"
    resultados = service.files().list(q=query).execute()
    archivos = resultados.get('files', [])

    media = MediaFileUpload(ruta_archivo, resumable=True)

    if archivos:
        service.files().update(fileId=archivos[0]['id'], media_body=media).execute()
    else:
        file_metadata = {'name': nombre_archivo, 'parents': [carpeta_id]}
        service.files().create(body=file_metadata, media_body=media).execute()

# 🔥 REEMPLAZA CON TU ID REAL
carpeta_id = "1Xwhd-tBtQhoLOHpU73KeJHCr3LeRTs7_"

subir_o_actualizar(ruta + "fact_ventas.csv", carpeta_id)
subir_o_actualizar(ruta + "fact_envios.csv", carpeta_id)