# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d6d8fdd8-ef02-41dd-93b7-eae5fe8e507b",
# META       "default_lakehouse_name": "LH_IT4W_Development",
# META       "default_lakehouse_workspace_id": "fb156000-eb61-412c-90ff-773f978dc018",
# META       "known_lakehouses": [
# META         {
# META           "id": "d6d8fdd8-ef02-41dd-93b7-eae5fe8e507b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

run_id = "a43159bc-4643-4376-abd5-41bb7a34f0b8"

fuentes = [
    "ventas",
    "devoluciones",
    "precios",
    "costos",
    "objetivos"
]

resultados = []

for fuente in fuentes:
    ruta = (
        f"Files/Landing/historico/{fuente}/"
        f"run_id={run_id}"
    )

    df = spark.read.parquet(ruta)

    resultados.append((
        fuente,
        df.count(),
        len(df.columns)
    ))

df_control = spark.createDataFrame(
    resultados,
    ["Fuente", "Filas", "Columnas"]
)

display(df_control)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
