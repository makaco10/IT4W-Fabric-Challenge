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
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# IT4W — Landing → Bronze | Backfill historico
# Carga los cinco conjuntos Parquet de una ejecución validada a las tablas Delta Bronze existentes, sin crear tablas ni alterar el esquema. Conserva los datos de negocio suponiendo tal caso, llegaron a Landing y se agregan metadatos de ingesta

# PARAMETERS CELL ********************

#Parametros
run_id = ""
batch_id = "historico_2024_2025"
load_mode = "historico"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Configuración y contratos originales

# CELL ********************

from datetime import datetime, timezone
from functools import reduce
import re
from delta.tables import DeltaTable
from pyspark.sql import functions as F, types as T

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

if not run_id or not re.fullmatch(r"[0-9a-fA-F-]{36}", run_id.strip()):
    raise ValueError("Falta run_id válido. Indicar un RunId real o pasarlo desde el pipeline.")

if load_mode != "historico" or batch_id != "historico_2024_2025":
    raise ValueError("Este notebook se ha diseñado exclusivamente para el backfill histórico.")

FUENTES = ["ventas", "devoluciones", "precios", "costos", "objetivos"]
EXPECTED_COUNTS = {"ventas": 80536, "devoluciones": 4616,
                   "precios": 641, "costos": 2424, "objetivos": 246}
METADATA_SPECS = [
    ("_batch_id", "STRING", False),
    ("_source_file", "STRING", False),
    ("_ingested_at", "TIMESTAMP", False),
    ("_load_mode", "STRING", False),
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CONTRACTS = {'costos': [('ERP_RowID', 'BIGINT', False), ('Periodo', 'STRING', True), ('ID_Producto', 'INT', True), ('Costo_Unitario', 'DECIMAL(18,2)', True)], 'devoluciones': [('ERP_RowID', 'BIGINT', False), 
             ('ID_Devolución', 'INT', True), ('ID_Venta', 'INT', True), ('Fecha', 'DATE', True), ('ID_Producto', 'INT', True), ('ID_Cliente', 'INT', True), ('ID_Tienda', 'INT', True), ('Cantidad_Devuelta', 'INT', True), 
             ('Motivo', 'STRING', True), ('Precio_Unitario', 'DECIMAL(18,2)', True), ('Nombre_Cliente', 'STRING', True), ('Ubicación_Cliente', 'STRING', True), ('Nombre_Tienda', 'STRING', True), ('Ubicación_Tienda', 'STRING',
              True), ('Nombre_Producto', 'STRING', True), ('Categoría_Producto', 'STRING', True)], 'objetivos': [('ERP_RowID', 'BIGINT', False), ('Periodo', 'STRING', True), ('ID_Tienda', 'INT', True), 
              ('Objetivo_Ventas', 'DECIMAL(18,2)', True)], 'precios': [('ERP_RowID', 'BIGINT', False), ('ID_Producto', 'INT', True), ('Vigencia_Desde', 'DATE', True), ('Precio_Lista', 'DECIMAL(18,2)', True)], 
              'ventas': [('ERP_RowID', 'BIGINT', False), ('ID_Venta', 'INT', True), ('ID_Pedido', 'INT', True), ('Fecha', 'DATE', True), ('ID_Producto', 'INT', True), ('ID_Cliente', 'INT', True), ('ID_Tienda', 'INT', True), 
              ('Cantidad', 'INT', True), ('Precio_Unitario', 'DECIMAL(18,2)', True), ('Canal_Venta', 'STRING', True), ('Categoría_Venta', 'STRING', True), ('Nombre_Cliente', 'STRING', True), ('Ubicación_Cliente', 'STRING', True), 
              ('Nombre_Tienda', 'STRING', True), ('Ubicación_Tienda', 'STRING', True), ('Nombre_Producto', 'STRING', True), ('Categoría_Producto', 'STRING', True), ('Subcategoría_Producto', 'STRING', True)]}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Validación previa: rutas, tipos y tablas Delta

# CELL ********************

def spark_type(spec: str) -> T.DataType:
    types = {
        "BIGINT": T.LongType(), "INT": T.IntegerType(),
        "DATE": T.DateType(), "STRING": T.StringType(),
        "TIMESTAMP": T.TimestampType()
    }
    if spec in types:
        return types[spec]
    dec = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", spec)
    if dec:
        return T.DecimalType(int(dec.group(1)), int(dec.group(2)))
    raise ValueError(f"Tipo fuera de contrato: {spec}")


def signature(schema: T.StructType):
    return [(f.name, f.dataType.simpleString(), f.nullable) for f in schema.fields]


def validate_one(source: str):
    path = f"Files/Landing/historico/{source}/run_id={run_id}"
    full_table = f"bronze.{source}"
    if not spark.catalog.tableExists(full_table):
        raise RuntimeError(f"No existe {full_table}. Ejecutar notebook 00 antes.")

    src = spark.read.format("parquet").load(path)
    raw_specs = CONTRACTS[source]
    expected_raw = [(n, spark_type(t).simpleString()) for n,t,_ in raw_specs]
    actual_raw = [(f.name, f.dataType.simpleString()) for f in src.schema.fields]
    if actual_raw != expected_raw:
        raise RuntimeError(
            f"ESQUEMA LANDING INCOMPATIBLE: {source}\n"
            f"Esperado: {expected_raw}\nActual: {actual_raw}\n"
            "No se aplican casts automáticos. Revisar Copy/contrato."
        )

    expected_target = T.StructType([
        T.StructField(n, spark_type(t), nullok)
        for n,t,nullok in raw_specs + METADATA_SPECS
    ])
    existing_target_schema = spark.table(full_table).schema
    if signature(existing_target_schema) != signature(expected_target):
        raise RuntimeError(
            f"CONTRATO BRONZE DIFERENTE: {full_table}\n"
            f"Esperado: {signature(expected_target)}\n"
            f"Actual: {signature(existing_target_schema)}"
        )

    n = src.count()
    if n != EXPECTED_COUNTS[source]:
        raise RuntimeError(
            f"Conteo diferente en {source}: esperado {EXPECTED_COUNTS[source]}, actual {n}. "
            "No se escribirá Bronze hasta revisar el origen."
        )
    if src.filter(F.col("ERP_RowID").isNull()).limit(1).count():
        raise RuntimeError(f"{source} tiene ERP_RowID nulo.")
    if src.groupBy("ERP_RowID").count().filter(F.col("count") > 1).limit(1).count():
        raise RuntimeError(f"{source} tiene ERP_RowID repetido dentro del lote.")

    print(f"PRECHECK OK | {source}: {n:,} filas, {len(src.columns)} columnas | {path}")
    return path, src, n


# Prevalidar las 5 fuentes antes de escribir la primera.
validated = {source: validate_one(source) for source in FUENTES}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Merge insert-only y comprobación de cambios inesperados

# CELL ********************

def merge_bronze(source: str, path: str, raw, count: int):
    target_name = f"bronze.{source}"
    base_fields = [n for n, _, _ in CONTRACTS[source]]

    incoming = (raw
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_load_mode", F.lit(load_mode))
    )
    # Conserva orden de columnas de las tablas 00.
    incoming = incoming.select(*[F.col(f"`{name}`") for name in base_fields],
                               "_batch_id", "_source_file", "_ingested_at", "_load_mode")

    current = spark.table(target_name).where(F.col("_batch_id") == batch_id)
    before = current.count()

    # Si la misma identidad tecnica tiene distintos valores de negocio, detenerse:
    # no sobreescribir en silencio. ERP_RowID solo es estable mientras
    # no se regenere la tabla SQL de origen.
    if before > 0:
        condition = ((F.col("s.ERP_RowID") == F.col("t.ERP_RowID")) &
                     (F.col("s._batch_id") == F.col("t._batch_id")))
        differences = reduce(
            lambda a, b: a | b,
            [~F.col(f"s.`{name}`").eqNullSafe(F.col(f"t.`{name}`"))
             for name in base_fields]
        )
        if (incoming.alias("s").join(current.alias("t"), condition, "inner")
                .filter(differences).limit(1).count()):
            raise RuntimeError(
                f"Datos cambiados en {target_name} para el lote {batch_id}. "
                "Se requiere revisión antes de modificar Bronze."
            )

    (DeltaTable.forName(spark, target_name).alias("t")
        .merge(incoming.alias("s"),
               "t._batch_id = s._batch_id AND t.ERP_RowID = s.ERP_RowID")
        .whenNotMatchedInsertAll()
        .execute())

    after = spark.table(target_name).filter(F.col("_batch_id") == batch_id).count()
    if after != count:
        raise RuntimeError(
            f"Reconciliación fallida en {target_name}: Landing={count}, Bronze={after}."
        )
    inserted = after - before
    print(f"BRONZE OK | {target_name} | Landing={count} | Insertadas={inserted} | Total lote={after}")
    return inserted, after

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Auditoría del backfill y resultados

# CELL ********************

def append_audit(source: str, started: datetime, status: str,
                 rows_read: int, rows_loaded: int, error: str = None):
    table_name = "audit.ingestion_runs"
    schema = spark.table(table_name).schema
    record = [(
        run_id, "PL_IT4W_Backfill_Historico", source, batch_id,
        load_mode, status, started, datetime.now(timezone.utc),
        int(rows_read), int(rows_loaded), 0, error
    )]
    (spark.createDataFrame(record, schema)
          .write.format("delta").mode("append").saveAsTable(table_name))


def register_success(source: str, path: str):
    table_name = "audit.batch_registry"
    registry = spark.table(table_name)
    already = (registry
        .filter((F.col("batch_id") == batch_id) &
                (F.col("source_name") == source) &
                (F.col("load_mode") == load_mode))
        .limit(1).count())
    if already:
        return
    schema = registry.schema
    ts = datetime.now(timezone.utc)
    record = [(batch_id, source, load_mode, path,
               "BRONZE_LOADED", None, ts, ts)]
    (spark.createDataFrame(record, schema)
          .write.format("delta").mode("append").saveAsTable(table_name))


summary = []
for source in FUENTES:
    path, raw, rows_read = validated[source]
    started = datetime.now(timezone.utc)
    try:
        inserted, total = merge_bronze(source, path, raw, rows_read)
        register_success(source, path)
        append_audit(source, started, "SUCCEEDED", rows_read, inserted)
        summary.append((source, rows_read, inserted, total, "SUCCEEDED"))
    except Exception as exc:
        try:
            append_audit(source, started, "FAILED", rows_read, 0, str(exc)[:3000])
        except Exception as audit_exc:
            print(f"No se pudo escribir auditoria de fallo: {audit_exc}")
        raise

result_df = spark.createDataFrame(
    summary, ["Fuente", "Landing", "Nuevas_en_Bronze", "Total_lote_Bronze", "Estado"]
)
display(result_df)
print("BACKFILL LANDING → BRONZE COMPLETADO. Silver, Gold y calidad de negocio van en etapas independientes.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

