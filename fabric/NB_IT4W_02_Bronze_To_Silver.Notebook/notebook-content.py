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

# MARKDOWN ********************

# **NB_IT4W_02_Bronze_To_Silver**

# MARKDOWN ********************

# Parametros

# CELL ********************

batch_id = "historico_2024_2025"
run_id = ""  # valor proporcionado desde @pipeline().RunId; no se usa para filtrar todo Bronze

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# configuracion

# CELL ********************

from datetime import datetime, timezone
from functools import reduce
from delta.tables import DeltaTable
from pyspark.sql import functions as F, types as T, Window

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
FUENTES = ["ventas", "devoluciones", "precios", "costos", "objetivos"]
KEYS = {"ventas": ["ID_Venta"], "devoluciones": ["ID_Devolución"],
        "precios": ["ID_Producto", "Vigencia_Desde"],
        "costos": ["Periodo", "ID_Producto"],
        "objetivos": ["Periodo", "ID_Tienda"]}
if not batch_id.strip():
    raise ValueError("batch_id obligatorio")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# contratos delta

# CELL ********************

# Spark SQL se limita a CREATE TABLE DDL para conservar NOT NULL fisicos.
def quote(name):
    return '`' + name.replace('`', '``') + '`'


def signature(schema):
    return [(f.name, f.dataType.simpleString(), f.nullable) for f in schema.fields]


def ensure_silver(source):
    bronze = spark.table(f"bronze.{source}")
    target = f"silver.{source}"
    expected = bronze.schema
    if not spark.catalog.tableExists(target):
        clauses = [f"{quote(f.name)} {f.dataType.simpleString().upper()}" +
                   (" NOT NULL" if not f.nullable else "") for f in expected.fields]
        spark.sql(f"CREATE TABLE {target} ({', '.join(clauses)}) USING DELTA")
    actual = spark.table(target).schema
    if signature(actual) != signature(expected):
        raise RuntimeError(f"Contrato Silver incompatible en {target}. Esperado={signature(expected)} Actual={signature(actual)}")
    return bronze


# Validar todos los destinos y origenes antes de empezar escrituras.
inputs = {}
for fuente in FUENTES:
    bronze = ensure_silver(fuente)
    current = bronze.where(F.col("_batch_id") == batch_id)
    if not current.limit(1).count():
        raise RuntimeError(f"No hay filas en bronze.{fuente} para lote {batch_id}")
    inputs[fuente] = current
print("PRECHECK Silver OK: 5 fuentes Bronze disponibles; contratos físicos verificados")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# calidad de datos 
# Las reglas son explicitas y las filas rechazadas quedan asociadas a un motivo. Duplicados conflictivos se apartan completos

# CELL ********************

# NOTA: ERP_RowID es identidad tecnica de SQL Server, nunca la clave de negocio.
def defined(col_name):
    return F.col(quote(col_name)).isNotNull()


def get_rules(source, df):
    key_cols = KEYS[source]
    rules = []
    for key in key_cols:
        rules.append((~defined(key), "NULL_BUSINESS_KEY", f"Clave de negocio requerida ausente: {key}"))
    n_keys = Window.partitionBy(*[F.col(quote(k)) for k in key_cols])
    augmented = df.withColumn("__key_count", F.count(F.lit(1)).over(n_keys))
    rules.append((F.col("__key_count") > 1, "DUPLICATE_BUSINESS_KEY",
                  "Varias filas del mismo lote comparten la clave de negocio; no se elige ganador arbitrario"))
    if source in ("ventas", "devoluciones"):
        qty = "Cantidad" if source == "ventas" else "Cantidad_Devuelta"
        rules += [
            (F.col("Fecha").isNull(), "MISSING_DATE", "Fecha de movimiento no informada"),
            (F.col("Fecha") < F.to_date(F.lit("2024-01-01")), "DATE_BEFORE_SCOPE", "Fecha anterior al alcance del challenge"),
            (F.col(qty).isNull() | (F.col(qty) <= 0), "INVALID_QUANTITY", "Cantidad faltante o no positiva"),
            (F.col("Precio_Unitario").isNull() | (F.col("Precio_Unitario") < 0), "INVALID_UNIT_PRICE", "Precio USD faltante o negativo"),
            (F.col("ID_Producto").isNull() | F.col("ID_Tienda").isNull(), "MISSING_RELATION", "Producto o tienda sin identificador"),
            (F.col("Ubicación_Tienda").isNull() |
             ~F.upper(F.trim(F.col("Ubicación_Tienda"))).isin("MÉXICO", "MEXICO", "BRASIL", "BRAZIL", "ITALIA", "ITALY"),
             "UNKNOWN_STORE_COUNTRY", "País de tienda sin moneda definida"),
        ]
        if source == "devoluciones":
            rules.append((F.col("ID_Venta").isNull(), "MISSING_SALE_REFERENCE", "Referencia a venta no informada"))
    elif source in ("costos", "objetivos"):
        # Los periodos se almacenan char(7): yyyy-MM
        per = F.trim(F.col("Periodo"))
        rules.append((F.col("Periodo").isNull() | ~per.rlike(r"^(2024|2025|2026)-(0[1-9]|1[0-2])$"),
                      "INVALID_PERIOD", "Periodo esperado en formato yyyy-MM (2024-2026)"))
        amount = "Costo_Unitario" if source == "costos" else "Objetivo_Ventas"
        rules.append((F.col(amount).isNull() | (F.col(amount) < 0), "INVALID_AMOUNT", "Importe USD faltante o negativo"))
    elif source == "precios":
        rules += [
            (F.col("Vigencia_Desde").isNull(), "INVALID_EFFECTIVE_DATE", "Vigencia de precio sin fecha"),
            (F.col("Precio_Lista").isNull() | (F.col("Precio_Lista") < 0),
             "INVALID_LIST_PRICE", "Precio lista USD faltante o negativo"),
        ]
    return augmented, rules


# Calcular rechazos SIN descartarlos; cada infracción origina una incidencia detallada.
plans = {}
for source in FUENTES:
    raw = inputs[source]
    df, rules = get_rules(source, raw)
    violation_flags = [F.coalesce(expr, F.lit(False)) for expr, _, _ in rules]
    bad = reduce(lambda a, b: a | b, violation_flags)
    good_df = df.filter(~bad).drop("__key_count")
    n_source = raw.count()
    n_good = good_df.count()
    # For each error rule materialize one incident (one source record may have multiple incidents).
    incidents = []
    payload_cols = [c for c in raw.columns if not c.startswith('_')]
    payload = F.to_json(F.struct(*[F.col(quote(c)) for c in payload_cols]), options={"ignoreNullFields": "false"})
    for predicate, code, description in rules:
        filtered = df.filter(F.coalesce(predicate, F.lit(False)))
        incidents.append(filtered.select(
            F.col("_batch_id").cast("string").alias("batch_id"),
            F.lit(source).alias("source_name"),
            F.col("_source_file").cast("string").alias("source_file"),
            payload.alias("source_record_json"),
            F.lit(code).alias("rule_code"),
            F.lit(description).alias("rule_description"),
            F.current_timestamp().alias("rejected_at")))
    combined = reduce(lambda a, b: a.unionByName(b), incidents)
    plans[source] = (good_df, combined, n_source, n_good)
    if n_good + (n_source - n_good) != n_source:
        raise RuntimeError(f"Inconsistencia recuento {source}")
    print(f"DQ {source}: {n_source} leído, {n_good} aptas, {n_source-n_good} filas observadas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Persistencia idempotente
# Usamos las claves reales y verificamos cambios de negocio antes de insertar

# CELL ********************

exception_schema = spark.table("dq_exceptions.records").schema
expected_exc = [(x.name, x.dataType.simpleString()) for x in exception_schema.fields]
for src, (_, incident_df, _, _) in plans.items():
    if [(x.name, x.dataType.simpleString()) for x in incident_df.schema.fields] != expected_exc:
        raise RuntimeError(f"Contrato dq_exceptions.records incompatible desde {src}")


def merge_exceptions(incident_df):
    # Sin duplicar incidencias al repetir el mismo lote. El JSON conserva ERP_RowID.
    (DeltaTable.forName(spark, "dq_exceptions.records").alias("t")
     .merge(incident_df.alias("s"),
            "t.batch_id = s.batch_id AND t.source_name = s.source_name "
            "AND t.rule_code = s.rule_code AND t.source_record_json = s.source_record_json")
     .whenNotMatchedInsertAll().execute())


def merge_silver(source, df):
    table = f"silver.{source}"
    keys = KEYS[source]
    # En Silver la clave es de negocio, independientemente del ID identity en Docker.
    join_expr = " AND ".join(f"t.{quote(k)} = s.{quote(k)}" for k in keys)
    old = spark.table(table)
    # No permitir sobreescritura silenciosa de datos de negocio por mismo key.
    business = [c for c in df.columns if c not in ("ERP_RowID", "_batch_id", "_source_file", "_ingested_at", "_load_mode")]
    diff = reduce(lambda a,b:a|b, [~F.col("s."+quote(c)).eqNullSafe(F.col("t."+quote(c))) for c in business])
    if df.alias("s").join(old.alias("t"),
                          reduce(lambda a,b:a&b,[F.col("s."+quote(k)) == F.col("t."+quote(k)) for k in keys]),
                          "inner").filter(diff).limit(1).count():
        raise RuntimeError(f"Cambios de negocio en {table} para keys existentes: requiere aprobacion explicita")
    (DeltaTable.forName(spark, table).alias("t")
     .merge(df.alias("s"), join_expr)
     .whenNotMatchedInsertAll().execute())


def write_audit(source, total, good, status):
    schema = spark.table("audit.ingestion_runs").schema
    now = datetime.now(timezone.utc)
    rec = [(run_id or "manual", "NB_IT4W_02_Bronze_To_Silver", source, batch_id,
            "silver", status, now, now, int(total), int(good), int(total-good), None)]
    spark.createDataFrame(rec, schema).write.format("delta").mode("append").saveAsTable("audit.ingestion_runs")


# Revalidar conflictos entre lote y datos Silver existentes antes de escribir cualquier fuente.
for source in FUENTES:
    good = plans[source][0]
    existing = spark.table(f"silver.{source}")
    keys = KEYS[source]
    pred = reduce(lambda a,b:a&b,[F.col("s."+quote(k)) == F.col("t."+quote(k)) for k in keys])
    business = [c for c in good.columns if c not in ("ERP_RowID", "_batch_id", "_source_file", "_ingested_at", "_load_mode")]
    diff = reduce(lambda a,b:a|b,[~F.col("s."+quote(c)).eqNullSafe(F.col("t."+quote(c))) for c in business])
    if good.alias("s").join(existing.alias("t"), pred, "inner").filter(diff).limit(1).count():
        raise RuntimeError(f"Conflicto con Silver existente en {source}. Se detiene ANTES de escribir.")

summary = []
for source in FUENTES:
    good, inc, read_n, valid_n = plans[source]
    merge_exceptions(inc)
    merge_silver(source, good)
    write_audit(source, read_n, valid_n, "SUCCEEDED_WITH_DQ" if read_n > valid_n else "SUCCEEDED")
    summary.append((source, read_n, valid_n, read_n-valid_n))
    print(f"SILVER OK: {source}: {valid_n} aptas; {read_n-valid_n} observadas")

display(spark.createDataFrame(summary,["fuente", "filas_bronze_lote", "filas_aptas", "filas_con_incidencia"]))
if any(x[3] > 0 for x in summary):
    print("ALERTA CALIDAD: existen filas con incidencias en dq_exceptions.records. Revisar antes de publicar indicadores.")
print("Silver finalizado. Las filas Bronze no se eliminaron.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
