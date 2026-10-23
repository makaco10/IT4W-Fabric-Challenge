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

# **NB_IT4W_03_Silver_To_Gold**

# MARKDOWN ********************

# Parametros

# CELL ********************

batch_id = "historico_2024_2025" 
run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Configuracion y API

# CELL ********************

import re
import calendar
from datetime import datetime, date
from decimal import Decimal
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from delta.tables import DeltaTable
from pyspark.sql import functions as F, types as T, Window

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
if not batch_id:
    raise ValueError("batch_id obligatorio")

# Nexo Retail: importes de origen USD; pais de VENTA = pais de TIENDA.
# ECB en Frankfurter v1; ultimo tipo de cambio disponible de cada mes.
CURRENCY = {"MX": "MXN", "BR": "BRL", "IT": "EUR"}


def q(n):
    return '`' + n.replace('`','``') + '`'


def country(expr):
    normalized = F.upper(F.trim(expr))
    return (F.when(normalized.isin("MÉXICO", "MEXICO"), F.lit("MX"))
             .when(normalized.isin("BRASIL", "BRAZIL"), F.lit("BR"))
             .when(normalized.isin("ITALIA", "ITALY"), F.lit("IT")))


def currency(expr):
    return (F.when(expr == "MX", F.lit("MXN"))
             .when(expr == "BR", F.lit("BRL"))
             .when(expr == "IT", F.lit("EUR")))


def cast_money(col, precision=22):
    return F.round(col, 2).cast(T.DecimalType(precision, 2))


required = ["silver.ventas", "silver.devoluciones", "silver.costos", "silver.precios", "silver.objetivos"]
for name in required:
    if not spark.catalog.tableExists(name):
        raise RuntimeError(f"Falta tabla Silver validada: {name}")
ventas = spark.table("silver.ventas")
devoluciones = spark.table("silver.devoluciones")
costos = spark.table("silver.costos")
precios = spark.table("silver.precios")
objetivos = spark.table("silver.objetivos")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Contratos Gold

# CELL ********************

GOLD_SPECS = {
    "gold.fx_mensual": [
        ("Periodo", "STRING", True), ("Moneda", "STRING", True),
        ("Fecha_Cotizacion", "DATE", True), ("Tasa_USD_Local", "DECIMAL(18,8)", True),
        ("Proveedor", "STRING", True)],
    "gold.fact_ventas": [
        ("ID_Venta", "INT", True), ("Fecha", "DATE", True),
        ("ID_Producto", "INT", True), ("ID_Tienda", "INT", True),
        ("Periodo", "STRING", True), ("Categoria_Producto", "STRING", False),
        ("Pais", "STRING", True), ("Moneda", "STRING", True),
        ("Cantidad", "INT", True), ("Precio_Unitario_USD", "DECIMAL(18,2)", True),
        ("Venta_Bruta_USD", "DECIMAL(22,2)", True),
        ("Costo_Unitario_USD", "DECIMAL(18,2)", False),
        ("Costo_Total_USD", "DECIMAL(22,2)", False),
        ("Margen_Bruto_USD", "DECIMAL(22,2)", False),
        ("Tasa_USD_Local", "DECIMAL(18,8)", True),
        ("Fecha_Cotizacion", "DATE", True),
        ("Venta_Bruta_Local", "DECIMAL(24,2)", True),
        ("_batch_id", "STRING", True)],
    "gold.fact_devoluciones": [
        ("ID_Devolucion", "INT", True), ("ID_Venta", "INT", True),
        ("Fecha", "DATE", True), ("ID_Producto", "INT", True),
        ("ID_Tienda", "INT", True), ("Periodo", "STRING", True),
        ("Pais", "STRING", True), ("Moneda", "STRING", True),
        ("Cantidad_Devuelta", "INT", True),
        ("Precio_Unitario_USD", "DECIMAL(18,2)", True),
        ("Devolucion_USD", "DECIMAL(22,2)", True),
        ("Tasa_USD_Local", "DECIMAL(18,8)", True),
        ("Fecha_Cotizacion", "DATE", True),
        ("Devolucion_Local", "DECIMAL(24,2)", True),
        ("_batch_id", "STRING", True)],
    "gold.fact_objetivos": [
        ("Periodo", "STRING", True), ("ID_Tienda", "INT", True),
        ("Objetivo_USD", "DECIMAL(18,2)", True),
        # Una tienda con objetivo pero sin venta no tiene pais confirmado: conservar la meta
        # con divisa pendiente y dejar incidencia, en lugar de perderla o inventar un pais.
        ("Pais", "STRING", False), ("Moneda", "STRING", False),
        ("Tasa_USD_Local", "DECIMAL(18,8)", False),
        ("Objetivo_Local", "DECIMAL(24,2)", False),
        ("_batch_id", "STRING", True)],
    "gold.lista_precios": [
        ("ID_Producto", "INT", True), ("Vigencia_Desde", "DATE", True),
        ("Precio_Lista_USD", "DECIMAL(18,2)", True),
        ("_batch_id", "STRING", True)],
    "gold.dim_producto": [
        ("ID_Producto", "INT", True), ("Nombre_Producto", "STRING", False),
        ("Categoria_Producto", "STRING", False), ("Subcategoria_Producto", "STRING", False)],
    "gold.dim_tienda": [
        ("ID_Tienda", "INT", True), ("Nombre_Tienda", "STRING", False),
        ("Pais", "STRING", True), ("Moneda", "STRING", True)],
}


def parsed_type(sqltype):
    primitives = {"INT": T.IntegerType(), "STRING": T.StringType(), "DATE": T.DateType()}
    if sqltype in primitives:
        return primitives[sqltype]
    match = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", sqltype)
    if match:
        return T.DecimalType(int(match.group(1)), int(match.group(2)))
    raise RuntimeError(f"Tipo fuera de contrato: {sqltype}")


def ensure_table(name):
    specs = GOLD_SPECS[name]
    expected = [(n, parsed_type(t).simpleString(), not required) for n,t,required in specs]
    if not spark.catalog.tableExists(name):
        columns = [f"{q(n)} {t}" + (" NOT NULL" if required else "") for n,t,required in specs]
        spark.sql(f"CREATE TABLE {name} ({', '.join(columns)}) USING DELTA")
    actual = [(x.name, x.dataType.simpleString(), x.nullable) for x in spark.table(name).schema.fields]
    if actual != expected:
        raise RuntimeError(f"Contrato fisico incompatible {name}: {actual} != {expected}")


def conform(df, name):
    """Nunca aceptar columnas nuevas o faltantes, ni cast automatico por evolucion."""
    specs = GOLD_SPECS[name]
    cols = [n for n,_,_ in specs]
    if df.columns != cols:
        raise RuntimeError(f"Proyeccion incorrecta {name}: {df.columns} frente a {cols}")
    for n, _, required in specs:
        if required and df.filter(F.col(q(n)).isNull()).limit(1).count():
            raise RuntimeError(f"{name}.{n}: columna obligatoria nula")
    for n, sqltype, _ in specs:
        if df.schema[n].dataType != parsed_type(sqltype):
            raise RuntimeError(f"{name}.{n}: tipo recibido {df.schema[n].dataType} != {sqltype}")
    return df


for name in GOLD_SPECS:
    ensure_table(name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Tipo de Cambio
# Descarga historica o lectura cacheada. Fallamos de forma visible si faltan tasas; jamas las sustituimos por 1

# CELL ********************

# A diferencia de 'latest', el rango permite verificar la fecha real de cada cotizacion.
months = set()
for tbl in [ventas, devoluciones]:
    months.update(r[0] for r in tbl.select(F.date_format("Fecha", "yyyy-MM").alias("mes")).distinct().collect())
months.update(r[0] for r in objetivos.select("Periodo").distinct().collect())
months = sorted(x for x in months if x is not None)
if not months:
    raise RuntimeError("No hay meses en Silver")

cached = spark.table("gold.fx_mensual")
existing = {(r[0], r[1]) for r in cached.select("Periodo", "Moneda").collect()}
missing_months = [m for m in months if any((m, c) not in existing for c in CURRENCY.values())]
new_rates = []
if missing_months:
    # Obtener TODA la serie una sola vez para evitar 24+ llamadas a la API.
    beginning = min(missing_months) + "-01"
    latest = max(missing_months)
    year, month = map(int, latest.split("-"))
    ending = f"{year:04d}-{month:02d}-{calendar.monthrange(year, month)[1]:02d}"
    url = f"https://api.frankfurter.dev/v1/{beginning}..{ending}"
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1,
                   status_forcelist=[429,500,502,503,504])))
    try:
        response = session.get(url, params={"base": "USD", "symbols": "MXN,BRL,EUR"}, timeout=35)
        response.raise_for_status()
        doc = response.json()
        if doc.get("base") != "USD" or not isinstance(doc.get("rates"), dict):
            raise RuntimeError("Respuesta Frankfurter no corresponde a USD o carece de serie")
        series = doc["rates"]
        for mes in missing_months:
            available = sorted(d for d in series if d.startswith(mes + "-") and d <= ending)
            if not available:
                raise RuntimeError(f"Frankfurter sin cotizaciones en {mes}")
            last_date = available[-1]
            for symbol in CURRENCY.values():
                rate = series[last_date].get(symbol)
                if rate is None or Decimal(str(rate)) <= 0:
                    raise RuntimeError(f"Sin USD/{symbol} valido para {mes}, fecha {last_date}")
                new_rates.append((mes, symbol, date.fromisoformat(last_date),
                                  Decimal(str(rate)), "Frankfurter v1 / ECB"))
    except Exception as exc:
        raise RuntimeError(
            "No se obtuvo FX oficial. NO se fabrican tasas ni se sustituyen por 1. "
            f"Error: {exc}") from exc

if new_rates:
    spec = GOLD_SPECS["gold.fx_mensual"]
    schema = T.StructType([T.StructField(n, parsed_type(t), not required) for n,t,required in spec])
    incoming_rates = conform(spark.createDataFrame(new_rates, schema), "gold.fx_mensual")
    (DeltaTable.forName(spark,"gold.fx_mensual").alias("t")
       .merge(incoming_rates.alias("s"),"t.Periodo=s.Periodo AND t.Moneda=s.Moneda")
       .whenNotMatchedInsertAll().execute())
fx = spark.table("gold.fx_mensual")
required_set = {(m,c) for m in months for c in CURRENCY.values()}
found = {(r[0],r[1]) for r in fx.select("Periodo", "Moneda").collect()}
if required_set - found:
    raise RuntimeError(f"Tasas FX pendientes: {sorted(required_set-found)}")
if fx.groupBy("Periodo", "Moneda").count().where(F.col("count") > 1).limit(1).count():
    raise RuntimeError("FX mensual duplicado; se detiene modelado Gold")
print(f"FX VALIDADO: {len(required_set)} cotizaciones mes-moneda en historico")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Modelo dimensional
# Se construyen dimensiones de producto y tienda a partir de los datos Silver validados

# CELL ********************

latest_prod = Window.partitionBy("ID_Producto").orderBy(F.col("Fecha").desc(), F.col("ID_Venta").desc())
dim_product = (ventas.withColumn("__r", F.row_number().over(latest_prod)).filter(F.col("__r") == 1)
  .select("ID_Producto", "Nombre_Producto", F.col("Categoría_Producto").alias("Categoria_Producto"),
          F.col("Subcategoría_Producto").alias("Subcategoria_Producto")))
latest_store = Window.partitionBy("ID_Tienda").orderBy(F.col("Fecha").desc(), F.col("ID_Venta").desc())
dim_store = (ventas.withColumn("__r", F.row_number().over(latest_store)).filter(F.col("__r") == 1)
  .select("ID_Tienda", "Nombre_Tienda", country(F.col("Ubicación_Tienda")).alias("Pais"))
  .withColumn("Moneda", currency(F.col("Pais"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Hechos de ventas y devoluciones
# Mantiene ventas sin costo para poder reportar faltantes. La devolucion usa su propia fecha para la conversion.

# CELL ********************

vv = (ventas.withColumn("Periodo", F.date_format("Fecha", "yyyy-MM"))
  .withColumn("Pais", country(F.col("Ubicación_Tienda")))
  .withColumn("Moneda", currency(F.col("Pais"))))
c = costos.select(F.col("ID_Producto").alias("c_prod"),F.col("Periodo").alias("c_mes"),
                 F.col("Costo_Unitario").alias("Costo_Unitario_USD"))
f = fx.select(F.col("Periodo").alias("f_mes"), F.col("Moneda").alias("f_moneda"),
              "Tasa_USD_Local", "Fecha_Cotizacion")
pre_join_v = vv.count()
joined_v = (vv.join(c, (vv.ID_Producto == c.c_prod) & (vv.Periodo == c.c_mes), "left")
    .join(f, (vv.Periodo == f.f_mes) & (vv.Moneda == f.f_moneda), "left"))
if joined_v.count() != pre_join_v:
    raise RuntimeError("Fact ventas multiplicada: revisar unicidad en Costos o FX")
venta = F.col("Cantidad") * F.col("Precio_Unitario")
costo = F.col("Cantidad") * F.col("Costo_Unitario_USD")
v_fact = joined_v.select(
    "ID_Venta", "Fecha", "ID_Producto", "ID_Tienda", "Periodo",
    F.col("Categoría_Producto").alias("Categoria_Producto"), "Pais", "Moneda", "Cantidad",
    F.col("Precio_Unitario").alias("Precio_Unitario_USD"),
    cast_money(venta).alias("Venta_Bruta_USD"), "Costo_Unitario_USD",
    cast_money(costo).alias("Costo_Total_USD"),
    cast_money(venta - costo).alias("Margen_Bruto_USD"),
    "Tasa_USD_Local", "Fecha_Cotizacion",
    cast_money(venta * F.col("Tasa_USD_Local"), 24).alias("Venta_Bruta_Local"), "_batch_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Objetivos y precios
# Granularidad tienda–mes para objetivos, vigencias por producto en precios

# CELL ********************

dd = (devoluciones.withColumn("Periodo", F.date_format("Fecha", "yyyy-MM"))
    .withColumn("Pais", country(F.col("Ubicación_Tienda")))
    .withColumn("Moneda", currency(F.col("Pais"))))
pre_join_d = dd.count()
joined_d = dd.join(f, (dd.Periodo == f.f_mes) & (dd.Moneda == f.f_moneda), "left")
if joined_d.count() != pre_join_d:
    raise RuntimeError("Fact devoluciones multiplicada: FX mensual duplicado")
ret_amount = F.col("Cantidad_Devuelta") * F.col("Precio_Unitario")
d_fact = joined_d.select(
    F.col("ID_Devolución").alias("ID_Devolucion"), "ID_Venta", "Fecha", "ID_Producto", "ID_Tienda",
    "Periodo", "Pais", "Moneda", "Cantidad_Devuelta",
    F.col("Precio_Unitario").alias("Precio_Unitario_USD"),
    cast_money(ret_amount).alias("Devolucion_USD"), "Tasa_USD_Local", "Fecha_Cotizacion",
    cast_money(ret_amount * F.col("Tasa_USD_Local"),24).alias("Devolucion_Local"), "_batch_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Persistencia y pruebas
# Merge por claves de negocio y verificaciones de cardinalidad, moneda y ausencia de duplicados

# CELL ********************

st = dim_store.select(F.col("ID_Tienda").alias("s_id"),"Pais","Moneda")
o = objetivos.join(st, objetivos.ID_Tienda == st.s_id, "left")
o = o.join(f,(o.Periodo == f.f_mes) & (o.Moneda == f.f_moneda), "left")
if o.count() != objetivos.count():
    raise RuntimeError("Objetivos multiplicados por dimension tienda o FX")
o_fact = o.select("Periodo", "ID_Tienda", F.col("Objetivo_Ventas").alias("Objetivo_USD"),
   "Pais", "Moneda", "Tasa_USD_Local",
   cast_money(F.col("Objetivo_Ventas") * F.col("Tasa_USD_Local"),24).alias("Objetivo_Local"),"_batch_id")
objetivos_sin_tienda = o_fact.filter(F.col("Pais").isNull()).count()
if objetivos_sin_tienda:
    print(f"ALERTA CALIDAD: {objetivos_sin_tienda} metas sin pais verificable por dimension tienda. "
          "Se conserva objetivo USD con valor local NULL; requiere revision de tienda.")
    # Incidencia idempotente sobre el contrato existente de dq_exceptions.records.
    unknown_keys = o_fact.where(F.col("Pais").isNull()).select("Periodo", "ID_Tienda")
    observed = objetivos.join(unknown_keys, ["Periodo", "ID_Tienda"], "inner")
    raw_cols = [c for c in objetivos.columns if not c.startswith("_")]
    payload = F.to_json(F.struct(*[F.col(q(c)) for c in raw_cols]),
                        options={"ignoreNullFields": "false"})
    incidents = observed.select(
        F.col("_batch_id").alias("batch_id"), F.lit("objetivos").alias("source_name"),
        F.col("_source_file").alias("source_file"), payload.alias("source_record_json"),
        F.lit("STORE_REFERENCE_MISSING").alias("rule_code"),
        F.lit("No es posible inferir la moneda de un objetivo sin tienda asociada").alias("rule_description"),
        F.current_timestamp().alias("rejected_at"))
    (DeltaTable.forName(spark,"dq_exceptions.records").alias("t")
      .merge(incidents.alias("s"), "t.batch_id=s.batch_id AND t.source_name=s.source_name "
             "AND t.rule_code=s.rule_code AND t.source_record_json=s.source_record_json")
      .whenNotMatchedInsertAll().execute())

list_prices = precios.select("ID_Producto", "Vigencia_Desde",
  F.col("Precio_Lista").alias("Precio_Lista_USD"),"_batch_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Persistencia y pruebas
# Merge por claves de negocio y verificaciones de cardinalidad, moneda y ausencia de duplicados

# CELL ********************

frames = {
    "gold.fact_ventas": v_fact, "gold.fact_devoluciones": d_fact,
    "gold.fact_objetivos": o_fact, "gold.lista_precios": list_prices,
    "gold.dim_producto": dim_product, "gold.dim_tienda": dim_store,
}
for name, frame in frames.items():
    conform(frame, name)

# Solo Costos ausentes pueden dejar margen sin calcular, no eliminamos la venta.
no_cost = v_fact.filter(F.col("Costo_Unitario_USD").isNull()).count()
print(f"CONTROL: Ventas sin costo mensual disponible = {no_cost} (conservadas para consulta #4)")

key_columns = {
    "gold.fact_ventas": ["ID_Venta"], "gold.fact_devoluciones": ["ID_Devolucion"],
    "gold.fact_objetivos": ["Periodo", "ID_Tienda"],
    "gold.lista_precios": ["ID_Producto", "Vigencia_Desde"],
    "gold.dim_producto": ["ID_Producto"], "gold.dim_tienda": ["ID_Tienda"],
}

# Si las claves son unicas, actualizar valores derivados es licito ejemplo costo tardio
# los esquemas fisicos NUNCA evolucionan. No se borran filas de históricos.
for name, frame in frames.items():
    keys = key_columns[name]
    if frame.groupBy(*[F.col(q(k)) for k in keys]).count().where(F.col("count") > 1).limit(1).count():
        raise RuntimeError(f"Duplicados Gold: {name} en {keys}")

for name, frame in frames.items():
    keys = key_columns[name]
    equality = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in keys)
    (DeltaTable.forName(spark,name).alias("t").merge(frame.alias("s"),equality)
      .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    print(f"GOLD OK {name}: {spark.table(name).count()} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert spark.table("gold.fact_ventas").count() == ventas.count(), "Ventas Gold ≠ Silver"
assert spark.table("gold.fact_devoluciones").count() == devoluciones.count(), "Devoluciones Gold ≠ Silver"
assert spark.table("gold.fact_objetivos").count() == objetivos.count(), "Objetivos Gold ≠ Silver"
assert spark.table("gold.lista_precios").count() == precios.count(), "Precios Gold ≠ Silver"
if v_fact.filter(F.col("Tasa_USD_Local").isNull()).limit(1).count():
    raise AssertionError("Ventas con FX no disponible")
if d_fact.filter(F.col("Tasa_USD_Local").isNull()).limit(1).count():
    raise AssertionError("Devoluciones con FX no disponible")
print("TESTS Gold OK: cardinalidad conservada y FX disponible")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
