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

# **NB_IT4W_00_Initialize_Contracts_PySpark**

# MARKDOWN ********************

# # . NexoRetail — Contrato de Datos


# CELL ********************

import re
from pyspark.sql import types as T

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
SCHEMAS = ("bronze", "silver", "gold", "dq_exceptions", "audit")

def spark_type(type_name: str) -> T.DataType:
    # Convierte solo los tipos de explicatamente aprobados del contrato
	
    primitive = {
        "BIGINT": T.LongType(),
        "INT": T.IntegerType(),
        "STRING": T.StringType(),
        "DATE": T.DateType(),
        "TIMESTAMP": T.TimestampType(),
    }
    name = type_name.strip().upper()
    if name in primitive:
        return primitive[name]
    decimal_match = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", name)
    if decimal_match:
        return T.DecimalType(int(decimal_match.group(1)), int(decimal_match.group(2)))
    raise ValueError(f"Tipo no permitido por el contrato: {type_name!r}")


def build_schema(column_specs: list[dict]) -> T.StructType:
    return T.StructType([
        T.StructField(
            item["name"],
            spark_type(item["spark_type"]),
            nullable=item["nullable"]
        )
        for item in column_specs
    ])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## . Contracts

# CELL ********************

CONTRACTS = {'costos': [{'name': 'ERP_RowID',
             'spark_type': 'BIGINT',
             'nullable': False,
             'sql_type': 'bigint',
             'max_chars': None,
             'identity': True},
            {'name': 'Periodo',
             'spark_type': 'STRING',
             'nullable': True,
             'sql_type': 'char',
             'max_chars': 7,
             'identity': False},
            {'name': 'ID_Producto',
             'spark_type': 'INT',
             'nullable': True,
             'sql_type': 'int',
             'max_chars': None,
             'identity': False},
            {'name': 'Costo_Unitario',
             'spark_type': 'DECIMAL(18,2)',
             'nullable': True,
             'sql_type': 'decimal',
             'max_chars': None,
             'identity': False}],
 'devoluciones': [{'name': 'ERP_RowID',
                   'spark_type': 'BIGINT',
                   'nullable': False,
                   'sql_type': 'bigint',
                   'max_chars': None,
                   'identity': True},
                  {'name': 'ID_Devolución',
                   'spark_type': 'INT',
                   'nullable': True,
                   'sql_type': 'int',
                   'max_chars': None,
                   'identity': False},
                  {'name': 'ID_Venta',
                   'spark_type': 'INT',
                   'nullable': True,
                   'sql_type': 'int',
                   'max_chars': None,
                   'identity': False},
                  {'name': 'Fecha',
                   'spark_type': 'DATE',
                   'nullable': True,
                   'sql_type': 'date',
                   'max_chars': None,
                   'identity': False},
                  {'name': 'ID_Producto',
                   'spark_type': 'INT',
                   'nullable': True,
                   'sql_type': 'int',
                   'max_chars': None,
                   'identity': False},
                  {'name': 'ID_Cliente',
                   'spark_type': 'INT',
                   'nullable': True,
                   'sql_type': 'int',
                   'max_chars': None,
                   'identity': False},
                  {'name': 'ID_Tienda',
                   'spark_type': 'INT',
                   'nullable': True,
                   'sql_type': 'int',
                   'max_chars': None,
                   'identity': False},
                  {'name': 'Cantidad_Devuelta',
                   'spark_type': 'INT',
                   'nullable': True,
                   'sql_type': 'int',
                   'max_chars': None,
                   'identity': False},
                  {'name': 'Motivo',
                   'spark_type': 'STRING',
                   'nullable': True,
                   'sql_type': 'nvarchar',
                   'max_chars': 200,
                   'identity': False},
                  {'name': 'Precio_Unitario',
                   'spark_type': 'DECIMAL(18,2)',
                   'nullable': True,
                   'sql_type': 'decimal',
                   'max_chars': None,
                   'identity': False},
                  {'name': 'Nombre_Cliente',
                   'spark_type': 'STRING',
                   'nullable': True,
                   'sql_type': 'nvarchar',
                   'max_chars': 200,
                   'identity': False},
                  {'name': 'Ubicación_Cliente',
                   'spark_type': 'STRING',
                   'nullable': True,
                   'sql_type': 'nvarchar',
                   'max_chars': 100,
                   'identity': False},
                  {'name': 'Nombre_Tienda',
                   'spark_type': 'STRING',
                   'nullable': True,
                   'sql_type': 'nvarchar',
                   'max_chars': 200,
                   'identity': False},
                  {'name': 'Ubicación_Tienda',
                   'spark_type': 'STRING',
                   'nullable': True,
                   'sql_type': 'nvarchar',
                   'max_chars': 100,
                   'identity': False},
                  {'name': 'Nombre_Producto',
                   'spark_type': 'STRING',
                   'nullable': True,
                   'sql_type': 'nvarchar',
                   'max_chars': 200,
                   'identity': False},
                  {'name': 'Categoría_Producto',
                   'spark_type': 'STRING',
                   'nullable': True,
                   'sql_type': 'nvarchar',
                   'max_chars': 100,
                   'identity': False}],
 'objetivos': [{'name': 'ERP_RowID',
                'spark_type': 'BIGINT',
                'nullable': False,
                'sql_type': 'bigint',
                'max_chars': None,
                'identity': True},
               {'name': 'Periodo',
                'spark_type': 'STRING',
                'nullable': True,
                'sql_type': 'char',
                'max_chars': 7,
                'identity': False},
               {'name': 'ID_Tienda',
                'spark_type': 'INT',
                'nullable': True,
                'sql_type': 'int',
                'max_chars': None,
                'identity': False},
               {'name': 'Objetivo_Ventas',
                'spark_type': 'DECIMAL(18,2)',
                'nullable': True,
                'sql_type': 'decimal',
                'max_chars': None,
                'identity': False}],
 'precios': [{'name': 'ERP_RowID',
              'spark_type': 'BIGINT',
              'nullable': False,
              'sql_type': 'bigint',
              'max_chars': None,
              'identity': True},
             {'name': 'ID_Producto',
              'spark_type': 'INT',
              'nullable': True,
              'sql_type': 'int',
              'max_chars': None,
              'identity': False},
             {'name': 'Vigencia_Desde',
              'spark_type': 'DATE',
              'nullable': True,
              'sql_type': 'date',
              'max_chars': None,
              'identity': False},
             {'name': 'Precio_Lista',
              'spark_type': 'DECIMAL(18,2)',
              'nullable': True,
              'sql_type': 'decimal',
              'max_chars': None,
              'identity': False}],
 'ventas': [{'name': 'ERP_RowID',
             'spark_type': 'BIGINT',
             'nullable': False,
             'sql_type': 'bigint',
             'max_chars': None,
             'identity': True},
            {'name': 'ID_Venta',
             'spark_type': 'INT',
             'nullable': True,
             'sql_type': 'int',
             'max_chars': None,
             'identity': False},
            {'name': 'ID_Pedido',
             'spark_type': 'INT',
             'nullable': True,
             'sql_type': 'int',
             'max_chars': None,
             'identity': False},
            {'name': 'Fecha',
             'spark_type': 'DATE',
             'nullable': True,
             'sql_type': 'date',
             'max_chars': None,
             'identity': False},
            {'name': 'ID_Producto',
             'spark_type': 'INT',
             'nullable': True,
             'sql_type': 'int',
             'max_chars': None,
             'identity': False},
            {'name': 'ID_Cliente',
             'spark_type': 'INT',
             'nullable': True,
             'sql_type': 'int',
             'max_chars': None,
             'identity': False},
            {'name': 'ID_Tienda',
             'spark_type': 'INT',
             'nullable': True,
             'sql_type': 'int',
             'max_chars': None,
             'identity': False},
            {'name': 'Cantidad',
             'spark_type': 'INT',
             'nullable': True,
             'sql_type': 'int',
             'max_chars': None,
             'identity': False},
            {'name': 'Precio_Unitario',
             'spark_type': 'DECIMAL(18,2)',
             'nullable': True,
             'sql_type': 'decimal',
             'max_chars': None,
             'identity': False},
            {'name': 'Canal_Venta',
             'spark_type': 'STRING',
             'nullable': True,
             'sql_type': 'nvarchar',
             'max_chars': 30,
             'identity': False},
            {'name': 'Categoría_Venta',
             'spark_type': 'STRING',
             'nullable': True,
             'sql_type': 'nvarchar',
             'max_chars': 100,
             'identity': False},
            {'name': 'Nombre_Cliente',
             'spark_type': 'STRING',
             'nullable': True,
             'sql_type': 'nvarchar',
             'max_chars': 200,
             'identity': False},
            {'name': 'Ubicación_Cliente',
             'spark_type': 'STRING',
             'nullable': True,
             'sql_type': 'nvarchar',
             'max_chars': 100,
             'identity': False},
            {'name': 'Nombre_Tienda',
             'spark_type': 'STRING',
             'nullable': True,
             'sql_type': 'nvarchar',
             'max_chars': 200,
             'identity': False},
            {'name': 'Ubicación_Tienda',
             'spark_type': 'STRING',
             'nullable': True,
             'sql_type': 'nvarchar',
             'max_chars': 100,
             'identity': False},
            {'name': 'Nombre_Producto',
             'spark_type': 'STRING',
             'nullable': True,
             'sql_type': 'nvarchar',
             'max_chars': 200,
             'identity': False},
            {'name': 'Categoría_Producto',
             'spark_type': 'STRING',
             'nullable': True,
             'sql_type': 'nvarchar',
             'max_chars': 100,
             'identity': False},
            {'name': 'Subcategoría_Producto',
             'spark_type': 'STRING',
             'nullable': True,
             'sql_type': 'nvarchar',
             'max_chars': 100,
             'identity': False}]}

PROPOSED_BUSINESS_KEYS = {'ventas': ['ID_Venta'],
 'devoluciones': ['ID_Devolución'],
 'costos': ['Periodo', 'ID_Producto'],
 'precios': ['ID_Producto', 'Vigencia_Desde'],
 'objetivos': ['Periodo', 'ID_Tienda']}

print("Columnas por fuente:", {name: len(cols) for name, cols in CONTRACTS.items()})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### . Creando Schemas

# CELL ********************

for schema_name in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`")

print("Esquemas disponibles:", ", ".join(SCHEMAS))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### . Definiendo Esquemas para Bronze, auditoria y datos o registros con incidencias

# CELL ********************

# Definir esquemas PySpark para Bronze, auditoría y registros con incidencias
# Metadatos de ingesta (obligatorios). Sin PK/UNIQUE falsas.
INGESTION_FIELDS = [
    {"name": "_batch_id", "spark_type": "STRING", "nullable": False},
    {"name": "_source_file", "spark_type": "STRING", "nullable": False},
    {"name": "_ingested_at", "spark_type": "TIMESTAMP", "nullable": False},
    {"name": "_load_mode", "spark_type": "STRING", "nullable": False},
]

BRONZE_SCHEMAS = {
    f"bronze.{source}": build_schema(columns + INGESTION_FIELDS)
    for source, columns in CONTRACTS.items()
}

CONTROL_SPECS = {'audit.ingestion_runs': [('run_id', 'STRING', False),
                          ('pipeline_name', 'STRING', False),
                          ('source_name', 'STRING', False),
                          ('batch_id', 'STRING', False),
                          ('load_mode', 'STRING', False),
                          ('status', 'STRING', False),
                          ('started_at', 'TIMESTAMP', False),
                          ('finished_at', 'TIMESTAMP', True),
                          ('rows_read', 'BIGINT', True),
                          ('rows_loaded', 'BIGINT', True),
                          ('rows_rejected', 'BIGINT', True),
                          ('error_message', 'STRING', True)],
 'audit.batch_registry': [('batch_id', 'STRING', False),
                          ('source_name', 'STRING', False),
                          ('load_mode', 'STRING', False),
                          ('landing_path', 'STRING', False),
                          ('batch_status', 'STRING', False),
                          ('file_sha256', 'STRING', True),
                          ('created_at', 'TIMESTAMP', False),
                          ('processed_at', 'TIMESTAMP', True)],
 'dq_exceptions.records': [('batch_id', 'STRING', False),
                              ('source_name', 'STRING', False),
                              ('source_file', 'STRING', True),
                              ('source_record_json', 'STRING', False),
                              ('rule_code', 'STRING', False),
                              ('rule_description', 'STRING', True),
                              ('rejected_at', 'TIMESTAMP', False)]}
CONTROL_SCHEMAS = {
    table_name: T.StructType([
        T.StructField(col_name, spark_type(type_name), nullable)
        for col_name, type_name, nullable in columns
    ])
    for table_name, columns in CONTROL_SPECS.items()
}

ALL_TABLE_SCHEMAS = {**BRONZE_SCHEMAS, **CONTROL_SCHEMAS}
assert len(BRONZE_SCHEMAS) == 5 and len(CONTROL_SCHEMAS) == 3
print("Tablas previstas:", list(ALL_TABLE_SCHEMAS))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### . Creacion de Tablas y Contrato estricto

# CELL ********************

REBUILD_EMPTY_TABLES = False 

def ddl_table_name(full_name: str) -> str:
    """Cita schema.tabla para el DDL; los nombres provienen del contrato."""
    parts = full_name.split(".")
    if len(parts) != 2 or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", x) for x in parts):
        raise ValueError(f"Nombre de tabla inválido: {full_name!r}")
    return ".".join(f"`{p}`" for p in parts)


def ddl_create_delta(table_name: str, schema: T.StructType) -> None:
    """Usa SQL solo para la declaración física de constraints."""
    column_definitions = []
    for field in schema.fields:
        if not field.name.isidentifier():
            raise ValueError(f"Nombre de columna invalido: {field.name!r}")
        nullability = "" if field.nullable else " NOT NULL"
        column_definitions.append(
            f"`{field.name}` {field.dataType.simpleString().upper()}{nullability}"
        )

    ddl = (
        f"CREATE TABLE {ddl_table_name(table_name)} (\n  "
        + ",\n  ".join(column_definitions)
        + "\n) USING DELTA"
    )
    spark.sql(ddl)


def field_signature(schema: T.StructType):
    return [
        (f.name, f.dataType.simpleString(), f.nullable)
        for f in schema.fields
    ]


def ensure_delta_table(table_name: str, expected_schema: T.StructType) -> str:
    if not spark.catalog.tableExists(table_name):
        ddl_create_delta(table_name, expected_schema)
        return "CREADA con constraints fisicos"

    actual = spark.table(table_name).schema
    if field_signature(actual) == field_signature(expected_schema):
        return "Existente y conforme; no modificada"

    # Nunca modificar ni borrar una tabla con registros.
    if spark.table(table_name).limit(1).count() != 0:
        raise RuntimeError(
            f"{table_name} tiene registros y su contrato difiere. "
            "No se realizará ninguna modificación automática."
        )

    if not REBUILD_EMPTY_TABLES:
        raise RuntimeError(
            f"{table_name} existe, está vacía y su contrato difiere. "
            "Para reconstruir SOLAMENTE tablas vacías, cambiar "
            "REBUILD_EMPTY_TABLES=True temporalmente y volver a ejecutar."
        )

    spark.sql(f"DROP TABLE {ddl_table_name(table_name)}")
    ddl_create_delta(table_name, expected_schema)
    return "RECREADA (sin registros previos) con constraints físicos"


creation_results = {
    name: ensure_delta_table(name, schema)
    for name, schema in ALL_TABLE_SCHEMAS.items()
}
for name, result in creation_results.items():
    print(f"{name}: {result}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### . Validacion de tablas — Tipos y nulabilidad fisica

# CELL ********************

def check_schema(table_name: str, expected: T.StructType) -> None:
    actual = spark.table(table_name).schema
    expected_fields = [(c.name, c.dataType.simpleString(), c.nullable) for c in expected.fields]
    actual_fields = [(c.name, c.dataType.simpleString(), c.nullable) for c in actual.fields]

    if actual_fields != expected_fields:
        raise RuntimeError(
            f"CONTRATO INCOMPATIBLE: {table_name}\n"
            f"Esperado: {expected_fields}\n"
            f"Actual:   {actual_fields}\n"
            "No se modificaron columnas existentes. Revisar el cambio en Git."
        )

    # Confirma formato Delta cuando el catálogo provee información del provider.
    for _name, _type, _nullable in actual_fields:
        if _type == "void":
            raise RuntimeError(f"Tipo VOID inesperado en {table_name}.{_name}")

    print(f"OK {table_name}: {len(actual_fields)} columnas y nulabilidad controlada")


for full_name, expected in ALL_TABLE_SCHEMAS.items():
    check_schema(full_name, expected)

print("Inicializacion y validacion completas. No se cargaron datos.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
