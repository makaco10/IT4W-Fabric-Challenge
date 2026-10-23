-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "d6d8fdd8-ef02-41dd-93b7-eae5fe8e507b",
-- META       "default_lakehouse_name": "LH_IT4W_Development",
-- META       "default_lakehouse_workspace_id": "fb156000-eb61-412c-90ff-773f978dc018",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "d6d8fdd8-ef02-41dd-93b7-eae5fe8e507b"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "known_warehouses": []
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- ## **1.Venta bruta 2025 en USD**
-- 
-- La columna Venta_Bruta_USD ya representa Cantidad × Precio_Unitario.

-- CELL ********************


SELECT
    SUM(Venta_Bruta_USD) AS Venta_Bruta_2025_USD
FROM gold.fact_ventas
WHERE Fecha >= '2025-01-01'
  AND Fecha < '2026-01-01';


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## **2.Margen bruto % 2025 por categoria**
--  Calculamos el porcentaje utilizando solamente las ventas con costo disponible. Tambien mostramos las lineas sin costo, para no presentar un margen engañoso.

-- MARKDOWN ********************


-- CELL ********************


SELECT
    Categoria_Producto,
    COUNT(*) AS Lineas_Venta,
    SUM(CASE
        WHEN Costo_Unitario_USD IS NULL THEN 1
        ELSE 0
    END) AS Lineas_Sin_Costo,
    SUM(CASE
        WHEN Costo_Unitario_USD IS NOT NULL
        THEN Venta_Bruta_USD
    END) AS Venta_Con_Costo_USD,
    SUM(Margen_Bruto_USD) AS Margen_Bruto_USD,
    ROUND(
        100.0 * SUM(Margen_Bruto_USD)
        / NULLIF(SUM(CASE
            WHEN Costo_Unitario_USD IS NOT NULL
            THEN Venta_Bruta_USD
        END), 0),
        2
    ) AS Margen_Bruto_Pct
FROM gold.fact_ventas
WHERE Fecha >= '2025-01-01'
  AND Fecha < '2026-01-01'
GROUP BY Categoria_Producto
ORDER BY Categoria_Producto


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## **2.Venta bruta 2025 por pais, moneda local**
-- Utilizamos las conversiones registradas en Gold y agrupamos por pais y moneda. No sumamos importes de distintas divisas.


-- CELL ********************


SELECT
    Pais,
    Moneda,
    SUM(Venta_Bruta_Local)
        AS Venta_Bruta_2025_Local
FROM gold.fact_ventas
WHERE Fecha >= '2025-01-01'
  AND Fecha < '2026-01-01'
GROUP BY Pais, Moneda
ORDER BY Pais, Moneda


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## **4.Enero 2026: venta bruta, lineas y costos faltantes**
-- 
-- Esta es especialmente importante para nuestro incremental, porque algunas ventas pueden no encontrar costo mensual.

-- CELL ********************


SELECT
    SUM(Venta_Bruta_USD)
        AS Venta_Bruta_Enero_2026_USD,
    COUNT(*) AS Lineas_Venta,
    SUM(CASE
        WHEN Costo_Unitario_USD IS NULL THEN 1
        ELSE 0
    END) AS Lineas_Sin_Costo
FROM gold.fact_ventas
WHERE Fecha >= '2026-01-01'
  AND Fecha < '2026-02-01'


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## **5.Precio de lista vigente de Smart TV 55" al 20/01/2026**
-- 
-- No buscamos el precio cobrado en una venta. Buscamos el ultimo precio de lista cuya vigencia comenzo hasta el 20 de enero inclusive.

-- CELL ********************


WITH precios_vigentes AS (
    SELECT
        d.ID_Producto,
        d.Nombre_Producto,
        p.Vigencia_Desde,
        p.Precio_Lista_USD,
        ROW_NUMBER() OVER (
            PARTITION BY p.ID_Producto
            ORDER BY p.Vigencia_Desde DESC
        ) AS rn
    FROM gold.lista_precios p
    INNER JOIN gold.dim_producto d
        ON d.ID_Producto = p.ID_Producto
    WHERE LOWER(d.Nombre_Producto)
          LIKE '%smart tv 55%'
      AND p.Vigencia_Desde <= '2026-01-20'
)
SELECT
    ID_Producto,
    Nombre_Producto,
    Vigencia_Desde,
    Precio_Lista_USD
FROM precios_vigentes
WHERE rn = 1
ORDER BY ID_Producto


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## **6. Idempotencia: ejecutar dos veces el pipeline**
-- Esta consulta debe ejecutarse despues de la primera corrida completa y nuevamente despues de la segunda.

-- CELL ********************


SELECT
    COUNT(*) AS Filas_Ventas_Final,
    COUNT(DISTINCT ID_Venta)
        AS IDs_Venta_Distintos,
    COUNT(*) - COUNT(DISTINCT ID_Venta)
        AS Duplicados_ID_Venta
FROM gold.fact_ventas


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************




SELECT
    COUNT_BIG(*) AS Total_Ventas,
    COUNT_BIG(DISTINCT ID_Venta) AS Ventas_Unicas,
    COUNT_BIG(*) - COUNT_BIG(DISTINCT ID_Venta) AS Duplicados
FROM gold.fact_ventas;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************


-- MARKDOWN ********************

