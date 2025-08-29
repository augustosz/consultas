-- 1. Common Table Expressions (CTEs)
-- CTE básica para ventas mensuales
WITH ventas_mensuales AS (
    SELECT 
        DATE_TRUNC('month', fecha_venta) as mes,
        SUM(monto) as total_ventas,
        COUNT(*) as num_transacciones
    FROM transacciones 
    WHERE fecha_venta >= '2024-01-01'
    GROUP BY DATE_TRUNC('month', fecha_venta)
)
SELECT 
    mes,
    total_ventas,
    LAG(total_ventas) OVER (ORDER BY mes) as ventas_mes_anterior,
    total_ventas - LAG(total_ventas) OVER (ORDER BY mes) as diferencia
FROM ventas_mensuales
ORDER BY mes;

-- CTE recursiva para jerarquía de empleados
WITH RECURSIVE jerarquia_empleados AS (
    SELECT id_empleado, nombre, id_jefe, 0 as nivel
    FROM empleados 
    WHERE id_jefe IS NULL
    UNION ALL
    SELECT e.id_empleado, e.nombre, e.id_jefe, j.nivel + 1
    FROM empleados e
    INNER JOIN jerarquia_empleados j ON e.id_jefe = j.id_empleado
)
SELECT * FROM jerarquia_empleados
ORDER BY nivel, nombre;

-- Múltiples CTEs para clientes activos y productos populares
WITH 
clientes_activos AS (
    SELECT cliente_id, COUNT(*) as total_pedidos
    FROM pedidos 
    WHERE fecha_pedido >= CURRENT_DATE - INTERVAL '1 year'
    GROUP BY cliente_id
    HAVING COUNT(*) >= 5
),
productos_populares AS (
    SELECT producto_id, COUNT(*) as veces_vendido, SUM(cantidad) as cantidad_total
    FROM detalle_pedidos dp
    JOIN pedidos p ON dp.pedido_id = p.id
    WHERE p.fecha_pedido >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY producto_id
    ORDER BY veces_vendido DESC
    LIMIT 100
)
SELECT c.cliente_id, c.total_pedidos, COUNT(DISTINCT pp.producto_id) as productos_populares_comprados
FROM clientes_activos c
JOIN pedidos p ON c.cliente_id = p.cliente_id
JOIN detalle_pedidos dp ON p.id = dp.pedido_id
JOIN productos_populares pp ON dp.producto_id = pp.producto_id
GROUP BY c.cliente_id, c.total_pedidos;

-- 2. Window Functions
SELECT 
    empleado_id,
    departamento,
    salario,
    ROW_NUMBER() OVER (PARTITION BY departamento ORDER BY salario DESC) as ranking_dept,
    RANK() OVER (PARTITION BY departamento ORDER BY salario DESC) as rank_dept,
    DENSE_RANK() OVER (PARTITION BY departamento ORDER BY salario DESC) as dense_rank_dept,
    LAG(salario, 1) OVER (PARTITION BY departamento ORDER BY salario) as salario_anterior,
    LEAD(salario, 1) OVER (PARTITION BY departamento ORDER BY salario) as salario_siguiente,
    AVG(salario) OVER (PARTITION BY departamento) as salario_promedio_dept,
    SUM(salario) OVER (PARTITION BY departamento) as masa_salarial_dept
FROM empleados;

-- Promedio móvil y suma acumulativa
SELECT 
    fecha_venta,
    monto_venta,
    AVG(monto_venta) OVER (ORDER BY fecha_venta ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as promedio_movil_7dias,
    SUM(monto_venta) OVER (ORDER BY fecha_venta ROWS UNBOUNDED PRECEDING) as suma_acumulativa
FROM ventas_diarias
ORDER BY fecha_venta;

-- 3. Creación y Optimización de Índices
-- Índice simple y compuesto
CREATE INDEX idx_cliente_fecha ON pedidos(cliente_id, fecha_pedido);
CREATE INDEX idx_pedidos_estado_fecha ON pedidos(estado, fecha_pedido DESC)
WHERE estado IN ('pendiente', 'procesando');

-- Índices funcionales
CREATE INDEX idx_email_lower ON usuarios(LOWER(email));
CREATE INDEX idx_precio_con_impuesto ON productos((precio * 1.21));

-- Índices especializados
CREATE INDEX idx_productos_busqueda ON productos USING gin(to_tsvector('spanish', nombre || ' ' || descripcion));
CREATE INDEX idx_ubicaciones_geo ON ubicaciones USING gist(coordenadas);

-- 4. Estrategias de Particionado
-- Particionado por rango
CREATE TABLE ventas (
    id SERIAL,
    cliente_id INTEGER,
    producto_id INTEGER,
    fecha_venta DATE NOT NULL,
    monto DECIMAL(10,2),
    cantidad INTEGER
) PARTITION BY RANGE (fecha_venta);

CREATE TABLE ventas_2024_01 PARTITION OF ventas
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- Particionado por hash
CREATE TABLE logs (
    id BIGSERIAL,
    usuario_id INTEGER,
    accion TEXT,
    timestamp TIMESTAMP DEFAULT NOW(),
    datos JSONB
) PARTITION BY HASH (usuario_id);

CREATE TABLE logs_0 PARTITION OF logs FOR VALUES WITH (modulus 4, remainder 0);

-- 5. EXPLAIN ANALYZE
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) 
SELECT c.nombre, COUNT(*) as total_pedidos
FROM clientes c
JOIN pedidos p ON c.id = p.cliente_id
WHERE p.fecha_pedido >= '2024-01-01'
GROUP BY c.id, c.nombre
ORDER BY total_pedidos DESC
LIMIT 10;

-- 6. Técnicas de Optimización Avanzada
-- Materialized view
CREATE MATERIALIZED VIEW reporte_ventas_mensual AS
SELECT DATE_TRUNC('month', fecha_venta) as mes, region, categoria_producto,
       COUNT(*) as total_transacciones, SUM(monto) as revenue_total,
       AVG(monto) as ticket_promedio, COUNT(DISTINCT cliente_id) as clientes_unicos
FROM ventas v
JOIN productos p ON v.producto_id = p.id
JOIN clientes c ON v.cliente_id = c.id
GROUP BY DATE_TRUNC('month', fecha_venta), region, categoria_producto;

CREATE INDEX idx_reporte_ventas_mes_region 
ON reporte_ventas_mensual(mes, region);

-- Función para refrescar vista materializada
CREATE OR REPLACE FUNCTION refresh_reporte_ventas()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY reporte_ventas_mensual;
END;
$$ LANGUAGE plpgsql;

-- 7. Casos de Uso: Segmentación RFM
WITH metricas_cliente AS (
    SELECT cliente_id, MAX(fecha_pedido) as ultima_compra, COUNT(*) as frecuencia,
           SUM(total) as valor_monetario,
           CURRENT_DATE - MAX(fecha_pedido) as dias_desde_ultima_compra
    FROM pedidos
    WHERE fecha_pedido >= CURRENT_DATE - INTERVAL '2 years'
    GROUP BY cliente_id
),
percentiles AS (
    SELECT PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY dias_desde_ultima_compra) as r80,
           PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY frecuencia) as f20,
           PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY valor_monetario) as m20
    FROM metricas_cliente
),
scores_rfm AS (
    SELECT mc.cliente_id, mc.ultima_compra, mc.frecuencia, mc.valor_monetario, mc.dias_desde_ultima_compra,
           CASE WHEN mc.dias_desde_ultima_compra <= p.r80 THEN 5 ELSE 1 END as recency_score,
           CASE WHEN mc.frecuencia >= p.f20 THEN 5 ELSE 1 END as frequency_score,
           CASE WHEN mc.valor_monetario >= p.m20 THEN 5 ELSE 1 END as monetary_score
    FROM metricas_cliente mc
    CROSS JOIN percentiles p
)
SELECT cliente_id, recency_score, frequency_score, monetary_score,
       (recency_score || frequency_score || monetary_score)::INTEGER as rfm_score
FROM scores_rfm
ORDER BY rfm_score DESC;

-- 8. Monitoreo y Alertas
-- Función para detectar queries lentas
CREATE OR REPLACE FUNCTION detectar_queries_lentas()
RETURNS TABLE (
    query_hash TEXT,
    query_sample TEXT,
    avg_time_ms NUMERIC,
    total_calls BIGINT,
    total_time_hours NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT md5(pss.query) as query_hash,
           LEFT(pss.query, 100) as query_sample,
           ROUND(pss.mean_time::numeric, 2) as avg_time_ms,
           pss.calls as total_calls,
           ROUND((pss.total_time / 1000 / 3600)::numeric, 2) as total_time_hours
    FROM pg_stat_statements pss
    WHERE pss.mean_time > 1000 OR pss.total_time > 300000
    ORDER BY pss.total_time DESC
    LIMIT 20;
END;
$$ LANGUAGE plpgsql;

-- 9. Bdest Practicess
-- Row Level security para multi-tenancy
CREATE POLICY tenant_policy ON datos_cliente
    FOR ALL TO aplicacion_user
    USING (tenant_id = current_setting('app.current_tenant_id')::INTEGER);
ALTER TABLE datos_cliente ENABLE ROW LEVEL SECURITY;
