# SQL Avanzado para Grandes Volúmenes de Datos

## Introducción
Cuando trabajamos con bases de datos que contienen millones de registros, las técnicas básicas de SQL ya no son suficientes.  
Esta guía aborda estrategias efectivas para:
- Optimizar consultas
- Mejorar el rendimiento
- Gestionar grandes volúmenes de datos de manera eficiente

---

## 1. Common Table Expressions (CTEs)

### Qué son
- Consultas temporales con nombre que existen solo durante la ejecución de una consulta principal.
- Mejoran la legibilidad del código, evitan subconsultas complejas y permiten recursión.

### Aplicaciones
- Ventas mensuales
- Jerarquías de empleados
- Análisis de clientes y productos populares

### Ejemplo de código
```sql
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
