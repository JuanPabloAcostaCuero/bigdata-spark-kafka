# Conjunto de datos utilizado

## Descripción

**Casos positivos de COVID-19 en Colombia**, publicado en el portal de Datos Abiertos de Colombia con información del Ministerio de Salud y Protección Social y del Instituto Nacional de Salud (INS).

- **Página del dataset:** https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia-/gt2j-8ykr
- **Enlace corto:** https://www.datos.gov.co/d/gt2j-8ykr
- **API usada por la aplicación (CSV):** https://www.datos.gov.co/resource/gt2j-8ykr.csv
- **Tamaño:** 6.390.971 registros y 23 columnas (consulta realizada durante el desarrollo; el portal puede actualizarlo).
- **Contenido:** un registro por caso confirmado, con fechas de reporte, notificación, diagnóstico y recuperación; departamento y ciudad; edad y su unidad de medida; sexo; fuente del contagio; ubicación y estado del caso; y datos de recuperación o fallecimiento.

Es un dataset adecuado para Big Data por su volumen y porque es un dato real, con los errores de captura propios de este tipo de fuentes. Eso permite justificar las fases de limpieza y transformación del procesamiento batch.

## Problemas encontrados y soluciones implementadas

| # | Problema encontrado | Evidencia | Solución en la aplicación |
|---|---|---|---|
| 1 | **Categorías escritas de varias formas.** Una misma categoría aparece con distintas mayúsculas y minúsculas. | En `estado` y `ubicacion` se encontraron variantes como `Leve`, `leve` y `LEVE`, y `Casa`, `casa` y `CASA` (por ejemplo, 24.201 registros con `leve/casa` y 26.422 con `leve/Casa`, además de 6.150.659 con `Leve/Casa`). | En `batch_job.py`, `initcap(trim(...))` unifica el texto. La pantalla *Limpieza* muestra que los valores distintos de `estado`, `ubicacion` y `recuperado` pasan de 4 a 3 en una muestra de 300.000 filas. |
| 2 | **Valores nulos escritos como texto.** | 41.268 registros tienen `N/A` en `estado` y en `ubicacion`. | Los valores `N/A`, `NA`, `NAN` y cadenas vacías se convierten en `null` real para que no cuenten como una categoría. |
| 3 | **Sexo con valores inconsistentes.** | Además de `M` (2.973.872) y `F` (3.417.087) aparecen `m` y `f` en minúscula (6 registros de cada una). | Se aplica `upper(trim(...))` y solo se aceptan `M` y `F`; cualquier otro valor pasa a `null` y la fila se descarta en el filtro de calidad. |
| 4 | **Todas las columnas llegan como texto.** Los números y las fechas (`2020-03-06 00:00:00`) no tienen tipo. | Al cargar el CSV, `printSchema()` muestra todo como `string`. | `cast` a entero para `edad` y `unidad_medida`, y `to_timestamp` para las seis columnas de fecha. |
| 5 | **Edad en unidades distintas.** La edad puede estar en años, meses o días, según `unidad_medida`. | Una columna `edad` con valor `3` puede significar 3 años, 3 meses o 3 días. | Se crea `edad_anios`: años se dejan igual, los meses se dividen entre 12 y los días se llevan a 0 años. Con ella se calculan `grupo_edad` y las estadísticas. |
| 6 | **Edades imposibles.** | En una muestra de 300.000 filas se detectó un registro con edad mayor a 110 años. | Se descartan las edades fuera del rango de 0 a 110 años (el paso queda registrado en la tabla de limpieza). |
| 7 | **Registros incompletos.** | Filas sin sexo, sin fecha de notificación o sin departamento no pueden analizarse. | `dropna(subset=[sexo, fecha_de_notificaci_n, departamento_nom])`. |
| 8 | **Posibles duplicados por caso.** | No se encontraron duplicados en las muestras analizadas (0 filas eliminadas), pero es un control de calidad obligatorio. | `dropDuplicates(["id_de_caso"])` y el resultado queda en la tabla de limpieza. |
| 9 | **Volumen que no cabe en una descarga simple.** Con 6,3 millones de filas, descargar todo en un solo paso es lento y poco práctico en un PC. | — | La aplicación descarga una muestra configurable (100.000, 300.000 o 1.000.000 de filas) por la API, en bloques de 10.000 y con reintentos. |
| 10 | **Sesgo por el orden de la fuente.** Si solo se descargan las primeras filas, la muestra contiene solo los primeros meses de 2020. | Las primeras 200.000 filas por `id_de_caso` cubrían únicamente de marzo a diciembre de 2020. | La descarga toma bloques en posiciones equidistantes del dataset completo, de modo que la muestra cubre de marzo de 2020 a 2022 y permite ver las distintas olas de contagio. |
| 11 | **Conexiones cortadas por la API pública.** | Durante las pruebas hubo errores de conexión (`ConnectionResetError`) en descargas consecutivas. | Función `_get` con hasta 6 reintentos y espera creciente. El archivo se escribe como `.part` y solo se renombra al terminar, para no usar una descarga incompleta. |

## Resultado de la limpieza (muestra de 300.000 filas)

| Paso | Antes | Después |
|---|---|---|
| Eliminar duplicados (`id_de_caso`) | 300.000 | 300.000 |
| Unificar categorías de `estado`, `ubicacion` y `recuperado` (valores distintos) | 4 | 3 |
| Descartar edades imposibles | 300.000 | 299.999 |
| Eliminar filas sin sexo, fecha o departamento | 299.999 | 299.999 |

Columnas nuevas creadas: `edad_anios`, `grupo_edad`, `anio_mes` y `dias_a_diagnostico`.

## Datos del tiempo real

En la etapa de tiempo real no se usa el CSV: el generador (`engine/producer.py`) crea casos sintéticos con la misma estructura básica (departamento, ciudad, sexo, edad, tipo de contagio y estado) y con pesos aproximados a los del dataset histórico. Los eventos se publican en el topic `casos_covid` de Kafka y los consume Spark Streaming.
