# Memoria de la fase 1 · Diseño del centro de datos e ingesta (RA1)

> Rellenad esta plantilla en Markdown dentro del repositorio. Las imágenes van en esta misma carpeta
> (`![descripción](nombre.png)`). La entrega es el **tag de Git** `F1-entrega` + el ZIP en Moodle.

## 0. Datos de la pareja

| | Miembro 1 | Miembro 2 |Miembro 3 |
|---|---|---|---|
| Nombre |Andrés |Álvaro |Antonio |
| Usuario de GitHub | andrewexee|AlvaroRamirez10 |AntonioDaw |
| Partes de las que es responsable principal | | |

**Horas dedicadas aproximadas:** ___  ·  **Commit final:** `______`

## 1. Resumen (máx. 10 líneas)


## 2. Desarrollo por criterio de evaluación

> Cada apartado se corrige con la fila correspondiente de la rúbrica. Sin evidencias, el apartado no se puntúa.
>
> **Normas de las evidencias (obligatorias):**
> - Cada apartado lleva **capturas de pantalla propias** (pantalla completa o ventana completa, no recortes sueltos)
>   en las que se vea el **reloj de Windows** y, cuando sea un cuaderno, la salida del **sello de evidencia**
>   (`sello(PAREJA)`, primera celda) o de un comando ejecutado en ese momento.
> - Guardad las capturas en esta carpeta con nombre descriptivo (`F2_D1_datanode_muerto.png`) e insertadlas
>   con `![descripción](F2_D1_datanode_muerto.png)`. Debajo de cada una, una línea explicando qué demuestra.
> - Además de las capturas: los comandos exactos, las salidas relevantes (copiadas como texto) y las tablas y gráficos generados.
> - Las evidencias deben coincidir con el historial de commits y con el cuaderno entregado.

### CE1.a · Diseño de la solución de almacenamiento

*Qué se espera:* Diagrama de arquitectura (zonas raw/bronze/silver/gold/cuarentena/control, servicios, flujos) y justificación de cada decisión. Enlazad `docs/ARQUITECTURA.md`.

La arquitectura completa está en [docs/ARQUITECTURA.md](../../docs/ARQUITECTURA.md). Guardamos los datos en HDFS y los organizamos en zonas. `raw` conserva los ficheros originales para poder volver a procesarlos; `bronze` contiene los datos con tipos y metadatos; `silver` guarda los datos limpios; y `gold` contiene resultados preparados para los informes. Los registros de control y los datos que necesitan revisión se guardan por separado.

Elegimos Parquet para las zonas procesadas porque está pensado para consultas analíticas y permite leer solo las columnas necesarias. Particionar por fecha o año ayuda a limitar los datos que Spark tiene que leer. Spark procesa los datos y YARN gestiona los recursos. JupyterLab sirve para ejecutar los notebooks; Hive Metastore y PostgreSQL guardan el catálogo; Superset muestra los resultados y Prometheus con Grafana ayuda a vigilar el sistema.

![Diagrama de la arquitectura](F1_E1_arquitectura_github_diagrama.png)

### CE1.b · Ingesta de datos

*Qué se espera:* Tabla de fuentes (frecuencia, volumen/día, mecanismo). Evidencia de la ingesta por lotes (salida de `ingesta.sh`, `/puerto/control/ingestas.jsonl`). Propuesta de ingesta incremental. Lectura con esquema y tratamiento de registros corruptos (TAREA B1).

La tabla de fuentes, con su frecuencia, volumen diario y mecanismo de ingesta, está en [docs/ARQUITECTURA.md](../../docs/ARQUITECTURA.md).

La ingesta por lotes se ejecuta con `bda.ps1 ingesta`. El script crea las carpetas de las zonas del lago, copia los ficheros de `/datos/raw` a `/puerto/raw` y guarda el manifiesto en `/puerto/control`. Para copiar usa `hdfs dfs -put -f`: si un fichero con el mismo nombre ya existe en el destino, lo sobrescribe. Así se pueden repetir los datos sin duplicar esos ficheros. Al final, añade una línea al historial `/puerto/control/ingestas.jsonl`.

Consultamos el historial con este comando:

```powershell
.\bda.ps1 hdfs dfs -cat /puerto/control/ingestas.jsonl
```

La salida registrada indica que la carga se hizo desde `/datos/raw` hacia `/puerto/raw` y tardó 38 segundos.

![Registro de ingestas](F1_E3_ingestas_jsonl.png)

Para simular una llegada incremental de AIS, `simular_llegada_ais.sh` copia los ficheros diarios, uno cada cierto tiempo, a `/puerto/landing/ais`. En un sistema real, una herramienta como Kafka o MQTT podría recibir las posiciones y Spark Structured Streaming procesarlas según llegan. Los puntos de control del proceso ayudarían a reanudarlo sin volver a leer todo desde el principio.

En la lectura de AIS declaramos el esquema y configuramos Spark en modo `PERMISSIVE` para guardar las líneas mal formadas en `_corrupto`. La ejecución contó 945371 posiciones AIS y encontró 735 líneas corruptas, que quedan disponibles para revisarlas.

![Lectura de AIS y líneas corruptas](F1_E3_corruptos.png)

En la TAREA B1 leímos `facturas_2025.csv` con `;` como separador y declaramos inicialmente las columnas como texto. Después cambiamos la coma decimal por un punto y convertimos `cantidad`, `precio_unitario` e `importe` a `double`. La salida de la celda muestra las cinco primeras filas y el esquema final.

![Lectura de facturas: cinco filas y esquema final](F1_E3_facturas.png)

### CE1.c · Formato de almacenamiento

*Qué se espera:* Tabla y gráficos de la comparativa de formatos con VUESTROS números; formato elegido por zona y por qué; particionado y problema de ficheros pequeños.

Comparamos seis formatos usando las posiciones AIS. Medimos el tamaño lógico guardado en HDFS y el tiempo de escritura y de dos consultas. Estos son los resultados de la primera ejecución:

| Formato | Tamaño (MB) | Escritura (s) | Consulta completa (s) | Consulta de 2 columnas (s) |
|---|---:|---:|---:|---:|
| CSV | 63,6 | 2,084 | 1,727 | 0,997 |
| JSON | 124,8 | 1,871 | 1,250 | 0,905 |
| Avro | 21,6 | 1,124 | 1,222 | 0,546 |
| ORC | 16,9 | 1,733 | 1,326 | 0,513 |
| Parquet Snappy | 11,3 | 1,728 | 0,685 | 0,407 |
| Parquet ZSTD | 7,6 | 0,896 | 0,355 | 0,309 |

![Tamaño de los formatos](formatos_tamano.png)

![Tiempos de los formatos](formatos_tiempos.png)

En la primera ejecución, JSON ocupó más espacio (124,8 MB) y Parquet ZSTD menos (7,6 MB). Para la consulta de dos columnas, Parquet ZSTD tardó 0,309 segundos, frente a 0,997 segundos en CSV. En esta prueba, Parquet ZSTD también tuvo el menor tiempo de escritura y de consulta completa. Son resultados de este conjunto de datos y de una ejecución; no garantizan los mismos tiempos en otros equipos.

Al repetir la medición, los tamaños se mantuvieron, pero los tiempos cambiaron. Las dos consultas fueron más rápidas en la segunda ejecución para los seis formatos; por ejemplo, la consulta completa de CSV pasó de 1,727 a 1,041 s y la consulta de dos columnas con Parquet ZSTD, de 0,309 a 0,180 s. Los tiempos de escritura no siguieron el mismo patrón: casi todos bajaron, pero Avro subió de 1,124 a 1,929 s. La caché, el calentamiento de Spark y la carga del equipo pueden influir en estas diferencias.

![Segunda ejecución de la comparativa de formatos](F1_E4_comparativa.png)

Para C2 proponemos Parquet ZSTD en bronze, silver y gold: en esta medición fue el formato más compacto y tuvo las consultas más rápidas. Parquet es columnar, por lo que Spark puede leer solo las columnas solicitadas (*column pruning*). Snappy suele priorizar menor coste de CPU; ZSTD suele comprimir más, aunque puede exigir más CPU. En nuestra medición ZSTD obtuvo mejores tiempos, pero no medimos el uso de CPU.

CSV, JSON y Avro almacenan los registros por filas; Parquet y ORC los almacenan por columnas. Avro, Parquet y ORC incluyen información de esquema en sus archivos; CSV y JSON no guardan un esquema formal. Los ficheros de texto sin comprimir se pueden dividir por líneas para procesarlos en paralelo; un fichero comprimido con gzip normalmente no se puede dividir de la misma manera. Avro, Parquet y ORC también admiten lectura paralela por bloques o grupos, según el formato y cómo se hayan escrito.

Particionar por fecha o año permite leer solo los periodos que necesita una consulta. Hay que evitar crear demasiados ficheros pequeños: cada fichero añade metadatos y trabajo de planificación, aunque contenga pocos datos. Reparticionar por la columna de partición antes de escribir ayuda a controlar cómo se distribuyen los datos.



### CE1.d · Procesamiento

*Qué se espera:* Consultas Spark SQL (código + resultado) guardadas en /puerto/gold; zona bronze de todas las fuentes (TAREA D1).



### CE1.e · Presentación al cliente

*Qué se espera:* Informe ejecutivo de 1 página: ≤ 3 gráficos, 3 conclusiones en lenguaje no técnico y 1 recomendación.



## 3. Problemas encontrados y cómo los resolvimos

| Problema / mensaje de error | Causa | Solución | Fuente consultada |
|---|---|---|---|
| | | | |

## 4. Conclusiones y mejoras propuestas


## 5. Declaración de uso de herramientas

Indicad qué herramientas de IA o fuentes externas habéis usado, **para qué** y qué parte es vuestra.
Recordad: el profesor puede pedir a cualquier miembro que defienda oralmente cualquier parte. Lo que no se sepa explicar se considera no realizado (cláusula de autoría de la programación).

| Herramienta / fuente | Para qué | Partes afectadas |
|---|---|---|
| | | |
