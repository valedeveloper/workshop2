# Especificación y Matriz de Reglas de Calidad de Datos (Great Expectations)

Este documento define formalmente el catálogo de reglas de calidad de datos implementadas en el pipeline analítico por lotes (**Spotify + Grammy Awards**), en cumplimiento con la sección 6.5 del taller (*Data Quality Rules and Validation Design*).

---

## 1. Principios de Validación y Política de Severidad

Cada regla de calidad de datos responde a un riesgo evidenciado durante el perfilado y protege un requerimiento analítico (**R1**, **R2**, **R3** o **R4**).

### Escala de Severidad y Acciones de la Tubería

| Severidad | Significado Operativo | Acción del Pipeline |
|---|---|---|
| **Critical** | La violación hace inseguro continuar el procesamiento o cargar al almacén. Invalida completamente el lote o el contrato estructural. | **Detención inmediata (*fail-fast*)**: interrumpe la ejecución de la tarea en Airflow y bloquea todas las etapas aguas abajo (transformación o carga al Data Warehouse). |
| **High** | La falla compromete directamente la exactitud métrica o la integridad relacional de los hechos. | **Bloqueo del lote**: genera excepción `QualityGateFailure` e impide la transformación/carga. No se permite imputación ni sustitución silenciosa de datos. |
| **Warning** | El hallazgo señala una limitación de cobertura o un patrón no crítico que no invalida la coherencia técnica del lote. | **Registro y notificación observable**: se documenta en el informe de validación (`validation_report.json`) y en logs de Airflow, permitiendo la continuación bajo política justificada. |
| **Informational** | Contexto operativo, métricas de volumen o señales de monitoreo continuo. | **Registro diagnóstico**: se preserva en metadatos para trazabilidad y auditoría. No bloquea el flujo. |

---

## 2. Matriz General de Reglas de Calidad

Para cada conjunto de datos del sistema (**Spotify Raw**, **Grammy Raw** y **Data Warehouse Curado**) se auditan las tres dimensiones fundamentales exigidas:
1. **Columnas**: Estructura, presencia y conformidad del esquema.
2. **Valores permitidos**: Dominios, rangos métricos, conjuntos de valores permitidos y no-nulidad.
3. **Filas repetidas**: Ausencia de redundancias, unicidad de claves primarias y consistencia en el grano analítico.

| Rule ID | Dataset / Capa | Atributo(s) | Dimensión de Calidad | Regla de Calidad | Métrica / Umbral Aceptable | Severidad | Requerimiento Relacionado |
|---|---|---|---|---|---|---|---|
| **R-SPOT-01-COLS** | Spotify Raw | `track_id`, `track_genre`, `popularity`, `duration_ms`, `danceability`, `energy`, `valence`, `artists`, `track_name`, `album_name` | Estructura / Conformidad | Todas las columnas analíticas requeridas deben estar presentes en el archivo de origen. | 100% de columnas requeridas presentes (0 columnas faltantes). | **Critical** | R1, R2, R3, R4 |
| **R-SPOT-02-VALS** | Spotify Raw | `popularity`, `danceability`, `energy`, `valence`, `track_id`, `track_genre` | Validez de Dominio y Completitud | Popularidad en $[0, 100]$; métricas de audio en $[0.0, 1.0]$; `track_id` y `track_genre` sin valores nulos ni vacíos. | 100% de valores dentro del rango esperado; 0 valores nulos en claves. | **High** | R2, R4 |
| **R-SPOT-03-DUPS** | Spotify Raw / Prep | `track_id`, `track_genre` | Unicidad y Consistencia del Grano | Ausencia de duplicados contradictorios en la clave compuesta canción-género; tras normalización, unicidad estricta en el grano. | 0 duplicados contradictorios en crudo; 100% de unicidad en `(track_id, track_genre)` en el lote normalizado. | **Critical** | R2, R4 |
| **R-GRAM-01-COLS** | Grammy Raw (PostgreSQL) | `year`, `title`, `published_at`, `updated_at`, `category`, `nominee`, `artist`, `workers`, `img`, `winner` | Estructura / Integridad de Esquema | El lote extraído de PostgreSQL debe coincidir exactamente con las 10 columnas del contrato de origen. | Coincidencia exacta del conjunto de 10 columnas (0 diferencias). | **Critical** | R1, R3 |
| **R-GRAM-02-VALS** | Grammy Raw (PostgreSQL) | `year`, `category`, `winner`, `artist` | Validez de Dominio y Completitud | `year` entero en $[1900, 2026]$; `category` y `winner` no nulos; `winner` booleano; `artist` monitoreado por nulos inherentes. | 100% de no-nulidad en `year`, `category`, `winner`; $\le 40\%$ de nulos en `artist` justificado por perfilado. | **Critical** (campos clave) / **Warning** (`artist` nulo) | R1, R3, R4 |
| **R-GRAM-03-DUPS** | Grammy Raw (PostgreSQL) | Todas las columnas / `(year, category, nominee, title, artist)` | Unicidad | Ausencia de filas completas duplicadas o registros de premiación repetidos en la misma categoría y año. | 0 filas duplicadas (100% de registros únicos). | **High** | R1, R3 |
| **R-CUR-01-COLS** | Curated DW | Esquemas de 6 tablas (`dim_date`, `dim_artist`, `dim_cancion`, `dim_genero`, `fact_music_award`, `fact_spotify_artist_track_genre`) | Integridad Estructural | Las 6 tablas del modelo dimensional deben existir, no estar vacías y cumplir con su esquema estrella exacto. | 6 tablas existentes, filas $> 0$, 100% de coincidencia de columnas. | **Critical** | R1, R2, R3, R4 |
| **R-CUR-02-VALS** | Curated DW | Claves foráneas (`date_sk`, `artist_sk`, `track_sk`, `genre_sk`), `award_count`, `decade`, medidas | Consistencia e Integridad Referencial | Cero claves foráneas huérfanas; `award_count = 1`; `decade = (year // 10) * 10`; medidas analíticas en rango válido. | 100% de integridad referencial (0 huérfanos); 100% de medidas en rango. | **Critical** | R1, R2, R3, R4 |
| **R-CUR-03-DUPS** | Curated DW | PKs sustitutas, claves de negocio y grano `(track_sk, artist_sk, genre_sk)` | Unicidad de Grano y Claves Primarias | Unicidad estricta de PKs en las 6 tablas; unicidad de claves de negocio normalizadas; unicidad del grano de crédito de artista. | 0 duplicados en PKs; 0 duplicados en claves de negocio; 100% unicidad en grano de hechos. | **Critical** | R1, R2, R3, R4 |

---

## 3. Especificación Detallada de Reglas por Conjunto de Datos

A continuación se detalla exhaustivamente para cada conjunto de datos: **qué se revisa**, **qué se considera aceptable** y **qué tan grave es si falla**, acompañados de su justificación ingenieril y la expectativa técnica en Great Expectations.

---

### Conjunto de Datos 1: Spotify Raw (`spotify_raw`)

#### 1. R-SPOT-01-COLS: Columnas Requeridas del Lote Spotify
- **Qué se revisa:** La presencia inalterada de las columnas analíticas indispensables en el archivo CSV: `track_id`, `track_genre`, `popularity`, `duration_ms`, `danceability`, `energy`, `valence`, `artists`, `track_name`, `album_name`. Si existe la columna de índice de exportación (`Unnamed: 0`), se valida que sea un índice secuencial antes de ser depurada.
- **Qué se considera aceptable:** El 100% de las columnas requeridas deben estar presentes en el DataFrame. Se considera inaceptable la ausencia de cualquiera de ellas.
- **Qué tan grave es si falla:** **Critical**. Si falta una columna estructural, las tareas de normalización, extracción de dimensiones y cálculo de popularidad fallarían con errores fatales de código (`KeyError`). Se bloquea la ejecución de inmediato sin intentar transformar.
- **Expectation GX:** `ExpectTableColumnsToMatchSet(column_set=REQUIRED_COLUMNS, exact_match=False)` y verificaciones por columna con `ExpectColumnToExist`.

#### 2. R-SPOT-02-VALS: Valores Permitidos en Métricas y Claves
- **Qué se revisa:**
  - `popularity`: que los valores numéricos pertenezcan al dominio cerrado $[0, 100]$.
  - `danceability`, `energy`, `valence`: que los valores de audio pertenezcan al dominio $[0.0, 1.0]$.
  - `track_id` y `track_genre`: que no contengan valores nulos (`NaN`), vacíos (`""`) ni espacios en blanco.
- **Qué se considera aceptable:** Cero violaciones de rango y cero valores nulos en claves (tasa de error 0.0%). Las métricas numéricas deben ser parseables sin truncamiento.
- **Qué tan grave es si falla:** **High**. Valores fuera de rango (ej. popularidad negativa o mayor a 100) invalidan las medias por género (R2) y las comparaciones de popularidad entre ganadores y no ganadores de Grammy (R4). El pipeline no permite imputaciones arbitrarias ni recortes forzados: el lote se bloquea.
- **Expectation GX:**
  - `ExpectColumnValuesToBeBetween(column='popularity', min_value=0, max_value=100)`
  - `ExpectColumnValuesToBeBetween(column='danceability', min_value=0.0, max_value=1.0)`
  - `ExpectColumnValuesToBeBetween(column='energy', min_value=0.0, max_value=1.0)`
  - `ExpectColumnValuesToBeBetween(column='valence', min_value=0.0, max_value=1.0)`
  - `ExpectColumnValuesToNotBeNull(column='track_id')`
  - `ExpectColumnValuesToNotBeNull(column='track_genre')`

#### 3. R-SPOT-03-DUPS: Filas Repetidas y Unicidad en Grano Canción-Género
- **Qué se revisa:** La duplicación de observaciones en el grano natural de la fuente: `(track_id, track_genre)`. En el perfilado se observaron 450 filas duplicadas sobre 444 claves únicas. Se revisa si los duplicados son copias concordantes (idénticos valores en todos los campos) o contradictorios (mismo track y género con popularidad o duración distinta).
- **Qué se considera aceptable:**
  - Si los duplicados son idénticos en todos sus campos, es aceptable su deduplicación determinista colapsándolos a una sola observación (obteniendo exactamente 113.550 filas).
  - Es completamente inaceptable la presencia de duplicados contradictorios en el lote.
  - En el lote normalizado que alimenta la transformación, la unicidad debe ser del 100% (cero duplicados en la clave compuesta).
- **Qué tan grave es si falla:** **Critical**. Dejar pasar duplicados contradictorios distorsionaría el peso de las canciones en la agregación de géneros (R2) y multiplicarían las observaciones de artistas en R4. Si hay contradicción, el lote se detiene inmediatamente.
- **Expectation GX:** `ExpectCompoundColumnsToBeUnique(column_list=['track_id', 'track_genre'])` sobre el lote normalizado, y control analítico de concordancia en la lectura del batch crudo.

---

### Conjunto de Datos 2: Grammy Awards Raw (`grammys_raw`)

#### 4. R-GRAM-01-COLS: Esquema y Columnas de la Extracción Relacional
- **Qué se revisa:** Que el lote extraído desde la tabla operativa en PostgreSQL (`workshop2.public.grammys`) contenga exactamente las 10 columnas esperadas: `year`, `title`, `published_at`, `updated_at`, `category`, `nominee`, `artist`, `workers`, `img`, `winner`.
- **Qué se considera aceptable:** Coincidencia exacta del conjunto de columnas (sin columnas omitidas ni columnas extrañas no documentadas).
- **Qué tan grave es si falla:** **Critical**. Un desfase en las columnas indica una corrupción en la base de datos de origen o en el proceso de extracción SQL, imposibilitando la carga dimensional. Detiene el avance hacia la transformación.
- **Expectation GX:** `ExpectTableColumnsToMatchSet(column_set=GRAMMY_COLUMNS, exact_match=True)`.

#### 5. R-GRAM-02-VALS: Valores Permitidos y Completitud de Premiaciones
- **Qué se revisa:**
  - `year`: debe ser numérico y estar en el rango de existencia de los Premios Grammy ($[1900, 2026]$).
  - `year`, `category` y `winner`: campos obligatorios, no pueden ser nulos ni cadenas vacías.
  - `winner`: debe admitir únicamente valores interpretables como booleanos (`True`, `False`, `"True"`, `"False"`).
  - `artist`: se audita la proporción de nulos (el perfilado identificó 1.840 nulos en 4.809 filas, correspondientes a premios de compositores, álbumes del año o categorías institucionales).
- **Qué se considera aceptable:**
  - 100% de no-nulidad y validez en `year`, `category` y `winner` (0 errores).
  - En `artist`, se acepta una tasa de nulos de hasta un 40% bajo política de severidad **Warning**, registrando la advertencia y permitiendo que se asigne `artist_sk = NULL` sin inventar nombres ni imputar datos erróneos.
  - Si `winner` sólo contiene valores `True`, se emite una advertencia de cobertura (sesgo de origen conocido).
- **Qué tan grave es si falla:**
  - En campos clave (`year`, `category`, `winner`): **Critical**. Sin año no se puede relacionar con `dim_date`, y sin categoría ni estatus de ganador no es posible calcular R1 ni R3. Bloquea la tubería.
  - En `artist`: **Warning**. No bloquea el lote pero se alerta formalmente para no atribuir esos premios en R1 ni R4.
- **Expectation GX:**
  - `ExpectColumnValuesToNotBeNull(column='year')`
  - `ExpectColumnValuesToBeBetween(column='year', min_value=1900, max_value=2026)`
  - `ExpectColumnValuesToNotBeNull(column='category')`
  - `ExpectColumnValuesToNotBeNull(column='winner')`
  - `ExpectColumnValuesToBeInSet(column='winner', value_set=[True, False, 'True', 'False', 1, 0])`

#### 6. R-GRAM-03-DUPS: Filas Repetidas en Premios Grammy
- **Qué se revisa:** Detección de filas redundantes completas o registros repetidos para una misma premiación en la misma edición y categoría (`year`, `category`, `nominee`, `title`, `artist`).
- **Qué se considera aceptable:** 0 filas repetidas (100% de unicidad en los registros de premiación).
- **Qué tan grave es si falla:** **High**. Filas duplicadas inflarían la medida de conteo de premios (`award_count`), distorsionando el KPI R3 (Top 5 géneros más premiados) y el porcentaje de artistas ganadores (R1). Bloquea el procesamiento.
- **Expectation GX:** `ExpectCompoundColumnsToBeUnique(column_list=['year', 'category', 'nominee', 'title', 'artist'])` y verificación de `ExpectTableRowCountToBeBetween`.

---

### Conjunto de Datos 3: Data Warehouse Curado / Preparado (`curated_warehouse`)

#### 7. R-CUR-01-COLS: Integridad de Esquema en las 6 Tablas Dimensionales
- **Qué se revisa:** Que las 4 dimensiones (`dim_date`, `dim_artist`, `dim_cancion`, `dim_genero`) y los 2 hechos (`fact_music_award`, `fact_spotify_artist_track_genre`) se encuentren materializados, contengan registros ($> 0$) y presenten exactamente las columnas del esquema relacional:
  - `dim_date`: `date_sk`, `year`, `decade`.
  - `dim_artist`: `artist_sk`, `artist_name`, `artist_name_normalized`.
  - `dim_cancion`: `track_sk`, `track_id`, `track_name`, `album_name`, `artists`, `duration_ms`, `explicit`.
  - `dim_genero`: `genre_sk`, `genre_name`.
  - `fact_music_award`: `music_award_sk`, `date_sk`, `artist_sk`, `category`, `nominee`, `title`, `winner`, `award_count`.
  - `fact_spotify_artist_track_genre`: `spotify_credit_sk`, `track_sk`, `artist_sk`, `genre_sk`, `popularity`, `danceability`, `energy`, `valence`.
- **Qué se considera aceptable:** 100% de cumplimiento en las 6 tablas: todas presentes, ninguna vacía y esquemas idénticos al contrato.
- **Qué tan grave es si falla:** **Critical**. Una discrepancia de esquema impediría la carga con COPY/INSERT en PostgreSQL o causaría errores de tabla inexistente. Bloquea la carga al DW (`load_curated_warehouse`).
- **Expectation GX:** `ExpectTableColumnsToMatchSet` y `ExpectTableRowCountToBeBetween(min_value=1)` aplicadas a cada una de las 6 tablas.

#### 8. R-CUR-02-VALS: Dominios Analíticos e Integridad Referencial de Claves Foráneas
- **Qué se revisa:**
  - Integridad referencial (cero huérfanos):
    - Todo `date_sk` en `fact_music_award` debe existir en `dim_date.date_sk`.
    - Todo `artist_sk` no nulo en `fact_music_award` debe existir en `dim_artist.artist_sk`.
    - Todo `track_sk` en `fact_spotify_artist_track_genre` debe existir en `dim_cancion.track_sk`.
    - Todo `genre_sk` en `fact_spotify_artist_track_genre` debe existir en `dim_genero.genre_sk`.
    - Todo `artist_sk` no nulo en `fact_spotify_artist_track_genre` debe existir en `dim_artist.artist_sk`.
  - Dominios pos-transformación:
    - En `dim_date`: `decade` debe ser matemáticamente consistente con `(year // 10) * 10`.
    - En `fact_music_award`: `award_count` debe ser estrictamente igual a 1 en cada fila, y `winner` debe ser booleano.
    - En `fact_spotify_artist_track_genre`: `popularity` en $[0, 100]$ y métricas de audio en $[0.0, 1.0]$.
- **Qué se considera aceptable:** Cero claves huérfanas (100% de integridad referencial) y 100% de cumplimiento en rangos y recuentos unitarios.
- **Qué tan grave es si falla:** **Critical**. Claves huérfanas provocarían violación de restricciones de Foreign Key en PostgreSQL o pérdida de datos en los `JOIN`s de los KPIs R1 a R4.
- **Expectation GX:**
  - `ExpectColumnValuesToBeInSet(column='award_count', value_set=[1])`
  - `ExpectColumnValuesToBeBetween(column='popularity', min_value=0, max_value=100)`
  - Verificación cruzada de inclusión de conjuntos para Foreign Keys.

#### 9. R-CUR-03-DUPS: Unicidad de Claves Primarias y Grano Analítico en el DW
- **Qué se revisa:**
  - Unicidad estricta de las Surrogate Keys (PKs): `date_sk`, `artist_sk`, `track_sk`, `genre_sk`, `music_award_sk`, `spotify_credit_sk`.
  - Unicidad de claves de negocio normalizadas: `artist_name_normalized` en `dim_artist`, `track_id` en `dim_cancion`, `genre_name` en `dim_genero`, `year` en `dim_date`.
  - Unicidad del grano en `fact_spotify_artist_track_genre`: la combinación `(track_sk, artist_sk, genre_sk)` debe ser estrictamente única.
- **Qué se considera aceptable:** Cero duplicados en PKs, cero duplicados en claves de negocio y cero duplicados en el grano del hecho (100% de unicidad).
- **Qué tan grave es si falla:** **Critical**. Claves primarias duplicadas romperían la carga relacional de base de datos. Duplicados en el grano del hecho multiplicarían artificialmente los cálculos de los 4 KPIs. Se detiene el proceso de carga.
- **Expectation GX:**
  - `ExpectColumnValuesToBeUnique(column='date_sk')`
  - `ExpectColumnValuesToBeUnique(column='artist_sk')`
  - `ExpectColumnValuesToBeUnique(column='artist_name_normalized')`
  - `ExpectColumnValuesToBeUnique(column='track_sk')`
  - `ExpectColumnValuesToBeUnique(column='track_id')`
  - `ExpectColumnValuesToBeUnique(column='genre_sk')`
  - `ExpectColumnValuesToBeUnique(column='genre_name')`
  - `ExpectColumnValuesToBeUnique(column='music_award_sk')`
  - `ExpectColumnValuesToBeUnique(column='spotify_credit_sk')`
  - `ExpectCompoundColumnsToBeUnique(column_list=['track_sk', 'artist_sk', 'genre_sk'])`

---

## 4. Trazabilidad de Reglas con Requerimientos Analíticos

| Requerimiento Analítico | Pregunta de Negocio | Reglas Críticas que lo Protegen |
|---|---|---|
| **R1** | Porcentaje de artistas de Spotify con Grammy ganador. | `R-SPOT-01-COLS`, `R-GRAM-01-COLS`, `R-GRAM-02-VALS`, `R-CUR-02-VALS` (integridad `artist_sk`), `R-CUR-03-DUPS` (unicidad de artista normalizado). |
| **R2** | Popularidad promedio y cantidad de canciones por género. | `R-SPOT-02-VALS` (rango popularidad), `R-SPOT-03-DUPS` (grano canción-género), `R-CUR-02-VALS` (integridad foránea de géneros), `R-CUR-03-DUPS` (unicidad de canciones y géneros). |
| **R3** | Top 5 géneros con más premios Grammy ganadores observados. | `R-GRAM-02-VALS` (`winner` válido), `R-GRAM-03-DUPS` (no duplicar premios), `R-CUR-02-VALS` (`award_count = 1`), `R-CUR-03-DUPS` (grano de hechos sin repetición). |
| **R4** | Popularidad media por artista comparando ganadores vs no ganadores. | `R-SPOT-02-VALS` (popularidad en $[0, 100]$), `R-SPOT-03-DUPS` (sin duplicados contradictorios), `R-GRAM-02-VALS` (estatus `winner`), `R-CUR-02-VALS` (consistencia relacional entre Spotify y Grammy). |
