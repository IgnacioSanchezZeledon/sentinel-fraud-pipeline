# CLAUDE.md — Acuerdo de trabajo para sentinel-fraud-pipeline

> Este archivo define **cómo** trabajamos. El **qué** está en `docs/PROMPT.md` (las 7 fases del pipeline).
> Antes de empezar **cualquier** sesión, Claude Code DEBE leer ambos archivos.
> Si hay conflicto: este archivo manda en proceso, el otro manda en alcance.

---

## 🛑 Reglas innegociables

1. **NUNCA** ejecutar varios micro-pasos seguidos sin parar. Después de cada micro-paso → **STOP** y esperar confirmación humana.
2. **NUNCA** commitear código sin haber ejecutado y aprobado su smoke test. Si no pasa, no hay commit.
3. **NUNCA** saltar a la siguiente fase sin haber cerrado todos los micro-pasos de la actual.
4. **NUNCA** dejar TODOs, `pass`, placeholders, mocks pegados o "ya lo arreglo después" en código que se commitea.
5. **NUNCA** hacer commits gigantes. Un commit = un cambio lógico verificable.
6. **NUNCA** modificar archivos fuera del scope del micro-paso actual sin avisar primero.

---

## 🔄 Workflow por micro-paso

Cada micro-paso es una unidad atómica. El ciclo es siempre el mismo:

1. **Anuncia** qué vas a hacer (1–2 frases máximo). Identificá el micro-paso por su número (ej. "Voy con 1.3").
2. **Implementá** el cambio mínimo necesario. Nada extra.
3. **Provee comandos exactos** de verificación que yo voy a correr localmente.
4. **🛑 STOP.** Esperá mi confirmación explícita ("ok", "✅", "siguiente", "anda").
5. **Sugerí mensaje de commit** siguiendo Conventional Commits.
6. **Hacé el commit** una vez yo confirme el mensaje.
7. **Avanzá** al siguiente micro-paso (volviendo al paso 1).

Excepción: si yo digo "anda solo hasta el paso X", podés saltarte los STOP intermedios hasta llegar ahí. Por defecto: STOP en cada paso.

---

## 📝 Convención de commits

Formato: `<tipo>(phase-<N>): <descripción>` — todo en inglés, minúsculas, modo imperativo, sin punto final.

Tipos válidos:
- `feat`: nueva funcionalidad
- `fix`: corrección de bug
- `chore`: tooling / config / infra (no afecta lógica de runtime)
- `test`: agregar o ajustar tests
- `docs`: solo documentación
- `refactor`: cambio interno sin alterar comportamiento

Ejemplos válidos:
```
chore(phase-1): add docker-compose with kafka and zookeeper
feat(phase-2): implement transaction producer with three modes
test(phase-3): add unit tests for silver deduplication
fix(phase-4): correct mlflow tracking uri for minio backend
docs(phase-7): write production-grade readme
```

Regla de oro: si no podés escribir un mensaje corto y claro, el cambio es demasiado grande. Partilo.

---

## 🏗️ Fases con micro-pasos

Cada fase del prompt original (`docs/PROMPT.md`) se descompone aquí en micro-pasos verificables. Marcar `[x]` solo después del commit confirmado.

### Fase 1 — Infraestructura

- [x] **1.1** Estructura de carpetas + `.gitkeep`s donde corresponda (`src/`, `tests/`, `data/`, `airflow/dags/`, `scripts/`, `docs/`)
  - Verificar: `tree -L 2 -a -I '.git'` muestra layout esperado
  - Commit: `chore(phase-1): scaffold project directory structure`
- [x] **1.2** `.env.example` con todas las variables (incluso las de fases futuras, comentadas si aún no se usan)
  - Verificar: `cp .env.example .env && grep -c '=' .env` cuenta todas las vars; ninguna línea termina en `=` vacío sin default justificado
  - Commit: `chore(phase-1): add .env.example with all required variables`
- [x] **1.3** `docker-compose.yml` solo con Zookeeper + Kafka (con healthchecks)
  - Verificar: `docker compose up -d zookeeper kafka` levanta sin errores; `docker compose ps` muestra ambos `healthy`
  - Commit: `chore(phase-1): add zookeeper and kafka services with healthchecks`
- [x] **1.4** Auto-creación del topic `transactions` (vía variable de entorno de Kafka o init container)
  - Verificar: sin crearlo a mano, `docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list` incluye `transactions`
  - Commit: `chore(phase-1): auto-create transactions topic on startup`
- [x] **1.5** Agregar MinIO + init container que crea buckets `bronze`, `silver`, `gold`, `mlflow`
  - Verificar: console en `http://localhost:9001` muestra los 4 buckets
  - Commit: `chore(phase-1): add minio with bucket initialization`
- [x] **1.6** Agregar Spark master + 1 worker, configurado para acceder a MinIO (s3a)
  - Verificar: UI en `http://localhost:8080` muestra 1 worker `ALIVE`
  - Commit: `chore(phase-1): add spark master and worker with s3a config`
- [x] **1.7** Agregar Postgres (para Airflow futuro)
  - Verificar: `docker compose exec postgres pg_isready` retorna OK
  - Commit: `chore(phase-1): add postgres for airflow metadata`
- [x] **1.8** `Makefile` con targets: `up`, `down`, `logs`, `health`, `clean`, `help`
  - Verificar: `make help` lista los targets; `make up && make down` ciclan sin error
  - Commit: `chore(phase-1): add makefile with common targets`
- [x] **1.9** `scripts/health_check.sh` que verifica los 4 servicios y retorna exit codes
  - Verificar: `make health` retorna `0` con todos verdes; matar Kafka y `make health` retorna `≠0`
  - Commit: `chore(phase-1): add health check script`
- [x] **1.10** Smoke test integral de Fase 1 desde estado limpio
  - Verificar: `make clean && cp .env.example .env && make up && sleep 30 && make health` termina verde
  - Commit (solo si hubo ajustes): `chore(phase-1): finalize infrastructure foundation`

### Fase 2 — Producer + Bronze

- [x] **2.1** Layout de `src/`, `pyproject.toml` (o `requirements.txt`), `pytest.ini`, `.python-version`
  - Verificar: en venv local, `pip install -r requirements.txt` corre sin errores
  - Commit: `chore(phase-2): bootstrap python project layout`
- [x] **2.2** Producer mínimo: conecta a Kafka y manda 1 mensaje hardcoded
  - Verificar: `python -m src.producer.transaction_producer --test` muestra el mensaje en `kafka-console-consumer`
  - Commit: `feat(phase-2): minimal kafka producer with single test message`
- [x] **2.3** Producer lee `data/creditcard.csv`, agrega `event_id` (UUID) y `event_timestamp` (ISO 8601), modo `slow` (1 msg/s)
  - Verificar: `make producer ARGS="--mode slow --limit 10"` publica 10 JSONs válidos con ambos campos
  - Commit: `feat(phase-2): csv-driven producer with event metadata`
- [x] **2.4** Modos `fast` y `realistic`, barra de progreso con `tqdm`, shutdown graceful con SIGINT
  - Verificar: los 3 modos funcionan; Ctrl+C cierra sin perder mensajes en buffer ni dejar el productor zombie
  - Commit: `feat(phase-2): add fast and realistic modes plus graceful shutdown`
- [x] **2.5** Bronze consumer: PySpark Structured Streaming desde Kafka → Delta en `s3a://bronze/transactions/`
  - Verificar: con producer corriendo, `spark.read.format("delta").load("s3a://bronze/transactions/").count()` aumenta entre lecturas
  - Commit: `feat(phase-2): bronze structured streaming consumer with delta sink`
- [x] **2.6** Audit columns (`_ingested_at`, `_kafka_offset`, `_kafka_partition`) + checkpoint para exactly-once
  - Verificar: matar y reiniciar el consumer no duplica filas; las 3 columnas existen y son no nulas
  - Commit: `feat(phase-2): add audit columns and checkpoint-based exactly-once`
- [x] **2.7** Smoke test integral de Fase 2 (500 mensajes end-to-end)
  - Verificar: `count(*) == 500` y `count(distinct event_id) == 500`
  - Commit (si aplica): `chore(phase-2): finalize bronze ingestion`

### Fase 3 — Silver

- [x] **3.1** Esqueleto del job: lectura Bronze + escritura noop a Silver con schema esperado
  - Verificar: `make silver` corre sin errores; tabla Delta vacía (o con filas crudas) en `s3a://silver/transactions/`
  - Commit: `feat(phase-3): silver job scaffold with read/write plumbing`
- [x] **3.2** Casts de tipos (V1–V28 → DoubleType, Amount → DecimalType, Class → IntegerType) + filtrado de nulos
  - Verificar: schema final tiene los tipos correctos; `df.filter(col("Amount").isNull()).count() == 0`
  - Commit: `feat(phase-3): add type casts and null filtering`
- [x] **3.3** Deduplicación por `event_id`
  - Verificar: ejecutar Silver dos veces sobre la misma Bronze deja el mismo count
  - Commit: `feat(phase-3): deduplicate by event_id`
- [x] **3.4** Features temporales y de monto (`transaction_hour`, `transaction_day_of_week`, `hour_sin`, `hour_cos`, `amount_bin`)
  - Verificar: columnas existen, sin nulos, valores en rangos esperados (`hour ∈ [0,23]`, `bin ∈ {low,medium,high,very_high}`)
  - Commit: `feat(phase-3): add temporal and amount-based features`
- [ ] **3.5** Features de ventana (`avg_amount_last_5`, `amount_zscore`, `is_high_amount`)
  - Verificar: columnas existen; spot-check manual contra el cálculo esperado en 5 filas
  - Commit: `feat(phase-3): add window-based statistical features`
- [ ] **3.6** Tests unitarios con fixture local de Spark (sin Kafka, sin MinIO)
  - Verificar: `make test-silver` pasa los 4 casos (dedup, null filter, amount_bin, feature presence)
  - Commit: `test(phase-3): add unit tests for silver transformations`

### Fase 4 — ML training + MLflow

- [ ] **4.1** Servicio `mlflow` en `docker-compose.yml`, artifacts en `s3://mlflow/`
  - Verificar: UI en `http://localhost:5000` carga; lista vacía de experimentos
  - Commit: `chore(phase-4): add mlflow service with minio artifact store`
- [ ] **4.2** `train.py` mínimo: carga Silver → Pandas, split 80/20 estratificado, RandomForest baseline, log a MLflow
  - Verificar: corrida visible en MLflow UI con métricas básicas (accuracy mínimo)
  - Commit: `feat(phase-4): minimal random forest training with mlflow logging`
- [ ] **4.3** `class_weight='balanced'` + métricas completas (precision, recall, f1, roc_auc)
  - Verificar: las 4 métricas aparecen logueadas en la corrida
  - Commit: `feat(phase-4): handle imbalance and log full classification metrics`
- [ ] **4.4** Artefactos visuales (`confusion_matrix.png`, `feature_importance.png`)
  - Verificar: ambos PNGs aparecen como artifacts en la corrida en MLflow UI
  - Commit: `feat(phase-4): log confusion matrix and feature importance plots`
- [ ] **4.5** Registro del modelo como `sentinel-fraud-model` y promoción a stage `Production`
  - Verificar: pestaña "Models" del MLflow UI muestra el modelo en stage `Production`
  - Commit: `feat(phase-4): register and promote best model in mlflow registry`
- [ ] **4.6** `predict.py`: carga modelo Production, scoring sobre Silver, escribe a `s3a://gold/predictions/`
  - Verificar: tabla existe; `fraud_probability ∈ [0, 1]`; `model_version` no nulo
  - Commit: `feat(phase-4): batch inference job writing predictions to gold`

### Fase 5 — Gold

- [ ] **5.1** `gld_fraud_scores`: join Silver + predictions con `fraud_probability`, `fraud_label`, `model_version`
  - Verificar: tabla existe; `fraud_probability ∈ [0, 1]`; `count == count(silver)`
  - Commit: `feat(phase-5): create gld_fraud_scores`
- [ ] **5.2** `gld_agg_by_customer` (customer_id sintético via `hash(V1, V2, V3)`)
  - Verificar: tabla existe; count distinct de customer_id razonable; agregaciones sin nulos
  - Commit: `feat(phase-5): aggregate fraud metrics by synthetic customer id`
- [ ] **5.3** `gld_agg_by_merchant` (merchant_id sintético via `hash(V4, V5, V6)`)
  - Verificar: tabla existe; fraud_rate por merchant en `[0, 1]`
  - Commit: `feat(phase-5): aggregate fraud metrics by synthetic merchant id`
- [ ] **5.4** `gld_agg_by_hour`
  - Verificar: exactamente 24 filas; `p95_amount` mayor que la media; fraud_rate global cercano al ~0.17% del dataset
  - Commit: `feat(phase-5): aggregate fraud metrics by hour of day`

### Fase 6 — Airflow

- [ ] **6.1** Servicios Airflow (init, webserver, scheduler) en `docker-compose.yml`
  - Verificar: UI en `http://localhost:8081`; login admin funciona; lista de DAGs vacía
  - Commit: `chore(phase-6): add airflow webserver and scheduler`
- [ ] **6.2** DAG mínimo `sentinel_fraud_pipeline` con solo `check_bronze` (BashOperator que cuenta filas en bronze)
  - Verificar: DAG visible; trigger manual exitoso
  - Commit: `feat(phase-6): add minimal pipeline dag with bronze check`
- [ ] **6.3** Agregar tareas `run_silver` → `run_inference` → `run_gold` → `notify` con dependencias
  - Verificar: trigger manual completo termina con todas las tareas verdes; el grafo se ve correcto en la UI
  - Commit: `feat(phase-6): wire full pipeline tasks in airflow dag`
- [ ] **6.4** `retries=3`, SLA, `schedule='@hourly'`, alertas en `on_failure_callback`
  - Verificar: una corrida programada se dispara sola; un fallo simulado dispara la alerta
  - Commit: `feat(phase-6): add retries, sla, and scheduled execution`

### Fase 7 — Grafana + README final

- [ ] **7.1** Servicio Grafana en `docker-compose.yml` con datasource auto-provisioned (PostgreSQL/Trino sobre Delta, según decidamos)
  - Verificar: UI en `http://localhost:3000`; datasource verde en Settings
  - Commit: `chore(phase-7): add grafana with provisioned datasource`
- [ ] **7.2** Dashboard JSON: panel "Transaction Feed"
  - Verificar: panel renderiza con datos reales en una ventana de tiempo razonable
  - Commit: `feat(phase-7): add transaction feed panel`
- [ ] **7.3** Dashboard: paneles "Fraud Distribution", "Active Alerts", "Model Metrics"
  - Verificar: los 4 paneles cargan sin errores con los Gold tables
  - Commit: `feat(phase-7): complete fraud dashboard panels`
- [ ] **7.4** README final: overview, arquitectura (con diagrama), business context (~$32B en pérdidas anuales por fraude), quick start, service URLs, decisiones técnicas
  - Verificar: renderiza bien en GitHub; todos los links funcionan; diagrama visible
  - Commit: `docs(phase-7): write production-grade readme`

---

## 🧪 Reglas de testing

- **Antes de cada commit**: al menos UN test (smoke o unitario) que verifique el cambio.
- Tests unitarios viven en `tests/`, espejando la estructura de `src/`.
- Smoke tests son comandos exactos definidos en este archivo o en `docs/PROMPT.md`.
- Si un cambio rompe un test que antes pasaba, **no se commitea**. Se arregla primero.
- Cuando un test falla en una verificación: pegar la salida completa, no resumirla.

---

## 📐 Reglas de código (todas las fases)

- **Python ≥ 3.11**.
- **Type hints** en toda función pública. **Docstrings** en módulos y funciones públicas (estilo Google o NumPy, consistente).
- **`logging` > `print`** siempre. Logger por módulo: `logger = logging.getLogger(__name__)`.
- **`pathlib.Path` > `os.path`**.
- **Configuración por env var** con default sensato. Centralizada en un `config.py` por componente; nunca leer `os.environ` desperdigado.
- **Manejo de errores explícito**: prohibido `except Exception: pass`. Captura específica → log → re-raise o handle deliberado y comentado.
- **Sin valores mágicos**: constantes nombradas o env vars.
- **Imports ordenados**: stdlib → third-party → local. `ruff` configurado en `pyproject.toml`.
- **No introducir dependencias nuevas** sin avisar primero qué y para qué.

---

## 🆘 Cuando algo falla

1. **No improvises soluciones.** Primero entender la causa.
2. **Mostrar el error completo** al usuario, no resumirlo.
3. **Proponer 1–2 hipótesis** y cómo descartarlas.
4. **Pedir confirmación** antes de aplicar fixes invasivos (rebajar versión de imagen, cambiar puerto, modificar config compartida, etc.).
5. **Una vez resuelto**: commitear con `fix(phase-N): <qué se arregló>`.

---

## 📋 Estado actual

> Actualizar al cierre de cada micro-paso confirmado.

- **Fase 0 — Git foundation:** ✅ completa
- **Fase 1 — Infraestructura:** ✅ completa (1.1 → 1.10)
- **Fase 2 — Producer + Bronze:** ✅ completa (2.1 → 2.7)
- **Fase actual:** Fase 3 — Silver (4/6 micro-pasos cerrados)
- **Próximo micro-paso:** `3.5` — Features de ventana (`avg_amount_last_5`, `amount_zscore`, `is_high_amount`)

---

## 🔗 Referencias

- Spec completo de fases: [`docs/PROMPT.md`](docs/PROMPT.md)
- Dataset: [Kaggle Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud)
