# Dashboard de Ventas — Padova SAC

Sistema de seguimiento de ventas y separaciones en tiempo real para los proyectos inmobiliarios de Padova SAC. Consta de dos componentes: un **ETL** que extrae datos del CRM Evolta y los sube a Google Sheets, y un **Dashboard web** desplegado en Railway que lee esos datos y los presenta visualmente.

---

## Arquitectura general

```
CRM Evolta (web)
      │
      │  Selenium scraping
      ▼
GitHub Actions (nube — 7am / 12pm / 5pm Lima)
      │
      │  gspread API
      ▼
Google Sheets (fuente de datos)
      │
      │  CSV público (fetch cada 1h)
      ▼
Flask App (Railway)
      │
      ▼
Dashboard web  ◄──── Usuario (navegador)
```

---

## Componente 1 — ETL

**Archivo:** `C:\ETL\ETL_Evolta_Padova_OneDrive.py`

### Qué hace
1. Abre Chrome headless con Selenium y hace login en Evolta (`v4.evolta.pe`)
2. Descarga el reporte de **Ventas** (años 2024, 2025, 2026) en formato XLSX/CSV
3. Descarga el reporte de **Stock** en formato XLSX
4. Transforma los datos:
   - **Unpivot de ventas**: Evolta exporta ítems de combo en columnas anchas (`TipoInmueble_1`, `TipoInmueble_2`…). El ETL los convierte a filas individuales con `pd.wide_to_long`
   - **Precios individuales por ítem**: usa `TotalLista` (precio post-descuento por ítem) como `PrecioVenta`, evitando que el precio total del combo se duplique en cada fila
   - **Conversión USD → PEN**: consulta la API del BCRP por fecha histórica de cada registro. Fallback a S/ 3.75 si la API falla. Genera columna `PrecioVentaSoles`
5. Sube los datos a **Google Sheets** (pestañas `VENTAS` y `STOCK`)
6. Copia el reporte `.xlsx` a OneDrive
7. Envía email de confirmación

### Configuración clave

| Variable | Valor |
|---|---|
| `USER_CRED` | Credenciales Evolta |
| `EMAIL_FROM` | `sistema.padova@gmail.com` |
| `EMAIL_TO` | `yleon@padovasac.com`, `carrunategui@constructorapadova.pe` |
| `DOWNLOAD_DIR` | `C:\Users\MKT\Documents\EVOLTA\descargas_stock` |
| `DOWNLOAD_DIR_VENTAS` | `C:\Users\MKT\Documents\EVOLTA\descargas_ventas` |
| `GSHEETS_SPREADSHEET_ID` | `15slphQ1xs7pkG4zH5mVwsnc1XKgGtLKah8-FHm7HMEM` |
| `GSHEETS_CREDENTIALS_FILE` | `evoltareportes-00ffe1b337be.json` |
| `AÑOS_VENTAS` | `[2024, 2025, 2026]` |
| `TARGET_PROJECTS` | SUNNY, LITORAL 900, HELIO - SANTA BEATRIZ, LOMAS DE CARABAYLLO |

### Programación
Corre vía **GitHub Actions** (nube, sin depender de PC local):
- 7:00 AM Lima (12:00 UTC)
- 12:00 PM Lima (17:00 UTC)
- 5:00 PM Lima (22:00 UTC)

También se puede ejecutar manualmente desde GitHub → Actions → "ETL Evolta" → Run workflow.

### Dependencias ETL
```
selenium, webdriver-manager, pandas, gspread, google-auth, requests, openpyxl, xlsxwriter
```

### Tipo de cambio (BCRP)
```python
# URL de consulta histórica
https://estadisticas.bcrp.gob.pe/estadisticas/series/api/PD04637PD/json/{fecha}/{fecha}/ing

# Lógica:
# 1. Busca TC para la fecha exacta del registro (FechaVenta o FechaSepDefinitiva)
# 2. Si no hay dato (feriado/fin de semana), retrocede hasta 7 días
# 3. Usa cache interno (_TC_CACHE) para no repetir consultas
# 4. Fallback: S/ 3.75 si la API falla
```

---

## Componente 2 — Dashboard web

**Repositorio:** `drevillapadova/dashboard_separacionesyventas` (branch `main`)
**URL producción:** `https://dashboardseparacionesyventas-production.up.railway.app/`

### Archivos

```
DashboardEvolta/
├── app.py              # Flask backend
├── Procfile            # Railway: gunicorn app:app --bind 0.0.0.0:$PORT
├── requirements.txt    # flask, gunicorn, pandas, gspread, google-auth, requests, apscheduler, pytz
└── templates/
    └── index.html      # Frontend completo (HTML + CSS + JS en un solo archivo)
```

### Backend (`app.py`)

- Framework: **Flask**
- Servidor: **gunicorn** (Railway)
- Cache en memoria (`_cache`) con datos de ventas y stock
- **Auto-refresh**: APScheduler ejecuta `actualizar_cache()` cada **1 hora**
- **Refresh manual**: endpoint `POST /api/refresh`

#### Endpoints

| Endpoint | Método | Descripción |
|---|---|---|
| `/` | GET | Sirve el dashboard HTML |
| `/api/data?t={timestamp}` | GET | Devuelve ventas + stock del cache. El parámetro `t` evita caché del navegador |
| `/api/refresh` | POST | Fuerza recarga desde Google Sheets |

#### Fuentes de datos (Google Sheets CSV público)

| Sheet | URL (`gid`) | Descripción |
|---|---|---|
| VENTAS | `gid=1351036806` | Reporte de ventas post-unpivot |
| STOCK | `gid=1599060189` | Estado actual de todos los inmuebles |

---

### Frontend (`index.html`)

Todo el frontend está en un único archivo HTML con CSS y JS embebidos.

#### Proyectos mostrados (whitelist)

| Clave interna | Nombre mostrado |
|---|---|
| `HELIO - SANTA BEATRIZ` | Helio - Santa Beatriz |
| `LITORAL 900` | Litoral 900 |
| `LOMAS DE CARABAYLLO` | Lomas de Carabayllo |
| `SUNNY` | Sunny |
| `DOMINGO ORUE` | D. Orue |

#### Filtros disponibles
- **Año**: 2024, 2025, 2026 (+ Todos)
- **Mes**: Todos, Enero…Diciembre
- **Proyecto**: Todos + botón por proyecto
- **Vista**: Todos / Ventas / Separaciones
- **Tipo**: Todos / Dpto. / Azotea / Estac. / Depósito
- **Buscar cliente**: texto libre

#### Lógica de datos — Ventas

- Fuente: sheet `VENTAS`
- **Columna precio**: `PrecioVentaSoles` (fallback a `PrecioVenta`)
- **Columna edificio/torre**: `T/M`
- **Fecha de referencia**: `FechaVenta` (fallback a `FechaEntrega_Minuta`)
- **Registros excluidos**: `Estado = Disponible | Bloqueado` o `EstadoOC = Devuelto`
- Incluye: Minuta, Entregado, **Cancelado** (= deuda cancelada = venta pagada completa)

#### Lógica de datos — Separaciones (Stock)

- Fuente: sheet `STOCK`
- **Columna precio**: `PrecioVentaSoles` (fallback a `PrecioVenta`, luego `PrecioLista`)
- **Columna edificio**: `Edificio`
- **Fecha de referencia**: `FechaSepDefinitiva`
- **Registros incluidos**: estados que contengan "separac"

#### Formato de inmuebles por proyecto

```
Lomas de Carabayllo:  "A3 - 502"   (Torre + Edificio - NroInmueble)
Sunny:                "T1 - 201"   (Torre 1 o 2 - NroInmueble)
Otros:                "807"        (NroInmueble directo)
```

#### Metas

- Almacenadas en `localStorage` del navegador
- Si no hay valor guardado, usa `METAS_DEFAULT` hardcodeado:

| Proyecto | Meta Unidades | Meta Monto |
|---|---|---|
| Helio - Santa Beatriz | 9 | S/ 3,850,000 |
| Litoral 900 | 4 | S/ 1,900,000 |
| Lomas de Carabayllo | 8 | S/ 1,400,000 |
| Sunny | 6 | S/ 1,300,000 |
| D. Orue | 2 | S/ 650,000 |

- El usuario puede editar las metas directamente en el dashboard (inputs)

#### Vistas

**Vista Todos**: 4 KPI cards globales (Separaciones totales, Ventas totales, Monto total, Gráfico de meta) + tabla resumen por proyecto con columnas: Meta Und, Meta Monto, Sep, Venta Real, Monto Real

**Vista por Proyecto**: 4 bloques — Meta (unidades + monto editables), Venta Real (unidades + monto con % y barra de progreso), Separaciones (total + monto estimado + sep+venta), Listado de stock

**Vista Operaciones**: tabla con todas las operaciones individuales (ventas + separaciones) con columnas: Proyecto, Estado, Tipo, Inmueble, Cliente, Precio S/, Fecha

---

## Flujo completo de una venta

```
1. Vendedor registra venta en Evolta CRM
2. ETL corre (7am/12pm/5pm) → scraping → Google Sheets actualizado
3. Dashboard auto-refresh cada 1h detecta nuevos datos
   ó usuario presiona ↻ Actualizar
4. Dashboard muestra la nueva venta en Resumen y Operaciones
```

---

## Casos especiales

### Combo (dpto + estacionamiento en una OC)
Evolta exporta los ítems en columnas anchas (`_1`, `_2`…). El ETL hace unpivot a filas separadas. El precio individual de cada ítem viene de `TotalLista_n` (precio post-descuento por ítem), no de `PrecioVenta` global (que es el total del combo).

### Ventas en dólares
Evolta puede registrar precios en USD. El ETL consulta el TC histórico del BCRP para la fecha de cada venta y genera `PrecioVentaSoles`. El dashboard usa siempre esta columna.

### Duplicados por devolución
Si un inmueble fue vendido, devuelto y vuelto a vender, aparecen dos OCs. El dashboard excluye registros con `EstadoOC = Devuelto`.

---

## Despliegue en Railway

1. Push a `main` en GitHub → Railway detecta el cambio y redespliega automáticamente
2. Variables de entorno: ninguna requerida (las URLs de Google Sheets están en el código)
3. Procfile: `web: gunicorn app:app --bind 0.0.0.0:$PORT`

---

## Estructura del repositorio

```
dashboard_separacionesyventas/
├── app.py                          # Flask backend
├── Procfile                        # Railway
├── requirements.txt                # Dependencias del dashboard
├── PROYECTO.md                     # Esta documentación
├── templates/
│   └── index.html                  # Frontend completo
├── etl/
│   ├── ETL_Evolta_Padova_OneDrive.py  # Script ETL
│   └── requirements.txt            # Dependencias del ETL
└── .github/
    └── workflows/
        └── etl.yml                 # GitHub Actions (cron 3x/día)
```

## Configurar GitHub Secrets (obligatorio para GitHub Actions)

Ir a: `github.com/drevillapadova/dashboard_separacionesyventas` → Settings → Secrets and variables → Actions

| Secret | Descripción |
|---|---|
| `EVOLTA_USER` | Usuario de Evolta CRM |
| `EVOLTA_PASS` | Contraseña de Evolta CRM |
| `EMAIL_PASS` | App password de Gmail (`sistema.padova@gmail.com`) |
| `GSHEETS_CREDENTIALS_B64` | Service account JSON en base64* |

**Generar el base64 del JSON (correr en tu PC una vez):**
```bash
python -c "import base64; print(base64.b64encode(open('C:/ETL/evoltareportes-00ffe1b337be.json','rb').read()).decode())"
```
Copiar el resultado completo como valor del secret `GSHEETS_CREDENTIALS_B64`.

## Pendientes / Mejoras futuras

- [ ] Agregar autenticación al dashboard (actualmente público)
- [ ] Histórico de metas por mes (actualmente solo guarda la meta actual)
