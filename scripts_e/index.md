# INER Mobility Network Pipeline

Este repositorio contiene una colección de scripts desarrollados para generar redes bipartitas y de contacto basadas en datos de movilidad en México. Utiliza archivos `.parquet` con información de ubicación de dispositivos móviles, organizados por día, y permite generar representaciones diarias y mensuales de interacción entre AGEBs.

## 📂 Estructura general del pipeline

1. Asignación del AGEB de origen ("home") para cada dispositivo móvil cada día.
2. Unificación mensual de archivos `home`.
3. Generación de red bipartita mensual (dispositivo -> AGEBs visitadas).
4. (Opcional) Generación de red de contacto entre AGEBs.

---

## 📜 Scripts principales

### `HomeAssignation_StartAGEB.py`

Asigna un AGEB de origen ("home") a cada dispositivo móvil para un día específico.

- Entrada: archivos `.parquet` diarios con lecturas por dispositivo.
- Salida: archivo `.csv` con columnas `caid` (ID del dispositivo) y `cvegeo` (AGEB de origen).
- El AGEB se asigna a partir de la **primera lectura del día** para cada dispositivo.
- ✅ Versión modificada: **procesa todos los AGEBs**, sin necesidad de pasar un archivo con AGEBs de interés.

---

### `JoinHomeAssign_StartAGEB.py`

Une múltiples archivos de asignación de "home" (uno por día) en un solo archivo `.parquet` mensual.

- Entrada: archivos `HomeAssign_StartAGEB_yyyy_mm_dd.csv`.
- Salida: archivo `.parquet` con columnas: `date`, `year`, `month`, `day`, `caid`, `ageb`.

---

### `INER_BiNetwork.py`

Genera una red bipartita mensual entre AGEBs de origen (`home`) y AGEBs visitadas (`location`).

- Entrada:
  - Dataset diario de movimientos (`.parquet`)
  - Archivo `.parquet` con asignaciones de home
- Salida: archivo `.parquet` con columnas:
  - `caid` (ID del dispositivo)
  - `day` (día de observación)
  - `home` (AGEB de residencia)
  - `location` (AGEB visitada)
  - `numpings` (número de lecturas en esa AGEB)

---

### `INER_ContactNetwork.py`

Construye una red de contactos entre AGEBs basada en co-presencia de dispositivos en ventanas de tiempo (5 minutos).

- Entrada:
  - Dataset de movimientos de un día específico
  - Archivo de asignación de "home" para ese día (`HomeAssign_StartAGEB_yyyy_mm_dd.csv`)
- Salida: red de contactos diaria en formato `.parquet` con columnas:
  - `home1`, `home2`: AGEBs de residencia de dos dispositivos
  - `contact`: AGEB compartido donde se encontraron
  - `count`: fuerza de contacto (producto del número de dispositivos en ese AGEB)

---

## ⚙️ Script de ejecución general

### `run_bipartite_network_modificado.sh`

Script Bash para automatizar el pipeline completo (puntos 1–3) para los meses de marzo y abril de 2020.

#### Funcionalidad:
- Ejecuta la asignación diaria de "home" para cada dispositivo (sin filtrar AGEBs).
- Une los archivos de asignación diaria en un `.parquet` mensual.
- Genera la red bipartita mensual con información de movilidad entre AGEBs.

#### Cómo usarlo:

```bash
chmod +x run_bipartite_network_modificado.sh
./run_bipartite_network_modificado.sh

