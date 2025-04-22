
# 🛰️ Scripts para Análisis de "Home Diario" y Primer Ping por Horario

Este conjunto de scripts permite analizar el comportamiento de dispositivos a partir de sus registros de ubicación (`pings`) en formato `.parquet`. Se enfoca principalmente en:

1. Asignar un "home diario" a cada dispositivo (`caid`) según su ubicación durante la madrugada (00:00–08:00).
2. Contar cuántos dispositivos se activan por primera vez **antes o después de las 08:00 a.m.**
3. Exportar resultados en formato `.csv` para análisis posterior.

---

## 📁 Estructura de entrada esperada

Los archivos `.parquet` deben estar organizados por fecha en la siguiente estructura:

```
/data/covid/aws/movement_parquet/
├── year=2020/
│   ├── month=03/
│   │   ├── day=15/
│   │   │   └── part-*.parquet
```

Cada archivo debe incluir al menos estas columnas:
- `caid`: identificador único del dispositivo
- `utc_timestamp`: tiempo en formato UNIX (BIGINT)
- `cve_ent`, `cve_mun`: claves de estado y municipio

---

## ⚙️ Script 1: `conteo_home_por_tiempo_y_primero.py`

**Objetivo:**  
Asigna el **home diario** a cada `caid` con estas reglas:

- Si el dispositivo tiene pings entre las **00:00 y 08:00**, se usa el municipio donde **más tiempo estuvo**.
- Si no tiene pings en ese rango, se asigna el municipio de su **primer ping del día**.

**Salida:**  
Un archivo `.csv` por día con el número de dispositivos cuyo "home" fue cada municipio (`cve_ent` + `cve_mun` concatenados como `cve_mun_full`):

```
conteo_home_2020_03_15.csv
```

**Ejemplo de ejecución:**

```bash
python conteo_home_por_tiempo_y_primero.py \
  -d 2020-03-15 \
  -db /data/covid/aws/movement_parquet \
  -o /data/shaday_data/GIT/movilidad/home_fusionado
```

---

## ⚙️ Script 2: `lanzar_conteo_home_batch.py`

**Objetivo:**  
Automatiza la ejecución del script anterior sobre un rango de fechas definido.

**Ejemplo de ejecución:**

```bash
python lanzar_conteo_home_batch.py
```

**Salida:**  
Varios archivos `conteo_home_YYYY_MM_DD.csv` generados en el directorio de salida.

---

## ⚙️ Script 3: `conteo_dispositivos_antes_despues_08.py`

**Objetivo:**  
Cuenta cuántos dispositivos se activan por primera vez:
- **Antes de las 08:00 a.m.**
- **Después de las 08:00 a.m.**

**Salida:**  
Un archivo resumen con un renglón por día:

```
primer_ping_08.csv
```

| fecha       | antes_08 | despues_08 |
|-------------|----------|------------|
| 2020-03-15  | 74312    | 12123      |
| 2020-03-16  | 75100    | 11211      |

**Ejemplo de ejecución:**

```bash
python conteo_dispositivos_antes_despues_08.py
```

> Puedes modificar el rango de fechas dentro del script, cambiando esta línea:

```python
fechas = pd.date_range(start="2020-03-15", end="2020-03-22")
```

---

## 🛠️ Requisitos

- Python 3.8 o superior
- DuckDB: `pip install duckdb`
- Pandas: `pip install pandas`
- tqdm: `pip install tqdm`

---

## 📌 Notas adicionales

- El campo `utc_timestamp` debe estar en formato UNIX (BIGINT).
- La clave `cve_mun_full` se construye como `cve_ent` + `cve_mun`, con ceros a la izquierda en `cve_mun` para longitud fija.
- Todos los resultados son exportados como `.csv` para facilitar análisis posteriores (en Python, R, Excel, etc.).
