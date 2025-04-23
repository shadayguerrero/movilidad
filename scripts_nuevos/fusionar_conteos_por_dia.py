import os
import pandas as pd
from glob import glob

# Función para normalizar claves tipo cve_ent + cve_mun (ej. '02004')
def normalizar_cve_mun(clave):
    clave_str = str(clave)
    return clave_str.zfill(5) if len(clave_str) < 5 else clave_str[:5]

# Rutas de entrada por método
paths = {
    "home_00_08": "/data/shaday_data/GIT/movilidad/outputs/filtered/home_00_08",
    "home_00_05": "/data/shaday_data/GIT/movilidad/outputs/filtered/home_00_05",  # ventana 00–05
    "home_primero": "/data/shaday_data/GIT/movilidad/outputs/home_00"             # primer ping
}

# Directorio de salida
output_dir = "/data/shaday_data/GIT/movilidad/outputs/combinados_por_dia"
os.makedirs(output_dir, exist_ok=True)

# Archivos por método
files = {
    metodo: sorted(glob(os.path.join(ruta, "*.csv")))
    for metodo, ruta in paths.items()
}

# Extraer fechas disponibles desde home_00_08
fechas = sorted([
    os.path.basename(f).split("_")[-1].replace(".csv", "")
    for f in files["home_00_08"]
])

# Procesar y combinar por fecha
for fecha in fechas:
    dfs = []
    for metodo, filelist in files.items():
        match = [f for f in filelist if fecha in f]
        if match:
            df = pd.read_csv(match[0])
            df.columns = ["cve_mun_full", f"num_dispositivos_{metodo}"]
            df["cve_mun_full"] = df["cve_mun_full"].apply(normalizar_cve_mun)  # 💡 clave corregida
        else:
            df = pd.DataFrame(columns=["cve_mun_full", f"num_dispositivos_{metodo}"])
        dfs.append(df)

    # Fusión progresiva por cve_mun_full
    df_final = dfs[0]
    for df in dfs[1:]:
        df_final = pd.merge(df_final, df, on="cve_mun_full", how="outer")

    # Llenar nulos y convertir a int
    df_final = df_final.fillna(0)
    for col in df_final.columns:
        if col.startswith("num_dispositivos_"):
            df_final[col] = df_final[col].astype(int)

    # Guardar resultado combinado por día
    output_path = os.path.join(output_dir, f"conteo_combinado_{fecha}.csv")
    df_final.to_csv(output_path, index=False)
    print(f"✅ Guardado: {output_path}")
