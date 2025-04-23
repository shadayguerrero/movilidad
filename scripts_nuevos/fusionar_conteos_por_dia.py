import os
import pandas as pd
from glob import glob

# Rutas de entrada por método
paths = {
    "home_00_08": "/data/shaday_data/GIT/movilidad/outputs/filtered/home_00_08",
    "home_00_05": "/data/shaday_data/GIT/movilidad/outputs/filtered/home_00_05",  # esta es 00–05
    "home_primero": "/data/shaday_data/GIT/movilidad/outputs/home_00"
}

# Directorio de salida
output_dir = "/data/shaday_data/GIT/movilidad/outputs/combinados_por_dia"
os.makedirs(output_dir, exist_ok=True)

# Archivos por método
files = {
    metodo: sorted(glob(os.path.join(ruta, "*.csv")))
    for metodo, ruta in paths.items()
}

# Extraer fechas disponibles en home_00_08
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
            df["cve_mun_full"] = df["cve_mun_full"].astype(str)  # 🔧 FORZAR TIPO STRING
        else:
            df = pd.DataFrame(columns=["cve_mun_full", f"num_dispositivos_{metodo}"])
        dfs.append(df)

    # Fusionar por cve_mun_full
    df_final = dfs[0]
    for df in dfs[1:]:
        df_final = pd.merge(df_final, df, on="cve_mun_full", how="outer")

    df_final = df_final.fillna(0)
    for col in df_final.columns:
        if col.startswith("num_dispositivos_"):
            df_final[col] = df_final[col].astype(int)

    # Guardar CSV final
    output_path = os.path.join(output_dir, f"conteo_combinado_{fecha}.csv")
    df_final.to_csv(output_path, index=False)
    print(f"✅ Guardado: {output_path}")
