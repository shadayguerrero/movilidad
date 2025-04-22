import os
import glob
import pandas as pd
import networkx as nx

# 📁 Directorio con archivos OD diarios
input_dir = "/data/shaday_data/GIT/movilidad/salidas_od"
archivos = sorted(glob.glob(f"{input_dir}/od_municipio_2020_*.csv"))

# 📁 Carpeta de salida para las métricas
output_metrics = os.path.join(input_dir, "metrics_por_dia")
os.makedirs(output_metrics, exist_ok=True)

for archivo in archivos:
    df = pd.read_csv(archivo)
    fecha = os.path.basename(archivo).replace("od_municipio_", "").replace(".csv", "")

    if df.empty:
        print(f"⚠️ Archivo vacío: {archivo}")
        continue

    # Crear grafo dirigido con pesos
    G = nx.DiGraph()
    for _, row in df.iterrows():
        G.add_edge(row["origen"], row["destino"], weight=row["peso"])

    # Calcular métricas por nodo
    grados_entrantes = dict(G.in_degree())
    grados_salientes = dict(G.out_degree())
    fuerzas_entrantes = dict(G.in_degree(weight="weight"))
    fuerzas_salientes = dict(G.out_degree(weight="weight"))

    resumen = pd.DataFrame({
        "cve_mun": list(G.nodes()),
        "grado_entrante": [grados_entrantes.get(n, 0) for n in G.nodes()],
        "grado_saliente": [grados_salientes.get(n, 0) for n in G.nodes()],
        "fuerza_entrante": [fuerzas_entrantes.get(n, 0) for n in G.nodes()],
        "fuerza_saliente": [fuerzas_salientes.get(n, 0) for n in G.nodes()],
    })

    # Guardar archivo de métricas
    resumen_path = os.path.join(output_metrics, f"metrics_{fecha}.csv")
    resumen.to_csv(resumen_path, index=False)
    print(f"📊 Guardadas métricas para {fecha}: {resumen_path}")

