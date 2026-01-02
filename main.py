import pandas as pd
from pathlib import Path

ruta = Path("C:\\Almacén\\USJ_2025_2026\\IngenieriadeDatos\\GitHub\\data-engineering_course-project\\pobmun")  # <-- cambia esto
archivos = sorted(ruta.glob("pobmun*.xls*"))

dfs = []

texto_a_eliminar = "Cifras de poblaci"

for f in archivos:
    df = pd.read_excel(f)

    # 1️⃣ Eliminar columna problemática si existe
    if "CPRO" in df.columns:
        df = df.drop(columns=["CPRO"])

    # 2️⃣ Eliminar filas de texto/nota
    df = df[~df.apply(
        lambda fila: fila.astype(str).str.contains(texto_a_eliminar, case=False, na=False).any(),
        axis=1
    )]

    # 3️⃣ (Opcional pero recomendado) origen del dato
    df["origen_archivo"] = f.name

    dfs.append(df)

# 4️⃣ Unir todo
df_total = pd.concat(dfs, ignore_index=True)

# 5️⃣ Guardar resultado
df_total.to_csv("pobmun_total.csv", index=False, encoding="utf-8")
