import pandas as pd
import matplotlib.pyplot as plt

# Datos de MongoDB
mongo_data = {
    "Velocidad Inserción": 14870,
    "Búsqueda por email": 3,
    "Conteo por departamento": 7,
    "Búsqueda rango edad": 2,
    "Agregación salario promedio": 56,
}

# Datos de MySQL
mysql_data = {
    "Velocidad Inserción": 7508,
    "Búsqueda por email": 1,
    "Conteo por departamento": 5,
    "Búsqueda rango edad": 1,
    "Agregación salario promedio": 174,
    "Búsqueda en JSON": 178,
    "Consulta compleja": 77,
}

df_mongo = pd.DataFrame(list(mongo_data.items()), columns=["Métrica", "MongoDB"])
df_mysql = pd.DataFrame(list(mysql_data.items()), columns=["Métrica", "MySQL"])

df = pd.merge(df_mongo, df_mysql, on="Métrica", how="outer")

insertion_metrics = df[df["Métrica"] == "Velocidad Inserción"]
query_metrics = df[df["Métrica"].isin(["Búsqueda por email", "Conteo por departamento", "Búsqueda rango edad"])]
aggregation_metrics = df[df["Métrica"].isin(["Agregación salario promedio"])]

fig1, ax1 = plt.subplots(figsize=(6, 4))
insertion_metrics.set_index("Métrica").plot.bar(rot=0, color=["tab:blue", "tab:orange"], ax=ax1)
ax1.set_title("Velocidad de Inserción (docs/segundo)\nMongoDB vs MySQL")
ax1.set_ylabel("docs/segundo")
ax1.grid(True, axis='y', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()

fig2, ax2 = plt.subplots(figsize=(8, 4))
query_metrics.set_index("Métrica").plot.bar(rot=45, color=["tab:blue", "tab:orange"], ax=ax2)
ax2.set_title("Tiempos de Consultas Simples (ms)\nMongoDB vs MySQL")
ax2.set_ylabel("ms")
ax2.grid(True, axis='y', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()

fig3, ax3 = plt.subplots(figsize=(10, 5))
aggregation_metrics.set_index("Métrica").plot.bar(rot=45, color=["tab:blue", "tab:orange"], ax=ax3)
ax3.set_title("Tiempos de Agregaciones y Consultas Complejas (ms)\nMongoDB vs MySQL")
ax3.set_ylabel("ms")
ax3.grid(True, axis='y', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()
