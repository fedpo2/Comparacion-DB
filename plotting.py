import pandas as pd
import matplotlib.pyplot as plt

# Datos consolidados (sin MongoDB, con SQL Server)
data = {
    "Velocidad Inserción": {"MySQL": 7508, "PostgreSQL": 9406, "SQL Server": 4340},
    "Búsqueda por email": {"MySQL": 1, "PostgreSQL": 2, "SQL Server": 3},
    "Conteo por departamento": {"MySQL": 5, "PostgreSQL": 16, "SQL Server": 476},
    "Búsqueda rango edad": {"MySQL": 1, "PostgreSQL": 2, "SQL Server": 6},
    "Agregación salario promedio": {"MySQL": 174, "PostgreSQL": 35, "SQL Server": 55},
    "Búsqueda en JSON": {"MySQL": 178, "PostgreSQL": 32, "SQL Server": 166},
    "Consulta compleja": {"MySQL": 77, "PostgreSQL": 20, "SQL Server": 116}
}

# Crear DataFrames por tipo de consulta
insertion_df = pd.DataFrame([data["Velocidad Inserción"]]).T
query_simple_df = pd.DataFrame({
    k: data[k] for k in ["Búsqueda por email", "Búsqueda rango edad"]
}).T
query_agg_count_df = pd.DataFrame({
    k: data[k] for k in ["Conteo por departamento", "Agregación salario promedio"]
}).T
query_json_complex_df = pd.DataFrame({
    k: data[k] for k in ["Búsqueda en JSON", "Consulta compleja"]
}).T

# Colores: MySQL (azul), PostgreSQL (verde), SQL Server (rojo)
colors = ["tab:blue", "tab:green", "tab:red"]

# --- Gráfico 1: Velocidad de Inserción ---
fig1, ax1 = plt.subplots(figsize=(8, 5))
insertion_df.plot.bar(ax=ax1, color=colors)
ax1.set_title("📈 Velocidad de Inserción (registros/segundo)\nMySQL vs PostgreSQL vs SQL Server")
ax1.set_ylabel("docs/segundo")
ax1.grid(True, axis='y', linestyle='--', alpha=0.6)
for container in ax1.containers:
    ax1.bar_label(container)
plt.tight_layout()
plt.show()
plt.close(fig1)
fig1.savefig("./insertvel.png")

# --- Gráfico 2: Consultas simples ---
fig2, ax2 = plt.subplots(figsize=(8, 5))
query_simple_df.plot.bar(ax=ax2, color=colors)
ax2.set_title("🔍 Tiempos de Consultas Simples (ms)\nMySQL vs PostgreSQL vs SQL Server")
ax2.set_ylabel("ms")
ax2.grid(True, axis='y', linestyle='--', alpha=0.6)
for container in ax2.containers:
    ax2.bar_label(container)
plt.tight_layout()
plt.show()
plt.close(fig2)
fig2.savefig("./consultas_simples.png")

# --- Gráfico 3: Conteos y agregaciones ---
fig3, ax3 = plt.subplots(figsize=(8, 5))
query_agg_count_df.plot.bar(ax=ax3, color=colors)
ax3.set_title("🧮 Conteos y Agregaciones (ms)\nMySQL vs PostgreSQL vs SQL Server")
ax3.set_ylabel("ms")
ax3.grid(True, axis='y', linestyle='--', alpha=0.6)
for container in ax3.containers:
    ax3.bar_label(container)
plt.tight_layout()
plt.show()
plt.close(fig3)
fig3.savefig("./agregacion_salario.png")

# --- Gráfico 4: JSON y consultas complejas ---
fig4, ax4 = plt.subplots(figsize=(8, 5))
query_json_complex_df.plot.bar(ax=ax4, color=colors)
ax4.set_title("🔌 JSON y Consultas Complejas (ms)\nMySQL vs PostgreSQL vs SQL Server")
ax4.set_ylabel("ms")
ax4.grid(True, axis='y', linestyle='--', alpha=0.6)
for container in ax4.containers:
    ax4.bar_label(container)
plt.tight_layout()
plt.show()
plt.close(fig4)
fig4.savefig("./json.png")
