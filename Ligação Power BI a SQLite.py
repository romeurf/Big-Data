import pandas as pd
import sqlite3

# Conexão com o banco SQLite
file_path = (r'C:\Users\Romeu\OneDrive\Documentos\Global_Happiness.db')
conn = sqlite3.connect(file_path)
# Leitura da tabela
df = pd.read_sql_query("SELECT * FROM global_happiness", conn)

conn.close()

# Retornar o DataFrame para o Power BI
df
