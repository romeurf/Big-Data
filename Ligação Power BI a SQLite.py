import pandas as pd
import sqlite3

# Conexão com o banco SQLite
conn = sqlite3.connect('C:/Users/Romeu/OneDrive/Ambiente de Trabalho/Uni/Bioinf/1║ Ano/2║ Semestre/Big Data/Relat≤rio/Global_Happiness.db')

# Leitura da tabela
df = pd.read_sql_query("SELECT * FROM global_happiness", conn)

conn.close()

# Retornar o DataFrame para o Power BI
df