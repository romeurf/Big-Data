import pandas as pd
from sqlalchemy import create_engine

# =============================================================================
# 1. Caminho e Leitura do ficheiro Excel
# =============================================================================
excel_file = 'merged_dataset.xlsx'
df = pd.read_excel(excel_file)

# =============================================================================
# 2. Limpeza dos nomes das colunas
# =============================================================================
df.columns = (
    df.columns
    .str.lower()
    .str.strip()
    .str.replace(' ', '_')
    .str.replace(r'[^a-z0-9_]', '', regex=True)
)

# =============================================================================
# 3. Definição do caminho e nome da base de dados SQLite
# =============================================================================
sqlite_file = 'Global_Happiness.db'
table_name = 'global_happiness'

# =============================================================================
# 4. Criação da conexão com a base de dados SQLite
# =============================================================================
engine = create_engine(f'sqlite:///{sqlite_file}')

# =============================================================================
# 5. Inserção dos dados na base de dados
# =============================================================================
df.to_sql(table_name, con=engine, if_exists='replace', index=False)

print('Dados guardados com sucesso em Global_Happiness.db')

# =============================================================================
# 6. Leitura dos dados da base de dados para verificação
# =============================================================================
df_verificado = pd.read_sql(f'SELECT * FROM {table_name}', con=engine)

# =============================================================================
# 7. Mostrar as primeiras linhas para verificação
# =============================================================================
print(df_verificado.head())

# =============================================================================
# 8. Guardar uma cópia do DataFrame em ficheiro Excel
# =============================================================================
df_verificado.to_excel('Global_Happiness_SQLite.xlsx', index=False)
print('Ficheiro guardado como Global_Happiness_SQLite.xlsx')