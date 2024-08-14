import mysql.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Cria uma conexão com o banco de dados
cnx = mysql.connector.connect(
  host="192.168.15.104",
  user="root",
  password="gridbancoteste",
  database="sup_geral"
)

tabelas = [
#    'log_leituras_2022_06',
#    'log_leituras_2022_07',
#    'log_leituras_2022_08',
#    'log_leituras_2022_09',
#    'log_leituras_2022_10',
#    'log_leituras_2022_11',
#    'log_leituras_2022_12',
    
#    'log_leituras_2023_01',
#    'log_leituras_2023_02',
#    'log_leituras_2023_03',
    'log_leituras_2023_04',
    'log_leituras_2023_05',
    'log_leituras_2023_06',
    'log_leituras_2023_07',
    'log_leituras_2023_08'
]

cod_campo_especificados = ['3', '114', '120']

# Cria um cursor
cursor = cnx.cursor()

inicio = datetime.now()
print(inicio)

# Executa uma consulta SQL para obter todos os cod_equipamentos únicos da última tabela
query_equipamentos = "SELECT DISTINCT cod_equipamento FROM log_leituras_2023_08 ORDER BY cod_equipamento"
cursor.execute(query_equipamentos)
print(datetime.now())

# Busca todos os resultados
resultados_equipamentos = cursor.fetchall()

# Converte os resultados em uma lista
cod_equipamentos = [str(resultado[0]) for resultado in resultados_equipamentos]

for cod_equipamento in cod_equipamentos:
    queries = []
    for tabela in tabelas:
        query = f"SELECT data_cadastro, valor, cod_campo FROM {tabela} WHERE cod_equipamento = {cod_equipamento} AND cod_campo IN ({', '.join(cod_campo_especificados)})"
        queries.append(query)

    final_query = " UNION ALL ".join(queries)
    cursor.execute(final_query)
    resultados = cursor.fetchall()

    # Converte os resultados em um DataFrame
    df = pd.DataFrame(resultados, columns=['data_cadastro', 'valor', 'cod_campo'])

    # Aplica a filtragem
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])
    df['rounded_time'] = df['data_cadastro'].dt.round('5min')
    df = df[~df.index.duplicated(keep='first')]
    df['valor'] = df['valor'].replace({'-1-1': np.nan})
    df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
    df = df.groupby(['rounded_time', 'cod_campo']).agg({'valor': 'mean'}).reset_index()
    df = df.replace([np.inf, -np.inf], np.nan).dropna()
    df = df.fillna(0)

    df_filtrado = df[(df['cod_campo'] == 120) & (df['valor'] > 0)]
    if not df_filtrado.empty:
        ultimo_valor = df_filtrado.iloc[-1]['valor']
        cod_campo_120 = ultimo_valor
                        
    df_pivoted = df.pivot(index='rounded_time', columns='cod_campo', values='valor')

    df_pivoted.columns = [f'valor_cod_campo_{col}' for col in df_pivoted.columns]

    df_pivoted = df_pivoted.reset_index()

    df_pivoted = df_pivoted.fillna(0)

    arquivo = "dados_geradores2/"
    caminho_arquivo_csv_filtrado = f"{arquivo}dados_filtrados_leitura_{cod_equipamento}.csv"

    caminho_arquivo_csv = f"{arquivo}dados_leitura_{cod_equipamento}.csv"

    if not os.path.exists(arquivo):
        os.makedirs(arquivo)

    df_pivoted.to_csv(caminho_arquivo_csv_filtrado, index=False)
    df.to_csv(caminho_arquivo_csv, index=False)

    print('\n----------------------------------------------------------------------------------------------------------------\n')
    print(f'equipamento: {cod_equipamento}')
    print(df)

final = datetime.now()
total = final - inicio
print(total)

# Fecha o cursor e a conexão
cursor.close()
cnx.close()



'''
import mysql.connector
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# Cria uma conexão com o banco de dados
cnx = mysql.connector.connect(
  host="192.168.15.104",
  user="root",
  password="gridbancoteste",
  database="sup_geral"
)

tabelas = [
    'log_leituras_2022_06',
    'log_leituras_2022_07',
    'log_leituras_2022_08',
    'log_leituras_2022_09',
    'log_leituras_2022_10',
    'log_leituras_2022_11',
    'log_leituras_2022_12',
    
    'log_leituras_2023_01',
    'log_leituras_2023_02',
    'log_leituras_2023_03',
    'log_leituras_2023_04',
    'log_leituras_2023_05',
    'log_leituras_2023_06',
    'log_leituras_2023_07',
    'log_leituras_2023_08'
]

cod_campo_especificados = ['3', '114', '120']

# Cria um cursor
cursor = cnx.cursor()

# Executa uma consulta SQL para obter todos os cod_equipamentos únicos da última tabela
query_equipamentos = "SELECT DISTINCT cod_equipamento FROM log_leituras_2023_08 ORDER BY cod_equipamento"
cursor.execute(query_equipamentos)

# Busca todos os resultados
resultados_equipamentos = cursor.fetchall()

# Converte os resultados em uma lista
cod_equipamentos = [resultado[0] for resultado in resultados_equipamentos]

queries = []

for tabela in tabelas:
    query = f"SELECT data_cadastro, valor, cod_campo FROM {tabela} WHERE cod_equipamento IN ({', '.join(map(str, cod_equipamentos))}) AND cod_campo IN ({', '.join(cod_campo_especificados)})"
    queries.append(query)

final_query = " UNION ALL ".join(queries)

print(datetime.now())

# Executa a consulta SQL
cursor.execute(final_query)

# Busca todos os resultados
resultados = cursor.fetchall()

for equipamento in cod_equipamentos:
    print(equipamento)
    
print(datetime.now())

# Fecha o cursor e a conexão
cursor.close()
cnx.close()
'''