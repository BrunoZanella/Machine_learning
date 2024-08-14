import mysql.connector
import pandas as pd
#from sqlalchemy import create_engine, MetaData, inspect
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta, time

from sklearn.metrics import r2_score
import statsmodels.api as sm
from sklearn.model_selection import train_test_split

from sklearn.preprocessing import StandardScaler
from sklearn import linear_model
from sklearn.linear_model import RidgeCV
from sklearn.preprocessing import scale


from scipy import stats
import numpy as np 


tempo_inicial = datetime.now()
print(tempo_inicial)

# Cria uma conexão com o banco de dados
cnx = mysql.connector.connect(
  host="192.168.15.104",
  user="root",
  password="gridbancoteste",
  database="sup_geral"
)

# Cria um cursor
#cursor = cnx.cursor()
cursor = cnx.cursor(buffered=True)

numero_tabelas = 9

# Calcular a quantidade de tabelas para treino (60%)
percent_treino = 0.6
quantidade_tabelas_treino = int(numero_tabelas * percent_treino)

# Calcular a quantidade de tabelas para teste (40%)
quantidade_tabelas_teste = numero_tabelas - quantidade_tabelas_treino

# Executa uma consulta SQL para obter todas as tabelas que começam com 'log_leituras_'
query_tabelas = f"""
    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE 
    TABLE_SCHEMA = 'sup_geral' AND 
    TABLE_NAME LIKE 'log_leituras_%' ORDER BY TABLE_NAME DESC LIMIT {numero_tabelas}
"""
cursor.execute(query_tabelas)

# Busca todos os resultados
resultados_tabelas = cursor.fetchall()

# Converte os resultados em uma lista
tabelas_lista = [resultado[0] for resultado in resultados_tabelas]

# Seleciona 60% das tabelas para treino e 40% para teste
tabelas_treino = tabelas_lista[:quantidade_tabelas_treino]
tabelas_teste = tabelas_lista[quantidade_tabelas_treino:]

tabelas = tabelas_treino

#print('\ntabelas',tabelas)
#print('\ntabelas_treino',tabelas_treino)
#print('\ntabelas_teste',tabelas_teste)
#print('\n')

ultima_tabela = resultados_tabelas[0][0] if resultados_tabelas else None

# Executa uma consulta SQL para obter todos os cod_equipamentos únicos da última tabela
query_equipamentos = f"SELECT DISTINCT cod_equipamento FROM {ultima_tabela} ORDER BY cod_equipamento"
cursor.execute(query_equipamentos)

# Busca todos os resultados
resultados_equipamentos = cursor.fetchall()

# Converte os resultados em uma lista
cod_equipamentos = [str(resultado[0]) for resultado in resultados_equipamentos]

# Calcular o total de equipamentos encontrados
total_equipamentos = len(cod_equipamentos)

# Imprimir o resultado
print(f"Total de {total_equipamentos} equipamentos na tabela {ultima_tabela}")

cod_campo_especificados = ['3', '114', '120']


def treinar_modelo_e_filtrar(X, y, cod_equipamento):
    # Dividir os dados em conjuntos de treinamento e teste
#   X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    X = scale(X)
    y = scale(y)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Normalizar os dados
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Treinar um modelo de regressão Ridge com validação cruzada para otimizar alpha
#    reg = RidgeCV(alphas=[0.1, 1.0, 10.0], cv=5)
#    reg = RidgeCV(alphas=[0.1, 1.0, 10.0], cv=10)
    reg = RidgeCV(alphas=[10.0, 100.0, 1000.0], cv=5)

    reg.fit(X_train_scaled, y_train)

    # Prever os valores com o modelo treinado nos dados de teste
    y_pred_test = reg.predict(X_test_scaled)

    # Calcular o coeficiente de determinação (R²) nos dados de teste
    r2_test = r2_score(y_test, y_pred_test)

    # Obter o coeficiente do modelo treinado
    coeficiente = reg.coef_[0]

    coeficiente = round(coeficiente, 4)
    r2_filtrado = round(r2_test, 4)

    return coeficiente, r2_filtrado



def verificar_e_obter_coeficiente(cod_equipamento, X, y, ultima_data):
    coeficiente_existente = 0.0
    acuracia_existente = 0.0
    data_existente = None

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS coeficiente_geradores (
            cod_equipamento INT,
            coeficiente FLOAT,
            acuracia FLOAT,
            ultima_data DATETIME
        )
    ''')

    # Verifica se o equipamento existe na tabela
    cursor.execute(f"SELECT * FROM coeficiente_geradores WHERE cod_equipamento = {cod_equipamento} ORDER BY cod_equipamento ASC")
    resultado = cursor.fetchone()

    if resultado is not None:
        coeficiente_existente = resultado[1]
        acuracia_existente = resultado[2]
        data_existente = resultado[3]
        print(f'Coeficiente Existente = {coeficiente_existente}, Acuracia = {acuracia_existente}')

    coeficiente_novo, acuracia_nova = treinar_modelo_e_filtrar(X, y, cod_equipamento)
    print(f'Novo Coeficiente = {coeficiente_novo}, Acuracia = {acuracia_nova}')

    if coeficiente_novo > 0 and (resultado is None or acuracia_nova >= acuracia_existente):
        coeficiente_existente = coeficiente_novo
        acuracia_existente = acuracia_nova
        data_existente = ultima_data

        # Converter o formato da data
        data_existente = pd.to_datetime(data_existente)

        if resultado is None:
            cursor.execute('''
                INSERT INTO coeficiente_geradores (cod_equipamento, coeficiente, acuracia, ultima_data)
                VALUES (%s, %s, %s, %s)
             ''', (cod_equipamento, coeficiente_existente, acuracia_existente, data_existente))
        else:
            cursor.execute('''
                UPDATE coeficiente_geradores
                SET coeficiente = %s, acuracia = %s, ultima_data = %s
                WHERE cod_equipamento = %s
             ''', (coeficiente_existente, acuracia_existente, data_existente, cod_equipamento))

        cnx.commit()

    return coeficiente_existente, acuracia_existente

       
            
for cod_equipamento in cod_equipamentos:
    try:
        queries = []
        for tabela in tabelas:
            query = f"SELECT data_cadastro, valor, cod_campo FROM {tabela} WHERE cod_equipamento = {cod_equipamento} AND cod_campo IN ({', '.join(cod_campo_especificados)})"
            queries.append(query)

        final_query = " UNION ALL ".join(queries)
        cursor.execute(final_query)
        resultados = cursor.fetchall()

        # Converte os resultados em um DataFrame
        df = pd.DataFrame(resultados, columns=['data_cadastro', 'valor', 'cod_campo'])

        # Converter a coluna 'data_cadastro' para o tipo datetime
        df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])

        if not df.empty:
            ultima_data = df['data_cadastro'].iloc[-1]
        
        # Arredondar o tempo para o intervalo de 5 minutos
        df['rounded_time'] = df['data_cadastro'].dt.round('5min')

        # Remova duplicatas do índice
        df = df[~df.index.duplicated(keep='first')]

        # Substituir valores problemáticos antes de converter para float
        df['valor'] = df['valor'].replace({'-1-1': np.nan})

        # Converter a coluna 'valor' para o tipo float
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')

        # Agregar valores duplicados pela média
        df = df.groupby(['rounded_time', 'cod_campo']).agg({'valor': 'mean'}).reset_index()

        # Remover linhas com valores infinitos
        df = df.replace([np.inf, -np.inf], np.nan).dropna()

        # Preencher valores ausentes usando interpolação linear
        df = df.interpolate()

        # Pivotear o DataFrame para obter as colunas desejadas
        df_pivoted = df.pivot(index='rounded_time', columns='cod_campo', values='valor')

        # Renomear as colunas
        df_pivoted.columns = [f'valor_cod_campo_{col}' for col in df_pivoted.columns]

        # Resetar o índice
        df_pivoted = df_pivoted.reset_index()

        # Filtrar o DataFrame para excluir linhas com valores nulos em ambas as colunas
        df_pivoted = df_pivoted.dropna(subset=['valor_cod_campo_3', 'valor_cod_campo_114'])

        # Preencher valores ausentes usando interpolação linear
        df_pivoted = df_pivoted.interpolate()

        # Filtrar outliers usando z-score
        z_scores = np.abs(stats.zscore(df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_114']]))
        outliers_mask = (z_scores < 3).all(axis=1)
        df_pivoted = df_pivoted[outliers_mask]


        print('\n----------------------------------------------------------------------------------------------------------------\n')
        print(f'equipamento: {cod_equipamento} \n')
        print('ultima_data', ultima_data)

        # Converter os valores de X e y para float
        X = df_pivoted['valor_cod_campo_3'].astype(float).values.reshape(-1, 1)  # Recursos de treinamento
        y = df_pivoted['valor_cod_campo_114'].astype(float)  # Alvo de previsão

        coeficiente = 0.0
        acuracia = 0.0

        # Verificar e obter o coeficiente
        coeficiente_existente, r2_filtrado_existente = verificar_e_obter_coeficiente(cod_equipamento, X, y, ultima_data)
        coeficiente += coeficiente_existente
        acuracia += r2_filtrado_existente


    except Exception as error:
        print('\n----------------------------------------------------------------------------------------------------------------\n')                            
        print(f"Erro ao processar equipamento {cod_equipamento}: {error}")
        continue  # Pular para o próximo equipamento em caso de erro

cursor.close()
cnx.close()

tempo_final = datetime.now()
total = tempo_final - tempo_inicial
print('\ntempo total de processamento',total)

