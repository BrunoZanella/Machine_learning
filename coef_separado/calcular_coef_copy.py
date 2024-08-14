import mysql.connector
import pandas as pd
#from sqlalchemy import create_engine, MetaData, inspect
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error
from datetime import datetime, timedelta, time
from numpy.linalg import cond
from pandas import to_datetime

from sklearn.linear_model import SGDRegressor
from sklearn.preprocessing import StandardScaler
from sklearn import linear_model
import time as tm

import os
import csv

from scipy import stats
import numpy as np 
import statsmodels.api as sm
from   sklearn.metrics import r2_score
import seaborn as sns


tempo_inicial = datetime.now()
print(tempo_inicial)

# Cria uma conexão com o banco de dados
cnx = mysql.connector.connect(
  host="192.168.15.104",
  user="root",
  password="gridbancoteste",
  database="sup_geral"
)

'''
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

'''

# Cria um cursor
cursor = cnx.cursor()

# Executa uma consulta SQL para obter todas as tabelas com o nome 'log_leituras_'
#query_tabelas = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'sup_geral' AND TABLE_NAME LIKE 'log_leituras_%'"
query_tabelas = """
                    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE 
                    TABLE_SCHEMA = 'sup_geral' AND 
                    TABLE_NAME LIKE 'log_leituras_%' ORDER BY TABLE_NAME DESC LIMIT 15
                """
cursor.execute(query_tabelas)

# Busca todos os resultados
resultados_tabelas = cursor.fetchall()

# Converte os resultados em uma lista
tabelas = [resultado[0] for resultado in resultados_tabelas]

ultima_tabela = resultados_tabelas[0][0] if resultados_tabelas else None

''' '''
# Executa uma consulta SQL para obter todos os cod_equipamentos únicos da última tabela
query_equipamentos = f"SELECT DISTINCT cod_equipamento FROM {ultima_tabela} ORDER BY cod_equipamento"
cursor.execute(query_equipamentos)

# Busca todos os resultados
resultados_equipamentos = cursor.fetchall()

# Converte os resultados em uma lista
cod_equipamentos = [str(resultado[0]) for resultado in resultados_equipamentos]
''' '''

# Calcular o total de equipamentos encontrados
total_equipamentos = len(cod_equipamentos)

# Imprimir o resultado
print(f"Total de equipamentos encontrados: {total_equipamentos}")

cod_campo_especificados = ['3', '114', '120']

# Caminho para o diretório e o arquivo CSV
arquivo_coeficiente = "coeficiente_geradores/"
caminho_arquivo_coeficiente = f"{arquivo_coeficiente}coeficiente_geradores.csv"

# Verificar se o diretório existe; se não, criá-lo
if not os.path.exists(arquivo_coeficiente):
    os.makedirs(arquivo_coeficiente)


      

            
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

        # Preencher valores ausentes com zero
        df = df.fillna(0)

        arquivo = "dados_geradores/"
        caminho_arquivo_csv = f"{arquivo}dados_leitura_{cod_equipamento}.csv"

        if not os.path.exists(arquivo):
            os.makedirs(arquivo)

        df.to_csv(caminho_arquivo_csv, index=False)
        
        # Pivotear o DataFrame para obter as colunas desejadas
        df_pivoted = df.pivot(index='rounded_time', columns='cod_campo', values='valor')

        # Renomear as colunas
        df_pivoted.columns = [f'valor_cod_campo_{col}' for col in df_pivoted.columns]

        # Resetar o índice
        df_pivoted = df_pivoted.reset_index()

        # Filtrar o DataFrame para excluir linhas com valores nulos em ambas as colunas
        df_pivoted = df_pivoted.dropna(subset=['valor_cod_campo_3', 'valor_cod_campo_114'])

        # Preencher valores ausentes com zero
        df_pivoted = df_pivoted.fillna(0)

        caminho_arquivo_csv_filtrado = f"{arquivo}dados_filtrados_leitura_{cod_equipamento}.csv"

                                                            
        df_pivoted.to_csv(caminho_arquivo_csv_filtrado, index=False)
        
        print('\n----------------------------------------------------------------------------------------------------------------\n')
        print(f'equipamento: {cod_equipamento} \n')
        print('ultima_data',ultima_data)

        # Converter os valores de X e y para float
        # colocar mais variaveis em X para regressao multipla
        X = df_pivoted['valor_cod_campo_3'].astype(float).values.reshape(-1, 1)  # Recursos de treinamento
        y = df_pivoted['valor_cod_campo_114'].astype(float)  # Alvo de previsão

        coeficiente = 0.0
        acuracia = 0.0

        def treinar_modelo_e_filtrar(X, y, cod_equipamento):
            # Treinar um modelo de regressão linear
        #    reg = linear_model.Ridge(alpha=.5)
            reg = LinearRegression()
            reg.fit(X, y)

            # Prever os valores com o modelo treinado
            y_pred = reg.predict(X)

            # Calcular os resíduos (diferenças entre os valores reais e previstos)
            residuos = y - y_pred

            # Calcular a média e o desvio padrão dos resíduos
            media_residuos = np.mean(residuos)
            desvio_padrao_residuos = np.std(residuos)

            # Definir um limiar (por exemplo, 2 desvios padrão) para identificar valores atípicos
            limiar = 2 * desvio_padrao_residuos

            # Filtrar os valores atípicos
            valores_normais = (abs(residuos) <= limiar)
            X_filtrado = X[valores_normais]
            y_filtrado = y[valores_normais]

            # Adicionar uma coluna de constante a X
            X2_filtrado = sm.add_constant(X_filtrado)

            # Ajustar o modelo de regressão linear
            est_filtrado = sm.OLS(y_filtrado, X2_filtrado)
            est2_filtrado = est_filtrado.fit()

            # Treinar um novo modelo com os valores filtrados
            reg_filtrado = LinearRegression()
            reg_filtrado.fit(X_filtrado, y_filtrado)

            # Prever os valores com o modelo treinado (filtrado)
            y_pred_filtrado = reg_filtrado.predict(X_filtrado)

            # Calcular o coeficiente de determinação (R²) após a filtragem
            r2_filtrado = r2_score(y_filtrado, y_pred_filtrado)
                                    
            coeficiente = round(reg_filtrado.coef_[0], 4)
            r2_filtrado = round(r2_filtrado, 4)
            
            return coeficiente, r2_filtrado

            '''
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
                intercepto = reg.intercept_

                coeficiente = round(coeficiente, 4)
                r2_filtrado = round(r2_test, 4)
                intercepto = round(intercepto, 4)
                print(intercepto)
                
                return coeficiente, r2_filtrado
            '''


        def verificar_e_obter_coeficiente(cod_equipamento):
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
                    
            linhas = []
            equipamento_existe = False
            if os.path.exists(caminho_arquivo_coeficiente):
                with open(caminho_arquivo_coeficiente, 'r') as arquivo_csv:
                    leitor_csv = csv.DictReader(arquivo_csv)
                    for linha in leitor_csv:
                        linhas.append(linha)
                        if linha and linha['cod_equipamento'] == str(cod_equipamento):
                            print(f'Coeficiente encontrado: {linha["coeficiente"]}, acuracia: {linha["Acuracia"]} \n')
                            coeficiente_existente = float(linha['coeficiente'])
                            acuracia_existente = float(linha['Acuracia'])
                            data_existente = to_datetime(linha['ultima_data'])

                            equipamento_existe = True

            coeficiente_novo, acuracia_nova = treinar_modelo_e_filtrar(X, y, cod_equipamento)
            print(f'Novo Coeficiente = {coeficiente_novo}, Acuracia = {acuracia_nova}')
            
            if (not equipamento_existe or acuracia_nova >= acuracia_existente):
                if coeficiente_novo > 0:
                    coeficiente_existente = coeficiente_novo
                    acuracia_existente = acuracia_nova
                    data_existente = ultima_data
                    data_existente = to_datetime(data_existente)

                    with open(caminho_arquivo_coeficiente, 'w', newline='') as arquivo_csv:
                        escritor_csv = csv.writer(arquivo_csv)
                        escritor_csv.writerow(['cod_equipamento', 'coeficiente', 'Acuracia', 'ultima_data'])
                        for linha in linhas:
                            if linha['cod_equipamento'] == str(cod_equipamento):
                                escritor_csv.writerow([str(cod_equipamento), str(coeficiente_existente), str(acuracia_existente), str(data_existente)])
                            else:
                                escritor_csv.writerow([linha['cod_equipamento'], linha['coeficiente'], linha['Acuracia'], linha['ultima_data']])
                        if not equipamento_existe:
                            escritor_csv.writerow([str(cod_equipamento), str(coeficiente_existente), str(acuracia_existente), str(data_existente)])

                    cursor.execute('''
                        INSERT INTO coeficiente_geradores (cod_equipamento, coeficiente, acuracia, ultima_data)
                        VALUES (%s, %s, %s, %s)
                    ''', (cod_equipamento, coeficiente_existente, acuracia_existente, data_existente))

                    cnx.commit()


            return coeficiente_existente, acuracia_existente

        # Verificar e obter o coeficiente
        coeficiente_existente, r2_filtrado_existente = verificar_e_obter_coeficiente(cod_equipamento)
        coeficiente += coeficiente_existente
        acuracia += r2_filtrado_existente


    except Exception as error:
        print('\n----------------------------------------------------------------------------------------------------------------\n')                            
        print(f"Erro ao processar equipamento {cod_equipamento}: {error}")
        continue  # Pular para o próximo equipamento em caso de erro

tempo_final = datetime.now()
total = tempo_final - tempo_inicial
print('\ntempo total de processamento',total)
