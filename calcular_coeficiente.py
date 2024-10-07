import mysql.connector
import pandas as pd
#from sqlalchemy import create_engine, MetaData, inspect
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier

from datetime import datetime, timedelta, time

from sklearn.metrics import r2_score
import statsmodels.api as sm
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn.tree import DecisionTreeRegressor
from sklearn.linear_model import LassoCV, ElasticNetCV

from sklearn.preprocessing import StandardScaler
from sklearn import linear_model
from sklearn.linear_model import RidgeCV
from sklearn.preprocessing import scale
from sklearn.svm import SVR
from sklearn.metrics import mean_squared_error
from sklearn.linear_model import Ridge, Lasso
from sklearn.preprocessing import PolynomialFeatures

from scipy import stats
import numpy as np 
import sys
from scipy.stats import boxcox
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge, LinearRegression
from sklearn.metrics import r2_score
import statsmodels.api as sm
from scipy import stats
from statsmodels.graphics.tsaplots import plot_acf


tempo_inicial = datetime.now()
print(tempo_inicial)

cnx = mysql.connector.connect(
  host="192.168.4.50",
  user="bruno",
  password="superbancoml",
  database="machine_learning"
)
cursor = cnx.cursor(buffered=True)

cnx_leitura = mysql.connector.connect(
  host="192.168.4.50",
  user="bruno",
  password="superbancoml",
  database="sup_geral"
)
cursor_leitura = cnx_leitura.cursor(buffered=True)


def selecionar_GMG():
    cursor_leitura.execute("SELECT codigo, ativo FROM tipos_equipamentos WHERE classe = 'GMG'")
    resultados = cursor_leitura.fetchall()
    codigos = []

    for resultado in resultados:
        codigos.append(resultado[0])
    
    return codigos


numero_tabelas = 10

percent_treino = 0.3

quantidade_tabelas_treino = int(numero_tabelas * percent_treino)

quantidade_tabelas_teste = numero_tabelas - quantidade_tabelas_treino

query_tabelas = f"""
    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE 
    TABLE_SCHEMA = 'sup_geral' AND 
    TABLE_NAME LIKE 'log_leituras_%' ORDER BY TABLE_NAME DESC LIMIT {numero_tabelas}
"""
cursor_leitura.execute(query_tabelas)

resultados_tabelas = cursor_leitura.fetchall()

tabelas_lista = [resultado[0] for resultado in resultados_tabelas]

tabelas_treino = tabelas_lista[:quantidade_tabelas_treino]
tabelas_teste = tabelas_lista[quantidade_tabelas_treino:]

tabelas = tabelas_treino

ultima_tabela = resultados_tabelas[0][0] if resultados_tabelas else None

codigos_GMG = selecionar_GMG()

codigos_GMG_str = ', '.join(map(str, codigos_GMG))

query_equipamentos = f"SELECT DISTINCT codigo FROM equipamentos WHERE cod_tipo_equipamento IN ({codigos_GMG_str}) AND ativo = 1"
cursor_leitura.execute(query_equipamentos)
resultados_equipamentos = cursor_leitura.fetchall()
cod_equipamentos = [str(resultado[0]) for resultado in resultados_equipamentos]

query_ultima_tabela = f"SELECT DISTINCT cod_equipamento FROM {ultima_tabela}"
cursor_leitura.execute(query_ultima_tabela)
resultados_ultima_tabela = cursor_leitura.fetchall()
cod_ultima_tabela = [str(resultado[0]) for resultado in resultados_ultima_tabela]

cod_equipamentos_validos = list(set(cod_equipamentos) & set(cod_ultima_tabela))

cod_equipamentos = cod_ultima_tabela

total_equipamentos = len(cod_equipamentos_validos)

cod_equipamentos_validos = sorted([int(cod) for cod in cod_equipamentos_validos])

cod_campo_especificados = ['3', '114']

print('\ntabelas',tabelas)
print('\nultima_tabela',ultima_tabela)
print('\n')
print(f"Total de {total_equipamentos} equipamentos na tabela {ultima_tabela}")


total_equipamentos_com_valores = 0
equipamentos_com_valores = []

for cod_equipamento in cod_equipamentos_validos:
    tem_valores = False
    for tabela in tabelas:
        query_verificar_valores = f"""
            SELECT COUNT(*) FROM {tabela} 
            WHERE cod_equipamento = {cod_equipamento} 
            AND cod_campo IN ({', '.join(cod_campo_especificados)}) 
            AND valor IS NOT NULL
            AND valor > 0
        """
        cursor_leitura.execute(query_verificar_valores)
        resultado = cursor_leitura.fetchone()
        if resultado[0] > 0:
            equipamentos_com_valores.append(cod_equipamento)
            tem_valores = True
            break
    
    if tem_valores:
        total_equipamentos_com_valores += 1

print(f"Total de equipamentos válidos com valores nos campos especificados: {total_equipamentos_com_valores}")


# Importante para verificar valores válidos em várias tabelas
equipamentos_validados = []

for cod_equipamento in cod_equipamentos_validos:
    tabelas_com_valores = 0
    
    for tabela in tabelas:
        query_verificar_valores = f"""
            SELECT COUNT(*) FROM {tabela} 
            WHERE cod_equipamento = {cod_equipamento} 
            AND cod_campo IN ({', '.join(cod_campo_especificados)}) 
            AND valor IS NOT NULL
            AND valor > 0
        """
        cursor_leitura.execute(query_verificar_valores)
        resultado = cursor_leitura.fetchone()
        
        if resultado[0] > 0:
            tabelas_com_valores += 1
        
        # Verificação se já atingiu o limite de 3 tabelas com valores válidos
        if tabelas_com_valores >= 3:
            equipamentos_validados.append(cod_equipamento)
            break

print(f"Total de equipamentos válidos com valores em três ou mais tabelas: {len(equipamentos_validados)}")

#equipamentos_validados = [2173,2656,2657,2658,2659,2660,2661,2662,2663,2664]



def treinar_modelo_e_filtrar(X, y, model, outlier_threshold=2):

    model.fit(X, y)
    y_pred = model.predict(X)

    residuos = y - y_pred
    media_residuos = np.mean(residuos)
    desvio_padrao_residuos = np.std(residuos)
    limiar = outlier_threshold * desvio_padrao_residuos

    valores_normais = (abs(residuos) <= limiar)
    X_filtrado = X[valores_normais]
    y_filtrado = y[valores_normais]

    X2_filtrado = sm.add_constant(X_filtrado)
    est_filtrado = sm.OLS(y_filtrado, X2_filtrado)
    est2_filtrado = est_filtrado.fit()

    model.fit(X_filtrado, y_filtrado)
    y_pred_filtrado = model.predict(X_filtrado)
    r2_filtrado = r2_score(y_filtrado, y_pred_filtrado)
    intercepto = model.intercept_
    coeficiente = model.coef_[0]

    p_values = est2_filtrado.pvalues
    coeficiente_significativo = p_values.iloc[1] <= 0.05

    coeficiente = round(coeficiente, 4)
    r2_filtrado = round(r2_filtrado, 4)
    intercepto = round(intercepto, 4)

    #####################################

    # Plota o gráfico de regressão
    # plt.figure()
    # plt.scatter(X, y, color='blue', label='Dados Reais')
    # plt.plot(X, y_pred, color='red', linewidth=2, label='Linha de Regressão')
    # plt.xlabel('Pot Ativa')
    # plt.ylabel('Load Speed')
    # plt.title('Regressão Linear: Pot Ativa vs Load Speed')
    # plt.legend()
    # plt.savefig('regression_plot.png')
    # plt.close()

    # # Imprime o resumo do modelo ajustado
    # print(est2_filtrado.summary())

    # # Teste Shapiro-Wilk para normalidade dos resíduos
    # stat, p_value = stats.shapiro(residuos)
    # print(f'Teste Shapiro-Wilk: Estatística={stat}, p-valor={p_value}')

    # # Histograma dos Resíduos
    # plt.figure()
    # plt.hist(residuos, bins=30, edgecolor='k')
    # plt.xlabel('Resíduos')
    # plt.ylabel('Frequência')
    # plt.title('Histograma dos Resíduos')
    # plt.savefig('histograma_residuos.png')
    # plt.close()

    # # Q-Q Plot dos Resíduos
    # plt.figure()
    # stats.probplot(residuos, dist="norm", plot=plt)
    # plt.title('Q-Q Plot dos Resíduos')
    # plt.savefig('qq_plot_residuos.png')
    # plt.close()

    # # Teste Durbin-Watson
    # durbin_watson = sm.stats.durbin_watson(residuos)
    # print(f'Teste Durbin-Watson: {durbin_watson}')

    # # Correlograma dos Resíduos (ACF)
    # plt.figure()
    # plot_acf(residuos, lags=50)
    # plt.title('Correlograma dos Resíduos')
    # plt.savefig('acf_residuos.png')
    # plt.close()

    # # Plotagem de ACF e PACF para verificar autocorrelação
    # fig, ax = plt.subplots(1, 2, figsize=(15, 5))
    # sm.graphics.tsa.plot_acf(residuos, lags=40, ax=ax[0])
    # sm.graphics.tsa.plot_pacf(residuos, lags=40, ax=ax[1])
    # plt.savefig('autocorrelation.png')
    # plt.show()
    
    #####################################

    return coeficiente, intercepto, r2_filtrado, coeficiente_significativo


def verificar_e_obter_coeficiente(cod_equipamento, X, y):
    coeficiente_existente = 0.0
    intercepto_existente = 0.0
    acuracia_existente = 0.0

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS coeficiente_geradores (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cod_equipamento INT,
            coeficiente FLOAT,
            intercepto FLOAT,
            acuracia FLOAT
        )
    ''')

    cursor.execute(f"SELECT * FROM coeficiente_geradores WHERE cod_equipamento = {cod_equipamento} ORDER BY cod_equipamento ASC")
    resultado = cursor.fetchone()

    if resultado is not None:
        coeficiente_existente = resultado[2]
        intercepto_existente = resultado[3]
        acuracia_existente = resultado[4]
        print(f'Coeficiente Existente = {coeficiente_existente}, Intercepto Existente = {intercepto_existente}, Acuracia = {acuracia_existente}')

    alphas = [0.001, 0.01, 0.1, 1.0, 10.0, 100.0]
    outlier_thresholds = [0.05, 0.5, 1, 1.5, 2, 2.5, 3, 3.5,4]

    for alpha in alphas:
        for outlier_threshold in outlier_thresholds:
            for model in [Ridge(alpha=alpha), Lasso(alpha=alpha)]:
                while True:
                    coeficiente_novo, intercepto_novo, acuracia_nova, coeficiente_significativo = treinar_modelo_e_filtrar(X, y, model, outlier_threshold)
                    print(f'Novo Coeficiente = {coeficiente_novo}, Novo Intercepto = {intercepto_novo}, Acuracia = {acuracia_nova}, Alpha = {alpha}, Outlier Threshold = {outlier_threshold}, Modelo = {type(model).__name__}')
                    
                    if coeficiente_significativo:
                        coeficiente_existente = coeficiente_novo
                        intercepto_existente = intercepto_novo
                        acuracia_existente = acuracia_nova

                        if resultado is None:
                            cursor.execute('''
                                INSERT INTO coeficiente_geradores (cod_equipamento, coeficiente, intercepto, acuracia)
                                VALUES (%s, %s, %s, %s)
                            ''', (cod_equipamento, coeficiente_existente, intercepto_existente, acuracia_existente))
                        else:
                            cursor.execute('''
                                UPDATE coeficiente_geradores
                                SET coeficiente = %s, intercepto = %s, acuracia = %s
                                WHERE cod_equipamento = %s
                            ''', (coeficiente_existente, intercepto_existente, acuracia_existente, cod_equipamento))

                        cnx.commit()
                        return coeficiente_existente, intercepto_existente, acuracia_existente
                    else:
                        print(f"O coeficiente para o equipamento {cod_equipamento} não é significativo. Recalculando com novos parâmetros...")
                        break

    print(f"O coeficiente para o equipamento {cod_equipamento} não pôde ser tornado significativo.")
    return coeficiente_existente, intercepto_existente, acuracia_existente


#for cod_equipamento in cod_equipamentos_validos:
for cod_equipamento in equipamentos_com_valores:
#for cod_equipamento in equipamentos_validados:
    try:
        queries = []
        for tabela in tabelas:
            query = f"SELECT data_cadastro, valor, cod_campo FROM {tabela} WHERE cod_equipamento = {cod_equipamento} AND cod_campo IN ({', '.join(cod_campo_especificados)})"
            queries.append(query)

        final_query = " UNION ALL ".join(queries)
        cursor_leitura.execute(final_query)
        resultados = cursor_leitura.fetchall()

        df = pd.DataFrame(resultados, columns=['data_cadastro', 'valor', 'cod_campo'])
        df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])
        df['rounded_time'] = df['data_cadastro'].dt.round('5min')
        df = df[~df.index.duplicated(keep='first')]
        df['valor'] = df['valor'].replace({'-1-1': np.nan})
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        df['valor'] = df['valor'].fillna(0)
        df = df.groupby(['rounded_time', 'cod_campo']).agg({'valor': 'mean'}).reset_index()
        df = df.replace([np.inf, -np.inf], np.nan).dropna()
        df = df.interpolate()
        df_pivoted = df.pivot(index='rounded_time', columns='cod_campo', values='valor')
        df_pivoted.columns = [f'valor_cod_campo_{col}' for col in df_pivoted.columns]
        df_pivoted = df_pivoted.reset_index()
        df_pivoted = df_pivoted.dropna(subset=['valor_cod_campo_3', 'valor_cod_campo_114'])
        df_pivoted = df_pivoted.interpolate()
        z_scores = np.abs(stats.zscore(df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_114']]))


        print('\n----------------------------------------------------------------------------------------------------------------\n')
        print(f'equipamento: {cod_equipamento} \n')


        X = df_pivoted[['valor_cod_campo_3']].astype(float).values
        y = df_pivoted['valor_cod_campo_114'].astype(float)

        coeficiente = 0.0
        acuracia = 0.0 
        intercepto = 0.0
        
        coeficiente_existente, intercepto_existente, r2_filtrado_existente = verificar_e_obter_coeficiente(cod_equipamento, X, y)
        coeficiente += coeficiente_existente
        acuracia += r2_filtrado_existente
        intercepto += intercepto_existente

        sys.stdout.flush()

        # correlation = df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_114', 'valor_cod_campo_21']].corr()
        # print(correlation)
        
        # plt.scatter(df_pivoted['valor_cod_campo_3'], df_pivoted['valor_cod_campo_114'])
        # plt.xlabel('Pot Ativa (valor_cod_campo_3)')
        # plt.ylabel('Load Speed (valor_cod_campo_114)')
        # plt.title('Relação entre Pot Ativa e Load Speed')
        # plt.savefig('scatter_plot.png')
        # plt.close()




    except Exception as error:
        print('\n----------------------------------------------------------------------------------------------------------------\n')                            
        print(f"Erro ao processar equipamento {cod_equipamento}: {error}")
        continue  # Pular para o próximo equipamento em caso de erro



# def treinar_modelo_e_filtrar(X, y, cod_equipamento):
#     # Treinar um modelo de regressão linear
# #    reg = linear_model.Ridge(alpha=.5)
#     reg = Ridge(alpha=1.0)
# #    reg = LinearRegression()
#     reg.fit(X, y)

#     # Prever os valores com o modelo treinado
#     y_pred = reg.predict(X)

#     # Calcular os resíduos (diferenças entre os valores reais e previstos)
#     residuos = y - y_pred

#     # Calcular a média e o desvio padrão dos resíduos
#     media_residuos = np.mean(residuos)
#     desvio_padrao_residuos = np.std(residuos)

#     # Definir um limiar (por exemplo, 2 desvios padrão) para identificar valores atípicos
#     limiar = 2 * desvio_padrao_residuos

#     # Filtrar os valores atípicos
#     valores_normais = (abs(residuos) <= limiar)
#     X_filtrado = X[valores_normais]
#     y_filtrado = y[valores_normais]

#     # Adicionar uma coluna de constante a X
#     X2_filtrado = sm.add_constant(X_filtrado)

#     # Ajustar o modelo de regressão linear
#     est_filtrado = sm.OLS(y_filtrado, X2_filtrado)
#     est2_filtrado = est_filtrado.fit()
    
#     # Treinar um novo modelo com os valores filtrados
#     reg_filtrado = LinearRegression()
#     reg_filtrado.fit(X_filtrado, y_filtrado)

#     # Prever os valores com o modelo treinado (filtrado)
#     y_pred_filtrado = reg_filtrado.predict(X_filtrado)

#     # Calcular o coeficiente de determinação (R²) após a filtragem
#     r2_filtrado = r2_score(y_filtrado, y_pred_filtrado)
#     intercepto = reg_filtrado.intercept_
#     coeficiente = reg_filtrado.coef_[0]

#     # Verificar se a média dos resíduos é próxima de zero
#     if abs(media_residuos) > 0.01:
#         print(f"A média dos resíduos é {media_residuos}, indicando que o modelo pode ter um viés.")

#     # Verificar a significância dos coeficientes
#     p_values = est2_filtrado.pvalues
#     for i, p_value in enumerate(p_values):
#         if p_value > 0.05:
#             print(f"O coeficiente para a variável {i} não é significativo (p={p_value}).")

#     coeficiente = round(coeficiente, 4)
#     r2_filtrado = round(r2_filtrado, 4)
#     intercepto = round(intercepto, 4)

#     return coeficiente, intercepto, r2_filtrado


# def verificar_e_obter_coeficiente(cod_equipamento, X, y):
#     coeficiente_existente = 0.0
#     intercepto_existente = 0.0
#     acuracia_existente = 0.0

#     cursor.execute('''
#         CREATE TABLE IF NOT EXISTS coeficiente_geradores (
#             id INT AUTO_INCREMENT PRIMARY KEY,
#             cod_equipamento INT,
#             coeficiente FLOAT,
#             intercepto FLOAT,
#             acuracia FLOAT
#         )
#     ''')

#     # Verifica se o equipamento existe na tabela
#     cursor.execute(f"SELECT * FROM coeficiente_geradores WHERE cod_equipamento = {cod_equipamento} ORDER BY cod_equipamento ASC")
#     resultado = cursor.fetchone()

#     if resultado is not None:
#         coeficiente_existente = resultado[2]
#         intercepto_existente = resultado[3]
#         acuracia_existente = resultado[4]
#         print(f'Coeficiente Existente = {coeficiente_existente}, Intercepto Existente = {intercepto_existente}, Acuracia = {acuracia_existente}')

#     coeficiente_novo, intercepto_novo, acuracia_nova = treinar_modelo_e_filtrar(X, y, cod_equipamento)
#     print(f'Novo Coeficiente = {coeficiente_novo}, Novo Intercepto = {intercepto_novo}, Acuracia = {acuracia_nova}')
    
# #    if coeficiente_novo > 0 and (resultado is None or acuracia_nova >= acuracia_existente) and acuracia_nova >= 0:
#     if coeficiente_novo > 0 and acuracia_nova >= 0:
#         coeficiente_existente = coeficiente_novo
#         intercepto_existente = intercepto_novo
#         acuracia_existente = acuracia_nova

#         if resultado is None:
#             cursor.execute('''
#                 INSERT INTO coeficiente_geradores (cod_equipamento, coeficiente, intercepto, acuracia)
#                 VALUES (%s, %s, %s, %s)
#              ''', (cod_equipamento, coeficiente_existente, intercepto_existente, acuracia_existente))
#         else:
#             cursor.execute('''
#                 UPDATE coeficiente_geradores
#                 SET coeficiente = %s, intercepto = %s, acuracia = %s
#                 WHERE cod_equipamento = %s
#              ''', (coeficiente_existente, intercepto_existente, acuracia_existente, cod_equipamento))

#         cnx.commit()
        
        
#     return coeficiente_existente, intercepto_existente, acuracia_existente

       
            
# for cod_equipamento in cod_equipamentos_validos:
#     try:
#         queries = []
#         for tabela in tabelas:
#             query = f"SELECT data_cadastro, valor, cod_campo FROM {tabela} WHERE cod_equipamento = {cod_equipamento} AND cod_campo IN ({', '.join(cod_campo_especificados)})"
#             queries.append(query)

#         final_query = " UNION ALL ".join(queries)
#         cursor_leitura.execute(final_query)
#         resultados = cursor_leitura.fetchall()

#         # Converte os resultados em um DataFrame
#         df = pd.DataFrame(resultados, columns=['data_cadastro', 'valor', 'cod_campo'])

#         # Converter a coluna 'data_cadastro' para o tipo datetime
#         df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])
        
#         # Arredondar o tempo para o intervalo de 5 minutos
#         df['rounded_time'] = df['data_cadastro'].dt.round('5min')

#         # Remova duplicatas do índice
#         df = df[~df.index.duplicated(keep='first')]

#         # Substituir valores problemáticos antes de converter para float
#         df['valor'] = df['valor'].replace({'-1-1': np.nan})

#     # Converter a coluna 'valor' para o tipo float
#         df['valor'] = pd.to_numeric(df['valor'], errors='coerce')

#         # Substituir valores NaN por zero
#         df['valor'] = df['valor'].fillna(0)

#         # Agregar valores duplicados pela média
#         df = df.groupby(['rounded_time', 'cod_campo']).agg({'valor': 'mean'}).reset_index()

#         # Remover linhas com valores infinitos
#         df = df.replace([np.inf, -np.inf], np.nan).dropna()

#         # Preencher valores ausentes usando interpolação linear
#         df = df.interpolate()

#         # Pivotear o DataFrame para obter as colunas desejadas
#         df_pivoted = df.pivot(index='rounded_time', columns='cod_campo', values='valor')

#         # Renomear as colunas
#         df_pivoted.columns = [f'valor_cod_campo_{col}' for col in df_pivoted.columns]

#         # Resetar o índice
#         df_pivoted = df_pivoted.reset_index()

#         # Filtrar o DataFrame para excluir linhas com valores nulos em ambas as colunas
#         df_pivoted = df_pivoted.dropna(subset=['valor_cod_campo_3', 'valor_cod_campo_114'])
# #        df_pivoted = df_pivoted.dropna(subset=['valor_cod_campo_3', 'valor_cod_campo_114', 'valor_cod_campo_23'])

#         # Preencher valores ausentes usando interpolação linear
#         df_pivoted = df_pivoted.interpolate()

#         # Filtrar outliers usando z-score
#         z_scores = np.abs(stats.zscore(df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_114']]))
# #        z_scores = np.abs(stats.zscore(df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_114' , 'valor_cod_campo_23']]))


#         print('\n----------------------------------------------------------------------------------------------------------------\n')
#         print(f'equipamento: {cod_equipamento} \n')

#         # Converter os valores de X e y para float
#         X = df_pivoted['valor_cod_campo_3'].astype(float).values.reshape(-1, 1)  # Recursos de treinamento
# #        X = df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_23']].astype(float).values  # Recursos de treinamento
#         y = df_pivoted['valor_cod_campo_114'].astype(float)  # Alvo de previsão

#         coeficiente = 0.0
#         acuracia = 0.0 
#         intercepto = 0.0
        
#         # Verificar e obter o coeficiente
#         coeficiente_existente, intercepto_existente, r2_filtrado_existente = verificar_e_obter_coeficiente(cod_equipamento, X, y)
#         coeficiente += coeficiente_existente
#         acuracia += r2_filtrado_existente
#         intercepto += intercepto_existente

#         sys.stdout.flush()

#     except Exception as error:
#         print('\n----------------------------------------------------------------------------------------------------------------\n')                            
#         print(f"Erro ao processar equipamento {cod_equipamento}: {error}")
#         continue  # Pular para o próximo equipamento em caso de erro

cursor.close()
cursor_leitura.close()
cnx.close()
cnx_leitura.close()

tempo_final = datetime.now()
total = tempo_final - tempo_inicial
print('\ntempo total de processamento',total)

