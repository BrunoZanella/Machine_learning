import mysql.connector
import pandas as pd
#from sqlalchemy import create_engine, MetaData, inspect
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
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
cursor = cnx.cursor(buffered=True)

def selecionar_GMG():
    cursor.execute("SELECT codigo, ativo FROM tipos_equipamentos WHERE classe = 'GMG'")
    resultados = cursor.fetchall()
    codigos = []

    for resultado in resultados:
        codigos.append(resultado[0])
    
    return codigos


numero_tabelas = 10

# Calcular a quantidade de tabelas para treino (60%)
percent_treino = 0.4
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

#tabelas_lista = tabelas_lista[-9:]

# Seleciona 60% das tabelas para treino e 40% para teste
tabelas_treino = tabelas_lista[:quantidade_tabelas_treino]
tabelas_teste = tabelas_lista[quantidade_tabelas_treino:]

tabelas = tabelas_treino

ultima_tabela = resultados_tabelas[0][0] if resultados_tabelas else None

# Obter a lista de códigos GMG
codigos_GMG = selecionar_GMG()

# Converter a lista de códigos em uma string formatada para a consulta SQL
codigos_GMG_str = ', '.join(map(str, codigos_GMG))

# Obter os códigos dos equipamentos ativos que são GMG
query_equipamentos = f"SELECT DISTINCT codigo FROM equipamentos WHERE cod_tipo_equipamento IN ({codigos_GMG_str}) AND ativo = 1"
cursor.execute(query_equipamentos)
resultados_equipamentos = cursor.fetchall()
cod_equipamentos = [str(resultado[0]) for resultado in resultados_equipamentos]

# Obter os códigos dos equipamentos na 'ultima_tabela'
query_ultima_tabela = f"SELECT DISTINCT cod_equipamento FROM {ultima_tabela}"
cursor.execute(query_ultima_tabela)
resultados_ultima_tabela = cursor.fetchall()
cod_ultima_tabela = [str(resultado[0]) for resultado in resultados_ultima_tabela]

# Comparar as duas listas de códigos
cod_equipamentos_validos = list(set(cod_equipamentos) & set(cod_ultima_tabela))

# Converter os resultados em uma lista
cod_equipamentos = cod_ultima_tabela

# Calcular o total de equipamentos encontrados
total_equipamentos = len(cod_equipamentos_validos)

# Converter os elementos da lista para inteiros e ordenar a lista
cod_equipamentos_validos = sorted([int(cod) for cod in cod_equipamentos_validos])

cod_campo_especificados = ['3', '114']

print('\ncod_equipamentos',cod_equipamentos)
print('\ncod_equipamentos_validos',cod_equipamentos_validos)
print('\ntabelas',tabelas)
print('\ntabelas_treino',tabelas_treino)
print('\ntabelas_teste',tabelas_teste)
print('\nultima_tabela',ultima_tabela)
print('\n')
print(f"Total de {total_equipamentos} equipamentos na tabela {ultima_tabela}")

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
    intercepto = reg_filtrado.intercept_
    coeficiente = reg_filtrado.coef_[0]

    # Verificar se a média dos resíduos é próxima de zero
    if abs(media_residuos) > 0.01:
        print(f"A média dos resíduos é {media_residuos}, indicando que o modelo pode ter um viés.")

    # Verificar a significância dos coeficientes
    p_values = est2_filtrado.pvalues
    for i, p_value in enumerate(p_values):
        if p_value > 0.05:
            print(f"O coeficiente para a variável {i} não é significativo (p={p_value}).")

    coeficiente = round(coeficiente, 4)
    r2_filtrado = round(r2_filtrado, 4)
    intercepto = round(intercepto, 4)

    return coeficiente, intercepto, r2_filtrado

'''
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
    intercepto = reg_filtrado.intercept_
    coeficiente = reg_filtrado.coef_[0]
    
    coeficiente = round(coeficiente, 4)
    r2_filtrado = round(r2_filtrado, 4)
    intercepto = round(intercepto, 4)
        
    return coeficiente, intercepto, r2_filtrado

'''


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
                
    return coeficiente, intercepto, r2_filtrado


'''


'''
def treinar_modelo_e_filtrar(X, y, cod_equipamento):
    # Definir os possíveis valores de alpha para a validação cruzada
    alphas = np.logspace(-4, 4, 9)

    # Treinar um modelo de regressão Ridge com validação cruzada
    reg = RidgeCV(alphas=alphas, store_cv_values=True)
    reg.fit(X, y)

    # Treinar um modelo SVM
    svm = SVR(kernel='linear')
    svm.fit(X, y)

    # Prever os valores com o modelo treinado
    y_pred_reg = reg.predict(X)
    y_pred_svm = svm.predict(X)

    # Calcular os resíduos (diferenças entre os valores reais e previstos)
    residuos_reg = y - y_pred_reg
    residuos_svm = y - y_pred_svm

    # Calcular a média e o desvio padrão dos resíduos
    media_residuos_reg = np.mean(residuos_reg)
    desvio_padrao_residuos_reg = np.std(residuos_reg)

    media_residuos_svm = np.mean(residuos_svm)
    desvio_padrao_residuos_svm = np.std(residuos_svm)

    # Definir um limiar (por exemplo, 2 desvios padrão) para identificar valores atípicos
    limiar_reg = 2 * desvio_padrao_residuos_reg
    limiar_svm = 2 * desvio_padrao_residuos_svm

    # Filtrar os valores atípicos
    valores_normais_reg = (abs(residuos_reg) <= limiar_reg)
    X_filtrado_reg = X[valores_normais_reg]
    y_filtrado_reg = y[valores_normais_reg]

    valores_normais_svm = (abs(residuos_svm) <= limiar_svm)
    X_filtrado_svm = X[valores_normais_svm]
    y_filtrado_svm = y[valores_normais_svm]

    # Treinar um novo modelo com os valores filtrados
    reg_filtrado = RidgeCV(alphas=alphas, store_cv_values=True)
    reg_filtrado.fit(X_filtrado_reg, y_filtrado_reg)

    svm_filtrado = SVR(kernel='linear')
    svm_filtrado.fit(X_filtrado_svm, y_filtrado_svm)

    # Prever os valores com o modelo treinado (filtrado)
    y_pred_filtrado_reg = reg_filtrado.predict(X_filtrado_reg)
    y_pred_filtrado_svm = svm_filtrado.predict(X_filtrado_svm)

    # Calcular o coeficiente de determinação (R²) após a filtragem
    r2_filtrado_reg = r2_score(y_filtrado_reg, y_pred_filtrado_reg)
    r2_filtrado_svm = r2_score(y_filtrado_svm, y_pred_filtrado_svm)

    # Calcular o erro quadrático médio (MSE) após a filtragem
    mse_filtrado_reg = mean_squared_error(y_filtrado_reg, y_pred_filtrado_reg)
    mse_filtrado_svm = mean_squared_error(y_filtrado_svm, y_pred_filtrado_svm)

    print('R2 regressao',r2_filtrado_reg, 'R2 SVM',r2_filtrado_svm,'MSE regressao',mse_filtrado_reg,'MSE SVM',mse_filtrado_svm)
    # Escolher o melhor modelo com base no R² e no MSE
    if r2_filtrado_reg > r2_filtrado_svm and mse_filtrado_reg < mse_filtrado_svm:
        melhor_modelo = 'Regressao linear'
        coeficiente = round(reg_filtrado.coef_[0], 4)
        intercepto = round(reg_filtrado.intercept_, 4)
        r2_filtrado = round(r2_filtrado_reg, 4)
    else:
        melhor_modelo = 'SVM'
        coeficiente = round(svm_filtrado.coef_[0][0], 4)  # Acessar o primeiro elemento do array
        intercepto = round(svm_filtrado.intercept_[0], 4)  # Acessar o primeiro elemento do array
        r2_filtrado = round(r2_filtrado_svm, 4)

    print(melhor_modelo)
    return coeficiente, intercepto, r2_filtrado


'''

'''

def treinar_modelo_e_filtrar(X, y, cod_equipamento):
    # Definir os possíveis valores de alpha para a validação cruzada
    alphas = np.logspace(-4, 4, 9)

    # Treinar um modelo de regressão Ridge com validação cruzada
    reg = RidgeCV(alphas=alphas, store_cv_values=True)
    reg.fit(X, y)

    # Treinar um modelo SVM
    svm = SVR(kernel='linear')
    svm.fit(X, y)

    # Treinar um modelo Lasso
    lasso = LassoCV(alphas=alphas, max_iter=10000)
    lasso.fit(X, y)

    # Treinar um modelo ElasticNet
    elastic = ElasticNetCV(alphas=alphas, max_iter=10000)
    elastic.fit(X, y)

    # Prever os valores com o modelo treinado
    y_pred_reg = reg.predict(X)
    y_pred_svm = svm.predict(X)
    y_pred_lasso = lasso.predict(X)
    y_pred_elastic = elastic.predict(X)

    # Calcular os resíduos (diferenças entre os valores reais e previstos)
    residuos_reg = y - y_pred_reg
    residuos_svm = y - y_pred_svm
    residuos_lasso = y - y_pred_lasso
    residuos_elastic = y - y_pred_elastic

    # Calcular a média e o desvio padrão dos resíduos
    media_residuos_reg = np.mean(residuos_reg)
    desvio_padrao_residuos_reg = np.std(residuos_reg)

    media_residuos_svm = np.mean(residuos_svm)
    desvio_padrao_residuos_svm = np.std(residuos_svm)

    media_residuos_lasso = np.mean(residuos_lasso)
    desvio_padrao_residuos_lasso = np.std(residuos_lasso)

    media_residuos_elastic = np.mean(residuos_elastic)
    desvio_padrao_residuos_elastic = np.std(residuos_elastic)

    # Definir um limiar (por exemplo, 2 desvios padrão) para identificar valores atípicos
    limiar_reg = 2 * desvio_padrao_residuos_reg
    limiar_svm = 2 * desvio_padrao_residuos_svm
    limiar_lasso = 2 * desvio_padrao_residuos_lasso
    limiar_elastic = 2 * desvio_padrao_residuos_elastic

    # Filtrar os valores atípicos
    valores_normais_reg = (abs(residuos_reg) <= limiar_reg)
    X_filtrado_reg = X[valores_normais_reg]
    y_filtrado_reg = y[valores_normais_reg]

    valores_normais_svm = (abs(residuos_svm) <= limiar_svm)
    X_filtrado_svm = X[valores_normais_svm]
    y_filtrado_svm = y[valores_normais_svm]

    valores_normais_lasso = (abs(residuos_lasso) <= limiar_lasso)
    X_filtrado_lasso = X[valores_normais_lasso]
    y_filtrado_lasso = y[valores_normais_lasso]

    valores_normais_elastic = (abs(residuos_elastic) <= limiar_elastic)
    X_filtrado_elastic = X[valores_normais_elastic]
    y_filtrado_elastic = y[valores_normais_elastic]

    # Treinar um novo modelo com os valores filtrados
    reg_filtrado = RidgeCV(alphas=alphas, store_cv_values=True)
    reg_filtrado.fit(X_filtrado_reg, y_filtrado_reg)

    svm_filtrado = SVR(kernel='linear')
    svm_filtrado.fit(X_filtrado_svm, y_filtrado_svm)

    lasso_filtrado = LassoCV(alphas=alphas)
    lasso_filtrado.fit(X_filtrado_lasso, y_filtrado_lasso)

    elastic_filtrado = ElasticNetCV(alphas=alphas)
    elastic_filtrado.fit(X_filtrado_elastic, y_filtrado_elastic)

    # Prever os valores com o modelo treinado (filtrado)
    y_pred_filtrado_reg = reg_filtrado.predict(X_filtrado_reg)
    y_pred_filtrado_svm = svm_filtrado.predict(X_filtrado_svm)
    y_pred_filtrado_lasso = lasso_filtrado.predict(X_filtrado_lasso)
    y_pred_filtrado_elastic = elastic_filtrado.predict(X_filtrado_elastic)

    # Calcular o coeficiente de determinação (R²) após a filtragem
    r2_filtrado_reg = r2_score(y_filtrado_reg, y_pred_filtrado_reg)
    r2_filtrado_svm = r2_score(y_filtrado_svm, y_pred_filtrado_svm)
    r2_filtrado_lasso = r2_score(y_filtrado_lasso, y_pred_filtrado_lasso)
    r2_filtrado_elastic = r2_score(y_filtrado_elastic, y_pred_filtrado_elastic)

    # Calcular o erro quadrático médio (MSE) após a filtragem
    mse_filtrado_reg = mean_squared_error(y_filtrado_reg, y_pred_filtrado_reg)
    mse_filtrado_svm = mean_squared_error(y_filtrado_svm, y_pred_filtrado_svm)
    mse_filtrado_lasso = mean_squared_error(y_filtrado_lasso, y_pred_filtrado_lasso)
    mse_filtrado_elastic = mean_squared_error(y_filtrado_elastic, y_pred_filtrado_elastic)

    print('R2 regressao',r2_filtrado_reg, 'R2 SVM',r2_filtrado_svm,'R2 ElasticNet',r2_filtrado_elastic, '--', 'MSE regressao',mse_filtrado_reg,'MSE SVM',mse_filtrado_svm,'MRE ElasticNet',mse_filtrado_elastic)

    # Escolher o melhor modelo com base no R² e no MSE
    modelos = ['Regressão Ridge', 'SVM', 'Lasso', 'ElasticNet']
    r2_scores = [r2_filtrado_reg, r2_filtrado_svm, r2_filtrado_lasso, r2_filtrado_elastic]
    mses = [mse_filtrado_reg, mse_filtrado_svm, mse_filtrado_lasso, mse_filtrado_elastic]
    coeficientes = [reg_filtrado.coef_[0], svm_filtrado.coef_[0][0], lasso_filtrado.coef_[0], elastic_filtrado.coef_[0]]
    interceptos = [reg_filtrado.intercept_, svm_filtrado.intercept_[0], lasso_filtrado.intercept_, elastic_filtrado.intercept_]

    melhor_modelo_idx = np.argmax(r2_scores)  # índice do modelo com o maior R²
    melhor_modelo = modelos[melhor_modelo_idx]
    coeficiente = round(coeficientes[melhor_modelo_idx], 4)
    intercepto = round(interceptos[melhor_modelo_idx], 4)
    r2_filtrado = round(r2_scores[melhor_modelo_idx], 4)

    print(melhor_modelo)
    return coeficiente, intercepto, r2_filtrado

'''

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

    # Verifica se o equipamento existe na tabela
    cursor.execute(f"SELECT * FROM coeficiente_geradores WHERE cod_equipamento = {cod_equipamento} ORDER BY cod_equipamento ASC")
    resultado = cursor.fetchone()

    if resultado is not None:
        coeficiente_existente = resultado[2]
        intercepto_existente = resultado[3]
        acuracia_existente = resultado[4]
        print(f'Coeficiente Existente = {coeficiente_existente}, Intercepto Existente = {intercepto_existente}, Acuracia = {acuracia_existente}')

    coeficiente_novo, intercepto_novo, acuracia_nova = treinar_modelo_e_filtrar(X, y, cod_equipamento)
    print(f'Novo Coeficiente = {coeficiente_novo}, Novo Intercepto = {intercepto_novo}, Acuracia = {acuracia_nova}')
    
#    if coeficiente_novo > 0 and (resultado is None or acuracia_nova >= acuracia_existente) and acuracia_nova >= 0:
    if coeficiente_novo > 0 and acuracia_nova >= 0:
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

       
            
for cod_equipamento in cod_equipamentos_validos:
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
    #    z_scores = np.abs(stats.zscore(df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_114']]))
    #    outliers_mask = (z_scores < 3).all(axis=1)
    #    df_pivoted = df_pivoted[outliers_mask]

        # Filtrar outliers usando z-score
        z_scores = np.abs(stats.zscore(df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_114']]))

        # Modificar a condição para manter os valores abaixo do limiar
#        outliers_mask = (z_scores < 2).all(axis=1) # ou 3 voltar se a acuacia estiver ruim

        # Alterar para ~outliers_mask para manter os valores abaixo do limiar
#        df_pivoted = df_pivoted[~outliers_mask] # voltar se a acuacia estiver ruim


        print('\n----------------------------------------------------------------------------------------------------------------\n')
        print(f'equipamento: {cod_equipamento} \n')

        # Converter os valores de X e y para float
        X = df_pivoted['valor_cod_campo_3'].astype(float).values.reshape(-1, 1)  # Recursos de treinamento
        y = df_pivoted['valor_cod_campo_114'].astype(float)  # Alvo de previsão

        coeficiente = 0.0
        acuracia = 0.0
        intercepto = 0.0
        
        # Verificar e obter o coeficiente
        coeficiente_existente, intercepto_existente, r2_filtrado_existente = verificar_e_obter_coeficiente(cod_equipamento, X, y)
        coeficiente += coeficiente_existente
        acuracia += r2_filtrado_existente
        intercepto += intercepto_existente


    except Exception as error:
        print('\n----------------------------------------------------------------------------------------------------------------\n')                            
        print(f"Erro ao processar equipamento {cod_equipamento}: {error}")
        continue  # Pular para o próximo equipamento em caso de erro

cursor.close()
cnx.close()

tempo_final = datetime.now()
total = tempo_final - tempo_inicial
print('\ntempo total de processamento',total)

