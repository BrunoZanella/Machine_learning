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

cod_campo_especificados = ['3', '23', '114']

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
        df_pivoted = df_pivoted.dropna(subset=['valor_cod_campo_3', 'valor_cod_campo_23', 'valor_cod_campo_114'])

        # Preencher valores ausentes usando interpolação linear
        df_pivoted = df_pivoted.interpolate()

        # Filtrar outliers usando z-score
    #    z_scores = np.abs(stats.zscore(df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_114']]))
    #    outliers_mask = (z_scores < 3).all(axis=1)
    #    df_pivoted = df_pivoted[outliers_mask]
        
        # Filtrar outliers usando z-score
        z_scores = np.abs(stats.zscore(df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_23', 'valor_cod_campo_114']]))
        
        # Modificar a condição para manter os valores abaixo do limiar
        outliers_mask = (z_scores < 2).all(axis=1) # ou 3
        
        # Alterar para ~outliers_mask para manter os valores abaixo do limiar
        df_pivoted = df_pivoted[~outliers_mask]


        print('\n----------------------------------------------------------------------------------------------------------------\n')
        print(f'equipamento: {cod_equipamento} \n')

        # Verificar se pelo menos 10% dos valores em cod_ultima_tabela são zero
        percentual_zeros = df['valor'].eq('0').sum() / len(df['valor'])
        limite_percentual_zeros = 0.1

        if percentual_zeros >= limite_percentual_zeros:
            print(f"Equipamento {cod_equipamento}: Mais de {limite_percentual_zeros * 100}% dos valores são zero. Descartando...")
            continue  # Pular para o próximo equipamento se a condição for atendida

        # Converter os valores de X e y para float
        X = df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_23']].astype(float).values  # Recursos de treinamento

    #    X = df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_23']].astype(float).values.reshape(-1, 1)  # Recursos de treinamento
        y = df_pivoted['valor_cod_campo_114'].astype(float)  # Alvo de previsão
        
        print(X.shape[0])
        # Verificar se o conjunto de dados não está vazio
        if X.shape[0] == 0:
            print(f"Equipamento {cod_equipamento}: Conjunto de dados vazio. Ignorando o treinamento.")
            continue
        
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

