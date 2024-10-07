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

cod_campo_especificados = ['3', '114','20','21','25','76']

# Definição de cod_campo_especificados como lista de listas
cod_campo_previsao = [
    ['3', '114'],
    ['20', '21'],
    ['3', '25'],
    ['76', '25']

    # ['3', '20'],
    # ['20', '3'],

    # ['3', '21'],
    # ['21', '3'],

    # ['3', '25'],
    # ['25', '3'],

    # ['3', '76'],
    # ['76', '3'],

    # ['3', '114'],
    # ['114', '3'],

    # ['20', '21'],
    # ['21', '20'],

    # ['20', '25'],
    # ['25', '20'],

    # ['20', '76'],
    # ['76', '20'],

    # ['20', '114'],
    # ['114', '20'],

    # ['25', '21'],
    # ['21', '25'],

    # ['25', '76'],
    # ['76', '25'],

    # ['25', '114'],
    # ['114', '25'],

    # ['76', '21'],
    # ['21', '76'],

    # ['76', '25'],
    # ['25', '76'],

    # ['76', '114'],
    # ['114', '76'],

    # ['114', '21'],
    # ['21', '114'],
]


# Emparelhamento dos valores de X e Y
cod_campo_pairs = cod_campo_previsao  # Agora, cod_campo_pairs já contém os pares na estrutura correta


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


# # Importante para verificar valores válidos em várias tabelas
# equipamentos_validados = []

# for cod_equipamento in cod_equipamentos_validos:
#     tabelas_com_valores = 0
    
#     for tabela in tabelas:
#         query_verificar_valores = f"""
#             SELECT COUNT(*) FROM {tabela} 
#             WHERE cod_equipamento = {cod_equipamento} 
#             AND cod_campo IN ({', '.join(cod_campo_especificados)}) 
#             AND valor IS NOT NULL
#             AND valor > 0
#         """
#         cursor_leitura.execute(query_verificar_valores)
#         resultado = cursor_leitura.fetchone()
        
#         if resultado[0] > 0:
#             tabelas_com_valores += 1
        
#         # Verificação se já atingiu o limite de 3 tabelas com valores válidos
#         if tabelas_com_valores >= 3:
#             equipamentos_validados.append(cod_equipamento)
#             break

# print(f"Total de equipamentos válidos com valores em três ou mais tabelas: {len(equipamentos_validados)}")

#equipamentos_validados = [2808,2235,2363]



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
    coeficiente_significativo = p_values[1] <= 0.05

    coeficiente = round(coeficiente, 4)
    r2_filtrado = round(r2_filtrado, 4)
    intercepto = round(intercepto, 4)

    #####################################

    # #Plota o gráfico de regressão
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


def verificar_e_obter_coeficiente(cod_equipamento, X, y, x_cod_campo, y_cod_campo):
    coeficiente_existente = 0.0
    intercepto_existente = 0.0
    acuracia_existente = 0.0

    # Verifique se a tabela já contém as colunas para X e Y, senão, crie-as
    cursor.execute(''' 
        CREATE TABLE IF NOT EXISTS novos_coeficiente_geradores (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cod_equipamento INT,
            X INT,
            Y INT,
            coeficiente FLOAT,
            intercepto FLOAT,
            acuracia FLOAT
        )
    ''')

    # Buscar coeficientes existentes para este equipamento e esses X e Y
    cursor.execute(f'''
        SELECT coeficiente, intercepto, acuracia FROM novos_coeficiente_geradores 
        WHERE cod_equipamento = {cod_equipamento} 
        AND X = {x_cod_campo} 
        AND Y = {y_cod_campo}
    ''')
    resultado = cursor.fetchone()
    print('resultado',resultado)
    
    if resultado is not None:
        coeficiente_existente = resultado[0]
        intercepto_existente = resultado[1]
        acuracia_existente = resultado[2]
        print(f'Coeficiente Existente = {coeficiente_existente}, Intercepto Existente = {intercepto_existente}, Acuracia = {acuracia_existente}')
    
    # Iniciar o valor dos novos coeficientes como None para facilitar a verificação
    coeficiente_novo = None
    intercepto_novo = None
    acuracia_nova = None

    alphas = [0.001, 0.01, 0.1, 1.0, 10.0, 100.0]
    outlier_thresholds = [0.05, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4]

    for alpha in alphas:
        for outlier_threshold in outlier_thresholds:
            for model in [Ridge(alpha=alpha), Lasso(alpha=alpha)]:
                while True:
                    coeficiente_novo, intercepto_novo, acuracia_nova, coeficiente_significativo = treinar_modelo_e_filtrar(X, y, model, outlier_threshold)

                    print(f'{cod_equipamento}\nNovo Coeficiente = {coeficiente_novo}, Novo Intercepto = {intercepto_novo}, Acuracia = {acuracia_nova}, Alpha = {alpha}, Outlier Threshold = {outlier_threshold}, Modelo = {type(model).__name__}')

                    if acuracia_nova < 0.70:
                        print(f'Nova acuracia menor que 70%: {"Sim" if acuracia_nova else "Não"}')
                        break      

                    print(f'Coeficiente Significativo: {"Sim" if coeficiente_significativo else "Não"}')

                    if coeficiente_significativo:
                        # Atualize apenas se os valores novos forem significativos
                        coeficiente_existente = coeficiente_novo
                        intercepto_existente = intercepto_novo
                        acuracia_existente = acuracia_nova

                        # Insere ou atualiza a tabela com os valores calculados
                        if resultado is None:
                            cursor.execute('''
                                INSERT INTO novos_coeficiente_geradores (cod_equipamento, X, Y, coeficiente, intercepto, acuracia)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            ''', (cod_equipamento, x_cod_campo, y_cod_campo, coeficiente_existente, intercepto_existente, acuracia_existente))
                        else:
                            cursor.execute('''
                                UPDATE novos_coeficiente_geradores
                                SET coeficiente = %s, intercepto = %s, acuracia = %s
                                WHERE cod_equipamento = %s AND X = %s AND Y = %s
                            ''', (coeficiente_existente, intercepto_existente, acuracia_existente, cod_equipamento, x_cod_campo, y_cod_campo))

                        cnx.commit()
                        return coeficiente_existente, intercepto_existente, acuracia_existente
                    else:
                        print(f"O coeficiente para o equipamento {cod_equipamento} não é significativo. Recalculando com novos parâmetros...")
                        break

    print(f"O coeficiente para o equipamento {cod_equipamento} não pôde ser tornado significativo.")
    return coeficiente_existente, intercepto_existente, acuracia_existente





# Processamento dos dados
for cod_equipamento in cod_equipamentos_validos:
#for cod_equipamento in equipamentos_com_valores:
#for cod_equipamento in equipamentos_validados:
    try:
        queries = []
        # Obter todos os valores de cod_campo distintos presentes na lista cod_campo_especificados
        todos_cod_campo = list(set([campo for par in cod_campo_pairs for campo in par]))

        for tabela in tabelas:
            # Construa a consulta para todos os valores de cod_campo
            query = f"SELECT data_cadastro, valor, cod_campo FROM {tabela} WHERE cod_equipamento = {cod_equipamento} AND cod_campo IN ({', '.join(todos_cod_campo)})"
            queries.append(query)

        final_query = " UNION ALL ".join(queries)
        cursor_leitura.execute(final_query)
        resultados = cursor_leitura.fetchall()

        # Criação do DataFrame a partir dos resultados da consulta
        df = pd.DataFrame(resultados, columns=['data_cadastro', 'valor', 'cod_campo'])
        df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])
        df['rounded_time'] = df['data_cadastro'].dt.round('5min')
        df = df[~df.index.duplicated(keep='first')]
        df['valor'] = df['valor'].replace({'-1-1': np.nan})
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        df['valor'] = df['valor'].fillna(0)
        
        # Agrupar por rounded_time e cod_campo, e calcular o valor médio
        df = df.groupby(['rounded_time', 'cod_campo']).agg({'valor': 'mean'}).reset_index()
        df = df.replace([np.inf, -np.inf], np.nan).dropna()
        df = df.interpolate()

        # Pivotar o DataFrame
        df_pivoted = df.pivot(index='rounded_time', columns='cod_campo', values='valor')
        df_pivoted.columns = [f'valor_cod_campo_{col}' for col in df_pivoted.columns]
        df_pivoted = df_pivoted.reset_index()

        # Processar cada par de X e Y
        for x, y in cod_campo_pairs:
            # Filtrar linhas onde ambos os valores de x e y estejam presentes
            df_filtered = df_pivoted.dropna(subset=[f'valor_cod_campo_{x}', f'valor_cod_campo_{y}'])

            if df_filtered.empty:
                print(f'Sem dados suficientes para {x} e {y} no equipamento {cod_equipamento}')
                continue

            # Interpolar valores faltantes
            df_filtered = df_filtered.interpolate()

            print('\n----------------------------------------------------------------------------------------------------------------\n')
            print(f'equipamento: {cod_equipamento}, X: {x}, Y: {y}')

            # Definir X e y para treinamento
            X = df_filtered[[f'valor_cod_campo_{x}']].astype(float).values
            y_values = df_filtered[f'valor_cod_campo_{y}'].astype(float).values

            # Chamar a função de treinamento com os dados filtrados
        #    model = LinearRegression()
        #    coeficiente, intercepto, r2_filtrado, coeficiente_significativo = treinar_modelo_e_filtrar(X, y_values, model)
            
            # Usar x e y como identificadores corretos para cod_campo
            coeficiente_existente, intercepto_existente, r2_filtrado_existente = verificar_e_obter_coeficiente(
                cod_equipamento, X, y_values, x_cod_campo=x, y_cod_campo=y
            )

        #    print('\n----------------------------------------------------------------------------------------------------------------\n')
        #    print(f'equipamento: {cod_equipamento}, X: {x}, Y: {y}')
        #    print(f'Coeficiente: {coeficiente}, Intercepto: {intercepto}, Acurácia (R²): {r2_filtrado}')
        #    print(f'Coeficiente Significativo: {"Sim" if coeficiente_significativo else "Não"}')
            sys.stdout.flush()

    except Exception as error:
        print('\n----------------------------------------------------------------------------------------------------------------\n')
        print(f"Erro ao processar equipamento {cod_equipamento}: {error}")
        continue
    
    
    
    

cursor.close()
cursor_leitura.close()
cnx.close()
cnx_leitura.close()

tempo_final = datetime.now()
total = tempo_final - tempo_inicial
print('\ntempo total de processamento',total)

