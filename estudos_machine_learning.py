import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from sklearn.model_selection import train_test_split
import warnings
from scipy.stats import kurtosis
import statsmodels.stats.api as sms
from statsmodels.compat import lzip
from statsmodels.graphics.gofplots import qqplot
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.stats.diagnostic import het_breuschpagan, het_goldfeldquandt,het_white
from statsmodels.stats.diagnostic import linear_harvey_collier, linear_reset, spec_white
from statsmodels.stats.diagnostic import linear_rainbow
from statsmodels.graphics.regressionplots import plot_leverage_resid2
from yellowbrick.regressor import CooksDistance
from statsmodels.stats.outliers_influence import OLSInfluence, variance_inflation_factor
from sklearn.linear_model import LinearRegression, ElasticNet, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
import mysql.connector
import pandas as pd
#from sqlalchemy import create_engine, MetaData, inspect
import matplotlib.pyplot as plt
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
from sklearn.preprocessing import PolynomialFeatures
from mpl_toolkits.mplot3d import Axes3D
from matplotlib import cm

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
import matplotlib
import pingouin as pg


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

cod_campo_especificados = ['3', '114', '21']

print('\ntabelas',tabelas)
print('\nultima_tabela',ultima_tabela)
print('\n')
print(f"Total de {total_equipamentos} equipamentos na tabela {ultima_tabela}")

equipamentos_validados = [574,2173]




# Restante do código permanece igual
for cod_equipamento in equipamentos_validados:
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
        df_pivoted = df_pivoted.dropna(subset=['valor_cod_campo_3', 'valor_cod_campo_114', 'valor_cod_campo_21'])

        # # Removendo linhas com valores zero nas colunas específicas
        # df_pivoted = df_pivoted[(df_pivoted['valor_cod_campo_3'] != 0) &
        #                         (df_pivoted['valor_cod_campo_114'] != 0) &
        #                         (df_pivoted['valor_cod_campo_21'] != 0)]

        df_pivoted = df_pivoted.interpolate()

        # Renomear colunas para facilitar a identificação nos gráficos
        # df_pivoted = df_pivoted.rename(columns={
        #     'valor_cod_campo_21': 'Pressão do Óleo',
        #     'valor_cod_campo_114': 'Load Speed',
        #     'valor_cod_campo_3': 'Potência Ativa'
        # })
        
        #######################################################################################################################################

        # Definindo as variáveis independentes (X) e dependente (y)
        # X: variáveis independentes - load speed e pressão do óleo
        # y: variável dependente - potência ativa
        
        # Criando modelo com a Scikit Learn
        X = df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_21']]  # Variáveis preditoras
        y = df_pivoted['valor_cod_campo_114']  # Variável dependente


        # Função para treinar e testar vários modelos
        def train_models(X, y):
            models = {
                "Linear Regression": LinearRegression(),
                "Ridge Regression": Ridge(),
                "Lasso Regression": Lasso(),
                "ElasticNet Regression": ElasticNet(),
                "Random Forest Regressor": RandomForestRegressor(),
                "Gradient Boosting Regressor": GradientBoostingRegressor(),
                "Support Vector Regressor (SVR)": SVR()
            }
            
            for name, model in models.items():
                model.fit(X, y)
                score = model.score(X, y)
                print(f'\n{name}:')
                print(f'  Score: {score}')
                print(f'  Intercept: {model.intercept_ if hasattr(model, "intercept_") else "N/A"}')
                print(f'  Coefficients: {model.coef_ if hasattr(model, "coef_") else "N/A"}')

        train_models(X, y)

        #######################################################################################################################################

        modelo = LinearRegression()
        modelo.fit(X,y)
        print('\nequipamento',cod_equipamento)
        print('modelo.score(X,y)',modelo.score(X,y))
        print('modelo.intercept_',modelo.intercept_)
        print('modelo.coef_',modelo.coef_)

        # Verificação se o score é menor que 0.6
        if modelo.score(X, y) <= 0.6: 
            # Removendo linhas com valores zero nas colunas específicas
            df_pivoted = df_pivoted[(df_pivoted['valor_cod_campo_3'] != 0) &
                                    (df_pivoted['valor_cod_campo_114'] != 0) &
                                    (df_pivoted['valor_cod_campo_21'] != 0)]

            X = df_pivoted[['valor_cod_campo_114', 'valor_cod_campo_21']]
            y = df_pivoted['valor_cod_campo_3']
            
            modelo = LinearRegression()
            modelo.fit(X,y)
            
            print('novo modelo.score(X,y)',modelo.score(X,y))
            print('novo modelo.intercept_',modelo.intercept_)
            print('novo modelo.coef_',modelo.coef_)

        #######################################################################################################################################

        # print(df_pivoted.head())

        # # Gerando os modelos com a Pingouin
        # Xp = X.to_numpy()
        # yp = y.to_numpy()

        # lm1=pg.linear_regression(X,y,add_intercept=True,relimp=True).round(4)
    #    print('\nlm1 - ',lm1)

        # com intercepto
        # lm1 = pg.linear_regression(X, y, add_intercept=True, relimp=True, as_dataframe=False)

        # print(lm1['df_model']) #graus de liberdade do modelo
        # print(lm1['df_resid']) #graus de liberdade dos resíduos

        # x = lm1['pred'].tolist()
        # Y = y.tolist()

        # plt.figure(figsize=(20,5))
        # plt.plot(x, linewidth=2, color='r')
        # plt.plot(Y, linewidth=0.5,color='b')
        # plt.title('Valores preditos e os valores reais',size=15)
        # plt.legend(['Predições','Real'],fontsize=15)
        # plt.savefig(f'plot_previsao_Pingouin_com_intercepto_{cod_equipamento}.png')
        # plt.show()

        # Exibindo as primeiras linhas das variáveis para verificação
        # print("Variáveis independentes (X):")
        # print(X.head())
        # print("\nVariável dependente (y):")
        # print(y.head())
        

        # # plot dos residuos
        # plt.figure(figsize=(20,5))
        # plt.plot(lm1['residuals'].tolist(), linewidth=2, color='g')
        # plt.legend(['resíduos'],fontsize=15)
        # plt.show()
        # plt.savefig(f'plot_residuos_{cod_equipamento}.png')
        
        
        #######################################################################################################################################

    #     # sem intercepto
    #    lm2=pg.linear_regression(X,y,add_intercept=False,relimp=True).round(4)
    # #    print('\nlm2 - ',lm2)

        # lm2 = pg.linear_regression(X, y, add_intercept=False, relimp=True, as_dataframe=False)

        # # Valores preditos vs Valores reais.
        # x2 = lm2['pred'].tolist()
        # Y2 = y.tolist()
        
        # plt.figure(figsize=(20,5))
        # plt.plot(x2, linewidth=2, color='r')
        # plt.plot(Y2, linewidth=0.5,color='b')
        # plt.title('Valores preditos e os valores reais',size=15)
        # plt.legend(['Predições','Real'],fontsize=15)
        # plt.show()
        # plt.savefig(f'plot_previsao_Pingouin_sem_intercepto_{cod_equipamento}.png')
        
        # # Resíduos do modelo sem intercepto:
        # plt.figure(figsize=(20,5))
        # plt.plot(lm2['residuals'].tolist(), linewidth=2, color='orange')
        # plt.legend(['resíduos'],fontsize=15)
        # plt.show()

        #######################################################################################################################################
        # modelos com a Statsmodels

        # # Adicionando uma constante (intercepto) para o modelo1
        # X_with_const = sm.add_constant(X)

        # # Treinando os modelos
        # modelo1 = sm.OLS(y, X_with_const).fit()  # Modelo com intercepto
        # modelo2 = sm.OLS(y, X['valor_cod_campo_21']).fit()  # Modelo sem intercepto (apenas com pressão do óleo)

    #    print('\n',modelo1.summary(title='Sumário do modelo com intercepto'))
    #    print('\n',modelo2.summary(title='Sumário do modelo sem intercepto'))


        # # Previsões dos modelos
        # Predicoes = pd.DataFrame()
        # Predicoes['Predições 1'] = modelo1.predict(X_with_const)
        # Predicoes['Predições 2'] = modelo2.predict(X['valor_cod_campo_21'])  # Previsão com uma única variável
        # Predicoes['Potência ativa (real)'] = df_pivoted['valor_cod_campo_3']

        # # Gráfico de Previsões
        # plt.figure(figsize=(20, 10))
        # Predicoes[['Predições 1', 'Predições 2', 'Potência ativa (real)']].plot(color=['r', 'g', 'b'])
        # plt.title("Previsões vs Potência ativa real")
        # plt.savefig(f'previsoes_{cod_equipamento}.png')
        # plt.show()

        # # Resíduos do modelo 1
        # residuos1 = modelo1.resid
        # fig, ax = plt.subplots(2, 2, figsize=(15, 6))
        # residuos1.plot(title="Resíduos do modelo 1", ax=ax[0][0])
        # sns.histplot(residuos1, kde=True, ax=ax[0][1])
        # plot_acf(residuos1, lags=40, ax=ax[1][0])
        # qqplot(residuos1, line='s', ax=ax[1][1])
        # plt.savefig(f'residuos_modelo1_{cod_equipamento}.png')
        # plt.show()

        # # Resíduos do modelo 2
        # residuos2 = modelo2.resid
        # fig, ax = plt.subplots(2, 2, figsize=(15, 6))
        # residuos2.plot(title="Resíduos do modelo 2", ax=ax[0][0])
        # sns.histplot(residuos2, kde=True, ax=ax[0][1])
        # plot_acf(residuos2, lags=40, ax=ax[1][0])
        # qqplot(residuos2, line='s', ax=ax[1][1])
        # plt.savefig(f'residuos_modelo2_{cod_equipamento}.png')
        # plt.show()

        # nome = ['Estatística', 'Probabilidade']
        # teste = sms.omni_normtest(modelo1.resid)
        # print('modelo 1',lzip(nome, teste))

        # print('Número condição do modelo 1 :',np.linalg.cond(modelo1.model.exog)) #  Se o valor for maior que 900, então há evidência para multicolinearidade.

        # nome2 = ['Estatística', 'Probabilidade']
        # teste2 = sms.omni_normtest(modelo2.resid)
        # print('modelo 2',lzip(nome2, teste2))
        
        # print('Número condição do modelo 2 :',np.linalg.cond(modelo2.model.exog))

        # # Adicionar os resíduos no DataFrame para facilitar o regplot
        # df_pivoted['residuos1'] = modelo1.resid
        # df_pivoted['residuos2'] = modelo2.resid

        # # Gráficos de resíduos vs variáveis independentes
        # fig, ax = plt.subplots(1, 2, figsize=(20, 7))
        # sns.regplot(x='valor_cod_campo_114', y='residuos1', data=df_pivoted, ax=ax[0])
        # sns.regplot(x='valor_cod_campo_21', y='residuos2', data=df_pivoted, ax=ax[1])
        # plt.savefig(f'regplot_residuos_{cod_equipamento}.png')
        # plt.show()

        # # Análise de pontos de alavanca: observações cujo os regressores apresentam padrão atípico
        # fig, ax = plt.subplots(1,2,figsize=(20,5))
        # plot_leverage_resid2(modelo1, ax = ax[0])
        # plot_leverage_resid2(modelo2, ax = ax[1])
        # plt.savefig(f'plot_analise_influencia{cod_equipamento}.png')
        
        # # influência de uma observação ou instâncias em uma regressão linear. Muitos pontos altamente influentes podem não ser adequados para regressão linear.
        # plt.figure(figsize=(20,5))
        # CooksDistance().fit(X, y).show()
        # plt.savefig(f'plot_medida_influencia{cod_equipamento}.png')

        # # Criação do objeto OLSInfluence
        # influence = OLSInfluence(modelo1)

        # # Propriedades e métodos de interesse:

        # # 1. Cook's Distance (medida de influência de cada ponto)
        # cooks_d = influence.cooks_distance
        # print("Cook's Distance (Distância de Cook):\n", cooks_d)

        # # 2. DFBETAS (influência de cada observação nos coeficientes de regressão)
        # dfbetas = influence.dfbetas
        # print("DFBETAS:\n", dfbetas)

        # # 3. DFFITS (influência de cada observação nas predições)
        # dffits = influence.dffits[0]  # Primeiro valor contém os dffits, segundo valor é o limite
        # print("DFFITS:\n", dffits)

        # # 4. Leverage (influência de cada observação, ou distância do ponto em relação aos outros)
        # hat_diag = influence.hat_matrix_diag
        # print("Leverage (Hat Matrix Diagonal):\n", hat_diag)

        # # 5. Residuals (Externally studentized residuals)
        # resid_studentized = influence.resid_studentized_external
        # print("Externally Studentized Residuals:\n", resid_studentized)

        # # 6. Press Residuals
        # press_residuals = influence.resid_press
        # print("Press Residuals:\n", press_residuals)

        # # 7. Covariance Ratios
        # cov_ratios = influence.cov_ratio
        # print("Covariance Ratios:\n", cov_ratios)

        # # 8. Influence Summary Table
        # summary_frame = influence.summary_frame()
        # print("\nResumo dos dados de influência:\n", summary_frame.head())

        # # identificando graficamente observações influentes
        # fig, ax = plt.subplots(1,2,figsize=(20,5))
        # OLSInfluence(modelo1).plot_influence(ax=ax[0])
        # OLSInfluence(modelo2).plot_influence(ax=ax[1])
        # plt.savefig(f'plot_observacoes_influentes{cod_equipamento}.png')

        #######################################################################################################################################

    #     # Criação de subplots para diferentes gráficos de dispersão
    #     plt.figure(figsize=(18, 6))

    #     # Gráfico de dispersão para valor_cod_campo_3 vs valor_cod_campo_114
    #     plt.subplot(1, 3, 1)
    #     plt.scatter(df_pivoted['valor_cod_campo_3'], df_pivoted['valor_cod_campo_114'])
    #     plt.xlabel('Pot Ativa (valor_cod_campo_3)')
    #     plt.ylabel('Load Speed (valor_cod_campo_114)')
    #     plt.title('Relação entre Pot Ativa e Load Speed')
    #     plt.grid(True)

    #     # Gráfico de dispersão para valor_cod_campo_3 vs valor_cod_campo_21
    #     plt.subplot(1, 3, 2)
    #     plt.scatter(df_pivoted['valor_cod_campo_3'], df_pivoted['valor_cod_campo_21'])
    #     plt.xlabel('Pot Ativa (valor_cod_campo_3)')
    #     plt.ylabel('Pressão do Óleo (valor_cod_campo_21)')
    #     plt.title('Relação entre Pot Ativa e Pressão do Óleo')
    #     plt.grid(True)

    #     # Gráfico de dispersão para valor_cod_campo_114 vs valor_cod_campo_21
    #     plt.subplot(1, 3, 3)
    #     plt.scatter(df_pivoted['valor_cod_campo_114'], df_pivoted['valor_cod_campo_21'])
    #     plt.xlabel('Load Speed (valor_cod_campo_114)')
    #     plt.ylabel('Pressão do Óleo (valor_cod_campo_21)')
    #     plt.title('Relação entre Load Speed e Pressão do Óleo')
    #     plt.grid(True)

    #     plt.tight_layout()
    # #    plt.savefig(f'plot_{cod_equipamento}.png')  # Salva o gráfico com um nome específico para cada equipamento
    # #    plt.close()
    #     plt.show()
        
        #######################################################################################################################################
        
        # # Gráfico 3D com X = valor_cod_campo_3 (azul), Y = valor_cod_campo_114 (vermelho), Z = valor_cod_campo_21 (verde)
        # fig = plt.figure(figsize=(10, 8))
        # ax = fig.add_subplot(111, projection='3d')

        # # Criação de um colormap para colorir os pontos com base em 'valor_cod_campo_3'
        # norm = plt.Normalize(df_pivoted['valor_cod_campo_3'].min(), df_pivoted['valor_cod_campo_3'].max())
        # colors = cm.viridis(norm(df_pivoted['valor_cod_campo_3']))  # Mapa de cores viridis aplicado aos pontos

        # # Gráfico de dispersão 3D com cores variando com base no eixo X (valor_cod_campo_3)
        # scatter = ax.scatter(df_pivoted['valor_cod_campo_3'], df_pivoted['valor_cod_campo_114'], df_pivoted['valor_cod_campo_21'], 
        #                     c=colors, marker='o')

        # # Alterando as cores dos textos dos eixos
        # ax.set_xlabel('Pot Ativa (valor_cod_campo_3)', color='blue')
        # ax.set_ylabel('Load Speed (valor_cod_campo_114)', color='red')
        # ax.set_zlabel('Pressão do Óleo (valor_cod_campo_21)', color='green')
        # ax.set_title(f'Gráfico 3D - Equipamento {cod_equipamento}')

        # # Remover a barra de cores (comentada a linha abaixo)
        # # fig.colorbar(scatter, ax=ax, label='Pot Ativa (valor_cod_campo_3)')

        # plt.show()  # Exibe o gráfico na tela para interação

        #######################################################################################################################################

        # grafico de correlacoes entre variaveis
        # plt.figure(figsize=(20,8))
        # plt.title('Correlação de Spearman',size=15)
        # sns.heatmap(df_pivoted.corr('spearman'), annot = True, cmap= "RdYlGn");
        # plt.savefig(f'plot_corelacao_{cod_equipamento}.png') 

        #######################################################################################################################################

        # Gráfico de boxplot para verificar a presença de valores extremos (caixa grande: maior parte dos dados. reta com traco: ate onde os dados podem ir sem ser outliners, se passar sao outliners. risco vermelho: media de valores )
        # df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_114', 'valor_cod_campo_21']].plot.box(figsize=(20, 6))

        # # Salva o gráfico em um arquivo
        # plt.title(f'Distribuição dos Valores - Equipamento {cod_equipamento}')
        # plt.savefig(f'plot_outliers_{cod_equipamento}.png')
        # plt.show()

        #######################################################################################################################################

        # Histograma para os dados (ver a quantidade de leituras para o valor do campo)
        # df_pivoted[['valor_cod_campo_3', 'valor_cod_campo_114', 'valor_cod_campo_21']].hist(figsize=(20, 8), bins=50)

        # # Adiciona um título ao gráfico
        # plt.suptitle(f'Histograma dos Valores - Equipamento {cod_equipamento}', fontsize=16)
        # plt.savefig(f'plot_Histograma_{cod_equipamento}.png')

        # # Exibe o gráfico na tela
        # plt.show()

        #######################################################################################################################################

        # # Renomear colunas para facilitar a identificação nos gráficos
        # df_pivoted = df_pivoted.rename(columns={
        #     'valor_cod_campo_21': 'Pressão do Óleo',
        #     'valor_cod_campo_114': 'Load Speed',
        #     'valor_cod_campo_3': 'Potência Ativa'
        # })

        # # Criar os subplots
        # fig, ax = plt.subplots(2, 2, figsize=(20, 10))

        # # Gráficos de dispersão entre as variáveis.
        # sns.scatterplot(x='Potência Ativa', y='Pressão do Óleo', data=df_pivoted, ax=ax[0][0])
        # ax[0][0].set_title('Potência Ativa vs Pressão do Óleo')

        # sns.scatterplot(x='Potência Ativa', y='Load Speed', data=df_pivoted, ax=ax[0][1])
        # ax[0][1].set_title('Potência Ativa vs Load Speed')

        # sns.scatterplot(x='Load Speed', y='Pressão do Óleo', data=df_pivoted, ax=ax[1][0])
        # ax[1][0].set_title('Load Speed vs Pressão do Óleo')

        # # Adicionar um gráfico adicional de sua escolha ou deixar vazio
        # ax[1][1].axis('off')  # Se não houver mais gráficos, desativar o último subplot

        # # Ajustar layout
        # plt.tight_layout()

        # # Salvar a figura
        # plt.savefig(f'plot_dispersao_{cod_equipamento}.png', dpi=300)
        # plt.show()

        
    except Exception as error:
        print('\n----------------------------------------------------------------------------------------------------------------\n')                            
        print(f"Erro ao processar equipamento {cod_equipamento}: {error}")
        continue
