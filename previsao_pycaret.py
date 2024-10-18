import mysql.connector
import pandas as pd
#from sqlalchemy import create_engine, MetaData, inspect
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
import os
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

import pickle
from pycaret.regression import setup, compare_models, finalize_model, predict_model
import aiomysql
import asyncio

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.contrib.fsm_storage.memory import MemoryStorage
import asyncio
import signal
import sys

TOKEN = "6674960909:AAEf4Ky64lM6NILX3wZ6HJEASTt42Q2vopc"

bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())


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
  database="sup_geral",
  autocommit=True,  # Isso garante que as transações sejam automaticamente confirmadas
  connection_timeout=600,  # Aumenta o tempo limite de conexão
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

percent_treino = 0.2

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


# Função para criar a pool de conexões assíncrona
async def create_pool():
    pool = await aiomysql.create_pool(
        host="192.168.4.50",
        user="bruno",
        password="superbancoml",
        db="machine_learning",
        minsize=5,  # Número mínimo de conexões
        maxsize=10  # Número máximo de conexões
    )
    return pool







# Criar tabela modelos_IA se ainda não existir
create_table_query = """
CREATE TABLE IF NOT EXISTS modelos_IA_py (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cod_equipamento INT NOT NULL,
    cod_campo_x INT NOT NULL,
    cod_campo_y INT NOT NULL,
    modelo LONGBLOB,
    data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
cursor.execute(create_table_query)
cnx.commit()



'''

# Processamento para cada equipamento válido
cod_equipamentos_validos = [2170]

# Criar a pasta "modelos_de_ia" se não existir
if not os.path.exists('modelos_de_ia'):
    os.makedirs('modelos_de_ia')



# Primeiro, baixe todos os dados necessários de todas as tabelas e campos para uma única variável
try:
    todos_cod_campo = list(set([campo for par in cod_campo_pairs for campo in par]))
    queries = []
    print('todos_cod_campo', todos_cod_campo)

    for tabela in tabelas:
        query = f"""
        SELECT cod_equipamento, data_cadastro, valor, cod_campo 
        FROM {tabela} 
        WHERE cod_campo IN ({', '.join(map(str, todos_cod_campo))})
        """
        queries.append(query)

    final_query = " UNION ALL ".join(queries)
    cursor_leitura.execute(final_query)
    resultados = cursor_leitura.fetchall()

    # Processar os dados uma vez e manter em uma variável
    df_global = pd.DataFrame(resultados, columns=['cod_equipamento', 'data_cadastro', 'valor', 'cod_campo'])
    df_global['data_cadastro'] = pd.to_datetime(df_global['data_cadastro'])
    df_global['rounded_time'] = df_global['data_cadastro'].dt.round('5min')
    df_global['valor'] = df_global['valor'].replace({'-1-1': np.nan})
    df_global['valor'] = pd.to_numeric(df_global['valor'], errors='coerce')
    df_global['valor'] = df_global['valor'].fillna(0)

    # Agrupar os valores e interpolar para preencher valores ausentes
    df_global = df_global.groupby(['cod_equipamento', 'rounded_time', 'cod_campo']).agg({'valor': 'mean'}).reset_index()
    df_global = df_global.replace([np.inf, -np.inf], np.nan).dropna()
    df_global = df_global.interpolate()

except Exception as error:
    print(f"Erro ao baixar os dados iniciais do banco de dados: {error}")



# Agora, o loop vai trabalhar com os dados já carregados na variável df_global
for cod_equipamento in cod_equipamentos_validos:
    try:
        # Filtrar os dados do equipamento específico
        df_equipamento = df_global[df_global['cod_equipamento'] == cod_equipamento]

        if df_equipamento.empty:
            print(f'Sem dados para o equipamento {cod_equipamento}')
            continue

        # Criar um pivot com todos os cod_campo para o equipamento
        df_pivoted = df_equipamento.pivot_table(
            index='rounded_time', 
            columns='cod_campo', 
            values='valor', 
            aggfunc='mean'
        ).reset_index()  # Isso garante que `rounded_time` volte a ser uma coluna

        print(df_pivoted.columns)

        df_pivoted.columns = [
            f'valor_cod_campo_{col}' if col != 'rounded_time' else 'rounded_time'
            for col in df_pivoted.columns
        ]
        
        # Substituir valores NaN por 0
        df_pivoted = df_pivoted.fillna(0)

        print('df_pivoted.head()\n', df_pivoted.head())

        df_pivoted = df_pivoted.reset_index()

        # Agora iterar pelos pares de cod_campo
        for x, y in cod_campo_pairs:

            df_filtered = df_pivoted.dropna(subset=[f'valor_cod_campo_{x}', f'valor_cod_campo_{y}'])

            # Verificar se a coluna 'rounded_time' está presente após o dropna
            print('df_filtered.columns\n', df_filtered.columns)

            if df_filtered.empty:
                print(f'Sem dados suficientes para {x} e {y} no equipamento {cod_equipamento}')
                continue

            # Interpolação dos valores ausentes
            df_filtered = df_filtered.interpolate()

            print('df_filtered.describe()\n',df_filtered.describe())

            # Remover linhas onde tanto valor_cod_campo_x quanto valor_cod_campo_y são 0
            df_filtered = df_filtered.loc[~((df_filtered[f'valor_cod_campo_{x}'] == 0) & 
                                            (df_filtered[f'valor_cod_campo_{y}'] == 0))]

            # Verificar o DataFrame após remover as linhas
            print("Depois de remover os zeros:")
            print(df_filtered.describe())

            # Verificar se os zeros foram removidos corretamente
            print(f"Número de linhas após remoção de zeros: {len(df_filtered)}")

            print('\n----------------------------------------------------------------------------------------------------------------\n')
            print(f'equipamento: {cod_equipamento}, X: {x}, Y: {y}')

            dados_treinamento = df_filtered.dropna()

            # Configurar e comparar modelos
            reg = setup(dados_treinamento, target=f'valor_cod_campo_{x}', session_id=123, use_gpu=False, log_experiment=False)
            melhor_modelo = compare_models()

            # Finalizar o modelo e fazer previsões
            modelo_treinado = finalize_model(melhor_modelo)

            previsoes = predict_model(modelo_treinado, data=dados_treinamento)
            print(previsoes.head())

            # Serializar o modelo usando pickle
            modelo_serializado = pickle.dumps(modelo_treinado)

            # Salvar o modelo como um arquivo .pkl
            try:
                filename = f'modelos_de_ia/modelo_equipamento_{cod_equipamento}_X_{x}_Y_{y}.pkl'
                with open(filename, 'wb') as f:
                    f.write(modelo_serializado)

                print(f"Modelo treinado salvo em: {filename}")
                
            except Exception as error:
                print(f"Erro ao salvar o modelo treinado: {error}")

    except Exception as error:
        print('\n----------------------------------------------------------------------------------------------------------------\n')
        print(f"Erro ao processar equipamento {cod_equipamento}: {error}")
        continue


'''


import os
import pandas as pd
import numpy as np
from datetime import datetime
from pycaret.regression import setup, compare_models, save_model, load_model, predict_model
import asyncio  # Para lidar com funções assíncronas
from aiogram import executor, Dispatcher

# Processamento para cada equipamento válido
#cod_equipamentos_validos = [2170]

# Criar a pasta "modelos_de_ia" se não existir
if not os.path.exists('modelos_de_ia'):
    os.makedirs('modelos_de_ia')




# Treinamento com os dados históricos
for cod_equipamento in cod_equipamentos_validos:
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

        print(f'\n{"-"*120}\n')
        print(f'equipamento: {cod_equipamento} \n')

        # X e y para o treino
        X = df_pivoted[['valor_cod_campo_3']].astype(float).values
        y = df_pivoted['valor_cod_campo_114'].astype(float)

        # Transformando o DataFrame para usar no PyCaret
        treino_df = pd.DataFrame({'valor_cod_campo_3': X.flatten(), 'valor_cod_campo_114': y})

        # Configurando o PyCaret (silent e verbose não existem mais; removidos)
        reg_setup = setup(data=treino_df, target='valor_cod_campo_114', session_id=123, log_experiment=False)

        # Treinando o modelo
        best_model = compare_models()

        # Salvando o modelo na pasta 'modelos_de_ia'
        modelo_path = os.path.join('modelos_de_ia', f'modelo_equipamento_{cod_equipamento}.pkl')
        save_model(best_model, modelo_path)

    except Exception as e:
        print(f"Erro no equipamento {cod_equipamento}: {e}")



# Função para usar o modelo salvo com dados em tempo real
async def aplicar_modelo_real(pool, cod_equipamento):
    try:
        # Recupera o modelo treinado
        modelo_path = os.path.join('modelos_de_ia', f'modelo_equipamento_{cod_equipamento}.pkl')
        model = load_model(modelo_path)

        # Loop para execução contínua
        while True:
            try:
                # Obtém os dados reais
                async with pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("""
                            SELECT valor_1, valor_2, valor_3, valor_4, valor_5, data_cadastro
                            FROM machine_learning.leituras_consecutivas
                            WHERE cod_campo = 114 AND cod_equipamento = %s
                        """, (cod_equipamento,))
                        resultado = await cursor.fetchone()

                if resultado and len(resultado) >= 3:
                    valor_3 = resultado[4]  # 'valor_3' usado para previsão

                    # Prevendo com o modelo carregado
                    real_df = pd.DataFrame({'valor_cod_campo_3': [valor_3]})
                    previsao = predict_model(model, data=real_df)

                    # Exibindo todas as colunas da previsão para depuração
                    print(f"Previsão para o equipamento {cod_equipamento}:")
                    print(previsao.head())  # Inspeciona a estrutura

                    # Verificando qual coluna existe para acessar a previsão
                    coluna_previsao = 'prediction_label' if 'prediction_label' in previsao.columns else previsao.columns[-1]
                    print(f"Previsão para o equipamento {cod_equipamento}: {previsao[coluna_previsao][0]}")

                else:
                    print(f"Sem dados reais para o equipamento {cod_equipamento}")

                # Aguardar alguns segundos antes de repetir o ciclo
                await asyncio.sleep(10)

            except Exception as e:
                print(f"Erro no loop do equipamento {cod_equipamento}: {e}")

    except Exception as e:
        print(f"Erro ao aplicar o modelo no equipamento {cod_equipamento}: {e}")




# Função de startup
async def on_startup(dp):
    try:
        pool = await create_pool()
        dp['pool'] = pool  # Associando a pool ao dispatcher
        await aplicar_modelo_real(pool, cod_equipamentos_validos[0])
    except asyncio.CancelledError:
        print("Tarefa de processamento cancelada.")
    except Exception as e:
        print(f"Erro durante o processamento dos equipamentos: {e}")

# Função de shutdown
async def on_shutdown(dp):
    pool = dp['pool']
    
    cursor.close()
    cursor_leitura.close()
    cnx.close()
    cnx_leitura.close()

    pool.close()
    await pool.wait_closed()

# Execução do polling
if __name__ == '__main__':
    try:
        executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)
    except KeyboardInterrupt:
        print("Interrompido pelo usuário.")




tempo_final = datetime.now()
total = tempo_final - tempo_inicial
print('\ntempo total de processamento',total)



