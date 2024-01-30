import mysql.connector
import pandas as pd
#from sqlalchemy import create_engine, MetaData, inspect
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error
from datetime import datetime, timedelta, time
from numpy.linalg import cond
from pandas import to_datetime
from datetime import datetime, timedelta

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

tempo_inicial = datetime.now()
print(tempo_inicial)

'''
# Executa uma consulta SQL para obter todos os cod_equipamentos únicos da última tabela
query_equipamentos = "SELECT DISTINCT cod_equipamento FROM log_leituras_2023_08 ORDER BY cod_equipamento"
cursor.execute(query_equipamentos)

# Busca todos os resultados
resultados_equipamentos = cursor.fetchall()

# Converte os resultados em uma lista
cod_equipamentos = [str(resultado[0]) for resultado in resultados_equipamentos]
'''

cod_equipamentos = ['434']

# Calcular o total de equipamentos encontrados
total_equipamentos = len(cod_equipamentos)

# Imprimir o resultado
print(f"Total de equipamentos encontrados: {total_equipamentos}")


# Caminho para o diretório e o arquivo CSV
arquivo_coeficiente = "coeficiente_geradores/"
caminho_arquivo_coeficiente = f"{arquivo_coeficiente}coeficiente_geradores.csv"

# Verificar se o diretório existe; se não, criá-lo
if not os.path.exists(arquivo_coeficiente):
    os.makedirs(arquivo_coeficiente)

# ----------------------------inicio arquivo data e hora --------------------------------------------------------

def atualizar_ultima_execucao(pasta_datas_salvas="datas_salvas", arquivo_variaveis_horas="variaveis_horas.txt", dias_passados=1, hora_desejada=time(00, 00)):
    # Verificar se a pasta 'datas_salvas' existe; se não, criá-la
    if not os.path.exists(pasta_datas_salvas):
        os.makedirs(pasta_datas_salvas)

    # Verificar se o arquivo 'variaveis_horas' existe; se não, criá-lo e salve a hora atual
    caminho_arquivo_variaveis_horas = os.path.join(pasta_datas_salvas, arquivo_variaveis_horas)

    # Variável para armazenar a última data e hora registradas
    ultima_data_hora_registrada = None

    # Verificar se o arquivo tem alguma data
    if os.path.exists(caminho_arquivo_variaveis_horas):
        with open(caminho_arquivo_variaveis_horas, "r") as arquivo:
            conteudo = arquivo.read().strip()

        # Se o arquivo tiver alguma data
        if conteudo:
            # Divide o conteúdo do arquivo em data e hora
            ultima_data_str, ultima_hora_str = conteudo.split()

            # Converte a data e hora do arquivo para um objeto datetime
            ultima_data_hora_registrada = datetime.strptime(f"{ultima_data_str} {ultima_hora_str}", "%Y-%m-%d %H:%M:%S")

            # Calcula a diferença em dias desde a última data
            diferenca_dias = (datetime.now() - ultima_data_hora_registrada).days

            # Verifica se é hora de executar novamente (13:28)
            if diferenca_dias >= dias_passados and datetime.now().time() >= hora_desejada:
                # Atualiza a data e hora da última execução
                ultima_data_hora_registrada = datetime.now()
                
                # Formata a nova data e hora
                nova_data_hora_str = ultima_data_hora_registrada.strftime('%Y-%m-%d %H:%M:%S')

                # Escreve a nova data e hora no arquivo
                with open(caminho_arquivo_variaveis_horas, "w") as arquivo:
                    arquivo.write(nova_data_hora_str)
                
# ------------------------------- inicio loop geradores ------------------------------------------

#                try:
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

                            #obter a ultima data desse equipamento
                        #    ultima_data = df['data_cadastro'].iloc[-1]
                            
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
                            
                            # Filtrar para apenas as linhas onde 'cod_campo' é 120 e 'valor' é maior que zero
                            df_filtrado = df[(df['cod_campo'] == 120) & (df['valor'] > 0)]
                            # Se o DataFrame filtrado não estiver vazio, obter o último valor
                            if not df_filtrado.empty:
                                ultimo_valor = df_filtrado.iloc[-1]['valor']
                                cod_campo_120 = ultimo_valor
                                
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

                            # Excluir linhas com valores zero em ambas as colunas 'valor_cod_campo_3' e 'valor_cod_campo_114'
                    #        df_pivoted = df_pivoted[(df_pivoted['valor_cod_campo_3'] != 0) & (df_pivoted['valor_cod_campo_114'] != 0)]

                            # Filtrar o DataFrame para incluir apenas valores menores ou iguais em 'valor_cod_campo_114' em relação a 100
                            df_pivoted = df_pivoted[df_pivoted['valor_cod_campo_114'] <= 100]
                            
                            # Encontre o último valor maior que zero na coluna 'valor_cod_campo_120'
                #            cod_campo_120 = df_pivoted['valor_cod_campo_120'][df_pivoted['valor_cod_campo_120'] > 0].iloc[-1]

                            # Preencher valores ausentes com zero na coluna 'valor_cod_campo_120'
                            df_pivoted['valor_cod_campo_120'] = df_pivoted['valor_cod_campo_120'].fillna(0)

                            # Encontrar o último valor maior que zero na coluna 'valor_cod_campo_120'
                            ultimos_valores = df_pivoted['valor_cod_campo_120'][df_pivoted['valor_cod_campo_120'] > 0]

                            if not ultimos_valores.empty:
                                cod_campo_120 = ultimos_valores.iloc[-1]


                            # Filtrar o DataFrame para incluir apenas valores menores ou iguais em 'valor_cod_campo_3' em relação a 'cod_campo_120'
                            df_pivoted = df_pivoted[df_pivoted['valor_cod_campo_3'] <= cod_campo_120]

                            # Preencher valores ausentes com zero
                            df_pivoted = df_pivoted.fillna(0)

                            
            #                arquivo = "dados_geradores/"
                            caminho_arquivo_csv_filtrado = f"{arquivo}dados_filtrados_leitura_{cod_equipamento}.csv"


                                
                            # Verificar se o diretório existe; se não, criá-lo
            #                if not os.path.exists(arquivo):
            #                    os.makedirs(arquivo)
                                                                                
                            df_pivoted.to_csv(caminho_arquivo_csv_filtrado, index=False)
                            
                            print('\n----------------------------------------------------------------------------------------------------------------\n')
                            print(f'equipamento: {cod_equipamento} \n')
                            print('ultima_data',ultima_data)

                        #    print(f"Os dados filtrados foram salvos em {caminho_arquivo_csv} e em {caminho_arquivo_csv_filtrado} \n")


                            # Converter os valores de X e y para float
                            # colocar mais variaveis em X para regressao multipla
                            X = df_pivoted['valor_cod_campo_3'].astype(float).values.reshape(-1, 1)  # Recursos de treinamento
                            y = df_pivoted['valor_cod_campo_114'].astype(float)  # Alvo de previsão

                            
                            def treinar_modelo_e_filtrar(X, y, cod_equipamento, cod_campo_120):
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

                                # Imprimir o resumo do modelo
                        #        print(est2_filtrado.summary())

                                print(f'\nPot. Nominal (cod_campo_120) = {cod_campo_120}')
                                                        
                                coeficiente = round(reg_filtrado.coef_[0], 4)
                                r2_filtrado = round(r2_filtrado, 4)
                                
                                return coeficiente, r2_filtrado
                            
                                '''
                              
                            def treinar_modelo_e_filtrar(X, y, cod_equipamento, cod_campo_120):
                                scaler = StandardScaler()
                                X = scaler.fit_transform(X)
                                reg = SGDRegressor()
                                reg.partial_fit(X, y)
                                y_pred = reg.predict(X)
                                residuos = y - y_pred
                                media_residuos = np.mean(residuos)
                                desvio_padrao_residuos = np.std(residuos)
                                limiar = 2 * desvio_padrao_residuos
                                valores_normais = (abs(residuos) <= limiar)
                                X_filtrado = X[valores_normais]
                                y_filtrado = y[valores_normais]
                                reg_filtrado = SGDRegressor()
                                reg_filtrado.partial_fit(X_filtrado, y_filtrado)
                                y_pred_filtrado = reg_filtrado.predict(X_filtrado)
                                r2_filtrado = r2_score(y_filtrado, y_pred_filtrado)
                                print(f'\nPot. Nominal (cod_campo_120) = {cod_campo_120}')
                                coeficiente = round(reg_filtrado.coef_[0], 4)
                                r2_filtrado = round(r2_filtrado, 4)
                                return coeficiente, r2_filtrado
                                
                                '''
  
                            # Exemplo de uso:
                #            coeficiente = treinar_modelo_e_filtrar(X, y, cod_equipamento, cod_campo_120)
                            coeficiente = 0.0
                            acuracia = 0.0

                            def verificar_e_obter_coeficiente(cod_equipamento):
                                coeficiente_existente = 0.0
                                acuracia_existente = 0.0
                                data_existente = None

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

                                coeficiente_novo, acuracia_nova = treinar_modelo_e_filtrar(X, y, cod_equipamento, cod_campo_120)
                                print(f'Novo Coeficiente = {coeficiente_novo}, Acuracia = {acuracia_nova}')
                                
                                if (not equipamento_existe or acuracia_nova >= acuracia_existente):
                        #            if acuracia_nova < 1.0 and coeficiente_novo >= 0:
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

                                #        print(f'Coeficiente do gerador {cod_equipamento} = {coeficiente_existente}, R² filtrado = {acuracia_existente}: salvo em {caminho_arquivo_coeficiente}')

                                return coeficiente_existente, acuracia_existente


                            # Verificar e obter o coeficiente
                            coeficiente_existente, r2_filtrado_existente = verificar_e_obter_coeficiente(cod_equipamento)
                            coeficiente += coeficiente_existente
                            acuracia += r2_filtrado_existente

                            '''

                            def verificar_e_obter_coeficiente(cod_equipamento):
                                coeficiente_existente = None
                                acuracia_existente = None
                                data_existente = None
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

                                if not equipamento_existe or ultima_data > data_existente:
                                    coeficiente_novo, acuracia_nova = treinar_modelo_e_filtrar(X, y, cod_equipamento, cod_campo_120)
                                    print(f'Novo Coeficiente = {coeficiente_novo}, Acuracia = {acuracia_nova}')

                                # Se o equipamento não existir ou a nova acurácia for maior que a existente
                                if (not equipamento_existe or acuracia_nova >= acuracia_existente) and acuracia_nova < 1.0 and coeficiente_novo >= 0:
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

                                    print(f'Coeficiente do gerador {cod_equipamento} = {coeficiente_existente}, R² filtrado = {acuracia_existente}: salvo em {caminho_arquivo_coeficiente}')

                                return coeficiente_existente, acuracia_existente


                            # Verificar e obter o coeficiente
                            coeficiente_existente, r2_filtrado_existente = verificar_e_obter_coeficiente(cod_equipamento)
                            coeficiente += coeficiente_existente
                            acuracia += r2_filtrado_existente

                            '''



                        except Exception as error:
                            print('\n----------------------------------------------------------------------------------------------------------------\n')                            
                            print(f"Erro ao processar equipamento {cod_equipamento}: {error}")
                            continue  # Pular para o próximo equipamento em caso de erro

#                except Exception as error:
#                    print("Erro ao conectar ao banco de dados ou durante a execução:", error)
        else:
            print("Ainda não é hora de executar o código principal.")

# ----------------------------- fim loop geradores ------------------------------------------------------------               
                
    else:
        # Se o arquivo não existir, cria-o e salva a data e hora atual
        ultima_data_hora_registrada = datetime.now()

        # Formata a data e hora
        nova_data_hora_str = ultima_data_hora_registrada.strftime('%Y-%m-%d %H:%M:%S')

        # Escreve a data e hora no arquivo
        with open(caminho_arquivo_variaveis_horas, "w") as arquivo:
            arquivo.write(nova_data_hora_str)


    # Calcule o tempo total de execução
    tempo_total = timedelta(seconds=int(tm.time() - tempo_inicial.timestamp()))

    print(f"Tempo total de execução: {tempo_total}")

    # Retorna a informação da última data e hora registradas
    return ultima_data_hora_registrada

# Exemplo de uso da função
#ultima_data_hora = atualizar_ultima_execucao()
#print(f"A última data e hora registradas foram: {ultima_data_hora}")



# ------------------- fim do arquivo data e hora-----------------------------------------------------------------

import time as sleep_module  

def tempo_para_proxima_execucao(dias_passados=1, hora_desejada=(00, 00)):
    # Obter a data e hora atuais
    agora = datetime.now()

    # Calcular a próxima data e hora desejada
    proxima_execucao = datetime(agora.year, agora.month, agora.day, hora_desejada[0], hora_desejada[1]) + timedelta(days=dias_passados)

    # Verificar se a próxima execução é no futuro
    if proxima_execucao <= agora:
        proxima_execucao += timedelta(days=1)

    # Calcular o tempo restante para a próxima execução
    tempo_restante = proxima_execucao - agora

    return tempo_restante

while True:
    try:
        print("Refazendo teste...")
        ultima_data_hora = atualizar_ultima_execucao()
        print(f"A última data e hora registradas foram: {ultima_data_hora}")

    except Exception as e:
        print(f"Ocorreu uma exceção: {e}")

    
#    finally:
#        for segundos_restantes in range(60, 0, -1):
#            print(f"Próxima execução em {segundos_restantes} segundos", end='\r')
#            tm.sleep(1)

            
    finally:
        tempo_restante = tempo_para_proxima_execucao()

        for segundos_restantes in range(tempo_restante.seconds, 0, -1):
            horas, minutos = divmod(segundos_restantes, 3600)
            minutos, segundos = divmod(minutos, 60)
            print(f"Próxima execução em {horas:02d}:{minutos:02d}:{segundos:02d}", end='\r')

            # Adicione uma verificação de interrupção para encerrar o script se necessário
            try:
                tm.sleep(1)
            except KeyboardInterrupt:
                print("\nScript interrompido pelo usuário.")
                exit()

        print("Próxima execução em breve!\n")
        
        # Ao finalizar a contagem regressiva, pule uma linha para evitar sobreposição
        print("\n")
