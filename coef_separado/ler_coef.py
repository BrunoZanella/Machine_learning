import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error
from datetime import datetime, timedelta, time

#import time
import os
import csv

from scipy import stats
import numpy as np 
import statsmodels.api as sm
from   sklearn.metrics import r2_score
import seaborn as sns

# Configurações de conexão com o banco de dados MySQL
dados = {
    'host': '192.168.15.104',
    'user': 'root',
    'password': 'gridbancoteste',
    'database': 'sup_geral'
}


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
#    'log_leituras_2023_04',
#    'log_leituras_2023_05',
#    'log_leituras_2023_06',
#    'log_leituras_2023_07',
    'log_leituras_2023_08'
]

# Criar a string de conexão usando SQLAlchemy
conexao_string = f"mysql+mysqlconnector://{dados['user']}:{dados['password']}@{dados['host']}/{dados['database']}"

#cod_equipamentos = ['294', '293', '1885']
#cod_equipamentos = ['1599', '1601', '1603', '1605', '1658']

# Obter a lista de equipamentos disponíveis no banco de dados
query_equipamentos = "SELECT DISTINCT cod_equipamento FROM log_leituras_2023_08  ORDER BY cod_equipamento ASC" 
df_equipamentos = pd.read_sql_query(query_equipamentos, create_engine(conexao_string))
cod_equipamentos = df_equipamentos['cod_equipamento'].tolist()
#print(cod_equipamentos)

cod_campo_especificados = ['3', '114', '120']







# ----------------------------inicio arquivo data e hora --------------------------------------------------------

def atualizar_ultima_execucao(pasta_datas_salvas="datas_salvas", arquivo_variaveis_horas="variaveis_horas.txt", dias_passados=1, hora_desejada=time(12, 00)):
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
    else:
        # Se o arquivo não existir, cria-o e salva a data e hora atual
        ultima_data_hora_registrada = datetime.now()

        # Formata a data e hora
        nova_data_hora_str = ultima_data_hora_registrada.strftime('%Y-%m-%d %H:%M:%S')

        # Escreve a data e hora no arquivo
        with open(caminho_arquivo_variaveis_horas, "w") as arquivo:
            arquivo.write(nova_data_hora_str)

    # Retorna a informação da última data e hora registradas
    return ultima_data_hora_registrada

# Exemplo de uso da função
#ultima_data_hora = atualizar_ultima_execucao()
#print(f"A última data e hora registradas foram: {ultima_data_hora}")


# ------------------- fim do arquivo data e hora-----------------------------------------------------------------

# Caminho para o diretório e o arquivo CSV
arquivo_coeficiente = "coeficiente_geradores/"
caminho_arquivo_coeficiente = f"{arquivo_coeficiente}coeficiente_geradores.csv"

# Verificar se o diretório existe; se não, criá-lo
if not os.path.exists(arquivo_coeficiente):
    os.makedirs(arquivo_coeficiente)



# Verificar se o diretório 'logs' existe; se não, criá-lo
pasta_logs = "logs"
if not os.path.exists(pasta_logs):
    os.makedirs(pasta_logs)

caminho_arquivo_log = os.path.join(pasta_logs, "log.txt")

ultimo_cod_equipamento_processado = cod_equipamentos[0]
if os.path.exists(caminho_arquivo_log):
    with open(caminho_arquivo_log, "r") as arquivo_log:
        ultimo_cod_equipamento_processado = int(arquivo_log.read().strip())
    print(f"Último equipamento processado: {ultimo_cod_equipamento_processado}")
else:
    print("Arquivo de log não encontrado.")


# Obter índice do último equipamento processado
ultimo_indice_processado = cod_equipamentos.index(ultimo_cod_equipamento_processado)
connection = create_engine(conexao_string)
queries = []


try:


    # Iterar sobre os índices dos equipamentos a partir do último processado
#    for indice in range(ultimo_indice_processado, len(cod_equipamentos)):
#        cod_equipamento = cod_equipamentos[indice]
    for cod_equipamento in cod_equipamentos:
        try:

            for tabela in tabelas:
        #        for cod_campo in cod_campo_especificados:
        #            query = f"SELECT data_cadastro, valor, cod_campo FROM {tabela} WHERE cod_equipamento = {cod_equipamento} AND cod_campo = {cod_campo}"
                query = f"SELECT data_cadastro, valor, cod_campo FROM {tabela} WHERE cod_equipamento = {cod_equipamento} AND cod_campo IN ({', '.join(cod_campo_especificados)})"
                queries.append(query)

            final_query = " UNION ALL ".join(queries)

            df = pd.read_sql_query(final_query, connection)
#            df = pd.read_sql_query(final_query, database_connection)

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

            # Preencher valores ausentes com zero
            df = df.fillna(0)

            # Filtrar para apenas as linhas onde 'cod_campo' é 120 e 'valor' é maior que zero
            df_filtrado = df[(df['cod_campo'] == 120) & (df['valor'] > 0)]
            # Se o DataFrame filtrado não estiver vazio, obter o último valor
            if not df_filtrado.empty:
                ultimo_valor = df_filtrado.iloc[-1]['valor']
                cod_campo_120 = ultimo_valor
                                
            # Pivotear o DataFrame para obter as colunas desejadas
            df_pivoted = df.pivot(index='rounded_time', columns='cod_campo', values='valor')

            # Renomear as colunas
            df_pivoted.columns = [f'valor_cod_campo_{col}' for col in df_pivoted.columns]

            # Resetar o índice
            df_pivoted = df_pivoted.reset_index()

            # Filtrar o DataFrame para excluir linhas com valores nulos em ambas as colunas
            df_pivoted = df_pivoted.dropna(subset=['valor_cod_campo_3', 'valor_cod_campo_114'])

            # Filtrar a coluna para remover valores negativos
            valores_positivos = df_pivoted['valor_cod_campo_120'][df_pivoted['valor_cod_campo_120'] > 0]

            if not valores_positivos.empty:
                # Encontrar o último valor maior que zero na coluna 'valor_cod_campo_120'
                cod_campo_120 = valores_positivos.iloc[-1]

            # Excluir linhas com valores zero em ambas as colunas 'valor_cod_campo_3' e 'valor_cod_campo_114'
            df_pivoted = df_pivoted[(df_pivoted['valor_cod_campo_3'] != 0) & (df_pivoted['valor_cod_campo_114'] != 0)]

            # Filtrar o DataFrame para incluir apenas valores menores ou iguais em 'valor_cod_campo_114' em relação a 100
            df_pivoted = df_pivoted[df_pivoted['valor_cod_campo_114'] <= 100]
            
            # Encontre o último valor maior que zero na coluna 'valor_cod_campo_120'
    #        cod_campo_120 = df_pivoted['valor_cod_campo_120'][df_pivoted['valor_cod_campo_120'] > 0].iloc[-1]

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

#            arquivo = "dados_geradores2/"
#            caminho_arquivo_csv_filtrado = f"{arquivo}dados_filtrados_leitura_{cod_equipamento}.csv"

#            caminho_arquivo_csv = f"{arquivo}dados_leitura_{cod_equipamento}.csv"

            # Verificar se o diretório existe; se não, criá-lo
#            if not os.path.exists(arquivo):
#                os.makedirs(arquivo)
   
#            df_pivoted.to_csv(caminho_arquivo_csv_filtrado, index=False)
#            df.to_csv(caminho_arquivo_csv, index=False)

            print('\n----------------------------------------------------------------------------------------------------------------\n')
        #    print(f"Os dados filtrados foram salvos em {caminho_arquivo_csv} e em {caminho_arquivo_csv_filtrado}")
            print(f'equipamento: {cod_equipamento}')


            # Converter os valores de X e y para float
            X = df_pivoted['valor_cod_campo_3'].astype(float).values.reshape(-1, 1)  # Recursos de treinamento
            y = df_pivoted['valor_cod_campo_114'].astype(float)  # Alvo de previsão

            '''
            def treinar_modelo_e_filtrar(X, y, cod_equipamento, cod_campo_120):
                # Treinar um modelo de regressão linear
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

                # Imprimir o resumo do modelo
                print(est2_filtrado.summary())

                print(f'\n {cod_equipamento} valor Pot. Nominal (cod_campo_120) = {cod_campo_120}')

                coeficiente = round(reg_filtrado.coef_[0], 4)
                return coeficiente
                '''
            # Exemplo de uso:
#            coeficiente = treinar_modelo_e_filtrar(X, y, cod_equipamento, cod_campo_120)

            coeficiente = 0.0
            
            def verificar_e_obter_coeficiente(cod_equipamento):

                # Se o arquivo já existir, verificar se o cod_equipamento já está presente
                if os.path.exists(caminho_arquivo_coeficiente):
                    with open(caminho_arquivo_coeficiente, 'r') as arquivo_csv:
                        leitor_csv = csv.reader(arquivo_csv)
                        dados = list(leitor_csv)

                        # Verificar se o cod_equipamento já está presente no arquivo
                        for linha in dados:
                            if linha and linha[0] == str(cod_equipamento):
                                print(f'Coeficiente encontrado: {linha[1]}, acuracia: {linha[2]}')
                                # Se o cod_equipamento já estiver presente, retorna o coeficiente
                                return float(linha[1])

                # Se o cod_equipamento não estiver presente, chama a função para obter o coeficiente
        #        coeficiente = treinar_modelo_e_filtrar(X, y, cod_equipamento, cod_campo_120)

                # Salvar o coeficiente
        #        with open(caminho_arquivo_coeficiente, 'a', newline='') as arquivo_csv:
        #            escritor_csv = csv.writer(arquivo_csv)
        #            escritor_csv.writerow([str(cod_equipamento), str(coeficiente)])

        #        print(f'Coeficiente de {cod_equipamento} = {coeficiente}: salvo em {caminho_arquivo_coeficiente}')

                else:
                    print('Pular para o próximo equipamento em caso de erro')
                    pass
                
                return coeficiente

            # Verificar e obter o coeficiente
            coeficiente_existente = verificar_e_obter_coeficiente(cod_equipamento)
            
            if coeficiente_existente == 0:
                # Atualizar o arquivo de log após processar um equipamento
                if cod_equipamento > ultimo_cod_equipamento_processado:
                    with open(caminho_arquivo_log, "w") as arquivo_log:
                        arquivo_log.write(str(cod_equipamento))
                print(f'Coeficiente encontrado e zero {coeficiente_existente}. pulando equipamento')
                continue
            
            coeficiente += coeficiente_existente

            '''
            # Filtrar o DataFrame para incluir apenas os dias em que 'valor_cod_campo_114' é 100%
            df_80_percent = df_pivoted[df_pivoted['valor_cod_campo_114'] >= 80]

            # Obter uma lista de datas únicas
            datas_unicas_80_percent = df_80_percent['rounded_time'].dt.date.unique()
            datas_unicas_80_percent_str = [data.strftime('%d-%m-%Y') for data in datas_unicas_80_percent] 
                 
            print('\ndatas_unicas_80_percent_str',datas_unicas_80_percent_str,'\n')
            '''
            
            # Filtrar o DataFrame para incluir apenas os dias em que 'valor_cod_campo_114' é 100%
            df_100_percent = df_pivoted[df_pivoted['valor_cod_campo_114'] == 100]
            
            # Obter os valores correspondentes
            valores_100_percent = df_100_percent['valor_cod_campo_114']
            
            # Obter uma lista de datas únicas
            datas_unicas_100_percent = df_100_percent['rounded_time'].dt.date.unique()
            datas_unicas_100_percent_str = [data.strftime('%d-%m-%Y') for data in datas_unicas_100_percent]
            
            print('datas_unicas_100_percent_str',datas_unicas_100_percent_str,'\n valores', valores_100_percent)
            
            for i in range(1, len(datas_unicas_100_percent_str)):
                data_inicio = datas_unicas_100_percent_str[i-1]
                data_fim = datas_unicas_100_percent_str[i-1]
                print('\n',i,f'- Equipamento: {cod_equipamento}','- Data -', data_inicio, '\n')

                # Converter a data para o formato adequado
                data_verificacao_str = datetime.strptime(data_inicio, '%d-%m-%Y').strftime('%Y-%m-%d')

                # Consultar a tabela de log_alarmes para o dia e equipamento específicos
                query_alarmes = f"SELECT * FROM log_alarmes WHERE cod_equipamento = {cod_equipamento} AND DATE(data_cadastro) = '{data_verificacao_str}'"
                df_alarmes = pd.read_sql_query(query_alarmes, connection)

                # Verificar se há alarmes
                if not df_alarmes.empty:
                    # Obter os nomes dos alarmes usando os códigos dos alarmes encontrados
                    codigos_alarmes = df_alarmes['cod_alarme'].unique()
                    codigos_alarmes_str = ', '.join(map(str, codigos_alarmes))
                    query_nome_alarmes = f"SELECT codigo, nome FROM lista_alarmes WHERE codigo IN ({codigos_alarmes_str})"
                    df_nome_alarmes = pd.read_sql_query(query_nome_alarmes, connection)
                        
                    # Realizar um merge entre os dataframes df_alarmes e df_nome_alarmes usando a coluna 'cod_alarme'
                    df_final = pd.merge(df_alarmes, df_nome_alarmes, left_on='cod_alarme', right_on='codigo', how='inner')
                        
#                    print(f"\n             Alarmes encontrados para o equipamento {cod_equipamento} na data {data_verificacao_str}:\n")
                    print(f"             Alarmes encontrados:\n")
                    print(df_final[['data_cadastro','cod_alarme', 'nome']])
                    print(f"\n")
                    
                '''
                # Filtrar o DataFrame para incluir apenas valores acima de 80% dentro dos dias de 100%
                df_80_percent_dentro_dos_dias_100_percent = df_pivoted[(df_pivoted['valor_cod_campo_114'] >= 80) & 
                                                                    (df_pivoted['rounded_time'].dt.date >= pd.to_datetime(data_inicio, dayfirst=True).date()) &
                                                                    (df_pivoted['rounded_time'].dt.date <= pd.to_datetime(data_fim, dayfirst=True).date())]

                if not df_80_percent_dentro_dos_dias_100_percent.empty:
                    print(f"Valores acima de 80% dentro dos dias em que 'valor_cod_campo_114' é 100%:")
                    print(df_80_percent_dentro_dos_dias_100_percent[['rounded_time', 'valor_cod_campo_114']])
                    print("\n")
                else:
                    print("Nenhum valor acima de 80% encontrado dentro dos dias em que 'valor_cod_campo_114' é 100%.\n")
                    
                '''

                valores_reais_load_speed = [valor for data, valor in zip(df_pivoted['rounded_time'], y) if
                                            data.date() >= pd.to_datetime(data_inicio, dayfirst=True).date() and
                                            data.date() <= pd.to_datetime(data_fim, dayfirst=True).date()]

                valores_reais_pot_ativa = [valor for data, valor in zip(df_pivoted['rounded_time'], X) if
                                        data.date() >= pd.to_datetime(data_inicio, dayfirst=True).date() and
                                        data.date() <= pd.to_datetime(data_fim, dayfirst=True).date()]

                valores_projetados_pot_ativa = [valor * coeficiente for valor in valores_reais_pot_ativa]

                valores_ratio_pot_ativa = [projeto / real if real > 0 else 0 for real, projeto in zip(valores_projetados_pot_ativa, valores_reais_load_speed)]

                valores_projetados_10_pot_ativa = [valor * coeficiente * 1.1 for valor in valores_reais_pot_ativa]
                valores_projetados_menos_10_pot_ativa = [valor * coeficiente * 0.9 for valor in valores_reais_pot_ativa]

                valores_projetados_20_pot_ativa = [valor * coeficiente * 1.3 for valor in valores_reais_pot_ativa]
                
                valores_projetados_menos_20_pot_ativa = [valor * coeficiente * 0.8 for valor in valores_reais_pot_ativa]

                horas_do_dia = [data.strftime('%d-%m-%Y %H:%M') for data in df_pivoted['rounded_time'] if 
                                data.date() >= pd.to_datetime(data_inicio, dayfirst=True).date() and 
                                data.date() <= pd.to_datetime(data_fim, dayfirst=True).date()]

                # Crie uma lista de rótulos no formato "HH:MM" correspondentes aos valores
                horas_do_dia_grafico = [data.strftime('%d-%m %H:%M') for data in df_pivoted['rounded_time'] if
                                        data.date() >= pd.to_datetime(data_inicio, dayfirst=True).date() and
                                        data.date() <= pd.to_datetime(data_fim, dayfirst=True).date()]

                # Inicialize listas para armazenar os índices dos pontos acima de 1.5
                indices_pontos_acima_1_5 = []

                for data, ratio in zip(horas_do_dia_grafico, valores_ratio_pot_ativa):
                    if ratio >= 1.0:
                        print(f"{data} - Valor {ratio} é maior que 1.5")
                        # Adiciona o índice do ponto acima de 1.5
                        indice = valores_ratio_pot_ativa.index(ratio)
                        indices_pontos_acima_1_5.append(indice)

                    elif ratio <= 0.5:
                        print(f"{data} - Valor {ratio} é menor que 0.5")

                print('\n')


                # Variáveis para rastrear as chamadas consecutivas
                contagem_consecutiva = 0
                ultima_funcao_chamada = None
                ultima_hora_chamada = None
                var_chamadas = 5
                max_aceitavel_max = 100
                max_aceitavel_min = 92
                min_aceitavel_max = 91
                min_aceitavel_min = 80
                
                # Função para verificar e marcar as chamadas consecutivas
                def verificar_chamadas_consecutivas(funcao_chamada, hora_chamada):
                    global contagem_consecutiva, ultima_funcao_chamada, ultima_hora_chamada
                    
                    # Verifica se a mesma função foi chamada novamente
                    if funcao_chamada == ultima_funcao_chamada:
                        
                        '''
                        # Verifica se a diferença nas horas é de 5 minutos
                        if (hora_chamada - ultima_hora_chamada).total_seconds() / 60 <= 5:
                            contagem_consecutiva += 1
                            if contagem_consecutiva >= var_chamadas:
                                print("     \033[91m" + f"{contagem_consecutiva} leituras consecutivas detectadas!" + "\033[0m")  # Exibir em vermelho
                                
                        ''' 
                        diferenca_minutos = (hora_chamada - ultima_hora_chamada).total_seconds() / 60
                        # Verifica se a diferença nas horas é de 5 minutos
   
                        if diferenca_minutos <= 5:
                            contagem_consecutiva += 1
                            if contagem_consecutiva >= var_chamadas:
                                print("     \033[91m" + f"{contagem_consecutiva} leituras consecutivas detectadas!" + "\033[0m")  # Exibir em vermelho
                        elif diferenca_minutos <= 10:
                            contagem_consecutiva += 1
                            if contagem_consecutiva >= var_chamadas:
                    #            print("     \033[96m" + f"{contagem_consecutiva} leituras consecutivas detectadas!" + "\033[0m")  # Exibir em azul
                                print("     \033[91m" + f"{contagem_consecutiva} leituras consecutivas detectadas!" + "\033[0m")  # Exibir em vermelho

                        # ao descomentar a parte de cima, comentar ate aqui  
                        
                        else:
                            # Se a diferença nas horas não for de 5 minutos, redefina a contagem consecutiva
                            contagem_consecutiva = 1
                    else:
                        # Se a função chamada é diferente, redefina a contagem consecutiva
                        contagem_consecutiva = 1
                    
                    ultima_funcao_chamada = funcao_chamada
                    ultima_hora_chamada = hora_chamada
                    
                    
                # Função para emitir alerta de cruzamento voltando
                def cruzamentos_mais(i, real_load_speed, projetado_20, horas_do_dia):
                    # Código ANSI para cor azul
                    cor_azul = "\033[96m"
                    cor_vermelha = "\033[91m"
                    cor_laranja = "\033[93m"  # Laranja é representado pela cor 93 no código ANSI
                    # Código ANSI para resetar a cor para a cor padrão
                    cor_reset = "\033[0m"
                    
                    # Verificar se real_load_speed é igual a 100
                    if real_load_speed >= max_aceitavel_min and real_load_speed <= max_aceitavel_max :
                        cor_mensagem = cor_vermelha
                    #    mensagem = f'             {i + 1} - {horas_do_dia} - Load Speed: {real_load_speed} está acima da reta de +20 {projetado_20}'
                        mensagem = f'             {i + 1} - {horas_do_dia} - Load Speed: {real_load_speed} está acima de {max_aceitavel_min}% {projetado_20}'
                    elif real_load_speed >= min_aceitavel_min and real_load_speed <= min_aceitavel_max:
                        cor_mensagem = cor_laranja
                        mensagem = f'             {i + 1} - {horas_do_dia} - Load Speed: {real_load_speed} está acima de {min_aceitavel_min}% {projetado_20}'
                    else:
                        cor_mensagem = cor_azul
                        mensagem = f'             {i + 1} - {horas_do_dia} - Real Load Speed: {real_load_speed} está acima da reta prevista {projetado_20}'
                    
                    # Mensagem de alerta com a cor correspondente
                    print(f"{cor_mensagem}{mensagem}{cor_reset}")
                #    print(f"{cor_laranja}{mensagem}{cor_reset}")

                    hora_chamada = pd.to_datetime(horas_do_dia, dayfirst=True)
                    verificar_chamadas_consecutivas(cruzamentos_mais, hora_chamada)

                        
                # Função para emitir alerta de cruzamento voltando
                def cruzamentos_menos(i, real_load_speed, projetado_menos_20, horas_do_dia):
                    
                    # Código ANSI para cor azul
                    cor_azul = "\033[96m"
                    cor_laranja = "\033[92m"  # Laranja é representado pela cor 93 no código ANSI
                    # Código ANSI para resetar a cor para a cor padrão
                    cor_reset = "\033[0m"
                    # Mensagem de alerta com a cor azul
                    mensagem = f'             {i + 1} - {horas_do_dia} - Real Load Speed: {real_load_speed} está abaixo da reta de -20 {projetado_menos_20}'
                    print(f"{cor_azul}{mensagem}{cor_reset}")

                    hora_chamada = pd.to_datetime(horas_do_dia, dayfirst=True)
                    verificar_chamadas_consecutivas(cruzamentos_menos, hora_chamada)
                        
                # Função para emitir alerta de cruzamento voltando
                def cruzamentos_normais(i, real_load_speed, projetado_menos_20, projetado_20, horas_do_dia):

                    # Código ANSI para cor azul
                    cor_verde = "\033[92m"
                    # Código ANSI para resetar a cor para a cor padrão
                    cor_reset = "\033[0m"
                    # Mensagem de alerta com a cor azul
                    mensagem = f'             {i + 1} - {horas_do_dia} - Real Load Speed: {real_load_speed} está normalizado entre {projetado_20} e {projetado_menos_20}'
                    print(f"{cor_verde}{mensagem}{cor_reset}")

                    hora_chamada = pd.to_datetime(horas_do_dia, dayfirst=True)
                    verificar_chamadas_consecutivas(cruzamentos_normais, hora_chamada)

                        
                for i, (real_load_speed, projetado_menos_20, projetado_20) in enumerate(zip(valores_reais_load_speed, valores_projetados_menos_20_pot_ativa, valores_projetados_20_pot_ativa)):
                    if real_load_speed > projetado_20:
                        cruzamentos_mais(i, real_load_speed, projetado_20, horas_do_dia[i])
                    elif real_load_speed < projetado_menos_20:
                        cruzamentos_menos(i, real_load_speed, projetado_menos_20, horas_do_dia[i])
                    elif real_load_speed >= projetado_menos_20 and real_load_speed <= projetado_20:
            #            cruzamentos_normais(i, real_load_speed, projetado_menos_20, projetado_20, horas_do_dia[i])
                        pass
                    
                # Plote os gráficos para ambos os tipos de valores
                plt.figure(figsize=(10, 6))

                plt.axhline(y=92, color='red', linestyle='--', label='Limite 92')
                plt.axhline(y=80, color='orange', linestyle='--', label='Limite 80')

                plt.plot(horas_do_dia_grafico, valores_reais_load_speed, label='Load Speed Real', color='blue') 
                # plt.plot(horas_do_dia_grafico, valores_reais_pot_ativa, label='Valores Reais Potência Ativa', color='blue')
                plt.plot(horas_do_dia_grafico, valores_projetados_pot_ativa, label=f'Load Speed previsto', color='green', linestyle='dashed')
                plt.plot(horas_do_dia_grafico, valores_ratio_pot_ativa, label=f'indice', color='orange', linestyle='dashed')
                plt.scatter([horas_do_dia_grafico[i] for i in indices_pontos_acima_1_5], [valores_ratio_pot_ativa[i] for i in indices_pontos_acima_1_5], color='black')        
        #        plt.plot(horas_do_dia_grafico, valores_projetados_20_pot_ativa, label=f'Potência Ativa ({coeficiente}) + 20%', color='green', linestyle='dashed')
        #        plt.plot(horas_do_dia_grafico, valores_projetados_menos_20_pot_ativa, label=f'Potência Ativa ({coeficiente}) - 20%', color='orange', linestyle='dashed')

                plt.xlabel('Hora')
                plt.ylabel('Valores')
                plt.title(f'Equipamento - {cod_equipamento} Valores Load Speed real e previsto ({data_inicio})')
                plt.grid(True)
                plt.legend()
            # Exiba o gráfico sem bloquear a execução
        #    plt.show(block=False)
            plt.show()

                    
        except Exception as error:
            print('\n----------------------------------------------------------------------------------------------------------------\n')
            print(f"Erro ao processar equipamento {cod_equipamento}: {error}")

        # Atualizar o arquivo de log após processar um equipamento
        if cod_equipamento > ultimo_cod_equipamento_processado:
            with open(caminho_arquivo_log, "w") as arquivo_log:
                arquivo_log.write(str(cod_equipamento))

    # Zerar a contagem ao finalizar o processamento de todos os equipamentos
    primeiro_cod_equipamento = cod_equipamentos[0] if cod_equipamentos else "0"
    with open(caminho_arquivo_log, "w") as arquivo_log:
        arquivo_log.write(str(primeiro_cod_equipamento))

except Exception as error:
    print("Erro ao conectar ao banco de dados ou durante a execução:", error)

