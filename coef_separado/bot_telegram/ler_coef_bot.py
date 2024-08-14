import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error
from datetime import datetime, timedelta, time
import os
import csv
from scipy import stats
import numpy as np 
#import statsmodels.api as sm
from   sklearn.metrics import r2_score
import seaborn as sns



# Cria uma conexão com o banco de dados
cnx = mysql.connector.connect(
  host="192.168.15.104",
  user="root",
  password="gridbancoteste",
  database="sup_geral"
)

dados = {
    'host': '192.168.15.104',
    'user': 'root',
    'password': 'gridbancoteste',
    'database': 'sup_geral'
}
conexao_string = f"mysql+mysqlconnector://{dados['user']}:{dados['password']}@{dados['host']}/{dados['database']}"
connection = create_engine(conexao_string)

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
    'log_leituras_2023_04',
    'log_leituras_2023_05',
    'log_leituras_2023_06',
    'log_leituras_2023_07',
    'log_leituras_2023_08'
]

cod_campo_especificados = ['3', '114', '120']

# Cria um cursor
cursor = cnx.cursor()

inicio = datetime.now()
print(inicio)


# Executa uma consulta SQL para obter todos os cod_equipamentos únicos da última tabela
query_equipamentos = "SELECT DISTINCT cod_equipamento FROM log_leituras_2023_08 ORDER BY cod_equipamento"
cursor.execute(query_equipamentos)
# Busca todos os resultados
resultados_equipamentos = cursor.fetchall()
# Converte os resultados em uma lista
cod_equipamentos = [str(resultado[0]) for resultado in resultados_equipamentos]


# Caminho para o diretório e o arquivo CSV
arquivo_coeficiente = "coeficiente_geradores/"
caminho_arquivo_coeficiente = f"{arquivo_coeficiente}coeficiente_geradores.csv"


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

        if diferenca_minutos <= 5 or diferenca_minutos <= 10:
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
        mensagem = f'             {i + 1} - {horas_do_dia} - Load Speed: {real_load_speed} está acima de {max_aceitavel_min}% (Valor previsto: {projetado_20})'
        
    elif real_load_speed >= min_aceitavel_min and real_load_speed <= min_aceitavel_max:
        cor_mensagem = cor_laranja
        mensagem = f'             {i + 1} - {horas_do_dia} - Load Speed: {real_load_speed} está acima de {min_aceitavel_min}% (Valor previsto: {projetado_20})'
    else:
        cor_mensagem = cor_azul
        mensagem = f'             {i + 1} - {horas_do_dia} - Load Speed: {real_load_speed} está acima da reta prevista (Valor previsto: {projetado_20})'
    
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
    mensagem = f'             {i + 1} - {horas_do_dia} - Load Speed: {real_load_speed} está abaixo da reta prevista (Valor previsto: {projetado_menos_20})'
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
    mensagem = f'             {i + 1} - {horas_do_dia} - Load Speed: {real_load_speed} está normalizado entre os valores previstos de ({projetado_20} e {projetado_menos_20})'
    print(f"{cor_verde}{mensagem}{cor_reset}")

    hora_chamada = pd.to_datetime(horas_do_dia, dayfirst=True)
    verificar_chamadas_consecutivas(cruzamentos_normais, hora_chamada)


def verificar_e_obter_coeficiente(cod_equipamento):
    coeficiente_existente = 0.0
    acuracia_existente = 0.0
    data_existente = None

    # Verifica se o equipamento existe na tabela
    cursor.execute(f"SELECT * FROM coeficiente_geradores WHERE cod_equipamento = {cod_equipamento}")
    resultado = cursor.fetchone()

    if resultado is not None:
        coeficiente_existente = resultado[1]
        acuracia_existente = resultado[2]
        data_existente = resultado[3]
        print(f'Coeficiente Existente = {coeficiente_existente}, Acuracia = {acuracia_existente}')

    return coeficiente_existente

        
        
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

        # Excluir linhas com valores zero em ambas as colunas 'valor_cod_campo_3' e 'valor_cod_campo_114'
        df_pivoted = df_pivoted[(df_pivoted['valor_cod_campo_3'] != 0) & (df_pivoted['valor_cod_campo_114'] != 0)]

        # Filtrar o DataFrame para incluir apenas valores menores ou iguais em 'valor_cod_campo_114' em relação a 100
        df_pivoted = df_pivoted[df_pivoted['valor_cod_campo_114'] <= 100]
        
        df_pivoted = df_pivoted.fillna(0)


        print('\n----------------------------------------------------------------------------------------------------------------\n')
        print(f'equipamento: {cod_equipamento}')


        # Converter os valores de X e y para float
        X = df_pivoted['valor_cod_campo_3'].astype(float).values.reshape(-1, 1)  # Recursos de treinamento
        y = df_pivoted['valor_cod_campo_114'].astype(float)  # Alvo de previsão

        coeficiente = 0.0
        
        # Verificar e obter o coeficiente
        coeficiente_existente = verificar_e_obter_coeficiente(cod_equipamento)
        
        if coeficiente_existente == 0:
    
            print(f'Coeficiente encontrado e zero {coeficiente_existente}. pulando equipamento')
            continue
        
        coeficiente += coeficiente_existente
        
        
        data_inicio = '01-04-2023'
        data_fim = '28-08-2023'
        
        print('\n',f'- Equipamento: {cod_equipamento}','- Data -', data_inicio, '\n')

        # Converter a data para o formato adequado
        data_verificacao_str = datetime.strptime(data_inicio, '%d-%m-%Y').strftime('%Y-%m-%d')

        query_alarmes = f"SELECT * FROM log_alarmes WHERE cod_equipamento = {cod_equipamento} AND DATE(data_cadastro) = '{data_verificacao_str}'"
        df_alarmes = pd.read_sql_query(query_alarmes, connection)

        df_final = df_alarmes

        if not df_alarmes.empty:
            codigos_alarmes = df_alarmes['cod_alarme'].unique()
            codigos_alarmes_str = ', '.join(map(str, codigos_alarmes))
            query_nome_alarmes = f"SELECT codigo, nome FROM lista_alarmes WHERE codigo IN ({codigos_alarmes_str})"
            df_nome_alarmes = pd.read_sql_query(query_nome_alarmes, connection)
            df_final = pd.merge(df_alarmes, df_nome_alarmes, left_on='cod_alarme', right_on='codigo', how='inner')
                
    
            print(f"             Alarmes encontrados:\n")
            print(df_final[['data_cadastro','cod_alarme', 'nome']])
            print(f"\n")
    

        valores_reais_load_speed = [valor for data, valor in zip(df_pivoted['rounded_time'], y) if
                                    data.date() >= pd.to_datetime(data_inicio, dayfirst=True).date() and
                                    data.date() <= pd.to_datetime(data_fim, dayfirst=True).date()]

        valores_reais_pot_ativa = [valor for data, valor in zip(df_pivoted['rounded_time'], X) if
                                data.date() >= pd.to_datetime(data_inicio, dayfirst=True).date() and
                                data.date() <= pd.to_datetime(data_fim, dayfirst=True).date()]

        valores_projetados_pot_ativa = [valor * coeficiente for valor in valores_reais_pot_ativa]
        valores_projetados_10_pot_ativa = [valor * coeficiente * 1.1 for valor in valores_reais_pot_ativa]
        valores_projetados_menos_10_pot_ativa = [valor * coeficiente * 0.9 for valor in valores_reais_pot_ativa]
        valores_projetados_20_pot_ativa = [valor * coeficiente * 1.3 for valor in valores_reais_pot_ativa]
        valores_projetados_menos_20_pot_ativa = [valor * coeficiente * 0.8 for valor in valores_reais_pot_ativa]
        valores_projetados_80_pot_ativa = [valor * coeficiente * 1.8 for valor in valores_reais_pot_ativa]

        valores_ratio_pot_ativa = [projeto / real if real > 0 else 0 for real, projeto in zip(valores_projetados_pot_ativa, valores_reais_load_speed)]

        horas_do_dia = [data.strftime('%d-%m-%Y %H:%M') for data in df_pivoted['rounded_time'] if 
                        data.date() >= pd.to_datetime(data_inicio, dayfirst=True).date() and 
                        data.date() <= pd.to_datetime(data_fim, dayfirst=True).date()]

        # Crie uma lista de rótulos no formato "HH:MM" correspondentes aos valores
        horas_do_dia_grafico = [data.strftime('%d-%m %H:%M') for data in df_pivoted['rounded_time'] if
                                data.date() >= pd.to_datetime(data_inicio, dayfirst=True).date() and
                                data.date() <= pd.to_datetime(data_fim, dayfirst=True).date()]

        # Verificar se o alarme 381 foi acionado
        if 381 in df_final['cod_alarme'].values:
            print("Alarme de emergência acionado nas seguintes horas no dia", data_inicio, ":")
            for hora in df_final[df_final['cod_alarme'] == 381]['data_cadastro']:
                print(hora)
                pass

        # Função para arredondar a hora para o intervalo de 5 minutos mais próximo, caso contrário, para o intervalo de 10 minutos mais próximo
        def round_time(time_str):
            time = datetime.strptime(time_str, '%d-%m %H:%M')
            minute = (time.minute // 5) * 5
            if time.minute % 5 >= 2.5:
                minute += 5
            if minute > 59:  # Se o minuto arredondado exceder 59
                minute = (time.minute // 10) * 10  # Arredonde para o intervalo de 10 minutos mais próximo
                if time.minute % 10 >= 5:
                    minute += 10
            minute = min(59, minute)  # Garantir que o minuto nunca exceda 59
            time = time.replace(minute=minute)
            return time.strftime('%d-%m %H:%M')

        for i, (real_load_speed, projetado_menos_20, projetado_20, projetado) in enumerate(zip(valores_reais_load_speed, valores_projetados_menos_20_pot_ativa, valores_projetados_20_pot_ativa, valores_projetados_pot_ativa)):
            if real_load_speed > projetado_20:
            #    cruzamentos_mais(i, real_load_speed, projetado_20, horas_do_dia[i])
                pass
            elif real_load_speed < projetado_menos_20:
            #    cruzamentos_menos(i, real_load_speed, projetado_menos_20, horas_do_dia[i])
                pass
            elif real_load_speed >= projetado_menos_20 and real_load_speed <= projetado_20:
        #        cruzamentos_normais(i, real_load_speed, projetado_menos_20, projetado_20, horas_do_dia[i])
                pass
            
            # teste com load speed * coeficiente
            if real_load_speed > projetado:
                cruzamentos_mais(i, real_load_speed, projetado, horas_do_dia[i])
            if real_load_speed < projetado:
                cruzamentos_menos(i, real_load_speed, projetado, horas_do_dia[i])

        '''
        # Plote os gráficos para ambos os tipos de valores
        plt.figure(figsize=(10, 6)) 
        plt.axhline(y=100, color='red', linestyle='--')
        plt.axhline(y=90, color='red', linestyle='--')
        plt.axhline(y=80, color='orange', linestyle='--')

        plt.plot(horas_do_dia_grafico, valores_reais_load_speed, label='Load Speed Real', color='blue') 
        plt.plot(horas_do_dia_grafico, valores_projetados_pot_ativa, label=f'Load Speed previsto', color='green', linestyle='dashed')
        plt.plot(horas_do_dia_grafico, valores_projetados_80_pot_ativa, label=f'Razao limite 80%', color='orange', linestyle='dashed')

        # Adicionar pontos vermelhos para as horas em que o alarme foi acionado
        if 381 in df_final['cod_alarme'].values:
            alarm_times = df_final[df_final['cod_alarme'] == 381]['data_cadastro'].apply(lambda x: x.strftime('%d-%m %H:%M')).tolist()
            alarm_times = [round_time(time) for time in alarm_times]  # Arredondar as horas do alarme
            alarm_times = [time for time in alarm_times if time in horas_do_dia_grafico]  # Verificar se a hora do alarme está em horas_do_dia_grafico
            alarm_vals = [valores_reais_load_speed[horas_do_dia_grafico.index(time)] for time in alarm_times]
            plt.scatter(alarm_times, alarm_vals, color='red')

    
        plt.xlabel('Hora')
        plt.ylabel('Valores')
        plt.title(f'Equipamento - {cod_equipamento} Valores Load Speed real e previsto ({data_inicio})')
        plt.grid(True)
        plt.legend()
        plt.show()
        '''
    except Exception as error:
        print('\n----------------------------------------------------------------------------------------------------------------\n')
        print(f"Erro ao processar equipamento {cod_equipamento}: {error}")

        
final = datetime.now()
total = final - inicio
print('\ntempo total de processamento',total)

cursor.close()
cnx.close()

