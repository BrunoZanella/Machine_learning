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
from collections import deque
import time


inicio = datetime.now()
print(inicio)


dados = {
    'host': '192.168.15.104',
    'user': 'root',
    'password': 'gridbancoteste',
    'database': 'sup_geral'
}
conexao_string = f"mysql+mysqlconnector://{dados['user']}:{dados['password']}@{dados['host']}/{dados['database']}"
connection = create_engine(conexao_string)


# Cria uma conexão com o banco de dados
cnx = mysql.connector.connect(
  host="192.168.15.104",
  user="root",
  password="gridbancoteste",
  database="sup_geral"
)
# Cria um cursor
cursor = cnx.cursor()

# Verifica se a tabela 'leituras_consecutivas' existe, se não, cria
cursor.execute("""
CREATE TABLE IF NOT EXISTS leituras_consecutivas (
  cod_equipamento INT,
  cod_campo INT,
  valor_1 FLOAT,
  valor_2 FLOAT,
  valor_3 FLOAT,
  valor_4 FLOAT,
  valor_5 FLOAT,
  PRIMARY KEY (cod_equipamento, cod_campo)
)
""")

tabelas = 'leituras'

# Executa uma consulta SQL para obter todos os cod_equipamentos únicos da última tabela
query_equipamentos = f"SELECT DISTINCT cod_equipamento FROM {tabelas} ORDER BY cod_equipamento"
cursor.execute(query_equipamentos)

# Busca todos os resultados
resultados_equipamentos = cursor.fetchall()

# Converte os resultados em uma lista
cod_equipamentos = [str(resultado[0]) for resultado in resultados_equipamentos]

#cod_equipamentos = ['2243','2268']

# Calcular o total de equipamentos encontrados
total_equipamentos = len(cod_equipamentos)

# Imprimir o resultado
print(f"Total de {total_equipamentos} equipamentos na tabela {tabelas}")

cod_campo_especificados = ['3', '114']


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

def fazer_previsao(valores_atuais, coeficiente):

    coeficiente_existente = verificar_e_obter_coeficiente(cod_equipamento_resultado)
#    previsoes = [(valor * coeficiente_existente + 10) for valor in valores_atuais]
    previsoes = [(valor * coeficiente_existente) for valor in valores_atuais]
    previsoes = [round(valor, 2) for valor in previsoes]

    return previsoes


try:
    while True:  # Loop infinito
        for cod_equipamento in cod_equipamentos:
            # Cria um dicionário para armazenar as listas para cada cod_campo
            valores = {cod: [0, 0, 0, 0, 0] for cod in cod_campo_especificados}
            try:
                query = f"SELECT data_cadastro, valor, cod_campo FROM {tabelas} WHERE cod_equipamento = {cod_equipamento} AND cod_campo IN ({', '.join(cod_campo_especificados)})"
                cursor.execute(query)
                resultados = cursor.fetchall()

                df = pd.DataFrame(resultados, columns=['data_cadastro', 'valor', 'cod_campo'])

                df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])

                df['valor'] = pd.to_numeric(df['valor'], errors='coerce')

                # Adiciona os valores à lista correspondente e imprime os três últimos valores para cada cod_campo
                for cod in cod_campo_especificados:
                    valores_cod_campo = df[df['cod_campo'] == int(cod)]['valor'].values
                    # Desloca os valores na lista
                    valores[cod] = list(valores_cod_campo[-5:])[::-1] + valores[cod][:5-len(valores_cod_campo[-5:])] # para prever 5 valores

                    # Atualiza a tabela 'leituras_consecutivas' com os novos valores
                    cursor.execute(f"""
                    INSERT INTO leituras_consecutivas (cod_equipamento, cod_campo, valor_1, valor_2, valor_3, valor_4, valor_5)
                    VALUES ({cod_equipamento}, {cod}, {valores[cod][4]}, {valores[cod][3]}, {valores[cod][2]}, {valores[cod][1]}, {valores[cod][0]})
                    ON DUPLICATE KEY UPDATE
                    valor_1 = leituras_consecutivas.valor_2,
                    valor_2 = leituras_consecutivas.valor_3,
                    valor_3 = leituras_consecutivas.valor_4,
                    valor_4 = leituras_consecutivas.valor_5,
                    valor_5 = {valores[cod][0]}
                    """)
                    cnx.commit()
    
                # Executa uma consulta SQL para obter todos os valores da tabela 'leituras_consecutivas'
                query_valores = f"SELECT cod_equipamento, cod_campo, valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = {cod_equipamento} ORDER BY cod_equipamento, cod_campo"
                cursor.execute(query_valores)

                # Busca todos os resultados
                resultados_valores = cursor.fetchall()

                # Imprime os resultados
                for resultado in resultados_valores:

                    coeficiente = 0.0

                    # Desempacota os valores do resultado
                    cod_equipamento_resultado, _, valor_1, valor_2, valor_3, valor_4, valor_5 = resultado  # Use _ para ignorar o valor de cod_campo

                    print('\n----------------------------------------------------------------------------------------------------------------\n')
                    print(f'cod_equipamento: {cod_equipamento_resultado}, valores: {valor_1}, {valor_2}, {valor_3}, {valor_4}, {valor_5}')
                    
                    # Fazer previsões
                    previsoes = fazer_previsao([valor_1, valor_2, valor_3, valor_4, valor_5], coeficiente)
                    print(f'Previsões: {previsoes}')


            except Exception as e:
                print(f"Erro ao processar o equipamento {cod_equipamento}: {str(e)}")

        print('\n****************************************************************************************************************\n')

        time.sleep(10)  # Pausa por 10 segundos antes de atualizar os valores novamente

except KeyboardInterrupt:
    print('Interrompido pelo usuário')

finally:
    # Fecha a conexão com o banco de dados após o término do loop
    cnx.close()
    cursor.close()
    print('encerrando o programa')
