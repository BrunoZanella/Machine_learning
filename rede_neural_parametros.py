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
import streamlit as st
from io import BytesIO

from datetime import datetime, timedelta, time

from sklearn.metrics import r2_score
import statsmodels.api as sm
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn.tree import DecisionTreeRegressor
from sklearn.linear_model import LassoCV, ElasticNetCV
import re
from difflib import SequenceMatcher
from sklearn.preprocessing import StandardScaler
from sklearn import linear_model
from sklearn.linear_model import RidgeCV
from sklearn.preprocessing import scale
from sklearn.svm import SVR
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import PolynomialFeatures
from mpl_toolkits.mplot3d import Axes3D
from matplotlib import cm
import os

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
#import pingouin as pg
from sklearn.multioutput import MultiOutputRegressor
import joblib
import time


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

cod_campo_especificados = ['3','6','7','8','9','10', '11,' '114', '21','76','25','20','77']

# total_equipamentos_com_valores = 0
# equipamentos_com_valores = []

# for cod_equipamento in cod_equipamentos_validos:
#     tem_valores = False
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
#             equipamentos_com_valores.append(cod_equipamento)
#             tem_valores = True
#             break

#     if tem_valores:
#         total_equipamentos_com_valores += 1

# print(f"Total de equipamentos válidos com valores nos campos especificados: {total_equipamentos_com_valores}")


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

equipamentos_validados = [2363, 2808]

print('equipamentos_validados',equipamentos_validados)

def formatar_valores(df):
    """
    Formata todas as colunas numéricas no DataFrame para mostrar apenas duas casas decimais.
    """
    num_cols = df.select_dtypes(include=['float64']).columns
    df[num_cols] = df[num_cols].round(2)
    return df


# Dicionário global para armazenar os parâmetros min e max dos motores
parametros_min_max_motores = {}

def carregar_parametros_min_max():
    # Buscar dados da tabela parametros_min_max_motores e armazenar em um dicionário
    global parametros_min_max_motores
    cursor.execute("SELECT motor, parametro, min_valor, max_valor FROM machine_learning.parametros_min_max_motores")
    resultados = cursor.fetchall()
    
    # Armazenar os resultados em um dicionário estruturado
    for linha in resultados:
        motor, parametro, min_valor, max_valor = linha
        if motor not in parametros_min_max_motores:
            parametros_min_max_motores[motor] = {}
        parametros_min_max_motores[motor][parametro] = {'min': min_valor, 'max': max_valor}
    
    print(f"Parâmetros min/max carregados para {len(parametros_min_max_motores)} motores.")



def verificar_alertas(df, marca, motor):
    # Se a tabela de parâmetros ainda não foi carregada, carregar agora
    if not parametros_min_max_motores:
        carregar_parametros_min_max()

    # Limpar o nome do motor para comparação (remover espaços e caracteres especiais)
    motor_limpo = re.sub(r'\s+', '', motor.lower())  # Remove espaços e coloca em minúsculas
    marca_limpa = marca.lower()

    # Variáveis para guardar parâmetros encontrados
    parametros_motor = None

    # Função para verificar se uma substring existe em uma string de forma flexível
    def comparar_nomes_motor(motor_tabela, motor_busca):
        motor_tabela_limpo = re.sub(r'\s+', '', motor_tabela.lower())
        return motor_busca in motor_tabela_limpo or motor_tabela_limpo in motor_busca

    # Procurar parâmetros baseados na marca e parte do motor (usando substrings mais flexíveis)
    for chave_motor in parametros_min_max_motores.keys():
        chave_motor_limpa = re.sub(r'\s+', '', chave_motor.lower())  # Limpa a chave do motor da tabela

        # Verifica se a marca e parte do motor estão presentes de forma flexível
        if (marca_limpa in chave_motor_limpa) and comparar_nomes_motor(chave_motor, motor_limpo):
            parametros_motor = parametros_min_max_motores[chave_motor]
            print(f"Parâmetros encontrados para {chave_motor}")
            break

    # Se não encontrar os parâmetros, avisar
    if not parametros_motor:
        print(f"Parâmetros não encontrados para motor {motor} da marca {marca}")
        return df


    # Normalizar as chaves para remover "(Bar)" ou outros elementos entre parênteses
    parametros_motor_normalizado = {re.sub(r'\s*\(.*?\)\s*', '', chave): valor for chave, valor in parametros_motor.items()}

    # Substituir valores padrão por valores da tabela

    min_load_speed = parametros_motor_normalizado.get('Load Speed', {}).get('min', 5)
    max_load_speed = parametros_motor_normalizado.get('Load Speed', {}).get('max', 80)

    min_temp_ar_admissao = parametros_motor_normalizado.get('Temperatura do ar de admissão', {}).get('min', 20)
    max_temp_ar_admissao = parametros_motor_normalizado.get('Temperatura do ar de admissão', {}).get('max', 100)

    min_pressao_admissao = parametros_motor_normalizado.get('Pressão de admissão', {}).get('min', 0)
    max_pressao_admissao = parametros_motor_normalizado.get('Pressão de admissão', {}).get('max', 6)

    min_potencia_ativa = parametros_motor_normalizado.get('Potência Ativa', {}).get('min', 20)
    max_potencia_ativa = parametros_motor_normalizado.get('Potência Ativa', {}).get('max', 400)

    min_pressao_oleo = parametros_motor_normalizado.get('Pressão do Óleo', {}).get('min', 3.5)
    max_pressao_oleo = parametros_motor_normalizado.get('Pressão do Óleo', {}).get('max', 5.0)

    min_rpm = parametros_motor_normalizado.get('RPM', {}).get('min', 1798)
    max_rpm = parametros_motor_normalizado.get('RPM', {}).get('max', 1802)

    min_temp_agua = parametros_motor_normalizado.get('Temperatura da água', {}).get('min', 103)
    max_temp_agua = parametros_motor_normalizado.get('Temperatura da água', {}).get('max', 103)
    alerta_temp_agua = parametros_motor_normalizado.get('Temperatura da água', {}).get('alerta', 90)
    
    print('parametros do equipamento', cod_equipamento, '\nmin_load_speed', min_load_speed, 'max_load_speed', max_load_speed,
          '\nmin_potencia_ativa', min_potencia_ativa, 'max_potencia_ativa', max_potencia_ativa,
          '\nmin_pressao_oleo', min_pressao_oleo, 'max_pressao_oleo', max_pressao_oleo,
          '\nmin_rpm', min_rpm, 'max_rpm', max_rpm,
          '\nmax_temp_agua', max_temp_agua, 'min_temp_agua', min_temp_agua,
          '\nmin_temp_ar_admissao', min_temp_ar_admissao, 'max_temp_ar_admissao', max_temp_ar_admissao,
          '\nmin_pressao_admissao', min_pressao_admissao, 'max_pressao_admissao', max_pressao_admissao)
    
    # Adiciona a coluna Alerta com valor padrão 0
    df['Alerta'] = 0
    
    # Implementação original de verificar_alertas, agora usando os valores min/max do dicionário
    df['Alerta_Equipamento'] = np.where(df['Prev_Load Speed'] == 0, 'Equipamento Desligado', 'Equipamento Ligado')

    # Definindo alertas para Pressão do Óleo
    df['Alerta_Pressao_Oleo'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_Pressao_do_Oleo'] < min_pressao_oleo, 'Alerta: Crítica', 
                np.where(df['Prev_Pressao_do_Oleo'] <= max_pressao_oleo, 'Pressão Normal', 'Alerta: Pressão Alta'))
    )

    df['Alerta_Pressao_Oleo_real'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_Pressao_do_Oleo'] < min_pressao_oleo, 'Alerta: Crítica', 
                np.where(df['Real_Pressao_do_Oleo'] <= max_pressao_oleo, 'Pressão Normal', 'Alerta: Pressão Alta'))
    )
    
    # Definindo alertas para RPM
    df['Alerta_RPM'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_RPM'] < min_rpm, 'Alerta: RPM Baixo', 
                np.where(df['Prev_RPM'] <= max_rpm, 'RPM Normal', 'Alerta: RPM Alto'))
    )

    df['Alerta_RPM_real'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_RPM'] < min_rpm, 'Alerta: RPM Baixo', 
                np.where(df['Real_RPM'] <= max_rpm, 'RPM Normal', 'Alerta: RPM Alto'))
    )
    
    # Definindo alertas para Temperatura da Água
    df['Alerta_Temperatura_Agua'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_Temperatura_Agua'] < min_temp_agua, 'Alerta: Temperatura Baixa', 
                np.where(df['Prev_Temperatura_Agua'] <= max_temp_agua, 'Temperatura Normal', 'Alerta: Temperatura Alta'))
    )

    df['Alerta_Temperatura_Agua_real'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_Temperatura_Agua'] < min_temp_agua, 'Alerta: Temperatura Baixa', 
                np.where(df['Real_Temperatura_Agua'] <= max_temp_agua, 'Temperatura Normal', 'Alerta: Temperatura Alta'))
    )
    
    # Definindo alertas para Load Speed
    df['Alerta_Load_Speed'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_Load Speed'] < min_load_speed, 'Alerta: Load Speed Baixo', 
                np.where(df['Prev_Load Speed'] <= max_load_speed, 'Load Speed Normal', 'Alerta: Load Speed Alto'))
    )

    df['Alerta_Load_Speed_real'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_Load_Speed'] < min_load_speed, 'Alerta: Load Speed Baixo', 
                np.where(df['Real_Load_Speed'] <= max_load_speed, 'Load Speed Normal', 'Alerta: Load Speed Alto'))
    )

    # Definindo alertas para Potência Ativa
    df['Alerta_Potencia_Ativa'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_Potencia_Ativa'] < min_potencia_ativa, 'Alerta: Potência Ativa Baixa', 
                np.where(df['Prev_Potencia_Ativa'] <= max_potencia_ativa, 'Potência Ativa Normal', 'Alerta: Potência Ativa Alta'))
    )

    df['Alerta_Potencia_Ativa_real'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_Potencia_Ativa'] < min_potencia_ativa, 'Alerta: Potência Ativa Baixa', 
                np.where(df['Real_Potencia_Ativa'] <= max_potencia_ativa, 'Potência Ativa Normal', 'Alerta: Potência Ativa Alta'))
    )
    
    # Definindo alertas para Temperatura do Ar de Admissão
    df['Alerta_Temperatura_Ar_Admissao'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_Temperatura_do_ar_de_admissao'] < min_temp_ar_admissao, 'Alerta: Temperatura Ar Baixa', 
                np.where(df['Prev_Temperatura_do_ar_de_admissao'] <= max_temp_ar_admissao, 'Temperatura Ar Normal', 'Alerta: Temperatura Ar Alta'))
    )

    df['Alerta_Temperatura_Ar_Admissao_real'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_Temperatura_do_ar_de_admissao'] < min_temp_ar_admissao, 'Alerta: Temperatura Ar Baixa', 
                np.where(df['Real_Temperatura_do_ar_de_admissao'] <= max_temp_ar_admissao, 'Temperatura Ar Normal', 'Alerta: Temperatura Ar Alta'))
    )
    # Definindo alertas para Pressão de Admissão
    df['Alerta_Pressao_Admissao'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_Pressao_de_admissao'] < min_pressao_admissao, 'Alerta: Pressão Admissão Baixa', 
                np.where(df['Prev_Pressao_de_admissao'] <= max_pressao_admissao, 'Pressão Admissão Normal', 'Alerta: Pressão Admissão Alta'))
    )

    df['Alerta_Pressao_Admissao_real'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_Pressao_de_admissao'] < min_pressao_admissao, 'Alerta: Pressão Admissão Baixa', 
                np.where(df['Real_Pressao_de_admissao'] <= max_pressao_admissao, 'Pressão Admissão Normal', 'Alerta: Pressão Admissão Alta'))
    )

    # Definindo se o equipamento está em alerta geral (se todos os parâmetros estiverem em alerta)
    colunas_alerta_previsto = ['Alerta_Pressao_Oleo', 'Alerta_RPM', 'Alerta_Temperatura_Agua', 
                               'Alerta_Load_Speed', 'Alerta_Potencia_Ativa', 
                               'Alerta_Temperatura_Ar_Admissao', 'Alerta_Pressao_Admissao']
    colunas_alerta_real = ['Alerta_Pressao_Oleo_real', 'Alerta_RPM_real', 'Alerta_Temperatura_Agua_real', 
                           'Alerta_Load_Speed_real', 'Alerta_Potencia_Ativa_real', 
                           'Alerta_Temperatura_Ar_Admissao_real', 'Alerta_Pressao_Admissao_real']

    df['Alerta'] = np.where(
        df['Prev_Load Speed'] == 0,
        'Equipamento Desligado',
        np.where(df[colunas_alerta_previsto].apply(lambda row: all('Alerta:' in val for val in row), axis=1), 
                 'Alerta Geral: Crítica', 
                 np.where(df[colunas_alerta_real].apply(lambda row: all('Alerta:' in val for val in row), axis=1), 
                          'Alerta Geral: Crítica', 'Sem Alerta Geral'))
    )

    def alerta_predominante(alertas):
        # Função para determinar o alerta que aparece mais vezes
        return max(set(alertas), key=alertas.count)

    # Criar uma nova DataFrame com alertas predominantes e valores finais
    df_alertas = pd.DataFrame()

    # Calcular os alertas predominantes para cada parâmetro
    df_alertas['Alerta_Load_Speed'] = alerta_predominante(df['Alerta_Load_Speed'].tolist())
    df_alertas['Ultimo_Load_Speed'] = df['Alerta_Load_Speed'].iloc[-1]

    df_alertas['Alerta_Potencia_Ativa'] = alerta_predominante(df['Alerta_Potencia_Ativa'].tolist())
    df_alertas['Ultimo_Potencia_Ativa'] = df['Alerta_Potencia_Ativa'].iloc[-1]

    df_alertas['Alerta_RPM'] = alerta_predominante(df['Alerta_RPM'].tolist())
    df_alertas['Ultimo_RPM'] = df['Alerta_RPM'].iloc[-1]

    df_alertas['Alerta_Temperatura_Agua'] = alerta_predominante(df['Alerta_Temperatura_Agua'].tolist())
    df_alertas['Ultimo_Temperatura_Agua'] = df['Alerta_Temperatura_Agua'].iloc[-1]

    df_alertas['Alerta_Pressao_Oleo'] = alerta_predominante(df['Alerta_Pressao_Oleo'].tolist())
    df_alertas['Ultimo_Pressao_Oleo'] = df['Alerta_Pressao_Oleo'].iloc[-1]

    df_alertas['Alerta_Temperatura_Ar_Admissao'] = alerta_predominante(df['Alerta_Temperatura_Ar_Admissao'].tolist())
    df_alertas['Ultimo_Temperatura_Ar_Admissao'] = df['Alerta_Temperatura_Ar_Admissao'].iloc[-1]

    df_alertas['Alerta_Pressao_Admissao'] = alerta_predominante(df['Alerta_Pressao_Admissao'].tolist())
    df_alertas['Ultimo_Pressao_Admissao'] = df['Alerta_Pressao_Admissao'].iloc[-1]

    # Definindo alertas para Corrente (L1, L2, L3) e Tensão (L1-L2, L2-L3, L3-L1)
    def alerta_corrente_tensao(coluna_corrente, coluna_tensao, df):
        # Alertas para Correntes
        df[f'Alerta_{coluna_corrente}_L1'] = np.where(
            abs(df[f'Prev_{coluna_corrente}_L1'] - df[f'Prev_{coluna_corrente}_L2']) > 5, 'Alerta: Corrente fora do padrão',
            np.where(abs(df[f'Prev_{coluna_corrente}_L1'] - df[f'Prev_{coluna_corrente}_L3']) > 5, 'Alerta: Corrente fora do padrão', 'Corrente Normal')
        )
        df[f'Alerta_{coluna_corrente}_L2'] = np.where(
            abs(df[f'Prev_{coluna_corrente}_L2'] - df[f'Prev_{coluna_corrente}_L1']) > 5, 'Alerta: Corrente fora do padrão',
            np.where(abs(df[f'Prev_{coluna_corrente}_L2'] - df[f'Prev_{coluna_corrente}_L3']) > 5, 'Alerta: Corrente fora do padrão', 'Corrente Normal')
        )
        df[f'Alerta_{coluna_corrente}_L3'] = np.where(
            abs(df[f'Prev_{coluna_corrente}_L3'] - df[f'Prev_{coluna_corrente}_L1']) > 5, 'Alerta: Corrente fora do padrão',
            np.where(abs(df[f'Prev_{coluna_corrente}_L3'] - df[f'Prev_{coluna_corrente}_L2']) > 5, 'Alerta: Corrente fora do padrão', 'Corrente Normal')
        )

        # Alertas para Tensões
        df[f'Alerta_{coluna_tensao}_L1_L2'] = np.where(
            abs(df[f'Prev_{coluna_tensao}_L1_L2'] - df[f'Prev_{coluna_tensao}_L2_L3']) > 5, 'Alerta: Tensão fora do padrão',
            np.where(abs(df[f'Prev_{coluna_tensao}_L1_L2'] - df[f'Prev_{coluna_tensao}_L3_L1']) > 5, 'Alerta: Tensão fora do padrão', 'Tensão Normal')
        )
        df[f'Alerta_{coluna_tensao}_L2_L3'] = np.where(
            abs(df[f'Prev_{coluna_tensao}_L2_L3'] - df[f'Prev_{coluna_tensao}_L1_L2']) > 5, 'Alerta: Tensão fora do padrão',
            np.where(abs(df[f'Prev_{coluna_tensao}_L2_L3'] - df[f'Prev_{coluna_tensao}_L3_L1']) > 5, 'Alerta: Tensão fora do padrão', 'Tensão Normal')
        )
        df[f'Alerta_{coluna_tensao}_L3_L1'] = np.where(
            abs(df[f'Prev_{coluna_tensao}_L3_L1'] - df[f'Prev_{coluna_tensao}_L1_L2']) > 5, 'Alerta: Tensão fora do padrão',
            np.where(abs(df[f'Prev_{coluna_tensao}_L3_L1'] - df[f'Prev_{coluna_tensao}_L2_L3']) > 5, 'Alerta: Tensão fora do padrão', 'Tensão Normal')
        )

    alerta_corrente_tensao('Corrente', 'Tensao', df)

    return df



# Lista para armazenar os dados dos equipamentos
equipamentos_ativos = []

import re


def limpar_motor(marca, motor):
    """
    Limpa e padroniza o nome do motor e marca para combiná-lo com a tabela de parâmetros.
    Exemplo: Volvo Penta VOLVO TAD 1344GE -> Volvo TAD 13
    """
    # Ajuste condicional para a marca
    if "Volvo Penta" in marca:
        marca_limpa = "Volvo"  # Sempre padronizar como "Volvo"
    else:
        marca_limpa = marca.strip().upper()  # Mantém a marca em maiúsculas para os outros casos

    # Limpar o nome do motor de espaços em branco extras, hifens e colocar em maiúsculas
    motor_limpo = motor.strip().upper()

    # Remover duplicações de marca no motor (ex: Volvo Penta VOLVO TAD 1344 -> TAD 1344)
    motor_limpo = motor_limpo.replace(marca_limpa.upper(), "").strip()

    # Remover hífen ou outros caracteres especiais entre a marca e o motor
    motor_limpo = re.sub(r'[-]', ' ', motor_limpo).strip()

    # Remover caracteres extras após os primeiros dois dígitos principais do código do motor (ex: TAD1344GE -> TAD13)
    motor_limpo = re.sub(r'(\d{2})\d*[A-Za-z]*$', r'\1', motor_limpo)

    # Adicionar espaço entre letras e números (TAD13 -> TAD 13)
    motor_limpo = re.sub(r'([A-Za-z]+)(\d+)', r'\1 \2', motor_limpo)

    # Remover caracteres especiais e espaços em branco extras
    motor_limpo = re.sub(r'\s+', ' ', motor_limpo).strip()

    # Corrigir padrões específicos para Scania
    if 'SCANIA' in marca_limpa.upper():
        # Padrão SCANIA 13 → SCANIA DC 13
        if re.match(r'^13$', motor_limpo):
            motor_limpo = 'DC 13'
        # Padrão SCANIA SACANIA DC 1253 A → SCANIA DC 12
        elif re.match(r'^SACANIA DC 1253 A$', motor_limpo) or re.match(r'^DC 1253 A$', motor_limpo):
            motor_limpo = 'DC 12'
        # Padrão SCANIA DC 13072 A → SCANIA DC 13
        elif re.match(r'^DC 13\d{2}', motor_limpo):
            motor_limpo = 'DC 13'
        # Padrão SCANIA DC 13 07 → SCANIA DC 13
        elif re.match(r'^DC 13 \d{2}', motor_limpo):
            motor_limpo = 'DC 13'
    
    # Corrigir padrões específicos para Volvo
    if 'VOLVO' in marca_limpa.upper():
        # Padrão Volvo TAD 1345GEB-B → Volvo TAD 13
        if re.match(r'^TAD 13', motor_limpo):
            motor_limpo = 'TAD 13'
        # Padrão Volvo TAD 1641 GE-8 → Volvo TAD 16
        elif re.match(r'^TAD 16', motor_limpo):
            motor_limpo = 'TAD 16'

    # Combinar a marca e o motor de maneira padronizada
    motor_padronizado = f"{marca_limpa} {motor_limpo}".strip()
    print('motor_padronizado:', motor_padronizado)

    return motor_padronizado


def carregar_equipamentos_ativos():
    """
    Função para carregar os equipamentos ativos do banco de dados e armazenar na lista `equipamentos_ativos`.
    """
    try:
        # Realiza a consulta para buscar os equipamentos ativos
        cursor.execute("""
                    SELECT 
                        e.codigo AS codigo_equipamento, 
                        e.cod_usina, 
                        e.motor, 
                        COALESCE(
                            CASE 
                                WHEN e.motor = 'N/I' THEN '' -- Caso o motor seja "N/I", não preenchermos a marca
                                WHEN e.motor LIKE '%TAD%' THEN 'Volvo'  -- Aqui já padronizamos como "Volvo"
                                WHEN e.motor LIKE '%TWD%' THEN 'Volvo'  -- Aqui também padronizamos como "Volvo"
                                WHEN e.motor LIKE '%DC%' THEN 'Scania'
                                WHEN e.motor LIKE '%NEF%' THEN 'FPT'
                                WHEN e.motor LIKE '%MWM%' THEN 'MWM'
                                WHEN e.motor LIKE '%PERKINS%' THEN 'Perkins'
                                ELSE li.descricao  -- Utiliza a descrição da lista, se disponível
                            END, 
                            '' -- Se não houver correspondência, deixamos em branco
                        ) AS marca
                    FROM 
                        sup_geral.equipamentos e
                    JOIN 
                        sup_geral.usinas u ON e.cod_usina = u.codigo
                    LEFT JOIN 
                        sup_geral.leituras lei ON e.codigo = lei.cod_equipamento 
                        AND lei.cod_campo = 397
                    LEFT JOIN 
                        sup_geral.lista_motores li ON lei.valor = li.valor
                    WHERE 
                        u.ativo = 1
                        AND e.motor IS NOT NULL
                        AND e.ativo = 1
                    GROUP BY 
                        e.codigo, e.cod_usina, e.motor, li.descricao;
        """)
        
        # Armazenar o resultado da consulta na lista `equipamentos_ativos`
        equipamentos_ativos.clear()
        
        for equipamento in cursor.fetchall():
            codigo_equipamento, cod_usina, motor, marca = equipamento
            # Limpar e padronizar o nome do motor e da marca
            motor_padronizado = limpar_motor(marca, motor)
            # Armazenar os dados limpos na lista
            equipamentos_ativos.append((codigo_equipamento, cod_usina, motor_padronizado, marca))
        
        print("Equipamentos ativos carregados com sucesso!")
    
    except Exception as e:
        print(f"Erro ao carregar equipamentos: {e}")




# Definindo os parâmetros de prioridade para verificação de alertas
lista_parametros = {
    'Real_Load_Speed': [
        'Alerta_Pressao_Admissao_real',
        'Alerta_Potencia_Ativa_real',
        'Alerta_RPM_real'
    ],
    'Real_Pressao_do_Oleo': [
        'Alerta_RPM_real',
        'Alerta_Pressao_Admissao_real',
        'Alerta_Potencia_Ativa_real'
    ],
    'Real_Temperatura_Agua': [
        'Alerta_Temperatura_Ar_Admissao_real',
        'Alerta_Load_Speed_real',
    ]
}

# Função para verificar alerta seguindo a lista de parâmetros
def verificar_alerta_seguindo_lista(row, lista_parametros):
    for parametro, dependentes in lista_parametros.items():
        if row[f'{parametro}'] == 1:
            for dependente in dependentes:
                if row[dependente] == 1:
                    return 1  # Alerta final
    return 0

        
# Dicionário para armazenar os modelos carregados
modelos_carregados = {}

import joblib
import numpy as np
import pandas as pd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix
import joblib



def fazer_previsao(cod_equipamento):
    try:
        # Definir os sensores e seus nomes
        sensores = {
            3: 'Potência Ativa',
            6: 'Tensao L1-L2',
            7: 'Tensao L2-L3',
            8: 'Tensao L3-L1',
            9: 'Corrente L1',
            10: 'Corrente L2',
            11: 'Corrente L3',
            20: 'RPM',
            21: 'Pressão do Óleo',
            25: 'Temperatura da água',
            76: 'Temperatura do ar de admissão',
            77: 'Pressão de admissão',
            114: 'Load Speed'
        }

        # Obter as leituras atuais dos sensores
        valores_atuais = {}
        for cod_campo, sensor_nome in sensores.items():
            cursor_leitura.execute(
                """
                SELECT valor_1, valor_2, valor_3, valor_4, valor_5 
                FROM machine_learning.leituras_consecutivas 
                WHERE cod_equipamento = %s AND cod_campo = %s
                """, 
                (int(cod_equipamento), cod_campo)
            )
            valores = cursor_leitura.fetchone()
            if valores:
                # Calcula a média dos valores disponíveis, ignorando NaNs
                valores_atuais[sensor_nome] = np.nanmean([float(val) for val in valores])

        # Verificar se o Load Speed é 0
        if 'Load Speed' in valores_atuais and valores_atuais['Load Speed'] == 0:
            print(f"Equipamento {cod_equipamento} pulado devido a Load Speed ser 0.")
            return None, False, False

        # Verificar se algum valor está vazio ou nulo
        if any(v is None for v in valores_atuais.values()):
            print(f"Erro: Valores nulos detectados nos sensores do equipamento {cod_equipamento}")
            return None, False, False

        # Carregar o modelo e o scaler treinados
        rf = joblib.load(f'rf_model_{cod_equipamento}.pkl')
        scaler = joblib.load(f'scaler_{cod_equipamento}.pkl')

        # Preparar o DataFrame para previsão
        input_data = pd.DataFrame([valores_atuais])

        # Definir colunas que foram usadas durante o treinamento
        colunas_treinadas = [
            'Pressão do Óleo', 'Load Speed', 'Potência Ativa', 
            'Temperatura do ar de admissão', 'Temperatura da água', 
            'RPM', 'Pressão de admissão', 'Tensao L1-L2', 
            'Tensao L2-L3', 'Tensao L3-L1', 'Corrente L1', 
            'Corrente L2', 'Corrente L3'
        ]

        # Garantir que todas as colunas treinadas estão presentes
        for coluna in colunas_treinadas:
            if coluna not in input_data.columns:
                input_data[coluna] = np.nan  # Atribuir NaN se a coluna estiver faltando

        # Filtrar apenas as colunas relevantes e remover registros com NaNs
        input_data = input_data[colunas_treinadas]
        input_data = input_data.interpolate().dropna()

        # Escalonar os dados de entrada
        input_data_scaled = scaler.transform(input_data)
        print('input_data_scaled',input_data_scaled)
        
        # Fazer previsões
        prediction = rf.predict(input_data_scaled)
        prediction_proba = rf.predict_proba(input_data_scaled)[:, 1]

        # Exibir os valores reais e as previsões
        print(f"\nValores reais para o equipamento {cod_equipamento}:")
        for sensor, valor in valores_atuais.items():
            print(f"{sensor}: {valor:.2f}")

        print(f"\nPrevisão de falha: {prediction[0]}")
        print(f"Probabilidade de falha: {prediction_proba[0]:.2f}")

        return prediction[0], prediction_proba[0], True  # Retornar a previsão e a probabilidade

    except Exception as e:
        print(f"Erro ao fazer previsão para o equipamento {cod_equipamento}: {e}")
        return None, False, False


   
codigos_alarmes_desejados = [1, 243, 244, 253, 256, 259, 262,265,269,272,273,279,280,281,301,304,350, 351, 352, 353, 356, 357, 381, 383, 384, 385, 386, 387, 388, 389, 390, 400, 401, 404, 405,411,412,413,414,415,416, 471, 472, 473,528, 590, 591, 592, 593, 594,595,596,597,598,599,600, 602, 603, 604, 611,615,616,617,631, 635, 637, 638, 657, 658,669,678, 725, 727, 728, 729, 730, 731, 732, 735]




# Lista para armazenar os DataFrames processados de cada equipamento
lista_dfs = []

# Iterar sobre cada equipamento validado
for cod_equipamento in equipamentos_validados:
    try:
        # 1. Recuperar dados das tabelas principais
        queries = []
        for tabela in tabelas:
            query = f"""
                SELECT data_cadastro, valor, cod_campo 
                FROM {tabela} 
                WHERE cod_equipamento = {cod_equipamento} 
                  AND cod_campo IN ({', '.join(map(str, cod_campo_especificados))})
            """
            queries.append(query)
        
        final_query = " UNION ALL ".join(queries)
        cursor_leitura.execute(final_query)
        resultados = cursor_leitura.fetchall()
        
        # Verificar se existem resultados para o equipamento
        if not resultados:
            print(f"Atenção: Nenhum dado de sensor encontrado para o equipamento {cod_equipamento}.")
            continue  # Pular para o próximo equipamento
        
        # Criar DataFrame com os resultados e tratar os dados
        df = pd.DataFrame(resultados, columns=['data_cadastro', 'valor', 'cod_campo'])
        df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])
        df['rounded_time'] = df['data_cadastro'].dt.round('5min')
        df = df[~df.index.duplicated(keep='first')]
        df['valor'] = df['valor'].replace({'-1-1': np.nan})
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        df['valor'] = df['valor'].fillna(0)
        
        # Agrupar e interpolar os dados
        df = df.groupby(['rounded_time', 'cod_campo']).agg({'valor': 'mean'}).reset_index()
        df = df.replace([np.inf, -np.inf], np.nan).dropna()
        df = df.interpolate()
        
        # Pivotar os dados para formato adequado
        df_pivoted = df.pivot(index='rounded_time', columns='cod_campo', values='valor')
        df_pivoted.columns = [f'valor_cod_campo_{col}' for col in df_pivoted.columns]
        df_pivoted = df_pivoted.reset_index()
        
        # Filtrar registros nulos em colunas essenciais
        colunas_essenciais = [
            'valor_cod_campo_3', 'valor_cod_campo_6', 'valor_cod_campo_7', 
            'valor_cod_campo_8', 'valor_cod_campo_9', 'valor_cod_campo_10', 
            'valor_cod_campo_11', 'valor_cod_campo_114', 'valor_cod_campo_21', 
            'valor_cod_campo_76', 'valor_cod_campo_25', 'valor_cod_campo_20', 
            'valor_cod_campo_77'
        ]
        df_pivoted = df_pivoted.dropna(subset=colunas_essenciais)
        df_pivoted = df_pivoted.interpolate()
        
        # Renomear colunas para facilitar a identificação
        df_pivoted = df_pivoted.rename(columns={
            'valor_cod_campo_21': 'Pressão do Óleo',
            'valor_cod_campo_114': 'Load Speed',
            'valor_cod_campo_3': 'Potência Ativa',
            'valor_cod_campo_6': 'Tensao L1-L2',
            'valor_cod_campo_7': 'Tensao L2-L3',
            'valor_cod_campo_8': 'Tensao L3-L1',
            'valor_cod_campo_9': 'Corrente L1',
            'valor_cod_campo_10': 'Corrente L2',
            'valor_cod_campo_11': 'Corrente L3',
            'valor_cod_campo_76': 'Temperatura do ar de admissão',
            'valor_cod_campo_25': 'Temperatura da água',
            'valor_cod_campo_20': 'RPM',
            'valor_cod_campo_77': 'Pressão de admissão',
        })
        
        # Verificar se df_pivoted está vazio após as operações
        if df_pivoted.empty:
            print(f"Atenção: df_pivoted está vazio após processamento para o equipamento {cod_equipamento}.")
            continue  # Pular para o próximo equipamento
        
        # 2. Recuperar dados da tabela log_alarmes para o equipamento atual
        query_log = f"""
            SELECT data_cadastro, cod_equipamento, cod_alarme 
            FROM log_alarmes 
            WHERE cod_equipamento = {cod_equipamento} 
              AND cod_alarme IN ({', '.join(map(str, codigos_alarmes_desejados))})
        """
        cursor_leitura.execute(query_log)
        resultados_log = cursor_leitura.fetchall()
        
        # Criar DataFrame para os alarmes
        df_log = pd.DataFrame(resultados_log, columns=['data_cadastro', 'cod_equipamento', 'cod_alarme'])
        if df_log.empty:
            # Se não houver alarmes para este equipamento, todas as falhas são 0
            df_pivoted['falha'] = 0
        else:
            # Processar os dados de log
            df_log['data_cadastro'] = pd.to_datetime(df_log['data_cadastro'])
            df_log['rounded_time'] = df_log['data_cadastro'].dt.round('5min')
            
            # Remover duplicatas e criar DataFrame de falhas
            df_log = df_log.drop_duplicates(subset=['rounded_time', 'cod_alarme'])
            df_falhas = df_log[['rounded_time']].drop_duplicates()
            df_falhas['falha'] = 1
            
            # Mesclar falhas com o DataFrame principal
            df_pivoted = pd.merge_asof(
                df_pivoted.sort_values('rounded_time'),
                df_falhas.sort_values('rounded_time'),
                on='rounded_time',
                direction='nearest',
                tolerance=pd.Timedelta('5min')
            )
            df_pivoted['falha'] = df_pivoted['falha'].fillna(0).astype(int)
        
        # 3. Selecionar as colunas preditoras (X) e a coluna alvo (y)
        colunas_para_prever = [
            'Pressão do Óleo', 'Load Speed', 'Potência Ativa', 
            'Temperatura do ar de admissão', 'Temperatura da água', 
            'RPM', 'Pressão de admissão', 'Tensao L1-L2', 
            'Tensao L2-L3', 'Tensao L3-L1', 'Corrente L1', 
            'Corrente L2', 'Corrente L3'
        ]
        
        # Garantir que todas as colunas estão presentes
        for coluna in colunas_para_prever:
            if coluna not in df_pivoted.columns:
                df_pivoted[coluna] = np.nan
        
        # Preencher NaNs nas colunas preditoras
        df_pivoted[colunas_para_prever] = df_pivoted[colunas_para_prever].interpolate()
        
        # Remover registros com NaNs nas colunas preditoras
        df_pivoted = df_pivoted.dropna(subset=colunas_para_prever)
        
        # Criar X e y como cópias independentes
        X = df_pivoted[colunas_para_prever].copy()
        y = df_pivoted['falha'].copy()
        
        # Adicionar o cod_equipamento aos dados
        X['cod_equipamento'] = cod_equipamento
        y.name = 'falha'
        
        # Adicionar ao lista de DataFrames
        lista_dfs.append(pd.concat([X, y], axis=1))

        # Dividir os dados em treino e teste
        X_train, X_test, y_train, y_test = train_test_split(
            X[colunas_para_prever], y, test_size=0.2, random_state=42, stratify=y
        )

        # Escalonar as features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        # Inicializar o modelo
        rf = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')

        # Treinar o modelo
        rf.fit(X_train_scaled, y_train)

        # Fazer previsões
        y_pred = rf.predict(X_test_scaled)
        y_pred_proba = rf.predict_proba(X_test_scaled)[:, 1]

        # Avaliar o modelo
        print(f"Relatório de Classificação para o equipamento {cod_equipamento}:")
        print(classification_report(y_test, y_pred))
        print("AUC-ROC:", roc_auc_score(y_test, y_pred_proba))
        print("Matriz de Confusão:")
        print(confusion_matrix(y_test, y_pred))

        # Salvar o modelo e o scaler
        joblib.dump(rf, f'rf_model_{cod_equipamento}.pkl')
        joblib.dump(scaler, f'scaler_{cod_equipamento}.pkl')


    except Exception as e:
        print(f"Erro ao processar equipamento {cod_equipamento}: {e}")

# Unir todos os DataFrames em um único
df_final = pd.concat(lista_dfs, ignore_index=True)





import signal
import sys

# Função para fechar as conexões e sair
def fechar_conexoes_e_sair(signal, frame):
    print("Encerrando o programa e fechando as conexões...")
    cursor.close()
    cursor_leitura.close()
    cnx.close()
    cnx_leitura.close()
    tempo_final = datetime.now()
    total = tempo_final - tempo_inicial
    print('\ntempo total de processamento', total)
    sys.exit(0)  # Sair do programa

# Registrar o manipulador de sinal para Ctrl+C (SIGINT)
signal.signal(signal.SIGINT, fechar_conexoes_e_sair)

try:
    while True:
        for cod_equipamento in equipamentos_validados:
            try:
                fazer_previsao(cod_equipamento)
                pass
            except Exception as e:
                print(f"Erro ao fazer previsão para o equipamento {cod_equipamento}: {e}")

        time.sleep(5)

except Exception as e:
    print(f"Erro inesperado: {e}")

finally:
    # Fechar conexões no final do loop principal, se ocorrer alguma exceção não tratada
    print("Fechando as conexões...")
    cursor.close()
    cursor_leitura.close()
    cnx.close()
    cnx_leitura.close()
    tempo_final = datetime.now()
    total = tempo_final - tempo_inicial
    print('\ntempo total de processamento', total)

