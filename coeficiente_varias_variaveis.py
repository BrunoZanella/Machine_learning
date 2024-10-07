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

#cod_campo_especificados = ['3', '114', '21','76','25','20','77']
cod_campo_especificados = ['3','6','7','8','9','10', '11,' '114', '21','76','25','20','77']

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


# Importante para verificar valores válidos em várias tabelas
equipamentos_validados = []

for cod_equipamento in cod_equipamentos_validos:
    tabelas_com_valores = 0

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
            tabelas_com_valores += 1
    
        # Verificação se já atingiu o limite de 3 tabelas com valores válidos
        if tabelas_com_valores >= 3:
            equipamentos_validados.append(cod_equipamento)
            break

print(f"Total de equipamentos válidos com valores em três ou mais tabelas: {len(equipamentos_validados)}")

#equipamentos_validados = [2765]

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
            abs(df[f'Prev_{coluna_corrente}_L1'] - df[f'Prev_{coluna_corrente}_L2']) > 10, 'Alerta: Corrente fora do padrão',
            np.where(abs(df[f'Prev_{coluna_corrente}_L1'] - df[f'Prev_{coluna_corrente}_L3']) > 10, 'Alerta: Corrente fora do padrão', 'Corrente Normal')
        )
        df[f'Alerta_{coluna_corrente}_L2'] = np.where(
            abs(df[f'Prev_{coluna_corrente}_L2'] - df[f'Prev_{coluna_corrente}_L1']) > 10, 'Alerta: Corrente fora do padrão',
            np.where(abs(df[f'Prev_{coluna_corrente}_L2'] - df[f'Prev_{coluna_corrente}_L3']) > 10, 'Alerta: Corrente fora do padrão', 'Corrente Normal')
        )
        df[f'Alerta_{coluna_corrente}_L3'] = np.where(
            abs(df[f'Prev_{coluna_corrente}_L3'] - df[f'Prev_{coluna_corrente}_L1']) > 10, 'Alerta: Corrente fora do padrão',
            np.where(abs(df[f'Prev_{coluna_corrente}_L3'] - df[f'Prev_{coluna_corrente}_L2']) > 10, 'Alerta: Corrente fora do padrão', 'Corrente Normal')
        )

        # Alertas para Tensões
        df[f'Alerta_{coluna_tensao}_L1_L2'] = np.where(
            abs(df[f'Prev_{coluna_tensao}_L1_L2'] - df[f'Prev_{coluna_tensao}_L2_L3']) > 10, 'Alerta: Tensão fora do padrão',
            np.where(abs(df[f'Prev_{coluna_tensao}_L1_L2'] - df[f'Prev_{coluna_tensao}_L3_L1']) > 10, 'Alerta: Tensão fora do padrão', 'Tensão Normal')
        )
        df[f'Alerta_{coluna_tensao}_L2_L3'] = np.where(
            abs(df[f'Prev_{coluna_tensao}_L2_L3'] - df[f'Prev_{coluna_tensao}_L1_L2']) > 10, 'Alerta: Tensão fora do padrão',
            np.where(abs(df[f'Prev_{coluna_tensao}_L2_L3'] - df[f'Prev_{coluna_tensao}_L3_L1']) > 10, 'Alerta: Tensão fora do padrão', 'Tensão Normal')
        )
        df[f'Alerta_{coluna_tensao}_L3_L1'] = np.where(
            abs(df[f'Prev_{coluna_tensao}_L3_L1'] - df[f'Prev_{coluna_tensao}_L1_L2']) > 10, 'Alerta: Tensão fora do padrão',
            np.where(abs(df[f'Prev_{coluna_tensao}_L3_L1'] - df[f'Prev_{coluna_tensao}_L2_L3']) > 10, 'Alerta: Tensão fora do padrão', 'Tensão Normal')
        )

    alerta_corrente_tensao('Corrente', 'Tensao', df)

    return df



'''

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
    
    print('parametros do equipamento',cod_equipamento,'\nmin_load_speed',min_load_speed,'max_load_speed',max_load_speed,'\nmin_potencia_ativa',min_potencia_ativa,'max_potencia_ativa',max_potencia_ativa,'\nmin_pressao_oleo',min_pressao_oleo,'max_pressao_oleo',max_pressao_oleo,'\nmin_rpm',min_rpm,'max_rpm',max_rpm,'\nmax_temp_agua',max_temp_agua,'min_temp_agua',min_temp_agua,'\nmin_temp_ar_admissao',min_temp_ar_admissao,'max_temp_ar_admissao',max_temp_ar_admissao,'\nmin_pressao_admissao',min_pressao_admissao,'max_pressao_admissao',max_pressao_admissao)
    # Implementação das regras de alerta usando os valores da tabela

    # Implementação original de verificar_alertas, agora usando os valores min/max do dicionário
    df['Alerta_Equipamento'] = np.where(df['Prev_Load Speed'] == 0, 'Equipamento Desligado', 'Equipamento Ligado')

    # Definindo alertas para Pressão do Óleo
    df['Alerta_Pressao_Oleo'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_Pressao_do_Oleo'] < min_pressao_oleo, 'Alerta: Crítica', 
                np.where(df['Prev_Pressao_do_Oleo'] <= max_pressao_oleo, 'Pressão Normal', 'Alerta: Pressão Alta'))
    )

    # Definindo alertas para RPM
    df['Alerta_RPM'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_RPM'] < min_rpm, 'Alerta: RPM Baixo', 
                np.where(df['Prev_RPM'] <= max_rpm, 'RPM Normal', 'Alerta: RPM Alto'))
    )

    # Definindo alertas para Temperatura da Água
    df['Alerta_Temperatura_Agua'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_Temperatura_Agua'] < min_temp_agua, 'Alerta: Temperatura Baixa', 
                np.where(df['Prev_Temperatura_Agua'] <= max_temp_agua, 'Temperatura Normal', 'Alerta: Temperatura Alta'))
    )

    # Definindo alertas para Load Speed
    df['Alerta_Load_Speed'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_Load Speed'] < min_load_speed, 'Alerta: Load Speed Baixo', 
                np.where(df['Prev_Load Speed'] <= max_load_speed, 'Load Speed Normal', 'Alerta: Load Speed Alto'))
    )

    # Definindo alertas para Potência Ativa
    df['Alerta_Potencia_Ativa'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_Potencia_Ativa'] < min_potencia_ativa, 'Alerta: Potência Ativa Baixa', 
                np.where(df['Prev_Potencia_Ativa'] <= max_potencia_ativa, 'Potência Ativa Normal', 'Alerta: Potência Ativa Alta'))
    )

    # Definindo alertas para Temperatura do Ar de Admissão
    df['Alerta_Temperatura_Ar_Admissao'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_Temperatura_do_ar_de_admissao'] < min_temp_ar_admissao, 'Alerta: Temperatura Ar Baixa', 
                np.where(df['Prev_Temperatura_do_ar_de_admissao'] <= max_temp_ar_admissao, 'Temperatura Ar Normal', 'Alerta: Temperatura Ar Alta'))
    )

    # Definindo alertas para Pressão de Admissão
    df['Alerta_Pressao_Admissao'] = np.where(
        df['Prev_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Prev_Pressao_de_admissao'] < min_pressao_admissao, 'Alerta: Pressão Admissão Baixa', 
                np.where(df['Prev_Pressao_de_admissao'] <= max_pressao_admissao, 'Pressão Admissão Normal', 'Alerta: Pressão Admissão Alta'))
    )


    df['Alerta_Equipamento'] = np.where(df['Real_Load Speed'] == 0, 'Equipamento Desligado', 'Equipamento Ligado')

    # Definindo alertas para Pressão do Óleo
    df['Alerta_Pressao_Oleo'] = np.where(
        df['Real_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_Pressao_do_Oleo'] < min_pressao_oleo, 'Alerta: Crítica', 
                np.where(df['Real_Pressao_do_Oleo'] <= max_pressao_oleo, 'Pressão Normal', 'Alerta: Pressão Alta'))
    )

    # Definindo alertas para RPM
    df['Alerta_RPM'] = np.where(
        df['Real_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_RPM'] < min_rpm, 'Alerta: RPM Baixo', 
                np.where(df['Real_RPM'] <= max_rpm, 'RPM Normal', 'Alerta: RPM Alto'))
    )

    # Definindo alertas para Temperatura da Água
    df['Alerta_Temperatura_Agua'] = np.where(
        df['Real_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_Temperatura_Agua'] < min_temp_agua, 'Alerta: Temperatura Baixa', 
                np.where(df['Real_Temperatura_Agua'] <= max_temp_agua, 'Temperatura Normal', 'Alerta: Temperatura Alta'))
    )

    # Definindo alertas para Load Speed
    df['Alerta_Load_Speed'] = np.where(
        df['Real_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_Load Speed'] < min_load_speed, 'Alerta: Load Speed Baixo', 
                np.where(df['Real_Load Speed'] <= max_load_speed, 'Load Speed Normal', 'Alerta: Load Speed Alto'))
    )

    # Definindo alertas para Potência Ativa
    df['Alerta_Potencia_Ativa'] = np.where(
        df['Real_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_Potencia_Ativa'] < min_potencia_ativa, 'Alerta: Potência Ativa Baixa', 
                np.where(df['Real_Potencia_Ativa'] <= max_potencia_ativa, 'Potência Ativa Normal', 'Alerta: Potência Ativa Alta'))
    )

    # Definindo alertas para Temperatura do Ar de Admissão
    df['Alerta_Temperatura_Ar_Admissao'] = np.where(
        df['Real_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_Temperatura_do_ar_de_admissao'] < min_temp_ar_admissao, 'Alerta: Temperatura Ar Baixa', 
                np.where(df['Real_Temperatura_do_ar_de_admissao'] <= max_temp_ar_admissao, 'Temperatura Ar Normal', 'Alerta: Temperatura Ar Alta'))
    )

    # Definindo alertas para Pressão de Admissão
    df['Alerta_Pressao_Admissao'] = np.where(
        df['Real_Load Speed'] == 0, 
        'Equipamento Desligado',
        np.where(df['Real_Pressao_de_admissao'] < min_pressao_admissao, 'Alerta: Pressão Admissão Baixa', 
                np.where(df['Real_Pressao_de_admissao'] <= max_pressao_admissao, 'Pressão Admissão Normal', 'Alerta: Pressão Admissão Alta'))
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
'''



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



# def fazer_previsao(cod_equipamento):
#     try:
#         # Recuperar o modelo do banco de dados para o equipamento específico
#         cursor.execute("SELECT modelo FROM modelos_IA WHERE cod_equipamento = %s ORDER BY data_cadastro DESC LIMIT 1", (cod_equipamento,))
#         modelo_blob = cursor.fetchone()

#         # Verificar se o modelo foi encontrado
#         if not modelo_blob:
#             print(f"Modelo não encontrado para o equipamento {cod_equipamento}")
#             return None, False, False

#         # Carregar o modelo a partir do blob
#         modelo_carregado = joblib.load(BytesIO(modelo_blob[0]))


        # Criar o DataFrame para os valores atuais com a ordem correta
        # df_atuais = pd.DataFrame({
        #     'Pressão do Óleo': valores_atuais['Pressão do Óleo'],
        #     'Load Speed': valores_atuais['Load Speed'],
        #     'Potência Ativa': valores_atuais['Potência Ativa'],
        #     'Temperatura do ar de admissão': valores_atuais['Temperatura do ar de admissão'],
        #     'Temperatura da água': valores_atuais['Temperatura da Água'],  # Ajustando o nome corretamente
        #     'RPM': valores_atuais['RPM'],
        #     'Pressão de admissão': valores_atuais['Pressão de admissão'],
        #     'Tensao L1-L2': valores_atuais['Tensao L1-L2'],
        #     'Tensao L2-L3': valores_atuais['Tensao L2-L3'],
        #     'Tensao L3-L1': valores_atuais['Tensao L3-L1'],
        #     'Corrente L1': valores_atuais['Corrente L1'],
        #     'Corrente L2': valores_atuais['Corrente L2'],
        #     'Corrente L3': valores_atuais['Corrente L3'],
        # })
        
        # # Criar o DataFrame para os valores atuais com o último valor de cada sensor
        # df_atuais = pd.DataFrame({
        #     'Pressão do Óleo': [valores_atuais['Pressão do Óleo'][-1]],
        #     'Load Speed': [valores_atuais['Load Speed'][-1]],
        #     'Potência Ativa': [valores_atuais['Potência Ativa'][-1]],
        #     'Temperatura do ar de admissão': [valores_atuais['Temperatura do ar de admissão'][-1]],
        #     'Temperatura da água': [valores_atuais['Temperatura da Água'][-1]],
        #     'RPM': [valores_atuais['RPM'][-1]],
        #     'Pressão de admissão': [valores_atuais['Pressão de admissão'][-1]],
        #     'Tensao L1-L2': [valores_atuais['Tensao L1-L2'][-1]],
        #     'Tensao L2-L3': [valores_atuais['Tensao L2-L3'][-1]],
        #     'Tensao L3-L1': [valores_atuais['Tensao L3-L1'][-1]],
        #     'Corrente L1': [valores_atuais['Corrente L1'][-1]],
        #     'Corrente L2': [valores_atuais['Corrente L2'][-1]],
        #     'Corrente L3': [valores_atuais['Corrente L3'][-1]],
        # })
        
# Dicionário para armazenar os modelos carregados
modelos_carregados = {}


def fazer_previsao(cod_equipamento):
    try:
        # Obter os valores atuais dos sensores (cod_campo)
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
            25: 'Temperatura da Água',
            76: 'Temperatura do ar de admissão',
            77: 'Pressão de admissão',
            114: 'Load Speed'
        }

        valores_atuais = {}
        for cod_campo, sensor_nome in sensores.items():
            cursor_leitura.execute(f"SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = %s", (int(cod_equipamento), cod_campo))
            valores = cursor_leitura.fetchone()
            # Corrige o tipo de dado para float e assegura que os valores estão corretos
            if valores:
                valores_atuais[sensor_nome] = [float(val) for val in valores]
        
        # Verificar se o valor do cod_campo 114 (Load Speed) é 0
        if 'Load Speed' in valores_atuais and any(val == 0 for val in valores_atuais['Load Speed']):
        #    print(f"Equipamento {cod_equipamento} pulado devido a Load Speed ser 0")
            return None, False, False
        
        # Verificar se algum dos valores está vazio ou nulo
        if any(v is None for v in valores_atuais.values()):
            print(f"Erro: Valores nulos detectados nos sensores do equipamento {cod_equipamento}")
            return None, False, False

        # Carregar a lista de equipamentos ativos (caso ainda não tenha sido carregada)
        if not equipamentos_ativos:
            carregar_equipamentos_ativos()
            
        # Procurar o equipamento na lista de equipamentos ativos
        equipamento_info = next((equipamento for equipamento in equipamentos_ativos if equipamento[0] == cod_equipamento), None)
        if equipamento_info:
            cod_usina, motor, marca = equipamento_info[1], equipamento_info[2], equipamento_info[3]
            print(f"Equipamento {cod_equipamento} pertence à usina {cod_usina} com motor {motor} da marca {marca}\n")
        else:
            print(f"Equipamento {cod_equipamento} não encontrado na lista de equipamentos ativos.\n")
            return None, False, False

        # Verificar se o modelo do equipamento já foi carregado
        if cod_equipamento in modelos_carregados:
            modelo_carregado = modelos_carregados[cod_equipamento]
            print(f'\nO modelo do equipamento {cod_equipamento} já foi carregado')
        else:
            # Recuperar o modelo do banco de dados para o equipamento específico
            cursor.execute("SELECT modelo FROM modelos_IA WHERE cod_equipamento = %s ORDER BY data_cadastro DESC LIMIT 1", (cod_equipamento,))
            modelo_blob = cursor.fetchone()
            print(f'Recuperando o modelo do banco de dados para o equipamento {cod_equipamento}\n')

            # Verificar se o modelo foi encontrado
            if not modelo_blob:
                print(f"Modelo não encontrado para o equipamento {cod_equipamento}")
                return None, False, False

            # Carregar o modelo a partir do blob
            modelo_carregado = joblib.load(BytesIO(modelo_blob[0]))
            
            # Armazenar o modelo carregado no dicionário
            modelos_carregados[cod_equipamento] = modelo_carregado

        # Criar o DataFrame para os valores atuais com o último valor de cada sensor
        df_atuais = pd.DataFrame({
            'Pressão do Óleo': [valores_atuais['Pressão do Óleo'][-1]],
            'Load Speed': [valores_atuais['Load Speed'][-1]],
            'Potência Ativa': [valores_atuais['Potência Ativa'][-1]],
            'Temperatura do ar de admissão': [valores_atuais['Temperatura do ar de admissão'][-1]],
            'Temperatura da água': [valores_atuais['Temperatura da Água'][-1]],
            'RPM': [valores_atuais['RPM'][-1]],
            'Pressão de admissão': [valores_atuais['Pressão de admissão'][-1]],
            'Tensao L1-L2': [valores_atuais['Tensao L1-L2'][-1]],
            'Tensao L2-L3': [valores_atuais['Tensao L2-L3'][-1]],
            'Tensao L3-L1': [valores_atuais['Tensao L3-L1'][-1]],
            'Corrente L1': [valores_atuais['Corrente L1'][-1]],
            'Corrente L2': [valores_atuais['Corrente L2'][-1]],
            'Corrente L3': [valores_atuais['Corrente L3'][-1]],
        })


        # Fazer a previsão com os valores atuais
        previsoes = modelo_carregado.predict(df_atuais)

        # Criar DataFrame com as previsões
        df_previsoes = pd.DataFrame(previsoes, columns=[
            'Prev_Pressao_do_Oleo',
            'Prev_Load Speed',
            'Prev_Potencia_Ativa', 
            'Prev_Temperatura_do_ar_de_admissao',
            'Prev_Temperatura_Agua', 
            'Prev_RPM', 'Prev_Pressao_de_admissao',
            'Prev_Tensao_L1_L2', 
            'Prev_Tensao_L2_L3',
            'Prev_Tensao_L3_L1', 
            'Prev_Corrente_L1', 
            'Prev_Corrente_L2',
            'Prev_Corrente_L3'
        ])

        # Adicionar a coluna de "Pressão do Óleo" real (do df_atuais)
        df_previsoes['Real_Pressao_do_Oleo'] = df_atuais['Pressão do Óleo']
        df_previsoes['Real_Load_Speed'] = df_atuais['Load Speed']
        df_previsoes['Real_RPM'] = df_atuais['RPM']
        df_previsoes['Real_Potencia_Ativa'] = df_atuais['Potência Ativa']
        df_previsoes['Real_Temperatura_do_ar_de_admissao'] = df_atuais['Temperatura do ar de admissão']
        df_previsoes['Real_Temperatura_Agua'] = df_atuais['Temperatura da água']
        df_previsoes['Real_Pressao_de_admissao'] = df_atuais['Pressão de admissão']

        # Aplicar a função para definir alerta final para cada linha do DataFrame
        df_previsoes['Alerta_Final'] = df_previsoes.apply(lambda row: verificar_alerta_seguindo_lista(row, lista_parametros), axis=1)

        # Formatar valores numéricos para duas casas decimais
        df_previsoes = formatar_valores(df_previsoes)

        # Aplicar as regras de negócio (verificar alertas)
        df_previsoes = verificar_alertas(df_previsoes, marca, motor)

    #    print('df_previsoes',df_previsoes)

        # Escrever o DataFrame em um arquivo CSV, atualizando os valores para o equipamento
        salvar_previsoes_csv(df_previsoes, cod_equipamento)

        # Retornar previsões e status
        return df_previsoes, True, True
    
    except Exception as error:
        print(f"Erro ao fazer previsões para o equipamento {cod_equipamento}: {error}")
        return None, False, False





def salvar_previsoes_csv(df_previsoes, cod_equipamento, caminho_csv='previsoes_equipamentos.csv'):
    try:
        # Definir o mapeamento das colunas antigas para os novos nomes
        renomear_colunas = {
            'Alerta': 'Alerta',
            'Alerta_Final': 'Alerta Final',
            'Alerta_Equipamento': 'Ativo',
                        
            'Prev_Load Speed': 'Load Speed',
            'Alerta_Load_Speed': 'Load Alerta',
            'Real_Load_Speed': 'Real Load Speed',
            'Alerta_Load_Speed_real': 'Real Load Alerta',

            'Prev_Temperatura_Agua': 'Temperatura da Água',
            'Alerta_Temperatura_Agua': 'Temp agua Alerta',
            'Real_Temperatura_Agua': 'Real Temperatura da Água',
            'Alerta_Temperatura_Agua_real': 'Real Temp agua Alerta',

            'Prev_Potencia_Ativa': 'Potência Ativa',
            'Alerta_Potencia_Ativa': 'Pot. Alerta',
            'Real_Potencia_Ativa': 'Real Potência Ativa',
            'Alerta_Potencia_Ativa_real': 'Real Pot. Alerta',

            'Prev_Pressao_do_Oleo': 'Pressão do Óleo',
            'Alerta_Pressao_Oleo': 'Press oleo Alerta',
            'Real_Pressao_do_Oleo': 'Real Pressão do Óleo',
            'Alerta_Pressao_Oleo_real': 'Real Press oleo Alerta',

            'Real_Temperatura_do_ar_de_admissao': 'Real Temperatura do Ar de Admissão',
            'Alerta_Temperatura_Ar_Admissao_real': 'Real Temp admissao Alerta',
            'Prev_Temperatura_do_ar_de_admissao': 'Temperatura do Ar de Admissão',
            'Alerta_Temperatura_Ar_Admissao': 'Temp admissao Alerta',

            'Prev_RPM': 'RPM',
            'Alerta_RPM': 'RPM alerta',
            'Real_RPM': 'Real RPM',
            'Alerta_RPM_real': 'Real RPM alerta',

            'Prev_Pressao_de_admissao': 'Pressão de Admissão',
            'Alerta_Pressao_Admissao': 'pressao adm alerta',
            'Real_Pressao_de_admissao': 'Real Pressão de Admissão',
            'Alerta_Pressao_Admissao_real': 'Real pressao adm alerta',
            
            'Prev_Corrente_L1': 'Corrente L1',
            'Alerta_Corrente_L1': 'Corrente L1 Alerta',
            'Prev_Corrente_L2': 'Corrente L2',
            'Alerta_Corrente_L2': 'Corrente L2 Alerta',
            'Prev_Corrente_L3': 'Corrente L3',
            'Alerta_Corrente_L3': 'Corrente L3 Alerta',
            'Prev_Tensao_L1_L2': 'Tensao L1-L2',
            'Alerta_Tensao_L1_L2': 'Tensao L1-L2 Alerta',
            'Prev_Tensao_L2_L3': 'Tensao L2-L3',
            'Alerta_Tensao_L2_L3': 'Tensao L2-L3 Alerta',
            'Prev_Tensao_L3_L1': 'Tensao L3-L1',
            'Alerta_Tensao_L3_L1': 'Tensao L3-L1 Alerta'
        }

        # Adicionar a coluna 'cod_equipamento' ao DataFrame de previsões e garantir que esteja na primeira posição
        df_previsoes['cod_equipamento'] = cod_equipamento
        df_previsoes = df_previsoes[['cod_equipamento'] + [col for col in df_previsoes.columns if col != 'cod_equipamento']]

        # Renomear as colunas do DataFrame
        df_previsoes.rename(columns=renomear_colunas, inplace=True)

        # Garantir que todas as colunas esperadas estão presentes no DataFrame
        colunas_ordenadas = [
            'cod_equipamento',
            'Ativo',
            'Alerta',
            'Alerta Final',

            'Load Speed',
            'Load Alerta',
            'Real Load Speed',
            'Real Load Alerta',

            'Temperatura da Água',
            'Temp agua Alerta',
            'Real Temperatura da Água',
            'Real Temp agua Alerta',

            'Potência Ativa',
            'Pot. Alerta',
            'Real Potência Ativa',
            'Real Pot. Alerta',

            'Pressão do Óleo',
            'Press oleo Alerta',
            'Real Pressão do Óleo',
            'Real Press oleo Alerta',

            'Temperatura do Ar de Admissão',
            'Temp admissao Alerta',
            'Real Temperatura do Ar de Admissão',
            'Real Temp admissao Alerta',

            'RPM',
            'RPM alerta',
            'Real RPM',
            'Real RPM alerta',

            'Pressão de Admissão',
            'pressao adm alerta',
            'Real Pressão de Admissão',
            'Real pressao adm alerta',

            'Corrente L1',
            'Corrente L1 Alerta',
            'Corrente L2',
            'Corrente L2 Alerta',
            'Corrente L3',
            'Corrente L3 Alerta',

            'Tensao L1-L2',
            'Tensao L1-L2 Alerta',
            'Tensao L2-L3',
            'Tensao L2-L3 Alerta',
            'Tensao L3-L1',
            'Tensao L3-L1 Alerta'
        ]

        # Adicionar colunas faltantes com valores padrão (NaN ou outro valor desejado)
        for col in colunas_ordenadas:
            if col not in df_previsoes.columns:
                df_previsoes[col] = pd.NA

        # Reordenar as colunas para corresponder à ordem definida
        df_previsoes = df_previsoes[colunas_ordenadas]

        # Se o arquivo não existir, criar um novo com o DataFrame atual e o equipamento
        if not os.path.exists(caminho_csv):
            df_previsoes.to_csv(caminho_csv, index=False)
        else:
            # Ler o arquivo CSV existente
            df_existente = pd.read_csv(caminho_csv)

            # Verificar se o equipamento já existe no CSV
            if cod_equipamento in df_existente['cod_equipamento'].values:
                # Atualizar os valores para o equipamento existente
                df_existente.loc[df_existente['cod_equipamento'] == cod_equipamento, df_previsoes.columns] = df_previsoes.values
            else:
                # Adicionar nova linha para o novo equipamento
                df_existente = pd.concat([df_existente, df_previsoes], ignore_index=True)

            # Garantir que as colunas do CSV final também estejam na ordem correta
            df_existente = df_existente[colunas_ordenadas]

            # Escrever de volta no CSV, sobrescrevendo o antigo
            df_existente.to_csv(caminho_csv, index=False)
        
        print(f"Previsões atualizadas para o equipamento {cod_equipamento}.")
    
    except Exception as error:
        print(f"Erro ao atualizar CSV para o equipamento {cod_equipamento}: {error}")



# # Criar tabela modelos_IA se ainda não existir
# create_table_query = """
# CREATE TABLE IF NOT EXISTS modelos_IA (
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     cod_equipamento INT NOT NULL,
#     modelo LONGBLOB,
#     acuracia FLOAT DEFAULT NULL,
#     data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# );
# """
# cursor.execute(create_table_query)
# cnx.commit()


# # Função para salvar o modelo no banco de dados
# def salvar_modelo_no_banco(cod_equipamento, file_path, acuracia):
#     with open(file_path, 'rb') as file:
#         modelo_binario = file.read()

#     # Verificar se o modelo já existe
#     select_query = "SELECT id FROM modelos_IA WHERE cod_equipamento = %s"
#     cursor.execute(select_query, (cod_equipamento,))
#     result = cursor.fetchone()

#     if result:
#         # Atualizar o modelo existente
#         update_query = """
#         UPDATE modelos_IA
#         SET modelo = %s, acuracia = %s, data_cadastro = CURRENT_TIMESTAMP
#         WHERE cod_equipamento = %s
#         """
#         cursor.execute(update_query, (modelo_binario, acuracia, cod_equipamento))
#     else:
#         # Inserir um novo modelo
#         insert_query = """
#         INSERT INTO modelos_IA (cod_equipamento, modelo, acuracia)
#         VALUES (%s, %s, %s)
#         """
#         cursor.execute(insert_query, (cod_equipamento, modelo_binario, acuracia))
    
#     cnx.commit()


# # Criar diretório 'modelos_IA' se ele não existir
# pasta_modelos = "modelos_IA"
# if not os.path.exists(pasta_modelos):
#     os.makedirs(pasta_modelos)
   

# for cod_equipamento in equipamentos_validados:
#     try:
#         queries = []
#         for tabela in tabelas:
#             query = f"SELECT data_cadastro, valor, cod_campo FROM {tabela} WHERE cod_equipamento = {cod_equipamento} AND cod_campo IN ({', '.join(cod_campo_especificados)})"
#             queries.append(query)

#         final_query = " UNION ALL ".join(queries)
#         cursor_leitura.execute(final_query)
#         resultados = cursor_leitura.fetchall()
        
#         df = pd.DataFrame(resultados, columns=['data_cadastro', 'valor', 'cod_campo'])
#         df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])
#         df['rounded_time'] = df['data_cadastro'].dt.round('5min')
#         df = df[~df.index.duplicated(keep='first')]
#         df['valor'] = df['valor'].replace({'-1-1': np.nan})
#         df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
#         df['valor'] = df['valor'].fillna(0)
#         df = df.groupby(['rounded_time', 'cod_campo']).agg({'valor': 'mean'}).reset_index()
#         df = df.replace([np.inf, -np.inf], np.nan).dropna()
#         df = df.interpolate()
#         df_pivoted = df.pivot(index='rounded_time', columns='cod_campo', values='valor')
#         df_pivoted.columns = [f'valor_cod_campo_{col}' for col in df_pivoted.columns]
#         df_pivoted = df_pivoted.reset_index()
#         df_pivoted = df_pivoted.dropna(subset=['valor_cod_campo_3', 'valor_cod_campo_6', 'valor_cod_campo_7', 'valor_cod_campo_8', 'valor_cod_campo_9', 'valor_cod_campo_10', 'valor_cod_campo_11', 'valor_cod_campo_114', 'valor_cod_campo_21', 'valor_cod_campo_76', 'valor_cod_campo_25', 'valor_cod_campo_20', 'valor_cod_campo_77'])

#         df_pivoted = df_pivoted.interpolate()

#         # Renomear colunas para facilitar a identificação nos gráficos
#         df_pivoted = df_pivoted.rename(columns={
#             'valor_cod_campo_21': 'Pressão do Óleo',
#             'valor_cod_campo_114': 'Load Speed',
#             'valor_cod_campo_3': 'Potência Ativa',
#             'valor_cod_campo_6': 'Tensao L1-L2',
#             'valor_cod_campo_7': 'Tensao L2-L3',
#             'valor_cod_campo_8': 'Tensao L3-L1',
#             'valor_cod_campo_9': 'Corrente L1',
#             'valor_cod_campo_10': 'Corrente L2',
#             'valor_cod_campo_11': 'Corrente L3',
#             'valor_cod_campo_76': 'Temperatura do ar de admissão',
#             'valor_cod_campo_25': 'Temperatura da água',
#             'valor_cod_campo_20': 'RPM',
#             'valor_cod_campo_77': 'Pressão de admissão',
#         })

#         # Certificar que X e y tenham as mesmas colunas
#         colunas_para_prever = [
#             'Pressão do Óleo', 'Load Speed', 'Potência Ativa', 'Temperatura do ar de admissão', 'Temperatura da água', 
#             'RPM', 'Pressão de admissão', 'Tensao L1-L2', 
#             'Tensao L2-L3', 'Tensao L3-L1', 'Corrente L1', 'Corrente L2', 
#             'Corrente L3'
#         ]

#         X = df_pivoted[colunas_para_prever]  # Variáveis preditoras (colunas específicas)
#         y = X.copy()  # As mesmas variáveis como dependentes (prevendo as mesmas)

#         # Usando RandomForest para multivariado
#         multioutput_regressor = MultiOutputRegressor(RandomForestRegressor())
#         multioutput_regressor.fit(X, y)

#         # Avaliar o modelo
#         score = multioutput_regressor.score(X, y)
#         print(f"Model Score {cod_equipamento}: {score}")

#         # Realizar previsões
#         previsoes = multioutput_regressor.predict(X)

#         # Supondo que `X_train` seja o DataFrame usado para treinar o modelo
#         colunas_treinamento = X.columns.tolist()
        
#         # Caminho para salvar o modelo
#         file_path = os.path.join(pasta_modelos, f'modelo_multivariado_{cod_equipamento}.pkl')

#         # Salvando o modelo para uso futuro
#         joblib.dump(multioutput_regressor, file_path)

#         # Salvar o modelo no banco de dados, incluindo a acurácia
#         salvar_modelo_no_banco(cod_equipamento, file_path, score)

#         # Remover o arquivo após salvar no banco de dados
#         os.remove(file_path)

#     except Exception as error:
#         print('\n----------------------------------------------------------------------------------------------------------------\n')                            
#         print(f"Erro ao processar equipamento {cod_equipamento}: {error}")
#         continue

# # Apagar a pasta 'modelos_IA' ao finalizar o loop
# if os.path.exists(pasta_modelos):
#     os.rmdir(pasta_modelos)



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

