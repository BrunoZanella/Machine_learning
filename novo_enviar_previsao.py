# enviar_previsao.py
import sys
import json
from bot import novo_enviar_previsao_valor_equipamento_alerta_novo, create_pool, id_chat_grupo, equipamentos_ativos, parametros_min_max_motores, lista_parametros, limites_variaveis, limites_tolerancia, lista_parametros_previsao
import aiomysql
from aiogram import Bot, Dispatcher, types
import asyncio
import mysql.connector
import atexit
import pandas as pd
from datetime import datetime, timedelta, time
import sys
from aiogram.types import ParseMode
import os
import glob
from collections import defaultdict
from aiogram.utils.exceptions import NetworkError

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.types import CallbackQuery

from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Command
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.exceptions import BotBlocked
import telebot
import time
import aiomysql
from aiogram import executor
from aiogram.contrib.middlewares.logging import LoggingMiddleware
import html
#from bot_relatorios import *

import logging
import csv
import io
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from aiogram.utils import executor
import aiomysql
import google.generativeai as genai
from datetime import datetime
import magic
from aiogram.types import Message
from PIL import Image
import tempfile
import re
import matplotlib.pyplot as plt

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle,FrameBreak, PageBreak
from reportlab.lib import colors
from reportlab.lib.units import cm
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer, Image, Paragraph
from bs4 import BeautifulSoup
from PIL import Image as PILImage
from reportlab.lib.colors import red, green, orange

import aiosmtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

from aiogram.utils.exceptions import MessageNotModified, MessageToDeleteNotFound
from asyncio import sleep

async def selecionar_GMG(pool):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT codigo, ativo FROM sup_geral.tipos_equipamentos WHERE classe = 'GMG'")
            resultados = await cursor.fetchall()
            codigos = [resultado['codigo'] for resultado in resultados]
        return codigos

async def obter_equipamentos_validos(tabelas, pool):
    codigos_GMG = await selecionar_GMG(pool)
    codigos_GMG_str = ', '.join(map(str, codigos_GMG))

    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            query_equipamentos = f"SELECT DISTINCT codigo FROM sup_geral.equipamentos WHERE cod_tipo_equipamento IN ({codigos_GMG_str}) AND ativo = 1"
            await cursor.execute(query_equipamentos)
            resultados_equipamentos = await cursor.fetchall()
            cod_equipamentos = [str(resultado['codigo']) for resultado in resultados_equipamentos]

            query_ultima_tabela = f"SELECT DISTINCT cod_equipamento FROM {tabelas}"
            await cursor.execute(query_ultima_tabela)
            resultados_ultima_tabela = await cursor.fetchall()
            cod_ultima_tabela = [str(resultado['cod_equipamento']) for resultado in resultados_ultima_tabela]

    cod_equipamentos_validos = list(set(cod_equipamentos) & set(cod_ultima_tabela))
    cod_equipamentos_validos = sorted([int(cod) for cod in cod_equipamentos_validos])

    return cod_equipamentos_validos




async def novo_verificar_e_obter_coeficiente_novo(cod_equipamento, pool):
    try:
        coeficientes = {}
        interceptos = {}
        
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
                    SELECT X, Y, coeficiente, intercepto 
                    FROM machine_learning.novos_coeficiente_geradores 
                    WHERE cod_equipamento = %s
                """
                await cursor.execute(query, (cod_equipamento,))
                resultados = await cursor.fetchall()
                
                if resultados:
                    for resultado in resultados:
                        X, Y, coeficiente, intercepto = resultado
                        coeficientes[(X, Y)] = coeficiente
                        interceptos[(X, Y)] = intercepto

        return coeficientes, interceptos
    except Exception as e:
        print(f"An error occurred in novo_verificar_e_obter_coeficiente_novo: {e}")
        return {}, {}


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
#    print('motor_padronizado:', motor_padronizado)

    return motor_padronizado

# Lista para armazenar os dados dos equipamentos
equipamentos_ativos = []

async def carregar_equipamentos_ativos(pool):
    """
    Função para carregar os equipamentos ativos do banco de dados e armazenar na lista `equipamentos_ativos`.
    """
    try:
        # Realiza a consulta para buscar os equipamentos ativos
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    SELECT 
                        e.codigo AS codigo_equipamento, 
                        e.cod_usina, 
                        u.nome AS nome_usina,  -- Adicionando a coluna nome da tabela usinas
                        e.nome AS nome_equipamento,  -- Adicionando a coluna nome da tabela equipamentos
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
                        ) AS marca,
                        e.potencia,  -- Adicionando a coluna potencia da tabela equipamentos
                        e.tensao     -- Adicionando a coluna tensao da tabela equipamentos
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
                        e.codigo, e.cod_usina, u.nome, e.nome, e.motor, li.descricao, e.potencia, e.tensao;
                """)
                
                # Armazenar o resultado da consulta na lista `equipamentos_ativos`
            #    equipamentos_ativos.clear()

                resultados = await cursor.fetchall()

                # Contador de equipamentos
                contador_equipamentos = 0

                for equipamento in resultados:
                    # Agora estamos desempacotando 8 valores, incluindo nome_equipamento
                    codigo_equipamento, cod_usina, nome_usina, nome_equipamento, motor, marca, potencia, tensao = equipamento
                    # Limpar e padronizar o nome do motor e da marca
                    motor_padronizado = limpar_motor(marca, motor)
                    # Armazenar os dados limpos na lista
                    equipamentos_ativos.append((codigo_equipamento, cod_usina, nome_usina, nome_equipamento, motor_padronizado, marca, potencia, tensao))
                    
                    # Incrementar o contador
                    contador_equipamentos += 1

                print(f"Equipamentos ativos carregados com sucesso! Total de equipamentos: {contador_equipamentos}")
    
    except Exception as e:
        print(f"Erro ao carregar equipamentos: {e}")






def formatar_valores(df):
    """
    Formata todas as colunas numéricas no DataFrame para mostrar apenas duas casas decimais.
    """
    num_cols = df.select_dtypes(include=['float64']).columns
    df[num_cols] = df[num_cols].round(2)
    return df

# Dicionário global para armazenar os parâmetros min e max dos motores
parametros_min_max_motores = {}

async def carregar_parametros_min_max(pool):
    # Buscar dados da tabela parametros_min_max_motores e armazenar em um dicionário
    global parametros_min_max_motores
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
#            await cursor.execute("SELECT motor, parametro, min_valor, max_valor FROM machine_learning.parametros_min_max_motores")
            await cursor.execute("SELECT motor, parametro, min_alerta, max_alerta FROM machine_learning.parametros_min_max_motores")
            resultados = await cursor.fetchall()
    
    # Armazenar os resultados em um dicionário estruturado
    for linha in resultados:
        motor, parametro, min_valor, max_valor = linha
        if motor not in parametros_min_max_motores:
            parametros_min_max_motores[motor] = {}
        parametros_min_max_motores[motor][parametro] = {'min': min_valor, 'max': max_valor}

#    print(f"Parâmetros min/max carregados para {len(parametros_min_max_motores)} motores.")

async def verificar_alertas(cod_equipamento, df, marca, motor, potencia,tensao, tensao_l1_l2, tensao_l2_l3, tensao_l3_l1, potencia_nominal, pool):
#async def verificar_alertas(cod_equipamento, df, marca, motor, potencia, tensao, pool):
    # Se a tabela de parâmetros ainda não foi carregada, carregar agora
    if not parametros_min_max_motores:
        await carregar_parametros_min_max(pool)  # Adicione 'await' aqui

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
    #        print(f"Parâmetros encontrados para {chave_motor}\n")
            break

    limites = {}
    # Se não encontrar os parâmetros, avisar
    if not parametros_motor:
    #    print(f"Parâmetros não encontrados para motor {motor} da marca {marca}\n")
        return df, limites

    # Normalizar as chaves para remover "(Bar)" ou outros elementos entre parênteses
    parametros_motor_normalizado = {re.sub(r'\s*\(.*?\)\s*', '', chave): valor for chave, valor in parametros_motor.items()}

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

    # print('\nparâmetros do equipamento', cod_equipamento, 
    #       '\nLoad Speed', min_load_speed, '-->', max_load_speed,
    #       '\nPot. Ativa', min_potencia_ativa, '-->', max_potencia_ativa,
    #       '\nPressao do oleo', min_pressao_oleo, '-->', max_pressao_oleo,
    #       '\nRPM', min_rpm, '-->', max_rpm,
    #       '\nTemp. agua', max_temp_agua, '-->', min_temp_agua,
    #       '\nTemp ar de admissao', min_temp_ar_admissao, '-->', max_temp_ar_admissao,
    #       '\nPressao de admissao', min_pressao_admissao, '-->', max_pressao_admissao)


    # Obtenha os limites de cada parâmetro
    limites = {
        'Load Speed': {'min': parametros_motor_normalizado.get('Load Speed', {}).get('min', 0),
                       'max': parametros_motor_normalizado.get('Load Speed', {}).get('max', 80)},
        'Temperatura do ar de admissão': {'min': parametros_motor_normalizado.get('Temperatura do ar de admissão', {}).get('min', 0),
                                          'max': parametros_motor_normalizado.get('Temperatura do ar de admissão', {}).get('max', 100)},
        'Pressão de admissão': {'min': parametros_motor_normalizado.get('Pressão de admissão', {}).get('min', 0.50),
                                'max': parametros_motor_normalizado.get('Pressão de admissão', {}).get('max', 1.50)},
        # 'Potência Ativa': {'min': parametros_motor_normalizado.get('Potência Ativa', {}).get('min', 0),
        #                'max': potencia * 0.9 if potencia else parametros_motor_normalizado.get('Potência Ativa', {}).get('max', 400)}, 
        'Potência Ativa': {
            'min': parametros_motor_normalizado.get('Potência Ativa', {}).get('min', 0),
            # O limite máximo é definido como 90% da Potência Nominal
            'max': potencia_nominal * 0.9 if potencia_nominal else parametros_motor_normalizado.get('Potência Ativa', {}).get('max', 400)
        },
        'Pressão do Óleo': {'min': parametros_motor_normalizado.get('Pressão do Óleo', {}).get('min', 3.5),
                            'max': parametros_motor_normalizado.get('Pressão do Óleo', {}).get('max', 5.0)},
        'RPM': {'min': parametros_motor_normalizado.get('RPM', {}).get('min', 1798),
                'max': parametros_motor_normalizado.get('RPM', {}).get('max', 1802)},
        'Temperatura da Água': {'min': parametros_motor_normalizado.get('Temperatura da água', {}).get('min', 30),
                                'max': parametros_motor_normalizado.get('Temperatura da água', {}).get('max', 103)},
        'Tensao Bateria': {'min': parametros_motor_normalizado.get('Bateria', {}).get('min', 10),
                'max': parametros_motor_normalizado.get('Bateria', {}).get('max', 29.50)},
        
        'Frequencia': {'min': parametros_motor_normalizado.get('Frequencia', {}).get('min', 59.5),
                'max': parametros_motor_normalizado.get('Frequencia', {}).get('max', 60.5)},
        
        'Consumo': {'min': parametros_motor_normalizado.get('Consumo', {}).get('min', 10),
                'max': parametros_motor_normalizado.get('Consumo', {}).get('max', 29.50)},
        

        'Tensao L1-L2': {
            'min': int(tensao_l1_l2 * 0.9),  # Converte para inteiro
            'max': int(tensao_l1_l2 * 1.1)   # Converte para inteiro
        },
        'Tensao L2-L3': {
            'min': int(tensao_l2_l3 * 0.9),  # Converte para inteiro
            'max': int(tensao_l2_l3 * 1.1)   # Converte para inteiro
        },
        'Tensao L3-L1': {
            'min': int(tensao_l3_l1 * 0.9),  # Converte para inteiro
            'max': int(tensao_l3_l1 * 1.1)   # Converte para inteiro
        }


        # 'Tensao L1-L2': {
        #     'min': tensao_l1_l2 * 0.9,
        #     'max': tensao_l1_l2 * 1.1
        # },
        # 'Tensao L2-L3': {
        #     'min': tensao_l2_l3 * 0.9,
        #     'max': tensao_l2_l3 * 1.1
        # },
        # 'Tensao L3-L1': {
        #     'min': tensao_l3_l1 * 0.9,
        #     'max': tensao_l3_l1 * 1.1
        # }

    }

    # print('\n')
    # for sensor, limite in limites.items():
    #     print(f"{sensor}: Min: {limite['min']}, Max: {limite['max']}")
        
    # Adiciona a coluna Alerta com valor padrão 0
    df['Alerta'] = 0

    return df, limites





# Função para verificar alerta seguindo a lista de parâmetros
def verificar_alerta_seguindo_lista(row, lista_parametros):
    for parametro, dependentes in lista_parametros.items():
    #    print(f"Verificando parametro: {parametro} com valor: {row[f'{parametro}']}")
        if row[f'{parametro}'] == 1:
            for dependente in dependentes:
    #            print(f"Verificando dependente: {dependente} com valor: {row[dependente]}")
                if row[dependente] == 1:
                    print("Alerta acionado!")
                    return 1  # Alerta final
    return 0


# Definindo os parâmetros de prioridade para verificação de alertas
lista_parametros = {
    'Real_Load_Speed': [
        'Alerta_Temperatura_Agua_real',
        'Alerta_Potencia_Ativa_real',
    ],
    'Real_Pressao_do_Oleo': [
        'Alerta_RPM_real',
    ],
    'Real_Temperatura_Agua': [
        'Alerta_Load_Speed_real',
        'Alerta_Temperatura_Ar_Admissao_real',
    ]
}

# Dicionário para especificar variáveis que entram nos limites mais e menos
limites_variaveis = {
    'limite_mais': [
        'Load Speed', 'Potência Ativa', 'Temperatura do ar de admissão',
        'Temperatura da água', 'RPM', 'Pressão de admissão', 
        'Tensao L1-L2', 'Tensao L2-L3', 'Tensao L3-L1'
    ],
    'limite_menos': [
        'Pressão do Óleo', 'RPM',
        'Tensao L1-L2', 'Tensao L2-L3', 'Tensao L3-L1'
    ]
}


# Dicionário de limites de tolerância (desvio permitido para cada variável)
limites_tolerancia = {
    'Load Speed': 20,
    'Pressão do Óleo': 0.5,
    'Potência Ativa': 25,
    'Potência Nominal': 30,
#    'Potência Ativa': None,  # Tolerância será baseada na Potência Nominal
#    'Potência Nominal': None,  # Tolerância será calculada dinamicamente
    'Temperatura do ar de admissão': 20,
    'Temperatura da Água': 15,
    'RPM': 50,
    'Tensao Bateria': 5,
    'Pressão de admissão': 0.5,
    'Tensao L1-L2': 10,
    'Tensao L2-L3': 10,
    'Tensao L3-L1': 10
}


async def novo_fazer_previsao_sempre_alerta_novo(cod_equipamento, pool):

    global equipamentos_ativos

    sensores = {
        3: 'Potência Ativa', 
        6: 'Tensao L1-L2',
        7: 'Tensao L2-L3',
        8: 'Tensao L3-L1',
        9: 'Corrente L1',
        10: 'Corrente L2',
        11: 'Corrente L3',
        16: 'Frequencia',
        19: 'Tensao Bateria',
        20: 'RPM',
        21: 'Pressão do Óleo',
        23: 'Consumo',
        24: 'Horimetro',
        25: 'Temperatura da Água',
        76: 'Temperatura do ar de admissão',
        77: 'Pressão de admissão',
        114: 'Load Speed',
        120: 'Potência Nominal',
    }
    contagem_limites = 4
    contagem_acima_do_limite = {}
    contagem_abaixo_do_limite = {}
    contagem_acima_do_limite_previsao = {}
    contagem_abaixo_do_limite_previsao = {}
    previsoes = {}
    valores_atuais = {}
    cod_usina = {}
    nome_usina = {}
    nome_equipamento = {}
    contagem_equipamentos_ativos = 0
    
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:

                # Obter valores atuais para cada sensor
                valores_atuais = {}
                async with conn.cursor() as cursor_valores:
                    for cod_campo, sensor_nome in sensores.items():
                        await cursor_valores.execute(
                            "SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = %s",
                            (cod_equipamento, cod_campo)
                        )
                        valores = await cursor_valores.fetchone()
                        if valores:
                            valores_atuais[sensor_nome] = [float(val) for val in valores]

                # Carregar a lista de equipamentos ativos (caso ainda não tenha sido carregada)
                if not equipamentos_ativos:
                    equipamentos_ativos = await carregar_equipamentos_ativos(pool)


                # Verificar se o valor do cod_campo 114 (Load Speed) é 0
                if 'Load Speed' in valores_atuais and any(val == 0 for val in valores_atuais['Load Speed']):
                #    print(f"Equipamento {cod_equipamento} pulado devido a Load Speed ser 0")
                    return None, None, None, None, None, False, False, False, False, None
                
                # Verificar se algum dos valores está vazio ou nulo
                if any(v is None for v in valores_atuais.values()):
                    print(f"Erro: Valores nulos detectados nos sensores do equipamento {cod_equipamento}")
                    return None, None, None, None, None, False, False, False, False, None

                # Verificar a data de cadastro para cod_campo = 3
                await cursor.execute(
                    "SELECT data_cadastro FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 3",
                    (cod_equipamento,)
                )
                data_cadastro = await cursor.fetchone()

                if data_cadastro is not None:
                    data_cadastro = data_cadastro[0]

                agora = datetime.now()

                if data_cadastro is None or (agora - data_cadastro > timedelta(hours=1)):
                #     await cursor.execute("""
                #         UPDATE machine_learning.leituras_consecutivas
                #         SET alerta = 0
                #         WHERE cod_equipamento = %s
                #     """, (cod_equipamento,))
                #     await conn.commit()
                    return {}, {}, {}, {}, {}, {}, {}, {}, {}, {}

                # Verificar se a diferença é maior que 15 minutos
                if (agora - data_cadastro) > timedelta(minutes=15):
                #    print(f"Equipamento {cod_equipamento}: Ignorado, data_cadastro é mais de 10 minutos atrás.")
                    return {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
    
#                print('\n----------------------------------------------------------------------------------------------------------------\n')

                # Procurar o equipamento na lista de equipamentos ativos
                equipamento_info = next((equipamento for equipamento in equipamentos_ativos if equipamento[0] == cod_equipamento), None)

                if equipamento_info:
                    print('\n----------------------------------------------------------------------------------------------------------------\n')
                    cod_usina, nome_usina, nome_equipamento, motor, marca, potencia, tensao = equipamento_info[1], equipamento_info[2], equipamento_info[3], equipamento_info[4], equipamento_info[5], equipamento_info[6], equipamento_info[7]

                    # Realizar contagem de equipamentos ativos para a usina
                    await cursor.execute(
                        "SELECT COUNT(codigo) FROM sup_geral.equipamentos WHERE cod_usina = %s AND ativo = 1",
                        (cod_usina,)
                    )
                    contagem_equipamentos_ativos = await cursor.fetchone()
                    if contagem_equipamentos_ativos:
                        contagem_equipamentos_ativos = contagem_equipamentos_ativos[0]

                    print(f"Equipamento {cod_equipamento} ({nome_equipamento}) pertence à usina {nome_usina} (código: {cod_usina}) que tem {contagem_equipamentos_ativos} equipamentos ativos. O motor {motor} da marca {marca}, de potencia {potencia} e tensao {tensao}")

                else:
    #                print(f"Equipamento {cod_equipamento} não encontrado na lista de equipamentos ativos.")
                    return None, None, None, None, None, False, False, False, False, None

                # Obter coeficientes e interceptos
                coeficientes, interceptos = await novo_verificar_e_obter_coeficiente_novo(cod_equipamento, pool)

                if not coeficientes or not interceptos:
                    return {}, {}, {}, {}, {}, {}, {}, {}, {}, {}

                # Calcular previsões para cada sensor que possui coeficiente
                for (X, Y), coeficiente in coeficientes.items():
                    sensor_X = sensores.get(X)
                    sensor_Y = sensores.get(Y)
                    if sensor_X and sensor_Y and sensor_X in valores_atuais and sensor_Y in valores_atuais:
                        previsoes[sensor_Y] = [
                            round(val * coeficiente + interceptos[(X, Y)], 1) 
                            for val in valores_atuais[sensor_X]
                        ]



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
                    'Tensao Bateria': [valores_atuais['Tensao Bateria'][-1]],
                })

                # Criar DataFrame com as previsões
                df_previsoes = pd.DataFrame(previsoes, columns=[
                    'Prev_Pressao_do_Oleo',
                    'Prev_Load Speed',
                    'Prev_Potencia_Ativa', 
                    'Prev_Temperatura_do_ar_de_admissao',
                    'Prev_Temperatura_Agua', 
                    'Prev_RPM', 'Prev_Pressao_de_admissao',
                ])

                # Adicionar a coluna de "Pressão do Óleo" real (do df_atuais)
                df_previsoes['Real_Pressao_do_Oleo'] = df_atuais['Pressão do Óleo']
                df_previsoes['Real_Load_Speed'] = df_atuais['Load Speed']
                df_previsoes['Real_RPM'] = df_atuais['RPM']
                df_previsoes['Real_Potencia_Ativa'] = df_atuais['Potência Ativa']
                df_previsoes['Real_Temperatura_do_ar_de_admissao'] = df_atuais['Temperatura do ar de admissão']
                df_previsoes['Real_Temperatura_Agua'] = df_atuais['Temperatura da água']
                df_previsoes['Real_Pressao_de_admissao'] = df_atuais['Pressão de admissão']
                df_previsoes['Real_Tensao_Bateria'] = df_atuais['Tensao Bateria']

                # Aplicar a função para definir alerta final para cada linha do DataFrame
            #    df_previsoes['Alerta_Final'] = df_previsoes.apply(lambda row: verificar_alerta_seguindo_lista(row, lista_parametros), axis=1)

                # Formatar valores numéricos para duas casas decimais
                df_previsoes = formatar_valores(df_previsoes)


                # Verifique se 'Potência Nominal' está em valores_atuais
                if 'Potência Nominal' in valores_atuais:
                    potencia_nominal_atual = valores_atuais['Potência Nominal'][-1]  # Último valor de Potência Nominal
                    potencia_nominal = 0.9 * potencia_nominal_atual  # Calcula 90% da Potência Nominal
                else:
                    potencia_nominal = 0  # Define como None caso não tenha o valor


                # Aplicar as regras de negócio (verificar alertas)
                df_previsoes, limites = await verificar_alertas(cod_equipamento, df_previsoes, marca, motor, potencia, tensao,
                                                                                                       valores_atuais['Tensao L1-L2'][-1],
                                                                                                        valores_atuais['Tensao L2-L3'][-1],
                                                                                                        valores_atuais['Tensao L3-L1'][-1],
                                                                                                        potencia_nominal,
                                                                                                        pool)

                # Verificar limites para cada sensor
                for cod_campo, sensor_nome in sensores.items():
                    if sensor_nome in valores_atuais:
                        valores_sensor = valores_atuais[sensor_nome]
                        
                        # Obter limites do dicionário de limites retornado pela função verificar_alertas
                        limite_menos = limites.get(sensor_nome, {}).get('min', -float('inf'))
                        limite_mais = limites.get(sensor_nome, {}).get('max', float('inf'))

                    #    print(f'{sensor_nome}: limite_menos', limite_menos,'limite_mais',limite_mais)

                        contagem_acima_do_limite[sensor_nome] = 0
                        contagem_abaixo_do_limite[sensor_nome] = 0

                        for i, valor in enumerate(valores_sensor):
                            if valor == 0:
                                continue

                            # Verifica se o sensor faz parte das variáveis para "limite mais"
                            if sensor_nome in limites_variaveis['limite_mais'] and valor > limite_mais:
                                contagem_acima_do_limite[sensor_nome] += 1

                            # Verifica se o sensor faz parte das variáveis para "limite menos"
                            if sensor_nome in limites_variaveis['limite_menos'] and valor < limite_menos:
                                contagem_abaixo_do_limite[sensor_nome] += 1

                # Verificar limites para os valores previstos, comparando com os valores reais
                for sensor_nome, valores_previsao in previsoes.items():
                    contagem_acima_do_limite_previsao[sensor_nome] = 0
                    contagem_abaixo_do_limite_previsao[sensor_nome] = 0

                    # Verificar se o sensor possui um limite de tolerância definido no dicionário
                    if sensor_nome in limites_tolerancia and sensor_nome in valores_atuais:
                        limite_tolerancia = limites_tolerancia[sensor_nome]
                        valores_atuais_sensor = valores_atuais[sensor_nome]  # Os valores reais

                        for valor_previsao, valor_atual in zip(valores_previsao, valores_atuais_sensor):
                            # Definir os limites com base no valor previsto
                            limite_mais_previsao = valor_previsao + limite_tolerancia
                            limite_menos_previsao = valor_previsao - limite_tolerancia

                            # Verificar se o valor atual excede esses limites
                            if valor_atual > limite_mais_previsao:
                                contagem_acima_do_limite_previsao[sensor_nome] += 1
                            if valor_atual < limite_menos_previsao:
                                contagem_abaixo_do_limite_previsao[sensor_nome] += 1
                    else:
                        print(f"Limite de tolerância ou valores atuais não definidos para o sensor: {sensor_nome}")


                    # Ajuste os contadores para 0 ou 1
                    contagem_acima_do_limite_previsao[sensor_nome] = 1 if contagem_acima_do_limite_previsao[sensor_nome] > contagem_limites else 0
                    contagem_abaixo_do_limite_previsao[sensor_nome] = 1 if contagem_abaixo_do_limite_previsao[sensor_nome] > contagem_limites else 0
                    contagem_acima_do_limite[sensor_nome] = 1 if contagem_acima_do_limite.get(sensor_nome, 0) > contagem_limites else 0
                    contagem_abaixo_do_limite[sensor_nome] = 1 if contagem_abaixo_do_limite.get(sensor_nome, 0) > contagem_limites else 0


                # Retornar valores reais e previstos junto com as contagens
                return (
                    nome_equipamento,
                    nome_usina,
                    cod_usina,
                    valores_atuais,                   # Valores reais
                    previsoes,                        # Valores previstos
                    contagem_abaixo_do_limite,        # Contagem de valores abaixo do limite real
                    contagem_acima_do_limite,         # Contagem de valores acima do limite real
                    contagem_abaixo_do_limite_previsao, # Contagem de valores abaixo do limite previsto
                    contagem_acima_do_limite_previsao,  # Contagem de valores acima do limite previsto
                    contagem_equipamentos_ativos,
                )


    except Exception as e:
        print(f"An error occurred in fazer_previsao_sempre_alerta: {e}")
        return {}, {}, {}, {}, {}, {}, {}, {}, {}, {}



lista_parametros_previsao = {
    'Load Speed': {
        'previsao': {
        #     'acima': {
        #         'Pressão do Óleo': {'tipo': 'previsao', 'condicao': 'abaixo'}
        #     },
        #     'abaixo': {
        #         'Pressão do Óleo': {'tipo': 'previsao', 'condicao': 'abaixo'}
        #     }
        },
        # 'real': {
        #     'acima': {
        #         'Pressão do Óleo': {'tipo': 'real', 'condicao': 'abaixo'}
        #     },
        #     'abaixo': {
        #         'Pressão do Óleo': {'tipo': 'real', 'condicao': 'abaixo'}
        #     }
        # }
    },
    'Pressão do Óleo': {
        # 'previsao': {
        #     'acima': {
        #         'Load Speed': {'tipo': 'previsao', 'condicao': 'acima'},
        #         'Temperatura da Água': {'tipo': 'previsao', 'condicao': 'acima'}
        #     },
        #     'abaixo': {
        #         'Load Speed': {'tipo': 'real', 'condicao': 'acima'},
        #         'Temperatura da Água': {'tipo': 'previsao', 'condicao': 'abaixo'}
        #      }
        # },
        'real': {
        #     'acima': {
        #         'Load Speed': {'tipo': 'previsao', 'condicao': 'acima'},
        #         'Temperatura da Água': {'tipo': 'real', 'condicao': 'acima'}
        #     },
    #         'abaixo': {
    #             'Pressão de admissão': {'tipo': 'real', 'condicao': 'acima'},
    #             'Temperatura da Água': {'tipo': 'real', 'condicao': 'acima'}
    #         },
        #     'abaixo': {
        #         'Load Speed': {'tipo': 'previsao', 'condicao': 'acima'},
        # #        'Temperatura da Água': {'tipo': 'real', 'condicao': 'abaixo'}
        #     }
        }
    },
    'Temperatura da Água': {
        # 'previsao': {
        #     'acima': {
        #         'Potência Ativa': {'tipo': 'real', 'condicao': 'acima'}
        #     },
        #     'abaixo': {
        #         'Potência Ativa': {'tipo': 'real', 'condicao': 'abaixo'}
        #     }
        # },
        'real': {
            'acima': {
                'Potência Ativa': {'tipo': 'real', 'condicao': 'acima'}
            },
            'abaixo': {
                'Potência Ativa': {'tipo': 'real', 'condicao': 'abaixo'}
            }
        }
    },
    'Temperatura do ar de admissão': {
        # 'previsao': {
        #     'acima': {
        #         'Potência Ativa': {'tipo': 'real', 'condicao': 'acima'}
        #     },
        #     'abaixo': {
        #         'RPM': {'tipo': 'real', 'condicao': 'abaixo'}
        #     }
        # },
        'real': {
            'acima': {
                'RPM': {'tipo': 'real', 'condicao': 'acima'}
            },
            'abaixo': {
                'Potência Ativa': {'tipo': 'real', 'condicao': 'abaixo'}
            }
        }
    },
    'Consumo': {
        'real': {
            'acima': {
                'Load Speed': {'tipo': 'real', 'condicao': 'acima'}
            },
            'abaixo': {
                'Potência Ativa': {'tipo': 'real', 'condicao': 'abaixo'}
            }
        }
    }
}


        
async def main():

    # Criar conexão com o banco
    pool = await create_pool()
    
    # Obtenha parâmetros necessários
    tabelas = 'sup_geral.leituras'
    cod_equipamentos = await obter_equipamentos_validos(tabelas, pool)
    cod_campo_especificados_processar_equipamentos = ['3','6','7','8','9','10', '11', '16', '19', '23', '24', '114', '21','76','25','20','77', '120']
    cod_campo_especificados = ['3', '114']


    # cod_equipamentos = json.loads(sys.argv[1])  # Carrega usando JSON
    # tabelas = sys.argv[2]
    # cod_campo_especificados_processar_equipamentos = json.loads(sys.argv[3])
    # equipamentos_ativos = json.loads(sys.argv[4])


    await novo_enviar_previsao_valor_equipamento_alerta_novo(cod_equipamentos, tabelas, cod_campo_especificados_processar_equipamentos, pool)

# Rodar o processo assíncrono
if __name__ == '__main__':
    import asyncio
    asyncio.run(main())