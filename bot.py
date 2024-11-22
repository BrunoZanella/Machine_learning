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
#from PIL import Image

TOKEN = "6959786383:AAF6Ob3oZcUf3C0zmAOjjIr8337cV5ZkJX4"

bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())

API_KEY = 'AIzaSyDf9hqXZvxOiCKaFSiIa0byrfEctP5mflI'
genai.configure(api_key=API_KEY)


# bot = Bot(token=TOKEN)
# dp = Dispatcher(bot, storage=MemoryStorage())
# dp.middleware.setup(LoggingMiddleware())

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


cnx = mysql.connector.connect(
  host="192.168.4.50",
  user="bruno",
  password="superbancoml"
#  database="sup_geral"
)

cursor = cnx.cursor(buffered=True)

async def create_pool():
    # Conexão assíncrona
    pool = await aiomysql.create_pool(
        host="192.168.4.50",
        user="bruno",
        password="superbancoml",
        db="machine_learning",
        minsize=1,
        maxsize=10
    )
    return pool




# Criar uma fila para ações prioritárias (botões)
botao_queue = asyncio.Queue()

# Função para processar botões com prioridade
async def processar_botoes_prioritarios():
    while True:
        # Aguarda por novas interações na fila
        tarefa_prioritaria = await botao_queue.get()
        
        # Processa a tarefa prioritária (no caso, a ação do botão)
        try:
            await tarefa_prioritaria
        except Exception as e:
            print(f"Erro ao processar tarefa prioritária: {e}")
        finally:
            botao_queue.task_done()
            

# Dicionário para armazenar temporariamente os dados do relatório
user_reports = {}

# Dicionário de funcionamento
funcionamento_map = {
    "Controle de demanda": "CONTROLE DE DEMANDA",
    "Horário de ponta": "HORARIO DE PONTA",
    "Operação contínua": "OPERACAO CONTINUA",
    "Falta de energia": "FALTA DE ENERGIA",
    "Geral": "Geral",
    "Agrogera GO": "aggo",
    "Agrogera MG": "agmg",
    "Agrogera BA": "AGROGERA",
}

# Chave inversa para mapear de volta para o nome original do teclado
reverse_funcionamento_map = {v: k for k, v in funcionamento_map.items()}


# Inicializando os modelos Gemini-Pro e Gemini-Pro Vision
gemini_model = genai.GenerativeModel('gemini-pro')
gemini_vision_model = genai.GenerativeModel("gemini-1.5-flash")

@dp.message_handler(content_types=[types.ContentType.DOCUMENT, types.ContentType.PHOTO])
async def handle_messages(message: types.Message):
    try:
        if message.content_type == types.ContentType.DOCUMENT:
            pass
        if message.content_type == types.ContentType.PHOTO:
            # Baixando a imagem
            photo = message.photo[-1]
            photo_data = await photo.download(destination=tempfile.mktemp(suffix='.jpg'))
        
            # Abrindo a imagem com PIL
            image = PILImage.open(photo_data.name)
            
            # Iniciando uma sessão de chat com o modelo Gemini-Pro Vision
            chat = gemini_vision_model.start_chat(history=[])
                
            # Enviando a imagem para o modelo Gemini-Pro Vision
            response = chat.send_message(image)
            response_text = response.text
            
            chat_gemini = gemini_model.start_chat(history=[])

            prompt_general = (
                "faça a seguinte descrição de forma clara e concisa em português:\n"
                f"{response_text}\n"
                "Use uma linguagem formal e organize as informações de maneira fácil de ler."
            )
            response_general = chat_gemini.send_message(prompt_general)
            formatted_general_report = response_general.text
                
        # Enviando a resposta de volta ao usuário
        await message.answer(formatted_general_report)
    
    except Exception as e:
        await message.answer(f"Ocorreu um erro ao processar sua mensagem: {e}")


async def fetch_alarm_descriptions(pool, alarm_codes):
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor() as cursor:
            descriptions = {}
            for code_tuple in alarm_codes:
                code = code_tuple[0]
                await cursor.execute("SELECT descricao FROM sup_geral.lista_alarmes WHERE codigo = %s", (code,))
                result = await cursor.fetchone()
                if result:
                    descriptions[code] = result[0]
                else:
                    descriptions[code] = "Descrição não encontrada"
            return descriptions

async def fetch_alarm_name(pool, alarm_codes):
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor() as cursor:
            descriptions = {}
            for code_tuple in alarm_codes:
                code = code_tuple[0]
                await cursor.execute("SELECT nome FROM sup_geral.lista_alarmes WHERE codigo = %s", (code,))
                result = await cursor.fetchone()
                if result:
                    descriptions[code] = result[0]
                else:
                    descriptions[code] = "Nome não encontrado"
            return descriptions
        
def split_report(report, max_length=4096):
    parts = []
    while len(report) > max_length:
        split_index = report[:max_length].rfind('\n\n')
        if split_index == -1:
            split_index = max_length
        parts.append(report[:split_index])
        report = report[split_index:]
    parts.append(report)
    return parts




async def Pressao_do_oleo(pool, cod_equipamento, data_previsto, data_cadastro_quebra):
    if not cod_equipamento:
        return "", "N/A", "N/A", "N/A"

    # Format data_previsto to match table name format
    data_previsto_str = data_previsto.strftime('%Y-%m-%d %H:%M:%S')
    data_previsto_dt = datetime.strptime(data_previsto_str, '%Y-%m-%d %H:%M:%S')
    ano_mes = data_previsto_dt.strftime('%Y_%m')
    tabela_nome = f"sup_geral.log_leituras_{ano_mes}"

    # Get previous month table name
    previous_month_dt = data_previsto_dt - timedelta(days=30)
    previous_ano_mes = previous_month_dt.strftime('%Y_%m')
    previous_tabela_nome = f"sup_geral.log_leituras_{previous_ano_mes}"

    # Define end date
    if data_cadastro_quebra in ['Não houve falha', 'Em operação']:
        end_date = data_previsto_dt + timedelta(hours=2)
    else:
        end_date = datetime.strptime(str(data_cadastro_quebra), '%Y-%m-%d %H:%M:%S')

    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            # Fetch average Pressao do oleo value for the previous month
            query_previous_month = f"""
                SELECT AVG(valor) AS avg_pressao_oleo_mes_anterior
                FROM {previous_tabela_nome}
                WHERE cod_equipamento = %s
                AND cod_campo = 21
                AND valor <> 0
            """
            await cursor.execute(query_previous_month, (cod_equipamento,))
            result_previous_month = await cursor.fetchone()

            # Fetch average and max Pressao do oleo value for the defined period
            query_period = f"""
                SELECT AVG(valor) AS avg_pressao_oleo_periodo, MAX(valor) AS max_pressao_oleo_periodo
                FROM {tabela_nome}
                WHERE cod_equipamento = %s
                AND data_cadastro >= %s
                AND data_cadastro <= %s
                AND cod_campo = 21
                AND valor <> 0
            """
            await cursor.execute(query_period, (cod_equipamento, data_previsto, end_date))
            result_period = await cursor.fetchone()

    avg_pressao_oleo_mes_anterior = result_previous_month['avg_pressao_oleo_mes_anterior']
    avg_pressao_oleo_periodo = result_period['avg_pressao_oleo_periodo']
    max_pressao_oleo_periodo = result_period['max_pressao_oleo_periodo']

    avg_pressao_oleo_mes_anterior_formatted = (
        f"{float(avg_pressao_oleo_mes_anterior):.2f}" if avg_pressao_oleo_mes_anterior is not None else "N/A"
    )
    avg_pressao_oleo_periodo_formatted = (
        f"{float(avg_pressao_oleo_periodo):.2f}" if avg_pressao_oleo_periodo is not None else "N/A"
    )
    max_pressao_oleo_periodo_formatted = (
        f"{float(max_pressao_oleo_periodo):.2f}" if max_pressao_oleo_periodo is not None else "N/A"
    )

    pressao_oleo_data = (
        f"    Pressão do óleo média do mês anterior: {avg_pressao_oleo_mes_anterior_formatted}\n"
        f"    Pressão do óleo média do período funcionando: {avg_pressao_oleo_periodo_formatted}\n"
        f"    Valor da Pressão do óleo mais alto do período funcionando: {max_pressao_oleo_periodo_formatted}\n"
    )

    return pressao_oleo_data, avg_pressao_oleo_mes_anterior_formatted, avg_pressao_oleo_periodo_formatted, max_pressao_oleo_periodo_formatted

async def Pressao_do_Combustivel(pool, cod_equipamento, data_previsto, data_cadastro_quebra):
    if not cod_equipamento:
        return "", "N/A", "N/A", "N/A"

    # Format data_previsto to match table name format
    data_previsto_str = data_previsto.strftime('%Y-%m-%d %H:%M:%S')
    data_previsto_dt = datetime.strptime(data_previsto_str, '%Y-%m-%d %H:%M:%S')
    ano_mes = data_previsto_dt.strftime('%Y_%m')
    tabela_nome = f"sup_geral.log_leituras_{ano_mes}"

    # Get previous month table name
    previous_month_dt = data_previsto_dt - timedelta(days=30)
    previous_ano_mes = previous_month_dt.strftime('%Y_%m')
    previous_tabela_nome = f"sup_geral.log_leituras_{previous_ano_mes}"

    # Define end date
    if data_cadastro_quebra in ['Não houve falha', 'Em operação']:
        end_date = data_previsto_dt + timedelta(hours=2)
    else:
        end_date = datetime.strptime(str(data_cadastro_quebra), '%Y-%m-%d %H:%M:%S')

    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            # Fetch average Pressao do oleo value for the previous month
            query_previous_month = f"""
                SELECT AVG(valor) AS avg_pressao_combustivel_mes_anterior
                FROM {previous_tabela_nome}
                WHERE cod_equipamento = %s
                AND cod_campo = 129
                AND valor <> 0
            """
            await cursor.execute(query_previous_month, (cod_equipamento,))
            result_previous_month = await cursor.fetchone()

            # Fetch average and max Pressao do oleo value for the defined period
            query_period = f"""
                SELECT AVG(valor) AS avg_pressao_combustivel_periodo, MAX(valor) AS max_pressao_combustivel_periodo
                FROM {tabela_nome}
                WHERE cod_equipamento = %s
                AND data_cadastro >= %s
                AND data_cadastro <= %s
                AND cod_campo = 129
                AND valor <> 0
            """
            await cursor.execute(query_period, (cod_equipamento, data_previsto, end_date))
            result_period = await cursor.fetchone()

    avg_pressao_combustivel_mes_anterior = result_previous_month['avg_pressao_combustivel_mes_anterior']
    avg_pressao_combustivel_periodo = result_period['avg_pressao_combustivel_periodo']
    max_pressao_combustivel_periodo = result_period['max_pressao_combustivel_periodo']

    avg_pressao_combustivel_mes_anterior_formatted = (
        f"{float(avg_pressao_combustivel_mes_anterior):.2f}" if avg_pressao_combustivel_mes_anterior is not None else "N/A"
    )
    avg_pressao_combustivel_periodo_formatted = (
        f"{float(avg_pressao_combustivel_periodo):.2f}" if avg_pressao_combustivel_periodo is not None else "N/A"
    )
    max_pressao_combustivel_periodo_formatted = (
        f"{float(max_pressao_combustivel_periodo):.2f}" if max_pressao_combustivel_periodo is not None else "N/A"
    )

    pressao_combustivel_data = (
        f"    Pressão do Combustivel média do mês anterior: {avg_pressao_combustivel_mes_anterior_formatted}\n"
        f"    Pressão do Combustivel média do período funcionando: {avg_pressao_combustivel_periodo_formatted}\n"
        f"    Valor da Pressão do Combustivel mais alto do período funcionando: {max_pressao_combustivel_periodo_formatted}\n"
    )

    return pressao_combustivel_data, avg_pressao_combustivel_mes_anterior_formatted, avg_pressao_combustivel_periodo_formatted, max_pressao_combustivel_periodo_formatted



async def Temperatura_agua(pool, cod_equipamento, data_previsto, data_cadastro_quebra):
    if not cod_equipamento:
        return "", "N/A", "N/A", "N/A"

    # Format data_previsto to match table name format
    data_previsto_str = data_previsto.strftime('%Y-%m-%d %H:%M:%S')
    data_previsto_dt = datetime.strptime(data_previsto_str, '%Y-%m-%d %H:%M:%S')
    ano_mes = data_previsto_dt.strftime('%Y_%m')
    tabela_nome = f"sup_geral.log_leituras_{ano_mes}"

    # Get previous month table name
    previous_month_dt = data_previsto_dt - timedelta(days=30)
    previous_ano_mes = previous_month_dt.strftime('%Y_%m')
    previous_tabela_nome = f"sup_geral.log_leituras_{previous_ano_mes}"

    # Define end date
    if data_cadastro_quebra in ['Não houve falha', 'Em operação']:
        end_date = data_previsto_dt + timedelta(hours=2)
    else:
        end_date = datetime.strptime(str(data_cadastro_quebra), '%Y-%m-%d %H:%M:%S')

    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            # Fetch average Pressao do oleo value for the previous month
            query_previous_month = f"""
                SELECT AVG(valor) AS avg_temperatura_agua_mes_anterior
                FROM {previous_tabela_nome}
                WHERE cod_equipamento = %s
                AND cod_campo = 25
                AND valor <> 0
            """
            await cursor.execute(query_previous_month, (cod_equipamento,))
            result_previous_month = await cursor.fetchone()

            # Fetch average and max Pressao do oleo value for the defined period
            query_period = f"""
                SELECT AVG(valor) AS avg_temperatura_agua_periodo, MAX(valor) AS max_temperatura_agua_periodo
                FROM {tabela_nome}
                WHERE cod_equipamento = %s
                AND data_cadastro >= %s
                AND data_cadastro <= %s
                AND cod_campo = 25
                AND valor <> 0
            """
            await cursor.execute(query_period, (cod_equipamento, data_previsto, end_date))
            result_period = await cursor.fetchone()

    avg_temperatura_agua_mes_anterior = result_previous_month['avg_temperatura_agua_mes_anterior']
    avg_temperatura_agua_periodo = result_period['avg_temperatura_agua_periodo']
    max_temperatura_agua_periodo = result_period['max_temperatura_agua_periodo']

    avg_temperatura_agua_mes_anterior_formatted = (
    #    f"{float(avg_temperatura_agua_mes_anterior):.2f}" if avg_temperatura_agua_mes_anterior is not None else "N/A" # duas casas decimais
        f"{round(float(avg_temperatura_agua_mes_anterior))}" if avg_temperatura_agua_mes_anterior is not None else "N/A"

    )
    avg_temperatura_agua_periodo_formatted = (
    #    f"{float(avg_temperatura_agua_periodo):.2f}" if avg_temperatura_agua_periodo is not None else "N/A"
        f"{round(float(avg_temperatura_agua_periodo))}" if avg_temperatura_agua_periodo is not None else "N/A"
    )
    max_temperatura_agua_periodo_formatted = (
    #    f"{float(max_temperatura_agua_periodo):.2f}" if max_temperatura_agua_periodo is not None else "N/A"
        f"{round(float(max_temperatura_agua_periodo))}" if max_temperatura_agua_periodo is not None else "N/A"
    )

    temperatura_agua_data = (
        f"    temperatura da água média do mês anterior: {avg_temperatura_agua_mes_anterior_formatted}\n"
        f"    temperatura da água média do período funcionando: {avg_temperatura_agua_periodo_formatted}\n"
        f"    Valor da temperatura da água mais alto do período funcionando: {max_temperatura_agua_periodo_formatted}\n"
    )

    return temperatura_agua_data, avg_temperatura_agua_mes_anterior_formatted, avg_temperatura_agua_periodo_formatted, max_temperatura_agua_periodo_formatted
    
    
    
async def Tensao_L1_L2(pool, cod_equipamento, data_previsto, data_cadastro_quebra):
    if not cod_equipamento:
        return ""

    # Format data_previsto to match table name format
    data_previsto_str = data_previsto.strftime('%Y-%m-%d %H:%M:%S')
    data_previsto_dt = datetime.strptime(data_previsto_str, '%Y-%m-%d %H:%M:%S')
    ano_mes = data_previsto_dt.strftime('%Y_%m')
    tabela_nome = f"sup_geral.log_leituras_{ano_mes}"

    # Define end date
    if data_cadastro_quebra in ['Não houve falha', 'Em operação']:
        end_date = data_previsto_dt + timedelta(hours=2)
    else:
        end_date = datetime.strptime(str(data_cadastro_quebra), '%Y-%m-%d %H:%M:%S')

    # Fetch average Pressao do oleo value
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            query = f"""
                SELECT AVG(valor) AS avg_tensao_l1_l2
                FROM {tabela_nome}
                WHERE cod_equipamento = %s
                AND data_cadastro >= %s
                AND data_cadastro <= %s
                AND cod_campo = 6
                AND valor <> 0
            """
            await cursor.execute(query, (cod_equipamento, data_previsto, end_date))
            result = await cursor.fetchone()

    if result and result['avg_tensao_l1_l2'] is not None:
    #    return f"    Tensão L1 L2 média: {result['avg_tensao_l1_l2']:.2f}\n" # para duas casas decimais
        return f"    Tensão L1 L2 média: {round(result['avg_tensao_l1_l2'])}\n"

    return ""

async def Tensao_L2_L3(pool, cod_equipamento, data_previsto, data_cadastro_quebra):
    if not cod_equipamento:
        return ""

    # Format data_previsto to match table name format
    data_previsto_str = data_previsto.strftime('%Y-%m-%d %H:%M:%S')
    data_previsto_dt = datetime.strptime(data_previsto_str, '%Y-%m-%d %H:%M:%S')
    ano_mes = data_previsto_dt.strftime('%Y_%m')
    tabela_nome = f"sup_geral.log_leituras_{ano_mes}"

    # Define end date
    if data_cadastro_quebra in ['Não houve falha', 'Em operação']:
        end_date = data_previsto_dt + timedelta(hours=2)
    else:
        end_date = datetime.strptime(str(data_cadastro_quebra), '%Y-%m-%d %H:%M:%S')

    # Fetch average Pressao do oleo value
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            query = f"""
                SELECT AVG(valor) AS avg_tensao_l2_l3
                FROM {tabela_nome}
                WHERE cod_equipamento = %s
                AND data_cadastro >= %s
                AND data_cadastro <= %s
                AND cod_campo = 7
                AND valor <> 0
            """
            await cursor.execute(query, (cod_equipamento, data_previsto, end_date))
            result = await cursor.fetchone()

    if result and result['avg_tensao_l2_l3'] is not None:
    #    return f"    Tensão L2 L3 média: {result['avg_tensao_l2_l3']:.2f}\n"
        return f"    Tensão L2 L3 média: {round(result['avg_tensao_l2_l3'])}\n"

    return ""

async def Tensao_L3_L1(pool, cod_equipamento, data_previsto, data_cadastro_quebra):
    if not cod_equipamento:
        return ""

    # Format data_previsto to match table name format
    data_previsto_str = data_previsto.strftime('%Y-%m-%d %H:%M:%S')
    data_previsto_dt = datetime.strptime(data_previsto_str, '%Y-%m-%d %H:%M:%S')
    ano_mes = data_previsto_dt.strftime('%Y_%m')
    tabela_nome = f"sup_geral.log_leituras_{ano_mes}"

    # Define end date
    if data_cadastro_quebra in ['Não houve falha', 'Em operação']:
        end_date = data_previsto_dt + timedelta(hours=2)
    else:
        end_date = datetime.strptime(str(data_cadastro_quebra), '%Y-%m-%d %H:%M:%S')

    # Fetch average Pressao do oleo value
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            query = f"""
                SELECT AVG(valor) AS avg_tensao_l3_l1
                FROM {tabela_nome}
                WHERE cod_equipamento = %s
                AND data_cadastro >= %s
                AND data_cadastro <= %s
                AND cod_campo = 8
                AND valor <> 0
            """
            await cursor.execute(query, (cod_equipamento, data_previsto, end_date))
            result = await cursor.fetchone()

    if result and result['avg_tensao_l3_l1'] is not None:
    #    return f"    Tensão L3 L1 média: {result['avg_tensao_l3_l1']:.2f}\n"
        return f"    Tensão L3 L1 média: {round(result['avg_tensao_l3_l1'])}\n"

    return ""



async def Corrente_L1 (pool, cod_equipamento, data_previsto, data_cadastro_quebra):
    if not cod_equipamento:
        return ""

    # Format data_previsto to match table name format
    data_previsto_str = data_previsto.strftime('%Y-%m-%d %H:%M:%S')
    data_previsto_dt = datetime.strptime(data_previsto_str, '%Y-%m-%d %H:%M:%S')
    ano_mes = data_previsto_dt.strftime('%Y_%m')
    tabela_nome = f"sup_geral.log_leituras_{ano_mes}"

    # Define end date
    if data_cadastro_quebra in ['Não houve falha', 'Em operação']:
        end_date = data_previsto_dt + timedelta(hours=2)
    else:
        end_date = datetime.strptime(str(data_cadastro_quebra), '%Y-%m-%d %H:%M:%S')

    # Fetch average Pressao do oleo value
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            query = f"""
                SELECT AVG(valor) AS avg_corrente_l1
                FROM {tabela_nome}
                WHERE cod_equipamento = %s
                AND data_cadastro >= %s
                AND data_cadastro <= %s
                AND cod_campo = 9
                AND valor <> 0
            """
            await cursor.execute(query, (cod_equipamento, data_previsto, end_date))
            result = await cursor.fetchone()

    if result and result['avg_corrente_l1'] is not None:
    #    return f"    Corrente L1 média: {result['avg_corrente_l1']:.2f}\n" # para duas casas decimais
        return f"    Corrente L1 média: {round(result['avg_corrente_l1'])}\n"

    return ""

async def Corrente_L2 (pool, cod_equipamento, data_previsto, data_cadastro_quebra):
    if not cod_equipamento:
        return ""

    # Format data_previsto to match table name format
    data_previsto_str = data_previsto.strftime('%Y-%m-%d %H:%M:%S')
    data_previsto_dt = datetime.strptime(data_previsto_str, '%Y-%m-%d %H:%M:%S')
    ano_mes = data_previsto_dt.strftime('%Y_%m')
    tabela_nome = f"sup_geral.log_leituras_{ano_mes}"

    # Define end date
    if data_cadastro_quebra in ['Não houve falha', 'Em operação']:
        end_date = data_previsto_dt + timedelta(hours=2)
    else:
        end_date = datetime.strptime(str(data_cadastro_quebra), '%Y-%m-%d %H:%M:%S')

    # Fetch average Pressao do oleo value
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            query = f"""
                SELECT AVG(valor) AS avg_corrente_l2
                FROM {tabela_nome}
                WHERE cod_equipamento = %s
                AND data_cadastro >= %s
                AND data_cadastro <= %s
                AND cod_campo = 10
                AND valor <> 0
            """
            await cursor.execute(query, (cod_equipamento, data_previsto, end_date))
            result = await cursor.fetchone()

    if result and result['avg_corrente_l2'] is not None:
    #    return f"    Corrente L2 média: {result['avg_corrente_l2']:.2f}\n"
        return f"    Corrente L2 média: {round(result['avg_corrente_l2'])}\n"

    return ""

async def Corrente_L3 (pool, cod_equipamento, data_previsto, data_cadastro_quebra):
    if not cod_equipamento:
        return ""

    # Format data_previsto to match table name format
    data_previsto_str = data_previsto.strftime('%Y-%m-%d %H:%M:%S')
    data_previsto_dt = datetime.strptime(data_previsto_str, '%Y-%m-%d %H:%M:%S')
    ano_mes = data_previsto_dt.strftime('%Y_%m')
    tabela_nome = f"sup_geral.log_leituras_{ano_mes}"

    # Define end date
    if data_cadastro_quebra in ['Não houve falha', 'Em operação']:
        end_date = data_previsto_dt + timedelta(hours=2)
    else:
        end_date = datetime.strptime(str(data_cadastro_quebra), '%Y-%m-%d %H:%M:%S')

    # Fetch average Pressao do oleo value
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            query = f"""
                SELECT AVG(valor) AS avg_corrente_l3
                FROM {tabela_nome}
                WHERE cod_equipamento = %s
                AND data_cadastro >= %s
                AND data_cadastro <= %s
                AND cod_campo = 11
                AND valor <> 0
            """
            await cursor.execute(query, (cod_equipamento, data_previsto, end_date))
            result = await cursor.fetchone()

    if result and result['avg_corrente_l3'] is not None:
    #    return f"    Corrente L3 média: {result['avg_corrente_l3']:.2f}\n"
        return f"    Corrente L3 média: {round(result['avg_corrente_l3'])}\n"

    return ""



async def fetch_report_data(pool, period, user_id, funcionamento):
    try:
        async with pool.acquire() as conn:
            await conn.ping(reconnect=True)  # Forçar reconexão
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT cod_usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
                result = await cursor.fetchone()
                if not result:
                    return "Usuário não encontrado.", "", ""

                cod_usuario = result[0]

                await cursor.execute("SELECT cod_usina FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
                cod_usinas = await cursor.fetchall()

                if not cod_usinas:
                    return ["Nenhuma usina associada ao usuário."], "", ""

                usinas = [usina[0] for usina in cod_usinas]

                if funcionamento == "Geral":
                    await cursor.execute("SELECT codigo FROM sup_geral.modelo_funcionamento")
                    modelos_funcionamento = await cursor.fetchall()
                    if not modelos_funcionamento:
                        return ["Nenhum modelo de funcionamento encontrado."], "", ""
                    modelos_funcionamento = [modelo[0] for modelo in modelos_funcionamento]
                    await cursor.execute("SELECT codigo FROM sup_geral.usinas WHERE codigo IN (%s) AND cod_modelo_funcionamento IN (%s)" % (','.join(['%s'] * len(usinas)), ','.join(['%s'] * len(modelos_funcionamento))), usinas + modelos_funcionamento)
                    filtered_usinas = await cursor.fetchall()

                elif funcionamento in ["aggo", "agmg", "AGROGERA"]:
                    await cursor.execute("SELECT codigo FROM sup_geral.usinas WHERE codigo IN (%s) AND obs = %s" % (','.join(['%s'] * len(usinas)), '%s'), usinas + [funcionamento])
                    filtered_usinas = await cursor.fetchall()
                else:
                    await cursor.execute("SELECT codigo FROM sup_geral.modelo_funcionamento WHERE descricao = %s", (funcionamento,))
                    modelos_funcionamento = await cursor.fetchall()
                    if not modelos_funcionamento:
                        return ["Nenhum modelo de funcionamento encontrado."], "", ""
                    modelos_funcionamento = [modelo[0] for modelo in modelos_funcionamento]
                    await cursor.execute("SELECT codigo FROM sup_geral.usinas WHERE codigo IN (%s) AND cod_modelo_funcionamento IN (%s)" % (','.join(['%s'] * len(usinas)), ','.join(['%s'] * len(modelos_funcionamento))), usinas + modelos_funcionamento)
                    filtered_usinas = await cursor.fetchall()

                if not filtered_usinas:
                    return ["Nenhuma usina encontrada com o modelo de funcionamento selecionado."], "", ""

                filtered_usinas = [usina[0] for usina in filtered_usinas]

                query = """
                    SELECT 
                        l.cod_equipamento, l.cod_usina, l.data_cadastro_previsto, l.data_cadastro_previsto_saida, l.data_cadastro_quebra, 
                        u.nome AS nome_usina, e.nome AS nome_equipamento
                    FROM 
                        machine_learning.log_relatorio_quebras l
                    JOIN
                        sup_geral.usinas u ON l.cod_usina = u.codigo
                    JOIN
                        sup_geral.equipamentos e ON l.cod_equipamento = e.codigo
                    WHERE 
                        l.cod_usina IN (%s) 
                        AND (l.data_cadastro_quebra >= NOW() - INTERVAL %s DAY OR l.data_cadastro_quebra IS NULL)
                        AND (l.data_cadastro_previsto >= NOW() - INTERVAL %s DAY)
                    ORDER BY 
                        l.cod_usina, l.cod_equipamento, l.data_cadastro_previsto
                """ % (', '.join(['%s'] * len(filtered_usinas)), '%s', '%s')
                await cursor.execute(query, filtered_usinas + [period, period])
                log_data = await cursor.fetchall()

                if not log_data:
                    return ["Nenhum dado encontrado para o período selecionado."], "", ""

                await cursor.execute("""
                    SELECT cod_usina, COUNT(*) AS num_geradores
                    FROM sup_geral.equipamentos
                    WHERE ativo = 1 
                    AND cod_tipo_equipamento IN (1, 3, 4, 12, 16, 18, 20, 22, 23, 27, 29, 33, 37, 40, 41, 43, 51, 55, 56)
                    GROUP BY cod_usina
                """)
                geradores_por_usina = await cursor.fetchall()
                geradores_por_usina_dict = {usina[0]: usina[1] for usina in geradores_por_usina}

                await cursor.execute("""
                    SELECT cod_equipamento 
                    FROM machine_learning.leituras_consecutivas 
                    WHERE alerta = 1 AND cod_campo = 114
                """)
                equipamentos_alerta = await cursor.fetchall()
                equipamentos_alerta_set = set(equipamento[0] for equipamento in equipamentos_alerta)

                telegram_report = "Relatório resumido:\n\n"
                usina_equipamento_map = {}
                usina_equipamento_map_gemini = {}

                dados_gemini = ""
                detailed_report = ""
                
                # Contadores para equipamentos em alerta e com tempo_total como data
                total_equipamentos_alerta = 0
                total_equipamentos_com_data = 0

                # Process log data and build content for each usina
                for row in log_data:
                    cod_equipamento, cod_usina, data_previsto, data_previsto_saida, data_quebra, nome_usina, nome_equipamento = row
                    total_equipamentos_alerta += 1                        

                    # Converte as strings de data para objetos datetime
                    data_previsto_dt = datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                    data_previsto_saida_dt = None
                    if data_previsto_saida:
                        data_previsto_saida_dt = datetime.strptime(str(data_previsto_saida), '%Y-%m-%d %H:%M:%S')

                    # Verificar se o equipamento está em alerta atualmente
                    if cod_equipamento in equipamentos_alerta_set:

#                        if not data_quebra and (not data_previsto_saida_dt or data_previsto_saida_dt >= current_time):
                        if not data_quebra:
                            data_quebra = 'Em operação'
                            tempo_total = 'Em operação'
                            tempo_anormalidade = 'Em operação' if not data_previsto_saida_dt else data_previsto_saida_dt - data_previsto_dt
                        else:
                            data_quebra = 'Não houve falha'
                            tempo_total = 'Não houve falha'
                            tempo_anormalidade = 'Não houve falha' if not data_previsto_saida_dt else data_previsto_saida_dt - data_previsto_dt
                    else:
                        if not data_quebra:
                            data_quebra = 'Não houve falha'
                            tempo_total = 'Não houve falha'
                            tempo_anormalidade = 'Não houve falha' if not data_previsto_saida_dt else data_previsto_saida_dt - data_previsto_dt
                        else:
                            data_previsto_saida = data_previsto_saida if data_previsto_saida else 'Indefinido'
                            if data_quebra not in ['Não houve falha', 'Em operação']:
                                tempo_anormalidade = 'Sem dados'
                                if data_previsto_saida == 'Indefinido':
                                    tempo_total = datetime.strptime(str(data_quebra), '%Y-%m-%d %H:%M:%S') - data_previsto_dt
                                    total_equipamentos_com_data += 1
                                else:
                                    tempo_anormalidade = data_previsto_saida_dt - data_previsto_dt if data_previsto_saida_dt else 'Sem dados'
                                    tempo_total = datetime.strptime(str(data_quebra), '%Y-%m-%d %H:%M:%S') - data_previsto_dt
                                    total_equipamentos_com_data += 1
                            else:
                                tempo_anormalidade = 'Sem dados'
                                tempo_total = 'Sem dados'



                    # Consultar a tabela valores_previsao para obter mais dados
                    await cursor.execute("""
                        SELECT DISTINCT alerta_80, alerta_100, previsao, valores_reais, valores_previstos, GROUP_CONCAT(DISTINCT alarmes) 
                        FROM machine_learning.valores_previsao 
                        WHERE cod_equipamento = %s 
                        AND ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 1000
                        GROUP BY cod_equipamento
                    """, (cod_equipamento, data_previsto))
                    previsao_data = await cursor.fetchone()

                    if previsao_data:
                        alerta_80, alerta_100, previsao, valores_reais, valores_previstos, alarmes = previsao_data
                        alerta_status = 'Alerta 80' if alerta_80 else 'Alerta 100' if alerta_100 else 'Previsão' if previsao else 'Nenhum alerta'

                        # Converter strings de valores para listas de floats
                        valores_reais = list(map(float, valores_reais.split(',')))
                        valores_previstos = list(map(float, valores_previstos.split(',')))

                        # Calcular a média dos valores reais
                    #    media_valores_reais = sum(valores_reais) / len(valores_reais)
                        
                        media_valores_reais = sum(valores_reais) / len(valores_reais) if valores_reais else 0.0

                        # Calcular a média dos valores previstos
                    #    media_valores_previstos = sum(valores_previstos) / len(valores_previstos)

                        media_valores_previstos = sum(valores_previstos) / len(valores_previstos) if valores_previstos else 0.0

                        # Calcular a porcentagem de diferença entre o real e o previsto
                    #    diferenca_percentual = ((media_valores_previstos - media_valores_reais) / media_valores_reais) * 100

                        # Calcular a porcentagem de diferença entre o real e o previsto, evitando divisão por zero
                        if media_valores_reais != 0:
                            diferenca_percentual = ((media_valores_previstos - media_valores_reais) / media_valores_reais) * 100
                        else:
                            diferenca_percentual = 0.0  # ou algum outro valor ou tratamento específico


                        # Formatar a saída com duas casas decimais e sinal de mais ou menos
                        if diferenca_percentual < 0:
                        #    diferenca_formatada = f"menos {abs(diferenca_percentual):.2f}%"
                            diferenca_formatada = f"{diferenca_percentual:.2f}"
                        else:
                        #    diferenca_formatada = f"mais {diferenca_percentual:.2f}%"
                            diferenca_formatada = f"{diferenca_percentual:.2f}"
        
        
                        # # Obter descrições dos alarmes
                        # if alarmes:
                        #     # Processar a string de alarmes para extrair os códigos
                        #     alarm_codes = []
                        #     for code in alarmes.split(','):
                        #         try:
                        #             alarm_codes.append((int(code.strip().strip('()')),))
                        #         except ValueError:
                        #             continue  # Ignorar códigos inválidos

                        # Obter descrições dos alarmes
                        if alarmes:
                            # Processar a string de alarmes para extrair os códigos
                            alarm_codes = []
                            for code in alarmes.split(','):
                                code = code.strip().strip('()')  # Remove parênteses e espaços extras
                                if code.isdigit():  # Verifica se `code` é um número válido
                                    try:
                                        alarm_codes.append((int(code),))
                                    except ValueError:
                                        continue  # Ignorar códigos inválidos




                            # Ordenar os códigos de alarme do mais antigo para o mais novo
                            alarm_codes.sort()

                            # Obter descrições dos alarmes
                            alarm_descriptions = await fetch_alarm_descriptions(pool, alarm_codes)
                            alarm_name = await fetch_alarm_name(pool, alarm_codes)

                            # Criar uma lista de descrições de alarmes ordenadas pelos códigos
                            ordered_alarm_descriptions = [alarm_descriptions.get(code[0], "Descrição não encontrada") for code in alarm_codes]
                            ordered_alarm_nome = [alarm_name.get(code[0], "Descrição não encontrada") for code in alarm_codes]

                            # Formatar alarmes_text com a descrição do primeiro alarme e contagem dos restantes
                            if ordered_alarm_nome:
                                first_alarm = ordered_alarm_nome[0].split(' - ')[-1].split(',')[0].strip()  # Extrair parte inicial da descrição do primeiro alarme
                                remaining_count = len(ordered_alarm_nome) - 1  # Contar os alarmes restantes
                                alarmes_text_tabela = f"{first_alarm} e mais {remaining_count}" if remaining_count > 0 else first_alarm
                            #    alarmes_text = ', '.join(alarm_descriptions.get(code[0], "Descrição não encontrada") for code in alarm_codes)

                                # Limpar os números iniciais das descrições dos alarmes
                                cleaned_descriptions = [re.sub(r'^\d+\s*-\s*', '', desc) for desc in ordered_alarm_descriptions]
                                alarmes_text = ', '.join(cleaned_descriptions)
                                    
                            else:
                                alarmes_text_tabela = 'Sem alarmes'
                        else:
                            alarmes_text = 'Sem alarmes'
                            alarmes_text_tabela = 'Sem alarmes'

    #                    pressao_oleo_data = await Pressao_do_oleo(pool, cod_equipamento, data_previsto, data_quebra)
                        pressao_oleo_data, avg_pressao_oleo_mes_anterior, avg_pressao_oleo_periodo, max_pressao_oleo_periodo = await Pressao_do_oleo(pool, cod_equipamento, data_previsto, data_quebra)
                        pressao_combustivel_data, avg_pressao_combustivel_mes_anterior, avg_pressao_combustivel_periodo, max_pressao_combustivel_periodo = await Pressao_do_Combustivel(pool, cod_equipamento, data_previsto, data_quebra)
                        temperatura_agua_data, avg_temperatura_agua_mes_anterior, avg_temperatura_agua_periodo, max_temperatura_agua_periodo = await Temperatura_agua(pool, cod_equipamento, data_previsto, data_quebra)

                        tensao_l1_l2_data = await Tensao_L1_L2(pool, cod_equipamento, data_previsto, data_quebra)
                        Tensao_L2_L3_data = await Tensao_L2_L3(pool, cod_equipamento, data_previsto, data_quebra)
                        Tensao_L3_L1_data = await Tensao_L3_L1(pool, cod_equipamento, data_previsto, data_quebra)
                        
                        Corrente_L1_data = await Corrente_L1(pool, cod_equipamento, data_previsto, data_quebra)
                        Corrente_L2_data = await Corrente_L2(pool, cod_equipamento, data_previsto, data_quebra)
                        Corrente_L3_data = await Corrente_L3(pool, cod_equipamento, data_previsto, data_quebra)

                        problem_description = f"""
                            <tr>
                                <td>{nome_usina}</td>
                                <td>{nome_equipamento}</td>
                                <td>{data_previsto}</td>
                                <td>{tempo_anormalidade}</td>
                                <td>{tempo_total}</td>
                                <td>{alerta_status}</td>
                                <td>{alarmes_text_tabela}</td>
                            </tr>
                        """
                        
                        # problem_description = f"""
                        #     <tr>
                        #         <td>{nome_usina}</td>
                        #         <td>{nome_equipamento}</td>
                        #         <td>{data_previsto}</td>
                        #         <td>{tempo_anormalidade}</td>
                        #         <td>{tempo_total}</td>
                        #         <td>{alerta_status}</td>
                        #         <td>{max(valores_reais)}</td>
                        #         <td>{max(valores_previstos)}</td>
                        #         <td>{alarmes_text_tabela}</td>
                        #     </tr>
                        # """
                        dados_gemini_problema = (
                            f"    Tempo anormalidade: {tempo_anormalidade}\n"
                            f"    Tempo total: {tempo_total}\n"
                            f"    Status: {alerta_status}\n"
                            f"    Valores reais: {valores_reais}\n"
                            f"    Valores previstos: {valores_previstos}\n"
                            f"    Porcentagem de diferença: {diferenca_formatada}\n\n"
                            f"    Alarmes: {alarmes_text}\n\n"
                            
                            f"    {pressao_oleo_data}\n"
                            f"    {pressao_combustivel_data}\n"
                            f"    {temperatura_agua_data}\n"

                            f"    {tensao_l1_l2_data}\n"
                            f"    {Tensao_L2_L3_data}\n"
                            f"    {Tensao_L3_L1_data}\n"
                            
                            f"    {Corrente_L1_data}\n"
                            f"    {Corrente_L2_data}\n"
                            f"    {Corrente_L3_data}\n"
                        )

                        dados_telegram_problema = (
                            f"    Tipo de Alerta: {alerta_status}\n"
                            f"    Load Speed %: {max(valores_reais)}\n"
                            f"    Valor previsto %: {max(valores_previstos)}\n"
                            f"    Alarmes: {alarmes_text_tabela}\n"
                        )
                    else:
                        problem_description = f"""
                            <tr>
                                <td>{nome_usina}</td>
                                <td>{nome_equipamento}</td>
                                <td>{data_previsto}</td>
                                <td>{tempo_anormalidade}</td>
                                <td>{tempo_total}</td>
                                <td>Indefinido</td>
                                <td>Indefinido</td>
                                <td colspan="6">Nenhum dado de previsão disponível.</td>
                            </tr>
                        """
                        dados_gemini_problema = (
                            "    Nenhum dado de previsão disponível.\n"
                        )
                        dados_telegram_problema = (
                            "    Nenhum dado de previsão disponível.\n"
                        )
                        
                        
                    detailed_report += problem_description
                    
                    
                    # Verificar equipamentos funcionando no horário próximo
                    await cursor.execute("""
                        SELECT cod_equipamento, MAX(valores_reais), MAX(valores_previstos)
                        FROM machine_learning.valores_previsao
                        WHERE cod_usina = %s
                        AND cod_equipamento != %s
                        AND ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300
                        GROUP BY cod_equipamento
                    """, (cod_usina, cod_equipamento, data_previsto))
                    funcionando_equipamentos = await cursor.fetchall()
                    if funcionando_equipamentos:
                        funcionando_text = "    Equipamentos funcionando na hora:\n"
                        funcionando_text_telegram = "    Equipamentos em paralelo:\n"
                    else:
                        funcionando_text = "    Equipamentos funcionando na hora: 0\n"
                        funcionando_text_telegram = "    Equipamentos em paralelo: 0\n"

                    for equip, valor_real, valor_previsto in funcionando_equipamentos:
                        try:
                            # Separar valores reais por vírgula e converter para float
                            valores_reais = [float(v) for v in valor_real.split(',')]
                        except ValueError:
                            valores_reais = [0.0]  # Defina um valor padrão caso a conversão falhe

                        try:
                            # Separar valores previstos por vírgula e converter para float
                            valores_previstos = [float(v) for v in valor_previsto.split(',')]
                        except ValueError:
                            valores_previstos = [0.0]  # Defina um valor padrão caso a conversão falhe

                        # Obter o nome do equipamento a partir de sup_geral.equipamentos
                        await cursor.execute("""
                            SELECT nome
                            FROM sup_geral.equipamentos
                            WHERE codigo = %s
                        """, (equip,))
                        equipamento_nome = await cursor.fetchone()
                        if equipamento_nome:
                            nome_do_equipamento = equipamento_nome[0]
                        else:
                            nome_do_equipamento = "Nome não encontrado"

                        funcionando_text += f"<br/>    - Equipamento {equip} ({nome_do_equipamento}):<br/>      Valores reais: {', '.join(map(str, valores_reais))}<br/>      Valores previstos: {', '.join(map(str, valores_previstos))}\n"
                        funcionando_text_telegram += f"\n    - Equipamento {equip} ({nome_do_equipamento}):\n      Load Speed %: {max(valores_reais)}\n      Valor previsto %: {max(valores_previstos)}\n"
                        
                    if (nome_usina, nome_equipamento) not in usina_equipamento_map:
                        usina_equipamento_map[(nome_usina, nome_equipamento)] = []
                    usina_equipamento_map[(nome_usina, nome_equipamento)].append(problem_description)

                    if (nome_usina, nome_equipamento) not in usina_equipamento_map_gemini:
                        usina_equipamento_map_gemini[(nome_usina, nome_equipamento)] = []
                    usina_equipamento_map_gemini[(nome_usina, nome_equipamento)].append(dados_gemini_problema)
                    
                    num_geradores = geradores_por_usina_dict.get(cod_usina, "Desconhecido")

                    if nome_usina not in detailed_report:
                        detailed_report += f"<b>Usina:</b> {nome_usina} - ({cod_usina})<br/><br/>    <b>Quantidade de geradores:</b> {num_geradores}<br/>{funcionando_text}<br/><br/>"

                    if nome_usina not in telegram_report:
                        telegram_report += f"Usina: {nome_usina}\n\n    Quantidade de geradores: {num_geradores}\n\n{funcionando_text_telegram}\n\n"
                        
                    telegram_report += (
                        f"  Equipamento: {nome_equipamento}\n"
                        f"    Data: {data_previsto}\n"
                        f"    Data falha: {data_quebra}\n\n"
                        f"{dados_telegram_problema}\n"
                        f"    --------------------------------------\n"
                    )

                    dados_gemini += (
                        f"Usina: {nome_usina}\n\n"
                        f"  Quantidade de geradores da usina: {num_geradores}\n\n"
                        f"  {funcionando_text_telegram}\n\n"
                        f"  Equipamento: {nome_equipamento} - ({cod_equipamento})\n"
                        f"    Data previsto: {data_previsto}\n"
                        f"    Data previsto saída: {data_previsto_saida}\n"
                        f"    Data quebra: {data_quebra}\n\n"
                        f"    Descrição do problema:\n"
                        f"{dados_gemini_problema}\n"
                        f"    --------------------------------------\n"
                    )


            #    detailed_report += "</table>"
                
                # Usar a API Gemini para formatar a descrição geral do relatório e gerar conclusões
                
                # models/chat-bison-001
                # models/text-bison-001
                # models/embedding-gecko-001
                # models/gemini-1.0-pro
                # models/gemini-1.0-pro-001
                # models/gemini-1.0-pro-latest
                # models/gemini-1.0-pro-vision-latest
                # models/gemini-1.5-flash
                # models/gemini-1.5-flash-001
                # models/gemini-1.5-flash-latest
                # models/gemini-1.5-pro
                # models/gemini-1.5-pro-001
                # models/gemini-1.5-pro-latest
                # models/gemini-pro
                # models/gemini-pro-vision
                # models/embedding-001
                # models/text-embedding-004
                # models/aqa

            #    model = genai.GenerativeModel('gemini-1.5-pro-latest')
                '''
                model = genai.GenerativeModel('gemini-1.5-flash-latest')
                chat = model.start_chat(history=[])
                context = (
                    "Aqui estão as definições dos parâmetros usados no relatório:<br/>"
                    f'- Use esses dados como parâmetro, {dados_gemini}<br/>'
                    "- Envie em formato HTML, utilizando <br/> para pular linha, <b> e </b> para negrito para formatar o texto do relatório, pois é um PDF que se gera.<br/>"

                    "- Ao dar enfase em alguma coisa, coloque negrito e cores nessa enfase. Enfase boa em cor verde e enfase ruim na cor vermelha."

                    "- Alerta 100: Valores reais estão em 100% do load speed, o que não é bom.<br/>"
                    "- Alerta 80: Valores reais estão em 80% ou mais do load speed, o que também não é bom para o gerador.<br/>"
                    "- Previsão: Valores reais do load speed estão fora do padrão dos valores previstos para este equipamento.<br/>"
                    
                    "- Data previsto: Hora que o gerador teve valores de load speed fora do padrão.<br/>"
                    "- Data previsto saída: Hora que os valores fora do padrão se normalizaram.<br/>"
                    "- Data quebra: Hora que o equipamento ficou indisponível, ou seja, parou e zerou seus valores do load speed. Não significa que ficou parado por esse tempo.<br/>"
                    
                    "- alarmes: Se não houver alarmes, coloque 'sem alarmes'.<br/>"
                    
                    "- Valores reais: São as leituras reais da porcentagem do load speed do gerador.<br/>"
                    "- Valores previstos: São as leituras previstas da porcentagem do load speed do gerador.<br/>"
                    "- Porcentagem de diferença é a porcentagem que o load speed real está em relação ao load speed previsto que deve ficar na faixa entre -20% e +20% para ser considerado aceitável.<br/>"
                    
                    "- Pressão do óleo: Deve ficar entre 4.8 e 5.0 bar de pressão para bom funcionamento.<br/>"
                    "- Valor da Pressão do óleo mais alto do período funcionando: Esse é o pico dos valores no período que estava funcionando. Deve ficar entre 4.8 e 5.0 bar de pressão para bom funcionamento.<br/>"
                    "- Fale a pressão do mês anterior e a pressão média do período funcionando, veja se houve muita diferença, tendo em mente que a pressão do mês atual deve ser melhor que a do mês anterior, mantendo-se entre 4.8 e 5.0 bar.<br/>"
                    "- Veja se houve algum alarme relacionado à pressão do óleo, se sim, mostre a pressão do mês anterior, a média enquanto funcionava e o pico.<br/>"
                    
                    "- Pressão de combustível deve estar mais alta que a do mês anterior.<br/>"
                    
                    "- O pico de temperatura da água (Valor da temperatura da água mais alto do período funcionando) não pode passar de 103 graus.<br/>"
                    "- A média de temperatura da água (temperatura da água média do período funcionando) deve ser menor que a do mês anterior (temperatura da água média do mês anterior).<br/>"
                    
                    "- Tensão L1 L2 média: Tensão entre as fases L1 e L2 que deve ser igual ou parecida à Tensão L2 L3 média e Tensão L3 L1 média.<br/>"
                    "- Corrente L1 média: Corrente no L1 média que deve ser igual ou parecida à Corrente L2 média e Corrente L3 média.<br/>"
                    
                    "- Quantidade de geradores: Quantidade total de geradores na usina, ligados ou não.<br/>"
                    "- Equipamentos funcionando na hora: Geradores ativos da mesma usina no mesmo momento que algum gerador estava com anomalia.<br/>"
                    
                    "Use essas definições para entender o contexto, mas não inclua essas explicações no relatório.<br/>"
                    "Não invente dados, use apenas os dados do banco de dados retornados, não minta.<br/>"
                    "Faça a média de valores apenas usando os valores da variável analisada.<br/>"
        
                    "<b> Alarmes do nivel 3 são preocupantes e causam a parada do equipamento<br/><br/>"
                    
                    "<b>Alarme: GENERATOR REVERSE POWER 1</b><br/>"
                    "Possível causa: Entupimento dos filtros<br/>"
                    "Possível solução: Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível<br/><br/>"

                    "<b>Alarme: GENERATOR REVERSE POWER 2</b><br/>"
                    "Possível causa: Entupimento dos filtros<br/>"
                    "Possível solução: Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível<br/><br/>"

                    "<b>Alarme: GENERATOR OVERCURRENT 1</b><br/>"
                    "Possível causa e solução: Verificar se há curto-circuito, carga aplicada, diodos, regulador de tensão<br/><br/>"

                    "<b>Alarme: GENERATOR OVERCURRENT 2</b><br/>"
                    "Possível causa e solução: Verificar se há curto-circuito, carga aplicada, diodos, regulador de tensão<br/><br/>"

                    "<b>Alarme: GENERATOR OVERCURRENT 3</b><br/>"
                    "Possível causa e solução: Verificar se há curto-circuito, carga aplicada, diodos, regulador de tensão<br/><br/>"

                    "<b>Alarme: GENERATOR OVERCURRENT 4</b><br/>"
                    "Possível causa e solução: Verificar se há curto-circuito, carga aplicada, diodos, regulador de tensão<br/><br/>"

                    "<b>Alarme: GENERATOR FAST OVERCURRENT 1</b><br/>"
                    "Possível causa: Verificar se há curto-circuito, carga aplicada, se há muitos motores ligados aos geradores<br/><br/>"

                    "<b>Alarme: GENERATOR FAST OVERCURRENT 2</b><br/>"
                    "Possível causa: Verificar se há curto-circuito, carga aplicada, se há muitos motores ligados aos geradores<br/><br/>"

                    "<b>Alarme: GENERATOR OVERVOLTAGE 1</b><br/>"
                    "Possível causa: Diodos em curto, ajuste no regulador de tensão<br/><br/>"

                    "<b>Alarme: GENERATOR OVERVOLTAGE 2</b><br/>"
                    "Possível causa: Diodos em curto, ajuste no regulador de tensão<br/><br/>"

                    "<b>Alarme: GENERATOR UNDERVOLTAGE 1</b><br/>"
                    "Possível causa: Ajuste no regulador de tensão, carga indutiva, cabos do alternador rompidos, excitratriz em curto<br/>"
                    "Possível solução: Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível<br/><br/>"

                    "<b>Alarme: GENERATOR UNDERVOLTAGE 2</b><br/>"
                    "Possível causa: Ajuste no regulador de tensão, carga indutiva, cabos do alternador rompidos, excitratriz em curto<br/>"
                    "Possível solução: Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível<br/><br/>"

                    "<b>Alarme: GENERATOR UNDERVOLTAGE 3</b><br/>"
                    "Possível causa: Ajuste no regulador de tensão, carga indutiva, cabos do alternador rompidos, excitratriz em curto<br/>"
                    "Possível solução: Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível<br/><br/>"

                    "<b>Alarme: GENERATOR OVERFREQUENCY 1</b><br/>"
                    "Possível causa: Retirada muito rápida da carga, trip externo<br/>"
                    "Possível solução: Se o gerador tiver atuador eletromecânico: verificar folgas no braço atuador, bomba desregulada, ganho do regulador de velocidade desregulado<br/><br/>"

                    "<b>Alarme: GENERATOR OVERFREQUENCY 2</b><br/>"
                    "Possível causa: Retirada muito rápida da carga, trip externo<br/>"
                    "Possível solução: Se o gerador tiver atuador eletromecânico: verificar folgas no braço atuador, bomba desregulada, ganho do regulador de velocidade desregulado<br/><br/>"

                    "<b>Alarme: GENERATOR OVERFREQUENCY 3</b><br/>"
                    "Possível causa: Retirada muito rápida da carga, trip externo<br/>"
                    "Possível solução: Se o gerador tiver atuador eletromecânico: verificar folgas no braço atuador, bomba desregulada, ganho do regulador de velocidade desregulado<br/><br/>"

                    "<b>Alarme: GENERATOR UNDERFREQUENCY 1</b><br/>"
                    "Possível causa: Carga alta aplicada instantaneamente, filtro entupido, diesel contaminado<br/>"
                    "Possível solução: Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível<br/><br/>"

                    "<b>Alarme: GENERATOR UNDERFREQUENCY 2</b><br/>"
                    "Possível causa: Carga alta aplicada instantaneamente, filtro entupido, diesel contaminado<br/>"
                    "Possível solução: Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível<br/><br/>"

                    "<b>Alarme: GENERATOR UNDERFREQUENCY 3</b><br/>"
                    "Possível causa: Carga alta aplicada instantaneamente, filtro entupido, diesel contaminado<br/>"
                    "Possível solução: Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível<br/><br/>"

                    "<b>Alarme: BUSBAR OVERVOLTAGE 1</b><br/>"
                    "Possível causa: Capacitores em excesso, concessionária com problemas no regulador de tensão<br/>"
                    "Possível solução: Verificar se o banco de capacitores está em modo automático<br/><br/>"

                    "<b>Alarme: BUSBAR OVERVOLTAGE 2</b><br/>"
                    "Possível causa: Capacitores em excesso, concessionária com problemas no regulador de tensão<br/>"
                    "Possível solução: Verificar se o banco de capacitores está em modo automático<br/><br/>"

                    "<b>Alarme: BUSBAR OVERVOLTAGE 3</b><br/>"
                    "Possível causa: Capacitores em excesso, concessionária com problemas no regulador de tensão<br/>"
                    "Possível solução: Verificar se o banco de capacitores está em modo automático<br/><br/>"

                    "<b>Alarme: BUSBAR UNDERVOLTAGE 1</b><br/>"
                    "Possível causa: Problemas com a concessionária, geradores com excesso de reativo<br/><br/>"

                    "<b>Alarme: BUSBAR UNDERVOLTAGE 2</b><br/>"
                    "Possível causa: Problemas com a concessionária, geradores com excesso de reativo<br/><br/>"

                    "<b>Alarme: BUSBAR UNDERVOLTAGE 3</b><br/>"
                    "Possível causa: Problemas com a concessionária, geradores com excesso de reativo<br/><br/>"

                    "<b>Alarme: BUSBAR UNDERVOLTAGE 4</b><br/>"
                    "Possível causa: Problemas com a concessionária, geradores com excesso de reativo<br/><br/>"

                    "<b>Alarme: DF/DT (ROCOF)</b><br/>"
                    "Possível causa: Desbalanceamento na carga ou frequência instável<br/>"
                    "Possível solução: Verificar a estabilidade da frequência e o balanceamento da carga<br/><br/>"

                    "<b>Alarme: GENERATOR OVERLOAD 1</b><br/>"
                    "Possível causa: Excesso de carga aplicada ao gerador<br/>"
                    "Possível solução: Reduzir a carga e verificar se há sobrecarga no gerador<br/><br/>"

                    "<b>Alarme: GENERATOR OVERLOAD 2</b><br/>"
                    "Possível causa: Excesso de carga aplicada ao gerador<br/>"
                    "Possível solução: Reduzir a carga e verificar se há sobrecarga no gerador<br/><br/>"

                    "<b>Alarme: GENERATOR OVERLOAD 3</b><br/>"
                    "Possível causa: Excesso de carga aplicada ao gerador<br/>"
                    "Possível solução: Reduzir a carga e verificar se há sobrecarga no gerador<br/><br/>"

                    "<b>Alarme: GENERATOR OVERLOAD 4</b><br/>"
                    "Possível causa: Excesso de carga aplicada ao gerador<br/>"
                    "Possível solução: Reduzir a carga e verificar se há sobrecarga no gerador<br/><br/>"

                    "<b>Alarme: GENERATOR OVERLOAD 5</b><br/>"
                    "Possível causa: Excesso de carga aplicada ao gerador<br/>"
                    "Possível solução: Reduzir a carga e verificar se há sobrecarga no gerador<br/><br/>"

                    "<b>Alarme: GENERATOR UNBALANCE CURRENT</b><br/>"
                    "Possível causa: CARGA MAL DISTRIBUÍDA ENTRE AS FASES<br/>"
                    "Possível solução:VERIFICAR SE TEM TERMINAL ROMPIDO, TC EM CURTO-CIRCUITO<br/><br/>"

                    "<b>Alarme: GENERATOR UNBALANCE VOLTAGE</b><br/>"
                    "Possível causa: QUEIMA DE PROTEÇÕES (REDE), CARGA MAL DISTRIBUÍDA<br/>"
                    "Possível solução: VERIFICAR ROMPIMENTO DE CABOS OU DO TERMINAL DE LIGAÇÃO DO ALTERNADOR<br/><br/>"

                    "<b>Alarme: GENERATOR REACTIVE POWER IMPORT (LOSS OF EXCIT)</b><br/>"
                    "Possível causa: Perda de excitação do gerador<br/>"
                    "Possível solução: VERIFICAR REGULADOR DE TENSÃO<br/><br/>"

                    "<b>Alarme: GENERATOR REACTIVE POWER EXPORT (OVEREXCITATION)</b><br/>"
                    "Possível causa: TENSÃO DO GERADOR ACIMA DA TENSÃO DO BARRAMENTO<br/>"
                    "Possível solução: CARGA INDUTIVA (MUITOS MOTORES LIGADOS)<br/><br/>"

                    "<b>Alarme: GOVERNOR REGULATION FAIL</b><br/>"
                    "Possível causa: FILTROS SUJOS, FALTA DE DIESEL<br/>"
                    "Possível solução: FILTROS SUJOS, FALTA DE DIESEL<br/><br/>"

                    "<b>Alarme: OVERSPEED 1</b><br/>"
                    "Possível causa: CARGA RETIRADA INSTANTANEAMENTE<br/>"
                    "Possível solução: FALTA DE COMBUSTÍVEL<br/><br/>"

                    "<b>Alarme: OVERSPEED 2</b><br/>"
                    "Possível causa: CARGA RETIRADA INSTANTANEAMENTE<br/>"
                    "Possível solução: FALTA DE COMBUSTÍVEL<br/><br/>"

                    "<b>Alarme: HZ/VOLT FAILURE</b><br/>"
                    "Possível causa: REGULADOR DE TENSÃO DESREGULADO<br/>"
                    "Possível solução: VERIFICAR SE HÁ ENTRADA DE AR E REGULADOR DE TENSÃO<br/><br/>"

                    "<b>Alarme: AUXILIARY POWER SUPPLY TERMINAL 1 UNDERVOLTAGE</b><br/>"
                    "Possível causa: VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO<br/>"
                    "Possível solução: VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO<br/><br/>"

                    "<b>Alarme: AUXILIARY POWER SUPPLY TERMINAL 1 OVERVOLTAGE</b><br/>"
                    "Possível causa: VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO<br/>"
                    "Possível solução: VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO<br/><br/>"

                    "<b>Alarme: AUXILIARY POWER SUPPLY TERMINAL 98 UNDERVOLTAGE</b><br/>"
                    "Possível causa: VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO<br/>"
                    "Possível solução: VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO<br/><br/>"

                    "<b>Alarme: AUXILIARY POWER SUPPLY TERMINAL 98 OVERVOLTAGE</b><br/>"
                    "Possível causa: VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO<br/>"
                    "Possível solução: VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO<br/><br/>"

                    "<b>Alarme: EIC COOL WATER HIGH 1</b><br/>"
                    "Possível causa: RADIADOR SUJO, VÁLVULA TERMOSTÁTICA TRAVADA, EXCESSO DE CARGA, CORREIA DA BOMBA D'ÁGUA ARREBENTADA<br/>"

                    "<b>Alarme: EIC COOL WATER HIGH 2</b><br/>"
                    "Possível causa: RADIADOR SUJO, VÁLVULA TERMOSTÁTICA TRAVADA, EXCESSO DE CARGA, CORREIA DA BOMBA D'ÁGUA ARREBENTADA<br/>"

                    "<b>Alarme: EIC OIL PRESSURE LOW 1</b><br/>"
                    "Possível causa: NÍVEL DE ÓLEO BAIXO/ALTO <br/>"
                    "Possível solução: VERIFICAR NÍVEL DO ÓLEO: COMPLETAR SE ESTIVER BAIXO, REALIZAR A TROCA SE JÁ ESTIVER VENCIDO <br/><br/>"

                    "<b>Alarme: EIC OIL PRESSURE LOW 2</b><br/>"
                    "Possível causa: NÍVEL DE ÓLEO BAIXO/ALTO <br/>"
                    "Possível solução: VERIFICAR NÍVEL DO ÓLEO: COMPLETAR SE ESTIVER BAIXO, REALIZAR A TROCA SE JÁ ESTIVER VENCIDO <br/><br/>"

                    "<b>Alarme: EMERGENCY STOP</b><br/>"
                    "Possível causa: acionamento da botoeira de emergencia <br/>"
                    "Possível solução: VERIFICAR alarmes presentes na ECU e no controlador <br/><br/>"

                    "<b>Alarme: EIC WARNING</b><br/>"
                    "Possível causa: Problema com a comunicação do controlador do motor <br/>"
                    "Possível solução: VERIFICAR alarmes presentes na ECU e no controlador <br/><br/>"

                    "<b>Alarme:  EIC YELLOW LAMP</b><br/>"
                    "Possível causa: ignorar e nao descrever este alarme <br/>"
                )

                # Ajuste do relatório geral
                general_report = (
                    "- Pule duas linhas.<br/>"
                    f"<br/>Período: {log_data} dias.<br/>"
                    "- Pule duas linhas.<br/>"
                )
                
                prompt_general = (
                    " pegue a data de inicio e fim das leituras do periodo selecionado, nao coloque a hora, pegue somente a data no modelo dia/mes/ano e escreva em portugues"
                    f"{general_report}<br/>"
                )
                response_general = chat.send_message(prompt_general)
                formatted_general_report = response_general.text

                
                conclusions_prompt = (
                    context + "<br/><br/>"
                    """
                    Baseando-se no seguinte relatório de ocorrências. Envie em formato html pulando linha para cada analise e nao criando tabela, se criar, use ASCII, cuidando para fechar todas as tags do html, veja se alguma tag esta errada no html para arrumar.
                    formate de forma atraente para a leitura, gere uma conclusão clara e detalhada que explique os alarmes e valores de maneira que seja fácil de entender para uma pessoa que não tem conhecimentos técnicos.

                    Pule duas linhas e de inicio Fale o nome da usina, colocando o titulo em negrito e pule duas linhas antes do titulo e depois.
                    Descreva o total de equipamentos da usina e a quantidade de equipamentos ativos na hora dessa usina, se tiver algum em funcionamento, diga qual em relação a qual da usina na hora x. Pule duas linhas.
                    - Coloque todos os equipamentos da usina juntos, fale 'Gerador (nome do equipamento)' e o nome da usina em negrito e a data e a hora, pule duas linhas antes e depois do nome.
                    - Diga se estiver em funcionamento e pule duas linhas.
                    - Diga qual o alerta e fale sobre. Diga sobre a Porcentagem de diferença, avise caso extrapole e pule duas linhas. 
                    - Diga sobre a Pressão do óleo. Mostre a media dos valores, descreva o fato se o valor estiver fora do padrão e pule duas linhas.
                    - Diga sobre a temperatura da agua e pule duas linhas.
                    - Diga sobre a Pressão do Combustivel e pule duas linhas.
                    - Diga sobre as correntes médias em Amperes e pule duas linhas.
                    - Diga sobre as tensões médias em Volts e pule duas linhas.
                    - Se o equipamento tiver Tempo total, quer dizer que teve falha, entao diga o tempo total e que o equipamento teve falha e devido a algum alarme X e pule duas linhas.
                    - Se o equipamento nao tiver Tempo total, quer dizer que nao teve falha, entao digaque nao teve falha e pule duas linhas.
                    - Faça sugestao para utilizar os outros equipamentos da usina se ver a necessidade de acordo com os equipamentos funcionando na hora da anormalizade de um equipamento e pule duas linhas.
                    - Compare as datas para o mesmo equipamento, veja se teve relacao uma parada com outra da mesma usina, veja se as paradas estao no mesmo periodo de tempo e pule duas linhas.
                    - Se o mesmo equipamento tiver mais de uma vez dado, descreva ele no mesmo campo, se tiver em funcionamento, diga que esta em alerta e pule duas linhas.
                    - Diga se teve algum alarme nessa hora, se sim, Faça uma análise detalhada dos alarmes encontrados e suas implicações, colocando o titulo em negrito e pule duas linhas.
                    - Para o alarme X, isso significa Y. É importante verificar Z para mitigar o risco.
                    - De sempre um pulo de duas linhas para cada análise detalhada dos alarmes encontrados e suas implicações que for descrever.
                    - O alarme W indica um potencial problema com a componente V; é aconselhável inspeção no ponto A e manutenção na peça B do gerador.
                    - Reveja o texto do html, veja se tem algum erro, arrume antes de enviar.
                    """
                )
                
                # Adicionar análise de alarmes ao relatório
                for key, descriptions in usina_equipamento_map_gemini.items():
                    nome_usina, nome_equipamento = key
                    for dados_gemini_problema in descriptions:
                        conclusions_prompt += f"{dados_gemini_problema}<br/>"

                conclusions_prompt += (
                    context + "pule duas linhas. Recomendações: pule duas linhas"
                    "- Envie em formato html pulando linha para cada recomendação, cuidando para fechar todas as tags e escrevendo corretamente."
                    "- Diga o nome do equipamento e da usina, Faça recomendações em geral, dizendo as possiveis falhas e soluções de acordo com os dados e pule duas linhas."
                    "- Tente dar um norte para a pessoa e pule duas linhas."
                    "- Reveja o texto do html, veja se tem algum erro, arrume antes de enviar."
                )

                response_conclusions = chat.send_message(conclusions_prompt)
                conclusions = response_conclusions.text

                # Juntar a descrição geral formatada, o relatório detalhado e as conclusões
                final_report = "<br/><br/>" + formatted_general_report + "<br/><br/>" + "<br/> -------------------------------------------------------- Conclusões -------------------------------------------------------- <br/><br/>" + conclusions

                detailed_report += final_report
                '''

                # Cálculo da assertibilidade em porcentagem
                if total_equipamentos_alerta > 0:
                    assertibilidade = (total_equipamentos_com_data / total_equipamentos_alerta) * 100
                else:
                    assertibilidade = 0  # Evita divisão por zero

                summary_data = {
                    "total_equipamentos_alerta": total_equipamentos_alerta,
                    "total_equipamentos_com_data": total_equipamentos_com_data,
                    "assertibilidade": assertibilidade
                }

                # Dividir o relatório em partes menores
                telegram_report = split_report(telegram_report)

    #            return [telegram_report, detailed_report, dados_gemini]
                return [telegram_report, detailed_report, dados_gemini, summary_data]

    except Exception as e:
        print(f"Erro ao buscar dados do relatório: {str(e)}")
        return ["Erro ao buscar dados do relatório. Tente novamente."], "", ""





# Variáveis globais para armazenar informações necessárias
global_vars = {
    "log_file_path": '/home/bruno/logs/log_bot.txt',
    "mensagem": "",
    "mensagem2": "",
    "user_id": None,
    "pdf_file_path": "",
    "id_grupo": '-1002000715508',
    "period": "",
    "funcionamento": "",
    "nome_usuario": "",
    "user_message": "",
    "last_read_line": 0
}


# Função assíncrona para monitorar o arquivo de log
async def monitor_log_file():
    await asyncio.sleep(10)
    id_grupo = global_vars["id_grupo"]

    while True:
        try:
            log_file_path = global_vars["log_file_path"]
            last_read_line = global_vars["last_read_line"]

        #    mensagem = global_vars["mensagem"]
        #    mensagem2 = global_vars["mensagem2"]
            user_id = global_vars["user_id"]
            chat_id = global_vars["user_id"]
            pdf_file_path = global_vars["pdf_file_path"]
            period = global_vars["period"]
            funcionamento = global_vars["funcionamento"]
            nome_usuario = global_vars["nome_usuario"]
            user_message = global_vars["user_message"]
            
            sys.stdout.flush()

            with open(log_file_path, 'r') as file:
                lines = file.readlines()
                current_total_lines = len(lines)

                if last_read_line < current_total_lines:
                    new_lines = lines[last_read_line:current_total_lines]
                    global_vars["last_read_line"] = current_total_lines

                    for line in new_lines:
                        # Ignora linhas que contenham "linhas no intervalo:"
                        if "linhas no intervalo:" not in line:
                            # Verifica se a linha contém 'Usuário' e 'pediu funcionamento'
                            if 'Usuário' in line and 'pediu funcionamento' in line:
                                # Procura por "ERROR" e "asyncio:Task was destroyed but it is pending" no intervalo
                                for error_line in new_lines:
                                    if "ERROR" in error_line and "asyncio:Task was destroyed but it is pending" in error_line:
                                        print("\n\nEncontrou um erro no intervalo!\n\n")
                                        pool = dp.pool  # Reutilizando o pool criado durante a inicialização
                                        report_data = await fetch_report_data(pool, period, user_id, funcionamento)
                                        telegram_report, detailed_report, dados_gemini, summary_data = report_data
                                        pdf_filename = f"relatorio_{funcionamento}_{period}_dias.pdf"

                                        # Reenvia o arquivo PDF ao usuário
                                        pdf_file_path = await create_pdf(detailed_report, dados_gemini, period, funcionamento, pdf_filename, summary_data)
                                        await bot.send_document(chat_id=user_id, document=InputFile(pdf_file_path, filename=pdf_filename))

                                        # Envia a mensagem2 ao grupo
                                        mensagem2 = f'Usuário {nome_usuario} completou o pedido do PDF com erro e sucesso.'
                                        await bot.send_message(id_grupo, mensagem2)
                                                    
                                        break
                                else:
                                    # Se não encontrar o erro, saia do loop atual
                                    print("\n\nNao Encontrou um erro no intervalo!\n\n")
                                    break

                    for line in new_lines:
                        # Ignora linhas que contenham "linhas no intervalo:"
                        if "linhas no intervalo:" not in line:
                            
                            if 'clicou no teste' in line:
                                for error_line in new_lines:
                                    if "ERROR" in error_line and "asyncio:Task was destroyed but it is pending" in error_line:
                                        print("\n\nEncontrou um erro no intervalo de Geradores Em Operação!\n\n")
                                        print('user_message.chat',user_message.chat)
                                        print('user_message.chat.id',user_message.chat.id)
                                        print('user_message',user_message)
                                        
                                        if user_message and user_message.chat and user_message.chat.id:
                                            # Reinicia a função teste_menu
                                            await teste_menu(user_message)

                                            # Envia a mensagem ao grupo
                                            mensagem_geradores = f'Usuário {nome_usuario} completou o pedido do Geradores Em Operação com erro e sucesso.'
                                            await bot.send_message(id_grupo, mensagem_geradores)

                                        else:
                                            print("user_message não está inicializado corretamente.")
                                        break
                                    
                                else:
                                    print("\n\nNão Encontrou um erro no intervalo de Geradores Em Operação!\n\n")
                                    break

        except Exception as e:
            print(f"Erro ao monitorar o arquivo de log: {str(e)}")

        await asyncio.sleep(30)




# Dicionário de períodos
period_map = {
    "1_dia": "1 dia",
    "2_dias": "2 dias",
    "7_dias": "7 dias",
    "15_dias": "15 dias",
    "1_mês": "30 dias"
}

# Função para criar botões inline para o período
def create_period_buttons():
    buttons = InlineKeyboardMarkup(row_width=2)
    
    # Adiciona "1 dia" e "2 dias" na mesma linha
    buttons.add(
        InlineKeyboardButton("1 dia", callback_data="period_1_dia"),
        InlineKeyboardButton("2 dias", callback_data="period_2_dias")
    )
    
    # Adiciona "7 dias" e "15 dias" na mesma linha
    buttons.add(
        InlineKeyboardButton("7 dias", callback_data="period_7_dias"),
        InlineKeyboardButton("15 dias", callback_data="period_15_dias")
    )
    
    # Adiciona "1 mês" em uma linha própria
    buttons.add(InlineKeyboardButton("1 mês", callback_data="period_1_mês"))
    
    return buttons

# Função para criar botões inline para o funcionamento
def create_funcionamento_buttons():
    buttons = InlineKeyboardMarkup(row_width=2)  # Define a largura padrão como 2 por linha

    # Adiciona "Controle de demanda" e "Horário de ponta" na mesma linha
    buttons.add(
        InlineKeyboardButton("Controle de demanda", callback_data="funcionamento_Controle_de_demanda"),
        InlineKeyboardButton("Horário de ponta", callback_data="funcionamento_Horário_de_ponta")
    )

    # Adiciona "Operação contínua" e "Falta de energia" na mesma linha
    buttons.add(
        InlineKeyboardButton("Operação contínua", callback_data="funcionamento_Operação_contínua"),
        InlineKeyboardButton("Falta de energia", callback_data="funcionamento_Falta_de_energia")
    )
    
    # Adiciona "Geral" em uma linha própria
    buttons.add(InlineKeyboardButton("Geral", callback_data="funcionamento_Geral"))
    
    # Define a largura como 3 botões por linha e adiciona os botões "Agrogera"
    agrogera_buttons = InlineKeyboardMarkup(row_width=3)
    agrogera_buttons.add(
        InlineKeyboardButton("Agrogera GO", callback_data="funcionamento_Agrogera_GO"),
        InlineKeyboardButton("Agrogera MG", callback_data="funcionamento_Agrogera_MG"),
        InlineKeyboardButton("Agrogera BA", callback_data="funcionamento_Agrogera_BA")
    )
    
    # Adiciona os botões "Agrogera" ao teclado principal
    buttons.inline_keyboard.extend(agrogera_buttons.inline_keyboard)
    
    return buttons

# Handler para o comando /relatorio
@dp.message_handler(commands=['relatorio'])
async def send_welcome(message: types.Message):
    await message.reply("Escolha o período do relatório:", reply_markup=create_period_buttons())

# Callback query para o período
@dp.callback_query_handler(lambda c: c.data.startswith('period_'))
async def handle_period_selection(callback_query: types.CallbackQuery):
    period_label = callback_query.data.replace("period_", "")
    period = period_map.get(period_label)
    period_number = int(period.split()[0])  # Isso extrai o número '2' de '2 dias'

    if not period:
        await callback_query.message.reply("Período inválido selecionado.")
        return

    user_id = callback_query.from_user.id
    user_reports[user_id] = {"period": period_number}

    # Apagar os botões de período
#    await bot.edit_message_reply_markup(callback_query.message.chat.id, callback_query.message.message_id, reply_markup=None)
    await bot.delete_message(chat_id=callback_query.message.chat.id, message_id=callback_query.message.message_id)

    # Mostrar botões de funcionamento
    await bot.send_message(callback_query.message.chat.id, "Selecione o tipo de funcionamento:", reply_markup=create_funcionamento_buttons())


# Callback query para o funcionamento

# Callback query para o funcionamento
@dp.callback_query_handler(lambda c: c.data.startswith('funcionamento_'))
async def handle_funcionamento_selection(callback_query: types.CallbackQuery):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    funcionamento_label = callback_query.data.replace("funcionamento_", "")
    funcionamento_label = funcionamento_label.replace("_", " ")

    funcionamento = funcionamento_map[funcionamento_label]
    user_id = callback_query.from_user.id
    user_reports[user_id]["funcionamento"] = funcionamento
    id_grupo = global_vars["id_grupo"]
    nome_usuario = ''
    period = user_reports[user_id]["period"]
    
    global_vars["period"] = period
    global_vars["funcionamento"] = funcionamento
    global_vars["user_id"] = user_id

    await bot.delete_message(chat_id=callback_query.message.chat.id, message_id=callback_query.message.message_id)

    await bot.send_message(callback_query.message.chat.id,f"Você escolheu o funcionamento: {funcionamento_label}. Aguarde enquanto buscamos os dados...")

    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor() as cursor:   
            await cursor.execute("SELECT nome_telegram FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
            result = await cursor.fetchone()
            if not result:
                return await bot.send_message(callback_query.message.chat.id,"Usuário não encontrado.")
            
            nome_usuario = result[0]
            global_vars["nome_usuario"] = nome_usuario

            global_vars["mensagem"] = f'Usuário {nome_usuario} pediu funcionamento: {funcionamento_label} para {period} dias.'
            print(global_vars["mensagem"])
            try:
                await bot.send_message(id_grupo, global_vars["mensagem"])
            except Exception as e:
                await bot.send_message(callback_query.message.chat.id,f"Erro ao enviar mensagem ao grupo: {e}")
            sys.stdout.flush()

    # Buscar os dados do relatório
    try:
        report_data = await fetch_report_data(pool, period, user_id, funcionamento)
    finally:
        # pool.close()
        # await pool.wait_closed()
        pass

    if not report_data or report_data[0] == ["Nenhum dado encontrado para o período selecionado."]:
        await bot.send_message(callback_query.message.chat.id,"Nenhum dado encontrado para o funcionamento selecionado.")
        await bot.send_message(callback_query.message.chat.id,"Escolha o período do relatório novamente:", reply_markup=create_period_buttons())
    else:
        telegram_report, detailed_report, dados_gemini, summary_data = report_data

        user_reports[user_id] = (detailed_report, period, funcionamento, dados_gemini, summary_data)

        # Criar e enviar o PDF diretamente sem botão
        pdf_filename = f"relatorio_{funcionamento}_{period}_dias.pdf"
        pdf_file_path = await create_pdf(detailed_report, dados_gemini, period, funcionamento, pdf_filename, summary_data)
        global_vars["pdf_file_path"] = pdf_file_path
        await bot.send_document(chat_id=callback_query.message.chat.id, document=InputFile(pdf_file_path, filename=pdf_filename))

        global_vars["mensagem2"] = f'Usuário {nome_usuario} completou o pedido do PDF.'
        print(global_vars["mensagem2"])

        try:
            await bot.send_message(id_grupo, global_vars["mensagem2"])
        except Exception as e:
            await bot.send_message(callback_query.message.chat.id,f"Erro ao enviar mensagem ao grupo: {e}")
        sys.stdout.flush()

        # Solicitar email do usuário
        email_button = InlineKeyboardMarkup().add(InlineKeyboardButton("Receber PDF por Email", callback_data="request_email"))
        await bot.send_message(callback_query.message.chat.id,"Deseja receber o PDF por email?", reply_markup=email_button)




# Handler to manage email request
@dp.callback_query_handler(lambda c: c.data == 'request_email')
async def handle_request_email(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização

    try:
        async with pool.acquire() as conn:
            await conn.ping(reconnect=True)  # Forçar reconexão
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT email FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
                result = await cursor.fetchone()
                if result and result[0]:
                    email = result[0]
                    email_buttons = InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Sim", callback_data="send_to_existing_email"),
                        InlineKeyboardButton("Não", callback_data="request_new_email")
                    )
                    await bot.send_message(user_id, f"O email cadastrado é {email}. Deseja enviar para este email?", reply_markup=email_buttons)
                else:
                    await bot.send_message(user_id, "Por favor, digite o email para enviar o PDF:")
                await bot.delete_message(chat_id=callback_query.message.chat.id, message_id=callback_query.message.message_id)
    except Exception as e:
        await bot.send_message(user_id, f"Erro ao verificar o email: {str(e)}")
        logger.error(f"Error in handle_request_email: {str(e)}")
    finally:
        # pool.close()
        # await pool.wait_closed()
        pass
    
# Handler to send to existing email
@dp.callback_query_handler(lambda c: c.data == 'send_to_existing_email')
async def handle_send_to_existing_email(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    await bot.delete_message(chat_id=callback_query.message.chat.id, message_id=callback_query.message.message_id)

    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT email FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
                result = await cursor.fetchone()
                if result and result[0]:
                    email = result[0]
                    detailed_report, period, funcionamento, dados_gemini, summary_data = user_reports[user_id]
                    pdf_filename = f"relatorio_{funcionamento}_{period}_dias.pdf"
                    pdf_file_path = await create_pdf(detailed_report, dados_gemini, period, funcionamento, pdf_filename, summary_data)
                    await send_email(email, pdf_file_path, pdf_filename)
                #    await bot.delete_message(chat_id=callback_query.message.chat.id, message_id=callback_query.message.message_id)
                    await bot.send_message(user_id, f"PDF enviado para o email {email} com sucesso.")
                else:
                    await bot.send_message(user_id, "Email não encontrado. Por favor, digite o email para enviar o PDF.")
    except Exception as e:
        await bot.send_message(user_id, f"Erro ao enviar o PDF: {str(e)}")
        logger.error(f"Error in handle_send_to_existing_email: {str(e)}")
    finally:
        # pool.close()
        # await pool.wait_closed()
        pass



# Handler to request new email
@dp.callback_query_handler(lambda c: c.data == 'request_new_email')
async def handle_request_new_email(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, "Por favor, digite o novo email para enviar o PDF:")
    await callback_query.answer()

@dp.message_handler(lambda message: '@' in message.text)
async def handle_email_input(message: types.Message):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    user_id = message.from_user.id
    if user_id in user_reports:
        try:
            to_email = message.text
            detailed_report, period, funcionamento, dados_gemini, summary_data = user_reports[user_id]
            pdf_filename = f"relatorio_{funcionamento}_{period}_dias.pdf"
            pdf_file_path = await create_pdf(detailed_report, dados_gemini, period, funcionamento, pdf_filename, summary_data)
            await send_email(to_email, pdf_file_path, pdf_filename)
        #    await message.reply(f"PDF enviado para o email {to_email} com sucesso.", reply_markup=main_keyboard)
            await message.bot.send_message(user_id, f"PDF enviado para o email {to_email} com sucesso.")
            os.remove(pdf_file_path)  # Remover o PDF após o envio por email

            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE machine_learning.usuarios_telegram
                        SET email = %s
                        WHERE id_telegram = %s
                    """, (to_email, user_id))
                    await conn.commit()

        except Exception as e:
            await message.reply(f"Erro ao enviar o PDF por email: {str(e)}")
            logger.error(f"Error in handle_email_input: {str(e)}")


async def send_email(to_email, pdf_file_path, filename):
    message = MIMEMultipart()
    message["From"] = "zanellabruno7@gmail.com"
    message["To"] = to_email
    message["Subject"] = "Relatório de previsões"

    body = "Segue em anexo o relatório em PDF."
    message.attach(MIMEText(body, "plain"))

    with open(pdf_file_path, "rb") as attachment:
        part = MIMEApplication(attachment.read(), _subtype="pdf")
        part.add_header("Content-Disposition", "attachment", filename=filename)
        message.attach(part)

    try:
        await aiosmtplib.send(
            message,
            hostname="smtp.gmail.com",
            port=587,
            username="zanellabruno7@gmail.com",
            password="wron fcmr ugbj ufhb",
            use_tls=False
        )
    except Exception as e:
        logger.error(f"Error in send_email: {str(e)}")

    # Delete the PDF file after sending the email
    # try:
    #     os.remove(pdf_file_path)
    #     logger.info(f"Deleted PDF file: {pdf_file_path}")
    # except Exception as e:
    #     logger.error(f"Error deleting PDF file: {str(e)}")






async def create_pdf(detailed_report, dados_gemini, period, funcionamento, filename, summary_data):
    temp_pdf = tempfile.mktemp(suffix=".pdf")
    doc = SimpleDocTemplate(temp_pdf, pagesize=A4, title="Relatório Detalhado",
                                topMargin=110, bottomMargin=40)
    
    elements = []

    logo_path = "/home/bruno/imagens/cabeçalho.png"
    
    # Adjust logo transparency
    pil_logo = PILImage.open(logo_path).convert("RGBA")
    alpha = pil_logo.split()[3]
    alpha = alpha.point(lambda p: p * 0.5)  # Adjust transparency to 50%
    pil_logo.putalpha(alpha)
    temp_logo_path = tempfile.mktemp(suffix=".png")
    pil_logo.save(temp_logo_path)
    
    logo = Image(temp_logo_path, width=120, height=50)

    # Obter o nome correto do funcionamento a partir do mapa reverso
    header_funcionamento = reverse_funcionamento_map.get(funcionamento, funcionamento)

    # Define a função para o cabeçalho
    def header(canvas, doc):
        canvas.saveState()
        logo.drawOn(canvas, 40, A4[1] - 65)

        # Configurações do cabeçalho
        canvas.setFont("Helvetica", 12)

        # Texto "Relatório {funcionamento}"
        header_text = f"Relatório {header_funcionamento}"
        header_width = canvas.stringWidth(header_text, "Helvetica", 12)
        canvas.drawString((A4[0] - header_width) / 2, A4[1] - 80, header_text)

        # Texto "Últimos {period} dias" em vermelho
        canvas.setFillColor(colors.red)
        period_text = f"Últimos {period} dias"
        period_width = canvas.stringWidth(period_text, "Helvetica", 12)
        canvas.drawString((A4[0] - period_width) / 2, A4[1] - 100, period_text)
        
        # Adiciona um espaço de duas linhas
        canvas.drawString((A4[0] - period_width) / 2, A4[1] - 100, "")
        canvas.restoreState()

    styles = getSampleStyleSheet()
    styleN = styles['Normal']
    styleH = styles['Heading1']

    # Estilo com recuo de 40px para a esquerda
    styleN_left_indent = ParagraphStyle(
        'NormalLeftIndent',
        parent=styleN,
        leftIndent=-40,
        rightIndent=-40
    )
    
    # Estilo para causas e soluções com recuo
    cause_solution_style = ParagraphStyle(
        'CauseSolutionStyle',
        parent=styleN,
        leftIndent=10  # Indentação de 10px para a direita
    )

    # Estilo para o período com indentação negativa
    period_style = ParagraphStyle(
        'PeriodStyle',
        parent=styleH,
        leftIndent=-40  # Indentação negativa de 40px para a esquerda
    )
        
    # Extract dates from the detailed report
    dates = []
    rows = detailed_report.split("<tr>")
    for row in rows[1:]:
        columns = row.split("<td>")
        if len(columns) > 3:
            date_str = columns[3].split("</td>")[0].strip()
            try:
                date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')  # Adjust the date format if necessary
                dates.append(date)
            except ValueError:
                continue

    if dates:
        start_date = min(dates).strftime('%d/%m/%Y')
        end_date = max(dates).strftime('%d/%m/%Y')
    else:
        start_date = 'N/A'
        end_date = 'N/A'
    
    elements.append(Spacer(1, 12))
    elements.append(Spacer(1, 12))
    elements.append(Spacer(1, 12))
    # Texto do período próximo à margem esquerda com 5px de distância
    elements.append(Paragraph(f"<b>Período: {start_date} à {end_date}</b>", period_style))
    elements.append(Spacer(1, 12)) 


    # Estilo para os totais com ajuste na indentação
    total_equipamentos_alerta_equipamentos = ParagraphStyle(
        'total_equipamentos_alerta_equipamentosStyle',
        parent=styleN,
        fontSize=10,  # Tamanho da fonte reduzido para 10
        leftIndent=-15  # Ajuste para alinhar na mesma posição do período
    )

    total_equipamentos_com_data_equipamentos = ParagraphStyle(
        'total_equipamentos_com_data_equipamentosStyle',
        parent=styleN,
        fontSize=10,
        leftIndent=-15
    )

    assertibilidade_equipamentos = ParagraphStyle(
        'assertibilidade_equipamentosStyle',
        parent=styleN,
        fontSize=10,
        leftIndent=130  # Aumentado para posicionar mais à direita
    )

    # Variáveis de exemplo para os valores
    # total_equipamentos_alerta = 50
    # total_equipamentos_com_data = 40
    # assertibilidade = 80.5

    # Extrair valores do report_summary para uso no PDF
    total_equipamentos_alerta = summary_data["total_equipamentos_alerta"]
    total_equipamentos_com_data = summary_data["total_equipamentos_com_data"]
    assertibilidade = summary_data["assertibilidade"]
    
    
    # Criar os parágrafos para cada item
    alerta_paragraph = Paragraph(f"Total de equipamentos previstos: {total_equipamentos_alerta}", total_equipamentos_alerta_equipamentos)
    assertibilidade_paragraph = Paragraph(f"Assertibilidade: {assertibilidade:.2f}%", assertibilidade_equipamentos)
    com_data_paragraph = Paragraph(f"Total de equipamentos com falha: {total_equipamentos_com_data}", total_equipamentos_com_data_equipamentos)

    # Definir a tabela com os parágrafos organizados nas posições desejadas
    data = [
        [alerta_paragraph, assertibilidade_paragraph],  # Primeira linha com alerta e assertibilidade lado a lado
        [com_data_paragraph]  # Segunda linha com total de equipamentos com data
    ]

    # Configurar a tabela
    table = Table(data, colWidths=[250, 250])  # Define larguras das colunas
    table.setStyle(TableStyle([
        ('ALIGN', (0, 0), (1, 0), 'LEFT'),  # Alinha à esquerda e direita na primeira linha
        ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
        ('SPAN', (0, 1), (1, 1)),  # Mescla as colunas na segunda linha
        ('ALIGN', (0, 1), (1, 1), 'CENTER'),  # Centraliza total_equipamentos_com_data
        ('TOPPADDING', (0, 0), (-1, -1), 5),  # Adiciona espaçamento
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))

    # Adicionar a tabela ao fluxo de elementos
    elements.append(table)
    elements.append(Spacer(1, 12))

    elements.append(Spacer(1, 12)) 
    
    header_style = ParagraphStyle(
        'HeaderStyle',
        parent=styles['Normal'],
        fontSize=8,  # Ajusta o tamanho da fonte para 10
        leading=8,   # Espaçamento entre linhas para centralizar verticalmente
        alignment=1,  # Centraliza o texto horizontalmente
        textColor=colors.black,
        valign='MIDDLE'  # Centraliza o texto verticalmente
    )
    body_style = ParagraphStyle(
        'BodyStyle',
        parent=styles['Normal'],
        fontSize=8,  # Ajusta o tamanho da fonte para 10
        leading=9,   # Espaçamento entre linhas para centralizar verticalmente
        alignment=1,  # Centraliza o texto horizontalmente
        textColor=colors.black,
        valign='MIDDLE'  # Centraliza o texto verticalmente
    )
    

    # Define the color white for the header text
    header_text_color = colors.whitesmoke

    data = [
        ['Usina', 'Equip.', 'Data', 'Dur. Anomalia', 'Dur. total', 'Alerta', 'Alarmes']
    ]
    
    # Extract data from the detailed report and sort by date
    report_data = []
    rows = detailed_report.split("<tr>")
    for row in rows[1:]:
        columns = row.split("<td>")
        row_data = []
        for i, col in enumerate(columns[1:], start=1):
            value = col.split("</td>")[0].strip()
            if i == 3:
                try:
                    value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    pass
            row_data.append(value)
        report_data.append(row_data)
    
    # Sort report data by the date column (index 2) in descending order
    report_data_sorted = sorted(report_data, key=lambda x: x[2], reverse=True)

    # Format date columns back to string
    for row in report_data_sorted:
        row[2] = row[2].strftime('%d/%m/%Y %H:%M')
    
    data.extend(report_data_sorted)

    # Apply white color only to the first row (header)
    styled_data = [
        [Paragraph(cell, ParagraphStyle('HeaderStyle', parent=header_style, textColor=header_text_color)) for cell in data[0]]
    ] + [
        [Paragraph(cell, body_style) for cell in row] for row in data[1:]
    ]

    column_widths = [140, 40, 80, 55, 55, 50, 100]

    table = Table(styled_data, colWidths=column_widths)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),  # Set header text color to white
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('TOPPADDING', (0, 1), (-1, -1), 8),  # Ajusta o padding superior para centralizar verticalmente
        ('BOTTOMPADDING', (0, 1), (-1, -1), 9),  # Ajusta o padding inferior para centralizar verticalmente
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  # Centraliza o texto verticalmente
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))

    elements.append(table)
    elements.append(Spacer(1, 12))

    # Define styles with different colors
    # styleN_red = ParagraphStyle('Normal', parent=styleN, textColor=red)
    # styleN_green = ParagraphStyle('Normal', parent=styleN, textColor=green)
    # styleN_orange = ParagraphStyle('Normal', parent=styleN, textColor=orange)
    # Define styles with different colors
    styleN_red = ParagraphStyle('NormalRed', parent=styleN_left_indent, textColor=colors.red)
    styleN_green = ParagraphStyle('NormalGreen', parent=styleN_left_indent, textColor=colors.green)
    styleN_orange = ParagraphStyle('NormalOrange', parent=styleN_left_indent, textColor=colors.orange)



    def format_difference(value):
        if -30 <= value <= 30:
            return Paragraph(f"O load Speed está {abs(value):.2f}% da reta prevista, ou seja, dentro do padrão de previsão", styleN_green)
        elif value < -30:
            return Paragraph(f"O load Speed está {abs(value):.2f}% da reta prevista, ou seja, abaixo do padrão", styleN_red)
        else:
            return Paragraph(f"Está {abs(value):.2f}% da reta prevista, ou seja, acima do padrão", styleN_red)

    def get_paragraph_style(text, condition, orange_condition=False):
        if condition:
            return Paragraph(text, styleN_red)
        elif orange_condition:
            return Paragraph(text, styleN_orange)
        else:
            return Paragraph(text, styleN_green)

    # Função para determinar a cor com base nas condições
    def get_color(value, lower_limit, upper_limit):
        if lower_limit <= value <= upper_limit:
            return "green"
        else:
            return "red"

    # Função para criar o texto detalhado com as condições de cores
    def create_detailed_text(data_1, data_2, data_3, data_1_color, data_2_color, data_3_color, tipo):
        texto = f"A pressão do {tipo} média do mês anterior foi de "

        if data_1 is not None:
            texto += f"<span color='{data_1_color}'>{data_1} Bar</span>"
        else:
            texto = texto.rstrip(", ")
        
        if data_2 is not None:
            texto += f", enquanto a pressão do {tipo} média do período em que o gerador esteve funcionando foi de "
            texto += f"<span color='{data_2_color}'>{data_2} Bar</span>"
        else:
            texto = texto.rstrip(", ")
        
        if data_3 is not None:
            texto += f", com o pico de pressão chegando a "
            texto += f"<span color='{data_3_color}'>{data_3} Bar</span>."
        else:
            texto = texto.rstrip(", ")

        return texto


    # Função para determinar a cor com base nas condições para temperatura da água
    def get_temp_color(temp_value, threshold_1, threshold_2, lower_limit, upper_limit):
        if temp_value is None:
            return "black"
        if temp_value > threshold_2:
            return "red"
        if threshold_1 < temp_value <= threshold_2:
            return "orange"
        if lower_limit <= temp_value <= upper_limit:
            return "green"
        return "black"

    # Função para criar o texto detalhado com as condições de cores para temperatura da água
    def create_detailed_text_temp(data_1, data_2, data_3, data_1_color, data_2_color, data_3_color):
        texto = "A temperatura da água média do mês anterior foi de "

        if data_1 is not None:
            texto += f"<span color='{data_1_color}'>{data_1} ºC</span>"
        else:
            texto = texto.rstrip(", ")
        
        if data_2 is not None:
            texto += f", enquanto a temperatura da água média do período em que o gerador esteve funcionando foi de "
            texto += f"<span color='{data_2_color}'>{data_2} ºC</span>"
        else:
            texto = texto.rstrip(", ")
        
        if data_3 is not None:
            texto += f", com o pico de temperatura chegando a "
            texto += f"<span color='{data_3_color}'>{data_3} ºC</span>."
        else:
            texto = texto.rstrip(", ")

        return texto


    def is_within_10_percent(reference, value):
        if reference is None or value is None:
            return False
        return abs(reference - value) <= reference * 0.10

    def get_color_tensao(tensao_l1, tensao_l2, tensao_l3):
        if tensao_l1 is None or tensao_l2 is None or tensao_l3 is None:
            return "black", "black", "black"
        
        color_l1 = "green"
        color_l2 = "green" if is_within_10_percent(tensao_l1, tensao_l2) else "red"
        color_l3 = "green" if is_within_10_percent(tensao_l1, tensao_l3) else "red"
        
        return color_l1, color_l2, color_l3

    def create_detailed_text_tensao(data_1, data_2, data_3, data_1_color, data_2_color, data_3_color, tipo):
        texto = f"A tensão média entre as fases: "
        
        if data_1 is not None:
            texto += f"<span color='{data_1_color}'>{tipo}1-{tipo}2</span> foi de "
            texto += f"<span color='{data_1_color}'>{data_1} Volts</span>"
        else:
            texto = texto.rstrip(", ")
        
        if data_2 is not None:
            texto += f", enquanto entre as fases: "
            texto += f"<span color='{data_2_color}'>{tipo}2-{tipo}3</span> foi de "
            texto += f"<span color='{data_2_color}'>{data_2} Volts</span>"
        else:
            texto = texto.rstrip(", ")
        
        if data_3 is not None:
            texto += f", e entre as fases: "
            texto += f"<span color='{data_3_color}'>{tipo}3-{tipo}1</span> foi de "
            texto += f"<span color='{data_3_color}'>{data_3} Volts</span>."
        else:
            texto = texto.rstrip(", ")

        return texto

    usinas = {}

    for entry in dados_gemini.split("--------------------------------------"):
        try:

            nome_usina = entry.split("Usina: ")[1].split("\n")[0].strip()
            nome_equipamento = entry.split("Equipamento: ")[1].split(" - ")[0].strip()
            numero_equipamento = re.search(r'\((\d+)\)', entry).group(1)

            # data_previsto = entry.split("Data previsto: ")[1].split("\n")[0].strip()
            # data_previsto_saida = entry.split("Data previsto saída: ")[1].split("\n")[0].strip()
            # data_quebra = entry.split("Data quebra: ")[1].split("\n")[0].strip()

            data_previsto = formatar_data(entry.split("Data previsto: ")[1].split("\n")[0].strip())
            data_previsto_saida = formatar_data(entry.split("Data previsto saída: ")[1].split("\n")[0].strip())
            data_quebra = formatar_data(entry.split("Data quebra: ")[1].split("\n")[0].strip())

            valores_reais = entry.split("Valores reais: [")[1].split("]")[0].strip().replace('\n', '').replace(' ', '').split(',')
            valores_previstos = entry.split("Valores previstos: [")[1].split("]")[0].strip().replace('\n', '').replace(' ', '').split(',')
            
            # nome_usina = entry.split("Usina: ")[1].split("\n")[0].strip() if "Usina: " in entry else "N/A"
            # nome_equipamento = entry.split("Equipamento: ")[1].split(" - ")[0].strip() if "Equipamento: " in entry else "N/A"
            # data_previsto = entry.split("Data previsto: ")[1].split("\n")[0].strip() if "Data previsto: " in entry else "N/A"
            # data_previsto_saida = entry.split("Data previsto saída: ")[1].split("\n")[0].strip() if "Data previsto saída: " in entry else "N/A"
            # data_quebra = entry.split("Data quebra: ")[1].split("\n")[0].strip() if "Data quebra: " in entry else "N/A"
            # valores_reais = entry.split("Valores reais: [")[1].split("]")[0].strip().replace('\n', '').replace(' ', '').split(',') if "Valores reais: [" in entry else []
            # valores_previstos = entry.split("Valores previstos: [")[1].split("]")[0].strip().replace('\n', '').replace(' ', '').split(',') if "Valores previstos: [" in entry else []
            
            
            tempo_anormalidade = entry.split("Tempo anormalidade: ")[1].split("\n")[0].strip() if "Tempo anormalidade: " in entry else "N/A"
            tempo_total = entry.split("Tempo total: ")[1].split("\n")[0].strip() if "Tempo total: " in entry else "N/A"
            alerta_status = entry.split("Status: ")[1].split("\n")[0].strip() if "Status: " in entry else "N/A"
            diferenca_formatada = entry.split("Porcentagem de diferença: ")[1].split("\n")[0].strip() if "Porcentagem de diferença: " in entry else "N/A"
            alarmes_text = entry.split("Alarmes: ")[1].split("\n\n")[0].strip() if "Alarmes: " in entry else "N/A"
            
            pressao_oleo_data_1 = entry.split("Pressão do óleo média do mês anterior: ")[1].split("\n")[0].strip() if "Pressão do óleo média do mês anterior: " in entry else "N/A"
            pressao_oleo_data_2 = entry.split("Pressão do óleo média do período funcionando: ")[1].split("\n")[0].strip() if "Pressão do óleo média do período funcionando: " in entry else "N/A"
            pressao_oleo_data_3 = entry.split("Valor da Pressão do óleo mais alto do período funcionando: ")[1].split("\n")[0].strip() if "Valor da Pressão do óleo mais alto do período funcionando: " in entry else "N/A"

            pressao_combustivel_data_1 = entry.split("Pressão do Combustivel média do mês anterior: ")[1].split("\n")[0].strip() if "Pressão do Combustivel média do mês anterior: " in entry else "N/A"
            pressao_combustivel_data_2 = entry.split("Pressão do Combustivel média do período funcionando: ")[1].split("\n")[0].strip() if "Pressão do Combustivel média do período funcionando: " in entry else "N/A"
            pressao_combustivel_data_3 = entry.split("Valor da Pressão do Combustivel mais alto do período funcionando: ")[1].split("\n")[0].strip() if "Valor da Pressão do Combustivel mais alto do período funcionando: " in entry else "N/A"

            temperatura_agua_data_1 = entry.split("temperatura da água média do mês anterior: ")[1].split("\n")[0].strip() if "temperatura da água média do mês anterior: " in entry else "N/A"
            temperatura_agua_data_2 = entry.split("temperatura da água média do período funcionando: ")[1].split("\n")[0].strip() if "temperatura da água média do período funcionando: " in entry else "N/A"
            temperatura_agua_data_3 = entry.split("Valor da temperatura da água mais alto do período funcionando: ")[1].split("\n")[0].strip() if "Valor da temperatura da água mais alto do período funcionando: " in entry else "N/A"

            tensao_l1_l2_data = entry.split("Tensão L1 L2 média: ")[1].split("\n")[0].strip() if "Tensão L1 L2 média: " in entry else "N/A"
            tensao_l2_l3_data = entry.split("Tensão L2 L3 média: ")[1].split("\n")[0].strip() if "Tensão L2 L3 média: " in entry else "N/A"
            tensao_l3_l1_data = entry.split("Tensão L3 L1 média: ")[1].split("\n")[0].strip() if "Tensão L3 L1 média: " in entry else "N/A"

            corrente_l1_data = entry.split("Corrente L1 média: ")[1].split("\n")[0].strip() if "Corrente L1 média: " in entry else "N/A"
            corrente_l2_data = entry.split("Corrente L2 média: ")[1].split("\n")[0].strip() if "Corrente L2 média: " in entry else "N/A"
            corrente_l3_data = entry.split("Corrente L3 média: ")[1].split("\n")[0].strip() if "Corrente L3 média: " in entry else "N/A"

            valores_reais_float = [float(value) for value in valores_reais] if valores_reais else []
            valores_previstos_float = [float(value) for value in valores_previstos] if valores_previstos else []

            if nome_usina not in usinas:
                usinas[nome_usina] = []

            usinas[nome_usina].append({
                'nome_equipamento': nome_equipamento,
                'numero_equipamento': numero_equipamento,
                'valores_reais': valores_reais_float,
                'valores_previstos': valores_previstos_float,
                'data_previsto': data_previsto,
                'data_previsto_saida': data_previsto_saida,
                'data_quebra': data_quebra,
                'tempo_anormalidade': tempo_anormalidade,
                'tempo_total': tempo_total,
                'alerta_status': alerta_status,
                'diferenca_formatada': diferenca_formatada,
                'alarmes_text': alarmes_text,
                'pressao_oleo_data_1': pressao_oleo_data_1,
                'pressao_oleo_data_2': pressao_oleo_data_2,
                'pressao_oleo_data_3': pressao_oleo_data_3,
                'pressao_combustivel_data_1': pressao_combustivel_data_1,
                'pressao_combustivel_data_2': pressao_combustivel_data_2,
                'pressao_combustivel_data_3': pressao_combustivel_data_3,
                'temperatura_agua_data_1': temperatura_agua_data_1,
                'temperatura_agua_data_2': temperatura_agua_data_2,
                'temperatura_agua_data_3': temperatura_agua_data_3,
                'tensao_l1_l2_data': tensao_l1_l2_data,
                'tensao_l2_l3_data': tensao_l2_l3_data,
                'tensao_l3_l1_data': tensao_l3_l1_data,
                'corrente_l1_data': corrente_l1_data,
                'corrente_l2_data': corrente_l2_data,
                'corrente_l3_data': corrente_l3_data
            })

        except IndexError:
            continue
        except ValueError as ve:
            print(f"Error parsing values: {ve}")
            continue




    elements.append(Spacer(1, 12))
    for nome_usina, equipamentos in usinas.items():
        elements.append(PageBreak())
        elements.append(Paragraph(f"Usina: {nome_usina}", ParagraphStyle('Heading1', parent=period_style)))
        elements.append(Spacer(1, 12))

        for equipamento in equipamentos:
        #    elements.append(Paragraph(f'<b>Equipamento:</b> {equipamento["nome_equipamento"]}', styleN))
        #    elements.append(Paragraph(f'<b>Equipamento:</b> {equipamento["nome_equipamento"]}', styleN_left_indent))
            elements.append(Paragraph(f'<b>Equipamento:</b> {equipamento["nome_equipamento"]} - ({equipamento["numero_equipamento"]})', styleN_left_indent))
            elements.append(Spacer(1, 12))  # Quebra de linha após o nome do equipamento

            # Criar os rótulos do eixo X
            x_labels = gerar_lista_tempo(equipamento['data_previsto'], len(equipamento['valores_reais']))

            # Gerar a legenda
            fig, ax = plt.subplots(figsize=(6, 0.5))
            ax.plot([], [], color='blue', linestyle='-', label='Valores reais do load speed %')
            ax.plot([], [], color='red', linestyle='--', label='Valores previstos do load speed %')
            ax.legend(loc='center', frameon=False)
            ax.axis('off')
            temp_img_legend = tempfile.mktemp(suffix=".png")
            fig.savefig(temp_img_legend, bbox_inches='tight')
            plt.close(fig)

            legend_img = Image(temp_img_legend, width=280, height=40)
            elements.append(legend_img)
            elements.append(Spacer(1, 12))  # Espaço após a legenda

            # Gerar o gráfico
            plt.figure(figsize=(6, 3))
            plt.plot(equipamento['valores_reais'], color='blue', linestyle='-')
            plt.plot(equipamento['valores_previstos'], color='red', linestyle='--')
            plt.xlabel('Tempo')
            plt.ylabel('Valores')
            plt.title(f'{equipamento["nome_equipamento"]}')
            plt.grid(True)
            plt.xticks(ticks=range(len(x_labels)), labels=x_labels, rotation=45)

            temp_img = tempfile.mktemp(suffix=".png")
            plt.savefig(temp_img, bbox_inches='tight')
            plt.close()

            graph = Image(temp_img, width=280, height=160)
            elements.append(graph)

            # plt.figure(figsize=(6, 3))
            # plt.plot(equipamento['valores_reais'], color='blue', linestyle='-')
            # plt.plot(equipamento['valores_previstos'], color='red', linestyle='--')
            # plt.xlabel(' ')
            # plt.ylabel('Valores')
            # plt.title(f'{equipamento["nome_equipamento"]}')
            # plt.grid(True)

            # temp_img = tempfile.mktemp(suffix=".png")
            # plt.savefig(temp_img, bbox_inches='tight')
            # plt.close()

            # graph = Image(temp_img, width=240, height=120)
            # elements.append(graph)

            elements.append(Spacer(1, 12))  # Quebra de linha após o nome do equipamento
            elements.append(Paragraph(f'<b>Data Previsto:</b> {equipamento["data_previsto"]}', styleN_left_indent))
            elements.append(Paragraph(f'<b>Data Previsto Saída:</b> {equipamento["data_previsto_saida"]}', styleN_left_indent))
            elements.append(get_paragraph_style(f"<b>Data Falha:</b> {equipamento['data_quebra']}", equipamento['data_quebra'] != 'Não houve falha'))
            elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos

            elements.append(Paragraph(f'<b>Tempo Anormalidade:</b> {equipamento["tempo_anormalidade"]}', styleN_left_indent))
            elements.append(Paragraph(f'<b>Tempo Total:</b> {equipamento["tempo_total"]}', styleN_left_indent))
            elements.append(Paragraph(f'<b>Status:</b> {equipamento["alerta_status"]}', styleN_left_indent))
        #    elements.append(Paragraph(f'<b>Porcentagem de Diferença:</b> {equipamento["diferenca_formatada"]}', styleN_left_indent))
            porcentagem_diferenca = float(equipamento['diferenca_formatada']) if equipamento['diferenca_formatada'] != 'N/A' else None
            porcentagem_diferenca_color = format_difference(porcentagem_diferenca) if porcentagem_diferenca is not None else "black"
            elements.append(porcentagem_diferenca_color)
            
            elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos

            # Verifica se os dados de pressão do óleo são válidos antes de adicionar
            if equipamento['pressao_oleo_data_1'] != 'N/A' or equipamento['pressao_oleo_data_2'] != 'N/A' or equipamento['pressao_oleo_data_3'] != 'N/A':
                elements.append(Paragraph(f'<b>Pressão do Óleo</b>', styleN_left_indent))
                
                pressao_oleo_data_1 = float(equipamento['pressao_oleo_data_1']) if equipamento['pressao_oleo_data_1'] != 'N/A' else None
                pressao_oleo_data_2 = float(equipamento['pressao_oleo_data_2']) if equipamento['pressao_oleo_data_2'] != 'N/A' else None
                pressao_oleo_data_3 = float(equipamento['pressao_oleo_data_3']) if equipamento['pressao_oleo_data_3'] != 'N/A' else None

                pressao_oleo_data_1_color = get_color(pressao_oleo_data_1, 4.8, 5.0) if pressao_oleo_data_1 is not None else "black"
                pressao_oleo_data_2_color = get_color(pressao_oleo_data_2, 4.8, 5.0) if pressao_oleo_data_2 is not None else "black"
                pressao_oleo_data_3_color = get_color(pressao_oleo_data_3, 4.8, 5.0) if pressao_oleo_data_3 is not None else "black"

                texto_detalhado_pressao = create_detailed_text(
                    equipamento['pressao_oleo_data_1'] if equipamento['pressao_oleo_data_1'] != 'N/A' else None,
                    equipamento['pressao_oleo_data_2'] if equipamento['pressao_oleo_data_2'] != 'N/A' else None,
                    equipamento['pressao_oleo_data_3'] if equipamento['pressao_oleo_data_3'] != 'N/A' else None,
                    pressao_oleo_data_1_color, pressao_oleo_data_2_color, pressao_oleo_data_3_color, "óleo"
                )
                elements.append(Paragraph(texto_detalhado_pressao, styleN_left_indent))
                elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos

            # Verifica se os dados de pressão de combustível são válidos antes de adicionar
            if equipamento['pressao_combustivel_data_1'] != 'N/A' or equipamento['pressao_combustivel_data_2'] != 'N/A' or equipamento['pressao_combustivel_data_3'] != 'N/A':
                elements.append(Paragraph(f'<b>Pressão do Combustível</b>', styleN_left_indent))
                
                pressao_combustivel_data_1 = float(equipamento['pressao_combustivel_data_1']) if equipamento['pressao_combustivel_data_1'] != 'N/A' else None
                pressao_combustivel_data_2 = float(equipamento['pressao_combustivel_data_2']) if equipamento['pressao_combustivel_data_2'] != 'N/A' else None
                pressao_combustivel_data_3 = float(equipamento['pressao_combustivel_data_3']) if equipamento['pressao_combustivel_data_3'] != 'N/A' else None

                pressao_combustivel_data_1_color = get_color(pressao_combustivel_data_1, 4.5, 5.0) if pressao_combustivel_data_1 is not None else "black"
                pressao_combustivel_data_2_color = get_color(pressao_combustivel_data_2, 4.5, 5.0) if pressao_combustivel_data_2 is not None else "black"
                pressao_combustivel_data_3_color = get_color(pressao_combustivel_data_3, 4.5, 5.0) if pressao_combustivel_data_3 is not None else "black"

                texto_detalhado_combustivel = create_detailed_text(
                    equipamento['pressao_combustivel_data_1'] if equipamento['pressao_combustivel_data_1'] != 'N/A' else None,
                    equipamento['pressao_combustivel_data_2'] if equipamento['pressao_combustivel_data_2'] != 'N/A' else None,
                    equipamento['pressao_combustivel_data_3'] if equipamento['pressao_combustivel_data_3'] != 'N/A' else None,
                    pressao_combustivel_data_1_color, pressao_combustivel_data_2_color, pressao_combustivel_data_3_color, "combustível"
                )
                elements.append(Paragraph(texto_detalhado_combustivel, styleN_left_indent))
                elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos


            # Verifica se os dados de temperatura da água são válidos antes de adicionar
            if equipamento['temperatura_agua_data_1'] != 'N/A' or equipamento['temperatura_agua_data_2'] != 'N/A' or equipamento['temperatura_agua_data_3'] != 'N/A':
                elements.append(Paragraph(f'<b>Temperatura da Água</b>', styleN_left_indent))

                temp_agua_data_1 = float(equipamento['temperatura_agua_data_1']) if equipamento['temperatura_agua_data_1'] != 'N/A' else None
                temp_agua_data_2 = float(equipamento['temperatura_agua_data_2']) if equipamento['temperatura_agua_data_2'] != 'N/A' else None
                temp_agua_data_3 = float(equipamento['temperatura_agua_data_3']) if equipamento['temperatura_agua_data_3'] != 'N/A' else None

                temp_agua_data_1_color = get_temp_color(temp_agua_data_1, 90, 103, 60, 90) if temp_agua_data_1 is not None else "black"
                temp_agua_data_2_color = get_temp_color(temp_agua_data_2, 90, 103, 60, 90) if temp_agua_data_2 is not None else "black"
                temp_agua_data_3_color = get_temp_color(temp_agua_data_3, 90, 103, 60, 90) if temp_agua_data_3 is not None else "black"

                texto_detalhado_temperatura = create_detailed_text_temp(
                    equipamento['temperatura_agua_data_1'] if equipamento['temperatura_agua_data_1'] != 'N/A' else None,
                    equipamento['temperatura_agua_data_2'] if equipamento['temperatura_agua_data_2'] != 'N/A' else None,
                    equipamento['temperatura_agua_data_3'] if equipamento['temperatura_agua_data_3'] != 'N/A' else None,
                    temp_agua_data_1_color, temp_agua_data_2_color, temp_agua_data_3_color
                )
                elements.append(Paragraph(texto_detalhado_temperatura, styleN_left_indent))
                
                elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos

            # Verifica se os dados de tensão entre as fases são válidos antes de adicionar
            if equipamento['tensao_l1_l2_data'] != 'N/A' or equipamento['tensao_l2_l3_data'] != 'N/A' or equipamento['tensao_l3_l1_data'] != 'N/A':
                elements.append(Paragraph(f'<b>Tensão entre as Fases</b>', styleN_left_indent))
                
                tensao_l1_l2_data = float(equipamento['tensao_l1_l2_data']) if equipamento['tensao_l1_l2_data'] != 'N/A' else None
                tensao_l2_l3_data = float(equipamento['tensao_l2_l3_data']) if equipamento['tensao_l2_l3_data'] != 'N/A' else None
                tensao_l3_l1_data = float(equipamento['tensao_l3_l1_data']) if equipamento['tensao_l3_l1_data'] != 'N/A' else None

                tensao_L1_L2_data_color, tensao_L2_L3_data_color, tensao_L3_L1_data_color = get_color_tensao(tensao_l1_l2_data, tensao_l2_l3_data, tensao_l3_l1_data)

                texto_detalhado_tensao = create_detailed_text_tensao(
                    equipamento['tensao_l1_l2_data'] if equipamento['tensao_l1_l2_data'] != 'N/A' else None,
                    equipamento['tensao_l2_l3_data'] if equipamento['tensao_l2_l3_data'] != 'N/A' else None,
                    equipamento['tensao_l3_l1_data'] if equipamento['tensao_l3_l1_data'] != 'N/A' else None,
                    tensao_L1_L2_data_color, tensao_L2_L3_data_color, tensao_L3_L1_data_color, "L"
                )
                elements.append(Paragraph(texto_detalhado_tensao, styleN_left_indent))
                elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos



            elements.append(Paragraph(f'<b>Alarmes:</b> {equipamento["alarmes_text"]}', styleN_left_indent))
            elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos

            causas_solucoes = buscar_causa_solucao(equipamento["alarmes_text"])
            alarmes_adicionados = set()
            if causas_solucoes:
                elements.append(Paragraph(f'<b>Descrição dos alarmes:</b>', styleN_left_indent))
                elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos
                for item in causas_solucoes:
                    if item["alarme"] not in alarmes_adicionados:
                        elements.append(Paragraph(f'<b>Alarme:</b> {item["alarme"]}', styleN))
                        if item.get("causa"):
                            elements.append(Paragraph(f'<b>Possível causa:</b> {item["causa"]}', cause_solution_style))
                        if item.get("solucao"):
                            elements.append(Paragraph(f'<b>Possível solução:</b> {item["solucao"]}', cause_solution_style))
                        elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre causas/soluções
                        alarmes_adicionados.add(item["alarme"])

            elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos

            sequencias = detectar_sequencia(equipamento["alarmes_text"])
            if sequencias:
                elements.append(Paragraph('<b>Sequências de alarmes:</b>', styleN_left_indent))
                elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre seções
                for base_nome, numeros in sequencias:
                    elements.append(Paragraph(f'<b>Alarme base:</b> {base_nome}', styleN))
                    elements.append(Paragraph(f'<b>Sequência:</b> {", ".join(map(str, numeros))}', styleN))
                    elements.append(Paragraph(f'<b>Informação adicional:</b> Sequência de alarmes até a parada do equipamento com o alarme {base_nome} 3', styleN))
                    elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre sequências

            elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos
            
    
    doc.build(elements, onFirstPage=header, onLaterPages=header)

    return temp_pdf


# Lista de alarmes com suas possíveis causas e soluções
alarmes_list = [
    {
        "nome": "GENERATOR REVERSE POWER 1",
        "causa": "Entupimento dos filtros",
        "solucao": "Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível"
    },
    {
        "nome": "GENERATOR REVERSE POWER 2",
        "causa": "Entupimento dos filtros",
        "solucao": "Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível"
    },
    {
        "nome": "GENERATOR OVERCURRENT 1",
        "causa": "Verificar se há curto-circuito, carga aplicada, diodos, regulador de tensão",
        "solucao": "Verificar se há curto-circuito, carga aplicada, diodos, regulador de tensão"
    },
    {
        "nome": "GENERATOR OVERCURRENT 2",
        "causa": "Verificar se há curto-circuito, carga aplicada, diodos, regulador de tensão",
        "solucao": "Verificar se há curto-circuito, carga aplicada, diodos, regulador de tensão"
    },
    {
        "nome": "GENERATOR OVERCURRENT 3",
        "causa": "Verificar se há curto-circuito, carga aplicada, diodos, regulador de tensão",
        "solucao": "Verificar se há curto-circuito, carga aplicada, diodos, regulador de tensão"
    },
    {
        "nome": "GENERATOR OVERCURRENT 4",
        "causa": "Verificar se há curto-circuito, carga aplicada, diodos, regulador de tensão",
        "solucao": "Verificar se há curto-circuito, carga aplicada, diodos, regulador de tensão"
    },
    {
        "nome": "GENERATOR FAST OVERCURRENT 1",
        "causa": "Verificar se há curto-circuito, carga aplicada, se há muitos motores ligados aos geradores",
    },
    {
        "nome": "GENERATOR FAST OVERCURRENT 2",
        "causa": "Verificar se há curto-circuito, carga aplicada, se há muitos motores ligados aos geradores",
    },
    {
        "nome": "GENERATOR OVERVOLTAGE 1",
        "causa": "Diodos em curto, ajuste no regulador de tensão",
    },
    {
        "nome": "GENERATOR OVERVOLTAGE 2",
        "causa": "Diodos em curto, ajuste no regulador de tensão",
    },
    {
        "nome": "GENERATOR UNDERVOLTAGE 1",
        "causa": "Ajuste no regulador de tensão, carga indutiva, cabos do alternador rompidos, excitratriz em curto",
        "solucao": "Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível",
    },
    {
        "nome": "GENERATOR UNDERVOLTAGE 2",
        "causa": "Ajuste no regulador de tensão, carga indutiva, cabos do alternador rompidos, excitratriz em curto",
        "solucao": "Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível",
    },
    {
        "nome": "GENERATOR UNDERVOLTAGE 3",
        "causa": "Ajuste no regulador de tensão, carga indutiva, cabos do alternador rompidos, excitratriz em curto",
        "solucao": "Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível",
    },
    {
        "nome": "GENERATOR OVERFREQUENCY 1",
        "causa": "Retirada muito rápida da carga, trip externo",
        "solucao": "Se o gerador tiver atuador eletromecânico: verificar folgas no braço atuador, bomba desregulada, ganho do regulador de velocidade desregulado",
    },
    {
        "nome": "GENERATOR OVERFREQUENCY 2",
        "causa": "Retirada muito rápida da carga, trip externo",
        "solucao": "Se o gerador tiver atuador eletromecânico: verificar folgas no braço atuador, bomba desregulada, ganho do regulador de velocidade desregulado",
    },
    {
        "nome": "GENERATOR OVERFREQUENCY 3",
        "causa": "Retirada muito rápida da carga, trip externo",
        "solucao": "Se o gerador tiver atuador eletromecânico: verificar folgas no braço atuador, bomba desregulada, ganho do regulador de velocidade desregulado",
    },
    {
        "nome": "GENERATOR UNDERFREQUENCY 1",
        "causa": "Carga alta aplicada instantaneamente, filtro entupido, diesel contaminado",
        "solucao": "Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível",
    },
    {
        "nome": "GENERATOR UNDERFREQUENCY 2",
        "causa": "Carga alta aplicada instantaneamente, filtro entupido, diesel contaminado",
        "solucao": "Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível",
    },
    {
        "nome": "GENERATOR UNDERFREQUENCY 3",
        "causa": "Carga alta aplicada instantaneamente, filtro entupido, diesel contaminado",
        "solucao": "Verificar entrada de ar, nível de combustível, obstrução do pescador de combustível",
    },
    {
        "nome": "BUSBAR OVERVOLTAGE 1",
        "causa": "Capacitores em excesso, concessionária com problemas no regulador de tensão",
        "solucao": "Verificar se o banco de capacitores está em modo automático",
    },
    {
        "nome": "BUSBAR OVERVOLTAGE 2",
        "causa": "Capacitores em excesso, concessionária com problemas no regulador de tensão",
        "solucao": "Verificar se o banco de capacitores está em modo automático",
    },
    {
        "nome": "BUSBAR OVERVOLTAGE 3",
        "causa": "Capacitores em excesso, concessionária com problemas no regulador de tensão",
        "solucao": "Verificar se o banco de capacitores está em modo automático",
    },
    {
        "nome": "BUSBAR UNDERVOLTAGE 1",
        "causa": "Problemas com a concessionária, geradores com excesso de reativo",
    },
    {
        "nome": "BUSBAR UNDERVOLTAGE 2",
        "causa": "Problemas com a concessionária, geradores com excesso de reativo",
    },
    {
        "nome": "BUSBAR UNDERVOLTAGE 3",
        "causa": "Problemas com a concessionária, geradores com excesso de reativo",
    },
    {
        "nome": "BUSBAR UNDERVOLTAGE 4",
        "causa": "Problemas com a concessionária, geradores com excesso de reativo",
    },
    {
        "nome": "DF/DT (ROCOF)",
        "causa": "Desbalanceamento na carga ou frequência instável",
        "solucao": "Verificar a estabilidade da frequência e o balanceamento da carga",
    },
    {
        "nome": "GENERATOR OVERLOAD 1",
        "causa": "Excesso de carga aplicada ao gerador",
        "solucao": "Reduzir a carga e verificar se há sobrecarga no gerador",
    },
    {
        "nome": "GENERATOR OVERLOAD 2",
        "causa": "Excesso de carga aplicada ao gerador",
        "solucao": "Reduzir a carga e verificar se há sobrecarga no gerador",
    },
    {
        "nome": "GENERATOR OVERLOAD 3",
        "causa": "Excesso de carga aplicada ao gerador",
        "solucao": "Reduzir a carga e verificar se há sobrecarga no gerador",
    },
    {
        "nome": "GENERATOR OVERLOAD 4",
        "causa": "Excesso de carga aplicada ao gerador",
        "solucao": "Reduzir a carga e verificar se há sobrecarga no gerador",
    },
    {
    "nome": "GENERATOR OVERLOAD 5",
    "causa": "Excesso de carga aplicada ao gerador",
    "solucao": "Reduzir a carga e verificar se há sobrecarga no gerador",
    },
    {
    "nome": "GENERATOR UNBALANCE CURRENT",
    "causa": "CARGA MAL DISTRIBUÍDA ENTRE AS FASES",
    "solucao": "VERIFICAR SE TEM TERMINAL ROMPIDO, TC EM CURTO-CIRCUITO",
    },
    {
    "nome": "GENERATOR UNBALANCE VOLTAGE",
    "causa": "QUEIMA DE PROTEÇÕES (REDE), CARGA MAL DISTRIBUÍDA",
    "solucao": "VERIFICAR ROMPIMENTO DE CABOS OU DO TERMINAL DE LIGAÇÃO DO ALTERNADOR",
    },
    {
    "nome": "GENERATOR REACTIVE POWER IMPORT (LOSS OF EXCIT)",
    "causa": "Perda de excitação do gerador",
    "solucao": "VERIFICAR REGULADOR DE TENSÃO",
    },
    {
    "nome": "GENERATOR REACTIVE POWER EXPORT (OVEREXCITATION)",
    "causa": "TENSÃO DO GERADOR ACIMA DA TENSÃO DO BARRAMENTO",
    "solucao": "CARGA INDUTIVA (MUITOS MOTORES LIGADOS)",
    },
    {
    "nome": "GOVERNOR REGULATION FAIL",
    "causa": "FILTROS SUJOS, FALTA DE DIESEL",
    "solucao": "FILTROS SUJOS, FALTA DE DIESEL",
    },
    {
    "nome": "OVERSPEED 1",
    "causa": "CARGA RETIRADA INSTANTANEAMENTE",
    "solucao": "FALTA DE COMBUSTÍVEL",
    },
    {
    "nome": "OVERSPEED 2",
    "causa": "CARGA RETIRADA INSTANTANEAMENTE",
    "solucao": "FALTA DE COMBUSTÍVEL",
    },
    {
    "nome": "HZ/VOLT FAILURE",
    "causa": "REGULADOR DE TENSÃO DESREGULADO",
    "solucao": "VERIFICAR SE HÁ ENTRADA DE AR E REGULADOR DE TENSÃO",
    },
    {
    "nome": "AUXILIARY POWER SUPPLY TERMINAL 1 UNDERVOLTAGE",
    "causa": "VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO",
    "solucao": "VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO",
    },
    {
        "nome": "AUXILIARY POWER SUPPLY TERMINAL 1 OVERVOLTAGE",
        "causa": "VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO",
        "solucao": "VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO",
    },
    {
    "nome": "AUXILIARY POWER SUPPLY TERMINAL 98 UNDERVOLTAGE",
    "causa": "VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO",
    "solucao": "VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO",
    },
    {
    "nome": "AUXILIARY POWER SUPPLY TERMINAL 98 OVERVOLTAGE",
    "causa": "VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO",
    "solucao": "VERIFICAR BATERIAS E CARREGADOR DE BATERIAS, ALTERNADOR DO MOTOR E CONTATO NO PENTE DE CONEXÃO",
    },
    {
    "nome": "EIC COOL WATER HIGH 1",
    "causa": "RADIADOR SUJO, VÁLVULA TERMOSTÁTICA TRAVADA, EXCESSO DE CARGA, CORREIA DA BOMBA D'ÁGUA ARREBENTADA",
    },
    {
    "nome": "EIC COOL WATER HIGH 2",
    "causa": "RADIADOR SUJO, VÁLVULA TERMOSTÁTICA TRAVADA, EXCESSO DE CARGA, CORREIA DA BOMBA D'ÁGUA ARREBENTADA",
    },
    {
    "nome": "EIC OIL PRESSURE LOW 1",
    "causa": "NÍVEL DE ÓLEO BAIXO/ALTO ",
    "solucao": "VERIFICAR NÍVEL DO ÓLEO: COMPLETAR SE ESTIVER BAIXO, REALIZAR A TROCA SE JÁ ESTIVER VENCIDO ",
    },
    {
    "nome": "EIC OIL PRESSURE LOW 2",
    "causa": "NÍVEL DE ÓLEO BAIXO/ALTO ",
    "solucao": "VERIFICAR NÍVEL DO ÓLEO: COMPLETAR SE ESTIVER BAIXO, REALIZAR A TROCA SE JÁ ESTIVER VENCIDO ",
    },
    {
    "nome": "EMERGENCY STOP",
    "causa": "acionamento da botoeira de emergencia ",
    "solucao": "VERIFICAR alarmes presentes na ECU e no controlador ",
    },
    {
    "nome": "EIC WARNING",
    "causa": "Problema com a comunicação do controlador do motor ",
    "solucao": "VERIFICAR alarmes presentes na ECU e no controlador ",
    },

]



# Função para buscar a possível causa e solução dos alarmes
def buscar_causa_solucao(alarmes_text):
    resultado = []
    alarmes = re.split(r',\s*', alarmes_text)

    for alarme in alarmes:
        for alarme_info in alarmes_list:
            if alarme.strip() == alarme_info["nome"]:
                resultado.append({
                    "alarme": alarme,
                    "causa": alarme_info.get("causa"),
                    "solucao": alarme_info.get("solucao")
                })
                break
    return resultado

# Função para detectar padrões de sequência de alarmes, ignorando alarmes com "MISSING ID"
def detectar_sequencia(alarmes_text):
    sequencias = {}
    alarmes = re.split(r',\s*', alarmes_text)

    for alarme in alarmes:
        base_nome = ' '.join(alarme.split()[:-1])
        numero = alarme.split()[-1]
        
        # Ignora alarmes que contenham "MISSING ID"
        if "MISSING ID" in base_nome:
            continue
        
        if numero.isdigit():
            numero = int(numero)
            if base_nome not in sequencias:
                sequencias[base_nome] = []
            sequencias[base_nome].append(numero)
    
    sequencia_final = []
    for base_nome, numeros in sequencias.items():
        numeros.sort()
        if any(num >= 3 for num in numeros):
            sequencia_final.append((base_nome, numeros))
    
    return sequencia_final




def formatar_data(data_str):
    # Verifica se data_str não é None, não é uma string vazia, e não é uma mensagem específica
    if data_str and data_str not in ['None', 'Não houve falha', 'Em operação', 'Indefinido']:
        try:
            data_obj = datetime.strptime(data_str, '%Y-%m-%d %H:%M:%S')
            return data_obj.strftime('%d/%m/%Y %H:%M:%S')
        except ValueError:
            pass
    # Retorna o valor original caso data_str seja uma mensagem específica ou inválida
    return data_str



def gerar_lista_tempo(data_inicial, intervalos, incremento_minutos=5):
    data_inicial_obj = datetime.strptime(data_inicial, '%d/%m/%Y %H:%M:%S')
    lista_tempo = [(data_inicial_obj + timedelta(minutes=i*incremento_minutos)).strftime('%H:%M') for i in range(intervalos)]
    return lista_tempo


async def clean_temp_files():
    temp_folder = "/tmp/"  # Altere para o caminho correto
    while True:
        try:
            # Encontrar todos os arquivos .pdf e imagens (assumindo .png e .jpg) na pasta tempfile
            pdf_files = glob.glob(os.path.join(temp_folder, "*.pdf"))
            image_files = glob.glob(os.path.join(temp_folder, "*.png")) + glob.glob(os.path.join(temp_folder, "*.jpg"))

            # Remover todos os arquivos encontrados
            for file_path in pdf_files + image_files:
                os.remove(file_path)
                print(f"Arquivo removido: {file_path}")

            # Aguardar 30 minutos (1800 segundos) antes de limpar novamente
            await asyncio.sleep(86400) # um dia

        except Exception as e:
            print(f"Erro ao limpar arquivos temporários: {str(e)}")
            await asyncio.sleep(3600)  # Aguarde 30 minutos mesmo em caso de erro para evitar loops rápidos








tabelas = 'sup_geral.leituras'

cod_campo_especificados = ['3', '114']


async def criar_tabela_usuarios_telegram(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS machine_learning.usuarios_telegram (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    usuario VARCHAR(100),
                    nome_supervisorio VARCHAR(100),
                    nome_telegram VARCHAR(100),
                    id_telegram BIGINT,
                    email VARCHAR(255) DEFAULT NULL,
                    bloqueado TINYINT DEFAULT 0,
                    primeiro_acesso TINYINT DEFAULT 1,
                    todos_modelo_funcionamento  TINYINT DEFAULT 0,
                    cod_usuario INT,
                    ativo TINYINT DEFAULT 1,
                    data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.commit()

async def criar_tabela_relatorio_quebras(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS machine_learning.relatorio_quebras (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    cod_equipamento INT,
                    cod_usina INT,
                    data_cadastro_previsto DATETIME DEFAULT NULL,
                    data_cadastro_quebra DATETIME DEFAULT NULL
                )
            """)
            await conn.commit()

async def criar_tabela_log_relatorio_quebras(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS machine_learning.log_relatorio_quebras (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    cod_equipamento INT,
                    cod_usina INT,
                    data_cadastro_previsto DATETIME DEFAULT NULL,
                    data_cadastro_quebra DATETIME DEFAULT NULL
                )
            """)
            await conn.commit()

async def criar_tabela_silenciar_bot(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS machine_learning.telegram_silenciar_bot (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    cod_usuario INT,
                    cod_equipamento INT,
                    tempo_silenciado INT,
                    receber_alarme TINYINT DEFAULT 0,
                    data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.commit()

async def criar_tabela_leituras(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS machine_learning.leituras_consecutivas (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    cod_equipamento INT,
                    cod_campo INT,
                    valor_1 FLOAT,
                    valor_2 FLOAT,
                    valor_3 FLOAT,
                    valor_4 FLOAT,
                    valor_5 FLOAT,
                    alerta TINYINT DEFAULT 0,
                    data_cadastro timestamp,
                    UNIQUE (cod_equipamento, cod_campo)
                )
            """)
            await conn.commit()

async def criar_tabela_valores_previsao(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS machine_learning.valores_previsao (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    cod_equipamento INT,
                    cod_usina INT,
                    data_cadastro DATETIME DEFAULT NULL,
                    data_cadastro_previsto DATETIME DEFAULT NULL,
                    data_cadastro_quebra DATETIME DEFAULT NULL,
                    alerta_80 TINYINT DEFAULT 0,
                    alerta_100 TINYINT DEFAULT 0,
                    previsao TINYINT DEFAULT 0,
                    valores_reais TEXT,
                    valores_previstos TEXT,
                    alarmes TEXT,
                    alarme_verificado TINYINT DEFAULT 0
                )
            """)
            await conn.commit()

async def criar_tabela_falhas_gerais(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS machine_learning.falhas_gerais (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    cod_equipamento INT,
                    cod_usina INT,
                    data_cadastro DATETIME DEFAULT NULL,
                    falha TINYINT DEFAULT 0,
                    alerta_80 TINYINT DEFAULT 0,
                    alerta_100 TINYINT DEFAULT 0,
                    previsao TINYINT DEFAULT 0
                )
            """)
            await conn.commit()

async def criar_tabela_usinas_usuario(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS machine_learning.usinas_usuario (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    cod_usuario INT,
                    cod_usina INT,
                    ativo TINYINT DEFAULT 1
                )
            """)
            await conn.commit()





async def selecionar_GMG(pool):
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT codigo, ativo FROM sup_geral.tipos_equipamentos WHERE classe = 'GMG'")
            resultados = await cursor.fetchall()
            codigos = [resultado['codigo'] for resultado in resultados]
        return codigos

async def obter_equipamentos_validos(tabelas, pool):
    codigos_GMG = await selecionar_GMG(pool)
    codigos_GMG_str = ', '.join(map(str, codigos_GMG))

    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
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



print('BOT inicializado.')

async def id_chat_grupo(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT id_telegram FROM machine_learning.usuarios_telegram WHERE usuario = 'Grupo'")
            result = await cursor.fetchone()
            if result is not None:
                ID_DO_CHAT_DO_GRUPO = result[0]
                return ID_DO_CHAT_DO_GRUPO
            else:
                print("Nenhum grupo encontrado")
                return None



async def id_chat_usuario(username, pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT id_telegram FROM machine_learning.usuarios_telegram WHERE usuario = %s", (username,))
            result = await cursor.fetchone()
            if result is not None:
                id_usuario = result[0]
                return id_usuario
            else:
                print("Usuário não encontrado")
                return None

        

async def enviar_menu_grupo(chat_id, state: FSMContext):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (chat_id,))
            result = await cursor.fetchone()
            
            if result:
                user_type = result[0]

                # Verificar se já existe uma mensagem armazenada no estado
                user_data = await state.get_data()
                if 'menu_message_id' in user_data:
                    try:
                        # Verificar se a mensagem ainda existe
                        await bot.edit_message_text(chat_id=chat_id, message_id=user_data['menu_message_id'], text="Recarregando o Menu...", reply_markup=None)
                    except:
                        # Se a mensagem não existir, envia novamente
                        await send_menu(chat_id, user_type, state)
                else:
                    await send_menu(chat_id, user_type, state)

async def send_menu(chat_id, user_type, state: FSMContext):
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    if user_type == 'Grupo':
        buttons = [
            InlineKeyboardButton("Digitar código da usina", callback_data='1'),
            InlineKeyboardButton("Digitar código do equipamento", callback_data='2'),
            InlineKeyboardButton("Inserir usuario", callback_data='3'),
            InlineKeyboardButton("Editar usuario ativo", callback_data='4'),
        ]
    else:
        buttons = [
            InlineKeyboardButton("Digitar código da usina", callback_data='1'),
            InlineKeyboardButton("Digitar código do equipamento", callback_data='2'),
            InlineKeyboardButton("Editar Usinas cadastradas", callback_data='5'),
            InlineKeyboardButton("Receber todos os tipos de notificações", callback_data='6'),
        ]
    
    keyboard.add(*buttons)
    sent_message = await bot.send_message(chat_id, "Escolha uma opção:", reply_markup=keyboard)
    
    # Armazenar o ID da mensagem no estado do FSMContext
    await state.update_data(menu_message_id=sent_message.message_id)

async def criar_menu():
#    menu_button = types.KeyboardButton('Menu')
    menu_button = types.KeyboardButton('/menu')
    teclado_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
    teclado_menu.add(menu_button)
    return teclado_menu




#@dp.message_handler(lambda message: message.text == "Geradores Em Operação")
@dp.message_handler(lambda message: message.text == "/geradores")
async def teste_menu(message: types.Message):
    print('**************************************** inicio geradores *************************************************************************')
    tempo_inicial = datetime.now()
    data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')
    print('clicou no teste', data_cadastro_formatada)
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização

    if message.from_user.id:
        user_id = message.from_user.id
    else:
        user_message = global_vars["user_message"]
        user_id = user_message.chat.id

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:   
            await cursor.execute("SELECT nome_telegram FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
            result = await cursor.fetchone()
            nome_usuario = ''
            if not result:
                return await message.reply("Usuário não encontrado.")
            nome_usuario = result[0]
                
    id_grupo = global_vars["id_grupo"]
    mensagem_operacao = f'Usuário {nome_usuario} pediu Geradores Em Operação.'
    try:
        await bot.send_message(id_grupo, mensagem_operacao)
    except Exception as e:
        await message.reply(f"Erro ao enviar mensagem ao grupo: {e}")

    try:
        chat_id = message.chat.id
        print('chat_id', chat_id)

        async with pool.acquire() as conn:
            await conn.ping(reconnect=True)  # Forçar reconexão
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (chat_id,))
                result = await cursor.fetchone()

                if result is not None:
                    username = result[0]
                    timestamp = int(time.mktime(message.date.timetuple()))
                    user_message = types.Message(message_id=message.message_id, from_user=message.from_user, chat=message.chat, date=timestamp, text=username)
                #    global_vars["user_message"] = user_message
                
                
                    user_message = types.Message(
                        message_id=message.message_id, 
                        from_user=message.from_user, 
                        chat=message.chat, 
                        date=timestamp, 
                        text=message.text  # ou qualquer outro texto que deseja
                    )
                    global_vars["user_message"] = user_message
                    print('user_message dentro do teste Menu',user_message)

                    async with pool.acquire() as conn:
                        await conn.ping(reconnect=True)  # Forçar reconexão
                        async with conn.cursor() as cursor:
                            await cursor.execute("SELECT nome, codigo FROM sup_geral.usuarios WHERE login = %s", (username,))
                            result = await cursor.fetchone()
                            if result is not None:
                                nome_supervisorio, cod_usuario = result

                                usinas_equipamentos = {}
                                await cursor.execute("SELECT codigo FROM sup_geral.usinas WHERE ativo = 1")
                                cod_usinas = await cursor.fetchall()
                                mensagem_total_equipamentos = ""

                                mensagem = f"Usina de {html.escape(nome_supervisorio)} e seus equipamentos:\n\n"
                                total_equipamentos = 0
                                total_equipamentos_true = 0

                                for cod_usina_tuple in cod_usinas:
                                    cod_usina = cod_usina_tuple[0]
                                    codigos_GMG = await selecionar_GMG(pool)

                                    placeholders = ', '.join(['%s'] * len(codigos_GMG))
                                    query = f"SELECT codigo FROM sup_geral.equipamentos WHERE cod_usina = %s AND cod_tipo_equipamento IN ({placeholders}) AND ativo = 1"
                                    await cursor.execute(query, [cod_usina] + list(codigos_GMG))
                                    cod_equipamentos = await cursor.fetchall()

                                    for equipamento_tuple in cod_equipamentos:
                                        equipamento = equipamento_tuple[0]
                                        await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(equipamento),))
                                        valores_atuais = await cursor.fetchone()

                                        if valores_atuais is not None and all(valor != 0 for valor in valores_atuais):
                                            coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(equipamento), pool)
                                            previsoes, alerta = await fazer_previsao(valores_atuais, coeficiente_existente, intercepto_existente, int(equipamento), pool)

                                            if previsoes and not all(valor == 0 for valor in previsoes):
                                                total_equipamentos += 1
                                                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (int(equipamento),))
                                                valores_atuais_114 = await cursor.fetchone()

                                                valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                                equipamentos_str = f'\n\nValores Atuais: {valores_atuais_str} \nValores Previstos: {previsoes} \n{alerta}' if alerta else f'{alerta}'
                                                if alerta:
                                                    total_equipamentos_true += 1

                                                if cod_usina not in usinas_equipamentos:
                                                    await cursor.execute("SELECT nome FROM sup_geral.usinas WHERE codigo = %s", (cod_usina,))
                                                    nome_usina = (await cursor.fetchone())[0]
                                                    usinas_equipamentos[cod_usina] = {'nome': nome_usina, 'equipamentos': []}

                                                usinas_equipamentos[cod_usina]['equipamentos'].append(f'Equipamento: {equipamento}: {equipamentos_str}\n\n')

                                mensagem = f"Usina de {html.escape(nome_supervisorio)} e seus equipamentos:\n\n"
                                for cod_usina, info in usinas_equipamentos.items():
                                #    mensagem += f'<b>Usina: {html.escape(info["nome"])}</b>\n\n<a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n'
                                    mensagem += f'<b>Usina: {html.escape(info["nome"])}</b>\n\n'
                                    for equipamento in info['equipamentos']:
                                        mensagem += html.escape(equipamento)

                                if len(mensagem) > 4000:
                                    partes_mensagem = [mensagem[i:i + 4000] for i in range(0, len(mensagem), 4000)]
                                    for parte in partes_mensagem:
                                        await bot.send_message(chat_id, parte, parse_mode='HTML')
                                else:
                                    await bot.send_message(chat_id, mensagem, parse_mode='HTML')

                                mensagem_total_equipamentos += f"\n\nGeradores Em Operação: {total_equipamentos}\nGeradores Em alerta: {total_equipamentos_true}\n"
                                print(f"\n\nGeradores Em Operação: {total_equipamentos}\nGeradores Em alerta: {total_equipamentos_true}\n")
                                await bot.send_message(chat_id, mensagem_total_equipamentos)
                                print('************************************************ fim dos geradores *****************************************************************')
                                sys.stdout.flush()
    except Exception as e:
        print(f"Erro em Geradores Em Operação: {e}")
        sys.stdout.flush()
                

@dp.message_handler(lambda message: message.text == "/menu", state='*')
async def Menu(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    print('clicou no Menu', chat_id)
    await enviar_menu_grupo(chat_id, state)  # Passa o 'state' aqui
    
class Form(StatesGroup):
    cod_usina = State()
    cod_equipamento = State()
    usuario = State()
    edit_usuario = State()
    ask_supervisor_name = State()
    ask_supervisor_edit_name = State()
    editar_usinas = State()

async def salvar_silenciamento(cod_usuario, cod_equipamento, tempo_silenciado):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor() as cursor:
            await cursor.execute("""
            SELECT * FROM machine_learning.telegram_silenciar_bot WHERE cod_usuario = %s AND cod_equipamento = %s
            """, (cod_usuario, cod_equipamento))
            result = await cursor.fetchone()
            if result:
                await cursor.execute("""
                UPDATE machine_learning.telegram_silenciar_bot
                SET tempo_silenciado = %s, data_cadastro = CURRENT_TIMESTAMP
                WHERE cod_usuario = %s AND cod_equipamento = %s
                """, (tempo_silenciado, cod_usuario, cod_equipamento))
            else:
                await cursor.execute("""
                INSERT INTO machine_learning.telegram_silenciar_bot (cod_usuario, cod_equipamento, tempo_silenciado)
                VALUES (%s, %s, %s)
                """, (cod_usuario, cod_equipamento, tempo_silenciado))
            await conn.commit()

async def buscar_cod_usuario(id_telegram):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor() as cursor:
            await cursor.execute("""
            SELECT cod_usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s
            """, (id_telegram,))
            result = await cursor.fetchone()
            return result[0] if result else None

async def buscar_cod_equipamentos(cod_usina):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor() as cursor:
            await cursor.execute("""
            SELECT codigo FROM sup_geral.equipamentos WHERE cod_usina = %s AND ativo = 1
            """, (cod_usina,))
            result = await cursor.fetchall()
            return [row[0] for row in result]


async def buscar_equipamentos_com_alerta(cod_usina):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor() as cursor:
            await cursor.execute("""
            SELECT codigo, nome FROM sup_geral.equipamentos WHERE cod_usina = %s AND ativo = 1
            """, (cod_usina,))
            equipamentos = await cursor.fetchall()
            
            equipamentos_com_alerta = []
            for codigo, nome in equipamentos:
                await cursor.execute("""
                SELECT alerta FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND alerta = 1
                """, (codigo,))
                alerta = await cursor.fetchone()
                if alerta:
                    equipamentos_com_alerta.append((codigo, nome))
    
    return equipamentos_com_alerta

async def buscar_alarmes_ativos(conn, cod_equipamento):
    limite_tempo = datetime.now() - timedelta(hours=6)
    async with conn.cursor() as cursor:
        await cursor.execute("""
        SELECT cod_alarme, visto, data_cadastro FROM sup_geral.alarmes_ativos
        WHERE cod_equipamento = %s AND (visto = 0 OR visto IS NULL) AND data_cadastro > %s
        """, (cod_equipamento, limite_tempo))
        result = await cursor.fetchall()
    return result

async def enviar_alarme_botao(cod_usina, query_user_id):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    cod_usuario = await buscar_cod_usuario(query_user_id)
    id_telegram = query_user_id
    
    if cod_usuario is None:
        print("Erro: usuário não encontrado na tabela machine_learning.usuarios_telegram.")
        return
    print('cod_usuario', cod_usuario)
    
    cod_equipamentos = await buscar_cod_equipamentos(cod_usina)
    if not cod_equipamentos:
        print(f"Erro: nenhum equipamento ativo encontrado para a usina {cod_usina}.")
        return
    print('cod_equipamentos', cod_equipamentos)

    equipamentos_alerta = await buscar_equipamentos_com_alerta(cod_usina)
    print("Equipamentos com alerta na usina", cod_usina, ":", equipamentos_alerta)

    mensagens_alarme = []

    if equipamentos_alerta:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for cod_equipamento, _ in equipamentos_alerta:
                    await cursor.execute("""
                    SELECT * FROM machine_learning.telegram_silenciar_bot 
                    WHERE cod_usuario = %s AND cod_equipamento = %s
                    """, (cod_usuario, cod_equipamento))
                    result = await cursor.fetchone()

                    if result:
                        await cursor.execute("""
                        UPDATE machine_learning.telegram_silenciar_bot
                        SET receber_alarme = 1
                        WHERE cod_usuario = %s AND cod_equipamento = %s
                        """, (cod_usuario, cod_equipamento))
                        await conn.commit()

                    else:
                        await cursor.execute("""
                        INSERT INTO machine_learning.telegram_silenciar_bot (cod_usuario, cod_equipamento, tempo_silenciado, receber_alarme, data_cadastro)
                        VALUES (%s, %s, 0, 1, %s)
                        """, (cod_usuario, cod_equipamento, datetime.now()))
                        await conn.commit()

                    alarmes_ativos = await buscar_alarmes_ativos(conn, cod_equipamento)
#                    print("Alarmes ativos para o equipamento", cod_equipamento, ":", alarmes_ativos)

                    if alarmes_ativos:
                        for alarme in alarmes_ativos:
                            if alarme:
                                cod_alarme = alarme[0]
                                visto = alarme[1]
                                data_cadastro = alarme[2]
                                print(f"Alarme ativo para o equipamento {cod_equipamento} : ({cod_alarme}, {visto}, {data_cadastro})")
                            else:
                                print(f"Alarme inválido retornado para o equipamento {cod_equipamento}")
                    else:
                        print(f"Sem alarmes ativos para o equipamento {cod_equipamento}")
            
                    descricoes_alarmes = []

                    for cod_alarme, _, _ in alarmes_ativos:
                        await cursor.execute("""
                        SELECT nome, descricao FROM sup_geral.lista_alarmes WHERE codigo = %s
                        """, (cod_alarme,))
                        alarme_info = await cursor.fetchone()
                        if alarme_info:
                            nome_alarme, descricao_alarme = alarme_info
                            print("Nome do alarme:", nome_alarme)
                            print("Descrição do alarme:", descricao_alarme)
                            descricoes_alarmes.append(descricao_alarme)
                        else:
                            print(f"Alarme com código {cod_alarme} não encontrado na tabela lista_alarmes.")

                    # Cria a mensagem de alarme com todas as descrições dos alarmes
                    if descricoes_alarmes:

                        await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                        result = await cursor.fetchone()
                        if result is not None:
                            nome, cod_usina, cod_usuario = result
                                
                        mensagem = f"🚨 Alarme ativo para o equipamento {cod_equipamento} - {nome}: \n\n" + "\n\n".join(descricoes_alarmes)
                        print( f"🚨 Alarme ativo para o equipamento {cod_equipamento} - {nome}: \n\n" + "\n\n".join(descricoes_alarmes))
                        mensagens_alarme.append(mensagem)
                    
                if mensagens_alarme:
                    await bot.send_message(id_telegram, "\n\n".join(mensagens_alarme), parse_mode='HTML')
                else:

                    await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                    result = await cursor.fetchone()
                    if result is not None:
                        nome, cod_usina, cod_usuario = result
                            
                    mensagem = f"🚨 Nenhum alarme encontrado para o equipamento {cod_equipamento} - {nome}, ou o alarme tem mais de 6 horas ativo"
                    print( f"🚨 Nenhum alarme encontrado para o equipamento {cod_equipamento} - {nome}")
                    await bot.send_message(id_telegram, mensagem, parse_mode='HTML')

                    print("Nenhum alarme encontrado.")

                await conn.commit()
                print("Atualização da tabela machine_learning.telegram_silenciar_bot concluída com sucesso.")
    else:
        print(f"Nenhum equipamento com alerta encontrado para a usina {cod_usina}.")




async def verificar_alarmes(pool):
#    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    while True:
        try:
            async with pool.acquire() as conn:
                await conn.ping(reconnect=True)  # Forçar reconexão
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                    SELECT * FROM machine_learning.telegram_silenciar_bot
                    WHERE receber_alarme = 1
                    """)
                    alarmes = await cursor.fetchall()
                    for alarme in alarmes:
                        try:
                            cod_usuario, cod_equipamento, tempo_silenciado = alarme[1], alarme[2], alarme[3]

                            await cursor.execute("""
                            SELECT alerta, cod_campo, valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas
                            WHERE cod_equipamento = %s
                            """, (cod_equipamento,))
                            leitura = await cursor.fetchone()
            #                print('cod_equipamento - ', cod_equipamento, ' leitura_consecutivas', leitura)

                            if leitura and (leitura[0] == 0 or (leitura[1] == 114 and all(value == 0 for value in leitura[2:]))):
                                if tempo_silenciado == 0:
                                    await cursor.execute("""
                                    DELETE FROM machine_learning.telegram_silenciar_bot
                                    WHERE cod_usuario = %s AND cod_equipamento = %s AND receber_alarme = 1 AND tempo_silenciado = 0
                                    """, (cod_usuario, cod_equipamento))
                                    await conn.commit()
                                    print(f"Entrada removida para o usuário {cod_usuario} e equipamento {cod_equipamento}.")
                                else:
                                    await cursor.execute("""
                                    UPDATE machine_learning.telegram_silenciar_bot
                                    SET receber_alarme = 0
                                    WHERE cod_usuario = %s AND cod_equipamento = %s
                                    """, (cod_usuario, cod_equipamento))
                                    await conn.commit()
                                    print(f"Coluna receber_alarme atualizada para 0 para o usuário {cod_usuario} e equipamento {cod_equipamento}.")

                                alarmes_ativos = await buscar_alarmes_ativos(conn, cod_equipamento)
                                print("Alarmes ativos para o equipamento", cod_equipamento, ":", alarmes_ativos)

                                await cursor.execute("""
                                SELECT id_telegram FROM machine_learning.usuarios_telegram WHERE cod_usuario = %s
                                """, (cod_usuario,))
                                id_telegram = (await cursor.fetchone())[0]

                                descricoes_alarmes = []

                                for alarme_info in alarmes_ativos:
                                    cod_alarme, _, _ = alarme_info
                                    await cursor.execute("""
                                    SELECT nome, descricao FROM sup_geral.lista_alarmes WHERE codigo = %s
                                    """, (cod_alarme,))
                                    alarme_descricao = await cursor.fetchone()
                                    if alarme_descricao:
                                        nome_alarme, descricao_alarme = alarme_descricao
                                        descricoes_alarmes.append(descricao_alarme)
                                    else:
                                        print(f"Alarme com código {cod_alarme} não encontrado na tabela lista_alarmes.")

                                # Cria a mensagem de alarme com todas as descrições dos alarmes
                                if descricoes_alarmes:
                                    mensagem = f"{cod_equipamento} 🚨 Alarme: \n\n" + "\n\n".join(descricoes_alarmes) + "\n\nO equipamento está sem potência ativa"
                                    await bot.send_message(id_telegram, mensagem, parse_mode='HTML')

                                    # Remover da lista 'sem_mensagem_silenciado' após enviar a mensagem
                                    await cursor.execute("SELECT cod_usina FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                    cod_usina_silenciada = (await cursor.fetchone())[0]

                                    # Remover o par (cod_usina_silenciada, cod_usuario) de sem_mensagem_silenciado
                                    sem_mensagem_silenciado.discard((cod_usina_silenciada, cod_usuario))
                                    print(f"Removido (cod_usina_silenciada: {cod_usina_silenciada}, cod_usuario: {cod_usuario}) da lista sem_mensagem_silenciado.")


                            await asyncio.sleep(1)
                        
                        except Exception as e:
                            print(f"Ocorreu um erro ao processar o equipamento {cod_equipamento}: {str(e)}")


            await asyncio.sleep(300)
#            await asyncio.sleep(10)

        except Exception as e:
            print(f"Erro durante a execução de verificar_alarmes: {e}")
            await asyncio.sleep(120)




codigos_alarmes_desejados = [1, 240, 243, 244, 253, 256, 258, 259, 262, 265, 269, 272, 273, 279, 280, 281, 301, 304, 333, 350, 351, 352, 353, 356, 357, 381, 383, 384, 385, 386, 387, 388, 389, 390, 400, 401, 404, 405,411,412,413,414,415,416, 471, 472, 473,528, 590, 591, 592, 593, 594,595,596,597,598,599,600, 602, 603, 604, 611,615,616,617,631, 635, 637, 638, 657, 658,669,678, 725, 727, 728, 729, 730, 731, 732, 735]


async def monitorar_leituras_consecutivas(pool):

    equipamentos_com_alerta = {}
    # Dicionário para armazenar alarmes e cod_equipamento
    equipamentos_alerta_0 = {}

    while True:

        tempo_inicial = datetime.now()
        data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')
    #    print('\n','------------------------------------------------------------- inicio monitorar_leituras_consecutivas --------------------------------------------------------------------------', data_cadastro_formatada,'\n')

        # print('equipamentos_com_alerta',equipamentos_com_alerta)
        # print('equipamentos_alerta_0',equipamentos_alerta_0)

        try:

            # async with pool.acquire() as conn:
            #     async with conn.cursor() as cursor:
            #         await cursor.execute("""
            #         SELECT cod_equipamento, cod_campo, alerta, valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas
            #         """)
            #         leituras = await cursor.fetchall()

            async with pool.acquire() as conn:
                await conn.ping(reconnect=True)  # Forçar reconexão
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                    SELECT cod_equipamento, cod_campo, alerta, valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas
                    """)
                    leituras = await cursor.fetchall()
                    for leitura in leituras:
                        try:
                            cod_equipamento, cod_campo, alerta, valor_1, valor_2, valor_3, valor_4, valor_5 = leitura

                        #    print(f"Leitura: cod_equipamento={cod_equipamento}, cod_campo={cod_campo}, alerta={alerta}, valores={[valor_1, valor_2, valor_3, valor_4, valor_5]}")

                            if alerta == 1:
                                tempo_inicial = datetime.now()
                                data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')

                                if cod_equipamento not in equipamentos_com_alerta:
                                    equipamentos_com_alerta[cod_equipamento] = datetime.now()
                                    print('Alerta ativado para o equipamento', cod_equipamento,data_cadastro_formatada,'\n')
                                    agora = datetime.now()

                                    # Consulta para obter o código da usina associada ao equipamento
                                    await cursor.execute("""
                                        SELECT cod_usina FROM sup_geral.equipamentos
                                        WHERE codigo = %s
                                    """, (cod_equipamento,))
                                    result = await cursor.fetchone()

                                    if result is not None:
                                        cod_usina = result[0]

                                    await cursor.execute("""
                                        SELECT data_cadastro_previsto, data_cadastro_quebra 
                                        FROM machine_learning.relatorio_quebras 
                                        WHERE cod_equipamento = %s
                                        ORDER BY data_cadastro_previsto DESC 
                                        LIMIT 1
                                    """, (cod_equipamento,))
                                    result = await cursor.fetchone()

                                    if result is not None:
                                        tempo_inicial = datetime.now()
                                        data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')
            
                                        ultima_data_prevista, ultima_data_cadastro_quebra = result
                                        if ultima_data_prevista and ultima_data_cadastro_quebra:
                                            await cursor.execute("""
                                                INSERT INTO machine_learning.relatorio_quebras 
                                                (cod_equipamento, cod_usina, data_cadastro_previsto) 
                                                VALUES (%s, %s, NOW())
                                            """, (cod_equipamento, cod_usina))
                                            await conn.commit()
                                            
                                            await cursor.execute("""
                                                INSERT INTO machine_learning.log_relatorio_quebras 
                                                (cod_equipamento, cod_usina, data_cadastro_previsto) 
                                                VALUES (%s, %s, NOW())
                                            """, (cod_equipamento, cod_usina))
                                            await conn.commit()
                                            print(f'Nova entrada 1 no relatório de log e quebras para o equipamento {cod_equipamento}',data_cadastro_formatada,'\n')

                                        elif ultima_data_prevista and not ultima_data_cadastro_quebra and (agora - ultima_data_prevista) < timedelta(hours=6):
                                            await cursor.execute("""
                                                UPDATE machine_learning.relatorio_quebras SET data_cadastro_previsto = %s WHERE cod_equipamento = %s ORDER BY data_cadastro_previsto DESC LIMIT 1
                                            """, (datetime.now(), cod_equipamento))
                                            await conn.commit()
                                        #    print(f'Atualização no relatório de quebras para o equipamento {cod_equipamento}',data_cadastro_formatada,'\n')
                                            
                                            await cursor.execute("""
                                                UPDATE machine_learning.log_relatorio_quebras SET data_cadastro_previsto = %s WHERE cod_equipamento = %s ORDER BY data_cadastro_previsto DESC LIMIT 1
                                            """, (datetime.now(), cod_equipamento))
                                            await conn.commit()
                                            print(f'Atualização 2 no relatório de log e quebras para o equipamento {cod_equipamento}',data_cadastro_formatada,'\n')
                                                                                    
                                        elif (ultima_data_prevista and not ultima_data_cadastro_quebra and (agora - ultima_data_prevista) >= timedelta(hours=6)) or (ultima_data_prevista and not ultima_data_cadastro_quebra and (agora - ultima_data_prevista) < timedelta(hours=1)):
                                            await cursor.execute("""
                                                INSERT INTO machine_learning.relatorio_quebras (cod_equipamento,cod_usina, data_cadastro_quebra, data_cadastro_previsto)
                                                VALUES (%s, %s, NULL, NOW())
                                            """, (cod_equipamento,cod_usina))
                                            await conn.commit()
                                        #    print(f'Nova entrada no relatório de quebras para o equipamento {cod_equipamento}',data_cadastro_formatada,'\n')

                                            await cursor.execute("""
                                                INSERT INTO machine_learning.log_relatorio_quebras (cod_equipamento,cod_usina, data_cadastro_quebra, data_cadastro_previsto)
                                                VALUES (%s, %s, NULL, NOW())
                                            """, (cod_equipamento,cod_usina))
                                            await conn.commit()
                                            print(f'Nova entrada 3 no relatório de log e quebras para o equipamento {cod_equipamento}',data_cadastro_formatada,'\n')
                                                                                    
                                        else:
                                            print(f'Nenhuma entrada adicionada ao relatório de quebras e de log para o equipamento {cod_equipamento}',data_cadastro_formatada,'\n')
                                    else:
                                        # Não há linha existente para o equipamento, então podemos criar uma nova linha
                                        await cursor.execute("""
                                            INSERT INTO machine_learning.relatorio_quebras (cod_equipamento,cod_usina, data_cadastro_quebra, data_cadastro_previsto)
                                            VALUES (%s, %s, NULL, NOW())
                                        """, (cod_equipamento,cod_usina))
                                        await conn.commit()

                                        await cursor.execute("""
                                            INSERT INTO machine_learning.log_relatorio_quebras (cod_equipamento,cod_usina, data_cadastro_quebra, data_cadastro_previsto)
                                            VALUES (%s, %s, NULL, NOW())
                                        """, (cod_equipamento,cod_usina))
                                        await conn.commit()
                                        print(f'Nova entrada 4 no relatório quebras e de log quebras para o equipamento {cod_equipamento}',data_cadastro_formatada,'\n')

                                if cod_equipamento in equipamentos_com_alerta:
                                    # Verifica se já está no dicionário, senão adiciona
                                    if cod_equipamento not in equipamentos_alerta_0:
                                        tempo_inicial = datetime.now()
                                        await cursor.execute("""
                                            SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                        """, (cod_equipamento,))
                                        alarmes_ativos = await cursor.fetchall()

                                        # Adiciona alarmes ativos ao dicionário
                                        equipamentos_alerta_0[cod_equipamento] = {
                                            'alarmes': [alarme[0] for alarme in alarmes_ativos],
                                            'tempo_alerta_0': tempo_inicial
                                        }
                                        print(f"Equipamento {cod_equipamento} com alerta em 1 adicionado ao dicionário com alarmes {equipamentos_alerta_0[cod_equipamento]['alarmes']}")
                                    else:
                                        # Atualiza apenas com novos alarmes, mantendo os antigos
                                        await cursor.execute("""
                                            SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                        """, (cod_equipamento,))
                                        alarmes_ativos = await cursor.fetchall()

                                        # Mantém os alarmes existentes
                                        alarmes_existentes = equipamentos_alerta_0[cod_equipamento]['alarmes']

                                        # Verifica se há novos alarmes, para adicionar apenas os novos
                                        novos_alarmes = [alarme[0] for alarme in alarmes_ativos if alarme[0] not in alarmes_existentes]

                                        if novos_alarmes:
                                            # Adiciona os novos alarmes à lista de alarmes existentes
                                            equipamentos_alerta_0[cod_equipamento]['alarmes'].extend(novos_alarmes)
                                            equipamentos_alerta_0[cod_equipamento]['tempo_alerta_0'] = datetime.now()
                                            print(f"Equipamento {cod_equipamento} com alerta em 1 atualizado com novos alarmes {novos_alarmes}")
                                        else:
                                        #    print(f"Equipamento {cod_equipamento} com alerta em 1 não tem novos alarmes, mantendo a lista atual {alarmes_existentes}")
                                            pass

                            elif alerta == 0:
                                if cod_equipamento in equipamentos_com_alerta:
           
#                                    if cod_campo == 114 and all(value == 0 for value in [valor_1, valor_2, valor_3, valor_4, valor_5]): # se todos os valores forem 0
                                    # Faça algo se pelo menos três dos valores forem iguais a zero
                                    if cod_campo == 114 and sum(value == 0 for value in [valor_1, valor_2, valor_3, valor_4, valor_5]) >= 3:

                                        # Consulta para obter o código da usina associada ao equipamento
                                        await cursor.execute("""
                                            SELECT cod_usina FROM sup_geral.equipamentos
                                            WHERE codigo = %s
                                        """, (cod_equipamento,))
                                        result = await cursor.fetchone()

                                        if result is not None:
                                            cod_usina = result[0]


                                        # Verifica se já está no dicionário, senão adiciona
                                        if cod_equipamento not in equipamentos_alerta_0:
                                            tempo_inicial = datetime.now()
                                            await cursor.execute("""
                                                SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                            """, (cod_equipamento,))
                                            alarmes_ativos = await cursor.fetchall()

                                            # Adiciona alarmes ativos ao dicionário
                                            equipamentos_alerta_0[cod_equipamento] = {
                                                'alarmes': [alarme[0] for alarme in alarmes_ativos],
                                                'tempo_alerta_0': tempo_inicial
                                            }
                                    #        print(f"Equipamento {cod_equipamento} com alerta em 0 com valores zerados adicionado ao dicionário com alarmes {equipamentos_alerta_0[cod_equipamento]['alarmes']}")
                                        else:
                                            # Atualiza apenas com novos alarmes, mantendo os antigos
                                            await cursor.execute("""
                                                SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                            """, (cod_equipamento,))
                                            alarmes_ativos = await cursor.fetchall()

                                            # Mantém os alarmes existentes
                                            alarmes_existentes = equipamentos_alerta_0[cod_equipamento]['alarmes']

                                            # Verifica se há novos alarmes, para adicionar apenas os novos
                                            novos_alarmes = [alarme[0] for alarme in alarmes_ativos if alarme[0] not in alarmes_existentes]

                                            if novos_alarmes:
                                                # Adiciona os novos alarmes à lista de alarmes existentes
                                                equipamentos_alerta_0[cod_equipamento]['alarmes'].extend(novos_alarmes)
                                                equipamentos_alerta_0[cod_equipamento]['tempo_alerta_0'] = datetime.now()
                                    #            print(f"Equipamento {cod_equipamento} com alerta em 0 com valores zerados atualizado com novos alarmes {novos_alarmes}")
                                            else:
                                    #            print(f"Equipamento {cod_equipamento} com alerta em 0 com valores zerados não tem novos alarmes, mantendo a lista atual {alarmes_existentes}")
                                                pass

                                    #    print(cod_equipamento,'valores zerados da funcao monitorar_leituras_consecutivas 1 - ',valor_1, valor_2, valor_3, valor_4, valor_5,'alarmes_ativos',alarmes_ativos)

                                        # Verifica se algum alarme existente está dentro dos codigos_alarmes_desejados
                                        alarmes_existentes = equipamentos_alerta_0[cod_equipamento]['alarmes']
                                        alarmes_desejados_encontrados = [alarme for alarme in alarmes_existentes if alarme in codigos_alarmes_desejados]

                                        print(cod_equipamento,'valores zerados da funcao monitorar_leituras_consecutivas 1 - ',valor_1, valor_2, valor_3, valor_4, valor_5,'alarmes_existentes',alarmes_existentes)

                                        if alarmes_desejados_encontrados:

                                            tempo_decorrido = datetime.now() - equipamentos_com_alerta[cod_equipamento]
                                            await cursor.execute("""
                                                SELECT data_cadastro_previsto, data_cadastro_quebra 
                                                FROM machine_learning.relatorio_quebras 
                                                WHERE cod_equipamento = %s
                                                ORDER BY id DESC 
                                                LIMIT 1
                                            """, (cod_equipamento,))
                                            data_cadastro_previsto, data_cadastro_quebra = await cursor.fetchone()
                                            print(cod_equipamento,'valores zerados da funcao monitorar_leituras_consecutivas 3 - ',valor_1, valor_2, valor_3, valor_4, valor_5,'alarmes_existentes',alarmes_existentes,'tempo_decorrido',tempo_decorrido)

                                            if data_cadastro_quebra is None:
                                                print(cod_equipamento,' - data_cadastro_quebra e none',data_cadastro_quebra,'data_cadastro_previsto',data_cadastro_previsto, 'tempo dentro do proximo loop - ',datetime.now() - data_cadastro_previsto)
                                                tempo_inicial = datetime.now()
                                                data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')

                                                if data_cadastro_previsto and (datetime.now() - data_cadastro_previsto) <= timedelta(hours=24):
                                                    await cursor.execute("""
                                                        UPDATE machine_learning.relatorio_quebras SET data_cadastro_quebra = %s WHERE cod_equipamento = %s ORDER BY data_cadastro_previsto DESC LIMIT 1
                                                    """, (datetime.now(), cod_equipamento))
                                                    await conn.commit()

                                                    await cursor.execute("""
                                                        UPDATE machine_learning.log_relatorio_quebras SET data_cadastro_quebra = %s WHERE cod_equipamento = %s ORDER BY data_cadastro_previsto DESC LIMIT 1
                                                    """, (datetime.now(), cod_equipamento))
                                                    await conn.commit()
                                                    del equipamentos_com_alerta[cod_equipamento]
                                                    del equipamentos_alerta_0[cod_equipamento]
                                                    print(f"Atualização do 5 registro de log e quebra para o equipamento {cod_equipamento}.",data_cadastro_formatada,'\n')

                                                else:
                                                    # await cursor.execute("""
                                                    #     INSERT INTO machine_learning.relatorio_quebras (cod_equipamento,cod_usina, data_cadastro_quebra, data_cadastro_previsto)
                                                    #     VALUES (%s, %s, NULL, NOW())
                                                    # """, (cod_equipamento,cod_usina))
                                                    # await conn.commit()

                                                    # await cursor.execute("""
                                                    #     INSERT INTO machine_learning.log_relatorio_quebras (cod_equipamento,cod_usina, data_cadastro_quebra, data_cadastro_previsto)
                                                    #     VALUES (%s, %s, NULL, NOW())
                                                    # """, (cod_equipamento,cod_usina))
                                                    # await conn.commit()
                                                    print(f"Inserção 6 de novo registro de log e quebra para o equipamento {cod_equipamento}.",data_cadastro_formatada,'\n')
                                                    del equipamentos_com_alerta[cod_equipamento]
                                                    del equipamentos_alerta_0[cod_equipamento]
                                            else:
                                                print(cod_equipamento,' - data_cadastro_quebra nao e none',data_cadastro_quebra,'data_cadastro_previsto',data_cadastro_previsto, 'tempo dentro do proximo loop - ',datetime.now() - data_cadastro_previsto)
                                                del equipamentos_com_alerta[cod_equipamento]
                                                del equipamentos_alerta_0[cod_equipamento]

                                        elif not alarmes_desejados_encontrados:
                                            print(cod_equipamento,'valores zerados da funcao monitorar_leituras_consecutivas 5 - ',valor_1, valor_2, valor_3, valor_4, valor_5,'alarmes_existentes',alarmes_existentes)
                                            await cursor.execute("""
                                                SELECT data_cadastro_previsto, data_cadastro_quebra 
                                                FROM machine_learning.relatorio_quebras 
                                                WHERE cod_equipamento = %s
                                                ORDER BY data_cadastro_previsto DESC 
                                                LIMIT 1
                                            """, (cod_equipamento,))
                                            result = await cursor.fetchall()
                                            if result:
                                                for data_cadastro_previsto, data_cadastro_quebra in result:
                                                    if data_cadastro_quebra is None:
                                                        await cursor.execute("""
                                                            DELETE FROM machine_learning.relatorio_quebras WHERE cod_equipamento = %s
                                                        """, (cod_equipamento,))
                                                        await conn.commit()
                                                        tempo_inicial = datetime.now()
                                                        data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')
                                                        print(f"Remoção 11 da linha da tabela de relatório de quebras para o equipamento sem alarme {cod_equipamento}.",data_cadastro_formatada,'\n')
                                                        del equipamentos_com_alerta[cod_equipamento]
                                                        del equipamentos_alerta_0[cod_equipamento]
                                                    else:
                                                        pass
                                                else:
                                                    print('sem resultado de pesquisa 3')

                        except Exception as e:
                            print(f"Erro ao processar leitura para o equipamento {leitura[0]}: {e}")

        except aiomysql.Error as e:
            print(f"Erro de conexão com o banco de dados: {e}")
            await asyncio.sleep(10)  # Espera 10 segundos antes de tentar reconectar

        tempo_final = datetime.now()
        data_cadastro_formatada_final = tempo_final.strftime('%d-%m-%Y %H:%M')

        # Calcular o tempo de execução
        tempo_execucao = tempo_final - tempo_inicial

        # Formatando a diferença de tempo
        segundos = tempo_execucao.total_seconds()
        horas = int(segundos // 3600)
        minutos = int((segundos % 3600) // 60)
        segundos_restantes = int(segundos % 60)

        # Exibindo o tempo total de execução
    #    print('\n','------------------------------------------------------------- fim monitorar_leituras_consecutivas --------------------------------------------------------------------------', data_cadastro_formatada_final)
    #    print(f'Tempo de execução do monitorar_leituras_consecutivas: {horas} horas, {minutos} minutos, {segundos_restantes} segundos\n')
        
        
        await asyncio.sleep(20)





monitorando = {}
ja_escreveu = {}

async def monitorar_quebras(pool):
    ultima_data_cadastro = {}
    global monitorando
    global ja_escreveu
    global alertas_enviados
    global alertas_enviados_previsao
    
    while True:
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                    SELECT cod_equipamento, cod_campo, alerta, valor_1, valor_2, valor_3, valor_4, valor_5, data_cadastro FROM machine_learning.leituras_consecutivas
                    """)
                    leituras = await cursor.fetchall()
                    for leitura in leituras:
                        cod_equipamento, cod_campo, alerta, valor_1, valor_2, valor_3, valor_4, valor_5, data_cadastro = leitura

                        if cod_equipamento not in ultima_data_cadastro or data_cadastro != ultima_data_cadastro[cod_equipamento]:
                            if cod_equipamento not in ultima_data_cadastro or (datetime.now() - ultima_data_cadastro[cod_equipamento]).total_seconds() < 600:
                                await cursor.execute("""
                                    SELECT cod_usina FROM sup_geral.equipamentos
                                    WHERE codigo = %s
                                """, (cod_equipamento,))
                                result = await cursor.fetchone()

                                if result is not None:
                                    cod_usina = result[0]

                                if cod_campo == 114 and any(value != 0 for value in (valor_1, valor_2, valor_3, valor_4, valor_5)):
                                    monitorando[cod_equipamento] = True    
                                
                                if cod_campo == 114 and all(value == 0 for value in (valor_1, valor_2, valor_3, valor_4, valor_5)):
                                    if cod_equipamento in monitorando and monitorando[cod_equipamento]:
                                        monitorando[cod_equipamento] = False
                                        print(f"O equipamento {cod_equipamento} da usina {cod_usina} zerou os valores:", cod_campo, valor_1, valor_2, valor_3, valor_4, valor_5, datetime.now())

                                        await cursor.execute("""
                                        SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                        """, (cod_equipamento,))
                                        alarmes_ativos = await cursor.fetchall()
                                        print(f"O equipamento {cod_equipamento} da usina {cod_usina} nao tem um alarme ativo: {alarmes_ativos} em monitorar_quebras")

                                        for (cod_alarme,) in alarmes_ativos:
                                            if cod_alarme in codigos_alarmes_desejados:
                                                if cod_equipamento not in ja_escreveu or not ja_escreveu[cod_equipamento]:
                                                    print(f"O equipamento {cod_equipamento} da usina {cod_usina} tem um alarme ativo: {cod_alarme} em monitorar_quebras")

                                                    alerta_80 = alerta if cod_equipamento in alertas_enviados else 0
                                                    alerta_100 = alerta if cod_equipamento in alertas_enviados else 0
                                                    previsao = alerta if cod_equipamento in alertas_enviados_previsao else 0
                                        
                                                    falha = 1 if previsao == 1 or alerta_80 == 1 or alerta_100 == 1 else 0

                                                    print('monitorar_quebras','alerta_80', alerta_80,'alerta_100', alerta_100,'previsao',previsao, 'falha', falha)

                                                    await cursor.execute("""
                                                    INSERT INTO machine_learning.falhas_gerais (cod_equipamento, cod_usina, data_cadastro, falha, alerta_80, alerta_100, previsao)
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                                    """, (cod_equipamento, cod_usina, datetime.now(), falha, alerta_80, alerta_100, previsao))
                                                    await conn.commit()
                                                    ja_escreveu[cod_equipamento] = True
                                    else:
                                        if cod_equipamento in ja_escreveu:
                                            del ja_escreveu[cod_equipamento]

                        #    print('cod_usina',cod_usina,'cod_equipamento',cod_equipamento,' ---- ','alertas_enviados_previsao',alertas_enviados_previsao,'alertas_enviados',alertas_enviados)
                            ultima_data_cadastro[cod_equipamento] = data_cadastro
        
            await asyncio.sleep(60)

        except Exception as e:
            print(f"Erro durante a execução de monitorar_quebras: {e}")
            await asyncio.sleep(40)
            
            

@dp.callback_query_handler(lambda query: query.data.startswith('receber_alarmes_'))
async def callback_receber_alarmes(query: CallbackQuery):
    print("Botão 'Receber Alarmes' clicado!")
    await query.answer("Você receberá os alarmes ate os equipamentos sairem do alerta.")

    cod_usina = int(query.data.split('_')[2])

    await enviar_alarme_botao(cod_usina, query.from_user.id)


@dp.callback_query_handler(lambda query: True, state=Form.usuario)
async def process_usuario_callback(query: CallbackQuery, state: FSMContext):
    nome_telegram = query.data

    print(f"Clicou no usuário com nome_telegram: {nome_telegram}")

    await query.answer(f"Você escolheu o usuário: {nome_telegram}")

    await state.update_data(selected_user_nome_telegram=nome_telegram)

    await Form.ask_supervisor_name.set()
    await bot.send_message(query.message.chat.id, f"Insira o nome de usuário do supervisorio para {nome_telegram}?")


'''
@dp.callback_query_handler(lambda query: True, state=Form.edit_usuario)
async def process_usuario_edit_callback(query: CallbackQuery, state: FSMContext):
    nome_telegram = query.data
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização

    print(f"Clicou no usuário com nome_telegram: {nome_telegram}")

    await query.answer(f"Você escolheu o usuário: {nome_telegram}")

    await state.update_data(selected_user_nome_telegram=nome_telegram)

    await Form.ask_supervisor_edit_name.set()

    # Obtém o status atual do usuário
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT ativo FROM machine_learning.usuarios_telegram WHERE nome_telegram = %s", (nome_telegram,))
            status_result = await cursor.fetchone()

            if status_result:
                current_status = "ativo" if status_result[0] == 1 else "desativado"

                # Envie uma mensagem informando o status atual do usuário
                await bot.send_message(query.message.chat.id, f"O usuário {nome_telegram} está atualmente {current_status}.")

                # Determina a ação inversa com base no status atual
                inverse_action = "desativar" if current_status == "ativo" else "ativar"

                # Pergunta ao usuário se deseja realizar a ação inversa
                await bot.send_message(query.message.chat.id, f"Deseja {inverse_action} o usuário {nome_telegram}? \nResponda '/{inverse_action}' para confirmar.")

            else:
                await bot.send_message(query.message.chat.id, "Não foi possível obter o status do usuário.")

    # Remover as opções de usuários após o usuário selecionar um
    await bot.delete_message(query.message.chat.id, query.message.message_id)

'''

@dp.callback_query_handler(lambda query: True, state=Form.edit_usuario)
async def process_usuario_edit_callback(query: CallbackQuery, state: FSMContext):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização

    # Verifica se o callback é para ativar/desativar todos
    if query.data.startswith("toggle_all_"):
        novo_status = int(query.data.split("_")[-1])  # Extrai o novo status (0 ou 1) da callback_data

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Atualiza todos os usuários exceto "Bruno Zanella"
                await cursor.execute("""
                    UPDATE machine_learning.usuarios_telegram 
                    SET ativo = %s 
                    WHERE nome_telegram NOT IN ('Bruno Zanella', 'BOT - Mensagens Gerais BRG')
                """, (novo_status,))
                await conn.commit()

                # Envia uma mensagem de confirmação
                status_text = "ativados" if novo_status == 1 else "desativados"
                await bot.send_message(query.message.chat.id, f"Todos os usuários foram {status_text}, exceto Bruno Zanella.")

        # Remove as opções de usuários após o botão de ativar/desativar todos ser pressionado
        await bot.delete_message(query.message.chat.id, query.message.message_id)
        return

    # Processa o clique em um usuário específico
    nome_telegram = query.data
    print(f"Clicou no usuário com nome_telegram: {nome_telegram}")

    await query.answer(f"Você escolheu o usuário: {nome_telegram}")

    await state.update_data(selected_user_nome_telegram=nome_telegram)

    await Form.ask_supervisor_edit_name.set()

    # Obtém o status atual do usuário
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT ativo FROM machine_learning.usuarios_telegram WHERE nome_telegram = %s", (nome_telegram,))
            status_result = await cursor.fetchone()

            if status_result:
                current_status = "ativo" if status_result[0] == 1 else "desativado"

                # Envie uma mensagem informando o status atual do usuário
                await bot.send_message(query.message.chat.id, f"O usuário {nome_telegram} está atualmente {current_status}.")

                # Determina a ação inversa com base no status atual
                inverse_action = "desativar" if current_status == "ativo" else "ativar"

                # Pergunta ao usuário se deseja realizar a ação inversa
                await bot.send_message(query.message.chat.id, f"Deseja {inverse_action} o usuário {nome_telegram}? \nResponda '/{inverse_action}' para confirmar.")

            else:
                await bot.send_message(query.message.chat.id, "Não foi possível obter o status do usuário.")

    # Remover as opções de usuários após o usuário selecionar um
    await bot.delete_message(query.message.chat.id, query.message.message_id)



@dp.message_handler(state=Form.ask_supervisor_edit_name)
async def process_edit_supervisor_name(message: types.Message, state: FSMContext):
    try:
        pool = dp.pool  # Reutilizando o pool criado durante a inicialização

        data = await state.get_data()
        user_nome_telegram = data.get('selected_user_nome_telegram')

        # Obtém a resposta do usuário
        response = message.text.strip().lower()

        # Verifica se a resposta é sim ou não
        if response == '/ativar':
            ativo = 1
        elif response == '/desativar':
            ativo = 0
        else:
            await bot.send_message(message.chat.id, "Por favor, responda '/ativar' ou '/desativar'.")
            return

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Atualiza o valor da coluna 'ativo' no banco de dados
                await cursor.execute("UPDATE machine_learning.usuarios_telegram SET ativo = %s WHERE nome_telegram = %s", (ativo, user_nome_telegram))
                await conn.commit()

                # Obtém a mensagem de confirmação
                confirmation_message = f"A coluna 'ativo' para {user_nome_telegram} foi atualizada para {ativo}."

                # Envia a mensagem de confirmação
                await bot.send_message(message.chat.id, confirmation_message)

                # Encerra o estado do formulário
                await state.finish()

    except Exception as e:
        await bot.send_message(message.chat.id, f"Ocorreu um erro ao processar a solicitação: {str(e)}")


           
@dp.message_handler(state=Form.ask_supervisor_name)
async def process_supervisor_name(message: types.Message, state: FSMContext):
    try:
        data = await state.get_data()
        user_nome_telegram = data.get('selected_user_nome_telegram')
        pool = dp.pool  # Reutilizando o pool criado durante a inicialização

        supervisor_name = message.text

        user_input = supervisor_name.lstrip('/')

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Atualiza o usuário do supervisorio e define ativo como 1
                await cursor.execute("UPDATE machine_learning.usuarios_telegram SET usuario = %s, ativo = 1 WHERE nome_telegram = %s", (user_input, user_nome_telegram))
                await conn.commit()

                # Obtém o ID do telegram para o usuário atualizado
                await cursor.execute("SELECT id_telegram FROM machine_learning.usuarios_telegram WHERE nome_telegram = %s", (user_nome_telegram,))
                id_telegram_result = await cursor.fetchone()

                # Obtém o código e o nome do usuário do supervisorio
                await cursor.execute("SELECT codigo, nome FROM sup_geral.usuarios WHERE login = %s", (user_input,))
                codigo_result = await cursor.fetchone()

                if codigo_result:
                    codigo, nome = codigo_result

                    # Atualiza o código do usuário, o nome do supervisorio e define ativo como 1
                    await cursor.execute("UPDATE machine_learning.usuarios_telegram SET cod_usuario = %s, nome_supervisorio = %s, ativo = 1 WHERE nome_telegram = %s", (codigo, nome, user_nome_telegram))
                    await conn.commit()

                # Mensagem de resposta para o grupo
                response_message_grupo = f"A informação foi atualizada.\n O usuário do supervisorio para {user_nome_telegram} é: {user_input}"
                await bot.send_message(message.chat.id, response_message_grupo)

                if id_telegram_result:
                    id_telegram = id_telegram_result[0]

                    # Mensagem de resposta para o usuário do telegram
                    response_message = f"A informação foi atualizada.\n O usuário do supervisorio para {user_nome_telegram} é: {user_input}"
                    await bot.send_message(id_telegram, response_message)

                    # Realiza as ações de boas-vindas e envio de previsão de valor de equipamento
                    await boas_vindas(message, id_telegram)

                    await state.finish()
                else:
                    await bot.send_message(message.chat.id, "Erro ao obter o ID do usuário atualizado.")

    except Exception as e:
        await bot.send_message(message.chat.id, f"Ocorreu um erro ao processar a solicitação: {str(e)}")





async def atualizar_usinas_usuario(pool):
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Buscar todos os cod_usuario exceto 0
                await cursor.execute(
                    "SELECT cod_usuario FROM machine_learning.usuarios_telegram WHERE cod_usuario <> 0"
                )
                cod_usuarios = await cursor.fetchall()
                cod_usuarios = [row[0] for row in cod_usuarios]

                if not cod_usuarios:
                    return  # Nenhum cod_usuario para processar

                # Gerar a consulta SQL dinâmica usando o operador IN
                format_strings = ','.join(['%s'] * len(cod_usuarios))
                query = f"""
                    SELECT cod_usuario, cod_usina 
                    FROM sup_geral.usuarios_ext_usinas 
                    WHERE cod_usuario IN ({format_strings})
                """
                await cursor.execute(query, tuple(cod_usuarios))
                usinas = await cursor.fetchall()

                # Preparar uma lista de comandos de inserção
                insercoes = []
                for usina in usinas:
                    cod_usuario = usina[0]
                    cod_usina = usina[1]

                    # Verificar se a combinação já existe
                    await cursor.execute(
                        "SELECT 1 FROM machine_learning.usinas_usuario WHERE cod_usuario = %s AND cod_usina = %s",
                        (cod_usuario, cod_usina)
                    )
                    existente = await cursor.fetchone()

                    if not existente:
                        insercoes.append((cod_usuario, cod_usina))

                if insercoes:
                    print(f'Inserindo a usina {cod_usina} que não pertencia ao usuario {cod_usuario}')
                    # Inserir dados na tabela machine_learning.usinas_usuario
                    await cursor.executemany(
                        "INSERT INTO machine_learning.usinas_usuario (cod_usuario, cod_usina) VALUES (%s, %s)",
                        insercoes
                    )
                    # Confirmar a transação
                    await conn.commit()

    except Exception as e:
        print(f"Erro ao processar atualização de usinas: {str(e)}")
        await asyncio.sleep(3600)  # Aguarde 1 hora antes de tentar novamente em caso de erro

    # Aguardar 1 semana (604800 segundos) antes de executar novamente aguarda 3 dias 259200
    await asyncio.sleep(259200)

                
                
                
           
                
# Variável global para armazenar as seleções temporárias do usuário
user_selections = {}


@dp.callback_query_handler(lambda c: c.data == '5')
async def editar_usinas_cadastradas(callback_query: types.CallbackQuery, state: FSMContext):
    chat_id = callback_query.message.chat.id

    try:
        pool = dp.pool  # Use o pool já existente no dispatcher
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT cod_usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (chat_id,))
                result = await cursor.fetchone()

                if result:
                    cod_usuario = result[0]
                    await state.update_data(user_selections={})

                    await cursor.execute("SELECT cod_usina, ativo FROM machine_learning.usinas_usuario WHERE cod_usuario = %s", (cod_usuario,))
                    usinas = await cursor.fetchall()

                    if usinas:
                        buttons = []
                        for cod_usina, ativo in usinas:
                            await cursor.execute("SELECT nome FROM sup_geral.usinas WHERE codigo = %s", (cod_usina,))
                            nome = await cursor.fetchone()
                            if nome:
                                checkbox = "✅" if ativo else "⬜"
                                async with state.proxy() as data:
                                    data['user_selections'][cod_usina] = ativo
                                buttons.append(InlineKeyboardButton(f"{checkbox} {nome[0]}", callback_data=f"toggle_{cod_usina}"))

                        keyboard = InlineKeyboardMarkup(row_width=1)
                        keyboard.add(*buttons)
                        
                        user_data = await state.get_data()
                        if 'edit_usinas_message_id' in user_data:
                            try:
                                await bot.delete_message(chat_id, user_data['edit_usinas_message_id'])
                            except Exception as e:
                                print(f"Erro ao apagar a mensagem anterior: {e}")
                                
                        await bot.delete_message(chat_id=chat_id, message_id=callback_query.message.message_id)
                        sent_message = await bot.send_message(chat_id, "Escolha uma usina para alternar:", reply_markup=keyboard)
                        await state.update_data(edit_usinas_message_id=sent_message.message_id)
                    else:
                        await bot.send_message(chat_id, "Nenhuma usina encontrada para este usuário.")
                else:
                    await bot.send_message(chat_id, "Usuário não encontrado.")

    except Exception as e:
        print(f"Ocorreu um erro ao processar a solicitação 1: {e}")
    #    await bot.send_message(chat_id, "Ocorreu um erro ao processar a sua solicitação. Por favor, tente novamente.")
    finally:
        await state.finish()


# Dicionário global para armazenar as tarefas temporizadoras ativas
active_timers = {}

@dp.callback_query_handler(lambda c: c.data.startswith('toggle_'))
async def toggle_usina_state(callback_query: types.CallbackQuery, state: FSMContext):
    chat_id = callback_query.message.chat.id
    message_id = callback_query.message.message_id
    data = callback_query.data.split('_')
    cod_usina = data[1]

    try:
        pool = dp.pool  # Use o pool já existente no dispatcher
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT cod_usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (chat_id,))
                result = await cursor.fetchone()

                if result:
                    cod_usuario = result[0]
                    user_data = await state.get_data()
                    if 'selections' not in user_data:
                        user_data['selections'] = {}
                    if cod_usuario not in user_data['selections']:
                        user_data['selections'][cod_usuario] = {}

                    keyboard = callback_query.message.reply_markup.inline_keyboard

                    for row in keyboard:
                        for button in row:
                            if button.callback_data == f"toggle_{cod_usina}":
                                if "⬜" in button.text:
                                    button.text = button.text.replace("⬜", "✅")
                                    user_data['selections'][cod_usuario][cod_usina] = 1
                                else:
                                    button.text = button.text.replace("✅", "⬜")
                                    user_data['selections'][cod_usuario][cod_usina] = 0

                    await state.update_data(selections=user_data['selections'])
                    try:
                        await bot.edit_message_reply_markup(chat_id, message_id, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
                    except MessageNotModified:
                        pass

                    # Reinicia o temporizador de 180 segundos após cada interação
                    await reset_timer(chat_id, message_id, state)

    except Exception as e:
        print(f"Ocorreu um erro ao processar a solicitação: {e}")
        await bot.send_message(chat_id, "Ocorreu um erro ao processar a sua solicitação. Por favor, tente novamente.")

async def reset_timer(chat_id: int, message_id: int, state: FSMContext):
    try:
        # Cancela qualquer tarefa anterior associada ao temporizador
        if chat_id in active_timers:
            active_timers[chat_id].cancel()

        # Inicia um novo temporizador de 180 segundos
        task = asyncio.create_task(finalizar_selecao_automatica(chat_id, message_id, state))
        active_timers[chat_id] = task

    except Exception as e:
        print(f"Ocorreu um erro ao reiniciar o temporizador: {e}")


async def finalizar_selecao_automatica(chat_id: int, message_id: int, state: FSMContext):
    await asyncio.sleep(60)  # Espera por 180 segundos

    try:
        pool = dp.pool  # Usa o pool já existente no dispatcher
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                user_data = await state.get_data()
                await cursor.execute("SELECT cod_usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (chat_id,))
                result = await cursor.fetchone()

                if result:
                    cod_usuario = result[0]

                    if cod_usuario in user_data.get('selections', {}):
                        for usina, ativo in user_data['selections'][cod_usuario].items():
                            await cursor.execute("""
                                UPDATE machine_learning.usinas_usuario
                                SET ativo = %s
                                WHERE cod_usuario = %s AND cod_usina = %s
                            """, (ativo, cod_usuario, usina))
                        await conn.commit()

                    # Tenta apagar a mensagem com os botões após o tempo de inatividade
                    try:
                        # Adicione um log para verificar o ID da mensagem e do chat antes de deletar
                        print(f"Tentando apagar a mensagem {message_id} no chat {chat_id}")

                        # Verifica o conteúdo da mensagem antes de apagar (pode ser útil para debugging)
                        await bot.delete_message(chat_id, message_id)
                        print(f"Mensagem {message_id} apagada com sucesso no chat {chat_id}")

                    except MessageToDeleteNotFound:
                        print(f"Mensagem {message_id} não encontrada no chat {chat_id}, pode já ter sido apagada.")
                    except Exception as e:
                        print(f"Erro ao tentar apagar a mensagem {message_id}: {e}")

                    await state.finish()

                    # Remove o temporizador do dicionário global
                    if chat_id in active_timers:
                        del active_timers[chat_id]
                else:
                    raise ValueError("Usuário não encontrado")

    except Exception as e:
        print(f"Ocorreu um erro ao finalizar a seleção automaticamente: {e}")
        await bot.send_message(chat_id, "Ocorreu um erro ao salvar as usinas selecionadas automaticamente. Por favor, tente novamente.")




@dp.callback_query_handler(lambda c: c.data == '6')
async def toggle_notificacoes(callback_query: types.CallbackQuery, state: FSMContext):
    chat_id = callback_query.message.chat.id
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização

    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Filtrar o usuário com base no id_telegram
                await cursor.execute("SELECT todos_modelo_funcionamento FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (chat_id,))
                resultado = await cursor.fetchone()

                if resultado is not None:
                    todos_modelo_funcionamento = resultado[0]
                    
                    # Inverter o valor de todos_modelo_funcionamento
                    novo_valor = 0 if todos_modelo_funcionamento == 1 else 1

                    # Atualizar o valor no banco de dados
                    await cursor.execute("""
                        UPDATE machine_learning.usuarios_telegram
                        SET todos_modelo_funcionamento = %s
                        WHERE id_telegram = %s
                    """, (novo_valor, chat_id))
                    await conn.commit()

                    # Construir a mensagem de resposta com base no novo valor
                    if novo_valor == 1:
                        status = "ativadas"
                        detalhe = "O usuário escolheu receber as mensagens de alerta 80% e 100% do load speed."
                    else:
                        status = "desativadas"
                        detalhe = "O usuário escolheu limitar as mensagens de alerta 80% e 100% do load speed."

                    mensagem_final = f"Notificações {status} com sucesso.\n{detalhe}"

                    # Apagar a mensagem dos botões
                    await bot.delete_message(chat_id=chat_id, message_id=callback_query.message.message_id)

                    # Enviar a mensagem final
                    await bot.send_message(chat_id, mensagem_final)

                else:
                    await bot.answer_callback_query(callback_query.id, "Usuário não encontrado.")
    except Exception as e:
        print(f"Ocorreu um erro ao processar a notificação: {e}")
        await bot.send_message(chat_id, "Ocorreu um erro ao processar a sua solicitação. Por favor, tente novamente.")






@dp.callback_query_handler(lambda query: query.data.startswith('fazer_previsao_usina_'))
async def callback_fazer_previsao_usina_menu(query: CallbackQuery):
    try:
        cod_usina = int(query.data.split('_')[3])

        await enviar_previsao_valor_usina_menu(cod_usina, chat_id=query.message.chat.id)
    except ValueError:
        print("Erro ao extrair código da usina.")

@dp.callback_query_handler(lambda query: query.data.startswith('fazer_previsao_equipamento_'))
async def callback_fazer_previsao_equipamento_menu(query: CallbackQuery):
    try:
        cod_equipamento = int(query.data.split('_')[3])
        print(cod_equipamento)
        response_message = await enviar_previsao_valor_equipamento_menu(query.message.chat.id, cod_equipamento)
        await bot.send_message(query.message.chat.id, response_message)
    except ValueError:
        await bot.send_message(query.message.chat.id, "Erro ao extrair código do equipamento.")

@dp.callback_query_handler(lambda query: True, state='*')
async def button(callback_query: types.CallbackQuery, state: FSMContext):
    query = callback_query
    await query.answer()

    await bot.edit_message_reply_markup(chat_id=query.message.chat.id, message_id=query.message.message_id, reply_markup=None)

    if query.data == '1':
    #    await bot.send_message(chat_id=query.message.chat.id, text="Digite o codigo da usina. Insira o número com '/' antes.")
        await bot.send_message(chat_id=query.message.chat.id, text="Digite o codigo da usina.")
        await Form.cod_usina.set()
    elif query.data == '2':
    #    await bot.send_message(chat_id=query.message.chat.id, text="Digite o codigo do equipamento. Insira o número com '/' antes.")
        await bot.send_message(chat_id=query.message.chat.id, text="Digite o codigo do equipamento.")
        await Form.cod_equipamento.set()
    elif query.data == '3':
        await process_usuario(query.message, state)
        await bot.send_message(chat_id=query.message.chat.id, text="Digite o usuário.")
        await Form.usuario.set()
    elif query.data == '4':
        await process_edit_usuario(query.message, state)
    #    await bot.send_message(chat_id=query.message.chat.id, text="Deseja /ativar ou /desativar o usuario?")
        await Form.edit_usuario.set()

    await bot.delete_message(chat_id=query.message.chat.id, message_id=query.message.message_id)



@dp.message_handler(state=Form.cod_usina)
async def process_cod_usina(message: types.Message, state: FSMContext):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
#    chat_id = await id_chat_grupo(pool)
    chat_id = message.chat.id
    user_input = message.text

    if user_input.isdigit():
        cod_usina = int(user_input)

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT nome, cod_cliente, cod_usuario FROM sup_geral.usinas WHERE codigo = %s", (cod_usina,))
                usina_result = await cursor.fetchone()

                if usina_result:
                    nome_usina, cod_cliente, cod_usuario = usina_result

                    await cursor.execute("SELECT codigo, nome FROM sup_geral.equipamentos WHERE cod_usina = %s", (cod_usina,))
                    equipamentos_result = await cursor.fetchall()

                    equipamentos_message = ""
                    for equipamento in equipamentos_result:
                        codigo, nome_equipamento = equipamento
                        equipamentos_message += f"Código: {codigo}, Nome: {nome_equipamento}\n"

                    keyboard = InlineKeyboardMarkup()
                    fazer_previsao_button = InlineKeyboardButton("Fazer Previsão", callback_data=f"fazer_previsao_usina_{cod_usina}")
                    keyboard.add(fazer_previsao_button)

                    response_message = (
                        f"Nome da Usina: {nome_usina}\n"
                        f'<a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n'
                        f"Código do Cliente: {cod_cliente}\n"
                        f"Código do Usuário: {cod_usuario}\n\n"
                        f"Equipamentos:\n"
                        f"{equipamentos_message}"
                    )

                    await bot.send_message(chat_id, response_message, reply_markup=keyboard, parse_mode='HTML')

                else:
                    await bot.send_message(chat_id, "Usina não encontrada.")
    else:
        await bot.send_message(chat_id, "Por favor, insira um número.")

    await state.finish()


@dp.message_handler(state=Form.cod_equipamento)
async def process_cod_equipamento(message: types.Message, state: FSMContext):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
#    chat_id = await id_chat_grupo(pool)
    chat_id = message.chat.id
    user_input = message.text

    if user_input.isdigit():
        cod_equipamento = int(user_input)

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT codigo, nome, cod_usina FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                equipamento_result = await cursor.fetchone()

                if equipamento_result:
                    codigo, nome_equipamento, cod_usina = equipamento_result

                    await cursor.execute("SELECT nome FROM sup_geral.usinas WHERE codigo = %s", (cod_usina,))
                    usina_result = await cursor.fetchone()

                    if usina_result:
                        nome_usina = usina_result[0]

                        response_message = (
                            f"Código do Equipamento: {codigo}\n"
                            f"Nome do Equipamento: {nome_equipamento}\n"
                            f"Código da Usina: {cod_usina}\n"
                            f"Nome da Usina: {nome_usina}\n"
                            f'<a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n'
                        )
                        
                        fazer_previsao_button_equipamento = InlineKeyboardButton("Fazer Previsão", callback_data=f'fazer_previsao_equipamento_{cod_equipamento}')
                        keyboard = InlineKeyboardMarkup().add(fazer_previsao_button_equipamento)

                        await bot.send_message(chat_id, response_message, reply_markup=keyboard, parse_mode='HTML')

                    else:
                        await bot.send_message(chat_id, "Usina não encontrada.")
                else:
                    await bot.send_message(chat_id, "Equipamento não encontrado.")
    else:
        await bot.send_message(chat_id, "Por favor, insira um número.")

    await state.finish()


@dp.message_handler(state=Form.usuario)
async def process_usuario(message: types.Message, state: FSMContext):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    chat_id = await id_chat_grupo(pool)

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT id, nome_supervisorio, nome_telegram FROM machine_learning.usuarios_telegram WHERE usuario = '0'")
            usuarios_result = await cursor.fetchall()

            if usuarios_result:
                keyboard = InlineKeyboardMarkup(row_width=1)

                for usuario in usuarios_result:
                    id, nome_supervisorio, nome_telegram = usuario
                    button = InlineKeyboardButton(nome_supervisorio, callback_data=str(nome_telegram))
                    keyboard.add(button)

                await bot.send_message(chat_id, "Escolha um usuário:", reply_markup=keyboard)

            else:
                await bot.send_message(chat_id, "Nenhum usuário encontrado.")

    await state.finish()

'''
@dp.message_handler(state=Form.edit_usuario)
async def process_edit_usuario(message: types.Message, state: FSMContext):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    chat_id = await id_chat_grupo(pool)

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT id, nome_supervisorio, nome_telegram, ativo FROM machine_learning.usuarios_telegram ")
            usuarios_result = await cursor.fetchall()

            if usuarios_result:
                keyboard = InlineKeyboardMarkup(row_width=1)

                for usuario in usuarios_result:
                    id, nome_supervisorio, nome_telegram, ativo = usuario
                    status = "Ativo" if ativo else "Inativo"  # Traduz o valor de ativo para "Ativo" ou "Inativo"
                    button_text = f"{nome_supervisorio} - {status}"  # Adiciona o status ao texto do botão
                    button = InlineKeyboardButton(button_text, callback_data=str(nome_telegram))
                    keyboard.add(button)

                message = await bot.send_message(chat_id, "Escolha um usuário:", reply_markup=keyboard)

                # Atualiza o estado do formulário para armazenar o ID da mensagem com as opções de usuários
                await state.update_data(user_options_message_id=message.message_id)

            else:
                await bot.send_message(chat_id, "Nenhum usuário encontrado.")

    await state.finish()
'''

@dp.message_handler(state=Form.edit_usuario)
async def process_edit_usuario(message: types.Message, state: FSMContext):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    chat_id = await id_chat_grupo(pool)

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # Consulta para buscar todos os usuários e seus status
            await cursor.execute("SELECT id, nome_supervisorio, nome_telegram, ativo FROM machine_learning.usuarios_telegram")
            usuarios_result = await cursor.fetchall()

            if usuarios_result:
                # Verifica a quantidade de usuários ativos e inativos
                total_ativos = sum(1 for usuario in usuarios_result if usuario[3] == 1)
                total_inativos = len(usuarios_result) - total_ativos
                
                # Determina a ação com base na maioria
                if total_ativos > total_inativos:
                    acao_majoritaria = "Desativar"
                    novo_status = 0
                else:
                    acao_majoritaria = "Ativar"
                    novo_status = 1
                
                # Cria o teclado com os usuários e o botão de ativar/desativar todos
                keyboard = InlineKeyboardMarkup(row_width=1)

                # Botão para ativar/desativar todos os usuários
                button_todos = InlineKeyboardButton(f"{acao_majoritaria} todos os usuários", callback_data=f"toggle_all_{novo_status}")
                keyboard.add(button_todos)

                # Adiciona um botão para cada usuário
                for usuario in usuarios_result:
                    id, nome_supervisorio, nome_telegram, ativo = usuario
                    status = "Ativo" if ativo else "Inativo"  # Traduz o valor de ativo para "Ativo" ou "Inativo"
                    button_text = f"{nome_supervisorio} - {status}"  # Adiciona o status ao texto do botão
                    button = InlineKeyboardButton(button_text, callback_data=str(nome_telegram))
                    keyboard.add(button)

                message = await bot.send_message(chat_id, "Escolha um usuário:", reply_markup=keyboard)

                # Atualiza o estado do formulário para armazenar o ID da mensagem com as opções de usuários
                await state.update_data(user_options_message_id=message.message_id)

            else:
                await bot.send_message(chat_id, "Nenhum usuário encontrado.")

    await state.finish()



async def converter_tempo_para_int(tempo):
    return int(tempo.rstrip('h'))

async def callback_silenciar_equipamento(query):
    try:
        cod_equipamento = int(query.data.split('_')[2])
        print(f"Botão 'Silenciar Equipamento' clicado para o equipamento {cod_equipamento}!")
        await query.answer("Equipamento silenciado com sucesso.")
        
        # Adicione aqui o código para silenciar o equipamento, se necessário
    except ValueError:
        print("Erro ao extrair código do equipamento.")

async def callback_silenciar_usina(query):
    try:
        cod_usina = int(query.data.split('_')[2])
        print(f"Botão 'Silenciar Usina' clicado para a usina {cod_usina}!")
        
        # Cria um teclado inline com opções de tempo
        keyboard = InlineKeyboardMarkup()
        botao_silenciar_equipamento_1 = InlineKeyboardButton("1 hora", callback_data=f"timer_silenciar_usina_{cod_usina}_1h")
        botao_silenciar_equipamento_6 = InlineKeyboardButton("6 horas", callback_data=f"timer_silenciar_usina_{cod_usina}_6h")
        botao_silenciar_equipamento_12 = InlineKeyboardButton("12 horas", callback_data=f"timer_silenciar_usina_{cod_usina}_12h")
        botao_silenciar_equipamento_24 = InlineKeyboardButton("24 horas", callback_data=f"timer_silenciar_usina_{cod_usina}_24h")
        botao_cancelar = InlineKeyboardButton("Cancelar", callback_data=f"cancelar")

        keyboard = InlineKeyboardMarkup().row(botao_silenciar_equipamento_1, botao_silenciar_equipamento_6, botao_silenciar_equipamento_12,botao_silenciar_equipamento_24)
        keyboard.row(botao_cancelar)

        # Envia o teclado para o usuário
        await bot.send_message(query.from_user.id, f'\nPor quanto tempo quer que a Usina {cod_usina} fique silenciada?', reply_markup=keyboard)

    except ValueError:
        print("Erro ao extrair código da Usina.")


@dp.callback_query_handler(lambda query: query.data == 'cancelar')
async def callback_cancelar(query: CallbackQuery):
#    await bot.edit_message_reply_markup(chat_id=query.message.chat.id, message_id=query.message.message_id, reply_markup=None)
    await bot.delete_message(chat_id=query.message.chat.id, message_id=query.message.message_id)

async def callback_silenciar_usina_tempo(query):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização

    try:
        cod_usina, tempo = query.data.split('_')[3], query.data.split('_')[4]
        tempo = converter_tempo_para_int(tempo)
        print(f"Usina {cod_usina} silenciada por {tempo} horas.")
        
        # Busca o cod_usuario na tabela machine_learning.usuarios_telegram
        cod_usuario = await buscar_cod_usuario(query.from_user.id)
        if cod_usuario is None:
            print("Erro: usuário não encontrado na tabela machine_learning.usuarios_telegram.")
            return

        # Busca os códigos dos equipamentos ativos associados à usina
        cod_equipamentos = await buscar_cod_equipamentos(cod_usina)
        if not cod_equipamentos:
            print(f"Erro: nenhum equipamento ativo encontrado para a usina {cod_usina}.")
            return

        # Salva o silenciamento para cada equipamento
        for cod_equipamento in cod_equipamentos:
            await salvar_silenciamento(cod_usuario, cod_equipamento, tempo)
        
        await query.answer(f"Usina {cod_usina} silenciada por {tempo} horas.")
    
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                SELECT usuario FROM machine_learning.usuarios_telegram WHERE cod_usuario = %s
                """, (cod_usuario,))
                nome_usuario = await cursor.fetchone()
                nome_usuario = nome_usuario[0] if nome_usuario else "Usuário Desconhecido"

                id_grupo = await id_chat_grupo(pool)    
                mensagem_grupo = f"ℹ️ O usuário {nome_usuario} silenciou a usina {cod_usina} por {tempo} horas."
                await bot.send_message(id_grupo, mensagem_grupo)

        # Remove a mensagem e os botões após o usuário escolher o tempo
        await bot.delete_message(chat_id=query.message.chat.id, message_id=query.message.message_id)

    except ValueError:
        print("Erro ao extrair código da Usina ou tempo.")


@dp.message_handler(commands=['start'])
async def enviar_boas_vindas(message: types.Message):
    chat_id = message.chat.id
    chat_type = message.chat.type
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização

    primeiro_acesso = None
    usuario = None
    
    id_grupo = await id_chat_grupo(pool)

    nome_usuario = message.from_user.first_name

    if id_grupo:
        await bot.send_message(id_grupo, f'O usuário {nome_usuario} acabou de logar no sistema')
    else:
        print("Erro: id_grupo é inválido")

    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT primeiro_acesso, usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (chat_id,))
            result = await cursor.fetchone()

            if result:
                primeiro_acesso, usuario = result

                if primeiro_acesso == 1:
                    if chat_type in ['group', 'supergroup']:
                        await cursor.execute("SELECT * FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (chat_id,))
                        result = await cursor.fetchone()
                        if result is None:
                            await cursor.execute("INSERT INTO machine_learning.usuarios_telegram (usuario, nome_supervisorio, nome_telegram, id_telegram, primeiro_acesso, cod_usuario, ativo) VALUES (%s, %s, %s, %s, %s, %s, 1)",
                                                ('Grupo', 'Grupo Telegram', message.chat.title, chat_id, '1', '0'))
                            teclado_menu = await criar_menu()
                            await message.reply("Grupo salvo com sucesso!\nUse o botão /menu abaixo para acessar o Menu.", reply_markup=teclado_menu)
                        else:
                            await cursor.execute("UPDATE machine_learning.usuarios_telegram SET usuario = %s, nome_supervisorio = %s, nome_telegram = %s, ativo = 1 WHERE id_telegram = %s",
                                                ('Grupo', 'Grupo Telegram', message.chat.title, chat_id))
                            teclado_menu = await criar_menu()
                            await message.reply("Grupo atualizado com sucesso!\nUse o botão /menu abaixo para acessar o Menu.", reply_markup=teclado_menu)
                        await conn.commit()
                    else:
                        await bot.send_message(chat_id, 'Olá! \nQual é o seu usuário no supervisório da BRG?\n                 <a href="https://supervisorio.brggeradores.com.br/beta/index.php">Acessar supervisório</a>', parse_mode='HTML')

                else:
                    await message.reply(f"Bem-vindo de volta, {usuario}!\nSeus equipamentos estão sendo atualizados constantemente. Qualquer intervenção, você será notificado aqui.", parse_mode='HTML')
                    await cursor.execute("UPDATE machine_learning.usuarios_telegram SET bloqueado = 0 WHERE id_telegram = %s", (chat_id,))
                    await conn.commit()
                #    await send_welcome(message)
            else:
                if chat_type in ['group', 'supergroup']:
                    await cursor.execute("SELECT * FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (chat_id,))
                    result = await cursor.fetchone()
                    if result is None:
                        await cursor.execute("INSERT INTO machine_learning.usuarios_telegram (usuario, nome_supervisorio, nome_telegram, id_telegram, primeiro_acesso, cod_usuario, ativo) VALUES (%s, %s, %s, %s, %s, %s, 1)",
                                            ('Grupo', 'Grupo Telegram', message.chat.title, chat_id, '1', '0'))
                        await conn.commit()
                        await message.reply("Grupo salvo com sucesso!\nDigite /menu para opções")
                    else:
                        await cursor.execute("UPDATE machine_learning.usuarios_telegram SET usuario = %s, nome_supervisorio = %s, nome_telegram = %s, ativo = 1 WHERE id_telegram = %s",
                                            ('Grupo', 'Grupo Telegram', message.chat.title, chat_id))
                        await message.reply("Grupo atualizado com sucesso!\nDigite /menu para opções")
                    await conn.commit()
                else:
                    await bot.send_message(chat_id, 'Olá! \nQual é o seu usuário no supervisório da BRG?\n                 <a href="https://supervisorio.brggeradores.com.br/beta/index.php">Acessar supervisório</a>', parse_mode='HTML')



async def cadastrar_usinas_usuario(pool, cod_usuario, cod_usinas):
    try:
        async with pool.acquire() as conn:
            await conn.ping(reconnect=True)  # Forçar reconexão
            async with conn.cursor() as cursor:
                for cod_usina in cod_usinas:
                    # Verificar se a usina já está cadastrada
                    await cursor.execute("""
                        SELECT COUNT(*) FROM machine_learning.usinas_usuario 
                        WHERE cod_usuario = %s AND cod_usina = %s
                    """, (cod_usuario, cod_usina))
                    
                    exists = await cursor.fetchone()
                    if exists and exists[0] > 0:
                        continue

                    # Inserir a usina se não existir
                    await cursor.execute("""
                        INSERT INTO machine_learning.usinas_usuario (cod_usuario, cod_usina, ativo)
                        VALUES (%s, %s, 1)
                    """, (cod_usuario, cod_usina))

                await conn.commit()
    except Exception as e:
        print(f"Erro ao cadastrar usinas para o usuário {cod_usuario}: {e}")



async def boas_vindas(message: types.Message, id_telegram=None):
    try:
        user_input = message.text.lstrip('/')
        username = user_input
        chat_id = message.chat.id
        pool = dp.pool  # Reutilizando o pool criado durante a inicialização

        async with pool.acquire() as conn:
            await conn.ping(reconnect=True)  # Forçar reconexão
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT nome, codigo FROM sup_geral.usuarios WHERE login = %s", (username,))
                result = await cursor.fetchone()

                if result is not None:
                    nome_supervisorio, cod_usuario = result

                    await cursor.execute("SELECT primeiro_acesso FROM machine_learning.usuarios_telegram WHERE usuario = %s", (username,))
                    primeiro_acesso_row = await cursor.fetchone()
                    if primeiro_acesso_row is not None:
                        primeiro_acesso = primeiro_acesso_row[0]

                        if primeiro_acesso == 1:
                            await cursor.execute("SELECT cod_usina FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
                            cod_usinas = await cursor.fetchall()

                            nomes_usinas = []

                            for cod_usina_tuple in cod_usinas:
                                cod_usina = cod_usina_tuple[0]
                                await cursor.execute("SELECT nome FROM sup_geral.usinas WHERE codigo = %s", (cod_usina,))
                                nome_usina_row = await cursor.fetchone()
                                if nome_usina_row is not None:
                                    nome_usina = nome_usina_row[0]
                                    nomes_usinas.append(f"{cod_usina} - {nome_usina}")
                                else:
                                    print(f"Nome da usina com código {cod_usina} não encontrado.")
                            nomes_usinas_str = '\n'.join(nomes_usinas)

                            if id_telegram:
                                await bot.send_message(id_telegram, f"Aqui está todas as usinas que {nome_supervisorio} está cadastrado:\n{nomes_usinas_str}")
                            else:
                                await bot.send_message(message.chat.id, f"Aqui está todas as usinas que {nome_supervisorio} está cadastrado:\n{nomes_usinas_str}")

                            # Chame a função para cadastrar as usinas
                            await cadastrar_usinas_usuario(pool, cod_usuario, [cod_usina[0] for cod_usina in cod_usinas])

                            # Chame send_welcome após enviar a mensagem inicial
                        #    await send_welcome(message)

                            await cursor.execute("UPDATE machine_learning.usuarios_telegram SET primeiro_acesso = 0 WHERE usuario = %s", (username,))
                            await conn.commit()
                            print('setando para zero o primeiro_acesso', username)
                    
                        else:
                            await message.reply("Usuário já existente.")
                            
                            # Chame send_welcome após enviar a mensagem inicial
                        #    await send_welcome(message)

                            # Chame a função para cadastrar as usinas
                            await cadastrar_usinas_usuario(pool, cod_usuario, [cod_usina[0] for cod_usina in cod_usinas])

                            await cursor.execute("UPDATE machine_learning.usuarios_telegram SET primeiro_acesso = 0 WHERE usuario = %s", (username,))
                            await conn.commit()
                            print('setando para zero o primeiro_acesso', username)
                            
                    else:
                        print("Nenhum valor retornado para o campo 'primeiro_acesso' na tabela 'usuarios_telegram'.")
                else:
                    await message.reply("Usuário não encontrado.")
    except Exception as e:
        print(f"Erro na função boas_vindas: {e}")


@dp.message_handler()
async def save_username(message: types.Message):
    try:
        user_input = message.text.lstrip('/')
        username = user_input
        chat_type = message.chat.type
        pool = dp.pool  # Reutilizando o pool criado durante a inicialização

        full_name = f"{message.from_user.first_name} {message.from_user.last_name}"
        name = f"{message.from_user.first_name}"
        chat_id = message.chat.id

        async with pool.acquire() as conn:
            await conn.ping(reconnect=True)  # Forçar reconexão
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT nome, codigo FROM sup_geral.usuarios WHERE login = %s", (username,))
                result = await cursor.fetchone()

                if result is not None:
                    nome_supervisorio, cod_usuario = result

                    await cursor.execute("SELECT * FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (chat_id,))
                    result = await cursor.fetchone()

                    if result is None:
                        query = "INSERT INTO machine_learning.usuarios_telegram (usuario, nome_supervisorio, nome_telegram, id_telegram, cod_usuario, ativo) VALUES (%s, %s, %s, %s, %s, %s)"
                        values = (username, nome_supervisorio, full_name, chat_id, cod_usuario, 1)
                        await cursor.execute(query, values)
                        await message.reply("Usuário salvo com sucesso!")
                    else:
                        await cursor.execute("UPDATE machine_learning.usuarios_telegram SET usuario = %s, nome_supervisorio = %s, nome_telegram = %s, cod_usuario = %s, ativo = %s WHERE id_telegram = %s",
                                           (username, nome_supervisorio, full_name, cod_usuario, 1, chat_id))
                        await message.reply("Usuário atualizado com sucesso!")

                    await conn.commit()

                    await boas_vindas(message)

                else:
                    await cursor.execute("SELECT * FROM machine_learning.usuarios_telegram WHERE nome_telegram = %s", (full_name,))
                    result = await cursor.fetchone()

                    if result is not None:

                        if chat_type in ['group', 'supergroup']:
                            await cursor.execute("SELECT * FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (chat_id,))
                            result = await cursor.fetchone()
                            if result is None:
                                await cursor.execute("SELECT data_cadastro FROM machine_learning.usuarios_telegram WHERE nome_telegram = %s", (full_name,))
                                data_cadastro = (await cursor.fetchone())[0]
                                await cursor.execute("UPDATE machine_learning.usuarios_telegram SET usuario = %s, nome_supervisorio = 'Grupo Telegram', id_telegram = %s, data_cadastro = %s WHERE nome_telegram = %s",
                                                   (username, chat_id, data_cadastro, full_name))
                                # await cursor.execute("UPDATE machine_learning.usuarios_telegram SET usuario = %s, nome_supervisorio = %s, id_telegram = %s, data_cadastro = %s WHERE nome_telegram = %s",
                                #                   (0, username, chat_id, data_cadastro, full_name))
                                await conn.commit()
                                await message.reply("Usuário atualizado com sucesso!")
                            else:
                                await message.reply("Você não pode editar o usuário aqui, se quiser editar, digite /menu!")

                    else:
                        query = "INSERT INTO machine_learning.usuarios_telegram (usuario, nome_telegram, nome_supervisorio, id_telegram, ativo) VALUES (%s, %s, %s, %s, %s)"
                        values = (0, full_name, username, chat_id, 1)
                        await cursor.execute(query, values)
                        await conn.commit()
                        await message.reply("Nome do usuário não encontrado, enviado para a administração!")
                        id_grupo = await id_chat_grupo(pool)
                        print(id_grupo)
                        await bot.send_message(id_grupo, f'Usuario {full_name} de ID ({chat_id}) não encontrado no banco de dados, digite /menu e insira manualmente')

    except Exception as e:
        print(f"Erro ao salvar o nome de usuário: {e}")



async def verificar_e_obter_coeficiente(cod_equipamento, pool):
    try:
        coeficiente_existente = 0.0
        intercepto_existente = 0.0
        async with pool.acquire() as conn:
            await conn.ping(reconnect=True)  # Forçar reconexão
            async with conn.cursor() as cursor:
                await cursor.execute(f"SELECT coeficiente, intercepto FROM machine_learning.coeficiente_geradores WHERE cod_equipamento = {cod_equipamento}")
                resultado = await cursor.fetchone()
                if resultado is not None:
                    coeficiente_existente, intercepto_existente = resultado
                else:
                #    return None, None  # Retorna None se o equipamento não estiver na tabela
                    return 0.0, 0.0
        return coeficiente_existente, intercepto_existente
    except Exception as e:
        print(f"An error occurred in verificar_e_obter_coeficiente: {e}")
        return 0.0, 0.0
    #    return None, None  # Retorna None em caso de erro



from statsmodels.tsa.ar_model import AutoReg
import numpy as np

async def fazer_previsao_sempre_alerta(valores_atuais, coeficiente, intercepto, cod_equipamento_resultado, pool):
    contagem_limites = 4

    # async with pool.acquire() as conn:
    #     await conn.ping(reconnect=True)  # Forçar reconexão
    #     async with conn.cursor() as cursor:
    #         await cursor.execute("SELECT data_cadastro FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 3", (int(cod_equipamento_resultado),))
    #         data_cadastro = await cursor.fetchone()

    #         if data_cadastro is not None:
    #             data_cadastro = data_cadastro[0]

    #         agora = datetime.now()

    #         if data_cadastro is None or (agora - data_cadastro > timedelta(hours=1)):
    #             await cursor.execute("""
    #                 UPDATE machine_learning.leituras_consecutivas
    #                 SET alerta = 0
    #                 WHERE cod_equipamento = %s
    #             """, (int(cod_equipamento_resultado),))
    #             await conn.commit()
    #             return False, False, False

    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor() as cursor:
            # Buscar o último data_cadastro para o equipamento e o alerta atual
            await cursor.execute("""
                SELECT data_cadastro, alerta 
                FROM machine_learning.leituras_consecutivas 
                WHERE cod_equipamento = %s AND cod_campo = 3
            """, (int(cod_equipamento_resultado),))
            resultado = await cursor.fetchone()

            if resultado is None:
                # Nenhum registro encontrado para o equipamento
                return False, False, False

            data_cadastro, alerta = resultado
            agora = datetime.now()

            # Verificar se data_cadastro está desatualizado (mais de 1 hora)
            if data_cadastro is None or (agora - data_cadastro > timedelta(hours=1)):
                if alerta == 1:  # Atualizar o alerta somente se ele for 1
                    await cursor.execute("""
                        UPDATE machine_learning.leituras_consecutivas
                        SET alerta = 0
                        WHERE cod_equipamento = %s AND alerta = 1
                    """, (int(cod_equipamento_resultado),))
                    await conn.commit()
                return False, False, False


        
        #    if data_cadastro is not None or (agora - data_cadastro <= timedelta(hours=1)):

            coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(cod_equipamento_resultado, pool)

            # Adiciona filtro para não processar equipamentos sem coeficiente
            if coeficiente_existente is None or intercepto_existente is None:
            #    print(f"Equipamento {cod_equipamento_resultado} não encontrado na tabela coeficiente_geradores.")
                return False, False, False  # Ou qualquer outra lógica de tratamento que você prefira


            if valores_atuais is not None:
                previsoes = [(valor * coeficiente_existente + intercepto_existente) for valor in valores_atuais]
            else:
                previsoes = []

            previsoes = [round(valor, 1) for valor in previsoes]

            await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 3", (int(cod_equipamento_resultado),))
            valores_atuais_3 = await cursor.fetchone()

            if valores_atuais_3 is None or all(v == 0 for v in valores_atuais_3):
                await cursor.execute("""
                UPDATE machine_learning.leituras_consecutivas
                SET alerta = 0
                WHERE cod_equipamento = %s
                """, (int(cod_equipamento_resultado),))
                await conn.commit()

                return previsoes, False, False

            # Obter valores atuais do cod_campo 114
            await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 114", (int(cod_equipamento_resultado),))
            valores_atuais_114 = await cursor.fetchone()
                        
            if valores_atuais_114 is None:
                return previsoes, False, False


            # await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 21", (int(cod_equipamento_resultado),))
            # valores_atuais_21 = await cursor.fetchone()

            # await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 76", (int(cod_equipamento_resultado),))
            # valores_atuais_76 = await cursor.fetchone()
            
            # # Adicionar previsão futura usando AutoReg
            # # Usamos os valores disponíveis de valor_1 a valor_5 para treinar o modelo AutoReg
            # valores_para_treinar = np.array(valores_atuais_114)
            # valores_para_treinar_previsoes = np.array(previsoes)
            # valores_para_treinar_21 = np.array(valores_atuais_21)
            # valores_para_treinar_76 = np.array(valores_atuais_76)

            # # Ajustar o modelo AutoReg e prever os próximos 5 valores
            # modelo_autoreg = AutoReg(valores_para_treinar, lags=1)  # Usando 1 lag para previsão simples
            # modelo_autoreg_previsoes = AutoReg(valores_para_treinar_previsoes, lags=1)  # Usando 1 lag para previsão simples
            # modelo_autoreg_21 = AutoReg(valores_para_treinar_21, lags=1)  # Usando 1 lag para previsão simples
            # modelo_autoreg_76 = AutoReg(valores_para_treinar_76, lags=1)  # Usando 1 lag para previsão simples
            
            # modelo_ajustado = modelo_autoreg.fit()
            # modelo_ajustado_previsoes = modelo_autoreg_previsoes.fit()
            # modelo_ajustado_21 = modelo_autoreg_21.fit()
            # modelo_ajustado_76 = modelo_autoreg_76.fit()

            # # Prever os próximos 5 valores (futuros)
            # previsoes_futuras = modelo_ajustado.predict(start=len(valores_para_treinar), end=len(valores_para_treinar) + 4)
            # previsoes_futuras_previsoes = modelo_ajustado_previsoes.predict(start=len(valores_para_treinar_previsoes), end=len(valores_para_treinar_previsoes) + 4)
            # previsoes_futuras_21 = modelo_ajustado_21.predict(start=len(valores_para_treinar_21), end=len(valores_para_treinar_21) + 4)
            # previsoes_futuras_76 = modelo_ajustado_76.predict(start=len(valores_para_treinar_76), end=len(valores_para_treinar_76) + 4)

            # previsoes_futuras = [round(valor, 1) for valor in previsoes_futuras]
            # previsoes_futuras_previsoes = [round(valor, 1) for valor in previsoes_futuras_previsoes]
            # previsoes_futuras_21 = [round(valor, 1) for valor in previsoes_futuras_21]
            # previsoes_futuras_76 = [round(valor, 1) for valor in previsoes_futuras_76]

            # print('\n\ncod_equipamento ',cod_equipamento_resultado)
            # print('\nvalores_atuais_114',valores_atuais_114)
            # print('\nprevisoes_futuras',previsoes_futuras)
            # print('\nprevisoes',previsoes)
            # print('\nprevisoes previstas',previsoes_futuras_previsoes)
            # print('\nvalores_atuais_21 ',valores_atuais_21)
            # print('\nprevisoes_futuras_21 ',previsoes_futuras_21)
            # print('\nvalores_atuais_76 - ', valores_atuais_76)
            # print('\nprevisoes_futuras_76 - ', previsoes_futuras_76)
            
            limite_mais = 15
            limite_menos = -15
            
            contagem_acima_do_limite = 0
            contagem_abaixo_do_limite = 0

            for i, valor in enumerate(valores_atuais_114):
                if valor == 0 or previsoes[i] == 0:
                    return previsoes, False, False

                limite_porcentagem_mais = round(0.15 * valor, 2)
                limite_porcentagem_menos = round(-0.15 * valor, 2)

                calculo = round(valor - previsoes[i], 2)

                if calculo < limite_menos:
                    contagem_abaixo_do_limite +=1
            #        print(cod_equipamento_resultado,'         ABAIXO', ' ',i,'  valor',valor,'     previsoes',previsoes[i], '     calculo =',calculo, '     limite_menos', limite_menos, '     contagem_abaixo_do_limite', contagem_abaixo_do_limite)

                elif abs(calculo) > limite_mais:
                    contagem_acima_do_limite += 1
            #        print(cod_equipamento_resultado,'         ACIMA', ' ',i,'  valor',valor,'     previsoes',previsoes[i], '     calculo =',calculo, '     limite_mais', limite_mais, '     contagem_acima_do_limite', contagem_acima_do_limite)

                elif not abs(calculo) > limite_mais or not calculo < limite_menos:
                    pass

            return previsoes, contagem_abaixo_do_limite >= contagem_limites, contagem_acima_do_limite >= contagem_limites

            

async def fazer_previsao_sempre(valores_atuais, coeficiente, intercepto, cod_equipamento_resultado):
    contagem_limites = 4
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização

    async with pool.acquire() as conn:
        await conn.ping(reconnect=True)  # Forçar reconexão
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT data_cadastro FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 3", (int(cod_equipamento_resultado),))
            data_cadastro = await cursor.fetchone()

    agora = datetime.now()

    if data_cadastro is not None and (agora - data_cadastro[0] <= timedelta(days=1)):
        coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(cod_equipamento_resultado, pool)

        if valores_atuais is not None:
            previsoes = [(valor * coeficiente_existente + intercepto_existente) for valor in valores_atuais]
        else:
            previsoes = []

        previsoes = [round(valor, 1) for valor in previsoes]

        async with pool.acquire() as conn:
            await conn.ping(reconnect=True)  # Forçar reconexão
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 114", (int(cod_equipamento_resultado),))
                valores_atuais_114 = await cursor.fetchone()

                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 3", (int(cod_equipamento_resultado),))
                valores_atuais_3 = await cursor.fetchone()

        if valores_atuais_3 is None or 0 in valores_atuais_3:
            return previsoes, False

        # Verificar se valores_atuais_114 não é None antes de iterar
        if valores_atuais_114 is None:
            return previsoes, False

        limite_mais = 16
        limite_menos = -16

        contagem_acima_do_limite = 0
        contagem_abaixo_do_limite = 0

        for i, valor in enumerate(valores_atuais_114):
            if valor == 0 or previsoes[i] == 0:
                return previsoes, False
            
            limite_porcentagem_mais = round(0.15 * valor, 2)
            limite_porcentagem_menos = round(-0.15 * valor, 2)

            calculo = round(valor - previsoes[i], 2)

            if calculo < limite_menos:
                contagem_abaixo_do_limite += 1
                print(cod_equipamento_resultado, 'ABAIXO', i, 'valor', valor, 'previsoes', previsoes[i], 'calculo =', calculo, 'limite_menos', limite_menos, 'contagem_abaixo_do_limite', contagem_abaixo_do_limite)

            elif abs(calculo) > limite_mais:
                contagem_acima_do_limite += 1
                print(cod_equipamento_resultado, 'ACIMA', i, 'valor', valor, 'previsoes', previsoes[i], 'calculo =', calculo, 'limite_mais', limite_mais, 'contagem_acima_do_limite', contagem_acima_do_limite)

        return previsoes, contagem_acima_do_limite > contagem_limites or contagem_abaixo_do_limite > contagem_limites

    else:
        async with pool.acquire() as conn:
            await conn.ping(reconnect=True)  # Forçar reconexão
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    UPDATE machine_learning.leituras_consecutivas
                    SET alerta = 0
                    WHERE cod_equipamento = %s
                """, (int(cod_equipamento_resultado),))
                await conn.commit()

        print('Mais de uma hora, setando para 0 o alerta', cod_equipamento_resultado)

        return 'NOT UPDATED BRO', False



async def fazer_previsao(valores_atuais, coeficiente, intercepto, cod_equipamento_resultado, pool):

    try:

        contagem_limites = 4
    #    pool = dp.pool  # Reutilizando o pool criado durante a inicialização

        async with pool.acquire() as conn:
            await conn.ping(reconnect=True)  # Forçar reconexão
            async with conn.cursor() as cursor:
                # Obter data_cadastro para o cod_campo 3
                await cursor.execute("SELECT data_cadastro FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 3", (int(cod_equipamento_resultado),))
                data_cadastro = await cursor.fetchone()
                
                # Obter valores atuais e data_cadastro para cod_campo 114
                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5, data_cadastro FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 114", (int(cod_equipamento_resultado),))
                result = await cursor.fetchone()
                
                if result is None:
            #        print(f"Valores atuais 114 para o equipamento {cod_equipamento_resultado} não encontrados.")
                    return [], False

                valores_atuais_114 = result[:-1]
                data_cadastro_consecutivas = result[-1]

                # Obter data_cadastro mais recente das leituras
                await cursor.execute("SELECT data_cadastro FROM sup_geral.leituras WHERE cod_equipamento = %s ORDER BY data_cadastro DESC LIMIT 1", (int(cod_equipamento_resultado),))
                result = await cursor.fetchone()

                if result is not None:
                    data_cadastro_leituras = result[0]
                else:
            #        print(f"Data de cadastro das leituras para o equipamento {cod_equipamento_resultado} não encontrada.")
                    return [], False

        agora = datetime.now()

        # Verificar se a última leitura é a mesma que a última consecutiva e se a diferença é maior que uma hora
        if data_cadastro_leituras == data_cadastro_consecutivas or (agora - data_cadastro_consecutivas > timedelta(hours=1)):
        #    print(f"A leitura mais recente é a mesma que a leitura consecutiva ou a diferença é maior que uma hora para o equipamento {cod_equipamento_resultado}.")
            return [], False

        if data_cadastro is not None and (agora - data_cadastro[0] <= timedelta(days=1)):
            coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(cod_equipamento_resultado, pool)

            if valores_atuais is not None:
                previsoes = [(valor * coeficiente_existente + intercepto_existente) for valor in valores_atuais]
            else:
                previsoes = []
                
            previsoes = [round(valor, 1) for valor in previsoes]

            if valores_atuais_114 is None:
        #        print(f"Valores atuais 114 para o equipamento {cod_equipamento_resultado} não encontrados.")
                return previsoes, False

            limite_mais = 16
            limite_menos = -16

            contagem_acima_do_limite = 0
            contagem_abaixo_do_limite = 0

            for i, valor in enumerate(valores_atuais_114):
                if valor == 0 or previsoes[i] == 0:
                    return previsoes, False
                
                limite_porcentagem_mais = round(0.15 * valor, 2)
                limite_porcentagem_menos = round(-0.15 * valor, 2)

                calculo = round(valor - previsoes[i], 2)

                if calculo < limite_menos:
                    contagem_abaixo_do_limite +=1
        #            print(cod_equipamento_resultado,' ABAIXO', ' ',i,' valor',valor,' previsoes',previsoes[i], ' calculo =',calculo, ' limite_menos', limite_menos, ' contagem_abaixo_do_limite', contagem_abaixo_do_limite)

                elif abs(calculo) > limite_mais:
                    contagem_acima_do_limite += 1
        #            print(cod_equipamento_resultado,' ACIMA', ' ',i,' valor',valor,' previsoes',previsoes[i], ' calculo =',calculo, ' limite_mais', limite_mais, ' contagem_acima_do_limite', contagem_acima_do_limite)

            return previsoes, contagem_acima_do_limite > contagem_limites or contagem_abaixo_do_limite > contagem_limites

        else:
            return 'NOT UPDATED BRO', False

    except Exception as e:
        print(f"An error occurred in fazer_previsao: {e}")
        return 0, False


'''

async def processar_equipamentos(cod_equipamentos, tabelas, cod_campo_especificados, pool):
    while True:
        for cod_equipamento in cod_equipamentos:
            # Inicializando dicionário para armazenar os valores dos últimos 5 dados por campo
            valores = {cod: [0, 0, 0, 0, 0] for cod in cod_campo_especificados}
            try:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        # Buscando leituras da tabela `leituras` com base nos campos especificados
                        query = f"""
                        SELECT data_cadastro, valor, cod_campo 
                        FROM {tabelas} 
                        WHERE cod_equipamento = %s 
                        AND cod_campo IN ({', '.join(cod_campo_especificados)})
                        """
                        await cursor.execute(query, (cod_equipamento,))
                        resultados = await cursor.fetchall()

                        # Convertendo para DataFrame para facilitar o processamento
                        df = pd.DataFrame(resultados, columns=['data_cadastro', 'valor', 'cod_campo'])
                        df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])
                        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')

                        # Iterando sobre os cod_campo especificados
                        for cod in cod_campo_especificados:
                            # Filtrando os valores mais recentes para cada cod_campo
                            valores_cod_campo = df[df['cod_campo'] == int(cod)]['valor'].values
                            valores[cod] = list(valores_cod_campo[-5:])[::-1] + valores[cod][:5-len(valores_cod_campo[-5:])]

                            # Buscando a data da última leitura na tabela `leituras`
                            await cursor.execute(f"SELECT data_cadastro FROM {tabelas} WHERE cod_equipamento = %s AND cod_campo = %s", (cod_equipamento, cod))
                            data_cadastro_leituras = await cursor.fetchone()
                            if data_cadastro_leituras:
                                data_cadastro_formatada_leituras = data_cadastro_leituras[0].strftime('%Y-%m-%d %H:%M:%S')

                                # Verificando se o campo já existe em `leituras_consecutivas`
                                await cursor.execute(f"SELECT data_cadastro FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = %s", (cod_equipamento, cod))
                                data_cadastro_consecutivas = await cursor.fetchone()

                                # Se não houver dados em `leituras_consecutivas`, insira diretamente da tabela `leituras`
                                if not data_cadastro_consecutivas:
                                #    print(f"Inserindo para cod_equipamento: {cod_equipamento}, cod_campo: {cod}, valores: {valores[cod]}")
                                    data_cadastro_formatada_consecutivas = data_cadastro_formatada_leituras  # Usar a data de leituras
                                    await cursor.execute(f"""
                                    INSERT INTO machine_learning.leituras_consecutivas 
                                    (cod_equipamento, cod_campo, valor_1, valor_2, valor_3, valor_4, valor_5, data_cadastro)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                    """, (cod_equipamento, cod, *valores[cod][::-1], data_cadastro_formatada_leituras))
                                    await conn.commit()

                                else:
                                    # Se houver, atualize se necessário
                                    data_cadastro_formatada_consecutivas = data_cadastro_consecutivas[0].strftime('%Y-%m-%d %H:%M:%S') if data_cadastro_consecutivas else data_cadastro_formatada_leituras

                                    if data_cadastro_formatada_leituras != data_cadastro_formatada_consecutivas:
                                #        print(f"Atualizando para cod_equipamento: {cod_equipamento}, cod_campo: {cod}, valores: {valores[cod]}")
                                        await cursor.execute(f"""
                                        INSERT INTO machine_learning.leituras_consecutivas 
                                        (cod_equipamento, cod_campo, valor_1, valor_2, valor_3, valor_4, valor_5, data_cadastro)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                        ON DUPLICATE KEY UPDATE
                                        valor_1 = machine_learning.leituras_consecutivas.valor_2,
                                        valor_2 = machine_learning.leituras_consecutivas.valor_3,
                                        valor_3 = machine_learning.leituras_consecutivas.valor_4,
                                        valor_4 = machine_learning.leituras_consecutivas.valor_5,
                                        valor_5 = VALUES(valor_5),
                                        data_cadastro = VALUES(data_cadastro)
                                        """, (cod_equipamento, cod, *valores[cod][::-1], data_cadastro_formatada_leituras))
                                        await conn.commit()

            except Exception as e:
                print(f"Erro ao processar o equipamento {cod_equipamento}: {str(e)}")
                
            except mysql.connector.errors.OperationalError as e:
                if 'Lost connection to MySQL server during query' in str(e):
                    print(f"Erro ao processar o equipamento {cod_equipamento}: {str(e)}")
                    continue
                else:
                    raise

        # Intervalo entre execuções
        await asyncio.sleep(30)

'''





from datetime import datetime, timedelta
import pandas as pd
import asyncio

'''

async def processar_equipamentos(cod_equipamentos, tabelas, cod_campo_especificados, pool):
    while True:
        tempo_inicial = datetime.now()
        data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')
        print('\n', '----------------------------------- inicio processar_equipamentos -----------------------------------', data_cadastro_formatada, '\n')

        try:
            async with pool.acquire() as conn:
                await conn.ping(reconnect=True)
                async with conn.cursor() as cursor:
                    # Buscando dados de todos os equipamentos e campos de uma vez
                    query = f"""
                    SELECT cod_equipamento, data_cadastro, valor, cod_campo 
                    FROM {tabelas} 
                    WHERE cod_equipamento IN ({', '.join(map(str, cod_equipamentos))}) 
                    AND cod_campo IN ({', '.join(cod_campo_especificados)})
                    """
                    await cursor.execute(query)
                    resultados = await cursor.fetchall()

                    # Convertendo para DataFrame
                    df = pd.DataFrame(resultados, columns=['cod_equipamento', 'data_cadastro', 'valor', 'cod_campo'])
                    df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])
                    df['valor'] = pd.to_numeric(df['valor'], errors='coerce')

                    # Processamento em paralelo por equipamento
                    tasks = [
                        processar_dados_por_equipamento(pool, df, cod_equipamento, cod_campo_especificados)
                        for cod_equipamento in cod_equipamentos
                    ]
                    await asyncio.gather(*tasks)

        except Exception as e:
            print(f"Erro ao processar os equipamentos: {str(e)}")

        # Tempo de execução
        tempo_final = datetime.now()
        data_cadastro_formatada_final = tempo_final.strftime('%d-%m-%Y %H:%M')
        tempo_execucao = tempo_final - tempo_inicial
        segundos = tempo_execucao.total_seconds()
        horas = int(segundos // 3600)
        minutos = int((segundos % 3600) // 60)
        segundos_restantes = int(segundos % 60)

        print('\n', '----------------------------------- fim processar_equipamentos -----------------------------------', data_cadastro_formatada_final)
        print(f'Tempo de execução: {horas} horas, {minutos} minutos, {segundos_restantes} segundos\n')
        
        await asyncio.sleep(30)

async def processar_dados_por_equipamento(pool, df, cod_equipamento, cod_campo_especificados):
    try:
        # Filtrar dados para o equipamento específico
        df_equipamento = df[df['cod_equipamento'] == cod_equipamento]

        # Verificar se a data de cadastro é mais recente que 1 hora
        uma_hora_atras = datetime.now() - timedelta(hours=1)

        for cod in cod_campo_especificados:
            df_campo = df_equipamento[df_equipamento['cod_campo'] == int(cod)]
            if df_campo.empty:
                continue

            data_cadastro_recente = df_campo['data_cadastro'].max()

            # Ignorar se a data de cadastro é mais antiga que 1 hora
            if data_cadastro_recente < uma_hora_atras:
            #    print(f"Equipamento {cod_equipamento}, campo {cod} ignorado. Dados desatualizados (mais de 1 hora).")
                continue

            valores_cod_campo = df_campo['valor'].values
            valor_recente = valores_cod_campo[-1] if len(valores_cod_campo) > 0 else 0

            # Pegue os últimos 4 valores e preencha com zeros se necessário
            valores = list(valores_cod_campo[-4:])[::-1]
            valores = [0] * (4 - len(valores)) + valores
            valores.append(valor_recente)

            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Verificar data_cadastro na tabela leituras_consecutivas
                    check_query = """
                    SELECT data_cadastro FROM machine_learning.leituras_consecutivas 
                    WHERE cod_equipamento = %s AND cod_campo = %s
                    ORDER BY data_cadastro DESC LIMIT 1
                    """
                    await cursor.execute(check_query, (cod_equipamento, cod))
                    data_cadastro_consecutiva = await cursor.fetchone()

                    # Continuar se data_cadastro for igual ou maior
                    if data_cadastro_consecutiva and data_cadastro_consecutiva[0] == data_cadastro_recente:
                    #    print(f"Equipamento {cod_equipamento}, campo {cod} já possui registro na data {data_cadastro_recente}.")
                        continue

                    # Inserir dados na tabela leituras_consecutivas
                    insert_query = """
                    INSERT INTO machine_learning.leituras_consecutivas 
                    (cod_equipamento, cod_campo, valor_1, valor_2, valor_3, valor_4, valor_5, data_cadastro)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                    valor_1 = valor_2,
                    valor_2 = valor_3,
                    valor_3 = valor_4,
                    valor_4 = valor_5,
                    valor_5 = VALUES(valor_5),
                    data_cadastro = VALUES(data_cadastro)
                    """
                    await cursor.execute(insert_query, (cod_equipamento, cod, *valores))
                    await conn.commit()

    except Exception as e:
        print(f"Erro ao processar dados do equipamento {cod_equipamento}: {str(e)}")

'''


async def processar_equipamentos(cod_equipamentos, tabela_leituras, cod_campos, pool):
    while True:
        inicio = datetime.now()
    #    print(f"[{inicio}] Iniciando processamento...")

        try:
            async with pool.acquire() as conn:
                await conn.ping(reconnect=True)
                async with conn.cursor() as cursor:
                    # Filtrando leituras pelos cod_campos selecionados
                    cod_campos_str = ','.join(map(str, cod_campos))  # Convertendo os cod_campos em uma string para a consulta
                    await cursor.execute(f"""
                        SELECT cod_equipamento, cod_campo, valor, data_cadastro
                        FROM {tabela_leituras}
                        WHERE cod_campo IN ({cod_campos_str})
                    """)
                    leituras = await cursor.fetchall()

                    # Filtrando leituras consecutivas pelos cod_campos selecionados
                    await cursor.execute(f"""
                        SELECT cod_equipamento, cod_campo, valor_1, valor_2, valor_3, valor_4, valor_5, data_cadastro
                        FROM leituras_consecutivas
                        WHERE cod_campo IN ({cod_campos_str})
                    """)
                    consecutivas = await cursor.fetchall()

            # Criar DataFrames
            df_leituras = pd.DataFrame(leituras, columns=['cod_equipamento', 'cod_campo', 'valor', 'data_cadastro'])
            df_consecutivas = pd.DataFrame(consecutivas, columns=[
                'cod_equipamento', 'cod_campo', 'valor_1', 'valor_2', 'valor_3', 'valor_4', 'valor_5', 'data_cadastro'
            ])

            # Merge para encontrar diferenças
            df_merged = pd.merge(
                df_leituras, df_consecutivas,
                on=['cod_equipamento', 'cod_campo'],
                how='left',
                suffixes=('_leituras', '_consecutivas')
            )

            # Filtrar registros onde data_cadastro mudou ou não existe na tabela consecutivas
            df_atualizar = df_merged[
                (df_merged['data_cadastro_consecutivas'].isnull()) |
                (df_merged['data_cadastro_leituras'] != df_merged['data_cadastro_consecutivas'])
            ]

            if not df_atualizar.empty:
                # Preparar valores para inserção/atualização
                # Modificar valores usando .loc para evitar o SettingWithCopyWarning
                df_atualizar.loc[:, 'valor_5'] = df_atualizar['valor']  # Usando o nome correto da coluna
                df_atualizar.loc[:, 'valor_4'] = df_atualizar['valor_4'].fillna(0)
                df_atualizar.loc[:, 'valor_3'] = df_atualizar['valor_3'].fillna(0)
                df_atualizar.loc[:, 'valor_2'] = df_atualizar['valor_2'].fillna(0)
                df_atualizar.loc[:, 'valor_1'] = df_atualizar['valor_1'].fillna(0)


                valores = df_atualizar[['cod_equipamento', 'cod_campo', 'valor_1', 'valor_2', 'valor_3', 'valor_4', 'valor_5', 'data_cadastro_leituras']].values.tolist()

                async with pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        # Inserção em lote
                        insert_query = f"""
                        INSERT INTO leituras_consecutivas 
                        (cod_equipamento, cod_campo, valor_1, valor_2, valor_3, valor_4, valor_5, data_cadastro)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        valor_1 = VALUES(valor_2),
                        valor_2 = VALUES(valor_3),
                        valor_3 = VALUES(valor_4),
                        valor_4 = VALUES(valor_5),
                        valor_5 = VALUES(valor_5),
                        data_cadastro = VALUES(data_cadastro)
                        """
                        await cursor.executemany(insert_query, valores)
                        await conn.commit()

    #            print(f"[{datetime.now()}] {len(valores)} registros atualizados/inseridos.")
            else:
    #            print(f"[{datetime.now()}] Nenhuma atualização necessária.")
                pass

        except Exception as e:
            print(f"Erro: {e}")

        # Tempo de execução
        fim = datetime.now()
        duracao = (fim - inicio).total_seconds()
    #    print(f"Processamento concluído em {duracao:.2f} segundos.")

        # Esperar 2 segundos
        await asyncio.sleep(2)





sem_mensagem_silenciado = set()

async def verificar_e_excluir_linhas_expiradas(pool):
    while True:
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT cod_usuario, cod_equipamento, tempo_silenciado, data_cadastro, receber_alarme FROM machine_learning.telegram_silenciar_bot")
                    results = await cursor.fetchall()

                    for result in results:
                        try:
                            cod_usuario_verifica_silenciado, cod_equipamento, tempo_silenciado, data_cadastro, receber_alarme = result

                            await cursor.execute("SELECT nome, cod_usina FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                            equipamento_result = await cursor.fetchone()
                            if equipamento_result is not None:
                                nome, cod_usina_silenciada = equipamento_result

                            sem_mensagem_silenciado.add((cod_usina_silenciada, cod_usuario_verifica_silenciado))

                            if tempo_silenciado > 0:
                                tempo_passado = datetime.now() - data_cadastro
                                if tempo_passado.total_seconds() / 3600 >= tempo_silenciado:
                                    if receber_alarme == 1:
                                        await cursor.execute("UPDATE machine_learning.telegram_silenciar_bot SET tempo_silenciado = 0 WHERE cod_equipamento = %s", (cod_equipamento,))
                                        sem_mensagem_silenciado.discard((cod_usina_silenciada, cod_usuario_verifica_silenciado))
                                        print(f"Coluna 'tempo_silenciado' atualizada para 0. Equipamento {cod_equipamento} configurado para receber alarmes novamente.")
                                    elif receber_alarme == 0:
                                        await cursor.execute("DELETE FROM machine_learning.telegram_silenciar_bot WHERE cod_equipamento = %s", (cod_equipamento,))
                                        print(f"Tempo de silenciamento finalizado. Linha correspondente ao equipamento {cod_equipamento} excluída da tabela.")
                                        sem_mensagem_silenciado.discard((cod_usina_silenciada, cod_usuario_verifica_silenciado))
                                    print('sem_mensagem_silenciado funcao', sem_mensagem_silenciado)
                            elif tempo_silenciado == 0:
                                if receber_alarme == 0:
                                    await cursor.execute("DELETE FROM machine_learning.telegram_silenciar_bot WHERE cod_equipamento = %s", (cod_equipamento,))
                                    print(f"Coluna 'tempo_silenciado' atualizada para 0. Equipamento {cod_equipamento} com linha apagada.")

                            await asyncio.sleep(1)
                        
                        except Exception as e:
                            print(f"Ocorreu um erro em verificar_e_excluir_linhas_expiradas ao processar o equipamento {cod_equipamento}: {str(e)}")

        except Exception as e:
            print(f"Ocorreu um erro: {e}")
        await asyncio.sleep(50)


codigo_de_alarmes_desejados = set([
    1, 243, 244, 253, 256, 259, 262, 265, 269, 272, 273, 279, 280, 281, 301, 304, 350, 351, 352, 353, 356, 357, 381,
    383, 384, 385, 386, 387, 388, 389, 390, 400, 401, 404, 405, 411, 412, 413, 414, 415, 416, 471, 472, 473, 528, 590,
    591, 592, 593, 594, 595, 596, 597, 598, 599, 600, 602, 603, 604, 611, 615, 616, 617, 631, 635, 637, 638, 657, 658,
    669, 678, 725, 727, 728, 729, 730, 731, 732, 735
])






ultimos_alertas = {}
alertas_enviados_previsao = set()
hora_media_alerta_1 = {}  # Dicionário para mapear o código do equipamento para a hora do alerta igual a 1
hora_media_alerta_saida = {}  # Dicionário para mapear o código do equipamento para a hora do alerta igual a 1


async def enviar_previsao_valor_equipamento_alerta(cod_equipamentos, tabelas, cod_campo_especificados, pool):
#async def enviar_previsao_valor_equipamento_alerta(cod_equipamentos, tabelas, cod_campo_especificados, pool, estado):
    usuarios_bloqueados = set()
    alertas_enviados_acima = set()
    # ultimos_alertas = {}
    # hora_media_alerta_1 = {}

    global ultimos_alertas
    global alertas_enviados_previsao
    global hora_media_alerta_1
    global hora_media_alerta_saida

    while True:
        alertas_por_usina = {}
        nomes_usuarios = []

        tempo_inicial = datetime.now()
        data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')
        print('\n','--------------------------------------------------- inicio enviar_previsao_valor_equipamento_alerta ----------------------------------------------------------------',data_cadastro_formatada,'\n')

        for cod_equipamento in cod_equipamentos:
            try:
                async with pool.acquire() as conn:
                    await conn.ping(reconnect=True)  # Forçar reconexão
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (int(cod_equipamento),))
                        valores_atuais_114 = await cursor.fetchone()

                        if valores_atuais_114 is None:
                            continue

                        await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(cod_equipamento),))
                        valores_atuais = await cursor.fetchone()
                        
                        if valores_atuais is None:
                            continue

                        # Calculando a média dos valores
                        media_valores_114 = sum(valores_atuais_114) / len(valores_atuais_114)

                        await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5, data_cadastro FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 114", (int(cod_equipamento),))
                        result = await cursor.fetchone()

                        if result is None:
                            continue

                        valores_atuais_114 = result[:-1]
                        data_cadastro_consecutivas = result[-1]

                        await cursor.execute("SELECT data_cadastro FROM sup_geral.leituras WHERE cod_equipamento = %s ORDER BY data_cadastro DESC LIMIT 1", (int(cod_equipamento),))
                        result = await cursor.fetchone()
                        
                        if result is not None:
                            data_cadastro_leituras = result[0]
                        else:
                            continue

                        if data_cadastro_leituras <= data_cadastro_consecutivas:
                            continue

                        agora = datetime.now()

                        if agora - data_cadastro_consecutivas > timedelta(hours=1):
                            continue

                #        print('equipamento',cod_equipamento,'na previsao')

                        coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(cod_equipamento), pool)
                        previsoes, alerta_abaixo, alerta_acima = await fazer_previsao_sempre_alerta(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento), pool)

                        
                        if isinstance(previsoes, (list, tuple)):
                            # Ajusta previsões acima de 100 para 100
                            previsoes = [min(previsao, 100) for previsao in previsoes]
                            # Ajusta previsões menores que 0 para 0
                            previsoes = [max(previsao, 0) for previsao in previsoes]


                        if cod_equipamento not in ultimos_alertas:
                            ultimos_alertas[cod_equipamento] = []
                        ultimos_alertas[cod_equipamento].append(int(alerta_abaixo or alerta_acima))
                        if len(ultimos_alertas[cod_equipamento]) > 5:
                            ultimos_alertas[cod_equipamento].pop(0)
                        
                    #    print('\ncod_equipamento ',cod_equipamento,' ultimos_alertas previsao ',ultimos_alertas[cod_equipamento],'\n valores_atuais_114 ',valores_atuais_114,'\n previsoes ',previsoes)

                        if len(ultimos_alertas[cod_equipamento]) == 5:
                            media_alerta = sum(ultimos_alertas[cod_equipamento]) / len(ultimos_alertas[cod_equipamento])
                            
                            if media_alerta == 1:
                                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(cod_equipamento),))
                                valores_atuais = await cursor.fetchone()

                                coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(cod_equipamento), pool)
                                previsoes, alerta_abaixo, alerta_acima = await fazer_previsao_sempre_alerta(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento), pool)

                                if isinstance(previsoes, (list, tuple)):
                                    # Ajusta previsões acima de 100 para 100
                                    previsoes = [min(previsao, 100) for previsao in previsoes]
                                    # Ajusta previsões menores que 0 para 0
                                    previsoes = [max(previsao, 0) for previsao in previsoes]
                                    
                            
                                # Calculando a média dos valores
                                media_valores_114 = sum(valores_atuais_114) / len(valores_atuais_114)

                                await cursor.execute("""
                                    SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                """, (cod_equipamento,))
                                alarmes_ativos = await cursor.fetchall()

                                valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                valores_previstos_str = ', '.join(map(str, previsoes))
                                alarmes_ativos_str = ', '.join(map(str, alarmes_ativos))

                                if cod_equipamento not in hora_media_alerta_1:
                                    hora_media_alerta_1[cod_equipamento] = datetime.now()
                                    
                                #    print(cod_equipamento, 'entrou em alerta e nao tem a hora, escrevendo a hora, veja se parou de escrever na tabela valores zerados')
                                    await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                    result = await cursor.fetchone()
                                    if result is not None:
                                        nome, cod_usina, cod_usuario = result

                                        await cursor.execute("""
                                            INSERT INTO machine_learning.valores_previsao 
                                            (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, previsao, valores_reais, valores_previstos, alarmes) 
                                            VALUES (%s, %s, NOW(), %s, NULL, 1, %s, %s, %s)
                                        """, (cod_equipamento, cod_usina, hora_media_alerta_1[cod_equipamento], valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
                                        await conn.commit()

                                elif cod_equipamento in hora_media_alerta_1:
                                    hora_previsao = hora_media_alerta_1[cod_equipamento]
                                #    print(cod_equipamento, 'entrou em alerta e ja tem a data alerta, veja se parou de escrever na tabela valores zerados')
                                    await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                    result = await cursor.fetchone()
                                    if result is not None:
                                        nome, cod_usina, cod_usuario = result
                                        await cursor.execute("""
                                            INSERT INTO machine_learning.valores_previsao 
                                            (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, previsao, valores_reais, valores_previstos, alarmes) 
                                            VALUES (%s, %s, NOW(), %s, NULL, 1, %s, %s, %s)
                                        """, (cod_equipamento, cod_usina, hora_previsao, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
                                        await conn.commit()
                                    
                                if cod_equipamento not in alertas_enviados_previsao:

                                    # valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                    # valores_previstos_str = ', '.join(map(str, previsoes))
                                    # equipamentos_str = f'\n\nValores Atuais:       {valores_atuais_str} \nValores Previstos:  {valores_previstos_str}'

                                    alertas_enviados_previsao.add(cod_equipamento)
                
                                    await cursor.execute("""
                                    UPDATE machine_learning.leituras_consecutivas
                                    SET alerta = 1
                                    WHERE cod_equipamento = %s
                                    """, (int(cod_equipamento),))
                                    await conn.commit()

                                    print('alerta de previsao adicionado',cod_equipamento)

                                    # Verificando se a média é menor que 50, se for, continue para a próxima iteração
                                    if media_valores_114 < 50:
                                        if cod_usina not in alertas_por_usina:
                                            alertas_por_usina[cod_usina] = []
                                        print('cod equipamento com valores menores que 50, nao continuar codigo', cod_equipamento,' media_valores_114',media_valores_114)
                                        continue

                                    mensagem = ''
                                    mensagem_grupo = ''
                                    # if alerta_abaixo:
                                    #     await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                    #     result = await cursor.fetchone()
                                    #     if result is not None:
                                    #         nome, cod_usina, cod_usuario = result
                                    #         mensagem = f"({nome}) - Load Speed abaixo do previsto {equipamentos_str}\n\n"
                                    #         mensagem_grupo = f"Equipamento: {cod_equipamento} ({nome}): {equipamentos_str}\n\nAlerta: Load Speed abaixo do previsto\n\n"

                                    if alerta_acima:            
                                #    elif alerta_acima:

                                        valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                        valores_previstos_str = ', '.join(map(str, previsoes))
                                        equipamentos_str = f'\n\nValores Atuais:       {valores_atuais_str} \nValores Previstos:  {valores_previstos_str}'

                                        await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                        result = await cursor.fetchone()
                                        if result is not None:
                                            nome, cod_usina, cod_usuario = result
                                            mensagem = f"({nome}) - Load Speed acima do previsto {equipamentos_str}\n\n"
                                            mensagem_grupo = f"Equipamento: {cod_equipamento} ({nome}): {equipamentos_str}\n\nAlerta: Load Speed acima do previsto\n\n"

                                    if cod_usina not in alertas_por_usina:
                                        alertas_por_usina[cod_usina] = []
                                    alertas_por_usina[cod_usina].append(mensagem)
                                    alertas_por_usina[cod_usina].append(mensagem_grupo)

                            elif media_alerta == 0 and cod_equipamento in alertas_enviados_previsao:
                                print('equipamento -',cod_equipamento,'em alerta 0, valores 114 previsao depois com',valores_atuais_114)

                                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(cod_equipamento),))
                                valores_atuais = await cursor.fetchone()
                                    
                                coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(cod_equipamento), pool)
                                previsoes, alerta_abaixo, alerta_acima = await fazer_previsao_sempre_alerta(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento), pool)

                                if isinstance(previsoes, (list, tuple)):
                                    # Ajusta previsões acima de 100 para 100
                                    previsoes = [min(previsao, 100) for previsao in previsoes]
                                    # Ajusta previsões menores que 0 para 0
                                    previsoes = [max(previsao, 0) for previsao in previsoes]
                                            
                            
                                await cursor.execute("""
                                    SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                """, (cod_equipamento,))
                                alarmes_ativos = await cursor.fetchall()
                            #    print('alarmes ativos previsao', cod_equipamento ,valores_atuais_114)

                                valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                valores_previstos_str = ', '.join(map(str, previsoes))
                                alarmes_ativos_str = ', '.join(map(str, alarmes_ativos))

                                hora_previsao = hora_media_alerta_1[cod_equipamento]
                                await cursor.execute("""
                                    INSERT INTO machine_learning.valores_previsao 
                                    (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, previsao, valores_reais, valores_previstos, alarmes) 
                                    VALUES (%s, %s, NOW(), %s, NULL, 1, %s, %s, %s)
                                """, (cod_equipamento, cod_usina, hora_previsao, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
                                await conn.commit()

                                if cod_equipamento not in hora_media_alerta_saida: 
                                    hora_media_alerta_saida[cod_equipamento] = datetime.now()
                                    await cursor.execute("""
                                        SELECT data_cadastro_previsto, data_cadastro_quebra 
                                        FROM machine_learning.relatorio_quebras 
                                        WHERE cod_equipamento = %s
                                        ORDER BY id DESC 
                                        LIMIT 1
                                    """, (cod_equipamento,))
                                    data_cadastro_previsto, data_cadastro_quebra = await cursor.fetchone()
                                #    print(cod_equipamento,'escrevendo na coluna data_cadastro_previsto_saida ')

                                    if data_cadastro_quebra is None:
                                #        print(cod_equipamento,' - data_cadastro_quebra e none',data_cadastro_quebra,'data_cadastro_previsto',data_cadastro_previsto)

                                        await cursor.execute("""
                                            UPDATE machine_learning.relatorio_quebras SET data_cadastro_previsto_saida = %s WHERE cod_equipamento = %s ORDER BY data_cadastro_previsto DESC LIMIT 1
                                        """, (hora_media_alerta_saida[cod_equipamento], cod_equipamento))
                                        await conn.commit()

                                        await cursor.execute("""
                                            UPDATE machine_learning.log_relatorio_quebras SET data_cadastro_previsto_saida = %s WHERE cod_equipamento = %s ORDER BY data_cadastro_previsto DESC LIMIT 1
                                        """, (hora_media_alerta_saida[cod_equipamento], cod_equipamento))
                                        await conn.commit()
                                    else:
                                        pass

#                                if all(value == 0 for value in valores_atuais_114[:-1]):
#                                if any(value == 0 for value in valores_atuais_114[:-1]):

                                # Verificar se pelo menos três valores são iguais a zero
                            #    if sum(value == 0 for value in valores_atuais_114[:-1]) >= 3:
                                if sum(1 for value in valores_atuais_114[:-1] if value == 0) >= 3:

                                    print(cod_equipamento,'alerta removido, alerta em 0 de previsao, zerou valores',all(value == 0 for value in valores_atuais_114[:-1]))
                                #    print('equipamento - ',cod_equipamento,' valores 114 previsao depois com',valores_atuais_114)

                                    await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(cod_equipamento),))
                                    valores_atuais = await cursor.fetchone()

                                    coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(cod_equipamento), pool)
                                    previsoes, alerta_abaixo, alerta_acima = await fazer_previsao_sempre_alerta(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento), pool)

                                    if isinstance(previsoes, (list, tuple)):
                                        # Ajusta previsões acima de 100 para 100
                                        previsoes = [min(previsao, 100) for previsao in previsoes]
                                        # Ajusta previsões menores que 0 para 0
                                        previsoes = [max(previsao, 0) for previsao in previsoes]
                                                    
                            
                                    await cursor.execute("""
                                        SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                    """, (cod_equipamento,))
                                    alarmes_ativos = await cursor.fetchall()
                                    print('alarmes_ativos para previsao',alarmes_ativos)

                                    valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                    valores_previstos_str = ', '.join(map(str, previsoes))
                                    alarmes_ativos_str = ', '.join(map(str, alarmes_ativos))



                                    # Extrair códigos dos alarmes_ativos_str
                                    alarmes_ativos_extracao = re.findall(r'\d+', alarmes_ativos_str)
                                    alarmes_ativos_list = list(map(int, alarmes_ativos_extracao))

                                    # Verificar alarmes correspondentes
                                    alarmes_correspondentes = [alarme for alarme in alarmes_ativos_list if alarme in codigos_alarmes_desejados]
                                    alarme_encontrado = len(alarmes_correspondentes) > 0
                                    alarme_verificado = 1 if alarme_encontrado else 0

                                    # Mostrar alarmes correspondentes se alarme_verificado for 1
                                    if alarme_verificado == 1:
                                #        print('cod_equipamento', cod_equipamento, 'alarme_verificado', alarme_verificado)
                                #        print('Alarmes correspondentes:', alarmes_correspondentes)
                                        pass

                                    # Definir o valor de `data_cadastro_quebra`
                                    data_cadastro_quebra = 'NOW()' if alarme_verificado == 1 else 'NULL'

                                #    hora_previsao = hora_media_alerta_1[cod_equipamento]

                                    # Inserir dados na tabela `valores_previsao`
                                    await cursor.execute(f"""
                                        INSERT INTO machine_learning.valores_previsao 
                                        (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, previsao, valores_reais, valores_previstos, alarmes) 
                                        VALUES (%s, %s, NOW(), %s, {data_cadastro_quebra}, 1, %s, %s, %s)
                                    """, (cod_equipamento, cod_usina, hora_previsao, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))

                                    await conn.commit()
                                    
                                    # hora_previsao = hora_media_alerta_1[cod_equipamento]
                                    # await cursor.execute("""
                                    #     INSERT INTO machine_learning.valores_previsao 
                                    #     (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, previsao, valores_reais, valores_previstos, alarmes) 
                                    #     VALUES (%s, %s, NOW(), %s, NOW(), 1, %s, %s, %s)
                                    # """, (cod_equipamento, cod_usina, hora_previsao, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
                                    # await conn.commit()

                                    await cursor.execute("""
                                    UPDATE machine_learning.leituras_consecutivas
                                    SET alerta = 0
                                    WHERE cod_equipamento = %s
                                    """, (int(cod_equipamento),))
                                    await conn.commit()
                                    print('escrevendo na coluna alerta 0 para previsao')

                                    # await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                    # result = await cursor.fetchone()
                                    
                                    # print('result para enviar nome da usina do alerta previsao',result)
                                    
                                    # if result is not None:
                                    #     nome, cod_usina, cod_usuario = result
                                    #     mensagem_previsao = f'🟢 Usina: {cod_usina} - {nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n'  + ''.join([msg for msg in mensagens if 'Alerta' in msg]) + '\n\nAlerta de previsao removido'
                                    #     print('mensagem_previsao',mensagem_previsao)
                                    #     id_grupo = await id_chat_grupo(pool)          
                                    #     await bot.send_message(id_grupo, mensagem_previsao, parse_mode='HTML')

                                    hora_previsao = hora_media_alerta_1.pop(cod_equipamento, None)
                                    alertas_enviados_previsao.remove(cod_equipamento)
                                    
                                    nomes_usuarios.clear()
                                    
                                    print('alerta removido de previsao',cod_equipamento)


            except Exception as e:
                print(f"Ocorreu um erro em enviar_previsao_valor_equipamento_alerta o equipamento {cod_equipamento}:  {e}")

        for cod_usina, mensagens in alertas_por_usina.items():

            # Verifica se qualquer mensagem não está vazia
            if not any(mensagem.strip() for mensagem in mensagens):
                print('usina com equipamento sem mensagem, ou seja, valor menor que 50% ',cod_usina)
                continue
            
            async with pool.acquire() as conn:
                await conn.ping(reconnect=True)  # Forçar reconexão
                async with conn.cursor() as cursor:
                    # Buscar o nome e o código do modelo de funcionamento da usina
                    await cursor.execute("SELECT nome, cod_modelo_funcionamento FROM sup_geral.usinas WHERE codigo = %s", (cod_usina,))
                    result = await cursor.fetchone()
                    if result:
                        nome_usina, cod_modelo_funcionamento = result
                        print('equipamento', cod_equipamento, 'nome usina', nome_usina, 'o código de funcionamento é:', cod_modelo_funcionamento,' esta dentro do loop da previsao')

                        # Buscar todos os cod_usuario associados à usina
                        await cursor.execute("SELECT cod_usuario FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario != 0 AND cod_usina = %s", (cod_usina,))
                        cod_usuarios = await cursor.fetchall()

                        if cod_usuarios:
                            nomes_usuarios = []

                            for cod_usuario_tuple in cod_usuarios:
                                cod_usuario = cod_usuario_tuple[0]

                                # Verificar se o usuário não está silenciado
                                if (cod_usina, cod_usuario) not in sem_mensagem_silenciado:

                                    # Verificar se a usina está ativa para o usuário
                                    await cursor.execute("SELECT ativo FROM machine_learning.usinas_usuario WHERE cod_usina = %s AND cod_usuario = %s", (cod_usina, cod_usuario))
                                    usina_ativa_row = await cursor.fetchone()
                                #    print("(loop 1 'previsao') A usina ",cod_usina, " Esta ativa? ",usina_ativa_row,' para o usuario ',cod_usuario)

                                    if usina_ativa_row and usina_ativa_row[0] == 1:
                                        await cursor.execute("SELECT id_telegram, usuario, ativo, telefone FROM machine_learning.usuarios_telegram WHERE cod_usuario = %s", (cod_usuario,))
                                        result = await cursor.fetchone()
                                        
                                        if result:
                                            id_telegram, nome_usuario, ativo, telefone = result
                                            if ativo == 1:
                                                try:
                                                    nomes_usuarios.append(nome_usuario)
                                                    botao_silenciar_usina = InlineKeyboardButton("Silenciar Usina", callback_data=f'silenciar_usina_{cod_usina}')
                                                    botao_receber_alarmes = InlineKeyboardButton("Receber Alarmes", callback_data=f'receber_alarmes_{cod_usina}')
                    
                                                    keyboard = InlineKeyboardMarkup().row(botao_silenciar_usina, botao_receber_alarmes)
                    
                                                    mensagem_final = f'🟡 <b>ALERTA!</b> \n\n{nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n' + ''.join([msg for msg in mensagens if 'Alerta' not in msg])
                                                    await bot.send_message(id_telegram, mensagem_final, reply_markup=keyboard, parse_mode='HTML')

                                                    print('inicio do payload previsao loop')

                                                    # Se o telefone é uma tupla, pega o primeiro elemento
                                                    if isinstance(telefone, tuple):
                                                        telefone = telefone[0]
                                                        
                                                    # Extrai o valor da tupla
                                                    if telefone:  # Verifica se telefone não é None
                                                        telefone = telefone[0]  # Pega o primeiro elemento da tupla
                                                        telefone_formatado = telefone.strip().replace(" ", "").replace("-", "")  # Formata o telefone
                                                        if not telefone_formatado.startswith("+"):
                                                            telefone_formatado = f"+{telefone_formatado}"  # Adiciona o "+" no início, se necessário

                                                        # Monta a mensagem
                                                        mensagem_final_wtss = (
                                                            f"🟡 **ALERTA!**\n\n"
                                                            f"Usina: {cod_usina} - {nome_usina}\n\n"
                                                #            f"Ir para usina: https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}\n\n"
                                                            + ''.join([msg for msg in mensagens if 'Alerta' in msg])
                                                        )

                                                        # Monta o payload
                                                        payload = {
                                                            "number": telefone_formatado,
                                                            "textMessage": {
                                                                "text": mensagem_final_wtss
                                                            }
                                                        }
                                                    else:
                                                        print("Nenhum telefone encontrado para o usuário.")

                                                    start_chat(payload)  # Chama a função para enviar via WhatsApp
                                                    print('payload',payload)
                                                    print('fim do payload previsao loop')


                                                    if cod_usuario in usuarios_bloqueados:
                                                        usuarios_bloqueados.remove(cod_usuario)
                                                        id_grupo = await id_chat_grupo(pool)
                                                        if id_grupo is not None:
                                                            await bot.send_message(id_grupo, f"🚫 🟢 O bot foi desbloqueado pelo usuário {nome_usuario} ({cod_usuario})")
                                                        await cursor.execute("UPDATE machine_learning.usuarios_telegram SET bloqueado = 0 WHERE cod_usuario = %s", (cod_usuario,))
                                                        await conn.commit()
                                                                                                                        
                                                except BotBlocked:
                                                    if cod_usuario not in usuarios_bloqueados:
                                                        usuarios_bloqueados.add(cod_usuario)
                                                        id_grupo = await id_chat_grupo(pool)
                                                        if id_grupo is not None:
                                                            await bot.send_message(id_grupo, f"🚫 O bot foi bloqueado pelo usuário {nome_usuario} ({cod_usuario})")
                                                        await cursor.execute("UPDATE machine_learning.usuarios_telegram SET bloqueado = 1 WHERE cod_usuario = %s", (cod_usuario,))
                                                        await conn.commit()

                                        else:
                                            continue
                                        
                            nomes_usuarios_str = ', '.join(nomes_usuarios)
                            id_grupo = await id_chat_grupo(pool)
                            
                            botao_silenciar_usina = InlineKeyboardButton("Silenciar Usina", callback_data=f'silenciar_usina_{cod_usina}')
                            botao_receber_alarmes = InlineKeyboardButton("Receber Alarmes", callback_data=f'receber_alarmes_{cod_usina}')
                            keyboard = InlineKeyboardMarkup().row(botao_silenciar_usina, botao_receber_alarmes)
                                                        
                            mensagem_final_grupo = f'🟡 <b>Enviada para {nomes_usuarios_str}!</b> \n\nUsina: {cod_usina} - {nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n'  + ''.join([msg for msg in mensagens if 'Alerta' in msg])
                        #    await bot.send_message(id_grupo, mensagem_final_grupo, parse_mode='HTML', reply_markup=main_keyboard)


                            print('inicio do payload previsao')

                            # Executa a consulta no banco
                            await cursor.execute("SELECT telefone FROM machine_learning.usuarios_telegram WHERE cod_usuario = 374")
                            # Retorna um único registro como tupla
                            telefone = await cursor.fetchone()

                            # Extrai o valor da tupla
                            if telefone:  # Verifica se telefone não é None
                                telefone = telefone[0]  # Pega o primeiro elemento da tupla
                                telefone_formatado = telefone.strip().replace(" ", "").replace("-", "")  # Formata o telefone
                                if not telefone_formatado.startswith("+"):
                                    telefone_formatado = f"+{telefone_formatado}"  # Adiciona o "+" no início, se necessário

                                # Monta a mensagem
                                mensagem_final_wtss = (
                                    f"**Enviada para {nomes_usuarios_str}!**\n\n"
                                    f"Usina: {cod_usina} - {nome_usina}\n\n"
                                    f"Ir para usina: https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}\n\n"
                                    + ''.join([msg for msg in mensagens if 'Alerta' in msg])
                                )

                                # Monta o payload
                                payload = {
                                    "number": telefone_formatado,
                                    "textMessage": {
                                        "text": mensagem_final_wtss
                                    }
                                }
                            else:
                                print("Nenhum telefone encontrado para o usuário.")

                            start_chat(payload)  # Chama a função para enviar via WhatsApp
                            print('payload',payload)
                            print('fim do payload previsao')
                                                    
                            await bot.send_message(id_grupo, mensagem_final_grupo, reply_markup=keyboard, parse_mode='HTML')
                            nomes_usuarios.clear()

                            sys.stdout.flush()

        tempo_final = datetime.now()
        data_cadastro_formatada_final = tempo_final.strftime('%d-%m-%Y %H:%M')

        # Calcular o tempo de execução
        tempo_execucao = tempo_final - tempo_inicial

        # Formatando a diferença de tempo
        segundos = tempo_execucao.total_seconds()
        horas = int(segundos // 3600)
        minutos = int((segundos % 3600) // 60)
        segundos_restantes = int(segundos % 60)

        # Exibindo o tempo total de execução
        print('\n','--------------------------------------------------- fim enviar_previsao_valor_equipamento_alerta ----------------------------------------------------------------', data_cadastro_formatada_final, '\n')
        print(f'Tempo de execução do enviar_previsao_valor_equipamento_alerta: {horas} horas, {minutos} minutos, {segundos_restantes} segundos\n')

        await asyncio.sleep(180)




ultimos_valores = {}
alertas_enviados = set()
hora_media_alerta_1_80 = {}  # Dicionário para mapear o código do equipamento para a hora do alerta igual a 1
hora_media_alerta_1_100 = {}  # Dicionário para mapear o código do equipamento para a hora do alerta igual a 1
hora_media_alerta_80_100_saida = {}  # Dicionário para mapear o código do equipamento para a hora do alerta igual a 1

async def enviar_alerta_80_100(cod_equipamentos, tabelas, cod_campo_especificados, pool):


    global ultimos_valores
    global alertas_enviados
    global hora_media_alerta_1_80
    global hora_media_alerta_1_100
    global hora_media_alerta_80_100_saida

    usuarios_bloqueados = set()
    alertas_enviados_acima = set()
    
    while True:
        alertas_por_usina = {}

        tempo_inicial = datetime.now()
        data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')
        print('\n','------------------------------------------------------------- inicio enviar_alerta_80_100 --------------------------------------------------------------------------', data_cadastro_formatada,'\n')


        for cod_equipamento in cod_equipamentos:
            try:
                async with pool.acquire() as conn:
                    await conn.ping(reconnect=True)  # Forçar reconexão
                    async with conn.cursor() as cursor:
                        # Busca os valores da tabela leituras_consecutivas para o cod_campo 114
                        await cursor.execute(
                            "SELECT valor_1, valor_2, valor_3, valor_4, valor_5, data_cadastro FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 114",
                            (int(cod_equipamento),)
                        )
                        result = await cursor.fetchone()

                        if result is None:
                            continue

                        valores_atuais_114 = result[:-1]  # Os cinco valores
                        data_cadastro_consecutivas = result[-1]

                        if valores_atuais_114 is None:
                            continue
                        
                        # Verifica se os dados são novos comparados à última leitura
                        await cursor.execute(
                            "SELECT data_cadastro FROM sup_geral.leituras WHERE cod_equipamento = %s ORDER BY data_cadastro DESC LIMIT 1",
                            (int(cod_equipamento),)
                        )
                        result = await cursor.fetchone()
                        if result is not None:
                            data_cadastro_leituras = result[0]
                        else:
                            continue

#                        if data_cadastro_leituras == data_cadastro_consecutivas:
#                            continue

                        if data_cadastro_leituras <= data_cadastro_consecutivas:
                            continue

                        agora = datetime.now()

                        # Ignora leituras muito antigas
                        if agora - data_cadastro_consecutivas > timedelta(hours=1):
                            continue

                        # Atualiza os últimos 5 valores
                        if cod_equipamento not in ultimos_valores:
                            ultimos_valores[cod_equipamento] = []
                        
                        # Adiciona os novos valores e remove os antigos se ultrapassar 5
                        ultimos_valores[cod_equipamento].extend(valores_atuais_114)
                        if len(ultimos_valores[cod_equipamento]) > 5:
                            ultimos_valores[cod_equipamento] = ultimos_valores[cod_equipamento][-5:]

                        # Só calcula a média se já houver 5 valores
                        if len(ultimos_valores[cod_equipamento]) == 5:
                            media = sum(ultimos_valores[cod_equipamento]) / 5

                            alerta_80 = False
                            alerta_100 = False

                            # Verifica a média, e não os valores individuais
                            if 80 <= media < 100:
                                alerta_80 = True
                                alerta_100 = False
                            elif media == 100:
                                alerta_80 = False
                                alerta_100 = True

                                
                            if media > 0.1 and media < 80 and not cod_equipamento in alertas_enviados:

                            #    if cod_equipamento not in hora_media_alerta_1_80: 
                                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(cod_equipamento),))
                                valores_atuais = await cursor.fetchone()
                                    
                                coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(cod_equipamento), pool)
                                previsoes, alerta_abaixo, alerta_acima = await fazer_previsao_sempre_alerta(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento), pool)

                                await cursor.execute("""
                                    SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                """, (cod_equipamento,))
                                alarmes_ativos = await cursor.fetchall()

                                await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                result = await cursor.fetchone()
                                if result is not None:
                                    nome, cod_usina, cod_usuario = result

                                    valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                    valores_previstos_str = ', '.join(map(str, previsoes))
                                    alarmes_ativos_str = ', '.join(map(str, alarmes_ativos))

                                    await cursor.execute("""
                                        INSERT INTO machine_learning.valores_previsao 
                                        (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, alerta_80, valores_reais, valores_previstos, alarmes) 
                                        VALUES (%s, %s, NOW(), NULL, NULL, 0, %s, %s, %s)
                                    """, (cod_equipamento, cod_usina, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
                                    await conn.commit()


                                    # Extrair códigos dos alarmes_ativos_str
                                    alarmes_ativos_extracao = re.findall(r'\d+', alarmes_ativos_str)
                                    alarmes_ativos_list = list(map(int, alarmes_ativos_extracao))

                                    # Verificar alarmes correspondentes
                                    alarmes_correspondentes = [alarme for alarme in alarmes_ativos_list if alarme in codigos_alarmes_desejados]
                                    alarme_encontrado = len(alarmes_correspondentes) > 0
                                    alarme_verificado = 1 if alarme_encontrado else 0

                                    # Mostrar alarmes correspondentes se alarme_verificado for 1
                                    if alarme_verificado == 1:
                                #        print('cod_equipamento', cod_equipamento, 'alarme_verificado', alarme_verificado)
                                #        print('Alarmes correspondentes:', alarmes_correspondentes)
                                        pass
                                    # await cursor.execute("""
                                    #     INSERT INTO machine_learning.valores_previsao 
                                    #     (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, alerta_80, valores_reais, valores_previstos, alarmes, alarme_verificado) 
                                    #     VALUES (%s, %s, NOW(), NULL, NULL, 0, %s, %s, %s, %s)
                                    # """, (cod_equipamento, cod_usina, valores_atuais_str, valores_previstos_str, alarmes_ativos_str, alarme_verificado))


                            elif media >= 80 and media < 100:

                                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(cod_equipamento),))
                                valores_atuais = await cursor.fetchone()
                                
                                coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(cod_equipamento), pool)
                                previsoes, alerta_abaixo, alerta_acima = await fazer_previsao_sempre_alerta(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento), pool)

                                if isinstance(previsoes, (list, tuple)):
                                    # Ajusta previsões acima de 100 para 100
                                    previsoes = [min(previsao, 100) for previsao in previsoes]
                                    # Ajusta previsões menores que 0 para 0
                                    previsoes = [max(previsao, 0) for previsao in previsoes]
                                            
                            
                                await cursor.execute("""
                                    SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                """, (cod_equipamento,))
                                alarmes_ativos = await cursor.fetchall()
                            #    print('equipamento: ',cod_equipamento,' alarmes_ativos 80%: ',alarmes_ativos)

                                valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                valores_previstos_str = ', '.join(map(str, previsoes))
                                alarmes_ativos_str = ', '.join(map(str, alarmes_ativos))

                                # Extrair códigos dos alarmes_ativos_str
                                alarmes_ativos_extracao = re.findall(r'\d+', alarmes_ativos_str)
                                alarmes_ativos_list = list(map(int, alarmes_ativos_extracao))

                                # Verificar alarmes correspondentes
                                alarmes_correspondentes = [alarme for alarme in alarmes_ativos_list if alarme in codigos_alarmes_desejados]
                                alarme_encontrado = len(alarmes_correspondentes) > 0
                                alarme_verificado = 1 if alarme_encontrado else 0

                                # Mostrar alarmes correspondentes se alarme_verificado for 1
                                if alarme_verificado == 1:
                            #        print('cod_equipamento', cod_equipamento, 'alarme_verificado', alarme_verificado)
                            #        print('Alarmes correspondentes em 80%:', alarmes_correspondentes)
                                    pass
                                    
                                if cod_equipamento not in hora_media_alerta_1_80: 
                                    hora_media_alerta_1_80[cod_equipamento] = datetime.now()

                                    await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                    result = await cursor.fetchone()
                                    if result is not None:
                                        nome, cod_usina, cod_usuario = result
                                        
                                        await cursor.execute("""
                                            INSERT INTO machine_learning.valores_previsao 
                                            (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, alerta_80, valores_reais, valores_previstos, alarmes) 
                                            VALUES (%s, %s, NOW(), %s, NULL, 1, %s, %s, %s)
                                        """, (cod_equipamento, cod_usina, hora_media_alerta_1_80[cod_equipamento], valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
                                        await conn.commit()

                                elif cod_equipamento in hora_media_alerta_1_80:
                                    hora_previsao = hora_media_alerta_1_80[cod_equipamento]
                                    
                                    await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                    result = await cursor.fetchone()
                                    if result is not None:
                                        nome, cod_usina, cod_usuario = result
                                        
                                        await cursor.execute("""
                                            INSERT INTO machine_learning.valores_previsao 
                                            (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, alerta_80, valores_reais, valores_previstos, alarmes) 
                                            VALUES (%s, %s, NOW(), %s, NULL, 1, %s, %s, %s)
                                        """, (cod_equipamento, cod_usina, hora_previsao, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
                                        await conn.commit()
                                
                                
                                if cod_equipamento not in alertas_enviados:

                                    print(f'alerta adicionado de load speed 80 {cod_equipamento}')
                                    await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(cod_equipamento),))
                                    valores_atuais = await cursor.fetchone()

                                    coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(cod_equipamento), pool)
                                    previsoes, alerta_abaixo, alerta_acima = await fazer_previsao_sempre_alerta(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento), pool)

                                    if isinstance(previsoes, (list, tuple)):
                                        # Ajusta previsões acima de 100 para 100
                                        previsoes = [min(previsao, 100) for previsao in previsoes]
                                        # Ajusta previsões menores que 0 para 0
                                        previsoes = [max(previsao, 0) for previsao in previsoes]
                                                    
                            
                                    valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                    valores_previstos_str = ', '.join(map(str, previsoes))
                                    equipamentos_str = f'\n\nValores Atuais: {valores_atuais_str} \nValores Previstos: {valores_previstos_str}\n\n'
                                    mensagem = ''
                                    mensagem_grupo = ''
                                    if alerta_80:
    #                                    mensagem = f"🟠❗ Equipamento: {cod_equipamento} ({nome})\n\n O load speed está acima de 80%.\n\n"
                                    #    mensagem = f"🟠❗ ({nome}) - O load speed está acima de 80%.\n\n"
                                        await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                        result = await cursor.fetchone()
                                        if result is not None:
                                            nome, cod_usina, cod_usuario = result
                                            mensagem = f"🟠 ❗ ({nome}) - O load speed está acima de 80%.{equipamentos_str}\n\n"
                                            mensagem_grupo = f"🟠 ❗ Equipamento: {cod_equipamento} ({nome}): {equipamentos_str}\n\nAlerta: O load speed está acima de 80%.\n\n"

                                    if cod_usina not in alertas_por_usina:
                                        alertas_por_usina[cod_usina] = []
                                    alertas_por_usina[cod_usina].append(mensagem)
                                    alertas_por_usina[cod_usina].append(mensagem_grupo)

                                    
                                    alertas_enviados.add(cod_equipamento)
                                    alertas_enviados_acima.add(cod_equipamento)

                                    await cursor.execute("""
                                    UPDATE machine_learning.leituras_consecutivas
                                    SET alerta = 1
                                    WHERE cod_equipamento = %s
                                    """, (int(cod_equipamento),))
                                    await conn.commit()

                            elif media == 100:
                                
                                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(cod_equipamento),))
                                valores_atuais = await cursor.fetchone()

                                coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(cod_equipamento), pool)
                                previsoes, alerta_abaixo, alerta_acima = await fazer_previsao_sempre_alerta(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento), pool)


                                if isinstance(previsoes, (list, tuple)):
                                    # Ajusta previsões acima de 100 para 100
                                    previsoes = [min(previsao, 100) for previsao in previsoes]
                                    # Ajusta previsões menores que 0 para 0
                                    previsoes = [max(previsao, 0) for previsao in previsoes]
                                            
                                        
                                await cursor.execute("""
                                    SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                """, (cod_equipamento,))
                                alarmes_ativos = await cursor.fetchall()

                                valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                valores_previstos_str = ', '.join(map(str, previsoes))
                                alarmes_ativos_str = ', '.join(map(str, alarmes_ativos))

                                # Extrair códigos dos alarmes_ativos_str
                                alarmes_ativos_extracao = re.findall(r'\d+', alarmes_ativos_str)
                                alarmes_ativos_list = list(map(int, alarmes_ativos_extracao))

                                # Verificar alarmes correspondentes
                                alarmes_correspondentes = [alarme for alarme in alarmes_ativos_list if alarme in codigos_alarmes_desejados]
                                alarme_encontrado = len(alarmes_correspondentes) > 0
                                alarme_verificado = 1 if alarme_encontrado else 0

                                # Mostrar alarmes correspondentes se alarme_verificado for 1
                                if alarme_verificado == 1:
                            #        print('cod_equipamento', cod_equipamento, 'alarme_verificado', alarme_verificado)
                            #        print('Alarmes correspondentes em 100%:', alarmes_correspondentes)
                                    pass
                                    
                                if cod_equipamento not in hora_media_alerta_1_100: 
                                    hora_media_alerta_1_100[cod_equipamento] = datetime.now()

                                    await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                    result = await cursor.fetchone()
                                    if result is not None:
                                        nome, cod_usina, cod_usuario = result
                                        
                                        await cursor.execute("""
                                            INSERT INTO machine_learning.valores_previsao 
                                            (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, alerta_100, valores_reais, valores_previstos, alarmes) 
                                            VALUES (%s, %s, NOW(), %s, NULL, 1, %s, %s, %s)
                                        """, (cod_equipamento, cod_usina, hora_media_alerta_1_100[cod_equipamento], valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
                                        await conn.commit()
                                    
                                elif cod_equipamento in hora_media_alerta_1_100:
                                    hora_previsao = hora_media_alerta_1_100[cod_equipamento]
                                    
                                    await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                    result = await cursor.fetchone()
                                    if result is not None:
                                        nome, cod_usina, cod_usuario = result
                                        
                                        await cursor.execute("""
                                            INSERT INTO machine_learning.valores_previsao 
                                            (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, alerta_100, valores_reais, valores_previstos, alarmes) 
                                            VALUES (%s, %s, NOW(), %s, NULL, 1, %s, %s, %s)
                                        """, (cod_equipamento, cod_usina, hora_previsao, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
                                        await conn.commit()

                                if cod_equipamento not in alertas_enviados:

                                    print(f'alerta adicionado de load speed 100 {cod_equipamento}')
                                    await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(cod_equipamento),))
                                    valores_atuais = await cursor.fetchone()

                                    coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(cod_equipamento), pool)
                                    previsoes, alerta_abaixo, alerta_acima = await fazer_previsao_sempre_alerta(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento), pool)

                                    if isinstance(previsoes, (list, tuple)):
                                        # Ajusta previsões acima de 100 para 100
                                        previsoes = [min(previsao, 100) for previsao in previsoes]
                                        # Ajusta previsões menores que 0 para 0
                                        previsoes = [max(previsao, 0) for previsao in previsoes]
                                                    
                                        
                                    valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                    valores_previstos_str = ', '.join(map(str, previsoes))
                                    equipamentos_str = f'\n\nValores Atuais: {valores_atuais_str} \nValores Previstos: {valores_previstos_str}\n\n'
                                    mensagem = ''
                                    mensagem_grupo = ''
                        #            if alerta_80:
                        #                mensagem = f"🟠❗ Equipamento: {cod_equipamento} ({nome})\n\n O load speed está acima de 80%.\n\n"
                        #                mensagem_grupo = f"❗ Equipamento: {cod_equipamento} ({nome}): {equipamentos_str}\n\n O load speed está acima de 80%.\n\n"
                                    if alerta_100:

                                        await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                        result = await cursor.fetchone()
                                        if result is not None:
                                            nome, cod_usina, cod_usuario = result
        #                                    mensagem = f"🔴 ‼️ Equipamento: {cod_equipamento} ({nome})\n\n O load speed esta em 100%.\n\n"
        #                                    mensagem = f"🔴 ‼️ ({nome}) - O load speed esta em 100%.\n\n"
                                            mensagem = f"🔴 ‼️ ({nome}) - O load speed está em 100%.{equipamentos_str}\n\n"
                                            mensagem_grupo = f"🔴‼️ Equipamento: {cod_equipamento} ({nome}): {equipamentos_str}\n\nAlerta: O load speed esta em 100%.\n\n"

                                    if cod_usina not in alertas_por_usina:
                                        alertas_por_usina[cod_usina] = []
                                    alertas_por_usina[cod_usina].append(mensagem)
                                    alertas_por_usina[cod_usina].append(mensagem_grupo)
                            #        print('alertas_por_usina[cod_usina] 100%',alertas_por_usina[cod_usina])

                                    
                                    alertas_enviados.add(cod_equipamento)
                                    alertas_enviados_acima.add(cod_equipamento)

                                    await cursor.execute("""
                                    UPDATE machine_learning.leituras_consecutivas
                                    SET alerta = 1
                                    WHERE cod_equipamento = %s
                                    """, (int(cod_equipamento),))
                                    await conn.commit()

                            elif media < 80 and cod_equipamento in alertas_enviados:

                                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(cod_equipamento),))
                                valores_atuais = await cursor.fetchone()

                                coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(cod_equipamento), pool)
                                previsoes, alerta_abaixo, alerta_acima = await fazer_previsao_sempre_alerta(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento), pool)


                                if isinstance(previsoes, (list, tuple)):
                                    # Ajusta previsões acima de 100 para 100
                                    previsoes = [min(previsao, 100) for previsao in previsoes]
                                    # Ajusta previsões menores que 0 para 0
                                    previsoes = [max(previsao, 0) for previsao in previsoes]
                                    
                                        
                                await cursor.execute("""
                                    SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                """, (cod_equipamento,))
                                alarmes_ativos = await cursor.fetchall()
                           #     print('alarmes_ativos para %: ',cod_equipamento ,alarmes_ativos)

                                valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                valores_previstos_str = ', '.join(map(str, previsoes))
                                alarmes_ativos_str = ', '.join(map(str, alarmes_ativos))
                                
                                if cod_equipamento in hora_media_alerta_1_100: 

                                    hora_previsao = hora_media_alerta_1_100[cod_equipamento]
                                    await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                    result = await cursor.fetchone()
                                #    print('escrevendo na tabela para alerta 100',cod_equipamento)
                                    if result is not None:
                                        nome, cod_usina, cod_usuario = result
                                        await cursor.execute("""
                                            INSERT INTO machine_learning.valores_previsao 
                                            (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra,alerta_80, alerta_100, valores_reais, valores_previstos, alarmes) 
                                            VALUES (%s, %s, NOW(), %s, NULL, 0, 1, %s, %s, %s)
                                        """, (cod_equipamento, cod_usina, hora_previsao, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
                                        await conn.commit()

                                if cod_equipamento in hora_media_alerta_1_80: 

                                    hora_previsao = hora_media_alerta_1_80[cod_equipamento]
                                    await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                    result = await cursor.fetchone()
                            #        print('escrevendo na tabela para alerta 80',cod_equipamento)
                                    if result is not None:
                                        nome, cod_usina, cod_usuario = result
                                        await cursor.execute("""
                                            INSERT INTO machine_learning.valores_previsao 
                                            (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra,alerta_80, alerta_100, valores_reais, valores_previstos, alarmes) 
                                            VALUES (%s, %s, NOW(), %s, NULL, 1, 0, %s, %s, %s)
                                        """, (cod_equipamento, cod_usina, hora_previsao, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
                                        await conn.commit()

                            #    print(cod_equipamento,'alerta quase removido de 80%, alerta em 0, esperando zerar valores',all(value == 0 for value in valores_atuais_114[:-1]))

                                # escrever na coluna data_cadastro_previsto_saida
                                if cod_equipamento not in hora_media_alerta_80_100_saida: 
                                    hora_media_alerta_80_100_saida[cod_equipamento] = datetime.now()
                                    await cursor.execute("""
                                        SELECT data_cadastro_previsto, data_cadastro_quebra 
                                        FROM machine_learning.relatorio_quebras 
                                        WHERE cod_equipamento = %s
                                        ORDER BY id DESC 
                                        LIMIT 1
                                    """, (cod_equipamento,))
                                    data_cadastro_previsto, data_cadastro_quebra = await cursor.fetchone()
                                    print(cod_equipamento,'escrevendo na coluna data_cadastro_previsto_saida 80% ')

                                    if data_cadastro_quebra is None:
    #                                    print(cod_equipamento,'- data_cadastro_quebra e -',data_cadastro_quebra,'data_cadastro_previsto',data_cadastro_previsto, 'tempo dentro do proximo loop - ',hora_media_alerta_80_100_saida[cod_equipamento] - data_cadastro_previsto)
                                    #    print(cod_equipamento,'- data_cadastro_quebra e -',data_cadastro_quebra,'data_cadastro_previsto',data_cadastro_previsto)

                                        await cursor.execute("""
                                            UPDATE machine_learning.relatorio_quebras SET data_cadastro_previsto_saida = %s WHERE cod_equipamento = %s ORDER BY data_cadastro_previsto DESC LIMIT 1
                                        """, (hora_media_alerta_80_100_saida[cod_equipamento], cod_equipamento))
                                        await conn.commit()

                                        await cursor.execute("""
                                            UPDATE machine_learning.log_relatorio_quebras SET data_cadastro_previsto_saida = %s WHERE cod_equipamento = %s ORDER BY data_cadastro_previsto DESC LIMIT 1
                                        """, (hora_media_alerta_80_100_saida[cod_equipamento], cod_equipamento))
                                        await conn.commit()

                                    # fim do escrever na coluna data_cadastro_previsto_saida
                                
#                                if all(value == 0 for value in valores_atuais_114[:-1]):
#                                if any(value == 0 for value in valores_atuais_114[:-1]):
                                # Verificar se pelo menos três valores são iguais a zero
                            #    if sum(value == 0 for value in valores_atuais_114[:-1]) >= 3:
                                if sum(1 for value in valores_atuais_114[:-1] if value == 0) >= 3:

                                    print(cod_equipamento,'alerta removido, alerta em 0 de 80%, zerou valores',all(value == 0 for value in valores_atuais_114[:-1]))
                                #    print('equipamento - ',cod_equipamento, 'valores 114 80 a 100% depois com',valores_atuais_114)

                                #    await cursor.execute("SELECT nome FROM sup_geral.usinas WHERE codigo = %s", (cod_usina,))
                                #    nome_usina = await cursor.fetchone()[0]
                                #    print('nome_usina 80%', nome_usina)


                                #    await cursor.execute("SELECT nome, cod_modelo_funcionamento FROM sup_geral.usinas WHERE codigo = %s", (cod_usina,))
                                #    result = await cursor.fetchone()
                                #    if result:
                                #        nome_usina, cod_modelo_funcionamento = result


                                #        mensagem_previsao = f'🟢 <b>Enviada para {nomes_usuarios_str}!</b> \n\nUsina: {cod_usina} - {nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n'  + ''.join([msg for msg in mensagens if 'Alerta' in msg]) + '\n\nAlerta de load speed removido'
                                #        await bot.send_message(id_grupo, mensagem_previsao, parse_mode='HTML')        

                                #        print('enviando a mensagem para o grupo para 80%')
                                    
                                    coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(cod_equipamento), pool)
                                    previsoes, alerta_abaixo, alerta_acima = await fazer_previsao_sempre_alerta(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento), pool)


                                    if isinstance(previsoes, (list, tuple)):
                                        # Ajusta previsões acima de 100 para 100
                                        previsoes = [min(previsao, 100) for previsao in previsoes]
                                        # Ajusta previsões menores que 0 para 0
                                        previsoes = [max(previsao, 0) for previsao in previsoes]

                                        
                                    await cursor.execute("""
                                        SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                    """, (cod_equipamento,))
                                    alarmes_ativos = await cursor.fetchall()

                                    valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                    valores_previstos_str = ', '.join(map(str, previsoes))
                                    alarmes_ativos_str = ', '.join(map(str, alarmes_ativos))


                                    if cod_equipamento in hora_media_alerta_1_100: 

                                        hora_previsao = hora_media_alerta_1_100[cod_equipamento]
                                        await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                        result = await cursor.fetchone()
                                        if result is not None:
                                            nome, cod_usina, cod_usuario = result

                                            # Extrair códigos dos alarmes_ativos_str
                                            alarmes_ativos_extracao = re.findall(r'\d+', alarmes_ativos_str)
                                            alarmes_ativos_list = list(map(int, alarmes_ativos_extracao))

                                            # Verificar alarmes correspondentes
                                            alarmes_correspondentes = [alarme for alarme in alarmes_ativos_list if alarme in codigos_alarmes_desejados]
                                            alarme_encontrado = len(alarmes_correspondentes) > 0
                                            alarme_verificado = 1 if alarme_encontrado else 0

                                            # Mostrar alarmes correspondentes se alarme_verificado for 1
                                            if alarme_verificado == 1:
                                        #        print('cod_equipamento', cod_equipamento, 'alarme_verificado', alarme_verificado)
                                        #        print('Alarmes correspondentes:', alarmes_correspondentes)
                                                pass

                                            # Definir o valor de `data_cadastro_quebra`
                                            data_cadastro_quebra = 'NOW()' if alarme_verificado == 1 else 'NULL'
                                        #    print('data_cadastro_quebra 100',data_cadastro_quebra, 'cod_equipamento', cod_equipamento)

                                            # Inserir dados na tabela `valores_previsao`
                                            await cursor.execute(f"""
                                                INSERT INTO machine_learning.valores_previsao 
                                                (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, alerta_80, alerta_100, valores_reais, valores_previstos, alarmes) 
                                                VALUES (%s, %s, NOW(), %s, {data_cadastro_quebra}, 0, 1, %s, %s, %s)
                                            """, (cod_equipamento, cod_usina, hora_previsao, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))

                                            await conn.commit()
                                    
                                            # await cursor.execute("""
                                            #     INSERT INTO machine_learning.valores_previsao 
                                            #     (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra,alerta_80, alerta_100, valores_reais, valores_previstos, alarmes) 
                                            #     VALUES (%s, %s, NOW(), %s, NOW(), 0, 1, %s, %s, %s)
                                            # """, (cod_equipamento, cod_usina, hora_previsao, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
                                            # await conn.commit()
                                            
                                            print(f'alerta removido para o equipamento {cod_equipamento}, escrevendo na tabela valores_previsao alerta_100 - hora_previsao',hora_previsao)

                                    if cod_equipamento in hora_media_alerta_1_80: 

                                        hora_previsao = hora_media_alerta_1_80[cod_equipamento]
                                        await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                        result = await cursor.fetchone()
                                        if result is not None:
                                            nome, cod_usina, cod_usuario = result

                                            # Extrair códigos dos alarmes_ativos_str
                                            alarmes_ativos_extracao = re.findall(r'\d+', alarmes_ativos_str)
                                            alarmes_ativos_list = list(map(int, alarmes_ativos_extracao))

                                            # Verificar alarmes correspondentes
                                            alarmes_correspondentes = [alarme for alarme in alarmes_ativos_list if alarme in codigos_alarmes_desejados]
                                            alarme_encontrado = len(alarmes_correspondentes) > 0
                                            alarme_verificado = 1 if alarme_encontrado else 0

                                            # Mostrar alarmes correspondentes se alarme_verificado for 1
                                            if alarme_verificado == 1:
                                        #        print('cod_equipamento', cod_equipamento, 'alarme_verificado', alarme_verificado)
                                        #        print('Alarmes correspondentes:', alarmes_correspondentes)
                                                pass

                                            # Definir o valor de `data_cadastro_quebra`
                                            data_cadastro_quebra = 'NOW()' if alarme_verificado == 1 else 'NULL'
                                        #    print('data_cadastro_quebra 80',data_cadastro_quebra, 'cod_equipamento', cod_equipamento)

                                            # # Inserir dados na tabela `valores_previsao`
                                            # await cursor.execute(f"""
                                            #     INSERT INTO machine_learning.valores_previsao 
                                            #     (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, alerta_80, alerta_100, valores_reais, valores_previstos, alarmes) 
                                            #     VALUES (%s, %s, NOW(), %s, {data_cadastro_quebra}, 1, 0, %s, %s, %s)
                                            # """, (cod_equipamento, cod_usina, hora_previsao, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))

                                            # await conn.commit()

                                            await cursor.execute("""
                                                INSERT INTO machine_learning.valores_previsao 
                                                (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra,alerta_80, alerta_100, valores_reais, valores_previstos, alarmes) 
                                                VALUES (%s, %s, NOW(), %s, NOW(), 1, 0, %s, %s, %s)
                                            """, (cod_equipamento, cod_usina, hora_previsao, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
                                            await conn.commit()
                                            
                                        #    hora_previsao = hora_media_alerta_1_80.pop(cod_equipamento, None)
                                            print(f'alerta removido para o equipamento {cod_equipamento}, escrevendo na tabela valores_previsao alerta_80')


                                    await cursor.execute("SELECT nome, cod_usina, cod_usuario FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
                                    result = await cursor.fetchone()
                                    print('result para enviar nome da usina do alerta 80 ou 100',result)
                                    if result is not None:
                                        nome, cod_usina, cod_usuario = result
                                    #    mensagem_previsao = f'🟢 <b>Enviada para {nomes_usuarios_str}!</b> \n\nUsina: {cod_usina} - {nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n'  + ''.join([msg for msg in mensagens if 'Alerta' in msg]) + '\n\nAlerta de load speed removido'
                                    #    print('mensagem_previsao saida 80 ou 100',mensagem_previsao)
                                    #    id_grupo = await id_chat_grupo(pool)          
                                    #    await bot.send_message(id_grupo, mensagem_previsao, parse_mode='HTML')
                                #        print('enviando a mensagem para o grupo para 80%')
                                        
                                        
                                    await cursor.execute("""
                                    UPDATE machine_learning.leituras_consecutivas
                                    SET alerta = 0
                                    WHERE cod_equipamento = %s
                                    """, (int(cod_equipamento),))
                                    await conn.commit()
                                    print('escrevendo na coluna alerta 0 para 80%')
                                    
                                    hora_previsao = hora_media_alerta_1_80.pop(cod_equipamento, None)
                                    hora_previsao = hora_media_alerta_1_100.pop(cod_equipamento, None)

                                    alertas_enviados_acima.remove(cod_equipamento)
                                    alertas_enviados.remove(cod_equipamento)

                                    print('alerta removido de load speed')
             
            except Exception as e:
                print(f"Ocorreu um erro em enviar_alerta_80_100 ao processar o equipamento {cod_equipamento}: {str(e)}")

        for cod_usina, mensagens in alertas_por_usina.items():
            async with pool.acquire() as conn:
                await conn.ping(reconnect=True)  # Forçar reconexão
                async with conn.cursor() as cursor:
                    # Buscar o nome e o código do modelo de funcionamento da usina
                    await cursor.execute("SELECT nome, cod_modelo_funcionamento FROM sup_geral.usinas WHERE codigo = %s", (cod_usina,))
                    result = await cursor.fetchone()
                    if result:
                        nome_usina, cod_modelo_funcionamento = result
                        print('equipamento', cod_equipamento, 'nome usina', nome_usina, 'o código de funcionamento é:', cod_modelo_funcionamento)

                        # Verificar se o modelo de funcionamento permite enviar mensagens
                        if cod_modelo_funcionamento not in [4, 12, 14]:
                            print('\nequipamento', cod_equipamento, 'nome usina', nome_usina, 'o código de funcionamento é autorizado a mandar mensagem:', cod_modelo_funcionamento)

                            # Buscar todos os cod_usuario associados à usina
                            await cursor.execute("SELECT cod_usuario FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario != 0 AND cod_usina = %s", (cod_usina,))
                            cod_usuarios = await cursor.fetchall()

                            if cod_usuarios:
                                nomes_usuarios = []

                                for cod_usuario_tuple in cod_usuarios:
                                    cod_usuario = cod_usuario_tuple[0]

                                    # Verificar se o usuário não está silenciado
                                    if (cod_usina, cod_usuario) not in sem_mensagem_silenciado:

                                        # Verificar se a usina está ativa para o usuário
                                        await cursor.execute("SELECT ativo FROM machine_learning.usinas_usuario WHERE cod_usina = %s AND cod_usuario = %s", (cod_usina, cod_usuario))
                                        usina_ativa_row = await cursor.fetchone()
                                #        print("(loop 1) A usina ",cod_usina, " Esta ativa? ",usina_ativa_row,' para o usuario ',cod_usuario)

                                        if usina_ativa_row and usina_ativa_row[0] == 1:
                                            await cursor.execute("SELECT id_telegram, usuario, ativo, telefone FROM machine_learning.usuarios_telegram WHERE cod_usuario = %s", (cod_usuario,))
                                            result = await cursor.fetchone()
                                            
                                            if result:
                                                id_telegram, nome_usuario, ativo, telefone = result
                                                if ativo == 1:
                                                    try:
                                                        nomes_usuarios.append(nome_usuario)
                                                        botao_silenciar_usina = InlineKeyboardButton("Silenciar Usina", callback_data=f'silenciar_usina_{cod_usina}')
                                                        botao_receber_alarmes = InlineKeyboardButton("Receber Alarmes", callback_data=f'receber_alarmes_{cod_usina}')

                                                        keyboard = InlineKeyboardMarkup().row(botao_silenciar_usina, botao_receber_alarmes)

                                                        mensagem_final = f'<b>ALERTA!</b> \n\nUsina: {cod_usina} - {nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n' + ''.join([msg for msg in mensagens if 'Alerta' not in msg])
#                                                        mensagem_final = f'<b>ALERTA!</b> \n\n{nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n' + ''.join([msg for msg in mensagens if 'Valores' not in msg])


                                                        print('inicio do payload 80% loop 1')

                                                        # Extrai o valor da tupla
                                                        if telefone:  # Verifica se telefone não é None
                                                            
                                                            # Se o telefone é uma tupla, pega o primeiro elemento
                                                            if isinstance(telefone, tuple):
                                                                telefone = telefone[0]  

                                                            telefone_formatado = telefone.strip().replace(" ", "").replace("-", "")  # Formata o telefone
                                                            if not telefone_formatado.startswith("+"):
                                                                telefone_formatado = f"+{telefone_formatado}"  # Adiciona o "+" no início, se necessário

                                                            # Monta a mensagem
                                                            mensagem_final_wtss = (
                                                                f"**ALERTA!**\n\n"
                                                                f"Usina: {cod_usina} - {nome_usina}\n\n"
                                                    #            f"Ir para usina: https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}\n\n"
                                                                + ''.join([msg for msg in mensagens if 'Alerta' in msg])
                                                            )

                                                            # Monta o payload
                                                            payload = {
                                                                "number": telefone_formatado,
                                                                "textMessage": {
                                                                    "text": mensagem_final_wtss
                                                                }
                                                            }
                                                        else:
                                                            print("Nenhum telefone encontrado para o usuário.")

                                                        start_chat(payload)  # Chama a função para enviar via WhatsApp
                                                        print('payload',payload)
                                                        print('fim do payload 80% loop 1')

                                                        await bot.send_message(id_telegram, mensagem_final, reply_markup=keyboard, parse_mode='HTML')

                                                        if cod_usuario in usuarios_bloqueados:
                                                            usuarios_bloqueados.remove(cod_usuario)
                                                            id_grupo = await id_chat_grupo(pool)
                                                            if id_grupo is not None:
                                                                await bot.send_message(id_grupo, f"🚫 🟢 O bot foi desbloqueado pelo usuário {nome_usuario} ({cod_usuario})")
                                                            await cursor.execute("UPDATE machine_learning.usuarios_telegram SET bloqueado = 0 WHERE cod_usuario = %s", (cod_usuario,))
                                                            await conn.commit()
                                                    
                                                    except BotBlocked:
                                                        if cod_usuario not in usuarios_bloqueados:
                                                            usuarios_bloqueados.add(cod_usuario)
                                                            id_grupo = await id_chat_grupo(pool)
                                                            if id_grupo is not None:
                                                                await bot.send_message(id_grupo, f"🚫 O bot foi bloqueado pelo usuário {nome_usuario} ({cod_usuario})")
                                                            await cursor.execute("UPDATE machine_learning.usuarios_telegram SET bloqueado = 1 WHERE cod_usuario = %s", (cod_usuario,))
                                                            await conn.commit()


                                nomes_usuarios_str = ', '.join(nomes_usuarios)
                                id_grupo = await id_chat_grupo(pool)    
                                botao_silenciar_usina = InlineKeyboardButton("Silenciar Usina", callback_data=f'silenciar_usina_{cod_usina}')
                                botao_receber_alarmes = InlineKeyboardButton("Receber Alarmes", callback_data=f'receber_alarmes_{cod_usina}')

                                keyboard = InlineKeyboardMarkup().row(botao_silenciar_usina, botao_receber_alarmes)
                                                        
                                mensagem_final_grupo = f'<b>Enviada para {nomes_usuarios_str}!</b> \n\nUsina: {cod_usina} - {nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n' + ''.join([msg for msg in mensagens if 'Alerta' in msg])


                                print('inicio do payload 80%')

                                # Executa a consulta no banco
                                await cursor.execute("SELECT telefone FROM machine_learning.usuarios_telegram WHERE cod_usuario = 374")
                                # Retorna um único registro como tupla
                                telefone = await cursor.fetchone()

                                # Extrai o valor da tupla
                                if telefone:  # Verifica se telefone não é None
                                    telefone = telefone[0]  # Pega o primeiro elemento da tupla
                                    telefone_formatado = telefone.strip().replace(" ", "").replace("-", "")  # Formata o telefone
                                    if not telefone_formatado.startswith("+"):
                                        telefone_formatado = f"+{telefone_formatado}"  # Adiciona o "+" no início, se necessário

                                    # Monta a mensagem
                                    mensagem_final_wtss = (
                                        f"**Enviada para {nomes_usuarios_str}!**\n\n"
                                        f"Usina: {cod_usina} - {nome_usina}\n\n"
                                        f"Ir para usina: https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}\n\n"
                                        + ''.join([msg for msg in mensagens if 'Alerta' in msg])
                                    )

                                    # Monta o payload
                                    payload = {
                                        "number": telefone_formatado,
                                        "textMessage": {
                                            "text": mensagem_final_wtss
                                        }
                                    }
                                else:
                                    print("Nenhum telefone encontrado para o usuário.")

                                start_chat(payload)  # Chama a função para enviar via WhatsApp
                                print('payload',payload)
                                print('fim do payload 80%')

                                await bot.send_message(id_grupo, mensagem_final_grupo, reply_markup=keyboard, parse_mode='HTML')
#                                await bot.send_message(id_grupo, mensagem_final_grupo, parse_mode='HTML', reply_markup=main_keyboard)

                                nomes_usuarios.clear()
                                sys.stdout.flush()

                        elif cod_modelo_funcionamento in [4, 12, 14]:

                            # Se o cod_modelo_funcionamento não estiver em [4, 12, 14], pule para a próxima iteração
                        #    print('\nequipamento', cod_equipamento, 'nome usina', nome_usina, 'o código de funcionamento nao é autorizado a mandar mensagem:', cod_modelo_funcionamento, 'Cliente aceitou envio desse equipamento')

                            # Buscar todos os cod_usuario associados à usina
                            await cursor.execute("SELECT cod_usuario FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario != 0 AND cod_usina = %s", (cod_usina,))
                            cod_usuarios = await cursor.fetchall()

                            if cod_usuarios:
                                nomes_usuarios = []

                                for cod_usuario_tuple in cod_usuarios:
                                    cod_usuario = cod_usuario_tuple[0]

                                    # Verificar se o usuário não está silenciado
                                    if (cod_usina, cod_usuario) not in sem_mensagem_silenciado:

                                        # Verificar se a usina está ativa para o usuário
                                        await cursor.execute("SELECT ativo FROM machine_learning.usinas_usuario WHERE cod_usina = %s AND cod_usuario = %s", (cod_usina, cod_usuario))
                                        usina_ativa_row = await cursor.fetchone()
                                    #    print("(loop 2) A usina ",cod_usina, " Esta ativa? ",usina_ativa_row,' para o usuario ',cod_usuario)

                                        if usina_ativa_row and usina_ativa_row[0] == 1:
                                            await cursor.execute("SELECT id_telegram, usuario, ativo, todos_modelo_funcionamento, telefone FROM machine_learning.usuarios_telegram WHERE cod_usuario = %s", (cod_usuario,))
                                            result = await cursor.fetchone()

                                            if result:
                                                id_telegram, nome_usuario, ativo, todos_modelo_funcionamento, telefone = result
                                                if ativo == 1 and todos_modelo_funcionamento == 1:
                                                    try:
                                                        nomes_usuarios.append(nome_usuario)
                                                        botao_silenciar_usina = InlineKeyboardButton("Silenciar Usina", callback_data=f'silenciar_usina_{cod_usina}')
                                                        botao_receber_alarmes = InlineKeyboardButton("Receber Alarmes", callback_data=f'receber_alarmes_{cod_usina}')

                                                        keyboard = InlineKeyboardMarkup().row(botao_silenciar_usina, botao_receber_alarmes)

                                                        mensagem_final = f'<b>ALERTA!</b> \n\nUsina: {cod_usina} - {nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n' + ''.join([msg for msg in mensagens if 'Alerta' not in msg])
#                                                        mensagem_final = f'<b>ALERTA!</b> \n\n{nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n' + ''.join([msg for msg in mensagens if 'Valores' not in msg])

                                                        print('inicio do payload 80% loop 2')

                                                        # Extrai o valor da tupla
                                                        if telefone:  # Verifica se telefone não é None
                                                            
                                                            # Se o telefone é uma tupla, pega o primeiro elemento
                                                            if isinstance(telefone, tuple):
                                                                telefone = telefone[0]  

                                                            telefone_formatado = telefone.strip().replace(" ", "").replace("-", "")  # Formata o telefone
                                                            if not telefone_formatado.startswith("+"):
                                                                telefone_formatado = f"+{telefone_formatado}"  # Adiciona o "+" no início, se necessário

                                                            # Monta a mensagem
                                                            mensagem_final_wtss = (
                                                                f"**ALERTA!**\n\n"
                                                                f"Usina: {cod_usina} - {nome_usina}\n\n"
                                                        #        f"Ir para usina: https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}\n\n"
                                                                + ''.join([msg for msg in mensagens if 'Alerta' in msg])
                                                            )

                                                            # Monta o payload
                                                            payload = {
                                                                "number": telefone_formatado,
                                                                "textMessage": {
                                                                    "text": mensagem_final_wtss
                                                                }
                                                            }
                                                        else:
                                                            print("Nenhum telefone encontrado para o usuário.")

                                                        start_chat(payload)  # Chama a função para enviar via WhatsApp
                                                        print('payload',payload)
                                                        print('fim do payload 80% loop 2')


                                                        await bot.send_message(id_telegram, mensagem_final, reply_markup=keyboard, parse_mode='HTML')

                                                        if cod_usuario in usuarios_bloqueados:
                                                            usuarios_bloqueados.remove(cod_usuario)
                                                            id_grupo = await id_chat_grupo(pool)
                                                            if id_grupo is not None:
                                                                await bot.send_message(id_grupo, f"🚫 🟢 O bot foi desbloqueado pelo usuário {nome_usuario} ({cod_usuario})")
                                                            await cursor.execute("UPDATE machine_learning.usuarios_telegram SET bloqueado = 0 WHERE cod_usuario = %s", (cod_usuario,))
                                                            await conn.commit()
                                                    
                                                    except BotBlocked:
                                                        if cod_usuario not in usuarios_bloqueados:
                                                            usuarios_bloqueados.add(cod_usuario)
                                                            id_grupo = await id_chat_grupo(pool)
                                                            if id_grupo is not None:
                                                                await bot.send_message(id_grupo, f"🚫 O bot foi bloqueado pelo usuário {nome_usuario} ({cod_usuario})")
                                                            await cursor.execute("UPDATE machine_learning.usuarios_telegram SET bloqueado = 1 WHERE cod_usuario = %s", (cod_usuario,))
                                                            await conn.commit()

                                nomes_usuarios_str = ', '.join(nomes_usuarios)
                                id_grupo = await id_chat_grupo(pool)

                                botao_silenciar_usina = InlineKeyboardButton("Silenciar Usina", callback_data=f'silenciar_usina_{cod_usina}')
                                botao_receber_alarmes = InlineKeyboardButton("Receber Alarmes", callback_data=f'receber_alarmes_{cod_usina}')

                                keyboard = InlineKeyboardMarkup().row(botao_silenciar_usina, botao_receber_alarmes)
                                                        
                                mensagem_final_grupo = f'<b>Enviada para {nomes_usuarios_str}!</b> \n\nUsina: {cod_usina} - {nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n' + ''.join([msg for msg in mensagens if 'Alerta' in msg])


                                print('inicio do payload 80%')

                                # Executa a consulta no banco
                                await cursor.execute("SELECT telefone FROM machine_learning.usuarios_telegram WHERE cod_usuario = 374")
                                # Retorna um único registro como tupla
                                telefone = await cursor.fetchone()

                                # Extrai o valor da tupla
                                if telefone:  # Verifica se telefone não é None
                                    telefone = telefone[0]  # Pega o primeiro elemento da tupla
                                    telefone_formatado = telefone.strip().replace(" ", "").replace("-", "")  # Formata o telefone
                                    if not telefone_formatado.startswith("+"):
                                        telefone_formatado = f"+{telefone_formatado}"  # Adiciona o "+" no início, se necessário

                                    # Monta a mensagem
                                    mensagem_final_wtss = (
                                        f"**Enviada para {nomes_usuarios_str}!**\n\n"
                                        f"Usina: {cod_usina} - {nome_usina}\n\n"
                                        f"Ir para usina: https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}\n\n"
                                        + ''.join([msg for msg in mensagens if 'Alerta' in msg])
                                    )

                                    # Monta o payload
                                    payload = {
                                        "number": telefone_formatado,
                                        "textMessage": {
                                            "text": mensagem_final_wtss
                                        }
                                    }
                                else:
                                    print("Nenhum telefone encontrado para o usuário.")

                                start_chat(payload)  # Chama a função para enviar via WhatsApp
                                print('payload',payload)
                                print('fim do payload 80%')

                                await bot.send_message(id_grupo, mensagem_final_grupo, reply_markup=keyboard, parse_mode='HTML')
#                                await bot.send_message(id_grupo, mensagem_final_grupo, parse_mode='HTML', reply_markup=main_keyboard)
                                nomes_usuarios.clear()
                                sys.stdout.flush()



        # tempo_inicial = datetime.now()
        # data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')
        # print('-------------------------------------------------------------------- fim enviar_alerta_80_100 ---------------------------------------------------------------------------------',data_cadastro_formatada,'\n')

        tempo_final = datetime.now()
        data_cadastro_formatada_final = tempo_final.strftime('%d-%m-%Y %H:%M')

        # Calcular o tempo de execução
        tempo_execucao = tempo_final - tempo_inicial

        # Formatando a diferença de tempo
        segundos = tempo_execucao.total_seconds()
        horas = int(segundos // 3600)
        minutos = int((segundos % 3600) // 60)
        segundos_restantes = int(segundos % 60)

        # Exibindo o tempo total de execução
        print('\n','------------------------------------------------------------- fim enviar_alerta_80_100 --------------------------------------------------------------------------', data_cadastro_formatada_final)
        print(f'Tempo de execução do enviar_alerta_80_100: {horas} horas, {minutos} minutos, {segundos_restantes} segundos\n')

        await asyncio.sleep(120)
      

import requests

def start_chat(payload):
    url_base = "http://192.168.15.60:8080"
    url = f"{url_base}/message/sendText/Suporte_BRG"

    try:
        response = requests.post(
            url,
            json=payload,
            headers={
                "apikey": "k3v14ilstiguaumoz8nzt",
                "Content-Type": "application/json"
            }
        )

        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "PENDING":
                print(f"A mensagem está na fila (PENDING). Aguardando entrega...")
                message_id = data['message']['id']
                check_message_status(message_id)
            else:
                print("Mensagem enviada com sucesso:", data)
        else:
            print(f"Erro ao enviar mensagem. Código: {response.status_code}, Resposta: {response.text}")

    except requests.exceptions.RequestException as e:
        print("Erro de conexão ao enviar mensagem:", e)
    except Exception as e:
        print("Erro inesperado ao enviar mensagem:", e)



# Função para verificar o status da mensagem após o envio
def check_message_status(message_id):
    url_base = "http://192.168.15.60:8080"
    url = f"{url_base}/message/status/{message_id}"

    # Espera um tempo antes de verificar o status novamente
    time.sleep(10)  # Aguarda 10 segundos (ajuste conforme necessário)

    try:
        # Faz uma nova requisição para verificar o status
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            status = data.get("status", "UNKNOWN")

            if status == "DELIVERED":
                print("Mensagem entregue com sucesso!")
            elif status == "PENDING":
                print("A mensagem ainda está na fila. Tentando novamente...")
                # Se o status ainda for PENDING, podemos verificar novamente após outro tempo
                check_message_status(message_id)
            else:
                print(f"Mensagem com status desconhecido: {status}")
        else:
            print(f"Erro ao verificar status. Código: {response.status_code}, Resposta: {response.text}")

    except Exception as e:
        print("Erro ao verificar status:", e)
        

async def processar_alertas_equipamento(cod_equipamento, cursor, pool, hora_media_alerta_1_100, hora_media_alerta_1_80, hora_media_alerta_80_100_saida):
    # Obter valores atuais para o equipamento
    await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(cod_equipamento),))
    valores_atuais = await cursor.fetchone()

    # Obter coeficiente e intercepto existentes
    coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(cod_equipamento), pool)
    previsoes, alerta_abaixo, alerta_acima = await fazer_previsao_sempre_alerta(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento), pool)

    # Limitar previsões para o intervalo 0-100
    if isinstance(previsoes, (list, tuple)):
        previsoes = [min(max(previsao, 0), 100) for previsao in previsoes]

    # Obter alarmes ativos
    await cursor.execute("SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s", (cod_equipamento,))
    alarmes_ativos = await cursor.fetchall()

    # Converter para string
    valores_atuais_str = ', '.join(map(str, valores_atuais))
    valores_previstos_str = ', '.join(map(str, previsoes))
    alarmes_ativos_str = ', '.join(map(str, alarmes_ativos))

    # Checar e inserir previsões na tabela `valores_previsao`
    if cod_equipamento in hora_media_alerta_1_100:
        hora_previsao = hora_media_alerta_1_100[cod_equipamento]
        await inserir_dados_previsao(cod_equipamento, hora_previsao, 0, 1, valores_atuais_str, valores_previstos_str, alarmes_ativos_str, cursor)

    if cod_equipamento in hora_media_alerta_1_80:
        hora_previsao = hora_media_alerta_1_80[cod_equipamento]
        await inserir_dados_previsao(cod_equipamento, hora_previsao, 1, 0, valores_atuais_str, valores_previstos_str, alarmes_ativos_str, cursor)

    # Atualizar coluna data_cadastro_previsto_saida para alertas removidos
    await atualizar_alerta_saida(cod_equipamento, hora_media_alerta_80_100_saida, cursor)



# Função para inserir dados na tabela `valores_previsao`
async def inserir_dados_previsao(cod_equipamento, hora_previsao, alerta_80, alerta_100, valores_atuais_str, valores_previstos_str, alarmes_ativos_str, cursor):
    await cursor.execute("SELECT nome, cod_usina FROM sup_geral.equipamentos WHERE codigo = %s", (cod_equipamento,))
    result = await cursor.fetchone()
    if result:
        cod_usina = result[1]
        await cursor.execute("""
            INSERT INTO machine_learning.valores_previsao 
            (cod_equipamento, cod_usina, data_cadastro, data_cadastro_previsto, data_cadastro_quebra, alerta_80, alerta_100, valores_reais, valores_previstos, alarmes) 
            VALUES (%s, %s, NOW(), %s, NULL, %s, %s, %s, %s, %s)
        """, (cod_equipamento, cod_usina, hora_previsao, alerta_80, alerta_100, valores_atuais_str, valores_previstos_str, alarmes_ativos_str))
        await cursor.connection.commit()

# Função para atualizar coluna `data_cadastro_previsto_saida`
async def atualizar_alerta_saida(cod_equipamento, hora_media_alerta_80_100_saida, cursor):
    if cod_equipamento not in hora_media_alerta_80_100_saida:
        hora_media_alerta_80_100_saida[cod_equipamento] = datetime.now()
        await cursor.execute("""
            SELECT data_cadastro_previsto, data_cadastro_quebra 
            FROM machine_learning.relatorio_quebras 
            WHERE cod_equipamento = %s ORDER BY id DESC LIMIT 1
        """, (cod_equipamento,))
        data_cadastro_previsto, data_cadastro_quebra = await cursor.fetchone()

        if data_cadastro_quebra is None:
            await cursor.execute("""
                UPDATE machine_learning.relatorio_quebras SET data_cadastro_previsto_saida = %s WHERE cod_equipamento = %s ORDER BY data_cadastro_previsto DESC LIMIT 1
            """, (hora_media_alerta_80_100_saida[cod_equipamento], cod_equipamento))
            await cursor.execute("""
                UPDATE machine_learning.log_relatorio_quebras SET data_cadastro_previsto_saida = %s WHERE cod_equipamento = %s ORDER BY data_cadastro_previsto DESC LIMIT 1
            """, (hora_media_alerta_80_100_saida[cod_equipamento], cod_equipamento))
            await cursor.connection.commit()


async def focar_alertas(cod_equipamentos, tabelas, cod_campo_especificados, pool):
    while True:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for equipamento in alertas_enviados:
                    await processar_alertas_equipamento(equipamento, cursor, pool, hora_media_alerta_1_100, hora_media_alerta_1_80, hora_media_alerta_80_100_saida)
        await asyncio.sleep(10)




async def enviar_cod_equipamento_usuario(message: types.Message, id_telegram=None):
    

    user_input = message.text.lstrip('/')
    username = user_input
    
    chat_id = message.chat.id

    cursor.execute("SELECT nome, codigo FROM sup_geral.usuarios WHERE login = %s", (username,))
    result = cursor.fetchone()
    if result is not None:
        nome_supervisorio, cod_usuario = result

        cursor.execute("SELECT primeiro_acesso FROM machine_learning.usuarios_telegram WHERE usuario = %s", (username,))
        primeiro_acesso = cursor.fetchone()[0]
        if primeiro_acesso == 1:
            
            cursor.execute("SELECT cod_usina FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
            cod_usinas = cursor.fetchall()

            mensagem = f"Usinas cadastradas de {nome_supervisorio} e seus equipamentos:\n"

            for cod_usina_tuple in cod_usinas:
                cod_usina = cod_usina_tuple[0]
                cursor.execute("SELECT codigo FROM sup_geral.equipamentos WHERE cod_usina = %s", (cod_usina,))
                cod_equipamentos = cursor.fetchall()
                equipamentos_lista = [str(resultado[0]) for resultado in cod_equipamentos]

                equipamentos_str = ', '.join(equipamentos_lista)

                mensagem += f"Usina: {cod_usina} - Equipamentos: {equipamentos_str}\n"

            if len(mensagem) > 4096:
                partes_mensagem = [mensagem[i:i+4096] for i in range(0, len(mensagem), 4096)]
                for parte in partes_mensagem:
                    if id_telegram:
                        await bot.send_message(id_telegram, parte)
                    else:
                        await bot.send_message(chat_id, parte)
            else:
                if id_telegram:
                    await bot.send_message(id_telegram, mensagem)
                else:
                    await bot.send_message(chat_id, mensagem)

        else:
            await message.reply("Usuário não encontrado.")


async def enviar_previsao_valor_equipamento(message: types.Message, id_telegram=None):
    user_input = message.text.lstrip('/')
    username = user_input
    chat_id = message.chat.id
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    valores_atuais_str = 0

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT nome, codigo FROM sup_geral.usuarios WHERE login = %s", (username,))
            result = await cursor.fetchone()
            if result is not None:
                nome_supervisorio, cod_usuario = result

                await cursor.execute("SELECT primeiro_acesso FROM machine_learning.usuarios_telegram WHERE usuario = %s", (username,))
                primeiro_acesso = (await cursor.fetchone())[0]

                if primeiro_acesso == 1:
                    
                    await cursor.execute("UPDATE machine_learning.usuarios_telegram SET primeiro_acesso = 0 WHERE usuario = %s", (username,))
                    await conn.commit()
                    print(username)
                    
                    await cursor.execute("SELECT cod_usina FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
                    cod_usinas = await cursor.fetchall()

                    mensagem = f"Usina de {nome_supervisorio} e seus equipamentos:\n\n"

                    for cod_usina_tuple in cod_usinas:
                        cod_usina = cod_usina_tuple[0]

                        await cursor.execute("SELECT codigo FROM sup_geral.equipamentos WHERE cod_usina = %s AND ativo = 1", (cod_usina,))
                        cod_equipamentos = await cursor.fetchall()

                        equipamentos_lista = [str(resultado[0]) for resultado in cod_equipamentos]

                        for equipamento in equipamentos_lista:
                            await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(equipamento),))
                            valores_atuais = await cursor.fetchone()

                            if valores_atuais is not None:
                                coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(equipamento), pool)
                                previsoes, alerta = await fazer_previsao(valores_atuais, coeficiente_existente, intercepto_existente, int(equipamento), pool)

                                await cursor.execute("SELECT valor FROM sup_geral.leituras WHERE cod_equipamento = %s AND cod_campo = 114 ORDER BY data_cadastro DESC LIMIT 1", (int(equipamento),))
                                valor_real = await cursor.fetchone()

                                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (int(equipamento),))
                                valores_atuais_114 = await cursor.fetchone()

                                if valor_real is not None:
                                    valor_real = valor_real[0]
                                else:
                                    valor_real = 'N/A'

                                if valores_atuais_114 is not None:
                                    valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                else:
                                    valores_atuais_str = "Valores atuais não disponíveis"

                                equipamentos_str = f'\nValores Atuais: {valores_atuais_str} \nValores Previstos: {previsoes}'

                                mensagem += f"Usina: {cod_usina} - Equipamentos: {equipamento}: {equipamentos_str}\n\n"

                    if len(mensagem) > 4096:
                        partes_mensagem = [mensagem[i:i+4096] for i in range(0, len(mensagem), 4096)]
                        for parte in partes_mensagem:
                            if id_telegram:
                                await bot.send_message(id_telegram, parte)
                            else:
                                await bot.send_message(chat_id, parte)
                    else:
                        if id_telegram:
                            await bot.send_message(id_telegram, mensagem)
                        else:
                            await bot.send_message(chat_id, mensagem)
                                            

                elif message.text == 'bruno.zanella':
                    usinas_equipamentos = {}
                
                    await cursor.execute("SELECT codigo FROM sup_geral.usinas WHERE ativo = 1")
                    cod_usinas = await cursor.fetchall()
                    mensagem_total_equipamentos = ""

                    mensagem = f"Usina de {nome_supervisorio} e seus equipamentos:\n\n"
                    total_equipamentos = 0
                    total_equipamentos_true = 0

                    for cod_usina_tuple in cod_usinas:
                        cod_usina = cod_usina_tuple[0]

                        codigos_GMG = await selecionar_GMG()

                        placeholders = ', '.join(['%s'] * len(codigos_GMG))
                        query = f"SELECT codigo FROM sup_geral.equipamentos WHERE cod_usina = %s AND cod_tipo_equipamento IN ({placeholders}) AND ativo = 1"
                        await cursor.execute(query, [cod_usina] + list(codigos_GMG))
                        cod_equipamentos = await cursor.fetchall()

                        equipamentos_lista = [str(resultado[0]) for resultado in cod_equipamentos]

                        for equipamento in equipamentos_lista:
                            await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (int(equipamento),))
                            valores_atuais = await cursor.fetchone()
                            if valores_atuais is not None:
                                if all(valor != 0 for valor in valores_atuais):
                                    coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(int(equipamento), pool)
                                    previsoes, alerta = await fazer_previsao(valores_atuais, coeficiente_existente, intercepto_existente, int(equipamento), pool)

                                    if isinstance(previsoes, int):
                                        if previsoes != 0:
                                            total_equipamentos += 1

                                            await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (int(equipamento),))
                                            valores_atuais_114 = await cursor.fetchone()

                                            valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                            if alerta:
                                                total_equipamentos_true += 1
                                                equipamentos_str = f'\n\nValores Atuais: {valores_atuais_str} \nValores Previstos: {previsoes} \n{alerta}'
                                            else:
                                                equipamentos_str = f'{alerta}'

                                            if cod_usina not in usinas_equipamentos:
                                                await cursor.execute("SELECT nome FROM sup_geral.usinas WHERE codigo = %s", (cod_usina,))
                                                nome_usina = await cursor.fetchone()[0]
                                                usinas_equipamentos[cod_usina] = {'nome': nome_usina, 'equipamentos': []}

                                            usinas_equipamentos[cod_usina]['equipamentos'].append(f'Equipamento: {equipamento}: {equipamentos_str}\n\n')


                                    elif isinstance(previsoes, list):
                                        if not all(valor == 0 for valor in previsoes):
                                            total_equipamentos += 1

                                            await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (int(equipamento),))
                                            valores_atuais_114 = await cursor.fetchone()

                                            valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                                            if alerta:
                                                total_equipamentos_true += 1
                                                equipamentos_str = f'\n\nValores Atuais: {valores_atuais_str} \nValores Previstos: {previsoes} \n{alerta}'
                                            else:
                                                equipamentos_str = f'{alerta}'

                                            if cod_usina not in usinas_equipamentos:
                                                await cursor.execute("SELECT nome FROM sup_geral.usinas WHERE codigo = %s", (cod_usina,))
                                                nome_usina = (await cursor.fetchone())[0]
                                                usinas_equipamentos[cod_usina] = {'nome': nome_usina, 'equipamentos': []}

                                            usinas_equipamentos[cod_usina]['equipamentos'].append(f'Equipamento: {equipamento}: {equipamentos_str}\n\n')

                                    mensagem = f"Usina de {nome_supervisorio} e seus equipamentos:\n\n"
                                    for cod_usina, info in usinas_equipamentos.items():
                                        mensagem += f'<b>Usina: {cod_usina} - {html.escape(info["nome"])}</b>  \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n'
                                        for equipamento in info['equipamentos']:
                                            mensagem += html.escape(equipamento)

                    await bot.send_message(chat_id, mensagem, parse_mode='HTML')

                    '''
                    if len(mensagem) > 4000:
                        partes_mensagem = [mensagem[i:i+4000] for i in range(0, len(mensagem), 4000)]
                        chat_id = 6870017547
                        for parte in partes_mensagem:
                            if id_telegram:
                                await bot.send_message(chat_id, parte, parse_mode='HTML')
                            else:
                                await bot.send_message(chat_id, parte, parse_mode='HTML')
                    else:
                        chat_id = 6870017547
                        if id_telegram:
                            await bot.send_message(chat_id, mensagem, parse_mode='HTML')
                        else:
                            await bot.send_message(chat_id, mensagem, parse_mode='HTML')
                    '''


                mensagem_total_equipamentos += f"\n\nGeradores Em Operação: {total_equipamentos}\nGeradores Em alerta: {total_equipamentos_true}\n/teste"
                print(f"\n\nGeradores Em Operação: {total_equipamentos}\nGeradores Em alerta: {total_equipamentos_true}\n")
                await bot.send_message(chat_id, mensagem_total_equipamentos)
                print('*****************************************************************************************************************')
                sys.stdout.flush()

            else:
                await message.reply("Usuário não encontrado.")



async def enviar_previsao_valor_usina_menu(cod_usina, chat_id=None):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    codigos_GMG = await selecionar_GMG(pool)

    placeholders = ', '.join(['%s'] * len(codigos_GMG))
    query = f"SELECT codigo FROM sup_geral.equipamentos WHERE cod_usina = %s AND cod_tipo_equipamento IN ({placeholders}) AND ativo = 1"
    
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(query, [cod_usina] + list(codigos_GMG))
            cod_equipamentos = await cursor.fetchall()

            equipamentos_lista = [str(resultado[0]) for resultado in cod_equipamentos]

            mensagem = f"Usina: {cod_usina} - Previsões para Equipamentos:\n\n"

            for equipamento in equipamentos_lista:
                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (equipamento,))
                valores_atuais = await cursor.fetchone()

                await cursor.execute("SELECT nome FROM sup_geral.equipamentos WHERE codigo = %s", (int(equipamento),))
                nome_equipamento_tuple = await cursor.fetchone()
                nome_equipamento = nome_equipamento_tuple[0] if nome_equipamento_tuple else "Nome não encontrado"

                if valores_atuais is None:
                    mensagem += f"Equipamento: {equipamento} ({nome_equipamento}) - Valores não encontrados na base de dados.\n\n"
                    continue

                coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(equipamento, pool)
                previsoes, alerta = await fazer_previsao_sempre(valores_atuais, coeficiente_existente, intercepto_existente, int(equipamento))

                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (int(equipamento),))
                valores_atuais_114 = await cursor.fetchone()

                if valores_atuais_114 is None:
                    mensagem += f"Equipamento: {equipamento} ({nome_equipamento}) - Valores do campo 114 não encontrados na base de dados.\n\n"
                    continue

                valores_atuais_str = ', '.join(map(str, valores_atuais_114))

                equipamentos_str = f'\nValores Atuais: {valores_atuais_str} \nValores Previstos: {previsoes}\n{alerta}'

                mensagem += f"Equipamento: {equipamento} - ({nome_equipamento}): {equipamentos_str}\n\n"

            target_chat_id = chat_id if chat_id is not None else chat_id

            if len(mensagem) > 4096:
                partes_mensagem = [mensagem[i:i+4096] for i in range(0, len(mensagem), 4096)]
                for parte in partes_mensagem:
                    await bot.send_message(target_chat_id, parte)
            else:
                await bot.send_message(target_chat_id, mensagem)





async def enviar_previsao_valor_equipamento_menu(chat_id, cod_equipamento):
    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT nome, cod_usina FROM sup_geral.equipamentos WHERE codigo = %s AND ativo = 1", (cod_equipamento,) )
            equipamento_info = await cursor.fetchone()
            
            if equipamento_info:
                nome_equipamento, cod_usina = equipamento_info

                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s", (cod_equipamento,))
                valores_atuais = await cursor.fetchone()

                await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (cod_equipamento,))
                valores_atuais_114 = await cursor.fetchone()
                    
                if valores_atuais:
                    coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(cod_equipamento, pool)
                    previsoes, alerta = await fazer_previsao_sempre(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento))

                    response_message = (
                        f"Previsões para o Equipamento {cod_equipamento} ({nome_equipamento}) Usina {cod_usina}:\n\n"
                        f"Valores Atuais: {', '.join(map(str, valores_atuais_114))}\n"
                        f"Valores Previstos: {previsoes}\n{alerta}"
                    )

                    return response_message
                else:
                    return "Não foram encontrados valores atuais para o equipamento."
            else:
                return "Equipamento não encontrado."



async def close_db(pool):

    if pool is None:
        print("A conexão com o banco de dados não foi estabelecida.")
        return
    
    print('fechando o banco e limpando listas')
    
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # Coloca todos os `cod_equipamentos_close` em uma única query de update
            if cod_equipamentos_close:
                await cursor.executemany("""
                    UPDATE machine_learning.leituras_consecutivas
                    SET alerta = 0
                    WHERE cod_equipamento = %s
                    """, [(int(cod_equipamento),) for cod_equipamento in cod_equipamentos_close])
                await cursor.executemany("""
                    UPDATE machine_learning.telegram_silenciar_bot
                    SET receber_alarme = 0
                    WHERE cod_equipamento = %s
                    """, [(int(cod_equipamento),) for cod_equipamento in cod_equipamentos_close])
                
                await conn.commit()  # Apenas um commit ao final

            # Seleciona todos os dados da tabela para verificar status
            await cursor.execute("""
                SELECT cod_usuario, cod_equipamento, tempo_silenciado, data_cadastro, receber_alarme 
                FROM machine_learning.telegram_silenciar_bot
            """)
            results = await cursor.fetchall()
            
            # Mapeia todos os equipamentos para reduzir consultas repetidas
            equipamentos_map = {}
            if results:
                cod_equipamentos = [result[1] for result in results]  # Lista de cod_equipamento para filtrar
                await cursor.executemany("""
                    SELECT nome, cod_usina, codigo 
                    FROM sup_geral.equipamentos 
                    WHERE codigo = %s
                """, [(cod_equipamento,) for cod_equipamento in cod_equipamentos])
                
                equipamento_results = await cursor.fetchall()
                equipamentos_map = {equipamento_result[2]: equipamento_result for equipamento_result in equipamento_results}

            # Processa cada resultado e realiza as atualizações necessárias
            updates = []
            deletes = []
            for result in results:
                cod_usuario_verifica_silenciado, cod_equipamento, tempo_silenciado, data_cadastro, receber_alarme = result
                equipamento_info = equipamentos_map.get(cod_equipamento)

                if equipamento_info:
                    nome, cod_usina_silenciada, _ = equipamento_info
                    
                    if tempo_silenciado > 0:
                        if receber_alarme == 1:
                            updates.append(cod_equipamento)
                            print(f"Coluna 'tempo_silenciado' atualizada para 0. Equipamento {cod_equipamento} configurado para receber alarmes novamente.")
                        elif receber_alarme == 0:
                            deletes.append(cod_equipamento)
                            print(f"Tempo de silenciamento finalizado. Linha correspondente ao equipamento {cod_equipamento} excluída da tabela.")
                    elif tempo_silenciado == 0 and receber_alarme == 0:
                        deletes.append(cod_equipamento)
                        print(f"Coluna 'tempo_silenciado' atualizada para 0. Equipamento {cod_equipamento} com linha apagada.")

            # Realiza batch updates para tempo_silenciado = 0
            if updates:
                await cursor.executemany("""
                    UPDATE machine_learning.telegram_silenciar_bot
                    SET tempo_silenciado = 0
                    WHERE cod_equipamento = %s
                """, [(cod_equipamento,) for cod_equipamento in updates])
                
            # Realiza batch deletes para remover as linhas da tabela
            if deletes:
                await cursor.executemany("""
                    DELETE FROM machine_learning.telegram_silenciar_bot
                    WHERE cod_equipamento = %s
                """, [(cod_equipamento,) for cod_equipamento in deletes])
                
            await conn.commit()  # Apenas um commit ao final de todas as operações

    print('db close 0')


    # Limpar as listas
    alertas_enviados.clear()
    alertas_enviados_previsao.clear()
    sem_mensagem_silenciado.clear()

    cnx.close()
    pool.close()  # Iniciar o fechamento do pool
    await pool.wait_closed()  # Aguardar o fechamento completo
    print('fechando conexao com o banco')
    

    
    
    
def selecionar_GMG_sincrono():
    cursor.execute("SELECT codigo, ativo FROM sup_geral.tipos_equipamentos WHERE classe = 'GMG'")
    resultados = cursor.fetchall()
    codigos = []

    for resultado in resultados:
        codigos.append(resultado[0])
    
    return codigos


def obter_equipamentos_validos_sincrono(tabelas):
    codigos_GMG = selecionar_GMG_sincrono()
    codigos_GMG_str = ', '.join(map(str, codigos_GMG))

    query_equipamentos = f"SELECT DISTINCT codigo FROM sup_geral.equipamentos WHERE cod_tipo_equipamento IN ({codigos_GMG_str}) AND ativo = 1"
    cursor.execute(query_equipamentos)
    resultados_equipamentos = cursor.fetchall()
    cod_equipamentos = [str(resultado[0]) for resultado in resultados_equipamentos]

    query_ultima_tabela = f"SELECT DISTINCT cod_equipamento FROM {tabelas}"
    cursor.execute(query_ultima_tabela)
    resultados_ultima_tabela = cursor.fetchall()
    cod_ultima_tabela = [str(resultado[0]) for resultado in resultados_ultima_tabela]

    cod_equipamentos_validos = list(set(cod_equipamentos) & set(cod_ultima_tabela))
    total_equipamentos = len(cod_equipamentos_validos)
    cod_equipamentos_validos = sorted([int(cod) for cod in cod_equipamentos_validos])

    return cod_equipamentos_validos

cod_equipamentos_close = obter_equipamentos_validos_sincrono(tabelas)    

# Função para enviar mensagem para todos os usuários ativos
async def enviar_mensagem_para_grupo(pool):
    usuarios_notificados = []
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT id_telegram, nome_telegram FROM machine_learning.usuarios_telegram WHERE ativo = 1")
            resultados = await cursor.fetchall()
            for resultado in resultados:
                id_telegram = resultado[0]
                nome_telegram = resultado[1]
                mensagem = (
                    "Novas implementações foram integradas ao BOT.\n"
                    "Clique em /start para criar o botão no teclado para geração de relatório."
                )
                try:
                    await bot.send_message(id_telegram, mensagem, parse_mode=ParseMode.MARKDOWN)
                    usuarios_notificados.append(nome_telegram)
                except Exception as e:
                    print(f"Erro ao enviar mensagem para {id_telegram}: {e}")

    # Enviar mensagem para o grupo com os nomes dos usuários notificados
    if usuarios_notificados:
        mensagem_grupo = (
            "Novas implementações foram integradas ao BOT.\n"
            "Clique em /start para criar o botão no teclado para geração de relatório.\n\n"
            "Usuários notificados:\n" + "\n".join(usuarios_notificados)
        )
        id_grupo = await id_chat_grupo(pool)
        if id_grupo:
            try:
                await bot.send_message(id_grupo, mensagem_grupo, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                print(f"Erro ao enviar mensagem para o grupo {id_grupo}: {e}")


# Chamar função de enviar mensagem para todos os usuários do grupo
async def notificar_todos_os_usuarios(pool):
    await enviar_mensagem_para_grupo(pool)
    






############################################### nova parte ########################################################################################################

'''
'''

async def novo_verificar_e_obter_coeficiente_novo(cod_equipamento, pool):
    try:
        coeficientes = {}
        interceptos = {}
        
        async with pool.acquire() as conn:
            await conn.ping(reconnect=True)  # Forçar reconexão
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
            await conn.ping(reconnect=True)  # Forçar reconexão
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
        await conn.ping(reconnect=True)  # Forçar reconexão
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

    # min_load_speed = parametros_motor_normalizado.get('Load Speed', {}).get('min', 5)
    # max_load_speed = parametros_motor_normalizado.get('Load Speed', {}).get('max', 80)

    # min_temp_ar_admissao = parametros_motor_normalizado.get('Temperatura do ar de admissão', {}).get('min', 20)
    # max_temp_ar_admissao = parametros_motor_normalizado.get('Temperatura do ar de admissão', {}).get('max', 100)

    # min_pressao_admissao = parametros_motor_normalizado.get('Pressão de admissão', {}).get('min', 0)
    # max_pressao_admissao = parametros_motor_normalizado.get('Pressão de admissão', {}).get('max', 6)

    # min_potencia_ativa = parametros_motor_normalizado.get('Potência Ativa', {}).get('min', 20)
    # max_potencia_ativa = parametros_motor_normalizado.get('Potência Ativa', {}).get('max', 400)

    # min_pressao_oleo = parametros_motor_normalizado.get('Pressão do Óleo', {}).get('min', 3.5)
    # max_pressao_oleo = parametros_motor_normalizado.get('Pressão do Óleo', {}).get('max', 5.0)

    # min_rpm = parametros_motor_normalizado.get('RPM', {}).get('min', 1798)
    # max_rpm = parametros_motor_normalizado.get('RPM', {}).get('max', 1802)

    # min_temp_agua = parametros_motor_normalizado.get('Temperatura da água', {}).get('min', 103)
    # max_temp_agua = parametros_motor_normalizado.get('Temperatura da água', {}).get('max', 103)
    # alerta_temp_agua = parametros_motor_normalizado.get('Temperatura da água', {}).get('alerta', 90)

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
        #    'max': potencia_nominal * 0.7 if potencia_nominal else parametros_motor_normalizado.get('Potência Ativa', {}).get('max', 400)
            'max': round(potencia_nominal * 0.8, 1) if potencia_nominal else round(parametros_motor_normalizado.get('Potência Ativa', {}).get('max', 400), 1)
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

    print('\n')
    for sensor, limite in limites.items():
        print(f"{sensor}: Min: {limite['min']}, Max: {limite['max']}")
        
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

# Função auxiliar para conversão segura de string para float
def converter_para_float(valor):
    try:
        return float(valor)
    except (ValueError, TypeError):
        return None
    
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
            await conn.ping(reconnect=True)  # Forçar reconexão
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
                    """
                    SELECT data_cadastro 
                    FROM machine_learning.leituras_consecutivas 
                    WHERE cod_equipamento = %s AND cod_campo = 3
                    """,
                    (cod_equipamento,)
                )
                data_cadastro = await cursor.fetchone()

                if data_cadastro:
                    data_cadastro = data_cadastro[0]

                agora = datetime.now()

                # Verificar se o data_cadastro está ausente ou desatualizado em mais de 1 hora
                if not data_cadastro or (agora - data_cadastro > timedelta(hours=1)):
                    await cursor.execute("""
                        UPDATE machine_learning.leituras_consecutivas
                        SET alerta = 0
                        WHERE cod_equipamento = %s
                    """, (cod_equipamento,))
                    await conn.commit()
                #    print(f"Equipamento {cod_equipamento}: alerta setado para 0, mais de 1 hora sem atualização.")
                    return {}, {}, {}, {}, {}, {}, {}, {}, {}, {}

                # Verificar se a diferença é maior que 15 minutos
                if (agora - data_cadastro) > timedelta(minutes=15):
                #    print(f"Equipamento {cod_equipamento}: Ignorado, data_cadastro é mais de 15 minutos atrás.")
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
                #    potencia_nominal = 0.9 * potencia_nominal_atual  # Calcula 90% da Potência Nominal
                    potencia_nominal = potencia_nominal_atual  # Calcula 90% da Potência Nominal
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

                # # Verificar limites para os valores previstos, comparando com os valores reais
                # for sensor_nome, valores_previsao in previsoes.items():
                #     contagem_acima_do_limite_previsao[sensor_nome] = 0
                #     contagem_abaixo_do_limite_previsao[sensor_nome] = 0

                #     # Verificar se o sensor possui um limite de tolerância definido no dicionário
                #     if sensor_nome in limites_tolerancia and sensor_nome in valores_atuais:
                #         limite_tolerancia = limites_tolerancia[sensor_nome]
                #         valores_atuais_sensor = valores_atuais[sensor_nome]  # Os valores reais

                #         for valor_previsao, valor_atual in zip(valores_previsao, valores_atuais_sensor):
                #             # Definir os limites com base no valor previsto
                #             limite_mais_previsao = valor_previsao + limite_tolerancia
                #             limite_menos_previsao = valor_previsao - limite_tolerancia

                #             # Verificar se o valor atual excede esses limites
                #             if valor_atual > limite_mais_previsao:
                #                 contagem_acima_do_limite_previsao[sensor_nome] += 1
                #             if valor_atual < limite_menos_previsao:
                #                 contagem_abaixo_do_limite_previsao[sensor_nome] += 1
                #     else:
                #         print(f"Limite de tolerância ou valores atuais não definidos para o sensor: {sensor_nome}")


                #     # Ajuste os contadores para 0 ou 1
                #     contagem_acima_do_limite_previsao[sensor_nome] = 1 if contagem_acima_do_limite_previsao[sensor_nome] > contagem_limites else 0
                #     contagem_abaixo_do_limite_previsao[sensor_nome] = 1 if contagem_abaixo_do_limite_previsao[sensor_nome] > contagem_limites else 0
                #     contagem_acima_do_limite[sensor_nome] = 1 if contagem_acima_do_limite.get(sensor_nome, 0) > contagem_limites else 0
                #     contagem_abaixo_do_limite[sensor_nome] = 1 if contagem_abaixo_do_limite.get(sensor_nome, 0) > contagem_limites else 0



                # Verificar limites para os valores previstos, comparando com os valores reais
                for sensor_nome, valores_previsao in previsoes.items():
                    contagem_acima_do_limite_previsao[sensor_nome] = 0
                    contagem_abaixo_do_limite_previsao[sensor_nome] = 0

                    # Verificar se o sensor possui um limite de tolerância definido e se os valores atuais existem
                    if sensor_nome in limites_tolerancia:
                        limite_tolerancia = limites_tolerancia[sensor_nome]
                        
                        # Verifique se os valores atuais para o sensor existem
                        if sensor_nome not in valores_atuais:
                            print(f"Valores atuais não disponíveis para o sensor: {sensor_nome}")
                            continue
                        
                        valores_atuais_sensor = valores_atuais[sensor_nome]  # Os valores reais

                        # Certifique-se de que os valores previstos e atuais são listas e têm o mesmo comprimento
                        if not isinstance(valores_previsao, list) or not isinstance(valores_atuais_sensor, list):
                            print(f"Os valores para {sensor_nome} não são listas ou estão incorretos.")
                            continue

                        for valor_previsao, valor_atual in zip(valores_previsao, valores_atuais_sensor):
                            # Converter para float
                            valor_previsao = converter_para_float(valor_previsao)
                            valor_atual = converter_para_float(valor_atual)

                            # Verificar se a conversão foi bem-sucedida
                            if valor_previsao is None or valor_atual is None:
                                print(f"Erro ao converter valores para float no sensor: {sensor_nome}")
                                continue

                            # Definir os limites com base no valor previsto
                            limite_mais_previsao = valor_previsao + limite_tolerancia
                            limite_menos_previsao = valor_previsao - limite_tolerancia

                            # Verificar se o valor atual excede esses limites
                            if valor_atual > limite_mais_previsao:
                                contagem_acima_do_limite_previsao[sensor_nome] += 1
                            if valor_atual < limite_menos_previsao:
                                contagem_abaixo_do_limite_previsao[sensor_nome] += 1
                    else:
                        print(f"Limite de tolerância não definido para o sensor: {sensor_nome}")

                # Ajuste os contadores para 0 ou 1
                for sensor_nome in sensores.values():
                    contagem_acima_do_limite_previsao[sensor_nome] = 1 if contagem_acima_do_limite_previsao.get(sensor_nome, 0) > contagem_limites else 0
                    contagem_abaixo_do_limite_previsao[sensor_nome] = 1 if contagem_abaixo_do_limite_previsao.get(sensor_nome, 0) > contagem_limites else 0
                    contagem_acima_do_limite[sensor_nome] = 1 if contagem_acima_do_limite.get(sensor_nome, 0) > contagem_limites else 0
                    contagem_abaixo_do_limite[sensor_nome] = 1 if contagem_abaixo_do_limite.get(sensor_nome, 0) > contagem_limites else 0




           #     for sensor in ['Load Speed', 'Potência Ativa', 'Pressão do Óleo', 'Temperatura da Água', 'Pressão de admissão', 'Temperatura do ar de admissão', 'Tensao L1-L2', 'Tensao L2-L3', 'Tensao L3-L1', 'Corrente L1', 'Corrente L2', 'Corrente L3', 'Frequencia', 'Tensao Bateria', 'RPM', 'Consumo', 'Horimetro', 'Temperatura da Água']:
                for sensor in ['Load Speed', 'Potência Ativa', 'Pressão do Óleo', 'Temperatura da Água', 'Pressão de admissão', 'Temperatura do ar de admissão']:
                    if sensor in valores_atuais:
                        print(f'\n{sensor} Atual:', valores_atuais[sensor])
                        print(f'Real Acima do Limite:', contagem_acima_do_limite.get(sensor, 0))
                        print(f'Real Abaixo do Limite:', contagem_abaixo_do_limite.get(sensor, 0))
                    if sensor in previsoes:
                        print(f'{sensor} Previsto:', previsoes[sensor])
                        print(f'Previsão Acima do Limite:', contagem_acima_do_limite_previsao.get(sensor, 0))
                        print(f'Previsão Abaixo do Limite:', contagem_abaixo_do_limite_previsao.get(sensor, 0))

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
    #    'previsao': {
        #     'acima': {
        #         'Pressão do Óleo': {'tipo': 'previsao', 'condicao': 'abaixo'}
        #     },
        #     'abaixo': {
        #         'Pressão do Óleo': {'tipo': 'previsao', 'condicao': 'abaixo'}
        #     }
    #    }
        'real': {
            'acima': {
                'Potência Ativa': {'tipo': 'real', 'condicao': 'acima'},
                'Pressão de admissão': {'tipo': 'real', 'condicao': 'acima'},
            },
        #     'abaixo': {
        #         'Pressão do Óleo': {'tipo': 'real', 'condicao': 'abaixo'}
        #     }
        }
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
            'abaixo': {
                'Pressão de admissão': {'tipo': 'real', 'condicao': 'acima'},
#                'Temperatura da Água': {'tipo': 'real', 'condicao': 'acima'}
                },
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



async def novo_enviar_previsao_valor_equipamento_alerta_novo(cod_equipamentos, tabelas, cod_campo_especificados, pool):
    equipamentos_alertados = []  # Lista para armazenar equipamentos que já foram alertados
    usinas_alertadas = {}  # Dicionário para armazenar usinas e os equipamentos em alerta
    usinas_enviadas = set()  # Conjunto para rastrear usinas que já foram alertadas
    contagem_ativos_por_usina = {}
    usuarios_bloqueados = set()

    contador_loop = 0  # Inicializando o contador de loops
    global equipamentos_ativos
    
    while True:
        contador_loop += 1  # Incrementa o contador no início de cada loop

        tempo_inicial = datetime.now()
        data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')
        print('\n',f'loop número {contador_loop} ------------------------------- inicio novo_enviar_previsao_valor_equipamento_alerta_novo -----------------------------------------------------', data_cadastro_formatada, '\n')

        try:

            if not equipamentos_ativos:
                await carregar_equipamentos_ativos(pool)
                await carregar_parametros_min_max(pool)

            await carregar_equipamentos_ativos(pool)
            await carregar_parametros_min_max(pool)

        except Exception as e:
            print(f"Erro ao processar novo_enviar_previsao_valor_equipamento_alerta_novo equipamento {cod_equipamento}: {str(e)}")

        for cod_equipamento in cod_equipamentos:
            try:
                async with pool.acquire() as conn:
                    await conn.ping(reconnect=True)  # Forçar reconexão
                    async with conn.cursor() as cursor:

                        await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (cod_equipamento,))
                        valores_atuais_114 = await cursor.fetchone()
                                        
                        if valores_atuais_114 is None:
                            continue

                        # Verifica se todos os valores são 0
                        if all(valor == 0 for valor in valores_atuais_114):
                            if cod_equipamento in equipamentos_alertados:
                                # Remove o equipamento da lista se já foi alertado e todos os valores são 0
                                equipamentos_alertados.remove(cod_equipamento)
                                print(f"Equipamento {cod_equipamento} removido da lista de alertas pois todos os valores são 0.")
                                
                                # Remove o equipamento da contagem de ativos na usina
                                await cursor.execute(
                                    "SELECT cod_usina FROM sup_geral.equipamentos WHERE codigo = %s",
                                    (cod_equipamento,)
                                )
                                resultado_usina = await cursor.fetchone()
                                if resultado_usina:
                                    cod_usina = resultado_usina[0]
                                    if cod_usina in contagem_ativos_por_usina:
                                        contagem_ativos_por_usina[cod_usina] -= 1
                                        if contagem_ativos_por_usina[cod_usina] <= 0:
                                            del contagem_ativos_por_usina[cod_usina]
                                            print(f"Usina {cod_usina} removida da contagem de ativos pois todos os equipamentos estão com valores zero.")
                            continue

                        # Obter o valor de cod_campo 3
                        await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_campo = 3 AND cod_equipamento = %s", (cod_equipamento,))
                        valores_atuais_3 = await cursor.fetchone()

                        # Verificar se cod_campo 3 é None ou se todos os valores são zero, caso seja, pule para a próxima iteração
                        if valores_atuais_3 is None or all(valor == 0 for valor in valores_atuais_3):
                            continue

                        # Executar a consulta para obter o cod_usina
                        await cursor.execute(
                            "SELECT cod_usina FROM sup_geral.equipamentos WHERE codigo = %s",
                            (cod_equipamento,)
                        )
                        resultado = await cursor.fetchone()

                        # Verifica se a consulta retornou um resultado
                        if resultado is not None:
                            cod_usina = resultado[0]  # Obtém o primeiro elemento da tupla
                        else:
                            print(f"Nenhuma usina encontrada para o equipamento {cod_equipamento}. Pulando...")
                            continue  # Pule para a próxima iteração se não houver usina

                        # Verificar se o equipamento já foi alertado
                        if cod_equipamento in [equip['cod_equipamento'] for equip in equipamentos_alertados]:
                            print(f"Equipamento {cod_equipamento} já foi alertado anteriormente.")

                            # Verificar se a usina já está no dicionário de alertas
                            if cod_usina not in usinas_alertadas:
                                usinas_alertadas[cod_usina] = []

                            # Adicionar o equipamento na lista de alertas da usina, se ainda não estiver presente
                            if cod_equipamento not in usinas_alertadas[cod_usina]:
                                usinas_alertadas[cod_usina].append(cod_equipamento)

                                # Calcula e armazena a contagem de equipamentos ativos para a usina atual apenas uma vez
                                if cod_usina not in contagem_ativos_por_usina:
                                    contagem_ativos_por_usina[cod_usina] = sum(
                                        equip.get('contagem_equipamentos_ativos', 0)
                                        for equip in equipamentos_alertados
                                        if equip.get('cod_usina') == cod_usina
                                    )

                                # Recupera a contagem de equipamentos ativos da usina
                                contagem_equipamentos_ativos = contagem_ativos_por_usina[cod_usina]

                                # Calcular o percentual de equipamentos alertados
                                percent_equipamentos_alertados = (
                                    len(usinas_alertadas[cod_usina]) / contagem_equipamentos_ativos
                                ) * 100
                                
                                print(f"(equipamento {cod_equipamento} usina {cod_usina}) contagem_equipamentos_ativos {contagem_equipamentos_ativos}, equipamentos alertados {usinas_alertadas[cod_usina]}, percent_equipamentos_alertados {percent_equipamentos_alertados}")

                                # Verificar a condição de alerta para enviar a mensagem
                                if percent_equipamentos_alertados >= 15 and cod_usina not in usinas_enviadas:

                                    # Buscar o nome e o código do modelo de funcionamento da usina
                                    await cursor.execute("SELECT nome, cod_modelo_funcionamento FROM sup_geral.usinas WHERE codigo = %s", (cod_usina,))
                                    result = await cursor.fetchone()
                                    if result:
                                        nome_usina, cod_modelo_funcionamento = result
                                        print('equipamento', cod_equipamento, 'nome usina', nome_usina, 'o código de funcionamento é:', cod_modelo_funcionamento)

                                        # Buscar todos os cod_usuario associados à usina
                                        await cursor.execute("SELECT cod_usuario FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario != 0 AND cod_usina = %s", (cod_usina,))
                                        cod_usuarios = await cursor.fetchall()

                                        if cod_usuarios:
                                            nomes_usuarios = []

                                            for cod_usuario_tuple in cod_usuarios:
                                                cod_usuario = cod_usuario_tuple[0]

                                                # Verificar se o usuário não está silenciado
                                                if (cod_usina, cod_usuario) not in sem_mensagem_silenciado:

                                                    # Verificar se a usina está ativa para o usuário
                                                    await cursor.execute("SELECT ativo FROM machine_learning.usinas_usuario WHERE cod_usina = %s AND cod_usuario = %s", (cod_usina, cod_usuario))
                                                    usina_ativa_row = await cursor.fetchone()
                                            #        print("(loop 1) A usina ",cod_usina, " Esta ativa? ",usina_ativa_row,' para o usuario ',cod_usuario)

                                                    if usina_ativa_row and usina_ativa_row[0] == 1:
                                                        await cursor.execute("SELECT id_telegram, usuario, ativo, telefone FROM machine_learning.usuarios_telegram WHERE cod_usuario = %s", (cod_usuario,))
                                                        result = await cursor.fetchone()
                                                        
                                                        if result:
                                                            id_telegram, nome_usuario, ativo, telefone = result
                                                            if ativo == 1:
                                                                try:
                                                                #    nomes_usuarios.append(nome_usuario)

                                                                    # Recuperar nome_usina da lista equipamentos_alertados
                                                                    nome_usina = next(
                                                                        (equip['nome_usina'] for equip in equipamentos_alertados if equip['cod_usina'] == cod_usina),
                                                                        "Nome da usina não encontrado"
                                                                    )

                                                                    mensagens_alertas = [
                                                                        equip['mensagem'] for equip in equipamentos_alertados
                                                                        if equip['cod_equipamento'] in usinas_alertadas[cod_usina]
                                                                    ]

                                                                    usinas_enviadas.add(cod_usina)  # Marca a usina como já alertada

                                                                    if cod_modelo_funcionamento not in [4, 12, 14]:

                                                                        nomes_usuarios.append(nome_usuario)
                                                                        
                                                                        # Constrói a mensagem com os equipamentos e as mensagens de alerta
                                                                        mensagem = (
                                                                            f"🔴 <b>ALERTA!</b> \n\nUsina: {cod_usina} - {nome_usina}\n"
                                                                            f"Equipamentos em alerta: {', '.join(str(equip) for equip in equipamentos_alertados)}\n"
                                                                        )

                                                                        # Adiciona as mensagens de alerta, se disponíveis
                                                                        if mensagens_alertas:
                                                                            mensagem += "\nMensagens de Alerta:\n" + "\n".join(f"- {msg}" for msg in mensagens_alertas)

                                                                        mensagem += (
                                                                            f'\n<a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para a usina</a>'
                                                                        )

                                                                        # Envia a mensagem formatada
                                                                        await bot.send_message(id_telegram, mensagem, parse_mode='HTML')


                                                                        print('inicio do payload previsao novo')

                                                                        # Executa a consulta no banco
                                                                    #    await cursor.execute("SELECT telefone FROM machine_learning.usuarios_telegram WHERE cod_usuario = 374")
                                                                        # Retorna um único registro como tupla
                                                                    #    telefone = await cursor.fetchone()

                                                                        # Extrai o valor da tupla
                                                                        if telefone:  # Verifica se telefone não é None

                                                                            # Se o telefone é uma tupla, pega o primeiro elemento
                                                                            if isinstance(telefone, tuple):
                                                                                telefone = telefone[0]        
                                                                                                                                                      
                                                                            telefone_formatado = telefone.strip().replace(" ", "").replace("-", "")  # Formata o telefone
                                                                            if not telefone_formatado.startswith("+"):
                                                                                telefone_formatado = f"+{telefone_formatado}"  # Adiciona o "+" no início, se necessário

                                                                            # Monta a mensagem
                                                                            mensagem_final_wtss = (
                                                                                f"🟡 ALERTA! \n\n"
                                                                                f"Usina: {cod_usina} - {nome_usina}\n"
                                                                    #            f"Ir para o equipamento: https://supervisorio.brggeradores.com.br/beta/detalhesgmg.php?codUsina={cod_usina}&codEquip={cod_equipamento}\n\n"
                                                                                f"Equipamento: {cod_equipamento} ({nome_equipamento})\n"
                                                                                f"Previsão para {chave_principal} acima do limite.\n"
                                                                                f"Valores reais: {valores_reais_formatados}\n"
                                                                                f"Valores previstos: {valores_previstos_formatados}\n\n"
                                                                                f"Parâmetros fora do padrão: {', '.join(condicoes_acima)}\n"
                                                                                f"Valores reais: {valores_reais_subparametro}\n"
                                                                                f"Valores previstos: {valores_previstos_subparametro}\n"
                                                                            )

                                                                            # Monta o payload
                                                                            payload = {
                                                                                "number": telefone_formatado,
                                                                                "textMessage": {
                                                                                    "text": mensagem_final_wtss
                                                                                }
                                                                            }
                                                                        else:
                                                                            print("Nenhum telefone encontrado para o usuário.")

                                                                        start_chat(payload)  # Chama a função para enviar via WhatsApp
                                                                        print('payload',payload)
                                                                        print('fim do payload previsao novo')
                                                
                                                
                                                                    else:                                                                    
                                                                        print(f"🔴 Alerta nao foi enviado pois o funcionamento nao e autorizado enviado para usina {cod_usina} com {percent_equipamentos_alertados}% dos equipamentos em alerta.")

                                                                    if cod_usuario in usuarios_bloqueados:
                                                                        usuarios_bloqueados.remove(cod_usuario)
                                                                        id_grupo = await id_chat_grupo(pool)
                                                                        if id_grupo is not None:
                                                                            await bot.send_message(id_grupo, f"🚫 🟢 O bot foi desbloqueado pelo usuário {nome_usuario} ({cod_usuario})")
                                                                        await cursor.execute("UPDATE machine_learning.usuarios_telegram SET bloqueado = 0 WHERE cod_usuario = %s", (cod_usuario,))
                                                                        await conn.commit()
                                                                
                                                                except BotBlocked:
                                                                    if cod_usuario not in usuarios_bloqueados:
                                                                        usuarios_bloqueados.add(cod_usuario)
                                                                        id_grupo = await id_chat_grupo(pool)
                                                                        if id_grupo is not None:
                                                                            await bot.send_message(id_grupo, f"🚫 O bot foi bloqueado pelo usuário {nome_usuario} ({cod_usuario})")
                                                                        await cursor.execute("UPDATE machine_learning.usuarios_telegram SET bloqueado = 1 WHERE cod_usuario = %s", (cod_usuario,))
                                                                        await conn.commit()

                                            nomes_usuarios_str = ', '.join(nomes_usuarios)
                                            id_grupo = await id_chat_grupo(pool)

                                            # Verificar se há mensagens de alerta na lista
                                            if mensagens_alertas:
                                                texto_alertas = "\n".join(f"- {msg}" for msg in mensagens_alertas)
                                            else:
                                                texto_alertas = "Nenhum alerta específico encontrado."

                                            # Montar a mensagem de alerta para o grupo
                                            mensagem_alerta_grupo = (
                                                f"🔴 <b>ALERTA ENVIADO!</b>\n\n"
                                                f"<b>Usuários Notificados:</b> {nomes_usuarios_str}\n\n"
                                                f"Equipamento: {cod_equipamento}\n"
                                                f"{texto_alertas}\n\n"
                                                f'<a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para a usina</a>'
                                            )

                                            # Enviar a mensagem
                                            await bot.send_message(id_grupo, mensagem_alerta_grupo, parse_mode='HTML')

                                            nomes_usuarios.clear()
                                            sys.stdout.flush()
                            continue


                        # Função para fazer previsões e obter as contagens
                        nome_equipamento, nome_usina, cod_usina, valores_atuais, previsoes, contagem_abaixo_do_limite, contagem_acima_do_limite, contagem_abaixo_do_limite_previsao, contagem_acima_do_limite_previsao, contagem_equipamentos_ativos = await novo_fazer_previsao_sempre_alerta_novo(cod_equipamento, pool)
                #        print('cod_equipamento',cod_equipamento,'nome_equipamento',nome_equipamento,'contagem_equipamentos_ativos',contagem_equipamentos_ativos)
                        
                        if previsoes is None or contagem_abaixo_do_limite is None or contagem_acima_do_limite is None:
                            continue
                #        print('cod_equipamento',cod_equipamento,'passou o if previsoes')

                        # Verificar as condições com base no dicionário lista_parametros_previsao
                        for chave_principal, condicoes in lista_parametros_previsao.items():
                #            print('chave_principal 1',chave_principal)
                #            print('condicoes 1',condicoes)

                            # if chave_principal == "Load Speed":
                            #     # Obter o valor atual de 'Load Speed'
                            #     load_speed_value = valores_atuais.get("Load Speed", 0)

                            #     # Se o valor for uma lista, pegue o ultimo elemento, senão use o valor diretamente
                            #     if isinstance(load_speed_value, list):
                            #         load_speed_value = load_speed_value[4] if load_speed_value else 0

                            #     # Verifica se o valor atual de 'Load Speed' está acima de 50
                            #     if load_speed_value < 50:
                            #         print(cod_equipamento, 'valor de load speed menor que 50', load_speed_value)
                            #         # Se o valor de 'Load Speed' for menor que 50, pula para o próximo parâmetro
                            #         continue


                            if chave_principal in previsoes:
                                print(f'\nChave principal: {chave_principal}')

                                # Verificar previsões e valores reais
                                for tipo_condicao in ['previsao', 'real']:
                                    if tipo_condicao in condicoes:
                                        tipo_chave_principal = tipo_condicao  # Definir se é previsao ou real
                                        print(f"Tipo de Condição: {tipo_chave_principal}")

                                        # Escolher as variáveis de contagem com base no tipo da chave principal
                                        if tipo_chave_principal == 'real':
                                            contagem_acima = contagem_acima_do_limite
                                            contagem_abaixo = contagem_abaixo_do_limite
                                        elif tipo_chave_principal == 'previsao':
                                            contagem_acima = contagem_acima_do_limite_previsao
                                            contagem_abaixo = contagem_abaixo_do_limite_previsao

                                        # Verificar 'acima do limite'
                                        if contagem_acima.get(chave_principal, 0) == 1:
                                            condicoes_acima = condicoes[tipo_chave_principal].get('acima', {})
                                            
                                            if not condicoes_acima:  # Verifica se condicoes_acima está vazio
                                                valores_reais_formatados = ', '.join(map(str, valores_atuais.get(chave_principal, ['N/A'])))
                                                valores_previstos_formatados = ', '.join(map(str, previsoes.get(chave_principal, ['N/A'])))
                                                mensagem = (
                                                    f"🟡 <b>ALERTA!</b> \n\nUsina: {cod_usina} - {nome_usina}\n"
                                                    f'<a href="https://supervisorio.brggeradores.com.br/beta/detalhesgmg.php?codUsina={cod_usina}&codEquip={cod_equipamento}">Ir para o equipamento</a>\n\n'
                                                    f"Equipamento: {cod_equipamento} ({nome_equipamento})\n"
                                                    f"Previsão para {chave_principal} acima do limite.\n"
                                                    f"Valores reais: {valores_reais_formatados}\n"
                                                    f"Valores previstos: {valores_previstos_formatados}\n\n"
                                                )
                                            #    await bot.send_message(id_usuario, mensagem, parse_mode='HTML')
                                                print(f'enviando mensagem {mensagem}')

                                                equipamentos_alertados.append({
                                                    'cod_equipamento': cod_equipamento,
                                                    'mensagem': mensagem,
                                                    'contagem_equipamentos_ativos': contagem_equipamentos_ativos,
                                                    'cod_usina': cod_usina,
                                                    'nome_usina': nome_usina,
                                                    })

                                                await cursor.execute("""
                                                    UPDATE machine_learning.leituras_consecutivas
                                                    SET alerta = 1
                                                    WHERE cod_equipamento = %s
                                                """, (cod_equipamento,))
                                                await conn.commit()
                                                                                            
                                                print(f"Equipamento {cod_equipamento} adicionado à lista de alertados sem condições específicas.")
                                                continue  # Vai para o próximo equipamento

                                            # Caso existam condicoes_acima, verificar parâmetros como antes
                                            todos_parametros_ok = True
                                            parametros_violados = []

                                            for subparametro, detalhes in condicoes_acima.items():
                                                tipo = detalhes['tipo']
                                                condicao = detalhes['condicao']

                                                if tipo == 'real':
                                                    valor_real = valores_atuais.get(subparametro, "N/A")
                                                    valor_previsto = previsoes.get(subparametro, "N/A")

                                                    if condicao == 'acima' and contagem_acima_do_limite.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado acima, atual: {valor_real}, previsto: {valor_previsto})")
                                        #                print(f"Falha no parâmetro {subparametro}: esperado 'acima', mas não está. \n(Atual: {valor_real}, previsto: {valor_previsto})")
                                                        break
                                                    elif condicao == 'abaixo' and contagem_abaixo_do_limite.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado abaixo, atual: {valor_real}, previsto: {valor_previsto})")
                                        #                print(f"Falha no parâmetro {subparametro}: esperado 'abaixo', mas não está. \n(Atual: {valor_real}, previsto: {valor_previsto})")
                                                        break

                                                elif tipo == 'previsao':
                                                    valor_previsto = previsoes.get(subparametro, "N/A")

                                                    if condicao == 'acima' and contagem_acima_do_limite_previsao.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado acima na previsão, previsto: {valor_previsto})")
                                        #                print(f"Falha no parâmetro {subparametro}: esperado 'acima' na previsão, mas não está. \n(Atual: {valor_real}, previsto: {valor_previsto})")
                                                        break
                                                    elif condicao == 'abaixo' and contagem_abaixo_do_limite_previsao.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado abaixo na previsão, previsto: {valor_previsto})")
                                        #                print(f"Falha no parâmetro {subparametro}: esperado 'abaixo' na previsão, mas não está. \n(Atual: {valor_real}, previsto: {valor_previsto})")
                                                        break

                                            if todos_parametros_ok:
                                                valores_reais_formatados = ', '.join(map(str, valores_atuais.get(chave_principal, ['N/A'])))
                                                valores_previstos_formatados = ', '.join(map(str, previsoes.get(chave_principal, ['N/A'])))
                                                valores_reais_subparametro = ', '.join(map(str, valores_atuais.get(subparametro, ['N/A'])))
                                                valores_previstos_subparametro = ', '.join(map(str, previsoes.get(subparametro, ['N/A'])))

                                                mensagem = (
                                                    f"🟡 <b>ALERTA!</b> \n\nUsina: {cod_usina} - {nome_usina}\n"
                                                    f'<a href="https://supervisorio.brggeradores.com.br/beta/detalhesgmg.php?codUsina={cod_usina}&codEquip={cod_equipamento}">Ir para o equipamento</a>\n\n'
                                                    f"Equipamento: {cod_equipamento} ({nome_equipamento})\n"
                                                    f"Previsão para {chave_principal} acima do limite.\n"
                                                    f"Valores reais: {valores_reais_formatados}\n"
                                                    f"Valores previstos: {valores_previstos_formatados}\n\n"
                                                    f"Parâmetros fora do padrão: {', '.join(condicoes_acima)}\n"
                                                    f"Valores reais: {valores_reais_subparametro}\n"
                                                    f"Valores previstos: {valores_previstos_subparametro}\n"
                                                )
                                            #    await bot.send_message(id_usuario, mensagem, parse_mode='HTML')
                                                print(f'enviando mensagem {mensagem}')

                                                # Adicionar o equipamento à lista de alertados
                                                equipamentos_alertados.append({
                                                    'cod_equipamento': cod_equipamento,
                                                    'mensagem': mensagem,
                                                    'contagem_equipamentos_ativos': contagem_equipamentos_ativos,
                                                    'cod_usina': cod_usina,
                                                    'nome_usina': nome_usina,
                                                })

                                                await cursor.execute("""
                                                    UPDATE machine_learning.leituras_consecutivas
                                                    SET alerta = 1
                                                    WHERE cod_equipamento = %s
                                                """, (cod_equipamento,))
                                                await conn.commit()
                                                
                                                print(f"Equipamento {cod_equipamento} adicionado à lista de alertados.")


                                        # Verificar 'abaixo do limite'
                                        if contagem_abaixo.get(chave_principal, 0) == 1:
                                            condicoes_abaixo = condicoes[tipo_chave_principal].get('abaixo', {})
                                            print(f"Condições 'abaixo': {condicoes_abaixo}")
                                            todos_parametros_ok = True
                                            parametros_violados = []
                                            subparametro = None  # Inicializa subparametro com um valor padrão

                                            for subparametro, detalhes in condicoes_abaixo.items():
                                                tipo = detalhes['tipo']
                                                condicao = detalhes['condicao']

                                                # Verificar contagem com base no tipo
                                                if tipo == 'real':
                                                    valor_real = valores_atuais.get(subparametro, "N/A")
                                                    valor_previsto = previsoes.get(subparametro, "N/A")

                                                    if condicao == 'acima' and contagem_acima_do_limite.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado acima, atual: {valor_real}, previsto: {valor_previsto})")
                                        #                print(f"Falha no parâmetro {subparametro}: esperado 'acima', mas não está. \n(Atual: {valor_real}, previsto: {valor_previsto})")
                                                        break
                                                    elif condicao == 'abaixo' and contagem_abaixo_do_limite.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado abaixo, atual: {valor_real}, previsto: {valor_previsto})")
                                        #                print(f"Falha no parâmetro {subparametro}: esperado 'abaixo', mas não está. \n(Atual: {valor_real}, previsto: {valor_previsto})")
                                                        break

                                                elif tipo == 'previsao':
                                                    valor_previsto = previsoes.get(subparametro, "N/A")

                                                    if condicao == 'acima' and contagem_acima_do_limite_previsao.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado acima na previsão, previsto: {valor_previsto})")
                                        #                print(f"Falha no parâmetro {subparametro}: esperado 'acima' na previsão, mas não está. \n(Atual: {valor_real}, previsto: {valor_previsto})")
                                                        break
                                                    elif condicao == 'abaixo' and contagem_abaixo_do_limite_previsao.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado abaixo na previsão, previsto: {valor_previsto})")
                                        #                print(f"Falha no parâmetro {subparametro}: esperado 'abaixo' na previsão, mas não está. \n(Atual: {valor_real}, previsto: {valor_previsto})")
                                                        break

                                            if todos_parametros_ok:
                                                # Transformando as listas de valores em strings sem colchetes
                                                valores_reais_formatados = ', '.join(map(str, valores_atuais.get(chave_principal, ['N/A'])))
                                                valores_previstos_formatados = ', '.join(map(str, previsoes.get(chave_principal, ['N/A'])))
                                                valores_reais_subparametro = ', '.join(map(str, valores_atuais.get(subparametro, ['N/A'])))
                                                valores_previstos_subparametro = ', '.join(map(str, previsoes.get(subparametro, ['N/A'])))

                                                # Verifica se condicoes_abaixo está vazio
                                                if not condicoes_abaixo:
                                                    mensagem = (
                                                        f"🟡 <b>ALERTA!</b> \n\nUsina: {cod_usina} - {nome_usina}\n"
                                                        f'<a href="https://supervisorio.brggeradores.com.br/beta/detalhesgmg.php?codUsina={cod_usina}&codEquip={cod_equipamento}">Ir para o equipamento</a>\n\n'
                                                        f"Equipamento: {cod_equipamento} ({nome_equipamento})\n"
                                                        f"Previsão para {chave_principal} abaixo do limite.\n"
                                                        f"Valores reais: {valores_reais_formatados}\n"
                                                        f"Valores previstos: {valores_previstos_formatados}\n"
                                                    )
                                                else:
                                                    mensagem = (
                                                        f"🟡 <b>ALERTA!</b> \n\nUsina: {cod_usina} - {nome_usina}\n"
                                                        f'<a href="https://supervisorio.brggeradores.com.br/beta/detalhesgmg.php?codUsina={cod_usina}&codEquip={cod_equipamento}">Ir para o equipamento</a>\n\n'
                                                        f"Equipamento: {cod_equipamento} ({nome_equipamento})\n"
                                                        f"Previsão para {chave_principal} abaixo do limite.\n"
                                                        f"Valores reais: {valores_reais_formatados}\n"
                                                        f"Valores previstos: {valores_previstos_formatados}\n\n"
                                                        f"Parâmetros fora do padrão: {', '.join(condicoes_abaixo)}\n"
                                                        f"Valores reais: {valores_reais_subparametro}\n"
                                                        f"Valores previstos: {valores_previstos_subparametro}\n"
                                                    )

                                                print(f'enviando mensagem {mensagem}')
                                            #    await bot.send_message(id_usuario, mensagem, parse_mode='HTML')

                                                # Adicionar o equipamento à lista de alertados
                                                equipamentos_alertados.append({
                                                    'cod_equipamento': cod_equipamento,
                                                    'mensagem': mensagem,
                                                    'contagem_equipamentos_ativos': contagem_equipamentos_ativos,
                                                    'cod_usina': cod_usina,
                                                    'nome_usina': nome_usina,
                                                    })

                                                await cursor.execute("""
                                                    UPDATE machine_learning.leituras_consecutivas
                                                    SET alerta = 1
                                                    WHERE cod_equipamento = %s
                                                """, (cod_equipamento,))
                                                await conn.commit()
                                                
                                                print(f"Equipamento {cod_equipamento} adicionado à lista de alertados.")
                            else:
    #                            print('sem chave principal',chave_principal)
                                pass
                                
            except Exception as e:
                print(f"Erro ao processar novo_enviar_previsao_valor_equipamento_alerta_novo equipamento {cod_equipamento}: {str(e)}")


        tempo_final = datetime.now()
        data_cadastro_formatada_final = tempo_final.strftime('%d-%m-%Y %H:%M')

        # Calcular o tempo de execução
        tempo_execucao = tempo_final - tempo_inicial

        # Formatando a diferença de tempo
        segundos = tempo_execucao.total_seconds()
        horas = int(segundos // 3600)
        minutos = int((segundos % 3600) // 60)
        segundos_restantes = int(segundos % 60)

        # Exibindo o tempo total de execução
        print('\n',f'loop número {contador_loop} ------------------------------------ fim novo_enviar_previsao_valor_equipamento_alerta_novo -----------------------------------------------------', data_cadastro_formatada_final)
        print(f'Tempo de execução do novo_enviar_previsao_valor_equipamento_alerta_novo: {horas} horas, {minutos} minutos, {segundos_restantes} segundos\n')

        await asyncio.sleep(30)



    ############################################### fim nova parte ########################################################################################################


############################################### inicio atualizar tabelas de quebras ########################################################################################################

async def adicionar_DataQuebra_FG(pool):  
    tamanho_lote = 1000000
    valor_offset = 0
    
    while True:
        print('Iniciando processamento de data_cadastro_quebra para tabela falhas gerais...')
        try:
            async with pool.acquire() as conn:    
                await conn.ping(reconnect=True)  # Forçar reconexão
                async with conn.cursor() as cursor:
                    # Selecionar linhas
                    await cursor.execute("""
                        SELECT 
                            vp.cod_equipamento, 
                            vp.cod_usina, 
                            vp.data_cadastro_quebra, 
                            vp.alerta_80,
                            vp.alerta_100, 
                            vp.previsao
                        FROM 
                            machine_learning.valores_previsao vp
                        WHERE 
                            NOT EXISTS (
                                SELECT 1 
                                FROM machine_learning.falhas_gerais fg
                                WHERE fg.cod_equipamento = vp.cod_equipamento 
                                AND fg.cod_usina = vp.cod_usina
                                AND fg.data_cadastro = vp.data_cadastro_quebra 
                            )
                            AND vp.data_cadastro_quebra IS NOT NULL
                            AND EXISTS (
                                SELECT 1
                                FROM machine_learning.log_relatorio_quebras lrq
                                WHERE lrq.cod_equipamento = vp.cod_equipamento 
                                AND lrq.cod_usina = vp.cod_usina
                                AND DATE_FORMAT(lrq.data_cadastro_quebra, '%%Y-%%m-%%d') = DATE_FORMAT(vp.data_cadastro_quebra, '%%Y-%%m-%%d')
                                AND vp.alarmes IN ('(1,)', '(243,)', '(244,)', '(253,)', '(256,)', 
                                    '(259,)', '(262,)', '(265,)', '(269,)', '(272,)', 
                                    '(273,)', '(279,)', '(280,)', '(281,)', '(301,)', 
                                    '(304,)', '(350,)', '(351,)', '(352,)', '(353,)', 
                                    '(356,)', '(357,)', '(381,)', '(383,)', '(384,)', 
                                    '(385,)', '(386,)', '(387,)', '(388,)', '(389,)', 
                                    '(390,)', '(400,)', '(401,)', '(404,)', '(405,)', 
                                    '(411,)', '(412,)', '(413,)', '(414,)', '(415,)', 
                                    '(416,)', '(471,)', '(472,)', '(473,)', '(528,)', 
                                    '(590,)', '(591,)', '(592,)', '(593,)', '(594,)', 
                                    '(595,)', '(596,)', '(597,)', '(598,)', '(599,)', 
                                    '(600,)', '(602,)', '(603,)', '(604,)', '(611,)', 
                                    '(615,)', '(616,)', '(617,)', '(631,)', '(635,)', 
                                    '(637,)', '(638,)', '(657,)', '(658,)', '(669,)', 
                                    '(678,)', '(725,)', '(727,)', '(728,)', '(729,)', 
                                    '(730,)', '(731,)', '(732,)', '(735,)', '(240,)', 
                                    '(258,)', '(333,)')
                            )                       
                        LIMIT %s OFFSET %s
                    """, (tamanho_lote, valor_offset))

                    linhas = await cursor.fetchall()
                    if not linhas:
                        await asyncio.sleep(2)
                    else:
                        # Inserir as linhas na tabela falhas_gerais
                        for linha in linhas:
                            cod_equipamento, cod_usina, data_cadastro_quebra, alerta_80, alerta_100, previsao = linha

                            await cursor.execute("""
                                INSERT INTO machine_learning.falhas_gerais (cod_equipamento, cod_usina, data_cadastro, falha, alerta_80, alerta_100, previsao)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (cod_equipamento, cod_usina, data_cadastro_quebra, 1, alerta_80, alerta_100, previsao))

                        #    print(f'Inserido registro para falha em equipamento {cod_equipamento}, cod_usina {cod_usina}, na data {data_cadastro_quebra} com alertas {alerta_80}, {alerta_100}, {previsao}.')

                        await conn.commit()
                    #    print('Inserções concluídas com sucesso.')
                    
                    # Atualizar o offset
                    valor_offset += tamanho_lote

        except Exception as e:
            print(f'Erro: {e}')

        # Pausa para rodar a função novamente após 1 hora
        print('Processamento concluído. Aguardando 1 hora para próxima execução...')
        await asyncio.sleep(3600)



############################################### fim atualizar tabelas de quebras ########################################################################################################


async def listar_tarefas_ativas():
    tasks = asyncio.all_tasks()
    for task in tasks:
        print(f"Tarefa: {task.get_name()} - Estado: {task._state}")



'''

# Inicializando o bot
async def on_startup(dp: Dispatcher):
    dp.pool = await create_pool()  
    
    try:
        # Cancelar quaisquer tarefas remanescentes que não foram finalizadas
        for task in asyncio.all_tasks():
            if task.get_name() in ["processos_menos_pesados", "processos_pesados"]:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    print(f"Tarefa {task.get_name()} cancelada na inicialização.")
    
        # Iniciar processamento de botões prioritários
        asyncio.create_task(processar_botoes_prioritarios())
    
        # Inicia os processos assíncronos sem travar o fluxo principal
        asyncio.create_task(processos_async_menos_pesados(dp), name="processos_menos_pesados")
        asyncio.create_task(processos_async_pesados(dp), name="processos_pesados")
        asyncio.create_task(processos_async_duplamente_pesados(dp), name="processos_duplamente_pesados")
    except Exception as e:
        print(f"Erro ao iniciar tarefas: {e}")

# Função para processos menos pesados
async def processos_async_menos_pesados(dp: Dispatcher):
    try:
        # Processos leves em paralelo, não interferem no fluxo principal
        tarefas = [
            monitorar_leituras_consecutivas(dp.pool),
            verificar_alarmes(dp.pool),
            verificar_e_excluir_linhas_expiradas(dp.pool),
            clean_temp_files(),
            atualizar_usinas_usuario(dp.pool),
            adicionar_DataQuebra_FG(dp.pool),
            monitor_log_file(),
        
        ]
        await asyncio.gather(*tarefas)  # Aguarda todas as tarefas completarem
    except asyncio.CancelledError:
        print("Tarefa de processos leves cancelada.")
    except Exception as e:
        print(f"Erro durante a execução dos processos leves: {e}")



# Função para processos pesados
async def processos_async_pesados(dp: Dispatcher):
    
    tabelas = 'sup_geral.leituras'
    cod_equipamentos = await obter_equipamentos_validos(tabelas, dp.pool)
    cod_campo_especificados_processar_equipamentos = ['3','6','7','8','9','10', '11', '16', '19', '23', '24', '114', '21','76','25','20','77', '120']
    try:
        # Processos pesados executando em segundo plano
        tarefas = [
            novo_enviar_previsao_valor_equipamento_alerta_novo(cod_equipamentos, tabelas, cod_campo_especificados_processar_equipamentos, dp.pool, equipamentos_ativos),
            focar_alertas(cod_equipamentos, tabelas, cod_campo_especificados, dp.pool),
        ]
        await asyncio.gather(*tarefas)  # Aguarda todas as tarefas completarem
    except asyncio.CancelledError:
        print("Tarefa de processos pesados cancelada.")
    except Exception as e:
        print(f"Erro durante a execução dos processos pesados: {e}")


async def processos_async_duplamente_pesados(dp: Dispatcher):
    
    tabelas = 'sup_geral.leituras'
    cod_equipamentos = await obter_equipamentos_validos(tabelas, dp.pool)
    cod_campo_especificados_processar_equipamentos = ['3','6','7','8','9','10', '11', '16', '19', '23', '24', '114', '21','76','25','20','77', '120']
    try:
        # Processos pesados executando em segundo plano
        tarefas = [
            processar_equipamentos(cod_equipamentos, tabelas, cod_campo_especificados_processar_equipamentos, dp.pool),
            enviar_previsao_valor_equipamento_alerta(cod_equipamentos, tabelas, cod_campo_especificados, dp.pool),
            enviar_alerta_80_100(cod_equipamentos, tabelas, cod_campo_especificados, dp.pool),
            
        ]
        await asyncio.gather(*tarefas)  # Aguarda todas as tarefas completarem
    except asyncio.CancelledError:
        print("Tarefa de processos pesados cancelada.")
    except Exception as e:
        print(f"Erro durante a execução dos processos pesados: {e}")




# Criação de tabelas
async def criar_tabelas(pool):
    await criar_tabela_usuarios_telegram(pool)
    await criar_tabela_relatorio_quebras(pool)
    await criar_tabela_log_relatorio_quebras(pool)
    await criar_tabela_silenciar_bot(pool)
    await criar_tabela_leituras(pool)
    await criar_tabela_valores_previsao(pool)
    await criar_tabela_falhas_gerais(pool)
    await criar_tabela_usinas_usuario(pool)
 

async def on_shutdown(dp: Dispatcher):
    await close_db(dp.pool)
    print("Fechando o pool de conexões...")
    if dp.pool is not None:
        dp.pool.close()
        await dp.pool.wait_closed()


if __name__ == "__main__":
    # Iniciar o polling com os callbacks on_startup e on_shutdown
    executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)
    
'''



import subprocess
import json

# Inicializando o bot
async def on_startup(dp: Dispatcher):
    dp.pool = await create_pool()

    # Obtenha parâmetros necessários
    tabelas = 'sup_geral.leituras'
    cod_equipamentos = await obter_equipamentos_validos(tabelas, dp.pool)
    cod_campo_especificados_processar_equipamentos = ['3','6','7','8','9','10', '11', '16', '19', '23', '24', '114', '21','76','25','20','77', '120']
    cod_campo_especificados = ['3', '114']
             
    try:
        # Cancelar quaisquer tarefas remanescentes que não foram finalizadas
        # for task in asyncio.all_tasks():
        #     if task.get_name() in ["processos_menos_pesados"]:
        #         task.cancel()
        #         try:
        #             await task
        #         except asyncio.CancelledError:
        #             print(f"Tarefa {task.get_name()} cancelada na inicialização.")
        
        # Inicia os processos assíncronos sem travar o fluxo principal
    #    asyncio.create_task(processos_async_menos_pesados(dp), name="processos_menos_pesados")

        # Iniciar processamento de botões prioritários
        asyncio.create_task(processar_botoes_prioritarios())
        
        # Executa cada função pesada em seu próprio núcleo com JSON
        subprocess.Popen(['taskset', '-c', '2', 'python3', '/home/bruno/codigos/processos_async_menos_pesados.py'])
        subprocess.Popen(['taskset', '-c', '3', 'python3', '/home/bruno/codigos/monitorar_leituras_consecutivas.py'])
        subprocess.Popen(['taskset', '-c', '4', 'python3', '/home/bruno/codigos/processar_equipamentos.py', json.dumps(cod_equipamentos), tabelas, json.dumps(cod_campo_especificados_processar_equipamentos)])
        subprocess.Popen(['taskset', '-c', '5', 'python3', '/home/bruno/codigos/enviar_previsao.py', json.dumps(cod_equipamentos), tabelas, json.dumps(cod_campo_especificados)])
        subprocess.Popen(['taskset', '-c', '6', 'python3', '/home/bruno/codigos/enviar_alerta.py', json.dumps(cod_equipamentos), tabelas, json.dumps(cod_campo_especificados)])
        subprocess.Popen(['taskset', '-c', '7', 'python3', '/home/bruno/codigos/novo_enviar_previsao.py', json.dumps(cod_equipamentos), tabelas, json.dumps(cod_campo_especificados_processar_equipamentos)])

    except Exception as e:
        print(f"Erro ao iniciar tarefas: {e}")



# Limitar tarefas simultâneas com Semaphore
async def processos_async_menos_pesados(dp: Dispatcher, limite_tarefas=5):
    semaforo = asyncio.Semaphore(limite_tarefas)

    async def tarefa_com_semaforo(func):
        async with semaforo:
            try:
                await func
            except Exception as e:
                print(f"Erro na execução de {func.__name__}: {e}")

    try:
        # Processos leves em paralelo, não interferem no fluxo principal
        tarefas = [
        #    tarefa_com_semaforo(monitorar_leituras_consecutivas(dp.pool)),
            tarefa_com_semaforo(verificar_alarmes(dp.pool)),
            tarefa_com_semaforo(verificar_e_excluir_linhas_expiradas(dp.pool)),
            tarefa_com_semaforo(clean_temp_files()),
            tarefa_com_semaforo(atualizar_usinas_usuario(dp.pool)),
            tarefa_com_semaforo(adicionar_DataQuebra_FG(dp.pool)),
    #        tarefa_com_semaforo(monitor_log_file()),
        ]
        await asyncio.gather(*tarefas)  # Aguarda todas as tarefas completarem
    except asyncio.CancelledError:
        print("Tarefa de processos leves cancelada.")
    except Exception as e:
        print(f"Erro durante a execução dos processos leves: {e}")




# Função para processos pesados
async def processos_async_pesados(dp: Dispatcher):
    
    tabelas = 'sup_geral.leituras'
    cod_equipamentos = await obter_equipamentos_validos(tabelas, dp.pool)
    cod_campo_especificados_processar_equipamentos = ['3','6','7','8','9','10', '11', '16', '19', '23', '24', '114', '21','76','25','20','77', '120']
    try:
        # Processos pesados executando em segundo plano
        tarefas = [
        #    novo_enviar_previsao_valor_equipamento_alerta_novo(cod_equipamentos, tabelas, cod_campo_especificados_processar_equipamentos, dp.pool),
        #    focar_alertas(cod_equipamentos, tabelas, cod_campo_especificados, dp.pool),
        ]
        await asyncio.gather(*tarefas)  # Aguarda todas as tarefas completarem
    except asyncio.CancelledError:
        print("Tarefa de processos pesados cancelada.")
    except Exception as e:
        print(f"Erro durante a execução dos processos pesados: {e}")



# Criação de tabelas
async def criar_tabelas(pool):
    await criar_tabela_usuarios_telegram(pool)
    await criar_tabela_relatorio_quebras(pool)
    await criar_tabela_log_relatorio_quebras(pool)
    await criar_tabela_silenciar_bot(pool)
    await criar_tabela_leituras(pool)
    await criar_tabela_valores_previsao(pool)
    await criar_tabela_falhas_gerais(pool)
    await criar_tabela_usinas_usuario(pool)
 

async def on_shutdown(dp: Dispatcher):
    await close_db(dp.pool)
    print("Fechando o pool de conexões...")
    if dp.pool is not None:
        dp.pool.close()
        await dp.pool.wait_closed()


if __name__ == "__main__":
    # Iniciar o polling com os callbacks on_startup e on_shutdown
    executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)


