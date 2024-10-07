from aiogram import Bot, Dispatcher, types
import asyncio
import mysql.connector
import atexit
import pandas as pd
from datetime import datetime, timedelta, time
import sys
from aiogram.types import ParseMode
import os

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




TOKEN = "6674960909:AAEf4Ky64lM6NILX3wZ6HJEASTt42Q2vopc"
#API_KEY = 'AIzaSyDfcjDbVZ2BbPJEuSpJ4wF_ATguRuffikA'

API_KEY = 'AIzaSyDf9hqXZvxOiCKaFSiIa0byrfEctP5mflI'

genai.configure(api_key=API_KEY)

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def create_pool():
    pool = await aiomysql.create_pool(
        host="192.168.4.50",
        user="bruno",
        password="superbancoml",
        db="machine_learning"
    )
    return pool

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
            


# Dicionário para armazenar temporariamente os dados do relatório
user_reports = {}


'''
class ReportStates(StatesGroup):
    period = State()
    funcionamento = State()

# Teclado principal
main_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
main_keyboard.add(KeyboardButton("Relatório"))
main_keyboard.add(KeyboardButton("Geradores Em Operação"))
main_keyboard.add(KeyboardButton("Menu"))

# Teclado de opções de relatórios
report_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
report_keyboard.add(
    KeyboardButton("1 dia"),
    KeyboardButton("2 dias"),
    KeyboardButton("7 dias"),
    KeyboardButton("15 dias"),
    KeyboardButton("1 mês")
)

# Teclado de opções de funcionamento
funcionamento_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
funcionamento_keyboard.add(
    KeyboardButton("Controle de demanda"),
    KeyboardButton("Horário de ponta"),
    KeyboardButton("Operação contínua"),
    KeyboardButton("Termoelétrica"),
    KeyboardButton("Falta de energia"),
    KeyboardButton("Geral"),
    KeyboardButton("Agrogera BA"),
    KeyboardButton("Agrogera GO"),
    KeyboardButton("Agrogera MG"),
)

# Dicionário de funcionamento
funcionamento_map = {
    "Controle de demanda": "CONTROLE DE DEMANDA",
    "Horário de ponta": "HORARIO DE PONTA",
    "Operação contínua": "OPERACAO CONTINUA",
    "Termoelétrica": "TERMOELETRICA",
    "Falta de energia": "FALTA DE ENERGIA",
    "Geral": "Geral",
    "Agrogera GO": "aggo",
    "Agrogera MG": "agmg",
    "Agrogera BA": "AGROGERA",
}

# Chave inversa para mapear de volta para o nome original do teclado
reverse_funcionamento_map = {v: k for k, v in funcionamento_map.items()}

@dp.message_handler(commands=['relatorio'])
async def send_welcome(message: types.Message):
    await message.reply("Para interagir com o bot, use o botão que aparece ao lado do microfone:", reply_markup=main_keyboard)

@dp.message_handler(lambda message: message.text == "Relatório")
async def show_report_options(message: types.Message):
    await message.reply("Escolha o período do relatório:", reply_markup=report_keyboard)

'''

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
                
                # Supondo que a variável `current_time` seja a data e hora atuais
                current_time = datetime.now()

                # Process log data and build content for each usina
                for row in log_data:
                    cod_equipamento, cod_usina, data_previsto, data_previsto_saida, data_quebra, nome_usina, nome_equipamento = row

                    # # Verificar se o equipamento está em alerta atualmente
                    # if cod_equipamento in equipamentos_alerta_set and not data_quebra:
                    #     data_quebra = 'Em funcionamento'
                    #     tempo_total = 'Em funcionamento'
                    #     tempo_anormalidade = 'Em funcionamento' if not data_previsto_saida else datetime.strptime(str(data_previsto_saida), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                    # elif not data_quebra:
                    #     data_quebra = 'Não houve falha'
                    #     tempo_total = 'Não houve falha'
                    #     tempo_anormalidade = 'Não houve falha' if not data_previsto_saida else datetime.strptime(str(data_previsto_saida), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                    # else:
                    #     data_previsto_saida = data_previsto_saida if data_previsto_saida else 'Indefinido'
                    #     if data_quebra not in ['Não houve falha', 'Em funcionamento'] and data_previsto_saida != 'Indefinido':
                    #         tempo_anormalidade = datetime.strptime(str(data_previsto_saida), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                    #         tempo_total = datetime.strptime(str(data_quebra), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                    #     else:
                    #         tempo_anormalidade = 'Não disponível'
                    #         tempo_total = 'Não disponível'



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
                                tempo_anormalidade = 'Sem comunicação'
                                if data_previsto_saida == 'Indefinido':
                                    tempo_total = datetime.strptime(str(data_quebra), '%Y-%m-%d %H:%M:%S') - data_previsto_dt
                                else:
                                    tempo_anormalidade = data_previsto_saida_dt - data_previsto_dt if data_previsto_saida_dt else 'Sem comunicação'
                                    tempo_total = datetime.strptime(str(data_quebra), '%Y-%m-%d %H:%M:%S') - data_previsto_dt
                            else:
                                tempo_anormalidade = 'Sem comunicação'
                                tempo_total = 'Sem comunicação'
                    
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
                        media_valores_reais = sum(valores_reais) / len(valores_reais)

                        # Calcular a média dos valores previstos
                        media_valores_previstos = sum(valores_previstos) / len(valores_previstos)

                        # Calcular a porcentagem de diferença entre o real e o previsto
                        diferenca_percentual = ((media_valores_previstos - media_valores_reais) / media_valores_reais) * 100

                        # Formatar a saída com duas casas decimais e sinal de mais ou menos
                        if diferenca_percentual < 0:
                        #    diferenca_formatada = f"menos {abs(diferenca_percentual):.2f}%"
                            diferenca_formatada = f"{diferenca_percentual:.2f}"
                        else:
                        #    diferenca_formatada = f"mais {diferenca_percentual:.2f}%"
                            diferenca_formatada = f"{diferenca_percentual:.2f}"
        
        
                        # Obter descrições dos alarmes
                        if alarmes:
                            # Processar a string de alarmes para extrair os códigos
                            alarm_codes = []
                            for code in alarmes.split(','):
                                try:
                                    alarm_codes.append((int(code.strip().strip('()')),))
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

                #    if nome_usina not in dados_gemini:
                #        dados_gemini += f"<b>Usina:</b> {nome_usina}<br/><br/>    <b>Quantidade de geradores:</b> {num_geradores}<br/>{funcionando_text}<br/><br/>"

                    if nome_usina not in telegram_report:
                        telegram_report += f"Usina: {nome_usina}\n\n    Quantidade de geradores: {num_geradores}\n\n{funcionando_text_telegram}\n\n"
                        
                    telegram_report += (
                        f"  Equipamento: {nome_equipamento}\n"
                        f"    Data: {data_previsto}\n"
                        f"    Data falha: {data_quebra}\n\n"
                    #    f"    Load Speed %: {max(valores_reais)}\n"
                    #    f"    Valor previsto %: {max(valores_previstos)}\n"
                    #    f"    Tipo de Alerta: {alerta_status}\n"
                    #    f"    Alarmes: {alarmes_text_tabela}\n\n"
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
                
                # Dividir o relatório em partes menores
                telegram_report = split_report(telegram_report)

    #            print('telegram_report',telegram_report,'\n','detailed_report',detailed_report,'\n','dados_gemini',dados_gemini)
    #            print('dados_gemini',dados_gemini)

    #            return [telegram_report, detailed_report]
                return [telegram_report, detailed_report, dados_gemini]
    except Exception as e:
        print(f"Erro ao buscar dados do relatório: {str(e)}")
        return ["Erro ao buscar dados do relatório. Tente novamente."], "", ""




# Variáveis globais para armazenar informações necessárias
global_vars = {
    "log_file_path": 'logs/log_bot.txt',
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
                                        pool = await create_pool()
                                        report_data = await fetch_report_data(pool, period, user_id, funcionamento)
                                        telegram_report, detailed_report, dados_gemini = report_data
                                        pdf_filename = f"relatorio_{funcionamento}_{period}_dias.pdf"

                                        # Reenvia o arquivo PDF ao usuário
                                        pdf_file_path = await create_pdf(detailed_report, dados_gemini, period, funcionamento, pdf_filename)
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



'''
@dp.message_handler(lambda message: message.text in ["1 dia", "2 dias", "7 dias", "15 dias", "1 mês"])
async def handle_report_period(message: types.Message):
    try:
        period_map = {
            "1 dia": 1,
            "2 dias": 2,
            "7 dias": 7,
            "15 dias": 15,
            "1 mês": 30
        }
        period = period_map[message.text]
        user_id = message.from_user.id
        user_reports[user_id] = {"period": period}
        await message.reply("Selecione o tipo de funcionamento:", reply_markup=funcionamento_keyboard)
    except Exception as e:
        await message.reply(f"Ocorreu um erro ao selecionar o período: {str(e)}")



@dp.message_handler(lambda message: message.text in funcionamento_map.keys())
async def handle_funcionamento_selection(message: types.Message):
    pool = await create_pool()
    try:
        funcionamento = funcionamento_map[message.text]
        user_id = message.from_user.id
        user_reports[user_id]["funcionamento"] = funcionamento
        id_grupo = global_vars["id_grupo"]
        nome_usuario = ''
        period = user_reports[user_id]["period"]
        
        global_vars["period"] = period
        global_vars["funcionamento"] = funcionamento
        global_vars["user_id"] = user_id

        await message.reply(f"Você escolheu o funcionamento: {message.text}. Aguarde enquanto buscamos os dados...", reply_markup=main_keyboard)

            
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:   
                await cursor.execute("SELECT nome_telegram FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
                result = await cursor.fetchone()
                nome_usuario = ''
                if not result:
                    return await message.reply("Usuário não encontrado.")
                nome_usuario = result[0]
        
                global_vars["nome_usuario"] = nome_usuario

                global_vars["mensagem"] = f'Usuário {nome_usuario} pediu funcionamento: {message.text} para {period} dias.'
                print(global_vars["mensagem"])
                try:
                    await bot.send_message(id_grupo, global_vars["mensagem"])
                except Exception as e:
                    await message.reply(f"Erro ao enviar mensagem ao grupo: {e}")
                sys.stdout.flush()

        # Buscar os dados do relatório
        try:
            report_data = await fetch_report_data(pool, period, user_id, funcionamento)
        finally:
            pool.close()
            await pool.wait_closed()
            pass

        if not report_data or report_data[0] == ["Nenhum dado encontrado para o período selecionado."]:
            await message.reply("Nenhum dado encontrado para o funcionamento selecionado.")
            await message.reply("Escolha o período do relatório novamente:", reply_markup=report_keyboard)
        else:
            telegram_report, detailed_report, dados_gemini = report_data

            user_reports[user_id] = (detailed_report, period, funcionamento, dados_gemini)

            # Criar e enviar o PDF diretamente sem botao
            pdf_filename = f"relatorio_{funcionamento}_{period}_dias.pdf"
            pdf_file_path = await create_pdf(detailed_report, dados_gemini, period, funcionamento, pdf_filename)
            global_vars["pdf_file_path"] = pdf_file_path
            await bot.send_document(chat_id=message.chat.id, document=InputFile(pdf_file_path, filename=pdf_filename))

            global_vars["mensagem2"] = f'Usuário {nome_usuario} completou o pedido do PDF.'
            print(global_vars["mensagem2"])

            try:
                await bot.send_message(id_grupo, global_vars["mensagem2"])
            except Exception as e:
                await message.reply(f"Erro ao enviar mensagem ao grupo: {e}")
            sys.stdout.flush()

            # Solicitar email do usuário
            email_button = InlineKeyboardMarkup().add(InlineKeyboardButton("Receber PDF por Email", callback_data="request_email"))
            await message.reply("Deseja receber o PDF por email?", reply_markup=email_button)

    except Exception as e:
        await message.reply(f"Ocorreu um erro ao processar sua solicitação: {str(e)}")
        logger.error(f"Error in handle_funcionamento_selection: {str(e)}")

'''


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
@dp.callback_query_handler(lambda c: c.data.startswith('funcionamento_'))
async def handle_funcionamento_selection(callback_query: types.CallbackQuery):
    pool = await create_pool()
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
        pool.close()
        await pool.wait_closed()

    if not report_data or report_data[0] == ["Nenhum dado encontrado para o período selecionado."]:
        await bot.send_message(callback_query.message.chat.id,"Nenhum dado encontrado para o funcionamento selecionado.")
        await bot.send_message(callback_query.message.chat.id,"Escolha o período do relatório novamente:", reply_markup=create_period_buttons())
    else:
        telegram_report, detailed_report, dados_gemini = report_data

        user_reports[user_id] = (detailed_report, period, funcionamento, dados_gemini)

        # Criar e enviar o PDF diretamente sem botão
        pdf_filename = f"relatorio_{funcionamento}_{period}_dias.pdf"
        pdf_file_path = await create_pdf(detailed_report, dados_gemini, period, funcionamento, pdf_filename)
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
    pool = await create_pool()

    try:
        async with pool.acquire() as conn:
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
        pool.close()
        await pool.wait_closed()
        pass
    
# Handler to send to existing email
@dp.callback_query_handler(lambda c: c.data == 'send_to_existing_email')
async def handle_send_to_existing_email(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    pool = await create_pool()
    await bot.delete_message(chat_id=callback_query.message.chat.id, message_id=callback_query.message.message_id)

    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT email FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
                result = await cursor.fetchone()
                if result and result[0]:
                    email = result[0]
                    detailed_report, period, funcionamento, dados_gemini = user_reports[user_id]
                    pdf_filename = f"relatorio_{funcionamento}_{period}_dias.pdf"
                    pdf_file_path = await create_pdf(detailed_report, dados_gemini, period, funcionamento, pdf_filename)
                    await send_email(email, pdf_file_path, pdf_filename)
#                    await bot.delete_message(chat_id=callback_query.message.chat.id, message_id=callback_query.message.message_id)
                    await bot.send_message(user_id, f"PDF enviado para o email {email} com sucesso.")
                else:
                    await bot.send_message(user_id, "Email não encontrado. Por favor, digite o email para enviar o PDF.")
    except Exception as e:
        await bot.send_message(user_id, f"Erro ao enviar o PDF: {str(e)}")
        logger.error(f"Error in handle_send_to_existing_email: {str(e)}")
    finally:
        pool.close()
        await pool.wait_closed()



# Handler to request new email
@dp.callback_query_handler(lambda c: c.data == 'request_new_email')
async def handle_request_new_email(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, "Por favor, digite o novo email para enviar o PDF:")
    await callback_query.answer()

@dp.message_handler(lambda message: '@' in message.text)
async def handle_email_input(message: types.Message):
    pool = await create_pool()
    user_id = message.from_user.id
    if user_id in user_reports:
        try:
            to_email = message.text
            detailed_report, period, funcionamento, dados_gemini = user_reports[user_id]
            pdf_filename = f"relatorio_{funcionamento}_{period}_dias.pdf"
            pdf_file_path = await create_pdf(detailed_report, dados_gemini, period, funcionamento, pdf_filename)
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






'''
async def send_email(to_email, pdf_file_path, filename):
    message = MIMEMultipart()
    message["From"] = "workflow@brggeradores.com.br"
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
            hostname="smtp.office365.com",
            port=587,
            username="workflow@brggeradores.com.br",
            password="r136789h#",
            use_tls=False
        )
    except Exception as e:
        logger.error(f"Error in send_email: {str(e)}")
'''

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
    try:
        os.remove(pdf_file_path)
        logger.info(f"Deleted PDF file: {pdf_file_path}")
    except Exception as e:
        logger.error(f"Error deleting PDF file: {str(e)}")




async def create_pdf(detailed_report, dados_gemini, period, funcionamento, filename):
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
        leading=6,   # Espaçamento entre linhas para centralizar verticalmente
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

    column_widths = [90, 40, 80, 55, 55, 50, 95]

    table = Table(styled_data, colWidths=column_widths)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),  # Set header text color to white
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('TOPPADDING', (0, 1), (-1, -1), 12),  # Ajusta o padding superior para centralizar verticalmente
        ('BOTTOMPADDING', (0, 1), (-1, -1), 12),  # Ajusta o padding inferior para centralizar verticalmente
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
            data_previsto = formatar_data(entry.split("Data previsto: ")[1].split("\n")[0].strip())
            data_previsto_saida = formatar_data(entry.split("Data previsto saída: ")[1].split("\n")[0].strip())
            data_quebra = formatar_data(entry.split("Data quebra: ")[1].split("\n")[0].strip())

            valores_reais = entry.split("Valores reais: [")[1].split("]")[0].strip().replace('\n', '').replace(' ', '').split(',')
            valores_previstos = entry.split("Valores previstos: [")[1].split("]")[0].strip().replace('\n', '').replace(' ', '').split(',')

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


# Função para detectar padrões de sequência de alarmes
def detectar_sequencia(alarmes_text):
    sequencias = {}
    alarmes = re.split(r',\s*', alarmes_text)

    for alarme in alarmes:
        base_nome = ' '.join(alarme.split()[:-1])
        numero = alarme.split()[-1]
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
    try:
        data_obj = datetime.strptime(data_str, '%Y-%m-%d %H:%M:%S')
        return data_obj.strftime('%d/%m/%Y %H:%M:%S')
    except ValueError:
        return data_str


def gerar_lista_tempo(data_inicial, intervalos, incremento_minutos=5):
    data_inicial_obj = datetime.strptime(data_inicial, '%d/%m/%Y %H:%M:%S')
    lista_tempo = [(data_inicial_obj + timedelta(minutes=i*incremento_minutos)).strftime('%H:%M') for i in range(intervalos)]
    return lista_tempo






# async def create_pdf(detailed_report, dados_gemini, period):
#     temp_pdf = tempfile.mktemp(suffix=".pdf")
#     doc = SimpleDocTemplate(temp_pdf, pagesize=A4, title="Relatório Detalhado")
#     elements = []

#     logo_path = "/home/bruno/imagens/cabeçalho.png"
    
#     # Adjust logo transparency
#     pil_logo = PILImage.open(logo_path).convert("RGBA")
#     alpha = pil_logo.split()[3]
#     alpha = alpha.point(lambda p: p * 0.5)  # Adjust transparency to 50%
#     pil_logo.putalpha(alpha)
#     temp_logo_path = tempfile.mktemp(suffix=".png")
#     pil_logo.save(temp_logo_path)
    
#     logo = Image(temp_logo_path, width=120, height=50)

#     # Define a função para o cabeçalho
#     def header(canvas, doc):
#         canvas.saveState()
#         logo.drawOn(canvas, 5, A4[1] - 55)

#         # Configurações do cabeçalho
#         canvas.setFont("Helvetica", 12)

#         # Texto "Relatório Geral"
#         header_text = "Relatório Geral"
#         header_width = canvas.stringWidth(header_text, "Helvetica", 12)
#         canvas.drawString((A4[0] - header_width) / 2, A4[1] - 50, header_text)

#         # Texto "Últimos {period} dias" em vermelho
#         canvas.setFillColor(colors.red)
#         period_text = f"Últimos {period} dias"
#         period_width = canvas.stringWidth(period_text, "Helvetica", 12)
#         canvas.drawString((A4[0] - period_width) / 2, A4[1] - 70, period_text)

#         canvas.restoreState()

#     styles = getSampleStyleSheet()
#     styleN = styles['Normal']
#     styleH = styles['Heading1']

#     # Extract dates from the detailed report
#     dates = []
#     rows = detailed_report.split("<tr>")
#     for row in rows[1:]:
#         columns = row.split("<td>")
#         if len(columns) > 3:
#             date_str = columns[3].split("</td>")[0].strip()
#             try:
#                 date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')  # Adjust the date format if necessary
#                 dates.append(date)
#             except ValueError:
#                 continue

#     if dates:
#         start_date = min(dates).strftime('%d/%m/%Y')
#         end_date = max(dates).strftime('%d/%m/%Y')
#     else:
#         start_date = 'N/A'
#         end_date = 'N/A'
    
#     elements.append(Spacer(1, 12))
#     elements.append(Paragraph(f"<b>Período: {start_date} à {end_date}</b>", styleH))
#     elements.append(Spacer(1, 12)) 

#     header_style = ParagraphStyle(
#         'HeaderStyle',
#         parent=styles['Normal'],
#         fontSize=8,
#         leading=10,
#         alignment=1,
#         textColor=colors.black
#     )
#     body_style = ParagraphStyle(
#         'BodyStyle',
#         parent=styles['Normal'],
#         fontSize=8,
#         leading=6,
#         alignment=1,
#         textColor=colors.black
#     )

#     data = [
#         ['Usina', 'Equip.', 'Data', 'Duração Anormalidade', 'Alerta', 'Load Speed %', 'Valor Previsto %', 'Alarmes']
#     ]

#     rows = detailed_report.split("<tr>")
#     for row in rows[1:]:
#         columns = row.split("<td>")
#         row_data = []
#         for i, col in enumerate(columns[1:], start=1):
#             value = col.split("</td>")[0].strip()
#             if i == 3:
#                 try:
#                     value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')
#                 except ValueError:
#                     pass
#             row_data.append(value)
#         data.append(row_data)

#     styled_data = [[Paragraph(cell, header_style) if i == 0 else Paragraph(cell, body_style) for i, cell in enumerate(row)] for row in data]

#     column_widths = [110, 35, 80, 80, 50, 60, 60, 110]

#     table = Table(styled_data, colWidths=column_widths)
#     table.setStyle(TableStyle([
#         ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
#         ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
#         ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
#         ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
#         ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
#         ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
#         ('GRID', (0, 0), (-1, -1), 1, colors.black),
#     ]))

#     elements.append(table)
#     elements.append(Spacer(1, 12))

#     usinas = {}
#     first_usina = True

#     for entry in dados_gemini.split("--------------------------------------"):
#         try:
#             nome_usina = entry.split("Usina: ")[1].split("\n")[0].strip()
#             nome_equipamento = entry.split("Equipamento: ")[1].split(" - ")[0].strip()
#             data_previsto = entry.split("Data previsto: ")[1].split("\n")[0].strip()
#             valores_reais = entry.split("Valores reais: [")[1].split("]")[0].strip().replace('\n', '').replace(' ', '').split(',')
#             valores_previstos = entry.split("Valores previstos: [")[1].split("]")[0].strip().replace('\n', '').replace(' ', '').split(',')

#             valores_reais_float = [float(value) for value in valores_reais]
#             valores_previstos_float = [float(value) for value in valores_previstos]

#             if nome_usina not in usinas:
#                 usinas[nome_usina] = []

#             usinas[nome_usina].append((nome_equipamento, valores_reais_float, valores_previstos_float, data_previsto))

#         except IndexError:
#             continue
#         except ValueError as ve:
#             print(f"Error parsing values: {ve}")
#             continue

#     for nome_usina, equipamentos in usinas.items():
#         elements.append(Paragraph(f"Usina: {nome_usina}", getSampleStyleSheet()["Heading2"]))

#         if first_usina:
#             legend = "<font color='red'>---</font> Valor previsto % <font color='blue'>―</font> Load Speed %"
#             elements.append(Paragraph(f"Informações do gráfico: {legend}", body_style))
#             first_usina = False

#         graph_data = []
#         for idx, (nome_equipamento, valores_reais, valores_previstos, data_previsto) in enumerate(equipamentos):
#             plt.figure(figsize=(6, 3))  # Tamanho ajustado para gráficos maiores
#             plt.plot(valores_reais, label='Valores Reais', color='blue', linestyle='-')
#             plt.plot(valores_previstos, label='Valores Previstos', color='red', linestyle='--')
#             plt.xlabel(' ')
#             plt.ylabel('Valores')
#             plt.title(f'{nome_equipamento}')
#             plt.grid(True)  # Adiciona grid ao gráfico
#             plt.legend().set_visible(False)  # Oculta a legenda dos gráficos individuais

#             temp_img = tempfile.mktemp(suffix=".png")
#             plt.savefig(temp_img, bbox_inches='tight')
#             plt.close()

#             graph = Image(temp_img, width=240, height=120)  # Ajustado para maior tamanho
#             graph_data.append(graph)

#             if (idx + 1) % 2 == 0:
#                 elements.append(Table([graph_data], colWidths=[240, 240]))
#                 elements.append(Spacer(1, 12))
#                 graph_data = []

#         if graph_data:
#             elements.append(Table([graph_data], colWidths=[240, 240]))
#             elements.append(Spacer(1, 12))

#     elements.append(Spacer(1, 40))
#     elements.append(Paragraph(detailed_report, getSampleStyleSheet()["BodyText"]))
#     elements.append(Spacer(1, 12))

#     doc.build(elements, onFirstPage=header, onLaterPages=header)

#     return temp_pdf




async def verificar_e_obter_coeficientee(cod_equipamento, pool):
    try:
        coeficiente_existente = 0.0
        intercepto_existente = 0.0
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(f"SELECT coeficiente, intercepto FROM machine_learning.coeficiente_geradores WHERE cod_equipamento = {cod_equipamento}")
                resultado = await cursor.fetchone()
                if resultado is not None:
                    coeficiente_existente, intercepto_existente = resultado
                else:
                    return 0.0, 0.0
        return coeficiente_existente, intercepto_existente
    except Exception as e:
        print(f"An error occurred in verificar_e_obter_coeficiente: {e}")
        return 0.0, 0.0



async def fazer_previsao(valores_atuais, coeficiente, intercepto, cod_equipamento_resultado, pool):

    try:
        contagem_limites = 4
        pool = await create_pool()

        async with pool.acquire() as conn:
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
            coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficientee(cod_equipamento_resultado, pool)

            if valores_atuais is not None:
                previsoes = [(valor * coeficiente_existente + intercepto_existente) for valor in valores_atuais]
            else:
                previsoes = []
                
            previsoes = [round(valor, 1) for valor in previsoes]

            if valores_atuais_114 is None:
            #    print(f"Valores atuais 114 para o equipamento {cod_equipamento_resultado} não encontrados.")
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
            #        print(cod_equipamento_resultado,' ABAIXO', ' ',i,' valor',valor,' previsoes',previsoes[i], ' calculo =',calculo, ' limite_menos', limite_menos, ' contagem_abaixo_do_limite', contagem_abaixo_do_limite)

                elif abs(calculo) > limite_mais:
                    contagem_acima_do_limite += 1
            #        print(cod_equipamento_resultado,' ACIMA', ' ',i,' valor',valor,' previsoes',previsoes[i], ' calculo =',calculo, ' limite_mais', limite_mais, ' contagem_acima_do_limite', contagem_acima_do_limite)

            return previsoes, contagem_acima_do_limite > contagem_limites or contagem_abaixo_do_limite > contagem_limites

        else:
            return 'NOT UPDATED BRO', False

    except Exception as e:
        print(f"An error occurred in fazer_previsao: {e}")
        return 0, False




async def selecionar_GMG(pool):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT codigo, ativo FROM sup_geral.tipos_equipamentos WHERE classe = 'GMG'")
            resultados = await cursor.fetchall()
            codigos = [resultado['codigo'] for resultado in resultados]
        return codigos


#@dp.message_handler(lambda message: message.text == "Geradores Em Operação")
@dp.message_handler(lambda message: message.text == "/geradores")
async def teste_menu(message: types.Message):
    try:
        print('*****************************************************************************************************************')
        tempo_inicial = datetime.now()
        data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')
        print('clicou no teste', data_cadastro_formatada)
        chat_id = message.chat.id
        pool = await create_pool()

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (chat_id,))
                result = await cursor.fetchone()

                if result is not None:
                    username = result[0]
                    timestamp = int(time.mktime(message.date.timetuple()))
                    user_message = types.Message(message_id=message.message_id, from_user=message.from_user, chat=message.chat, date=timestamp, text=username)
                    # await enviar_previsao_valor_equipamento(user_message, username)

                    valores_atuais_str = 0

                    async with pool.acquire() as conn:
                        async with conn.cursor() as cursor:
                            await cursor.execute("SELECT nome, codigo FROM sup_geral.usuarios WHERE login = %s", (username,))
                            result = await cursor.fetchone()
                            if result is not None:
                                nome_supervisorio, cod_usuario = result

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

                                if len(mensagem) > 4000:
                                    partes_mensagem = [mensagem[i:i + 4000] for i in range(0, len(mensagem), 4000)]
                                    for parte in partes_mensagem:
                                        await bot.send_message(chat_id, parte, parse_mode='HTML')
                                else:
                                    await bot.send_message(chat_id, mensagem, parse_mode='HTML')

                                mensagem_total_equipamentos += f"\n\nGeradores Em Operação: {total_equipamentos}\nGeradores Em alerta: {total_equipamentos_true}\n/teste"
                                print(f"\n\nGeradores Em Operação: {total_equipamentos}\nGeradores Em alerta: {total_equipamentos_true}\n")
                                await bot.send_message(chat_id, mensagem_total_equipamentos)
                                print('*****************************************************************************************************************')
                                sys.stdout.flush()
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.stdout.flush()



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
                            #        print(f"Inserindo para cod_equipamento: {cod_equipamento}, cod_campo: {cod}, valores: {valores[cod]}")
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
                                    #    print(f"Atualizando para cod_equipamento: {cod_equipamento}, cod_campo: {cod}, valores: {valores[cod]}")
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
        await asyncio.sleep(10)



'''
async def processar_equipamentos(cod_equipamentos, tabelas, cod_campo_especificados, pool):
    while True:
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    for cod_equipamento in cod_equipamentos:
                        # Inicializando dicionário para armazenar os valores dos últimos 5 dados por campo
                        valores = {cod: [0, 0, 0, 0, 0] for cod in cod_campo_especificados}

                        # Definindo o intervalo de tempo de 30 minutos atrás
                        agora = datetime.now()
                        limite_tempo = agora - timedelta(minutes=30)
                        
                        # Buscando leituras da tabela `leituras` com base nos campos especificados e no intervalo de 30 minutos
                        query = f"""
                        SELECT data_cadastro, valor, cod_campo 
                        FROM {tabelas} 
                        WHERE cod_equipamento = %s 
                        AND cod_campo IN ({', '.join(cod_campo_especificados)})
                        AND data_cadastro >= %s
                        """
                        await cursor.execute(query, (cod_equipamento, limite_tempo))
                        resultados = await cursor.fetchall()

                        # Convertendo os resultados para DataFrame para facilitar o processamento
                        df = pd.DataFrame(resultados, columns=['data_cadastro', 'valor', 'cod_campo'])
                        df['data_cadastro'] = pd.to_datetime(df['data_cadastro'], errors='coerce')
                        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')

                        # Removendo entradas onde `data_cadastro` é nulo (NaT)
                        df = df.dropna(subset=['data_cadastro'])

                        # Lista para armazenar todos os dados de inserção em massa
                        insercoes = []

                        # Iterando sobre os cod_campo especificados
                        for cod in cod_campo_especificados:
                            # Filtrando os valores mais recentes para cada cod_campo
                            valores_cod_campo = df[df['cod_campo'] == int(cod)]['valor'].values
                            valores[cod] = list(valores_cod_campo[-5:])[::-1] + valores[cod][:5-len(valores_cod_campo[-5:])]

                            # Buscando a última data de cadastro em `leituras_consecutivas`
                            await cursor.execute(f"SELECT data_cadastro FROM machine_learning.leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = %s", (cod_equipamento, cod))
                            data_cadastro_consecutivas = await cursor.fetchone()

                            # Se não houver data, insere os valores
                            if not data_cadastro_consecutivas and not df.empty:
                                insercoes.append((cod_equipamento, cod, *valores[cod][::-1], df['data_cadastro'].max().strftime('%Y-%m-%d %H:%M:%S')))
                            else:
                                # Verifica se precisa atualizar os dados
                                if not df.empty:
                                    data_cadastro_formatada_consecutivas = data_cadastro_consecutivas[0].strftime('%Y-%m-%d %H:%M:%S') if data_cadastro_consecutivas else None
                                    if df['data_cadastro'].max().strftime('%Y-%m-%d %H:%M:%S') != data_cadastro_formatada_consecutivas:
                                        insercoes.append((cod_equipamento, cod, *valores[cod][::-1], df['data_cadastro'].max().strftime('%Y-%m-%d %H:%M:%S')))

                        # Inserção em massa dos dados
                        if insercoes:
                            insercao_query = """
                            INSERT INTO machine_learning.leituras_consecutivas
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
                            await cursor.executemany(insercao_query, insercoes)
                            await conn.commit()

        except Exception as e:
            print(f"Erro ao processar os equipamentos: {str(e)}")

        # Intervalo entre execuções
        await asyncio.sleep(10)
'''


from statsmodels.tsa.ar_model import AutoReg
import numpy as np
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from pmdarima import auto_arima



async def verificar_e_obter_coeficiente(cod_equipamento, pool):
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
        print(f"An error occurred in verificar_e_obter_coeficiente: {e}")
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
    print('motor_padronizado:', motor_padronizado)

    return motor_padronizado

# Lista para armazenar os dados dos equipamentos
equipamentos_ativos = []

async def carregar_equipamentos_ativos(pool, equipamentos_ativos):
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
                equipamentos_ativos.clear()

                resultados = await cursor.fetchall()

                for equipamento in resultados:
                    # Agora estamos desempacotando 8 valores, incluindo nome_equipamento
                    codigo_equipamento, cod_usina, nome_usina, nome_equipamento, motor, marca, potencia, tensao = equipamento
                    # Limpar e padronizar o nome do motor e da marca
                    motor_padronizado = limpar_motor(marca, motor)
                    # Armazenar os dados limpos na lista
                    equipamentos_ativos.append((codigo_equipamento, cod_usina, nome_usina, nome_equipamento, motor_padronizado, marca, potencia, tensao))
                
                print("Equipamentos ativos carregados com sucesso!")
    
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
    
    print(f"Parâmetros min/max carregados para {len(parametros_min_max_motores)} motores.")

async def verificar_alertas(cod_equipamento, df, marca, motor, potencia,tensao, tensao_l1_l2, tensao_l2_l3, tensao_l3_l1, pool):
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
            print(f"Parâmetros encontrados para {chave_motor}\n")
            break

    # Se não encontrar os parâmetros, avisar
    if not parametros_motor:
        print(f"Parâmetros não encontrados para motor {motor} da marca {marca}\n")
        return df

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

    '''
    print('\nparâmetros do equipamento', cod_equipamento, 
          '\nLoad Speed', min_load_speed, '-->', max_load_speed,
          '\nPot. Ativa', min_potencia_ativa, '-->', max_potencia_ativa,
          '\nPressao do oleo', min_pressao_oleo, '-->', max_pressao_oleo,
          '\nRPM', min_rpm, '-->', max_rpm,
          '\nTemp. agua', max_temp_agua, '-->', min_temp_agua,
          '\nTemp ar de admissao', min_temp_ar_admissao, '-->', max_temp_ar_admissao,
          '\nPressao de admissao', min_pressao_admissao, '-->', max_pressao_admissao)

    '''
    
    # Obtenha os limites de cada parâmetro
    limites = {
        'Load Speed': {'min': parametros_motor_normalizado.get('Load Speed', {}).get('min', 0),
                       'max': parametros_motor_normalizado.get('Load Speed', {}).get('max', 80)},
        'Temperatura do ar de admissão': {'min': parametros_motor_normalizado.get('Temperatura do ar de admissão', {}).get('min', 0),
                                          'max': parametros_motor_normalizado.get('Temperatura do ar de admissão', {}).get('max', 100)},
        'Pressão de admissão': {'min': parametros_motor_normalizado.get('Pressão de admissão', {}).get('min', 0),
                                'max': parametros_motor_normalizado.get('Pressão de admissão', {}).get('max', 6)},
        'Potência Ativa': {'min': parametros_motor_normalizado.get('Potência Ativa', {}).get('min', 0),
        #               'max': potencia * 1.1 if potencia else parametros_motor_normalizado.get('Potência Ativa', {}).get('max', 400)}, 
                       'max': potencia * 0.9 if potencia else parametros_motor_normalizado.get('Potência Ativa', {}).get('max', 400)}, 
        'Pressão do Óleo': {'min': parametros_motor_normalizado.get('Pressão do Óleo', {}).get('min', 3.5),
                            'max': parametros_motor_normalizado.get('Pressão do Óleo', {}).get('max', 5.0)},
        'RPM': {'min': parametros_motor_normalizado.get('RPM', {}).get('min', 1798),
                'max': parametros_motor_normalizado.get('RPM', {}).get('max', 1802)},
        'Temperatura da Água': {'min': parametros_motor_normalizado.get('Temperatura da água', {}).get('min', 30),
                                'max': parametros_motor_normalizado.get('Temperatura da água', {}).get('max', 103)},
        'Tensao Bateria': {'min': parametros_motor_normalizado.get('Bateria', {}).get('min', 10),
                'max': parametros_motor_normalizado.get('Bateria', {}).get('max', 29.50)},
        
        'Frequencia': {'min': parametros_motor_normalizado.get('Frequencia', {}).get('min', 59),
                'max': parametros_motor_normalizado.get('Frequencia', {}).get('max', 61)},
        'Consumo': {'min': parametros_motor_normalizado.get('Consumo', {}).get('min', 10),
                'max': parametros_motor_normalizado.get('Consumo', {}).get('max', 29.50)},
        
        
        'Tensao L1-L2': {
            'min': tensao_l1_l2 * 0.9,
            'max': tensao_l1_l2 * 1.1
        },
        'Tensao L2-L3': {
            'min': tensao_l2_l3 * 0.9,
            'max': tensao_l2_l3 * 1.1
        },
        'Tensao L3-L1': {
            'min': tensao_l3_l1 * 0.9,
            'max': tensao_l3_l1 * 1.1
        }
        # 'Tensao L1-L2': {'min': tensao * 0.9 if tensao else parametros_motor_normalizado.get('Tensao', {}).get('min', 380 * 0.9),
        #                 'max': tensao * 1.1 if tensao else parametros_motor_normalizado.get('Tensao', {}).get('max', 380 * 1.1)},
        # 'Tensao L2-L3': {'min': tensao * 0.9 if tensao else parametros_motor_normalizado.get('Tensao', {}).get('min', 380 * 0.9),
        #                 'max': tensao * 1.1 if tensao else parametros_motor_normalizado.get('Tensao', {}).get('max', 380 * 1.1)},
        # 'Tensao L3-L1': {'min': tensao * 0.9 if tensao else parametros_motor_normalizado.get('Tensao', {}).get('min', 380 * 0.9),
        #                 'max': tensao * 1.1 if tensao else parametros_motor_normalizado.get('Tensao', {}).get('max', 380 * 1.1)}

    }

    # Adiciona a coluna Alerta com valor padrão 0
    df['Alerta'] = 0

    '''
    
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
    '''

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
    'Load Speed': 10,
    'Pressão do Óleo': 1,
    'Potência Ativa': 50,
    'Temperatura do ar de admissão': 10,
    'Temperatura da Água': 15,
    'RPM': 50,
    'Tensao Bateria': 5,
    'Pressão de admissão': 0.5,
    'Tensao L1-L2': 10,
    'Tensao L2-L3': 10,
    'Tensao L3-L1': 10
}

async def fazer_previsao_sempre_alerta(cod_equipamento, pool, equipamentos_ativos):
    sensores = {
        3: 'Potência Ativa', 
        6: 'Tensao L1-L2',
        7: 'Tensao L2-L3',
        8: 'Tensao L3-L1',
        16: 'Frequencia',
        19: 'Tensao Bateria',
        20: 'RPM',
        21: 'Pressão do Óleo',
        23: 'Consumo',
        24: 'Horimetro',
        25: 'Temperatura da Água',
        76: 'Temperatura do ar de admissão',
        77: 'Pressão de admissão',
        114: 'Load Speed'
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
                    await carregar_equipamentos_ativos(pool, equipamentos_ativos)
                                

                # Verificar se o valor do cod_campo 114 (Load Speed) é 0
                if 'Load Speed' in valores_atuais and any(val == 0 for val in valores_atuais['Load Speed']):
                #    print(f"Equipamento {cod_equipamento} pulado devido a Load Speed ser 0")
                    return None, None, None, None, None, False, False, False, False
                
                # Verificar se algum dos valores está vazio ou nulo
                if any(v is None for v in valores_atuais.values()):
                    print(f"Erro: Valores nulos detectados nos sensores do equipamento {cod_equipamento}")
                    return None, None, None, None, None, False, False, False, False

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
                    await cursor.execute("""
                        UPDATE machine_learning.leituras_consecutivas
                        SET alerta = 0
                        WHERE cod_equipamento = %s
                    """, (cod_equipamento,))
                    await conn.commit()
                    return {}, {}, {}, {}, {}, {}, {}, {}, {}

                # Verificar se a diferença é maior que 15 minutos
                if (agora - data_cadastro) > timedelta(minutes=10):
                #    print(f"Equipamento {cod_equipamento}: Ignorado, data_cadastro é mais de 10 minutos atrás.")
                    return {}, {}, {}, {}, {}, {}, {}, {}, {}
    
                print('\n----------------------------------------------------------------------------------------------------------------\n')

                # Procurar o equipamento na lista de equipamentos ativos
                equipamento_info = next((equipamento for equipamento in equipamentos_ativos if equipamento[0] == cod_equipamento), None)

                if equipamento_info:
                    cod_usina, nome_usina, nome_equipamento, motor, marca, potencia, tensao = equipamento_info[1], equipamento_info[2], equipamento_info[3], equipamento_info[4], equipamento_info[5], equipamento_info[6], equipamento_info[7]
                    print(f"Equipamento {cod_equipamento} ({nome_equipamento}) pertence à usina {nome_usina} (código: {cod_usina}) com motor {motor} da marca {marca}, de potencia {potencia} e tensao {tensao}")
                else:
                    print(f"Equipamento {cod_equipamento} não encontrado na lista de equipamentos ativos.")
                    return None, None, None, None, None, False, False, False, False



                # Obter coeficientes e interceptos
                coeficientes, interceptos = await verificar_e_obter_coeficiente(cod_equipamento, pool)

                if not coeficientes or not interceptos:
                    return {}, {}, {}, {}, {}, {}, {}, {}, {}

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
                df_previsoes['Alerta_Final'] = df_previsoes.apply(lambda row: verificar_alerta_seguindo_lista(row, lista_parametros), axis=1)

                # Formatar valores numéricos para duas casas decimais
                df_previsoes = formatar_valores(df_previsoes)

                # Aplicar as regras de negócio (verificar alertas)
                df_previsoes, limites = await verificar_alertas(cod_equipamento, df_previsoes, marca, motor, potencia, tensao,
                                                                                                       valores_atuais['Tensao L1-L2'][-1],
                                                                                                        valores_atuais['Tensao L2-L3'][-1],
                                                                                                        valores_atuais['Tensao L3-L1'][-1],
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

                            # if valor < limite_menos:
                            #     contagem_abaixo_do_limite[sensor_nome] += 1
                            # elif valor > limite_mais:
                            #     contagem_acima_do_limite[sensor_nome] += 1

                #        print(f"{sensor_nome}: Abaixo do limite: {contagem_abaixo_do_limite[sensor_nome]}, Acima do limite: {contagem_acima_do_limite[sensor_nome]}\n")




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

                # Printar os valores e previsões
                for sensor in ['Load Speed', 'Potência Ativa', 'RPM', 'Pressão do Óleo', 'Temperatura da Água', 'Pressão de admissão', 'Temperatura do ar de admissão', 'Tensao Bateria', 'Frequencia', 'Consumo', 'Horimetro']:
                    if sensor in valores_atuais:
                        print(f'\n{sensor} Atual:', valores_atuais[sensor])
                        print(f'Contagem Acima do Limite Real ({sensor}):', contagem_acima_do_limite.get(sensor, 0))
                        print(f'Contagem Abaixo do Limite Real ({sensor}):', contagem_abaixo_do_limite.get(sensor, 0))
                    if sensor in previsoes:
                        print(f'{sensor} Previsto:', previsoes[sensor])
                        print(f'Contagem Acima do Limite Previsão ({sensor}):', contagem_acima_do_limite_previsao.get(sensor, 0))
                        print(f'Contagem Abaixo do Limite Previsão ({sensor}):', contagem_abaixo_do_limite_previsao.get(sensor, 0))


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
                    contagem_acima_do_limite_previsao  # Contagem de valores acima do limite previsto
                )


    except Exception as e:
        print(f"An error occurred in fazer_previsao_sempre_alerta: {e}")
        return {}, {}, {}, {}, {}, {}, {}, {}, {}



# Dicionário de dependência para as previsões

# lista_parametros_previsao = {
#     'Load Speed': {
#         'tipo': 'previsao',  # Definindo o tipo da chave principal
#         'acima': {
#             'Potência Ativa': {'tipo': 'real', 'condicao': 'acima'}, 
#         },
#         'abaixo': {
#             'Pressão do Óleo': {'tipo': 'previsao', 'condicao': 'abaixo'}
#         },
#         'tipo': 'real',  # Definindo o tipo da chave principal
#         'acima': {
#             'Potência Ativa': {'tipo': 'real', 'condicao': 'acima'}, 
#         },
#         'abaixo': {
#             'Pressão do Óleo': {'tipo': 'real', 'condicao': 'abaixo'}
#         },
#     },
#     'Pressão do Óleo': {
#         'tipo': 'previsao',  # Definindo o tipo da chave principal
#         'acima': {
#             'Temperatura da Água': {'tipo': 'real', 'condicao': 'acima'}
#         },
#         'abaixo': {
#             'Temperatura da Água': {'tipo': 'real', 'condicao': 'abaixo'}
#         },

#     },
#     'Temperatura da Água': {
#         'tipo': 'previsao',  # Definindo o tipo da chave principal
#         'acima': {
#             'Potência Ativa': {'tipo': 'real', 'condicao': 'acima'}, 
#         },
#         'abaixo': {
#             'Potência Ativa': {'tipo': 'real', 'condicao': 'abaixo'}, 
#         }
#     },
#     'Temperatura do ar de admissão': {
#         'tipo': 'previsao',  # Definindo o tipo da chave principal
#         'acima': {
#             'Potência Ativa': {'tipo': 'real', 'condicao': 'acima'}, 
#         },
#         'abaixo': {
#             'RPM': {'tipo': 'previsao', 'condicao': 'acima'}, 
#         }
#     },
# }

lista_parametros_previsao = {
    'Load Speed': {
        'previsao': {
            'acima': {
                'Pressão do Óleo': {'tipo': 'previsao', 'condicao': 'abaixo'}
            },
            'abaixo': {
                'Pressão do Óleo': {'tipo': 'previsao', 'condicao': 'abaixo'}
            }
        },
        'real': {
            'acima': {
                'Pressão do Óleo': {'tipo': 'real', 'condicao': 'abaixo'}
            },
            'abaixo': {
                'Pressão do Óleo': {'tipo': 'real', 'condicao': 'abaixo'}
            }
        }
    },
    'Pressão do Óleo': {
        'previsao': {
            'acima': {
                'Load Speed': {'tipo': 'previsao', 'condicao': 'acima'},
                'Temperatura da Água': {'tipo': 'previsao', 'condicao': 'acima'}
            },
            'abaixo': {
                'Load Speed': {'tipo': 'real', 'condicao': 'acima'},
                'Temperatura da Água': {'tipo': 'previsao', 'condicao': 'abaixo'}
            }
        },
        'real': {
            'acima': {
                'Load Speed': {'tipo': 'previsao', 'condicao': 'acima'},
                'Temperatura da Água': {'tipo': 'real', 'condicao': 'acima'}
            },
            'abaixo': {
                'Load Speed': {'tipo': 'previsao', 'condicao': 'acima'},
                'Temperatura da Água': {'tipo': 'real', 'condicao': 'abaixo'}
            }
        }
    },
    'Temperatura da Água': {
        'previsao': {
            'acima': {
                'Potência Ativa': {'tipo': 'real', 'condicao': 'acima'}
            },
            'abaixo': {
                'Potência Ativa': {'tipo': 'real', 'condicao': 'abaixo'}
            }
        },
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
        'previsao': {
            'acima': {
                'Potência Ativa': {'tipo': 'real', 'condicao': 'acima'}
            },
            'abaixo': {
                'RPM': {'tipo': 'previsao', 'condicao': 'abaixo'}
            }
        },
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



async def enviar_previsao_valor_equipamento_alerta(cod_equipamentos, tabelas, cod_campo_especificados, pool, equipamentos_ativos):

    while True:
        for cod_equipamento in cod_equipamentos:
            try:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (cod_equipamento,))
                        valores_atuais_114 = await cursor.fetchone()

                        if valores_atuais_114 is None:
                            continue

                        # Função para fazer previsões e obter as contagens
                        nome_equipamento, nome_usina, cod_usina, valores_atuais, previsoes, contagem_abaixo_do_limite, contagem_acima_do_limite, contagem_abaixo_do_limite_previsao, contagem_acima_do_limite_previsao = await fazer_previsao_sempre_alerta(cod_equipamento, pool, equipamentos_ativos)

                        if previsoes is None or contagem_abaixo_do_limite is None or contagem_acima_do_limite is None:
                            continue

                        # Imprimir as previsões e contagens para depuração
                    #    print(f"\n{cod_equipamento} - \nvalores_atuais: {valores_atuais} \nPrevisões: {previsoes} \n[contagem_abaixo_do_limite - {contagem_abaixo_do_limite}\ncontagem_acima_do_limite - {contagem_acima_do_limite}]\n[contagem_abaixo_do_limite_previsao, {contagem_abaixo_do_limite_previsao}\ncontagem_acima_do_limite_previsao, {contagem_acima_do_limite_previsao}]")

                        # Obtém o id_telegram do cod_usuario=374
                        await cursor.execute("SELECT id_telegram FROM usuarios_telegram WHERE cod_usuario = %s", (374,))
                        id_telegram_result = await cursor.fetchone()

                        if id_telegram_result is None:
                            print("Nenhum id_telegram encontrado para o cod_usuario = 374.")
                            return

                        id_usuario = id_telegram_result[0]
    
                        '''
                        # Verificar as condições com base no dicionário lista_parametros_previsao
                        for chave_principal, condicoes in lista_parametros_previsao.items():
                            if chave_principal in previsoes:
                                tipo_chave_principal = condicoes.get('tipo', 'real')  # Obtém o tipo da chave principal
                                print(f'\nChave principal: {chave_principal}, Tipo: {tipo_chave_principal}')
                                
                                # Escolher as variáveis de contagem com base no tipo da chave principal
                                if tipo_chave_principal == 'real':
                                    contagem_acima = contagem_acima_do_limite
                                    contagem_abaixo = contagem_abaixo_do_limite
                                elif tipo_chave_principal == 'previsao':
                                    contagem_acima = contagem_acima_do_limite_previsao
                                    contagem_abaixo = contagem_abaixo_do_limite_previsao

                                # Verificar 'acima do limite'
                                if contagem_acima.get(chave_principal, 0) == 1:
                                    condicoes_acima = condicoes.get('acima', {})
                                    print(f"Condições 'acima': {condicoes_acima}")
                                    todos_parametros_ok = True
                                    parametros_violados = []

                                    for subparametro, detalhes in condicoes_acima.items():
                                        tipo = detalhes['tipo']
                                        condicao = detalhes['condicao']
                                        print(f"Subparâmetro: {subparametro}, Tipo: {tipo}, Condição: {condicao}")

                                        if tipo == 'real':
                                            if condicao == 'acima' and contagem_acima.get(subparametro, 0) != 1:
                                                todos_parametros_ok = False
                                                parametros_violados.append(f"{subparametro} (esperado acima)")
                                                print(f"Falha no parâmetro {subparametro}: esperado 'acima', mas não está.")
                                                break
                                            elif condicao == 'abaixo' and contagem_abaixo.get(subparametro, 0) != 1:
                                                todos_parametros_ok = False
                                                parametros_violados.append(f"{subparametro} (esperado abaixo)")
                                                print(f"Falha no parâmetro {subparametro}: esperado 'abaixo', mas não está.")
                                                break
                                        elif tipo == 'previsao':
                                            if condicao == 'acima' and contagem_acima_do_limite_previsao.get(subparametro, 0) != 1:
                                                todos_parametros_ok = False
                                                parametros_violados.append(f"{subparametro} (esperado acima na previsão)")
                                                print(f"Falha no parâmetro {subparametro}: esperado 'acima' na previsão, mas não está.")
                                                break
                                            elif condicao == 'abaixo' and contagem_abaixo_do_limite_previsao.get(subparametro, 0) != 1:
                                                todos_parametros_ok = False
                                                parametros_violados.append(f"{subparametro} (esperado abaixo na previsão)")
                                                print(f"Falha no parâmetro {subparametro}: esperado 'abaixo' na previsão, mas não está.")
                                                break

                                    if todos_parametros_ok:
                                        mensagem = (f"Alerta no equipamento {cod_equipamento}\n Previsão para {chave_principal} acima do limite.\n "
#                                                    f"Verifique os seguintes parâmetros relacionados:\n {', '.join(parametros_violados)}.")
                                                    f"Verifique os seguintes parâmetros relacionados:\n {', '.join(condicoes_acima)}.")
                                        print(mensagem)
                                        await bot.send_message(id_usuario, mensagem)

                                # Verificar 'abaixo do limite'
                                if contagem_abaixo.get(chave_principal, 0) == 1:
                                    condicoes_abaixo = condicoes.get('abaixo', {})
                                    print(f"Condições 'abaixo': {condicoes_abaixo}")
                                    todos_parametros_ok = True
                                    parametros_violados = []

                                    for subparametro, detalhes in condicoes_abaixo.items():
                                        tipo = detalhes['tipo']
                                        condicao = detalhes['condicao']
                                        print(f"Subparâmetro: {subparametro}, Tipo: {tipo}, Condição: {condicao}")

                                        if tipo == 'real':
                                            if condicao == 'acima' and contagem_acima.get(subparametro, 0) != 1:
                                                todos_parametros_ok = False
                                                parametros_violados.append(f"{subparametro} (esperado acima)")
                                                print(f"Falha no parâmetro {subparametro}: esperado 'acima', mas não está.")
                                                break
                                            elif condicao == 'abaixo' and contagem_abaixo.get(subparametro, 0) != 1:
                                                todos_parametros_ok = False
                                                parametros_violados.append(f"{subparametro} (esperado abaixo)")
                                                print(f"Falha no parâmetro {subparametro}: esperado 'abaixo', mas não está.")
                                                break
                                        elif tipo == 'previsao':
                                            if condicao == 'acima' and contagem_acima_do_limite_previsao.get(subparametro, 0) != 1:
                                                todos_parametros_ok = False
                                                parametros_violados.append(f"{subparametro} (esperado acima na previsão)")
                                                print(f"Falha no parâmetro {subparametro}: esperado 'acima' na previsão, mas não está.")
                                                break
                                            elif condicao == 'abaixo' and contagem_abaixo_do_limite_previsao.get(subparametro, 0) != 1:
                                                todos_parametros_ok = False
                                                parametros_violados.append(f"{subparametro} (esperado abaixo na previsão)")
                                                print(f"Falha no parâmetro {subparametro}: esperado 'abaixo' na previsão, mas não está.")
                                                break

                                    if todos_parametros_ok:
                                        mensagem = (f"Alerta no equipamento {cod_equipamento}\n Previsão para {chave_principal} abaixo do limite.\n "
#                                                    f"Verifique os seguintes parâmetros relacionados:\n{', '.join(parametros_violados)}.")
                                                    f"Verifique os seguintes parâmetros relacionados:\n{', '.join(condicoes_abaixo)}.")

                                        print(mensagem)
                                        await bot.send_message(id_usuario, mensagem)
                            '''



                        # Verificar as condições com base no dicionário lista_parametros_previsao
                        for chave_principal, condicoes in lista_parametros_previsao.items():
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
                                            print(f"Condições 'acima': {condicoes_acima}")
                                            todos_parametros_ok = True
                                            parametros_violados = []

                                            for subparametro, detalhes in condicoes_acima.items():
                                                tipo = detalhes['tipo']
                                                condicao = detalhes['condicao']

                                                # Verificar contagem com base no tipo
                                                if tipo == 'real':
                                                    valor_real = valores_atuais.get(subparametro, "N/A")
                                                    valor_previsto = previsoes.get(subparametro, "N/A")

                                                    if condicao == 'acima' and contagem_acima_do_limite.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado acima, atual: {valor_real}, previsto: {valor_previsto})")
                                                        print(f"Falha no parâmetro {subparametro}: esperado 'acima', mas não está.")
                                                        break
                                                    elif condicao == 'abaixo' and contagem_abaixo_do_limite.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado abaixo, atual: {valor_real}, previsto: {valor_previsto})")
                                                        print(f"Falha no parâmetro {subparametro}: esperado 'abaixo', mas não está.")
                                                        break

                                                elif tipo == 'previsao':
                                                    valor_previsto = previsoes.get(subparametro, "N/A")

                                                    if condicao == 'acima' and contagem_acima_do_limite_previsao.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado acima na previsão, previsto: {valor_previsto})")
                                                        print(f"Falha no parâmetro {subparametro}: esperado 'acima' na previsão, mas não está.")
                                                        break
                                                    elif condicao == 'abaixo' and contagem_abaixo_do_limite_previsao.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado abaixo na previsão, previsto: {valor_previsto})")
                                                        print(f"Falha no parâmetro {subparametro}: esperado 'abaixo' na previsão, mas não está.")
                                                        break

                                            if todos_parametros_ok:
                                                # Transformando as listas de valores em strings sem colchetes
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
                                                print(f'enviando mensagem para a chave {chave_principal} e o parametro {condicoes_acima}')
                                                await bot.send_message(id_usuario, mensagem, parse_mode='HTML')

                                        # Verificar 'abaixo do limite'
                                        if contagem_abaixo.get(chave_principal, 0) == 1:
                                            condicoes_abaixo = condicoes[tipo_chave_principal].get('abaixo', {})
                                            print(f"Condições 'abaixo': {condicoes_abaixo}")
                                            todos_parametros_ok = True
                                            parametros_violados = []

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
                                                        print(f"Falha no parâmetro {subparametro}: esperado 'acima', mas não está.")
                                                        break
                                                    elif condicao == 'abaixo' and contagem_abaixo_do_limite.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado abaixo, atual: {valor_real}, previsto: {valor_previsto})")
                                                        print(f"Falha no parâmetro {subparametro}: esperado 'abaixo', mas não está.")
                                                        break

                                                elif tipo == 'previsao':
                                                    valor_previsto = previsoes.get(subparametro, "N/A")

                                                    if condicao == 'acima' and contagem_acima_do_limite_previsao.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado acima na previsão, previsto: {valor_previsto})")
                                                        print(f"Falha no parâmetro {subparametro}: esperado 'acima' na previsão, mas não está.")
                                                        break
                                                    elif condicao == 'abaixo' and contagem_abaixo_do_limite_previsao.get(subparametro, 0) != 1:
                                                        todos_parametros_ok = False
                                                        parametros_violados.append(f"{subparametro} (esperado abaixo na previsão, previsto: {valor_previsto})")
                                                        print(f"Falha no parâmetro {subparametro}: esperado 'abaixo' na previsão, mas não está.")
                                                        break

                                            if todos_parametros_ok:
                                                # Transformando as listas de valores em strings sem colchetes
                                                valores_reais_formatados = ', '.join(map(str, valores_atuais.get(chave_principal, ['N/A'])))
                                                valores_previstos_formatados = ', '.join(map(str, previsoes.get(chave_principal, ['N/A'])))
                                                valores_reais_subparametro = ', '.join(map(str, valores_atuais.get(subparametro, ['N/A'])))
                                                valores_previstos_subparametro = ', '.join(map(str, previsoes.get(subparametro, ['N/A'])))

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
                                                print(f'enviando mensagem para a chave {chave_principal} e o parametro {condicoes_abaixo}')
                                                await bot.send_message(id_usuario, mensagem, parse_mode='HTML')



                        # # Verificar as condições com base no dicionário lista_parametros_previsao
                        # for chave_principal, condicoes in lista_parametros_previsao.items():
                        #     if chave_principal in previsoes:
                        #         tipo_chave_principal = condicoes.get('tipo', 'real')  # Obtém o tipo da chave principal
                        #         print(f'\nChave principal: {chave_principal}, Tipo: {tipo_chave_principal}')

                        #         # Escolher as variáveis de contagem com base no tipo da chave principal
                        #         if tipo_chave_principal == 'real':
                        #             contagem_acima = contagem_acima_do_limite
                        #             contagem_abaixo = contagem_abaixo_do_limite
                        #         elif tipo_chave_principal == 'previsao':
                        #             contagem_acima = contagem_acima_do_limite_previsao
                        #             contagem_abaixo = contagem_abaixo_do_limite_previsao

                        #         # Verificar 'acima do limite'
                        #         if contagem_acima.get(chave_principal, 0) == 1:
                        #             condicoes_acima = condicoes.get('acima', {})
                        #             print(f"Condições 'acima': {condicoes_acima}")
                        #             todos_parametros_ok = True
                        #             parametros_violados = []

                        #             for subparametro, detalhes in condicoes_acima.items():
                        #                 tipo = detalhes['tipo']
                        #                 condicao = detalhes['condicao']

                        #                 # Verificar contagem apenas se o tipo corresponder (real ou previsão)
                        #                 if tipo == 'real':
                        #                     valor_real = valores_atuais.get(subparametro, "N/A")
                        #                     valor_previsto = previsoes.get(subparametro, "N/A")
                        #                     print('tipo', tipo, 'condicao', condicao, 'contagem_acima', contagem_acima, 'subparametro', subparametro)

                        #                     if condicao == 'acima' and contagem_acima_do_limite.get(subparametro, 0) != 1:
                        #                         todos_parametros_ok = False
                        #                         parametros_violados.append(f"{subparametro} (esperado acima, atual: {valor_real}, previsto: {valor_previsto})")
                        #                         print(f"Falha no parâmetro {subparametro}: esperado 'acima', mas não está.")
                        #                         break
                        #                     elif condicao == 'abaixo' and contagem_abaixo_do_limite.get(subparametro, 0) != 1:
                        #                         todos_parametros_ok = False
                        #                         parametros_violados.append(f"{subparametro} (esperado abaixo, atual: {valor_real}, previsto: {valor_previsto})")
                        #                         print(f"Falha no parâmetro {subparametro}: esperado 'abaixo', mas não está.")
                        #                         break

                        #                 elif tipo == 'previsao':
                        #                     valor_previsto = previsoes.get(subparametro, "N/A")
                        #                     print('tipo', tipo, 'condicao', condicao)

                        #                     if condicao == 'acima' and contagem_acima_do_limite_previsao.get(subparametro, 0) != 1:
                        #                         todos_parametros_ok = False
                        #                         parametros_violados.append(f"{subparametro} (esperado acima na previsão, previsto: {valor_previsto})")
                        #                         print(f"Falha no parâmetro {subparametro}: esperado 'acima' na previsão, mas não está.")
                        #                         break
                        #                     elif condicao == 'abaixo' and contagem_abaixo_do_limite_previsao.get(subparametro, 0) != 1:
                        #                         todos_parametros_ok = False
                        #                         parametros_violados.append(f"{subparametro} (esperado abaixo na previsão, previsto: {valor_previsto})")
                        #                         print(f"Falha no parâmetro {subparametro}: esperado 'abaixo' na previsão, mas não está.")
                        #                         break



                        #             if todos_parametros_ok:
                        #                 # Transformando as listas de valores em strings sem colchetes
                        #                 valores_reais_formatados = ', '.join(map(str, valores_atuais.get(chave_principal, ['N/A'])))
                        #                 valores_previstos_formatados = ', '.join(map(str, previsoes.get(chave_principal, ['N/A'])))
                        #                 valores_reais_subparametro = ', '.join(map(str, valores_atuais.get(subparametro, ['N/A'])))
                        #                 valores_previstos_subparametro = ', '.join(map(str, previsoes.get(subparametro, ['N/A'])))

                        #                 mensagem = (
                        #                     f"🟡 <b>ALERTA!</b> \n\nUsina: {cod_usina} - {nome_usina}\n"
                        #                     f'<a href="https://supervisorio.brggeradores.com.br/beta/detalhesgmg.php?codUsina={cod_usina}&codEquip={cod_equipamento}">Ir para o equipamento</a>\n\n'
                        #                     f"Equipamento: {cod_equipamento} ({nome_equipamento})\n"
                        #                     f"Previsão para {chave_principal} acima do limite.\n"
                        #                     f"Valores reais: {valores_reais_formatados}\n"
                        #                     f"Valores previstos: {valores_previstos_formatados}\n\n"
                        #                     f"Parâmetros fora do padrão: {', '.join(condicoes_acima)}\n"
                        #                     f"Valores reais: {valores_reais_subparametro}\n"
                        #                     f"Valores previstos: {valores_previstos_subparametro}\n"
                        #                 )
                        #             #    print(mensagem)
                        #                 print(f'enviando mensagem para a chave {chave_principal} e o parametro {condicoes_acima}')
                        #                 await bot.send_message(id_usuario, mensagem, parse_mode='HTML')


                        #         # Verificar 'abaixo do limite'
                        #         if contagem_abaixo.get(chave_principal, 0) == 1:
                        #             condicoes_abaixo = condicoes.get('abaixo', {})
                        #             print(f"Condições 'abaixo': {condicoes_abaixo}")
                        #             todos_parametros_ok = True
                        #             parametros_violados = []

                        #             for subparametro, detalhes in condicoes_abaixo.items():
                        #                 tipo = detalhes['tipo']
                        #                 condicao = detalhes['condicao']

                        #                 # Verificar contagem apenas se o tipo corresponder (real ou previsão)
                        #                 if tipo == 'real':
                        #                     valor_real = valores_atuais.get(subparametro, "N/A")
                        #                     valor_previsto = previsoes.get(subparametro, "N/A")
                        #                     print('tipo', tipo, 'condicao', condicao, 'contagem_abaixo', contagem_abaixo, 'subparametro', subparametro)

                        #                     if condicao == 'acima' and contagem_acima_do_limite.get(subparametro, 0) != 1:
                        #                         todos_parametros_ok = False
                        #                         parametros_violados.append(f"{subparametro} (esperado acima, atual: {valor_real}, previsto: {valor_previsto})")
                        #                         print(f"Falha no parâmetro {subparametro}: esperado 'acima', mas não está.")
                        #                         break
                        #                     elif condicao == 'abaixo' and contagem_abaixo_do_limite.get(subparametro, 0) != 1:
                        #                         todos_parametros_ok = False
                        #                         parametros_violados.append(f"{subparametro} (esperado abaixo, atual: {valor_real}, previsto: {valor_previsto})")
                        #                         print(f"Falha no parâmetro {subparametro}: esperado 'abaixo', mas não está.")
                        #                         break

                        #                 elif tipo == 'previsao':
                        #                     valor_previsto = previsoes.get(subparametro, "N/A")
                        #                     print('tipo', tipo, 'condicao', condicao)

                        #                     if condicao == 'acima' and contagem_acima_do_limite_previsao.get(subparametro, 0) != 1:
                        #                         todos_parametros_ok = False
                        #                         parametros_violados.append(f"{subparametro} (esperado acima na previsão, previsto: {valor_previsto})")
                        #                         print(f"Falha no parâmetro {subparametro}: esperado 'acima' na previsão, mas não está.")
                        #                         break
                        #                     elif condicao == 'abaixo' and contagem_abaixo_do_limite_previsao.get(subparametro, 0) != 1:
                        #                         todos_parametros_ok = False
                        #                         parametros_violados.append(f"{subparametro} (esperado abaixo na previsão, previsto: {valor_previsto})")
                        #                         print(f"Falha no parâmetro {subparametro}: esperado 'abaixo' na previsão, mas não está.")
                        #                         break

                        #             if todos_parametros_ok:
                        #                 # Transformando as listas de valores em strings sem colchetes
                        #                 valores_reais_formatados = ', '.join(map(str, valores_atuais.get(chave_principal, ['N/A'])))
                        #                 valores_previstos_formatados = ', '.join(map(str, previsoes.get(chave_principal, ['N/A'])))
                        #                 valores_reais_subparametro = ', '.join(map(str, valores_atuais.get(subparametro, ['N/A'])))
                        #                 valores_previstos_subparametro = ', '.join(map(str, previsoes.get(subparametro, ['N/A'])))

                        #                 mensagem = (
                        #                     f"🟡 <b>ALERTA!</b> \n\nUsina: {cod_usina} - {nome_usina}\n"
                        #                     f'<a href="https://supervisorio.brggeradores.com.br/beta/detalhesgmg.php?codUsina={cod_usina}&codEquip={cod_equipamento}">Ir para o equipamento</a>\n\n'
                        #                     f"Equipamento: {cod_equipamento} ({nome_equipamento})\n"
                        #                     f"Previsão para {chave_principal} abaixo do limite.\n"
                        #                     f"Valores reais: {valores_reais_formatados}\n"
                        #                     f"Valores previstos: {valores_previstos_formatados}\n\n"
                        #                     f"Parâmetros fora do padrão: {', '.join(condicoes_abaixo)}\n"
                        #                     f"Valores reais: {valores_reais_subparametro}\n"
                        #                     f"Valores previstos: {valores_previstos_subparametro}\n"
                        #                 )
                        #             #    print(mensagem)
                        #                 print(f'enviando mensagem para a chave {chave_principal} e o parametro {condicoes_abaixo}')
                        #                 await bot.send_message(id_usuario, mensagem, parse_mode='HTML')

                                        
                                        

            except Exception as e:
                print(f"Erro ao processar equipamento {cod_equipamento}: {str(e)}")
        await asyncio.sleep(10)








import asyncio

# Lista de alarmes a ser verificada
alarmes_relevantes = {1, 243, 244, 253, 256, 259, 262, 265, 269, 272, 273, 279, 280, 281, 301, 304, 350, 351, 352, 
                      353, 356, 357, 381, 383, 384, 385, 386, 387, 388, 389, 390, 400, 401, 404, 405, 411, 412, 
                      413, 414, 415, 416, 471, 472, 473, 528, 590, 591, 592, 593, 594, 595, 596, 597, 598, 599, 
                      600, 602, 603, 604, 611, 615, 616, 617, 631, 635, 637, 638, 657, 658, 669, 678, 725, 727, 
                      728, 729, 730, 731, 732, 735}




async def check_and_update(pool):
    while True:
        print('Entrando no loop de verificação')
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Selecionar linhas onde valores_reais = '0.0, 0.0, 0.0, 0.0, 0.0'
                await cursor.execute("""
                    SELECT id, cod_equipamento, valores_reais, alarmes, data_cadastro 
                    FROM machine_learning.valores_previsao 
                    WHERE valores_reais = '0.0, 0.0, 0.0, 0.0, 0.0'
                """)
                rows = await cursor.fetchall()

                for row in rows:
                    id_linha, cod_equipamento, valores_reais, alarmes, data_cadastro = row

                    # Limpar e converter alarmes para uma lista de inteiros
                    alarmes_list = []
                    if alarmes:
                        alarmes = alarmes.strip().split(',')
                        alarmes_list = [int(alarme.strip(" ()")) for alarme in alarmes if alarme.strip(" ()").isdigit()]

                    # Verificar se algum alarme está na lista de alarmes relevantes
                    if any(alarme in alarmes_relevantes for alarme in alarmes_list):
                        # Consultar o último data_cadastro_quebra para o mesmo cod_equipamento
                        await cursor.execute("""
                            SELECT data_cadastro_quebra 
                            FROM machine_learning.valores_previsao 
                            WHERE cod_equipamento = %s 
                            AND data_cadastro_quebra IS NOT NULL
                            ORDER BY data_cadastro_quebra DESC
                            LIMIT 1
                        """, (cod_equipamento,))
                        last_data_cadastro_quebra = await cursor.fetchone()

                        # Se existir um data_cadastro_quebra e estiver dentro de 5 minutos, pular a atualização
                        if last_data_cadastro_quebra:
                            last_data_cadastro_quebra = last_data_cadastro_quebra[0]
                            if data_cadastro - last_data_cadastro_quebra <= timedelta(minutes=5):
                #                print(f'Skipping update for ID {id_linha} as the last update was within 5 minutes.')
                                continue

                        # Atualizar a coluna data_cadastro_quebra com o valor de data_cadastro
                        await cursor.execute("""
                            UPDATE machine_learning.valores_previsao 
                            SET data_cadastro_quebra = %s 
                            WHERE id = %s
                        """, (data_cadastro, id_linha))
                #        print(f'Atualizado data_cadastro_quebra para ID {id_linha} com data {data_cadastro}')

                        # Confirmar todas as atualizações de uma só vez
                        await conn.commit()
                        print('fim da atualizacao')
        await asyncio.sleep(10)


async def check_and_update_falhas(pool):
    while True:
        print("Iniciando verificação de falhas")
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Seleciona as linhas onde data_cadastro_quebra não é nulo
                await cursor.execute("""
                    SELECT id, cod_equipamento, cod_usina, data_cadastro_quebra, alerta_80, alerta_100, previsao 
                    FROM machine_learning.valores_previsao 
                    WHERE data_cadastro_quebra IS NOT NULL
                """)
                rows = await cursor.fetchall()

                # Dicionário para controlar os últimos registros por equipamento e alerta com timestamp
                last_insert_time = {}

                for row in rows:
                    id, cod_equipamento, cod_usina, data_cadastro_quebra, alerta_80, alerta_100, previsao = row
                    skip_entry = False

                    # Verificar se o equipamento e o alerta já foram inseridos dentro de 2 minutos antes e depois
                    if cod_equipamento in last_insert_time:
                        for alert, timestamp in last_insert_time[cod_equipamento].items():
                            time_diff = abs((data_cadastro_quebra - timestamp).total_seconds())
                            if time_diff <= 120:  # 2 minutos antes e depois
                                if (alert == 'alerta_80' and alerta_80) or \
                                   (alert == 'alerta_100' and alerta_100) or \
                                   (alert == 'previsao' and previsao):
                                    skip_entry = True
                    #                print(f"Pulando entrada para equipamento {cod_equipamento} devido a alerta {alert} já registrado dentro de 2 minutos com data {data_cadastro_quebra}.")
                                    break
                            if time_diff > 120:  # 2 minutos antes e depois
                                if (alert == 'alerta_80' and alerta_80) or \
                                   (alert == 'alerta_100' and alerta_100) or \
                                   (alert == 'previsao' and previsao):
                                    skip_entry = True
                    #                print(f"Pulando entrada para equipamento {cod_equipamento} devido a alerta {alert} já registrado antes de 2 minutos com data {data_cadastro_quebra}.")
                                    break
                    if skip_entry:
                        continue

                    # Atualizar last_insert_time para o equipamento e alerta
                    if cod_equipamento not in last_insert_time:
                        last_insert_time[cod_equipamento] = {}
                    
                    # Inserir os dados na tabela falhas_gerais
                    inserted = False

                    if alerta_80 and 'alerta_80' not in last_insert_time[cod_equipamento]:
                        await cursor.execute("""
                            INSERT INTO machine_learning.falhas_gerais (cod_equipamento, cod_usina, data_cadastro, falha, alerta_80)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (cod_equipamento, cod_usina, data_cadastro_quebra, 1, alerta_80))
                #        print(f'Inserido alerta_80 para equipamento {cod_equipamento} na falhas_gerais com data {data_cadastro_quebra}.')
                        last_insert_time[cod_equipamento]['alerta_80'] = data_cadastro_quebra
                        inserted = True

                    if alerta_100 and 'alerta_100' not in last_insert_time[cod_equipamento]:
                        await cursor.execute("""
                            INSERT INTO machine_learning.falhas_gerais (cod_equipamento, cod_usina, data_cadastro, falha, alerta_100)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (cod_equipamento, cod_usina, data_cadastro_quebra, 1, alerta_100))
                #        print(f'Inserido alerta_100 para equipamento {cod_equipamento} na falhas_gerais com data {data_cadastro_quebra}.')
                        last_insert_time[cod_equipamento]['alerta_100'] = data_cadastro_quebra
                        inserted = True

                    if previsao and 'previsao' not in last_insert_time[cod_equipamento]:
                        await cursor.execute("""
                            INSERT INTO machine_learning.falhas_gerais (cod_equipamento, cod_usina, data_cadastro, falha, previsao)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (cod_equipamento, cod_usina, data_cadastro_quebra, 1, previsao)) 
                #        print(f'Inserido previsao para equipamento {cod_equipamento} na falhas_gerais com data {data_cadastro_quebra}.')
                        last_insert_time[cod_equipamento]['previsao'] = data_cadastro_quebra
                        inserted = True

                    # Se nenhum valor foi inserido, insira uma linha com 0 em todos
                    if not inserted:
                        await cursor.execute("""
                            INSERT INTO machine_learning.falhas_gerais (cod_equipamento, cod_usina, data_cadastro, falha, alerta_80, alerta_100, previsao)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (cod_equipamento, cod_usina, data_cadastro_quebra, 1, 0, 0, 0))
                #        print(f'Inserido valores 0 para equipamento {cod_equipamento} na falhas_gerais com data {data_cadastro_quebra}.')
 
                    # Confirma as atualizações na tabela falhas_gerais
                    await conn.commit()

                print('escrito em falhas')
        # Esperar por 10 segundos antes de verificar novamente
        await asyncio.sleep(10)

async def adicionar_DataQuebra_FG(pool):  # Funçao para passar data_cadastro_quebra/valores_previsão para data_cadastro/falhas_gerais
    tamanho_lote = 1000000
    valor_offset = 0
    
    while True:
        print('Iniciando data_cadastro_quebra para tabela falhas gerais formatada como data_cadastro')
        
        async with pool.acquire() as conn:   # vp = tabela valores_previsao e fg = falhas_gerais 
            async with conn.cursor() as cursor:
                # Preparar a consulta SQL para selecionar linhas
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
                        
                    LIMIT %s OFFSET %s
                """, (tamanho_lote, valor_offset))

                linhas = await cursor.fetchall()


                # Inserir as linhas selecionadas na tabela falhas_gerais
                for linha in linhas:
                    cod_equipamento, cod_usina, data_cadastro_quebra, alerta_80, alerta_100, previsao = linha

                    await cursor.execute("""
                        INSERT INTO machine_learning.falhas_gerais (cod_equipamento, cod_usina, data_cadastro, falha, alerta_80, alerta_100, previsao)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (cod_equipamento, cod_usina, data_cadastro_quebra, 1, alerta_80, alerta_100, previsao))

                #    print('Inserido registro para falha em equipamento {data_cadastro_quebra} na tabela falhas_gerais.')

                
                # Confirmar a transação
                await conn.commit()
                print('Transação de dados pelas planilhas valores_previsto e falhas_gerais feitas com sucesso')
                # Atualizar o offset para o próximo lote
                valor_offset += tamanho_lote
                

async def on_startup(dp):
    dp.pool = await create_pool()

    # Certifique-se de que as tasks estão sendo aguardadas
    try:
        asyncio.create_task(processar_equipamentos_async(dp))
        asyncio.create_task(outros_processos_async(dp))
    except Exception as e:
        print(f"Erro ao iniciar tarefas: {e}")

async def processar_equipamentos_async(dp):
    try:
        tabelas = 'sup_geral.leituras'
        cod_equipamentos = await obter_equipamentos_validos(tabelas, dp.pool)
    #    cod_campo_especificados = ['3', '114', '21', '76']
        cod_campo_especificados = ['3','6','7','8','9','10', '11', '16', '19', '23', '24', '114', '21','76','25','20','77']
        await processar_equipamentos(cod_equipamentos, tabelas, cod_campo_especificados, dp.pool)
    
        await check_and_update(dp.pool)
        await check_and_update_falhas(dp.pool)
        await adicionar_DataQuebra_FG(dp.pool)
    
    except asyncio.CancelledError:
        print("Tarefa de processamento de equipamentos cancelada.")
    except Exception as e:
        print(f"Erro durante o processamento dos equipamentos: {e}")

async def outros_processos_async(dp):
    try:
        tabelas = 'sup_geral.leituras'
        cod_equipamentos = await obter_equipamentos_validos(tabelas, dp.pool)
        cod_campo_especificados = ['3','6','7','8','9','10', '11', '16', '19', '23', '24', '114', '21','76','25','20','77']

        # Certifique-se de que as tarefas são aguardadas
        tarefas = [
            asyncio.create_task(enviar_previsao_valor_equipamento_alerta(cod_equipamentos, tabelas, cod_campo_especificados, dp.pool, equipamentos_ativos)),
        ]
        await asyncio.gather(*tarefas)
    except asyncio.CancelledError:
        print("Tarefa de outros processos cancelada.")
    except Exception as e:
        print(f"Erro durante a execução de outros processos: {e}")


async def on_shutdown(dp):
    dp.pool.close()
    await dp.pool.wait_closed()
    
if __name__ == '__main__':
    try:
        executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)
    except KeyboardInterrupt:
        print("Interrompido pelo usuário")







