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
# Teclado principal
main_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
main_keyboard.add(KeyboardButton("Relatório"))

# Teclado de opções de relatórios
report_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
report_keyboard.add(
    KeyboardButton("1 dia"),
    KeyboardButton("2 dias"),
    KeyboardButton("7 dias"),
    KeyboardButton("15 dias"),
    KeyboardButton("1 mês")
)

#@dp.message_handler(commands=['relatorio'])
#@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    await message.reply("Bem-vindo! Escolha uma opção:", reply_markup=main_keyboard)

@dp.message_handler(lambda message: message.text == "Relatório")
async def show_report_options(message: types.Message):
    await message.reply("Escolha o período do relatório:", reply_markup=report_keyboard)
'''

class ReportStates(StatesGroup):
    period = State()
    funcionamento = State()

# Teclado principal
main_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
main_keyboard.add(KeyboardButton("Relatório"))
main_keyboard.add(KeyboardButton("Geradores Em Operação"))

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
    await message.reply("Bem-vindo! para interagir com o bot, use o botão que aparece ao lado do microfone:", reply_markup=main_keyboard)

@dp.message_handler(lambda message: message.text == "Relatório")
async def show_report_options(message: types.Message):
    await message.reply("Escolha o período do relatório:", reply_markup=report_keyboard)




# Inicializando os modelos Gemini-Pro e Gemini-Pro Vision
gemini_model = genai.GenerativeModel('gemini-pro')
gemini_vision_model = genai.GenerativeModel('gemini-pro-vision')

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
            image = Image.open(photo_data.name)
            
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


'''
async def fetch_report_data(pool, period, user_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT cod_usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
            result = await cursor.fetchone()
            if not result:
                return ["Usuário não encontrado."]
            cod_usuario = result[0]

            await cursor.execute("SELECT cod_usina FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
            cod_usinas = await cursor.fetchall()

            if not cod_usinas:
                return ["Nenhuma usina associada ao usuário."]

            usinas = [usina[0] for usina in cod_usinas]

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
            """ % (', '.join(['%s'] * len(usinas)), '%s', '%s')
            await cursor.execute(query, usinas + [period, period])
            log_data = await cursor.fetchall()

            if not log_data:
                return ["Nenhum dado encontrado para o período selecionado."]

            # Buscar número de geradores por usina
            await cursor.execute("""
                SELECT cod_usina, COUNT(*) AS num_geradores
                FROM sup_geral.equipamentos
                WHERE ativo = 1 
                  AND cod_tipo_equipamento IN (1, 3, 4, 12, 16, 18, 20, 22, 23, 27, 29, 33, 37, 40, 41, 43, 51, 55, 56)
                GROUP BY cod_usina
            """)
            geradores_por_usina = await cursor.fetchall()
            geradores_por_usina_dict = {usina[0]: usina[1] for usina in geradores_por_usina}

            # Buscar equipamentos em alerta na tabela leituras_consecutivas
            await cursor.execute("""
                SELECT cod_equipamento 
                FROM machine_learning.leituras_consecutivas 
                WHERE alerta = 1 AND cod_campo = 114
            """)
            equipamentos_alerta = await cursor.fetchall()
            equipamentos_alerta_set = set(equipamento[0] for equipamento in equipamentos_alerta)

            # Prepare the summarized report for Telegram
            telegram_report = "Relatório resumido:\n\n"
            usina_equipamento_map = {}
            usina_equipamento_map_gemini = {}

            dados_gemini = ""
            detailed_report = ""
'''


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

            #    print('telegram_report',telegram_report,'\n','detailed_report',detailed_report,'\n','dados_gemini',dados_gemini)
    #            print('dados_gemini',dados_gemini)

    #            return [telegram_report, detailed_report]
                return [telegram_report, detailed_report, dados_gemini]
    except Exception as e:
        print(f"Erro ao buscar dados do relatório: {str(e)}")
        return ["Erro ao buscar dados do relatório. Tente novamente."], "", ""




'''
@dp.message_handler(lambda message: message.text in ["1 dia", "2 dias", "7 dias", "15 dias", "1 mês"])
async def handle_report_period(message: types.Message):
    period_map = {
        "1 dia": 1,
        "2 dias": 2,
        "7 dias": 7,
        "15 dias": 15,
        "1 mês": 30
    }
    period = period_map[message.text]
    await message.reply(f"Você escolheu o relatório de {message.text}. Aguarde enquanto buscamos os dados...", reply_markup=main_keyboard)

    # Buscar os dados do relatório
    user_id = message.from_user.id
    pool = await create_pool()
    telegram_report, detailed_report, dados_gemini = await fetch_report_data(pool, period, user_id)

    # Enviar o relatório resumido no Telegram
    for part in telegram_report:
        await message.reply(part)

    # Armazenar o relatório detalhado para o PDF
    user_reports[user_id] = (detailed_report, period, dados_gemini)

    # Enviar botão para gerar PDF
    generate_pdf_button = InlineKeyboardMarkup().add(InlineKeyboardButton("Gerar PDF", callback_data="generate_pdf"))
    await message.reply("Deseja gerar o PDF do relatório?", reply_markup=generate_pdf_button)

    id_grupo = '-1002000715508'  # Certifique-se de que este ID esteja correto
        
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:   
            await cursor.execute("SELECT nome_telegram FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
            result = await cursor.fetchone()
            if not result:
                return await message.reply("Usuário não encontrado.")
            nome_usuario = result[0]

            mensagem = f'Usuário {nome_usuario} pediu relatório de {period} dias.'
            try:
                await bot.send_message(id_grupo, mensagem)
            except Exception as e:
                await message.reply(f"Erro ao enviar mensagem ao grupo: {e}")
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
        id_grupo = '-1002000715508'  # Certifique-se de que este ID esteja correto
        nome_usuario = ''
        
        period = user_reports[user_id]["period"]
        await message.reply(f"Você escolheu o funcionamento: {message.text}. Aguarde enquanto buscamos os dados...", reply_markup=main_keyboard)

            
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:   
                await cursor.execute("SELECT nome_telegram FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
                result = await cursor.fetchone()
                nome_usuario = ''
                if not result:
                    return await message.reply("Usuário não encontrado.")
                nome_usuario = result[0]

                mensagem = f'Usuário {nome_usuario} pediu funcionamento: {message.text} para {period} dias.'
                print(mensagem)
                try:
                    await bot.send_message(id_grupo, mensagem)
                except Exception as e:
                    await message.reply(f"Erro ao enviar mensagem ao grupo: {e}")
                
        # Buscar os dados do relatório
        try:
            report_data = await fetch_report_data(pool, period, user_id, funcionamento)
        finally:
            pool.close()
            await pool.wait_closed()

        if not report_data or report_data[0] == ["Nenhum dado encontrado para o período selecionado."]:
#        if report_data == "":
            await message.reply("Nenhum dado encontrado para o funcionamento selecionado.")
            await message.reply("Escolha o período do relatório novamente:", reply_markup=report_keyboard)
        else:
            telegram_report, detailed_report, dados_gemini = report_data

            # Enviar o relatório resumido no Telegram
        #    for part in telegram_report:
        #        await message.reply(part)

            user_reports[user_id] = (detailed_report, period, funcionamento, dados_gemini)

            # Criar e enviar o PDF diretamente sem botao
            pdf_filename = f"relatorio_{funcionamento}_{period}_dias.pdf"
            pdf_file_path = await create_pdf(detailed_report, dados_gemini, period, funcionamento, pdf_filename)
            await bot.send_document(chat_id=message.chat.id, document=InputFile(pdf_file_path, filename=pdf_filename))

            mensagem2 = f'Usuário {nome_usuario} completou o pedido do PDF.'
            print(mensagem2)

            try:
                await bot.send_message(id_grupo, mensagem2)
            except Exception as e:
                await message.reply(f"Erro ao enviar mensagem ao grupo: {e}")

            # Solicitar email do usuário
            email_button = InlineKeyboardMarkup().add(InlineKeyboardButton("Receber PDF por Email", callback_data="request_email"))
            await message.reply("Deseja receber o PDF por email?", reply_markup=email_button)

    except Exception as e:
        await message.reply(f"Ocorreu um erro ao processar sua solicitação: {str(e)}")
        logger.error(f"Error in handle_funcionamento_selection: {str(e)}")


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
    except Exception as e:
        await bot.send_message(user_id, f"Erro ao verificar o email: {str(e)}")
        logger.error(f"Error in handle_request_email: {str(e)}")
    finally:
        pool.close()
        await pool.wait_closed()

# Handler to send to existing email
@dp.callback_query_handler(lambda c: c.data == 'send_to_existing_email')
async def handle_send_to_existing_email(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    pool = await create_pool()

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
            await message.reply(f"PDF enviado para o email {to_email} com sucesso.", reply_markup=main_keyboard)
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




async def verificar_e_obter_coeficiente(cod_equipamento, pool):
    try:
        coeficiente_existente = 0.0
        intercepto_existente = 0.0
        pool = await create_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(f"SELECT * FROM machine_learning.coeficiente_geradores WHERE cod_equipamento = {cod_equipamento}")
                resultado = await cursor.fetchone()
                if resultado is not None:
                    coeficiente_existente = resultado[2]
                    intercepto_existente = resultado[3]
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
            coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(cod_equipamento_resultado, pool)

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


async def selecionar_GMG():
    try:
        pool = await create_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT codigo, ativo FROM sup_geral.tipos_equipamentos WHERE classe = 'GMG'")
                resultados = await cursor.fetchall()
                codigos = [resultado[0] for resultado in resultados]
        return codigos
    except Exception as e:
        print(f"An error occurred in selecionar_GMG: {e}")
        return []


#@dp.message_handler(commands=['teste'])
@dp.message_handler(lambda message: message.text == "Geradores Em Operação")
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



if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
    











''' funcionando bem '''
'''




from aiogram import Bot, Dispatcher, types
import asyncio
import mysql.connector
import atexit
import pandas as pd
from datetime import datetime, timedelta, time
import sys

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
from bot_relatorios import *

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
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle,FrameBreak
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


TOKEN = "6674960909:AAEf4Ky64lM6NILX3wZ6HJEASTt42Q2vopc"
#API_KEY = 'AIzaSyDfcjDbVZ2BbPJEuSpJ4wF_ATguRuffikA'

API_KEY = 'AIzaSyDf9hqXZvxOiCKaFSiIa0byrfEctP5mflI'

genai.configure(api_key=API_KEY)

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())


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



# Teclado principal
main_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
main_keyboard.add(KeyboardButton("Relatório"))

# Teclado de opções de relatórios
report_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
report_keyboard.add(
    KeyboardButton("1 dia"),
    KeyboardButton("2 dias"),
    KeyboardButton("7 dias"),
    KeyboardButton("15 dias"),
    KeyboardButton("1 mês")
)

@dp.message_handler(commands=['relatorio'])
async def send_welcome(message: types.Message):
    await message.reply("Bem-vindo! Escolha uma opção:", reply_markup=main_keyboard)

@dp.message_handler(lambda message: message.text == "Relatório")
async def show_report_options(message: types.Message):
    await message.reply("Escolha o período do relatório:", reply_markup=report_keyboard)


# Inicializando os modelos Gemini-Pro e Gemini-Pro Vision
gemini_model = genai.GenerativeModel('gemini-pro')
#gemini_vision_model = genai.GenerativeModel('gemini-pro-vision')
gemini_vision_model = genai.GenerativeModel('gemini-1.5-flash')

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
            image = Image.open(photo_data.name)
            
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
    if data_cadastro_quebra in ['Não houve falha', 'Em funcionamento']:
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
    if data_cadastro_quebra in ['Não houve falha', 'Em funcionamento']:
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
    if data_cadastro_quebra in ['Não houve falha', 'Em funcionamento']:
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
        f"{float(avg_temperatura_agua_mes_anterior):.2f}" if avg_temperatura_agua_mes_anterior is not None else "N/A"
    )
    avg_temperatura_agua_periodo_formatted = (
        f"{float(avg_temperatura_agua_periodo):.2f}" if avg_temperatura_agua_periodo is not None else "N/A"
    )
    max_temperatura_agua_periodo_formatted = (
        f"{float(max_temperatura_agua_periodo):.2f}" if max_temperatura_agua_periodo is not None else "N/A"
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
    if data_cadastro_quebra in ['Não houve falha', 'Em funcionamento']:
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
        return f"    Tensão L1 L2 média: {result['avg_tensao_l1_l2']:.2f}\n"

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
    if data_cadastro_quebra in ['Não houve falha', 'Em funcionamento']:
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
        return f"    Tensão L2 L3 média: {result['avg_tensao_l2_l3']:.2f}\n"

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
    if data_cadastro_quebra in ['Não houve falha', 'Em funcionamento']:
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
        return f"    Tensão L3 L1 média: {result['avg_tensao_l3_l1']:.2f}\n"

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
    if data_cadastro_quebra in ['Não houve falha', 'Em funcionamento']:
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
        return f"    Corrente L1 média: {result['avg_corrente_l1']:.2f}\n"

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
    if data_cadastro_quebra in ['Não houve falha', 'Em funcionamento']:
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
        return f"    Corrente L2 média: {result['avg_corrente_l2']:.2f}\n"

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
    if data_cadastro_quebra in ['Não houve falha', 'Em funcionamento']:
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
        return f"    Corrente L3 média: {result['avg_corrente_l3']:.2f}\n"

    return ""


async def fetch_report_data(pool, period, user_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT cod_usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
            result = await cursor.fetchone()
            if not result:
                return ["Usuário não encontrado."]
            cod_usuario = result[0]

            await cursor.execute("SELECT cod_usina FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
            cod_usinas = await cursor.fetchall()

            if not cod_usinas:
                return ["Nenhuma usina associada ao usuário."]

            usinas = [usina[0] for usina in cod_usinas]

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
            """ % (', '.join(['%s'] * len(usinas)), '%s', '%s')
            await cursor.execute(query, usinas + [period, period])
            log_data = await cursor.fetchall()

            if not log_data:
                return ["Nenhum dado encontrado para o período selecionado."]

            # Buscar número de geradores por usina
            await cursor.execute("""
                SELECT cod_usina, COUNT(*) AS num_geradores
                FROM sup_geral.equipamentos
                WHERE ativo = 1 
                  AND cod_tipo_equipamento IN (1, 3, 4, 12, 16, 18, 20, 22, 23, 27, 29, 33, 37, 40, 41, 43, 51, 55, 56)
                GROUP BY cod_usina
            """)
            geradores_por_usina = await cursor.fetchall()
            geradores_por_usina_dict = {usina[0]: usina[1] for usina in geradores_por_usina}

            # Buscar equipamentos em alerta na tabela leituras_consecutivas
            await cursor.execute("""
                SELECT cod_equipamento 
                FROM machine_learning.leituras_consecutivas 
                WHERE alerta = 1 AND cod_campo = 114
            """)
            equipamentos_alerta = await cursor.fetchall()
            equipamentos_alerta_set = set(equipamento[0] for equipamento in equipamentos_alerta)

            # Prepare the summarized report for Telegram
            telegram_report = "Relatório resumido:\n\n"
            usina_equipamento_map = {}
            usina_equipamento_map_gemini = {}

            dados_gemini = ""
            detailed_report = ""
            


            # Process log data and build content for each usina
            for row in log_data:
                cod_equipamento, cod_usina, data_previsto, data_previsto_saida, data_quebra, nome_usina, nome_equipamento = row

                # Verificar se o equipamento está em alerta atualmente
                if cod_equipamento in equipamentos_alerta_set and not data_quebra:
                    data_quebra = 'Em funcionamento'
                    tempo_total = 'Em funcionamento'
                    tempo_anormalidade = 'Em funcionamento' if not data_previsto_saida else datetime.strptime(str(data_previsto_saida), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                elif not data_quebra:
                    data_quebra = 'Não houve falha'
                    tempo_total = 'Não houve falha'
                    tempo_anormalidade = 'Não houve falha' if not data_previsto_saida else datetime.strptime(str(data_previsto_saida), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                else:
                    data_previsto_saida = data_previsto_saida if data_previsto_saida else 'Indefinido'
                    if data_previsto_saida != 'Indefinido':
                        tempo_anormalidade = datetime.strptime(str(data_previsto_saida), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                    else:
                        tempo_anormalidade = 'Não disponível'
                    
                    if data_quebra not in ['Não houve falha', 'Em funcionamento']:
                        tempo_total = datetime.strptime(str(data_quebra), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                    else:
                        tempo_total = 'Não disponível'

                
                # Consultar a tabela valores_previsao para obter mais dados
                await cursor.execute("""
                    SELECT DISTINCT alerta_80, alerta_100, previsao, valores_reais, valores_previstos, GROUP_CONCAT(DISTINCT alarmes) 
                    FROM machine_learning.valores_previsao 
                    WHERE cod_equipamento = %s 
                    AND ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300
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
                        diferenca_formatada = f"menos {abs(diferenca_percentual):.2f}%"
                    else:
                        diferenca_formatada = f"mais {diferenca_percentual:.2f}%"
    
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
                            <td>{alerta_status}</td>
                            <td>{max(valores_reais)}</td>
                            <td>{max(valores_previstos)}</td>
                            <td>{alarmes_text_tabela}</td>
                        </tr>
                    """
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
                            <td>{tempo_total}</td>
                            <td>Indefinido</td>
                            <td>Indefinido</td>
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

            ''' '''
        #    model = genai.GenerativeModel('gemini-1.5-pro-latest')
            model = genai.GenerativeModel('gemini-1.5-flash-latest')
            chat = model.start_chat(history=[])
            context = (
                "Aqui estão as definições dos parâmetros usados no relatório:<br/>"
                f'- Use esses dados como parâmetro, {dados_gemini}<br/>'
                "- Envie em formato HTML, utilizando <br/> para pular linha, <b> e </b> para negrito para formatar o texto do relatório, pois é um PDF que se gera.<br/>"
                
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
                - Ao dar enfase em alguma coisa, coloque negrito e cores nessa enfase. Enfase boa em cor verde e enfase ruim na cor vermelha.
                - Diga se estiver em funcionamento e pule duas linhas.
                - Diga qual o alerta e fale sobre. Diga sobre a Porcentagem de diferença, avise caso extrapole e pule duas linhas. 
                - Diga sobre a Pressão do óleo. Mostre a media dos valores, descreva o fato se o valor estiver fora do padrão e pule duas linhas.
                - Diga sobre a temperatura da agua e pule duas linhas.
                - Diga sobre a Pressão do Combustivel e pule duas linhas.
                - Diga sobre as correntes médias em Amperes e pule duas linhas.
                - Diga sobre as tensões médias em Volts e pule duas linhas.
                - Se o equipamento tiver Tempo total, quer dizer que teve falha, entao diga o tempo total e que o equipamento teve falha e devido a algum alarme X e pule duas linhas.
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

            # Finalizar com explicações específicas sobre os alarmes

#                 - Faça recomendações, dizendo as possiveis falhas e soluções e pule duas linhas.
#                 - Diga se teve algum alarme nessa hora, se sim, fale quantos e pule duas linhas.
#                 - diga qual o alerta e fale sobre, pule duas linhas.
#                 "- Tente prever em quanto tempo o equipamento pode parar e pule duas linhas"
#                 "- Reveja o texto do html, veja se tem algum erro, arrume antes de enviar."


            conclusions_prompt += (
                 context + "pule duas linhas. Recomendações: pule duas linhas"
                "- Envie em formato html pulando linha para cada recomendação, cuidando para fechar todas as tags e escrevendo corretamente."
                "- Diga o nome do equipamento e da usina, Faça recomendações em geral, dizendo as possiveis falhas e soluções de acordo com os dados e pule duas linhas."
                "- Tente dar um norte para a pessoa e pule duas linhas."
            )

            response_conclusions = chat.send_message(conclusions_prompt)
            conclusions = response_conclusions.text

            # Juntar a descrição geral formatada, o relatório detalhado e as conclusões
            final_report = "<br/><br/>" + formatted_general_report + "<br/><br/>" + "<br/> -------------------------------------------------------- Conclusões -------------------------------------------------------- <br/><br/>" + conclusions

            ''' '''
            # Dividir o relatório em partes menores
            telegram_report = split_report(telegram_report)
        #    detailed_report += final_report

#            print('telegram_report',telegram_report,'\n','detailed_report',detailed_report,'\n','dados_gemini',dados_gemini)
            print('dados_gemini',dados_gemini)

#            return [telegram_report, detailed_report]
            return [telegram_report, detailed_report, dados_gemini]



@dp.message_handler(lambda message: message.text in ["1 dia", "2 dias", "7 dias", "15 dias", "1 mês"])
async def handle_report_period(message: types.Message):
    period_map = {
        "1 dia": 1,
        "2 dias": 2,
        "7 dias": 7,
        "15 dias": 15,
        "1 mês": 30
    }
    period = period_map[message.text]
    await message.reply(f"Você escolheu o relatório de {message.text}. Aguarde enquanto buscamos os dados...", reply_markup=main_keyboard)

    # Buscar os dados do relatório
    user_id = message.from_user.id
    pool = await create_pool()
    telegram_report, detailed_report, dados_gemini = await fetch_report_data(pool, period, user_id)
    
    # Enviar o relatório resumido no Telegram
    for part in telegram_report:
        await message.reply(part)

    # Armazenar o relatório detalhado para o PDF
    user_reports[user_id] = (detailed_report, period, dados_gemini)

    # Enviar botão para gerar PDF
    generate_pdf_button = InlineKeyboardMarkup().add(InlineKeyboardButton("Gerar PDF", callback_data="generate_pdf"))
    await message.reply("Deseja gerar o PDF do relatório?", reply_markup=generate_pdf_button)


@dp.callback_query_handler(lambda c: c.data == "generate_pdf")
async def process_callback_generate_pdf(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id

    # Obter os dados do relatório armazenados temporariamente
    stored_data = user_reports.get(user_id)

    if not stored_data:
        await bot.answer_callback_query(callback_query.id, "Erro ao gerar o PDF. Tente novamente.")
        return

    detailed_report, period, dados_gemini = stored_data

    # Criar e enviar o PDF com o relatório
    pdf_file = await create_pdf(detailed_report, dados_gemini, period)
    await bot.send_document(chat_id=callback_query.message.chat.id, document=InputFile(pdf_file, filename="relatorio.pdf"))
        
    # try:
    #     # Criar e enviar o PDF com o relatório
    #     pdf_file = await create_pdf(detailed_report, dados_gemini, period)
    #     await bot.send_document(chat_id=callback_query.message.chat.id, document=InputFile(pdf_file, filename="relatorio.pdf"))
    # except Exception as e:
    #     await bot.answer_callback_query(callback_query.id, "Erro ao gerar o PDF. Tente novamente.")
    #     await bot.send_message(callback_query.message.chat.id, "Ocorreu um erro ao gerar o PDF. Por favor, tente gerar o relatório novamente.")



async def create_pdf(detailed_report, dados_gemini, period):
    temp_pdf = tempfile.mktemp(suffix=".pdf")
    doc = SimpleDocTemplate(temp_pdf, pagesize=A4, title="Relatório Detalhado")
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

    # Define a função para o cabeçalho
    def header(canvas, doc):
        canvas.saveState()
        logo.drawOn(canvas, 5, A4[1] - 55)

        # Configurações do cabeçalho
        canvas.setFont("Helvetica", 12)

        # Texto "Relatório Geral"
        header_text = "Relatório Geral"
        header_width = canvas.stringWidth(header_text, "Helvetica", 12)
        canvas.drawString((A4[0] - header_width) / 2, A4[1] - 50, header_text)

        # Texto "Últimos {period} dias" em vermelho
        canvas.setFillColor(colors.red)
        period_text = f"Últimos {period} dias"
        period_width = canvas.stringWidth(period_text, "Helvetica", 12)
        canvas.drawString((A4[0] - period_width) / 2, A4[1] - 70, period_text)

        canvas.restoreState()

    styles = getSampleStyleSheet()
    styleN = styles['Normal']
    styleH = styles['Heading1']

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
    elements.append(Paragraph(f"<b>Período: {start_date} à {end_date}</b>", styleH))
    elements.append(Spacer(1, 12)) 

    header_style = ParagraphStyle(
        'HeaderStyle',
        parent=styles['Normal'],
        fontSize=8,
        leading=10,
        alignment=1,
        textColor=colors.black
    )
    body_style = ParagraphStyle(
        'BodyStyle',
        parent=styles['Normal'],
        fontSize=8,
        leading=6,
        alignment=1,
        textColor=colors.black
    )

    data = [
        ['Usina', 'Equip.', 'Data', 'Duração Anormalidade', 'Alerta', 'Load Speed %', 'Valor Previsto %', 'Alarmes']
    ]

    rows = detailed_report.split("<tr>")
    for row in rows[1:]:
        columns = row.split("<td>")
        row_data = []
        for i, col in enumerate(columns[1:], start=1):
            value = col.split("</td>")[0].strip()
            if i == 3:
                try:
                    value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')
                except ValueError:
                    pass
            row_data.append(value)
        data.append(row_data)

    styled_data = [[Paragraph(cell, header_style) if i == 0 else Paragraph(cell, body_style) for i, cell in enumerate(row)] for row in data]

    column_widths = [110, 35, 80, 80, 50, 60, 60, 110]

    table = Table(styled_data, colWidths=column_widths)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))

    elements.append(table)
    elements.append(Spacer(1, 12))

    # Define styles with different colors
    styleN_red = ParagraphStyle('Normal', parent=styleN, textColor=red)
    styleN_green = ParagraphStyle('Normal', parent=styleN, textColor=green)
    styleN_orange = ParagraphStyle('Normal', parent=styleN, textColor=orange)

    usinas = {}

    for entry in dados_gemini.split("--------------------------------------"):
        try:
            nome_usina = entry.split("Usina: ")[1].split("\n")[0].strip()
            nome_equipamento = entry.split("Equipamento: ")[1].split(" - ")[0].strip()
            data_previsto = entry.split("Data previsto: ")[1].split("\n")[0].strip()
            data_previsto_saida = entry.split("Data previsto saída: ")[1].split("\n")[0].strip()
            data_quebra = entry.split("Data quebra: ")[1].split("\n")[0].strip()
            valores_reais = entry.split("Valores reais: [")[1].split("]")[0].strip().replace('\n', '').replace(' ', '').split(',')
            valores_previstos = entry.split("Valores previstos: [")[1].split("]")[0].strip().replace('\n', '').replace(' ', '').split(',')
            tempo_anormalidade = entry.split("Tempo anormalidade: ")[1].split("\n")[0].strip()
            tempo_total = entry.split("Tempo total: ")[1].split("\n")[0].strip()
            alerta_status = entry.split("Status: ")[1].split("\n")[0].strip()
            diferenca_formatada = entry.split("Porcentagem de diferença: ")[1].split("\n")[0].strip()
            alarmes_text = entry.split("Alarmes: ")[1].split("\n\n")[0].strip()
            
            pressao_oleo_data_1 = entry.split("Pressão do óleo média do mês anterior: ")[1].split("\n")[0].strip()
            pressao_oleo_data_2 = entry.split("Pressão do óleo média do período funcionando: ")[1].split("\n")[0].strip()
            pressao_oleo_data_3 = entry.split("Valor da Pressão do óleo mais alto do período funcionando: ")[1].split("\n")[0].strip()

            pressao_combustivel_data_1 = entry.split("Pressão do Combustivel média do mês anterior: ")[1].split("\n")[0].strip()
            pressao_combustivel_data_2 = entry.split("Pressão do Combustivel média do período funcionando: ")[1].split("\n")[0].strip()
            pressao_combustivel_data_3 = entry.split("Valor da Pressão do Combustivel mais alto do período funcionando: ")[1].split("\n")[0].strip()

            temperatura_agua_data_1 = entry.split("temperatura da água média do mês anterior: ")[1].split("\n")[0].strip()
            temperatura_agua_data_2 = entry.split("temperatura da água média do período funcionando: ")[1].split("\n")[0].strip()
            temperatura_agua_data_3 = entry.split("Valor da temperatura da água mais alto do período funcionando: ")[1].split("\n")[0].strip()

            tensao_l1_l2_data = entry.split("Tensão L1 L2 média: ")[1].split("\n")[0].strip()
            tensao_l2_l3_data = entry.split("Tensão L2 L3 média: ")[1].split("\n")[0].strip()
            tensao_l3_l1_data = entry.split("Tensão L3 L1 média: ")[1].split("\n")[0].strip()

            corrente_l1_data = entry.split("Corrente L1 média: ")[1].split("\n")[0].strip()
            corrente_l2_data = entry.split("Corrente L2 média: ")[1].split("\n")[0].strip()
            corrente_l3_data = entry.split("Corrente L3 média: ")[1].split("\n")[0].strip()

            valores_reais_float = [float(value) for value in valores_reais]
            valores_previstos_float = [float(value) for value in valores_previstos]

            if nome_usina not in usinas:
                usinas[nome_usina] = []

            usinas[nome_usina].append({
                'nome_equipamento': nome_equipamento,
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

    styles = getSampleStyleSheet()
    normal = styles['Normal']

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


    for nome_usina, equipamentos in usinas.items():
        elements.append(Paragraph(f"Usina: {nome_usina}", getSampleStyleSheet()["Heading2"]))
        elements.append(Spacer(1, 12))

        for equipamento in equipamentos:
            elements.append(Paragraph(f'<b>Equipamento:</b> {equipamento["nome_equipamento"]}', styleN))
            elements.append(Spacer(1, 12))  # Quebra de linha após o nome do equipamento
            
            plt.figure(figsize=(6, 3))
            plt.plot(equipamento['valores_reais'], color='blue', linestyle='-')
            plt.plot(equipamento['valores_previstos'], color='red', linestyle='--')
            plt.xlabel(' ')
            plt.ylabel('Valores')
            plt.title(f'{equipamento["nome_equipamento"]}')
            plt.grid(True)

            temp_img = tempfile.mktemp(suffix=".png")
            plt.savefig(temp_img, bbox_inches='tight')
            plt.close()

            graph = Image(temp_img, width=240, height=120)
            elements.append(graph)

            elements.append(Spacer(1, 12))  # Quebra de linha após o nome do equipamento
            elements.append(Paragraph(f'<b>Data Previsto:</b> {equipamento["data_previsto"]}', styleN))
            elements.append(Paragraph(f'<b>Data Previsto Saída:</b> {equipamento["data_previsto_saida"]}', styleN))
            elements.append(get_paragraph_style(f"Data Quebra: {equipamento['data_quebra']}", equipamento['data_quebra'] != 'Não houve falha'))
            elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos

        #    elements.append(Paragraph(f'<b>Valores Reais:</b> {equipamento["valores_reais"]}', styleN))
        #    elements.append(Paragraph(f'<b>Valores Previstos:</b> {equipamento["valores_previstos"]}', styleN))
            elements.append(Paragraph(f'<b>Tempo Anormalidade:</b> {equipamento["tempo_anormalidade"]}', styleN))
            elements.append(Paragraph(f'<b>Tempo Total:</b> {equipamento["tempo_total"]}', styleN))
            elements.append(Paragraph(f'<b>Status:</b> {equipamento["alerta_status"]}', styleN))
            elements.append(Paragraph(f'<b>Porcentagem de Diferença:</b> {equipamento["diferenca_formatada"]}', styleN))

            elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos




            # if equipamento['pressao_oleo_data_1'] != 'N/A' or equipamento['pressao_oleo_data_2'] != 'N/A' or equipamento['pressao_oleo_data_3'] != 'N/A':
            #     elements.append(Paragraph(f'<b>Pressão do Óleo</b>', styleN))
            # # Verifica se os dados de pressão do óleo são válidos antes de adicionar
            # if equipamento['pressao_oleo_data_1'] != 'N/A':
            #     elements.append(get_paragraph_style(
            #         f"Media do mês Anterior: {equipamento['pressao_oleo_data_1']}", 
            #         not (4.8 <= float(equipamento['pressao_oleo_data_1']) <= 5.0)
            #     ))
            # if equipamento['pressao_oleo_data_2'] != 'N/A':
            #     elements.append(get_paragraph_style(
            #         f"Média em funcionamento: {equipamento['pressao_oleo_data_2']}", 
            #         not (4.8 <= float(equipamento['pressao_oleo_data_2']) <= 5.0)
            #     ))
            # if equipamento['pressao_oleo_data_3'] != 'N/A':
            #     elements.append(get_paragraph_style(
            #         f"Pico de pressão durante funcionamento: {equipamento['pressao_oleo_data_3']}", 
            #         not (4.8 <= float(equipamento['pressao_oleo_data_3']) <= 5.0)
            #     ))

            # if equipamento['pressao_oleo_data_1'] != 'N/A' or equipamento['pressao_oleo_data_2'] != 'N/A' or equipamento['pressao_oleo_data_3'] != 'N/A':
            #     elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos




            # if equipamento['pressao_combustivel_data_1'] != 'N/A' or equipamento['pressao_combustivel_data_2'] != 'N/A' or equipamento['pressao_combustivel_data_3'] != 'N/A':
            #     elements.append(Paragraph(f'<b>Pressão de Combustível</b>', styleN))
            # # Verifica se os dados de pressão de combustível são válidos antes de adicionar
            # if equipamento['pressao_combustivel_data_1'] != 'N/A':
            #     elements.append(get_paragraph_style(
            #         f"Media do mês Anterior: {equipamento['pressao_combustivel_data_1']}", 
            #         not (4.5 <= float(equipamento['pressao_combustivel_data_1']) <= 5.0)
            #     ))
            # if equipamento['pressao_combustivel_data_2'] != 'N/A':
            #     elements.append(get_paragraph_style(
            #         f"Média em funcionamento: {equipamento['pressao_combustivel_data_2']}", 
            #         not (4.5 <= float(equipamento['pressao_combustivel_data_2']) <= 5.0)
            #     ))
            # if equipamento['pressao_combustivel_data_3'] != 'N/A':
            #     elements.append(get_paragraph_style(
            #         f"Pico de pressão durante funcionamento: {equipamento['pressao_combustivel_data_3']}", 
            #         not (4.5 <= float(equipamento['pressao_combustivel_data_3']) <= 5.0)
            #     ))

            # if equipamento['pressao_combustivel_data_1'] != 'N/A' or equipamento['pressao_combustivel_data_2'] != 'N/A' or equipamento['pressao_combustivel_data_3'] != 'N/A':
            #     elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos

            # if equipamento['temperatura_agua_data_1'] != 'N/A' or equipamento['temperatura_agua_data_2'] != 'N/A' or equipamento['temperatura_agua_data_3'] != 'N/A':
            #     elements.append(Paragraph(f'<b>Temperatura da Água</b>', styleN))
            # # Verifica se os dados de temperatura da água são válidos antes de adicionar
            # if equipamento['temperatura_agua_data_1'] != 'N/A':
            #     try:
            #         temp_agua_data_1 = float(equipamento['temperatura_agua_data_1'])
            #         if temp_agua_data_1 < float(equipamento['temperatura_agua_data_1']):
            #             elements.append(get_paragraph_style(
            #                 f"Media do mês Anterior: {equipamento['temperatura_agua_data_1']} ºC - Temperatura abaixo do mês anterior", 
            #                 False,
            #                 orange_condition=False
            #             ))
            #         elif temp_agua_data_1 > float(equipamento['temperatura_agua_data_1']):
            #             elements.append(get_paragraph_style(
            #                 f"Media do mês Anterior: {equipamento['temperatura_agua_data_1']} ºC - Temperatura maior que o mês anterior", 
            #                 False,
            #                 orange_condition=True
            #             ))
            #         else:
            #             elements.append(get_paragraph_style(
            #                 f"Media do mês Anterior: {equipamento['temperatura_agua_data_1']} ºC", 
            #                 False,
            #                 orange_condition=False
            #             ))
            #     except ValueError:
            #         elements.append(Paragraph(f"Media do mês Anterior: {equipamento['temperatura_agua_data_1']} ºC", styleN))
            
            # if equipamento['temperatura_agua_data_2'] != 'N/A':
            #     try:
            #         temp_agua_data_2 = float(equipamento['temperatura_agua_data_2'])
            #         temp_agua_data_1 = float(equipamento['temperatura_agua_data_1'])
            #         if temp_agua_data_2 < temp_agua_data_1:
            #             elements.append(get_paragraph_style(
            #                 f"Média em funcionamento: {equipamento['temperatura_agua_data_2']} ºC - Temperatura abaixo do mês anterior", 
            #                 False,
            #                 orange_condition=False
            #             ))
            #         elif temp_agua_data_2 > temp_agua_data_1:
            #             elements.append(get_paragraph_style(
            #                 f"Média em funcionamento: {equipamento['temperatura_agua_data_2']} ºC - Temperatura maior que o mês anterior", 
            #                 False,
            #                 orange_condition=True
            #             ))
            #         else:
            #             elements.append(get_paragraph_style(
            #                 f"Média em funcionamento: {equipamento['temperatura_agua_data_2']} ºC", 
            #                 False,
            #                 orange_condition=False
            #             ))
            #     except ValueError:
            #         elements.append(Paragraph(f"Média em funcionamento: {equipamento['temperatura_agua_data_2']} ºC", styleN))
            
            # if equipamento['temperatura_agua_data_3'] != 'N/A':
            #     try:
            #         temp_agua_data_3 = float(equipamento['temperatura_agua_data_3'])
            #         if temp_agua_data_3 < 90 and temp_agua_data_3 < float(equipamento['temperatura_agua_data_3']):
            #             elements.append(get_paragraph_style(
            #                 f"Pico de temperatura durante funcionamento: {equipamento['temperatura_agua_data_3']} ºC - Pico muito alto, próximo de 90ºC", 
            #                 False,
            #                 orange_condition=False
            #             ))
            #         elif temp_agua_data_3 > 90 and temp_agua_data_3 < float(equipamento['temperatura_agua_data_3']):
            #             elements.append(get_paragraph_style(
            #                 f"Pico de temperatura durante funcionamento: {equipamento['temperatura_agua_data_3']} ºC - Temperatura maior que o pico máximo permitido (90ºC)", 
            #                 False,
            #                 orange_condition=True
            #             ))
            #         elif temp_agua_data_3 > float(equipamento['temperatura_agua_data_3']):
            #             elements.append(get_paragraph_style(
            #                 f"Pico de temperatura durante funcionamento: {equipamento['temperatura_agua_data_3']} ºC - Temperatura maior que o mês anterior", 
            #                 False,
            #                 orange_condition=True
            #             ))
            #         else:
            #             elements.append(get_paragraph_style(
            #                 f"Pico de temperatura durante funcionamento: {equipamento['temperatura_agua_data_3']} ºC", 
            #                 False,
            #                 orange_condition=False
            #             ))
            #     except ValueError:
            #         elements.append(Paragraph(f"Pico de temperatura durante funcionamento: {equipamento['temperatura_agua_data_3']} ºC", styleN))


            # if equipamento['temperatura_agua_data_1'] != 'N/A' or equipamento['temperatura_agua_data_2'] != 'N/A' or equipamento['temperatura_agua_data_3'] != 'N/A':
            #     elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos

            # Verifica se os dados de pressão do óleo são válidos antes de adicionar
            if equipamento['pressao_oleo_data_1'] != 'N/A' or equipamento['pressao_oleo_data_2'] != 'N/A' or equipamento['pressao_oleo_data_3'] != 'N/A':
                elements.append(Paragraph(f'<b>Pressão do Óleo</b>', styleN))
                
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
                elements.append(Paragraph(texto_detalhado_pressao, styleN))
                elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos

            # Verifica se os dados de pressão de combustível são válidos antes de adicionar
            if equipamento['pressao_combustivel_data_1'] != 'N/A' or equipamento['pressao_combustivel_data_2'] != 'N/A' or equipamento['pressao_combustivel_data_3'] != 'N/A':
                elements.append(Paragraph(f'<b>Pressão do Combustível</b>', styleN))
                
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
                elements.append(Paragraph(texto_detalhado_combustivel, styleN))
                elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos


            # Verifica se os dados de temperatura da água são válidos antes de adicionar
            if equipamento['temperatura_agua_data_1'] != 'N/A' or equipamento['temperatura_agua_data_2'] != 'N/A' or equipamento['temperatura_agua_data_3'] != 'N/A':
                elements.append(Paragraph(f'<b>Temperatura da Água</b>', styleN))

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
                elements.append(Paragraph(texto_detalhado_temperatura, styleN))
                
                elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos

            # Verifica se os dados de tensão entre as fases são válidos antes de adicionar
            if equipamento['tensao_l1_l2_data'] != 'N/A' or equipamento['tensao_l2_l3_data'] != 'N/A' or equipamento['tensao_l3_l1_data'] != 'N/A':
                elements.append(Paragraph(f'<b>Tensão entre as Fases</b>', styleN))
                
                tensao_l1_l2_data = float(equipamento['tensao_l1_l2_data']) if equipamento['tensao_l1_l2_data'] != 'N/A' else None
                tensao_l2_l3_data = float(equipamento['tensao_l2_l3_data']) if equipamento['tensao_l2_l3_data'] != 'N/A' else None
                tensao_l3_l1_data = float(equipamento['tensao_l3_l1_data']) if equipamento['tensao_l3_l1_data'] != 'N/A' else None

                tensao_L1_L2_data_color = get_color(tensao_l1_l2_data, 280, 300) if tensao_l1_l2_data is not None else "black"
                tensao_L2_L3_data_color = get_color(tensao_l2_l3_data, 310, 330) if tensao_l2_l3_data is not None else "black"
                tensao_L3_L1_data_color = get_color(tensao_l3_l1_data, 280, 300) if tensao_l3_l1_data is not None else "black"

                texto_detalhado_tensao = create_detailed_text_tensao(
                    equipamento['tensao_l1_l2_data'] if equipamento['tensao_l1_l2_data'] != 'N/A' else None,
                    equipamento['tensao_l2_l3_data'] if equipamento['tensao_l2_l3_data'] != 'N/A' else None,
                    equipamento['tensao_l3_l1_data'] if equipamento['tensao_l3_l1_data'] != 'N/A' else None,
                    tensao_L1_L2_data_color, tensao_L2_L3_data_color, tensao_L3_L1_data_color, "L"
                )
                elements.append(Paragraph(texto_detalhado_tensao, styleN))
                elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos
    

            elements.append(Paragraph(f'<b>Alarmes:</b> {equipamento["alarmes_text"]}', styleN))

            elements.append(Spacer(1, 12))  # Adiciona um espaçamento entre equipamentos
            
    doc.build(elements, onFirstPage=header, onLaterPages=header)

    return temp_pdf






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



async def verificar_e_obter_coeficiente(cod_equipamento, pool):
    try:
        coeficiente_existente = 0.0
        intercepto_existente = 0.0
        pool = await create_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(f"SELECT * FROM machine_learning.coeficiente_geradores WHERE cod_equipamento = {cod_equipamento}")
                resultado = await cursor.fetchone()
                if resultado is not None:
                    coeficiente_existente = resultado[2]
                    intercepto_existente = resultado[3]
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
            coeficiente_existente, intercepto_existente = await verificar_e_obter_coeficiente(cod_equipamento_resultado, pool)

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


async def selecionar_GMG():
    try:
        pool = await create_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT codigo, ativo FROM sup_geral.tipos_equipamentos WHERE classe = 'GMG'")
                resultados = await cursor.fetchall()
                codigos = [resultado[0] for resultado in resultados]
        return codigos
    except Exception as e:
        print(f"An error occurred in selecionar_GMG: {e}")
        return []

@dp.message_handler(commands=['teste'])
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



if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
    



'''



''' sem csv'''

'''
import logging
import csv
import io
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InputFile
from aiogram.utils import executor
import aiomysql
import google.generativeai as genai
from datetime import datetime
import magic
from aiogram.types import Message
from PIL import Image
import tempfile

TOKEN = "6674960909:AAEf4Ky64lM6NILX3wZ6HJEASTt42Q2vopc"
#API_KEY = 'AIzaSyDfcjDbVZ2BbPJEuSpJ4wF_ATguRuffikA'

API_KEY = 'AIzaSyDf9hqXZvxOiCKaFSiIa0byrfEctP5mflI'

genai.configure(api_key=API_KEY)

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())

async def create_pool():
    pool = await aiomysql.create_pool(
        host="192.168.4.50",
        user="bruno",
        password="superbancoml",
        db="machine_learning"
    )
    return pool


# Teclado principal
main_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
main_keyboard.add(KeyboardButton("Relatório"))

# Teclado de opções de relatórios
report_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
report_keyboard.add(
    KeyboardButton("1 dia"),
    KeyboardButton("2 dias"),
    KeyboardButton("7 dias"),
    KeyboardButton("15 dias"),
    KeyboardButton("1 mês")
)

@dp.message_handler(commands=['relatorio'])
async def send_welcome(message: types.Message):
    await message.reply("Bem-vindo! Escolha uma opção:", reply_markup=main_keyboard)

@dp.message_handler(lambda message: message.text == "Relatório")
async def show_report_options(message: types.Message):
    await message.reply("Escolha o período do relatório:", reply_markup=report_keyboard)

async def fetch_report_data(pool, period, user_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT cod_usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
            result = await cursor.fetchone()
            if not result:
                return ["Usuário não encontrado."]
            cod_usuario = result[0]

            await cursor.execute("SELECT cod_usina FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
            cod_usinas = await cursor.fetchall()

            if not cod_usinas:
                return ["Nenhuma usina associada ao usuário."]

            usinas = [usina[0] for usina in cod_usinas]

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
            """ % (', '.join(['%s'] * len(usinas)), '%s', '%s')
            await cursor.execute(query, usinas + [period, period])
            log_data = await cursor.fetchall()

            if not log_data:
                return ["Nenhum dado encontrado para o período selecionado."]

            # Buscar número de geradores por usina
            await cursor.execute("""
                SELECT cod_usina, COUNT(*) AS num_geradores
                FROM sup_geral.equipamentos
                WHERE ativo = 1 
                  AND cod_tipo_equipamento IN (1, 3, 4, 12, 16, 18, 20, 22, 23, 27, 29, 33, 37, 40, 41, 43, 51, 55, 56)
                GROUP BY cod_usina
            """)
            geradores_por_usina = await cursor.fetchall()
            geradores_por_usina_dict = {usina[0]: usina[1] for usina in geradores_por_usina}

            # Buscar equipamentos em alerta na tabela leituras_consecutivas
            await cursor.execute("""
                SELECT cod_equipamento 
                FROM machine_learning.leituras_consecutivas 
                WHERE alerta = 1 AND cod_campo = 114
            """)
            equipamentos_alerta = await cursor.fetchall()
            equipamentos_alerta_set = set(equipamento[0] for equipamento in equipamentos_alerta)

            detailed_report = ""
            problem_descriptions = []
            usina_equipamento_map = {}
            alerta_status = []
            valores_reais = []
            valores_previstos = []
            alarmes_text = []

            for row in log_data:
                cod_equipamento, cod_usina, data_previsto, data_previsto_saida, data_quebra, nome_usina, nome_equipamento = row

                # Verificar se o equipamento está em alerta atualmente
                if cod_equipamento in equipamentos_alerta_set and not data_quebra:
                    data_quebra = 'Em funcionamento'
                elif not data_quebra:
                    data_quebra = 'Não houve falha'
                    
                data_previsto_saida = data_previsto_saida if data_previsto_saida else 'Indefinido'

                if data_quebra not in ['Não houve falha', 'Em funcionamento'] and data_previsto_saida != 'Indefinido':
                    tempo_anormalidade = datetime.strptime(str(data_previsto_saida), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                    tempo_total = datetime.strptime(str(data_quebra), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                else:
                    tempo_anormalidade = 'Não disponível'
                    tempo_total = 'Não disponível'

                # Consultar a tabela valores_previsao para obter mais dados
                await cursor.execute("""
                    SELECT DISTINCT alerta_80, alerta_100, previsao, MAX(valores_reais), MAX(valores_previstos), GROUP_CONCAT(DISTINCT alarmes) 
                    FROM machine_learning.valores_previsao 
                    WHERE cod_equipamento = %s 
                    AND ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300
                    GROUP BY cod_equipamento
                """, (cod_equipamento, data_previsto))
                previsao_data = await cursor.fetchone()
                
                if previsao_data:
                    alerta_80, alerta_100, previsao, valores_reais, valores_previstos, alarmes = previsao_data
                    alerta_status = 'Alerta 80' if alerta_80 else 'Alerta 100' if alerta_100 else 'Previsão' if previsao else 'Nenhum alerta'

                    # Obter descrições dos alarmes
                    if alarmes:
                        # Processar a string de alarmes para extrair os códigos
                        alarm_codes = []
                        for code in alarmes.split(','):
                            try:
                                alarm_codes.append((int(code.strip().strip('()')),))
                            except ValueError:
                                continue  # Ignorar códigos inválidos

                        alarm_descriptions = await fetch_alarm_descriptions(pool, alarm_codes)
                        alarmes_text = ', '.join(alarm_descriptions.get(code[0], "Descrição não encontrada") for code in alarm_codes)
                    else:
                        alarmes_text = 'Sem alarmes'

                    problem_description = (
                        f"    Tempo anormalidade: {tempo_anormalidade}\n"
                        f"    Tempo total: {tempo_total}\n"
                        f"    Status: {alerta_status}\n"
                        f"    Valores reais: {valores_reais}\n"
                        f"    Valores previstos: {valores_previstos}\n\n"
                        f"    Alarmes: {alarmes_text}\n"
                    )
                else:
                    problem_description = (
                        "    Nenhum dado de previsão disponível.\n"
                    )


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
                else:
                    funcionando_text = "    Equipamentos funcionando na hora: 0\n"

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

                    funcionando_text += f"    - Equipamento {equip} ({nome_do_equipamento}):\n      Valores reais: {', '.join(map(str, valores_reais))}\n      Valores previstos: {', '.join(map(str, valores_previstos))}\n"


                if (nome_usina, nome_equipamento) not in usina_equipamento_map:
                    usina_equipamento_map[(nome_usina, nome_equipamento)] = []
                usina_equipamento_map[(nome_usina, nome_equipamento)].append(problem_description)

                num_geradores = geradores_por_usina_dict.get(cod_usina, "Desconhecido")

                if nome_usina not in detailed_report:
                    detailed_report += f"Usina: {nome_usina} - ({cod_usina})\n\n    Quantidade de geradores: {num_geradores}\n{funcionando_text}\n\n"

                detailed_report += (
                    f"  Equipamento: {nome_equipamento} - ({cod_equipamento})\n"
                    f"    Data previsto: {data_previsto}\n"
                    f"    Data previsto saída: {data_previsto_saida}\n"
                    f"    Data quebra: {data_quebra}\n\n"
                    f"    Descrição do problema:\n"
                    f"{problem_description}\n"
                    f"    --------------------------------------\n"
                )


            # Ajuste do relatório geral
            general_report = (
                f"\nPeríodo: Últimos {log_data} dias.\n"
            )
            
            # Usar a API Gemini para formatar a descrição geral do relatório e gerar conclusões

            try:
                model = genai.GenerativeModel('gemini-1.5-pro-latest')
                chat = model.start_chat(history=[])
                context = (
                    "Aqui estão as definições dos parâmetros usados no relatório:\n"
                    f'- Use esses dados como parametro, {detailed_report}\n'
                    "- Alerta 100: Valores reais estão em 100% do load speed, o que não é bom.\n"
                    "- Alerta 80: Valores reais estão em 80% ou mais do load speed, o que também não é bom para o gerador.\n"
                    "- Previsão: Valores reais do load speed estão fora do padrão dos valores previstos para este equipamento.\n"
                    "- Data previsto: Hora que o gerador teve valores de load speed fora do padrão.\n"
                    "- Data previsto saída: Hora que os valores fora do padrão se normalizaram.\n"
                    "- Data quebra: Hora que o equipamento ficou indisponível, ou seja, parou e zerou seus valores do load speed. Não significa que ficou parado por esse tempo.\n"
                    "- alarmes: Se não houver alarmes, coloque 'sem alarmes'.\n"
                    "- Valores reais: e as leituras reais da porcentagm do load speed do gerador.\n"
                    "- Valores previstos: e as leituras previstas da porcentagm do load speed do gerador.\n"
                    "- Quantidade de geradores: quantidade total de geradores na usina, ligados ou nao. \n"
                    "- Equipamentos funcionando na hora: geradores ativos da mesma usina no mesmo momento que algum gerador estava com anomalia.\n"
                    "Use essas definições para entender o contexto, mas não inclua essas explicações no relatório.\n"
                    "Não invente dado, use apenas os dados do banco de dados retornados, não minta.\n"
                )

                prompt_general = (
                    " use somente o inicio e fim, pegando a data de inicio e fim das leituras, nao coloque a hora, pegue somente a data no modelo dia/mes/ano e escreva em portugues"
                    f"{general_report}\n"
                )
                response_general = chat.send_message(prompt_general)
                formatted_general_report = response_general.text

                # Geração da conclusão usando a API Gemini
                conclusions_prompt = (
                    context + "\n\n"
                    "Baseando-se no seguinte relatório de ocorrências, gere uma conclusão clara e detalhada que explique os alarmes de maneira que seja fácil de entender para uma pessoa que não tem conhecimentos técnicos, falando qual o nome do equipamento e de qual usina.\n"
                    "Descreva a quantidade de equipamentos que se tem na usina.\n"
                    "Faça sugestao para utilizar os outros equipamentos da usina se ver a necessidade de acordo com os equipamentos funcionando na hora da anormalizade de um equipamento.\n"
                    "Compare as datas para o mesmo equipamento, veja se teve relacao uma parada com outra, veja se as paradas estao no mesmo periodo de tempo.\n"
                    "De sempre um pulo de uma linha para cada usina que for descrever\n"
                    "A seguir, uma análise detalhada dos alarmes encontrados e suas implicações:\n"
                    "De sempre um pulo de uma linha para cada análise detalhada dos alarmes encontrados e suas implicações que for descrever\n"
                )
                # Adicionar análise de alarmes ao relatório
                for key, descriptions in usina_equipamento_map.items():
                    nome_usina, nome_equipamento = key
                    for problem_description in descriptions:
                        conclusions_prompt += f"{problem_description}\n"

                # Finalizar com explicações específicas sobre os alarmes
                conclusions_prompt += (
                    "Recomendações:\n"
                    "- Para o alarme X, isso significa Y. É importante verificar Z para mitigar o risco.\n"
                    "- De sempre um pulo de uma linha para cada análise detalhada dos alarmes encontrados e suas implicações que for descrever\n"
                    "- O alarme W indica um potencial problema com a componente V; é aconselhável inspeção no ponto A e manutenção na peça B do gerador.\n"
                )

                response_conclusions = chat.send_message(conclusions_prompt)
                conclusions = response_conclusions.text

                # Juntar a descrição geral formatada, o relatório detalhado e as conclusões
                final_report = formatted_general_report + "\n\n" + detailed_report + "\n-------------Conclusões---------------\n\n" + conclusions

                # Dividir o relatório em partes menores
                report_parts = split_report(final_report)

                return report_parts

            except Exception as e:
                logging.error(f"Erro ao gerar texto com a API Gemini: {e}")
                return ["Ocorreu um erro ao gerar o relatório. Tente novamente mais tarde."]





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



# Inicializando os modelos Gemini-Pro e Gemini-Pro Vision
gemini_model = genai.GenerativeModel('gemini-pro')
gemini_vision_model = genai.GenerativeModel('gemini-pro-vision')

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
            image = Image.open(photo_data.name)
            
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



@dp.message_handler(lambda message: message.text in ["1 dia", "2 dias", "7 dias", "15 dias", "1 mês"])
async def handle_report_period(message: types.Message):
    period_map = {
        "1 dia": 1,
        "2 dias": 2,
        "7 dias": 7,
        "15 dias": 15,
        "1 mês": 30
    }
    period = period_map[message.text]
    await message.reply(f"Você escolheu o relatório de {message.text}. Aguarde enquanto buscamos os dados...", reply_markup=main_keyboard)

    # Buscar os dados do relatório
    user_id = message.from_user.id
    pool = await create_pool()
    report_parts = await fetch_report_data(pool, period, user_id)

    # Enviar o relatório em partes
    for part in report_parts:
        await message.reply(part)

    pool.close()
    await pool.wait_closed()

if __name__ == '__main__':

    executor.start_polling(dp, skip_updates=True)

'''

''' gerar csv'''

'''
import logging
import csv
import io
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InputFile
from aiogram.utils import executor
import aiomysql
import google.generativeai as genai
from datetime import datetime
import magic
from aiogram.types import Message
from PIL import Image
import tempfile

TOKEN = "6674960909:AAEf4Ky64lM6NILX3wZ6HJEASTt42Q2vopc"
#API_KEY = 'AIzaSyDfcjDbVZ2BbPJEuSpJ4wF_ATguRuffikA'

API_KEY = 'AIzaSyDf9hqXZvxOiCKaFSiIa0byrfEctP5mflI'

genai.configure(api_key=API_KEY)

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())

async def create_pool():
    pool = await aiomysql.create_pool(
        host="192.168.4.50",
        user="bruno",
        password="superbancoml",
        db="machine_learning"
    )
    return pool


# Teclado principal
main_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
main_keyboard.add(KeyboardButton("Relatório"))

# Teclado de opções de relatórios
report_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
report_keyboard.add(
    KeyboardButton("1 dia"),
    KeyboardButton("2 dias"),
    KeyboardButton("7 dias"),
    KeyboardButton("15 dias"),
    KeyboardButton("1 mês")
)

@dp.message_handler(commands=['relatorio'])
async def send_welcome(message: types.Message):
    await message.reply("Bem-vindo! Escolha uma opção:", reply_markup=main_keyboard)

@dp.message_handler(lambda message: message.text == "Relatório")
async def show_report_options(message: types.Message):
    await message.reply("Escolha o período do relatório:", reply_markup=report_keyboard)

async def fetch_report_data(pool, period, user_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT cod_usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
            result = await cursor.fetchone()
            if not result:
                return ["Usuário não encontrado."]
            cod_usuario = result[0]

            await cursor.execute("SELECT cod_usina FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
            cod_usinas = await cursor.fetchall()

            if not cod_usinas:
                return ["Nenhuma usina associada ao usuário."]

            usinas = [usina[0] for usina in cod_usinas]

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
            """ % (', '.join(['%s'] * len(usinas)), '%s', '%s')
            await cursor.execute(query, usinas + [period, period])
            log_data = await cursor.fetchall()

            if not log_data:
                return ["Nenhum dado encontrado para o período selecionado."]

            # Buscar número de geradores por usina
            await cursor.execute("""
                SELECT cod_usina, COUNT(*) AS num_geradores
                FROM sup_geral.equipamentos
                WHERE ativo = 1 
                  AND cod_tipo_equipamento IN (1, 3, 4, 12, 16, 18, 20, 22, 23, 27, 29, 33, 37, 40, 41, 43, 51, 55, 56)
                GROUP BY cod_usina
            """)
            geradores_por_usina = await cursor.fetchall()
            geradores_por_usina_dict = {usina[0]: usina[1] for usina in geradores_por_usina}

            # Buscar equipamentos em alerta na tabela leituras_consecutivas
            await cursor.execute("""
                SELECT cod_equipamento 
                FROM machine_learning.leituras_consecutivas 
                WHERE alerta = 1 AND cod_campo = 114
            """)
            equipamentos_alerta = await cursor.fetchall()
            equipamentos_alerta_set = set(equipamento[0] for equipamento in equipamentos_alerta)

            detailed_report = ""
            problem_descriptions = []
            usina_equipamento_map = {}
            csv_data = []
            alerta_status = []
            valores_reais = []
            valores_previstos = []
            alarmes_text = []

            for row in log_data:
                cod_equipamento, cod_usina, data_previsto, data_previsto_saida, data_quebra, nome_usina, nome_equipamento = row

                # Verificar se o equipamento está em alerta atualmente
                if cod_equipamento in equipamentos_alerta_set and not data_quebra:
                    data_quebra = 'Em funcionamento'
                elif not data_quebra:
                    data_quebra = 'Não houve falha'
                    
                data_previsto_saida = data_previsto_saida if data_previsto_saida else 'Indefinido'

                if data_quebra not in ['Não houve falha', 'Em funcionamento'] and data_previsto_saida != 'Indefinido':
                    tempo_anormalidade = datetime.strptime(str(data_previsto_saida), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                    tempo_total = datetime.strptime(str(data_quebra), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                else:
                    tempo_anormalidade = 'Não disponível'
                    tempo_total = 'Não disponível'

                # Consultar a tabela valores_previsao para obter mais dados
                await cursor.execute("""
                    SELECT DISTINCT alerta_80, alerta_100, previsao, MAX(valores_reais), MAX(valores_previstos), GROUP_CONCAT(DISTINCT alarmes) 
                    FROM machine_learning.valores_previsao 
                    WHERE cod_equipamento = %s 
                    AND ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300
                    GROUP BY cod_equipamento
                """, (cod_equipamento, data_previsto))
                previsao_data = await cursor.fetchone()
                
                if previsao_data:
                    alerta_80, alerta_100, previsao, valores_reais, valores_previstos, alarmes = previsao_data
                    alerta_status = 'Alerta 80' if alerta_80 else 'Alerta 100' if alerta_100 else 'Previsão' if previsao else 'Nenhum alerta'

                    # Obter descrições dos alarmes
                    if alarmes:
                        # Processar a string de alarmes para extrair os códigos
                        alarm_codes = []
                        for code in alarmes.split(','):
                            try:
                                alarm_codes.append((int(code.strip().strip('()')),))
                            except ValueError:
                                continue  # Ignorar códigos inválidos

                        alarm_descriptions = await fetch_alarm_descriptions(pool, alarm_codes)
                        alarmes_text = ', '.join(alarm_descriptions.get(code[0], "Descrição não encontrada") for code in alarm_codes)
                    else:
                        alarmes_text = 'Sem alarmes'

                    problem_description = (
                        f"    Tempo anormalidade: {tempo_anormalidade}\n"
                        f"    Tempo total: {tempo_total}\n"
                        f"    Status: {alerta_status}\n"
                        f"    Valores reais: {valores_reais}\n"
                        f"    Valores previstos: {valores_previstos}\n\n"
                        f"    Alarmes: {alarmes_text}\n"
                    )
                else:
                    problem_description = (
                        "    Nenhum dado de previsão disponível.\n"
                    )


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
                else:
                    funcionando_text = "    Equipamentos funcionando na hora: 0\n"

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

                    funcionando_text += f"    - Equipamento {equip} ({nome_do_equipamento}):\n      Valores reais: {', '.join(map(str, valores_reais))}\n      Valores previstos: {', '.join(map(str, valores_previstos))}\n"


                if (nome_usina, nome_equipamento) not in usina_equipamento_map:
                    usina_equipamento_map[(nome_usina, nome_equipamento)] = []
                usina_equipamento_map[(nome_usina, nome_equipamento)].append(problem_description)

                num_geradores = geradores_por_usina_dict.get(cod_usina, "Desconhecido")

                if nome_usina not in detailed_report:
                    detailed_report += f"Usina: {nome_usina} - ({cod_usina})\n\n    Quantidade de geradores: {num_geradores}\n{funcionando_text}\n\n"

                detailed_report += (
                    f"  Equipamento: {nome_equipamento} - ({cod_equipamento})\n"
                    f"    Data previsto: {data_previsto}\n"
                    f"    Data previsto saída: {data_previsto_saida}\n"
                    f"    Data quebra: {data_quebra}\n\n"
                    f"    Descrição do problema:\n"
                    f"{problem_description}\n"
                    f"    --------------------------------------\n"
                )

                # Adicionar dados ao CSV
                csv_data.append([
                    cod_usina, nome_usina, cod_equipamento, nome_equipamento, num_geradores,
                    data_previsto, data_previsto_saida, data_quebra,
                    tempo_anormalidade, tempo_total, alerta_status,
                    valores_reais, valores_previstos, alarmes_text
                ])

            # Ajuste do relatório geral
            general_report = (
                f"\nPeríodo: Últimos {log_data} dias.\n"
            )
            
            # Usar a API Gemini para formatar a descrição geral do relatório e gerar conclusões

            try:
                model = genai.GenerativeModel('gemini-1.5-pro-latest')
                chat = model.start_chat(history=[])
                context = (
                    "Aqui estão as definições dos parâmetros usados no relatório:\n"
                    f'- Use esses dados como parametro, {detailed_report}\n'
                    "- Alerta 100: Valores reais estão em 100% do load speed, o que não é bom.\n"
                    "- Alerta 80: Valores reais estão em 80% ou mais do load speed, o que também não é bom para o gerador.\n"
                    "- Previsão: Valores reais do load speed estão fora do padrão dos valores previstos para este equipamento.\n"
                    "- Data previsto: Hora que o gerador teve valores de load speed fora do padrão.\n"
                    "- Data previsto saída: Hora que os valores fora do padrão se normalizaram.\n"
                    "- Data quebra: Hora que o equipamento ficou indisponível, ou seja, parou e zerou seus valores do load speed. Não significa que ficou parado por esse tempo.\n"
                    "- alarmes: Se não houver alarmes, coloque 'sem alarmes'.\n"
                    "- Valores reais: e as leituras reais da porcentagm do load speed do gerador.\n"
                    "- Valores previstos: e as leituras previstas da porcentagm do load speed do gerador.\n"
                    "- Quantidade de geradores: quantidade total de geradores na usina, ligados ou nao. \n"
                    "- Equipamentos funcionando na hora: geradores ativos da mesma usina no mesmo momento que algum gerador estava com anomalia.\n"
                    "Use essas definições para entender o contexto, mas não inclua essas explicações no relatório.\n"
                    "Não invente dado, use apenas os dados do banco de dados retornados, não minta.\n"
                )

                prompt_general = (
                    " use somente o inicio e fim, pegando a data de inicio e fim das leituras, nao coloque a hora, pegue somente a data no modelo dia/mes/ano e escreva em portugues"
                    f"{general_report}\n"
                )
                response_general = chat.send_message(prompt_general)
                formatted_general_report = response_general.text

                # Geração da conclusão usando a API Gemini
                conclusions_prompt = (
                    context + "\n\n"
                    "Baseando-se no seguinte relatório de ocorrências, gere uma conclusão clara e detalhada que explique os alarmes de maneira que seja fácil de entender para uma pessoa que não tem conhecimentos técnicos, falando qual o nome do equipamento e de qual usina.\n"
                    "Descreva a quantidade de equipamentos que se tem na usina.\n"
                    "Faça sugestao para utilizar os outros equipamentos da usina se ver a necessidade de acordo com os equipamentos funcionando na hora da anormalizade de um equipamento.\n"
                    "Compare as datas para o mesmo equipamento, veja se teve relacao uma parada com outra, veja se as paradas estao no mesmo periodo de tempo.\n"
                    "De sempre um pulo de uma linha para cada usina que for descrever\n"
                    "A seguir, uma análise detalhada dos alarmes encontrados e suas implicações:\n"
                    "De sempre um pulo de uma linha para cada análise detalhada dos alarmes encontrados e suas implicações que for descrever\n"
                )
                # Adicionar análise de alarmes ao relatório
                for key, descriptions in usina_equipamento_map.items():
                    nome_usina, nome_equipamento = key
                    for problem_description in descriptions:
                        conclusions_prompt += f"{problem_description}\n"

                # Finalizar com explicações específicas sobre os alarmes
                conclusions_prompt += (
                    "Recomendações:\n"
                    "- Para o alarme X, isso significa Y. É importante verificar Z para mitigar o risco.\n"
                    "- De sempre um pulo de uma linha para cada análise detalhada dos alarmes encontrados e suas implicações que for descrever\n"
                    "- O alarme W indica um potencial problema com a componente V; é aconselhável inspeção no ponto A e manutenção na peça B do gerador.\n"
                )

                response_conclusions = chat.send_message(conclusions_prompt)
                conclusions = response_conclusions.text

                # Juntar a descrição geral formatada, o relatório detalhado e as conclusões
                final_report = formatted_general_report + "\n\n" + detailed_report + "\n-------------Conclusões---------------\n\n" + conclusions

                # Dividir o relatório em partes menores
                report_parts = split_report(final_report)

                # Criar o arquivo CSV
                csv_buffer = io.StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow([
                    "Cod Usina", "Nome Usina", "Cod Equipamento", "Nome Equipamento",
                    "Quantidade de geradores","Data Previsto", "Data Previsto Saída", "Data Quebra",
                    "Tempo Anormalidade", "Tempo Total", "Status",
                    "Valores Reais", "Valores Previstos", "Alarmes"
                ])
                csv_writer.writerows(csv_data)
                csv_buffer.seek(0)

                return report_parts, csv_buffer.getvalue()

            except Exception as e:
                logging.error(f"Erro ao gerar texto com a API Gemini: {e}")
                return ["Ocorreu um erro ao gerar o relatório. Tente novamente mais tarde."]





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



# Inicializando os modelos Gemini-Pro e Gemini-Pro Vision
gemini_model = genai.GenerativeModel('gemini-pro')
gemini_vision_model = genai.GenerativeModel('gemini-pro-vision')

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
            image = Image.open(photo_data.name)
            
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



@dp.message_handler(lambda message: message.text in ["1 dia", "2 dias", "7 dias", "15 dias", "1 mês"])
async def handle_report_period(message: types.Message):
    period_map = {
        "1 dia": 1,
        "2 dias": 2,
        "7 dias": 7,
        "15 dias": 15,
        "1 mês": 30
    }
    period = period_map[message.text]
    await message.reply(f"Você escolheu o relatório de {message.text}. Aguarde enquanto buscamos os dados...", reply_markup=main_keyboard)

    # Buscar os dados do relatório
    user_id = message.from_user.id
    pool = await create_pool()
    report_parts, csv_data = await fetch_report_data(pool, period, user_id)

    # Enviar o relatório em partes
    for part in report_parts:
        await message.reply(part)

    # Criar e enviar o arquivo CSV
    csv_file = io.BytesIO(csv_data.encode('utf-8'))
    csv_file.name = "relatorio.csv"
    await message.reply_document(InputFile(csv_file))

    pool.close()
    await pool.wait_closed()

if __name__ == '__main__':

    executor.start_polling(dp, skip_updates=True)
'''
    
    
''' gera pdf'''
'''

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

from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet
from io import BytesIO

TOKEN = "6674960909:AAEf4Ky64lM6NILX3wZ6HJEASTt42Q2vopc"
#API_KEY = 'AIzaSyDfcjDbVZ2BbPJEuSpJ4wF_ATguRuffikA'

API_KEY = 'AIzaSyDf9hqXZvxOiCKaFSiIa0byrfEctP5mflI'

genai.configure(api_key=API_KEY)

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())

# Dicionário para armazenar temporariamente os dados do relatório
user_reports = {}

async def create_pool():
    pool = await aiomysql.create_pool(
        host="192.168.4.50",
        user="bruno",
        password="superbancoml",
        db="machine_learning"
    )
    return pool


# Teclado principal
main_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
main_keyboard.add(KeyboardButton("Relatório"))

# Teclado de opções de relatórios
report_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
report_keyboard.add(
    KeyboardButton("1 dia"),
    KeyboardButton("2 dias"),
    KeyboardButton("7 dias"),
    KeyboardButton("15 dias"),
    KeyboardButton("1 mês")
)

@dp.message_handler(commands=['relatorio'])
async def send_welcome(message: types.Message):
    await message.reply("Bem-vindo! Escolha uma opção:", reply_markup=main_keyboard)

@dp.message_handler(lambda message: message.text == "Relatório")
async def show_report_options(message: types.Message):
    await message.reply("Escolha o período do relatório:", reply_markup=report_keyboard)




async def fetch_report_data(pool, period, user_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT cod_usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
            result = await cursor.fetchone()
            if not result:
                return ["Usuário não encontrado."]
            cod_usuario = result[0]

            await cursor.execute("SELECT cod_usina FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
            cod_usinas = await cursor.fetchall()

            if not cod_usinas:
                return ["Nenhuma usina associada ao usuário."]

            usinas = [usina[0] for usina in cod_usinas]

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
            """ % (', '.join(['%s'] * len(usinas)), '%s', '%s')
            await cursor.execute(query, usinas + [period, period])
            log_data = await cursor.fetchall()

            if not log_data:
                return ["Nenhum dado encontrado para o período selecionado."]

            # Buscar número de geradores por usina
            await cursor.execute("""
                SELECT cod_usina, COUNT(*) AS num_geradores
                FROM sup_geral.equipamentos
                WHERE ativo = 1 
                  AND cod_tipo_equipamento IN (1, 3, 4, 12, 16, 18, 20, 22, 23, 27, 29, 33, 37, 40, 41, 43, 51, 55, 56)
                GROUP BY cod_usina
            """)
            geradores_por_usina = await cursor.fetchall()
            geradores_por_usina_dict = {usina[0]: usina[1] for usina in geradores_por_usina}

            # Buscar equipamentos em alerta na tabela leituras_consecutivas
            await cursor.execute("""
                SELECT cod_equipamento 
                FROM machine_learning.leituras_consecutivas 
                WHERE alerta = 1 AND cod_campo = 114
            """)
            equipamentos_alerta = await cursor.fetchall()
            equipamentos_alerta_set = set(equipamento[0] for equipamento in equipamentos_alerta)

            detailed_report = ""
            problem_descriptions = []
            usina_equipamento_map = {}

            for row in log_data:
                cod_equipamento, cod_usina, data_previsto, data_previsto_saida, data_quebra, nome_usina, nome_equipamento = row

                # Verificar se o equipamento está em alerta atualmente
                if cod_equipamento in equipamentos_alerta_set and not data_quebra:
                    data_quebra = 'Em funcionamento'
                elif not data_quebra:
                    data_quebra = 'Não houve falha'
                    
                data_previsto_saida = data_previsto_saida if data_previsto_saida else 'Indefinido'

                if data_quebra not in ['Não houve falha', 'Em funcionamento'] and data_previsto_saida != 'Indefinido':
                    tempo_anormalidade = datetime.strptime(str(data_previsto_saida), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                    tempo_total = datetime.strptime(str(data_quebra), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                else:
                    tempo_anormalidade = 'Não disponível'
                    tempo_total = 'Não disponível'

                # Consultar a tabela valores_previsao para obter mais dados
                await cursor.execute("""
                    SELECT DISTINCT alerta_80, alerta_100, previsao, MAX(valores_reais), MAX(valores_previstos), GROUP_CONCAT(DISTINCT alarmes) 
                    FROM machine_learning.valores_previsao 
                    WHERE cod_equipamento = %s 
                    AND ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300
                    GROUP BY cod_equipamento
                """, (cod_equipamento, data_previsto))
                previsao_data = await cursor.fetchone()
                
                if previsao_data:
                    alerta_80, alerta_100, previsao, valores_reais, valores_previstos, alarmes = previsao_data
                    alerta_status = 'Alerta 80' if alerta_80 else 'Alerta 100' if alerta_100 else 'Previsão' if previsao else 'Nenhum alerta'

                    # Obter descrições dos alarmes
                    if alarmes:
                        # Processar a string de alarmes para extrair os códigos
                        alarm_codes = []
                        for code in alarmes.split(','):
                            try:
                                alarm_codes.append((int(code.strip().strip('()')),))
                            except ValueError:
                                continue  # Ignorar códigos inválidos

                        alarm_descriptions = await fetch_alarm_descriptions(pool, alarm_codes)
                        alarmes_text = ', '.join(alarm_descriptions.get(code[0], "Descrição não encontrada") for code in alarm_codes)
                    else:
                        alarmes_text = 'Sem alarmes'

                #     problem_description = (
                #         f"    Tempo anormalidade: {tempo_anormalidade}\n"
                #         f"    Tempo total: {tempo_total}\n"
                #         f"    Status: {alerta_status}\n"
                #         f"    Valores reais: {valores_reais}\n"
                #         f"    Valores previstos: {valores_previstos}\n\n"
                #         f"    Alarmes: {alarmes_text}\n"
                #     )
                # else:
                #     problem_description = (
                #         "    Nenhum dado de previsão disponível.\n"
                #     )

                    problem_description = f"""
                        <b>Tempo de Anormalidade:</b> {tempo_anormalidade} <br/>
                        <b>Tempo Total:</b> {tempo_total} <br/>
                        <b>Alerta:</b> {alerta_status} <br/>
                        <b>Valores Reais:</b> {valores_reais} <br/>
                        <b>Valores Previstos:</b> {valores_previstos} <br/>
                        <b>Alarmes:</b> {alarmes} <br/>
                    """
                else:
                    problem_description = """
                        <b>Nenhum dado de previsão disponível.</b>
                """
                    

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
                else:
                    funcionando_text = "    Equipamentos funcionando na hora: 0\n"

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

                    funcionando_text += f"    - Equipamento {equip} ({nome_do_equipamento}):\n      Valores reais: {', '.join(map(str, valores_reais))}\n      Valores previstos: {', '.join(map(str, valores_previstos))}\n"


                if (nome_usina, nome_equipamento) not in usina_equipamento_map:
                    usina_equipamento_map[(nome_usina, nome_equipamento)] = []
                usina_equipamento_map[(nome_usina, nome_equipamento)].append(problem_description)

                num_geradores = geradores_por_usina_dict.get(cod_usina, "Desconhecido")

                if nome_usina not in detailed_report:
                    detailed_report += f"<b>Usina:</b> {nome_usina} - ({cod_usina})<br/><br/>    <b>Quantidade de geradores:</b> {num_geradores}<br/>{funcionando_text}<br/><br/>"

                # detailed_report += (
                #     f"  Equipamento: {nome_equipamento} - ({cod_equipamento})\n"
                #     f"    Data previsto: {data_previsto}\n"
                #     f"    Data previsto saída: {data_previsto_saida}\n"
                #     f"    Data quebra: {data_quebra}\n\n"
                #     f"    Descrição do problema:\n"
                #     f"{problem_description}\n"
                #     f"    --------------------------------------\n"
                # )

                detailed_report +=  f"""
                    <b>Nome do Equipamento:</b> {nome_equipamento} <br/>
                    <b>Data Prevista:</b> {data_previsto} <br/>
                    <b>Data Prevista de Saída:</b> {data_previsto_saida} <br/>
                    <b>Data de Quebra:</b> {data_quebra} <br/>
                    <b>Descrição do problema:</b>
                    <b>{problem_description}</b>
                    <br/>    --------------------------------------  <br/>
                """
                
            # Ajuste do relatório geral
            general_report = (
            #    f"\nPeríodo: Últimos {log_data} dias.\n"
                f"<br/>Período: Últimos {log_data} dias.<br/>"
            )
            
            # Usar a API Gemini para formatar a descrição geral do relatório e gerar conclusões
            try:
                model = genai.GenerativeModel('gemini-1.5-pro-latest')
                chat = model.start_chat(history=[])
                context = (
                    "Aqui estão as definições dos parâmetros usados no relatório:\n"
                    f'- Use esses dados como parametro, {detailed_report}\n'
                    "- Alerta 100: Valores reais estão em 100% do load speed, o que não é bom.\n"
                    "- Alerta 80: Valores reais estão em 80% ou mais do load speed, o que também não é bom para o gerador.\n"
                    "- Previsão: Valores reais do load speed estão fora do padrão dos valores previstos para este equipamento.\n"
                    "- Data previsto: Hora que o gerador teve valores de load speed fora do padrão.\n"
                    "- Data previsto saída: Hora que os valores fora do padrão se normalizaram.\n"
                    "- Data quebra: Hora que o equipamento ficou indisponível, ou seja, parou e zerou seus valores do load speed. Não significa que ficou parado por esse tempo.\n"
                    "- alarmes: Se não houver alarmes, coloque 'sem alarmes'.\n"
                    "- Valores reais: e as leituras reais da porcentagm do load speed do gerador.\n"
                    "- Valores previstos: e as leituras previstas da porcentagm do load speed do gerador.\n"
                    "- Quantidade de geradores: quantidade total de geradores na usina, ligados ou nao. \n"
                    "- Equipamentos funcionando na hora: geradores ativos da mesma usina no mesmo momento que algum gerador estava com anomalia.\n"
                    "Use essas definições para entender o contexto, mas não inclua essas explicações no relatório.\n"
                    "Não invente dado, use apenas os dados do banco de dados retornados, não minta.\n"
                )

                prompt_general = (
                    " use somente o inicio e fim, pegando a data de inicio e fim das leituras, nao coloque a hora, pegue somente a data no modelo dia/mes/ano e escreva em portugues"
                    f"{general_report}\n"
                )
                response_general = chat.send_message(prompt_general)
                formatted_general_report = response_general.text

                # Geração da conclusão usando a API Gemini
                conclusions_prompt = (
                    context + "\n\n"
                    "Baseando-se no seguinte relatório de ocorrências, gere uma conclusão clara e detalhada que explique os alarmes de maneira que seja fácil de entender para uma pessoa que não tem conhecimentos técnicos, falando qual o nome do equipamento e de qual usina.\n"
                    "Descreva a quantidade de equipamentos que se tem na usina.\n"
                    "Faça sugestao para utilizar os outros equipamentos da usina se ver a necessidade de acordo com os equipamentos funcionando na hora da anormalizade de um equipamento.\n"
                    "Compare as datas para o mesmo equipamento, veja se teve relacao uma parada com outra, veja se as paradas estao no mesmo periodo de tempo.\n"
                    "De sempre um pulo de uma linha para cada usina que for descrever\n"
                    "A seguir, uma análise detalhada dos alarmes encontrados e suas implicações:\n"
                    "De sempre um pulo de uma linha para cada análise detalhada dos alarmes encontrados e suas implicações que for descrever\n"
                )
                # Adicionar análise de alarmes ao relatório
                for key, descriptions in usina_equipamento_map.items():
                    nome_usina, nome_equipamento = key
                    for problem_description in descriptions:
                        conclusions_prompt += f"{problem_description}\n"

                # Finalizar com explicações específicas sobre os alarmes
                conclusions_prompt += (
                    "Recomendações:\n"
                    "- Para o alarme X, isso significa Y. É importante verificar Z para mitigar o risco.\n"
                    "- De sempre um pulo de uma linha para cada análise detalhada dos alarmes encontrados e suas implicações que for descrever\n"
                    "- O alarme W indica um potencial problema com a componente V; é aconselhável inspeção no ponto A e manutenção na peça B do gerador.\n"
                )

                response_conclusions = chat.send_message(conclusions_prompt)
                conclusions = response_conclusions.text

                # Juntar a descrição geral formatada, o relatório detalhado e as conclusões
#                final_report = formatted_general_report + "\n\n" + detailed_report + "\n-------------Conclusões---------------\n\n" + conclusions
                final_report = formatted_general_report + "<br/><br/>" + detailed_report + "<br/>-------------Conclusões---------------<br/><br/>" + conclusions

                # Dividir o relatório em partes menores
                report_parts = split_report(final_report)

                return report_parts

            except Exception as e:
                logging.error(f"Erro ao gerar texto com a API Gemini: {e}")
                return ["Ocorreu um erro ao gerar o relatório. Tente novamente mais tarde."]



# Inicializando os modelos Gemini-Pro e Gemini-Pro Vision
gemini_model = genai.GenerativeModel('gemini-pro')
gemini_vision_model = genai.GenerativeModel('gemini-pro-vision')

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
            image = Image.open(photo_data.name)
            
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

@dp.message_handler(lambda message: message.text in ["1 dia", "2 dias", "7 dias", "15 dias", "1 mês"])
async def handle_report_period(message: types.Message):
    period_map = {
        "1 dia": 1,
        "2 dias": 2,
        "7 dias": 7,
        "15 dias": 15,
        "1 mês": 30
    }
    period = period_map[message.text]
    await message.reply(f"Você escolheu o relatório de {message.text}. Aguarde enquanto buscamos os dados...", reply_markup=main_keyboard)

    # Buscar os dados do relatório
    user_id = message.from_user.id
    pool = await create_pool()
    report_parts = await fetch_report_data(pool, period, user_id)

    # Armazenar os dados do relatório temporariamente
    user_reports[user_id] = report_parts

    # Enviar o relatório em partes
#    for part in report_parts:
#        await message.reply(part, reply_markup=main_keyboard, parse_mode='HTML')

    # Enviar botão para gerar PDF
    generate_pdf_button = InlineKeyboardMarkup().add(InlineKeyboardButton("Gerar PDF", callback_data="generate_pdf"))
    await message.reply("Deseja gerar o PDF do relatório?", reply_markup=generate_pdf_button)

@dp.callback_query_handler(lambda c: c.data == "generate_pdf")
async def process_callback_generate_pdf(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id

    # Obter os dados do relatório armazenados temporariamente
    report_parts = user_reports.get(user_id)

    if not report_parts:
        await bot.answer_callback_query(callback_query.id, "Erro ao gerar o PDF. Tente novamente.")
        return

    # Criar e enviar o PDF com o relatório
    pdf_file = await create_pdf(report_parts)
    await bot.send_document(chat_id=callback_query.message.chat.id, document=InputFile(pdf_file, filename="relatorio.pdf"))

async def create_pdf(report_parts):
    logo_path = "../imagens/cabeçalho.png"  # Altere para o caminho do seu logotipo
    temp_pdf = tempfile.mktemp(suffix=".pdf")

    doc = SimpleDocTemplate(temp_pdf, pagesize=A4)
    elements = []

    # Adicionando logo no canto esquerdo de cada página com 50% de transparência e tamanho de 25x50
    logo = Image(logo_path, width=25, height=50)
    
    def header(canvas, doc):
        canvas.saveState()
        canvas.setFillAlpha(0.5)
        logo.drawOn(canvas, 2 * cm, A4[1] - 2 * cm - 50)
        canvas.restoreState()

    for part in report_parts:
        elements.append(Paragraph(part, getSampleStyleSheet()["BodyText"]))
        elements.append(Spacer(1, 12))

    doc.build(elements, onFirstPage=header, onLaterPages=header)
    return temp_pdf




if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
    
    

'''


''' gera PDF com a tabela em outro PDF'''
'''

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

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle,FrameBreak
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics



TOKEN = "6674960909:AAEf4Ky64lM6NILX3wZ6HJEASTt42Q2vopc"
#API_KEY = 'AIzaSyDfcjDbVZ2BbPJEuSpJ4wF_ATguRuffikA'

API_KEY = 'AIzaSyDf9hqXZvxOiCKaFSiIa0byrfEctP5mflI'

genai.configure(api_key=API_KEY)

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())

# Dicionário para armazenar temporariamente os dados do relatório
user_reports = {}

async def create_pool():
    pool = await aiomysql.create_pool(
        host="192.168.4.50",
        user="bruno",
        password="superbancoml",
        db="machine_learning"
    )
    return pool


# Teclado principal
main_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
main_keyboard.add(KeyboardButton("Relatório"))

# Teclado de opções de relatórios
report_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
report_keyboard.add(
    KeyboardButton("1 dia"),
    KeyboardButton("2 dias"),
    KeyboardButton("7 dias"),
    KeyboardButton("15 dias"),
    KeyboardButton("1 mês")
)

@dp.message_handler(commands=['relatorio'])
async def send_welcome(message: types.Message):
    await message.reply("Bem-vindo! Escolha uma opção:", reply_markup=main_keyboard)

@dp.message_handler(lambda message: message.text == "Relatório")
async def show_report_options(message: types.Message):
    await message.reply("Escolha o período do relatório:", reply_markup=report_keyboard)


# Inicializando os modelos Gemini-Pro e Gemini-Pro Vision
gemini_model = genai.GenerativeModel('gemini-pro')
gemini_vision_model = genai.GenerativeModel('gemini-pro-vision')

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
            image = Image.open(photo_data.name)
            
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



async def fetch_report_data(pool, period, user_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT cod_usuario FROM machine_learning.usuarios_telegram WHERE id_telegram = %s", (user_id,))
            result = await cursor.fetchone()
            if not result:
                return ["Usuário não encontrado."]
            cod_usuario = result[0]

            await cursor.execute("SELECT cod_usina FROM sup_geral.usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
            cod_usinas = await cursor.fetchall()

            if not cod_usinas:
                return ["Nenhuma usina associada ao usuário."]

            usinas = [usina[0] for usina in cod_usinas]

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
            """ % (', '.join(['%s'] * len(usinas)), '%s', '%s')
            await cursor.execute(query, usinas + [period, period])
            log_data = await cursor.fetchall()

            if not log_data:
                return ["Nenhum dado encontrado para o período selecionado."]

            # Buscar número de geradores por usina
            await cursor.execute("""
                SELECT cod_usina, COUNT(*) AS num_geradores
                FROM sup_geral.equipamentos
                WHERE ativo = 1 
                  AND cod_tipo_equipamento IN (1, 3, 4, 12, 16, 18, 20, 22, 23, 27, 29, 33, 37, 40, 41, 43, 51, 55, 56)
                GROUP BY cod_usina
            """)
            geradores_por_usina = await cursor.fetchall()
            geradores_por_usina_dict = {usina[0]: usina[1] for usina in geradores_por_usina}

            # Buscar equipamentos em alerta na tabela leituras_consecutivas
            await cursor.execute("""
                SELECT cod_equipamento 
                FROM machine_learning.leituras_consecutivas 
                WHERE alerta = 1 AND cod_campo = 114
            """)
            equipamentos_alerta = await cursor.fetchall()
            equipamentos_alerta_set = set(equipamento[0] for equipamento in equipamentos_alerta)

            # Prepare the summarized report for Telegram
            telegram_report = "Relatório resumido:\n\n"
            usina_equipamento_map = {}
            detailed_report = ""

            # Registrar a fonte que você deseja usar
            pdfmetrics.registerFont(TTFont('Helvetica-Bold', '../imagens/helvetica-bold.ttf'))  # Substitua 'Helvetica-Bold.ttf' pelo caminho do seu arquivo de fonte

            # Initialize the document content
            doc_content = []

            # Define a function to create and append table content
            def append_table(data):
                styles = getSampleStyleSheet()
                styleNormal = styles['Normal']
                styleHeader = styles['Normal']
                styleHeader.fontSize = 6  # Setting font size to 6 points
                styleHeader.fontName = 'Helvetica-Bold'  # Set font to bold

                colWidths = [160, 25, 63, 73, 45, 35, 50, 55, 90]  # Define column widths
                t = Table(data, colWidths=colWidths, rowHeights=20)

                t.setStyle(TableStyle([
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),  # Align left
                    ('FONT', (0, 0), (-1, 0), 'Helvetica-Bold', 6),  # Font and size for header
                    ('FONT', (0, 1), (-1, -1), 'Helvetica', 5),  # Font and size for body
                    ('GRID', (0, 0), (-1, -1), 0.01, colors.black),  # Grid lines
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  # Vertical align middle
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 2),  # Bottom padding
                    ('TOPPADDING', (0, 0), (-1, -1), 2),  # Top padding
                ]))
                
                doc_content.append(t)

            # Data for the table with the header
            data = [
                ['Usina', 'Equip.', 'Data Prevista', 'Duração Anormalidade', 'Tempo Total', 'Alerta', 'Load Speed %', 'Valor Previsto %', 'Alarmes']
            ]

            # Process log data and build content for each usina
            for row in log_data:
                cod_equipamento, cod_usina, data_previsto, data_previsto_saida, data_quebra, nome_usina, nome_equipamento = row

                # Verificar se o equipamento está em alerta atualmente
                if cod_equipamento in equipamentos_alerta_set and not data_quebra:
                    data_quebra = 'Em funcionamento'
                elif not data_quebra:
                    data_quebra = 'Não houve falha'
                    
                data_previsto_saida = data_previsto_saida if data_previsto_saida else 'Indefinido'

                if data_quebra not in ['Não houve falha', 'Em funcionamento'] and data_previsto_saida != 'Indefinido':
                    tempo_anormalidade = datetime.strptime(str(data_previsto_saida), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                    tempo_total = datetime.strptime(str(data_quebra), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(data_previsto), '%Y-%m-%d %H:%M:%S')
                else:
                    tempo_anormalidade = 'Não disponível'
                    tempo_total = 'Não disponível'
                    
                # Consultar a tabela valores_previsao para obter mais dados
                await cursor.execute("""
                    SELECT DISTINCT alerta_80, alerta_100, previsao, valores_reais, valores_previstos, GROUP_CONCAT(DISTINCT alarmes) 
                    FROM machine_learning.valores_previsao 
                    WHERE cod_equipamento = %s 
                    AND ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300
                    GROUP BY cod_equipamento
                """, (cod_equipamento, data_previsto))
                previsao_data = await cursor.fetchone()
                
                if previsao_data:
                    alerta_80, alerta_100, previsao, valores_reais, valores_previstos, alarmes = previsao_data
                    alerta_status = 'Alerta 80' if alerta_80 else 'Alerta 100' if alerta_100 else 'Previsão' if previsao else 'Nenhum alerta'

                    # Converter strings de valores para listas de floats
                    valores_reais = list(map(float, valores_reais.split(',')))
                    valores_previstos = list(map(float, valores_previstos.split(',')))


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
                        else:
                            alarmes_text_tabela = 'Sem alarmes'
                    else:
                        alarmes_text = 'Sem alarmes'
                        alarmes_text_tabela = 'Sem alarmes'

                    problem_description = f"""
                        <b>Tempo de Anormalidade:</b> {tempo_anormalidade} <br/>
                        <b>Tempo Total:</b> {tempo_total} <br/>
                        <b>Alerta:</b> {alerta_status} <br/>
                        <b>Valores Reais:</b> {valores_reais} <br/>
                        <b>Valores Previstos:</b> {valores_previstos} <br/>
                        <b>Alarmes:</b> {alarmes} <br/>
                        
                    """
                else:
                    problem_description = """
                        <b>Nenhum dado de previsão disponível.</b>
                """

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
                else:
                    funcionando_text = "    Equipamentos funcionando na hora: 0\n"

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

                    funcionando_text += f"<br/>    - Equipamento {equip} ({nome_do_equipamento}):\n      Valores reais: {', '.join(map(str, valores_reais))}\n      Valores previstos: {', '.join(map(str, valores_previstos))}\n"

                if (nome_usina, nome_equipamento) not in usina_equipamento_map:
                    usina_equipamento_map[(nome_usina, nome_equipamento)] = []
                usina_equipamento_map[(nome_usina, nome_equipamento)].append(problem_description)

                num_geradores = geradores_por_usina_dict.get(cod_usina, "Desconhecido")

                if nome_usina not in detailed_report:
                    detailed_report += f"<b>Usina:</b> {nome_usina} - ({cod_usina})<br/><br/>    <b>Quantidade de geradores:</b> {num_geradores}<br/>{funcionando_text}<br/><br/>"

                detailed_report +=  f"""
                
                    <b>Equipamento:</b> {nome_equipamento} <br/>
                    <b>Data Prevista:</b> {data_previsto} <br/>
                    <b>Data Prevista de Saída:</b> {data_previsto_saida} <br/>
                    <b>Data de Quebra:</b> {data_quebra} <br/><br/>
                    <b>Descrição do problema:</b><br/>
                    <b>{problem_description}</b><br/>
                    <br/>    --------------------------------------  <br/> <br/>
                """

                telegram_report += (
                    f"Usina: {nome_usina}\n"
                    f"  Equipamento: {nome_equipamento}\n"
                    f"    Valores reais: {valores_reais}\n"
                    f"    Valores previstos: {valores_previstos}\n"
                    f"    Tipo de Alerta: {alerta_status}\n"
                    f"    Alarmes: {alarmes_text}\n\n"
                )


               # Add usina data to the table
                data.append([
                    str(nome_usina), str(nome_equipamento), str(data_previsto), str(tempo_anormalidade), str(tempo_total), str(alerta_status), str(max(valores_reais)), str(max(valores_previstos)), str(alarmes_text_tabela)
                ])
                
            # Append the final table with all usina data to the document content
            append_table(data)

            # Create the PDF document
            pdf_filename = 'relatorio.pdf'
            doc = SimpleDocTemplate(pdf_filename, pagesize=letter)

            # Build the PDF with accumulated content
            doc.build(doc_content)
            
            
            # Ajuste do relatório geral
            general_report = (
                f"<br/>Período: Últimos {log_data} dias.<br/>"
            )
            
            # Usar a API Gemini para formatar a descrição geral do relatório e gerar conclusões
        #    model = genai.GenerativeModel('gemini-1.5-pro-latest')
            model = genai.GenerativeModel('gemini-1.5-flash-latest')
            chat = model.start_chat(history=[])
            context = (
                "Aqui estão as definições dos parâmetros usados no relatório:<br/>"
                f'- Use esses dados como parametro, {detailed_report}<br/>'
                "- Envie em formato html, utilizando <br/> para pular linha, <b> e </b> para negrito para formatar o texto do relatorio, pois e um pdf que se gera.<br/>"
                "- Alerta 100: Valores reais estão em 100% do load speed, o que não é bom.<br/>"
                "- Alerta 80: Valores reais estão em 80% ou mais do load speed, o que também não é bom para o gerador.<br/>"
                "- Previsão: Valores reais do load speed estão fora do padrão dos valores previstos para este equipamento.<br/>"
                "- Data previsto: Hora que o gerador teve valores de load speed fora do padrão.<br/>"
                "- Data previsto saída: Hora que os valores fora do padrão se normalizaram.<br/>"
                "- Data quebra: Hora que o equipamento ficou indisponível, ou seja, parou e zerou seus valores do load speed. Não significa que ficou parado por esse tempo.<br/>"
                "- alarmes: Se não houver alarmes, coloque 'sem alarmes'.<br/>"
                "- Valores reais: e as leituras reais da porcentagm do load speed do gerador.<br/>"
                "- Valores previstos: e as leituras previstas da porcentagm do load speed do gerador.<br/>"
                "- Quantidade de geradores: quantidade total de geradores na usina, ligados ou nao. <br/>"
                "- Equipamentos funcionando na hora: geradores ativos da mesma usina no mesmo momento que algum gerador estava com anomalia.<br/>"
                "Use essas definições para entender o contexto, mas não inclua essas explicações no relatório.<br/>"
                "Não invente dado, use apenas os dados do banco de dados retornados, não minta.<br/>"
            )

            prompt_general = (
                " pegue a data de inicio e fim das leituras do periodo selecionado, nao coloque a hora, pegue somente a data no modelo dia/mes/ano e escreva em portugues"
                f"{general_report}<br/>"
                " Não escreva mais nada alem da data"
            )
            response_general = chat.send_message(prompt_general)
            formatted_general_report = response_general.text

            # Geração da conclusão usando a API Gemini
            conclusions_prompt = (
                context + "<br/><br/>"
                "Baseando-se no seguinte relatório de ocorrências, gere uma conclusão clara e detalhada que explique os alarmes de maneira que seja fácil de entender para uma pessoa que não tem conhecimentos técnicos, falando qual o nome do equipamento e de qual usina.<br/>"
                "Descreva a quantidade de equipamentos que se tem na usina.<br/>"
                "Faça sugestao para utilizar os outros equipamentos da usina se ver a necessidade de acordo com os equipamentos funcionando na hora da anormalizade de um equipamento.<br/>"
                "Compare as datas para o mesmo equipamento, veja se teve relacao uma parada com outra, veja se as paradas estao no mesmo periodo de tempo.<br/>"
                "De sempre um pulo de uma linha para cada usina que for descrever<br/>"
                "A seguir, uma análise detalhada dos alarmes encontrados e suas implicações:<br/>"
                "De sempre um pulo de uma linha para cada análise detalhada dos alarmes encontrados e suas implicações que for descrever<br/>"
            )
            # Adicionar análise de alarmes ao relatório
            for key, descriptions in usina_equipamento_map.items():
                nome_usina, nome_equipamento = key
                for problem_description in descriptions:
                    conclusions_prompt += f"{problem_description}<br/>"

            # Finalizar com explicações específicas sobre os alarmes
            conclusions_prompt += (
                "Recomendações:<br/>"
                "- Para o alarme X, isso significa Y. É importante verificar Z para mitigar o risco.<br/>"
                "- De sempre um pulo de uma linha para cada análise detalhada dos alarmes encontrados e suas implicações que for descrever<br/>"
                "- O alarme W indica um potencial problema com a componente V; é aconselhável inspeção no ponto A e manutenção na peça B do gerador.<br/>"
            )

            response_conclusions = chat.send_message(conclusions_prompt)
            conclusions = response_conclusions.text

            # Juntar a descrição geral formatada, o relatório detalhado e as conclusões
            final_report = formatted_general_report + "<br/><br/>" + "<br/>-------------Conclusões---------------<br/><br/>" + conclusions

            # Dividir o relatório em partes menores
            telegram_report = split_report(telegram_report)
            detailed_report += final_report

            return [telegram_report, detailed_report]



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

@dp.message_handler(lambda message: message.text in ["1 dia", "2 dias", "7 dias", "15 dias", "1 mês"])
async def handle_report_period(message: types.Message):
    period_map = {
        "1 dia": 1,
        "2 dias": 2,
        "7 dias": 7,
        "15 dias": 15,
        "1 mês": 30
    }
    period = period_map[message.text]
    await message.reply(f"Você escolheu o relatório de {message.text}. Aguarde enquanto buscamos os dados...", reply_markup=main_keyboard)

    # Buscar os dados do relatório
    user_id = message.from_user.id
    pool = await create_pool()
    telegram_report, detailed_report = await fetch_report_data(pool, period, user_id)

    # Enviar o relatório resumido no Telegram
    for part in telegram_report:
        await message.reply(part)
        
        
    # Armazenar o relatório detalhado para o PDF
    user_reports[user_id] = detailed_report

    # Enviar botão para gerar PDF
    generate_pdf_button = InlineKeyboardMarkup().add(InlineKeyboardButton("Gerar PDF", callback_data="generate_pdf"))
    await message.reply("Deseja gerar o PDF do relatório?", reply_markup=generate_pdf_button)



@dp.callback_query_handler(lambda c: c.data == "generate_pdf")
async def process_callback_generate_pdf(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id

    # Obter os dados do relatório armazenados temporariamente
    detailed_report = user_reports.get(user_id)

    if not detailed_report:
        await bot.answer_callback_query(callback_query.id, "Erro ao gerar o PDF. Tente novamente.")
        return

    # Criar e enviar o PDF com o relatório
    pdf_file = await create_pdf(detailed_report)
    await bot.send_document(chat_id=callback_query.message.chat.id, document=InputFile(pdf_file, filename="relatorio.pdf"))

async def create_pdf(detailed_report):
    temp_pdf = tempfile.mktemp(suffix=".pdf")
    doc = SimpleDocTemplate(temp_pdf, pagesize=A4)
    elements = []

    # Adicionando logo no canto esquerdo de cada página
    logo_path = "../imagens/cabeçalho.png"  # Altere para o caminho do seu logotipo
    logo = Image(logo_path, width=100, height=50)

    def header(canvas, doc):
        canvas.saveState()
        logo.drawOn(canvas, 5, A4[1] - 55)  # 2px from left, 2px from top
        canvas.restoreState()

    # Adicionando conteúdo ao PDF
    elements.append(Spacer(1, 40))  # Espaço após o cabeçalho
    elements.append(Paragraph(detailed_report, getSampleStyleSheet()["BodyText"]))
    elements.append(Spacer(1, 12))

    doc.build(elements, onFirstPage=header, onLaterPages=header)
    return temp_pdf



if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
    


'''