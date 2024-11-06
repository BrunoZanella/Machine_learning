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

# Configuração de log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração de conexão com MySQL (conexão síncrona para setup inicial)
cnx = mysql.connector.connect(
    host="192.168.4.50",
    user="bruno",
    password="superbancoml"
)
cursor = cnx.cursor(buffered=True)

# Função para criar pool assíncrono
async def create_pool():
    pool = await aiomysql.create_pool(
        host="192.168.4.50",
        user="bruno",
        password="superbancoml",
        db="machine_learning",
        minsize=1,
        maxsize=10
    )
    return pool

















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



async def buscar_alarmes_ativos(pool, cod_equipamento):
    limite_tempo = datetime.now() - timedelta(hours=6)
    
    # Adquire uma conexão da pool
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # Executa a consulta
            await cursor.execute("""
            SELECT cod_alarme, visto, data_cadastro 
            FROM sup_geral.alarmes_ativos
            WHERE cod_equipamento = %s 
            AND (visto = 0 OR visto IS NULL) 
            AND data_cadastro > %s
            """, (cod_equipamento, limite_tempo))
            
            # Obtém os resultados
            result = await cursor.fetchall()
    
    return result

async def verificar_alarmes(pool):
#    pool = dp.pool  # Reutilizando o pool criado durante a inicialização
    while True:
        try:
            async with pool.acquire() as conn:
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


codigos_alarmes_desejados = [1, 243, 244, 253, 256, 259, 262,265,269,272,273,279,280,281,301,304,350, 351, 352, 353, 356, 357, 381, 383, 384, 385, 386, 387, 388, 389, 390, 400, 401, 404, 405,411,412,413,414,415,416, 471, 472, 473,528, 590, 591, 592, 593, 594,595,596,597,598,599,600, 602, 603, 604, 611,615,616,617,631, 635, 637, 638, 657, 658,669,678, 725, 727, 728, 729, 730, 731, 732, 735]
async def monitorar_leituras_consecutivas(pool):
    equipamentos_com_alerta = {}
    # Dicionário para armazenar alarmes e cod_equipamento
    equipamentos_alerta_0 = {}

    while True:

        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                    SELECT cod_equipamento, cod_campo, alerta, valor_1, valor_2, valor_3, valor_4, valor_5 FROM machine_learning.leituras_consecutivas
                    """)
                    leituras = await cursor.fetchall()
                    for leitura in leituras:
                        try:
                            cod_equipamento, cod_campo, alerta, valor_1, valor_2, valor_3, valor_4, valor_5 = leitura

                            if alerta == 1:
                                tempo_inicial = datetime.now()
                                data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')

                                if cod_equipamento not in equipamentos_com_alerta:
                                    equipamentos_com_alerta[cod_equipamento] = datetime.now()
                                    print('Alerta ativado para o equipamento', cod_equipamento,data_cadastro_formatada,'\n')
                                #    print('equipamentos_com_alerta',equipamentos_com_alerta)
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
                                    
                                    # if cod_campo == 114 and any(value == 0 for value in [valor_1, valor_2, valor_3, valor_4, valor_5]): # se algum valor for 0

                                    #     # Verifica se já está no dicionário, senão adiciona
                                    #     if cod_equipamento not in equipamentos_alerta_0:
                                    #         tempo_inicial = datetime.now()
                                    #         await cursor.execute("""
                                    #             SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                    #         """, (cod_equipamento,))
                                    #         alarmes_ativos = await cursor.fetchall()

                                    #         # Adiciona alarmes ativos ao dicionário
                                    #         equipamentos_alerta_0[cod_equipamento] = {
                                    #             'alarmes': [alarme[0] for alarme in alarmes_ativos],
                                    #             'tempo_alerta_0': tempo_inicial
                                    #         }
                                    #         print(f"Equipamento {cod_equipamento} com alerta em 0 sem valores zerados adicionado ao dicionário com alarmes {equipamentos_alerta_0[cod_equipamento]['alarmes']}")
                                    #     else:
                                    #         # Atualiza apenas com novos alarmes, mantendo os antigos
                                    #         await cursor.execute("""
                                    #             SELECT cod_alarme FROM sup_geral.alarmes_ativos WHERE cod_equipamento = %s
                                    #         """, (cod_equipamento,))
                                    #         alarmes_ativos = await cursor.fetchall()

                                    #         # Mantém os alarmes existentes
                                    #         alarmes_existentes = equipamentos_alerta_0[cod_equipamento]['alarmes']

                                    #         # Verifica se há novos alarmes, para adicionar apenas os novos
                                    #         novos_alarmes = [alarme[0] for alarme in alarmes_ativos if alarme[0] not in alarmes_existentes]

                                    #         if novos_alarmes:
                                    #             # Adiciona os novos alarmes à lista de alarmes existentes
                                    #             equipamentos_alerta_0[cod_equipamento]['alarmes'].extend(novos_alarmes)
                                    #             equipamentos_alerta_0[cod_equipamento]['tempo_alerta_0'] = datetime.now()
                                    #             print(f"Equipamento {cod_equipamento} com alerta em 0 sem valores zerados atualizado com novos alarmes {novos_alarmes}")
                                    #         else:
                                    #             print(f"Equipamento {cod_equipamento} com alerta em 0 sem valores zerados não tem novos alarmes, mantendo a lista atual {alarmes_existentes}")

                                        

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

async def processar_equipamentos(cod_equipamentos, tabelas, cod_campo_especificados, pool):
    while True:
#        tempo_inicial = datetime.now()
#        print('inicio do processamento dos equipamentos',tempo_inicial)

        try:
            async with pool.acquire() as conn:
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

#        tempo_final = datetime.now()
#        print('\ntempo total de processamento dos equipamentos e campos', tempo_final - tempo_inicial)

        await asyncio.sleep(10)

async def processar_dados_por_equipamento(pool, df, cod_equipamento, cod_campo_especificados):
    try:
        # Filtrar dados para o equipamento específico
        df_equipamento = df[df['cod_equipamento'] == cod_equipamento]

        for cod in cod_campo_especificados:
            valores_cod_campo = df_equipamento[df_equipamento['cod_campo'] == int(cod)]['valor'].values
            data_cadastro_recente = df_equipamento[df_equipamento['cod_campo'] == int(cod)]['data_cadastro'].max()
            
            # Pegue o valor mais recente
            valor_recente = valores_cod_campo[-1] if len(valores_cod_campo) > 0 else 0
            
            # Pegue os últimos 4 valores anteriores, mais o valor recente para inserir em valor_5
            valores = list(valores_cod_campo[-4:])[::-1]  # Últimos 4 valores
            valores = [0] * (4 - len(valores)) + valores  # Preencher com zeros se necessário

            # Adicionar o valor recente no final
            valores.append(valor_recente)

            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Verifique se já existe um registro com a mesma data_cadastro ou mais recente na tabela `leituras_consecutivas`
                    check_query = """
                    SELECT data_cadastro FROM machine_learning.leituras_consecutivas 
                    WHERE cod_equipamento = %s AND cod_campo = %s
                    ORDER BY data_cadastro DESC LIMIT 1
                    """
                    await cursor.execute(check_query, (cod_equipamento, cod))
                    data_cadastro_consecutiva = await cursor.fetchone()
                    
                    # Se já existir um registro com data_cadastro maior ou igual, pula a inserção
                    if data_cadastro_consecutiva and data_cadastro_consecutiva[0] >= data_cadastro_recente:
                    #    print(f"Registro com data_cadastro mais recente já existe para equipamento {cod_equipamento} e campo {cod}. Pulando inserção.")
                        continue

                    # Atualizar leituras consecutivas
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
                    # Execute a query com os valores corretos
                    await cursor.execute(insert_query, (cod_equipamento, cod, *valores))

                # Commit na conexão
                await conn.commit()

    except Exception as e:
        print(f"Erro ao processar dados do equipamento {cod_equipamento}: {str(e)}")

async def adicionar_DataQuebra_FG(pool):  # Funçao para passar data_cadastro_quebra/valores_previsão para data_cadastro/falhas_gerais
    
    tamanho_lote = 1000000
    valor_offset = 0
    
    while True:
    #    print('Iniciando data_cadastro_quebra para tabela falhas gerais formatada como data_cadastro')
        try:
            async with pool.acquire() as conn:    
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
                                    '(730,)', '(731,)', '(732,)', '(735,)')
                            )                       
                        LIMIT %s OFFSET %s
                    """, (tamanho_lote, valor_offset))

                    linhas = await cursor.fetchall()
                    if not linhas:
                    #    print('Pedro, Nenhum registro encontrado. Saindo do loop.')
                        await asyncio.sleep(2)  
                        continue
                    
                
                    # Inserir as linhas selecionadas na tabela falhas_gerais
                    for linha in linhas:
                        cod_equipamento, cod_usina, data_cadastro_quebra, alerta_80, alerta_100, previsao = linha

                        await cursor.execute("""
                            INSERT INTO machine_learning.falhas_gerais (cod_equipamento, cod_usina, data_cadastro, falha, alerta_80, alerta_100, previsao)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (cod_equipamento, cod_usina, data_cadastro_quebra, 1, alerta_80, alerta_100, previsao))

                        print(f'Inserido registro para falha 1 em equipamento {cod_equipamento}, cod_usina {cod_usina}, na data {data_cadastro_quebra} com alertas {alerta_80},{alerta_100},{previsao} na tabela falhas_gerais.')

                    await conn.commit()
                #    print('Transação de dados pelas planilhas valores_previsto e falhas_gerais feitas com sucesso')
                    # Atualizar o offset para o próximo lote
                    valor_offset += tamanho_lote
        except:

            await asyncio.sleep(3600)





# Inicialização das tabelas
async def criar_tabelas(pool):
    await criar_tabela_usuarios_telegram(pool)
    await criar_tabela_relatorio_quebras(pool)
    await criar_tabela_log_relatorio_quebras(pool)
    await criar_tabela_silenciar_bot(pool)
    await criar_tabela_leituras(pool)
    await criar_tabela_valores_previsao(pool)
    await criar_tabela_falhas_gerais(pool)
    await criar_tabela_usinas_usuario(pool)

# Processos menos pesados
async def processos_async_menos_pesados(pool):
    try:
        tarefas = [
            monitorar_leituras_consecutivas(pool),
            verificar_alarmes(pool),
            verificar_e_excluir_linhas_expiradas(pool),
            clean_temp_files(),
            atualizar_usinas_usuario(pool),
            adicionar_DataQuebra_FG(pool),
        ]
        await asyncio.gather(*tarefas)
    except asyncio.CancelledError:
        print("Tarefa de processos leves cancelada.")
    except Exception as e:
        print(f"Erro durante a execução dos processos leves: {e}")

# Processos pesados
async def processos_async_pesados(pool):
    tabelas = 'sup_geral.leituras'
    cod_equipamentos = await obter_equipamentos_validos(tabelas, pool)
    cod_campo_especificados = ['3','6','7','8','9','10', '11', '16', '19', '23', '24', '114', '21','76','25','20','77', '120']
    try:
        tarefas = [
            processar_equipamentos(cod_equipamentos, tabelas, cod_campo_especificados, pool),
        #    novo_enviar_previsao_valor_equipamento_alerta_novo(cod_equipamentos, tabelas, cod_campo_especificados, pool),
        #    enviar_previsao_valor_equipamento_alerta(cod_equipamentos, tabelas, cod_campo_especificados, pool),
        #    enviar_alerta_80_100(cod_equipamentos, tabelas, cod_campo_especificados, pool),
        #    focar_alertas(cod_equipamentos, tabelas, cod_campo_especificados, pool),
        ]
        await asyncio.gather(*tarefas)
    except asyncio.CancelledError:
        print("Tarefa de processos pesados cancelada.")
    except Exception as e:
        print(f"Erro durante a execução dos processos pesados: {e}")

# Fechamento do pool
async def close_pool(pool):
    pool.close()
    await pool.wait_closed()
    print("Pool de conexões fechado.")

# Função principal para inicialização
async def main():
    pool = await create_pool()
    await criar_tabelas(pool)
    try:
        # Executar processos assíncronos
        await asyncio.gather(
            processos_async_menos_pesados(pool),
            processos_async_pesados(pool)
        )
    finally:
        await close_pool(pool)

# Executa o script
if __name__ == "__main__":
    asyncio.run(main())

