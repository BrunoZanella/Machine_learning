
import os
import re
import uuid
from groq import Groq
from collections import deque
from PyPDF2 import PdfReader
from sentence_transformers import SentenceTransformer
from chromadb import Client
from chromadb.config import Settings
import pandas as pd

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
from fastapi import FastAPI, Request
import requests
import subprocess
from uvicorn import Config, Server

# Obter a hora atual
hora_atual = datetime.now().hour

# Determinar o período do dia
if 5 <= hora_atual < 12:
    periodo_do_dia = "Bom%20dia%20"
elif 12 <= hora_atual < 18:
    periodo_do_dia = "Boa%20tarde%20"
else:
    periodo_do_dia = "Boa%20noite%20"


app = FastAPI()

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


#from bot import TOKEN, bot, dp

TOKEN = "6674960909:AAEf4Ky64lM6NILX3wZ6HJEASTt42Q2vopc"

bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())

os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"  # 0: Tudo, 1: Info, 2: Warnings, 3: Erros


# Comandos que o bot não pode responder
blocked_commands = [
    '*.com*',  # Bloqueia qualquer texto que contenha ".com"
    '*.net*',  # Bloqueia qualquer texto que contenha ".net"
    '*gmail*',     # Bloqueia qualquer texto que contenha "gmail", como emails    
    '*hotmail*',     # Bloqueia qualquer texto que contenha "hotmail", como emails    
    '/start',
    '/geradores',
    '/relatorio', 
    '/menu', 
    "1 dia",
    "2 dias",
    "7 dias",
    "15 dias",
    "1 mês"
    "1_dia",
    "2_dias",
    "7_dias",
    "15_dias",
    "1_mês",
    'geral',
    "CONTROLE DE DEMANDA",
    "HORARIO DE PONTA",
    "OPERACAO CONTINUA",
    "FALTA DE ENERGIA",
    "Geral",
    "aggo",
    "agmg",
    "AGROGERA",
    "Digitar código da usina",
    "Digitar código do equipamento",
    "Inserir usuario",
    "Editar usuario ativo",
    "Editar Usinas cadastradas",
    "Receber todos os tipos de notificações",
]

# LLM

client = Client(
    Settings(
         persist_directory=r"/home/bruno/documentos/chroma_BRG0_db"   # Bruno mudar
    )
)

def extract_text_from_pdf(pdf_path):
    text = ""
    
    try:
        reader = PdfReader(pdf_path)
        for page in reader.pages:
            text += page.extract_text()
    except Exception as e:
        print(f"Erro ao processar o arquivo PDF: {pdf_path}. Detalhes: {e}")
    return text

def chunks_total(text, max_chunk_size=1024):
    words = text.split()
    chunks = []
    current_chunk = []
    current_length = 0

    for word in words:
        current_length += len(word) + 1  # Inclui espaço
        if current_length > max_chunk_size:
            chunks.append(" ".join(current_chunk))
            current_chunk = []
            current_length = len(word) + 1
        current_chunk.append(word)

    if current_chunk:
        chunks.append(" ".join(current_chunk))
    
    return chunks

def split_text_into_chunks(texto_extraido, max_chunk_size=None):

    chunks = []
    partes = texto_extraido.split("O Código")
    
    
    if partes[0]:
        chunks.append("O Código" + partes[0])

    
    for parte in partes[1:]:
        if max_chunk_size is None:
            chunks.append("O Código" + parte)
        else:
            if len(chunks[-1]) + len(parte) > max_chunk_size:
                chunks.append("O Código" + parte)
            else:
                chunks[-1] += "O Código" + parte

    return chunks

import torch

def generate_embeddings(chunks, model_name="sentence-transformers/all-MiniLM-L6-v2"):

    model = SentenceTransformer(model_name)

    embeddings = model.encode(chunks, convert_to_numpy=True)  # Passando diretamente a lista de chunks

    return embeddings

collection_name = "pdf_embeddings_brg"

try:
    collections = client.list_collections()  # Método que lista todas as coleções
    if collection_name in collections:
        client.delete_collection(name=collection_name)
        print(f"Coleção {collection_name} excluída com sucesso!")
    else:
        print(f"Coleção {collection_name} não existia.")
except Exception as e:
    print(f"Ocorreu um erro ao tentar excluir a coleção: {e}")

try:
    client.delete_collection(name=collection_name)
except:
    pass  # Ignorar erros se a coleção não existir

collection = client.create_collection(name=collection_name)
print(f"Coleção {collection_name} criada ou recriada com sucesso!")


# Lista de arquivos PDF
pdf_files = [
    r"/home/bruno/documentos/Apostila 3 - markdown.pdf",
    r"/home/bruno/documentos/Apostila 6 - markdown.pdf",
    r"/home/bruno/documentos/Apostila 5 - normal.pdf",
    r"/home/bruno/documentos/Apostila 2  - markdown.pdf",
    # r"/home/bruno/documentos/Apostila 4 - normal.pdf",
    r"/home/bruno/documentos/Alarmes e Possíveis Causas.pdf",
    r"/home/bruno/documentos/47704191_BR Elétrico.pdf",
    r"/home/bruno/documentos/Alarmes do motor.pdf"
]
doc_id = 0

for pdf_file in pdf_files:
    if os.path.exists(pdf_file):
        print(f"Processando: {pdf_file}")
        text = extract_text_from_pdf(pdf_file)
        if "Alarmes e Possíveis Causas" in pdf_file:
                print("Fazendo chunk de Alarmes")
                chunks = split_text_into_chunks(text)
        elif "47704191_BR Elétrico" in pdf_file:  
                print("Realizando chunks de BR - Elétrico")
                chunks = chunks_total(text)
        else:
                print("Arquivo não identificado. Usando chunk padrão.")
                chunks = chunks_total(text) 
        embeddings = generate_embeddings(chunks)  

        # Adiciona os chunks, embeddings e metadados na coleção
        for i, chunk in enumerate(chunks):
        #    print(i)
            chunk_id = f"doc{doc_id}_chunk{i}"  # ID único para cada chunk
            collection.add(
                embeddings=[embeddings[i]],       # Embedding do chunk
                documents=[chunk],                # Conteúdo do chunk
                metadatas=[{"file_name": pdf_file}],  # Metadados com o nome do arquivo
                ids=[chunk_id]                    # ID único
            )
        doc_id += 1
    else:
        print(f"Arquivo não encontrado: {pdf_file}")

print("Indexação concluída!")

def query_chroma(query, top_k=5):
    try:
        embedding = generate_embeddings([query])
        
        # Certifique-se de que top_k é um inteiro válido
        if not isinstance(top_k, int):
            raise ValueError(f"top_k deve ser um número inteiro. Recebido: {type(top_k)}")
        
        results = collection.query(
            query_embeddings=embedding,
            n_results=top_k  # Passando o número de resultados esperado
        )
        return results
    except ValueError as e:
        print(f"Erro durante a consulta: {e}")
        return None



client_groq = Groq(
    api_key= "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"
)



historico_perguntas = deque(maxlen=5)
historico_respostas = deque(maxlen=5)

def query_and_prompt(user_query, top_k=3):
    try:
        historico_perguntas.append(user_query)

        # Certifique-se de que top_k é um inteiro válido
        if not isinstance(top_k, int):
            raise ValueError(f"top_k deve ser um número inteiro. Recebido: {type(top_k)}")
        
        embedding = generate_embeddings([user_query])
        results = collection.query(
            query_embeddings=embedding,
            n_results=top_k  # Passando o número de resultados esperado
        )

        prompt_content = (
            "Responda de forma curta, clara e informativa à pergunta do usuário com base nos documentos. "
            "Caso o usuário pergunte um valor, verifique se há o valor e forneça o valor solicitado junto com a unidade ou grandeza associada. "
            "Por exemplo, se o valor for uma pressão, inclua a unidade como 'bar' ou 'kPa'. Certifique-se de identificar corretamente a grandeza com base no contexto. "
            "Responda em Markdown, se possível, com tópicos com soluções. "
            "Exemplos:\n"
            "# Títulos:\n"
            "    # Título 1 (h1)\n"
            "    ## Título 2 (h2)\n"
            "    ### Título 3 (h3)\n"
            "    #### Título 4 (h4)\n"
            "    ##### Título 5 (h5)\n"
            "    ###### Título 6 (h6)\n"
            "Ênfase:\n"
            "    **Texto em negrito**\n"
            "    __Texto em negrito__\n"
            "    *Texto em itálico*\n"
            "    _Texto em itálico_\n"
            "Listas:\n"
            "    - Item 1\n"
            "    - Item 2\n"
            "    - Item 3\n"
            "    1. Primeiro item\n"
            "    2. Segundo item\n"
            "    3. Terceiro item\n"
            "Citações:\n"
            "    > Esta é uma citação.\n"
            "Código:\n"
            "    `codigo_em_linha`\n"
            "    ```Bloco de código```\n"
            "Tabelas:\n"
            "    | Cabeçalho 1 | Cabeçalho 2 |\n"
            "    |-------------|-------------|\n"
            "    | Linha 1, Col 1 | Linha 1, Col 2 |\n"
            "    | Linha 2, Col 1 | Linha 2, Col 2 |\n"
            "Separadores:\n"
            "    ---\n"
            "    ***\n"
            "    ___\n"
            "Escapando Caracteres:\n"
            "    \\*Texto com asterisco\\*\n"
            "Se o usuário não perguntar algo do documento, responda de forma natural com os dados que tem. Não invente respostas, apenas entregue o que tem, caso haja relação. Se não houver, diga que não sabe.\n"
            "Se a pessoa pedir a localização da BRG Geradores, a localização é: https://maps.app.goo.gl/9ZdwWcWzg1Ujuy3f9\n"
            "Se a pessoa pedir a localização da GRID Geradores, a localização é: https://maps.app.goo.gl/Pssputwd5syeTdw16\n"
            "Se a pessoa pedir a localização da SDO, a localização é: https://maps.app.goo.gl/FRrhnBG9f6HdhChf7\n"
            "Sempre envie respostas curtas para o usuário. "
            "Tudo que começa com TWD, TAD, DC, D8, são motores de geradores da BRG Geradores. "
            "Evite motores marítimos e foque nos motores industriais.\n"
            "Não de o nome e nem o caminho do arquivo"
            f"Utilize o {periodo_do_dia} para saber o período atual.\n"
            f"""Se perceber que a pessoa está indo para o caminho de querer falar com alguém, pergunte de qual região está falando. Para as seguintes regiões, envie os números:
            - Se for **entorno de Goiânia-Goiás**, o representante é [João Victor Lião](https://api.whatsapp.com/send?phone=5562999043154&text={periodo_do_dia}tudo%20bem%3F%20e%20eu%20gostaria%20de%20solicitar%20um%20servi%C3%A7o%20de%20voc%C3%AAs).
            - Se for **Goiânia-Goiás**, o representante é [Vitor](https://api.whatsapp.com/send?phone=5562982134286&text={periodo_do_dia}tudo%20bem%3F%20e%20eu%20gostaria%20de%20solicitar%20um%20servi%C3%A7o%20de%20voc%C3%AAs).
            - Se for **Sul do Brasil**, o representante é [João Victor Lião](https://api.whatsapp.com/send?phone=5562999043154&text={periodo_do_dia}tudo%20bem%3F%20e%20eu%20gostaria%20de%20solicitar%20um%20servi%C3%A7o%20de%20voc%C3%AAs).
            - Se for **Sudeste do Brasil**, o representante é [Sérgio Mota](https://api.whatsapp.com/send?phone=5562981171423&text={periodo_do_dia}tudo%20bem%3F%20e%20eu%20gostaria%20de%20solicitar%20um%20servi%C3%A7o%20de%20voc%C3%AAs).
            - Se for **Nordeste do Brasil**, o representante é [Otávio A. Curado](https://api.whatsapp.com/send?phone=5562999735481&text={periodo_do_dia}tudo%20bem%3F%20e%20eu%20gostaria%20de%20solicitar%20um%20servi%C3%A7o%20de%20voc%C3%AAs).
            - Se for **Norte do Brasil**, o representante é [Otávio A. Curado](https://api.whatsapp.com/send?phone=5562981171407&text={periodo_do_dia}tudo%20bem%3F%20e%20eu%20gostaria%20de%20solicitar%20um%20servi%C3%A7o%20de%20voc%C3%AAs).
            - Se for **Mato Grosso**, o representante é [Otávio A. Curado](https://api.whatsapp.com/send?phone=5562981171407&text={periodo_do_dia}tudo%20bem%3F%20e%20eu%20gostaria%20de%20solicitar%20um%20servi%C3%A7o%20de%20voc%C3%AAs).
            - Se for **Mato Grosso do Sul**, o representante é [Otávio A. Curado](https://api.whatsapp.com/send?phone=5562981171407&text={periodo_do_dia}tudo%20bem%3F%20e%20eu%20gostaria%20de%20solicitar%20um%20servi%C3%A7o%20de%20voc%C3%AAs).
            - Se não for nenhuma das regiões acima, o representante é [Vitor](https://api.whatsapp.com/send?phone=5562982134286&text={periodo_do_dia}tudo%20bem%3F%20e%20eu%20gostaria%20de%20solicitar%20um%20servi%C3%A7o%20de%20voc%C3%AAs).
            """
        )


        if historico_perguntas:
            prompt_content += "Histórico de perguntas recentes do usuário:\n"
            for idx, pergunta in enumerate(historico_perguntas, 1):
                prompt_content += f"{idx}. {pergunta}\n"
            prompt_content += "\n"

        for i, doc in enumerate(results['documents'][0]):
            metadata = results['metadatas'][0][i]
            prompt_content += f"Documento {i+1} (arquivo: {metadata['file_name']}):\n{doc}\n\n"

        prompt_content += f"Pergunta: {user_query}"

        # Enviar o prompt para o Groq API
        chat_completion = client_groq.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt_content,
                }
            ],
            model="llama3-70b-8192",  
        )
        
        resposta = chat_completion.choices[0].message.content
        historico_respostas.append(resposta)
        return resposta

    except Exception as e:
        print(f"Erro ao processar a consulta: {e}")
        return None

@dp.message_handler(lambda message: message.text.lower() in blocked_commands)
async def handle_blocked_commands(message: types.Message):
    await message.reply("Desculpe, não posso responder a isso.")

user_data = {}
@dp.message_handler(lambda message: not any(command in message.text.lower() for command in blocked_commands))
async def handle_text_message(message: types.Message):
    pool = await create_pool()
    
    user_query = message.text 
    if message.from_user.full_name:
        user_name = message.from_user.full_name
    else:
        user_name = message.from_user.first_name

    user_id = message.from_user.id
    try:
        response = query_and_prompt(user_query) 
        print("Iniciando a inserção de pergunta e resposta na tabela LLM_Bot.")
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                    INSERT INTO machine_learning.LLM_Bot (nome_user, pergunta_user, resposta_bot)
                    VALUES (%s, %s, %s)
                    """, (user_name, user_query, response))
                    await conn.commit()
                    print(f"Inserido pergunta e resposta para usuario {user_name}")
        except Exception as e:
            print(f"Erro no banco: {e}")

        # user_data[user_id] = {
        #     'user_query': user_query,
        #     'response': response
        # }
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id][message.message_id] = {
            'user_query': user_query,
            'response': response
        }

        rating_markup = InlineKeyboardMarkup(row_width=5)
        rating_markup.add(*[InlineKeyboardButton(f"⭐ {i}", callback_data=f"rating_{i}_{message.message_id}") for i in range(1, 6)])
        
        await message.reply(f"{response}", parse_mode='HTML', reply_markup=rating_markup)   
    except Exception as e:
        await message.reply(f"Ocorreu um erro ao processar sua consulta: {e}")


@dp.callback_query_handler(lambda callback_query: callback_query.data.startswith("rating_"))
async def handle_rating(callback_query: types.CallbackQuery):
    pool = await create_pool()
    data_parts = callback_query.data.split("_")
    rating = int(data_parts[1])
    message_id = int(data_parts[2]) # adição
    user_name = callback_query.from_user.full_name
    user_id = callback_query.from_user.id  

    #message_data = user_data.get(user_id)
    message_data = user_data.get(user_id, {}).get(message_id)
    
    if not message_data:
        await callback_query.answer("Dados não encontrados. Tente novamente.", show_alert=True)
        return
    
    pergunta = message_data['user_query']  
    resposta_bot = message_data['response']

    new_text = (
        f"{callback_query.message.text}\n\n"
        f"{user_name}, Obrigado por sua avaliação de {rating} estrela{'s' if rating > 1 else ''}! "
    )
     
    print('Iniciando a inserção de avaliação na tabela LLM_Bot.')
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                UPDATE machine_learning.LLM_Bot
                SET avaliacao = %s
                WHERE nome_user = %s
                AND pergunta_user = %s
                AND resposta_bot = %s
                """, (rating, user_name, pergunta, resposta_bot))
                
                await conn.commit()
                print(f"Inserido avaliação {rating} para pergunta {pergunta}")
                
    except Exception as e:
        print(f"Erro ao tentar inserir no banco: {e}")
                
    try:
        await callback_query.message.edit_text(new_text, parse_mode='HTML')
        await callback_query.answer()  
    except Exception as e:
        await callback_query.answer("Erro ao atualizar a mensagem. Tente novamente.", show_alert=True)

    await callback_query.answer(f"{user_name}, Obrigado por sua avaliação de {rating} estrela {'s' if rating > 1 else ''}!")







# Função para criar ou obter a coleção
def get_or_create_collection():
    try:
        collections = [c.name for c in client.list_collections()]  # Lista as coleções existentes
        if collection_name in collections:
            print(f"Carregando coleção existente: {collection_name}.")
            return client.get_collection(name=collection_name)  # Retorna a coleção existente
        else:
            print(f"Criando nova coleção: {collection_name}.")
            return client.create_collection(name=collection_name)  # Cria a coleção se não existir
    except Exception as e:
        print(f"Erro ao acessar ou criar a coleção: {e}")
        return None  # Retorna None caso haja erro
    
    
# Função para enviar resposta para o WhatsApp
def send_response_to_whatsapp(phone, message):
    evolution_api_url = "http://192.168.15.60:8080"  # Servidor de envio
    evolution_api_instance = "Suporte_BRG"
    evolution_api_key = "k3v14ilstiguaumoz8nzt"

    route = f"{evolution_api_url}/message/sendText/{evolution_api_instance}"
    headers = {
        'Content-Type': 'application/json',
        'apikey': evolution_api_key
    }
    payload = {
        "number": phone,
        "textMessage": {
            "text": message
        }
    }

    try:
        response = requests.post(route, json=payload, headers=headers)
        return response.json()  # Retorna a resposta da API
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

@app.post("/webhook")
async def receive_webhook(request: Request):
    try:
        # Parse da requisição
        data = await request.json()

        # Validar o evento do webhook
        if data.get("event") != "messages.upsert":
            return {"status": "error", "message": "Evento desconhecido"}

        # Extração de informações do webhook
        user_query = data.get("data", {}).get("message", {}).get("conversation")
        sender = data.get("data", {}).get("key", {}).get("remoteJid")

        if not user_query or not sender:
            return {"status": "error", "message": "Dados incompletos no webhook"}

        # Chamar a função de processamento
        response = query_and_prompt(user_query=user_query, top_k=5)

        # Validar a resposta antes de enviá-la
        if response is None:
            return {"status": "error", "message": "Erro ao processar a consulta"}

        # Enviar resposta ao WhatsApp
        send_response_to_whatsapp(sender, response)

        # Responder ao webhook
        return {"status": "success", "message": "Mensagem enviada", "data": response}

    except Exception as e:
        # Registrar e retornar erros inesperados
        print(f"Erro no webhook: {e}")
        return {"status": "error", "message": f"Erro interno: {e}"}






# Inicialização do FastAPI e Bot Telegram
async def main():
    # Configuração do servidor FastAPI
    config = Config(app=app, host="0.0.0.0", port=8000, loop="asyncio")
    server = Server(config)

    # Inicia o bot do Telegram e o FastAPI simultaneamente
    telegram_task = asyncio.create_task(dp.start_polling())
    fastapi_task = asyncio.create_task(server.serve())

    await asyncio.gather(telegram_task, fastapi_task)


if __name__ == "__main__":
    print("Inicializando o Webhook e o Bot Telegram...")
    asyncio.run(main())



# if __name__ == "__main__":
#     print("Bot iniciado! Aguardando mensagens...")
#     executor.start_polling(dp, skip_updates=True)


