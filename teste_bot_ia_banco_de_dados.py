# Chat with an intelligent assistant in your terminal
from openai import OpenAI

import logging
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.contrib.middlewares.logging import LoggingMiddleware
import aiomysql
import asyncio
import google.generativeai as genai
import re
import spacy
from aiogram.types import ParseMode
from aiogram.utils import executor


# Carregando o modelo SpaCy para Português
nlp = spacy.load('pt_core_news_sm')

# Configurações
TOKEN = "6674960909:AAEf4Ky64lM6NILX3wZ6HJEASTt42Q2vopc"
API_KEY = 'AIzaSyDf9hqXZvxOiCKaFSiIa0byrfEctP5mflI'

genai.configure(api_key=API_KEY)
# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração do bot
bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())

async def create_pool():
    return await aiomysql.create_pool(
        host="192.168.4.50",
        user="bot_consultas",
        password="@ssistente_2024",
        db="machine_learning",
        autocommit=True
    )


# Point to the local server
client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")

history = [
    {"role": "system", "content": "Você é um assistente inteligente. Você sempre fornece respostas bem fundamentadas que são corretas e úteis."},
    {"role": "user", "content": "Olá, apresente-se a alguém que está abrindo este programa pela primeira vez. Seja conciso."},
]

while True:
    completion = client.chat.completions.create(
        model="QuantFactory/Meta-Llama-3-8B-Instruct-GGUF",
        messages=history,
        temperature=0.7,
        stream=True,
    )

    new_message = {"role": "assistant", "content": ""}
    
    for chunk in completion:
        if chunk.choices[0].delta.content:
            print(chunk.choices[0].delta.content, end="", flush=True)
            new_message["content"] += chunk.choices[0].delta.content

    history.append(new_message)
    
    # Uncomment to see chat history
    # import json
    # gray_color = "\033[90m"
    # reset_color = "\033[0m"
    # print(f"{gray_color}\n{'-'*20} History dump {'-'*20}\n")
    # print(json.dumps(history, indent=2))
    # print(f"\n{'-'*55}\n{reset_color}")

    print()
    history.append({"role": "user", "content": input("> ")})