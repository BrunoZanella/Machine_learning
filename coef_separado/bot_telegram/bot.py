
''' 

from telegram import Bot
import telebot
from aiogram import Bot, types, exceptions
import asyncio
from telebot.apihelper import ApiTelegramException

TOKEN = "6959786383:AAF6Ob3oZcUf3C0zmAOjjIr8337cV5ZkJX4"
CHAT_ID = "6870017547"

bot = Bot(token=TOKEN)

# Armazena os IDs de chat
chat_ids = set()

async def send_message():
    try:
        await bot.send_message(chat_id=CHAT_ID, text="Olá, mundo!")
    except Exception as e:
        if 'Forbidden: bot was blocked by the user' in str(e):
            print(f"Erro: Bot bloqueado pelo usuário de ID: {CHAT_ID}")
        else:
            print(e)


        
async def get_updates():
    updates = await bot.get_updates()
    for update in updates:
        if update.message is not None:
            chat_id = update.message.chat.id
            # Verifica se o ID do chat já foi impresso
            if chat_id not in chat_ids:
                user = update.message.from_user
                if user.first_name and user.last_name:
                    full_name = f"{user.first_name} {user.last_name}"
                    print(f"ID do chat: {chat_id}, Nome: {full_name}")
                else:
                    print(f"ID do chat: {chat_id}, Nome: {user.first_name}")
                chat_ids.add(chat_id)


async def main():
    await send_message()
    await get_updates()
    await bot.session.close()

asyncio.run(main())


'''
#manda mensagem para todos ids que mandaram /start

from telegram import Bot
import telebot
from aiogram import Bot, types, exceptions
import asyncio
from telebot.apihelper import ApiTelegramException

TOKEN = "6959786383:AAF6Ob3oZcUf3C0zmAOjjIr8337cV5ZkJX4"
CHAT_ID = "6870017547"

bot = Bot(token=TOKEN)

# Armazena os IDs de chat e nomes
chat_ids = {}

async def get_updates():
    updates = await bot.get_updates()
    for update in updates:
        if update.message is not None:
            chat_id = update.message.chat.id
            # Verifica se o ID do chat já foi impresso
            if chat_id not in chat_ids:
                user = update.message.from_user
                if user.first_name and user.last_name:
                    full_name = f"{user.first_name} {user.last_name}"
                    print(f"ID do chat: {chat_id}, Nome: {full_name}")
                else:
                    print(f"ID do chat: {chat_id}, Nome: {user.first_name}")
                chat_ids[chat_id] = full_name
                
async def send_welcome_messages():

    try:
        for chat_id, full_name in chat_ids.items():
            await bot.send_message(chat_id=chat_id, text=f"Bem-vindo, {full_name}!")
    except Exception as e:
        if 'Forbidden: bot was blocked by the user' in str(e):
            print(f"Erro: Bot bloqueado pelo usuário de ID: {CHAT_ID}")
        else:
            print(e)
            

async def main():
    await get_updates()
    await send_welcome_messages()
    await bot.session.close()

asyncio.run(main())

''' '''