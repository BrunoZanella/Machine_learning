import mysql.connector
from telegram import Bot
from aiogram import Bot, types, exceptions
import asyncio
from datetime import datetime, timedelta, time
#from ler_coef_bot import *

inicio = datetime.now()

TOKEN = "6959786383:AAF6Ob3oZcUf3C0zmAOjjIr8337cV5ZkJX4"
#CHAT_ID = "6870017547"
ID_DO_CHAT_DO_GRUPO = "-4028077570"

bot = Bot(token=TOKEN)

# Armazena os IDs de chat e nomes
chat_ids = {}

# Lista de números de usinas para os quais enviar mensagens
usinas_para_enviar = ['434']


# Conexão com o banco de dados
cnx = mysql.connector.connect(
  host="192.168.15.104",
  user="root",
  password="gridbancoteste",
  database="sup_geral"
)

cursor = cnx.cursor()



# Função para criar a tabela
def criar_tabela():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS usuarios_telegram (
        usuario VARCHAR(255),
        descricao VARCHAR(255),
        id_telegram BIGINT,
        bloqueado TINYINT DEFAULT 0,
        usina INT,
        data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cnx.commit()



# Função para inserir um usuário ou grupo
def inserir_usuario(usuario, descricao, id_telegram, usina, is_group=False):
    # Se for um grupo, define usina como 0
    if is_group:
        usina = 0

    # Verifica se o usuário já existe
    cursor.execute("SELECT * FROM usuarios_telegram WHERE id_telegram = %s", (id_telegram,))
    if cursor.fetchone() is not None:
        print(f"Usuário {usuario} já existe, não inserindo")
        return

    # Se o usuário não existir, insere no banco de dados
    query = "INSERT INTO usuarios_telegram (usuario, descricao, id_telegram, usina) VALUES (%s, %s, %s, %s)"
    values = (usuario, descricao, id_telegram, usina)
    cursor.execute(query, values)
    cnx.commit()




# Função para ler a tabela
def ler_tabela():
    cursor.execute("SELECT * FROM usuarios_telegram")
    return cursor.fetchall()


async def get_updates():
    updates = await bot.get_updates()
    for update in updates:
        if update.message is not None:
            chat_id = update.message.chat.id
            chat_type = update.message.chat.type
            full_name = None
            # Verifica se o ID do chat já foi impresso
            if chat_id not in chat_ids:
                user = update.message.from_user
                if chat_type in ['group', 'supergroup']:
                    full_name = 'Grupo'
                    descricao = update.message.chat.title
                    print(f"ID do chat: {chat_id}, Nome do Grupo: {full_name}, Descricao {descricao}")
                    inserir_usuario(full_name, descricao, chat_id, 0, is_group=True)  # Insere o grupo no banco de dados
                elif user.first_name and user.last_name:
                    full_name = f"{user.first_name} {user.last_name}"
                    print(f"ID do chat: {chat_id}, Nome: {full_name}")
                    inserir_usuario(full_name, '', chat_id, 434)  # Insere o usuário no banco de dados
                else:
                    print(f"ID do chat: {chat_id}, Nome: {user.first_name}")
                    inserir_usuario(user.first_name, '', chat_id, 434)  # Insere o usuário no banco de dados
                chat_ids[chat_id] = full_name


# Função para buscar equipamentos
def buscar_equipamentos(usina):
    cursor.execute("SELECT nome, codigo FROM equipamentos WHERE cod_usina = %s", (usina,))
    return cursor.fetchall()


async def send_welcome_messages():
    try:
        for usina in usinas_para_enviar:
            # Busca os IDs de chat para a usina
            cursor.execute("SELECT id_telegram, usuario FROM usuarios_telegram WHERE usina = %s", (usina,))
            usuarios = cursor.fetchall()

            # Busca os equipamentos para a usina
            equipamentos = buscar_equipamentos(usina)

            for usuario in usuarios:
                chat_id = usuario[0]
                full_name = usuario[1]

                # Cria a mensagem com as informações dos equipamentos
                mensagem = f"Bem-vindo, {full_name}!\nAqui estão os equipamentos para a sua usina: {usina}\n"
                mensagem_grupo = f"Enviado para, {full_name}!\nOs equipamentos da usina: {usina}\n"
                for equipamento in equipamentos:
                    nome = equipamento[0]
                    codigo = equipamento[1]
                    mensagem += f"Nome: {nome}, Código: {codigo}\n"
                    mensagem_grupo += f"Nome: {nome}, Código: {codigo}\n"

                # Envia a mensagem para o usuário
                await bot.send_message(chat_id=chat_id, text=mensagem)

                # Envia uma cópia da mensagem para o grupo
                await bot.send_message(chat_id=ID_DO_CHAT_DO_GRUPO, text=mensagem_grupo)

                cursor.execute("UPDATE usuarios_telegram SET bloqueado = 0 WHERE id_telegram = %s", (chat_id,))
                cnx.commit()
                
    except Exception as e:
        if 'Forbidden: bot was blocked by the user' in str(e):
            print(f"Erro: Bot bloqueado pelo usuário de ID: {chat_id}")  # Use chat_id aqui
            cursor.execute("UPDATE usuarios_telegram SET bloqueado = 1 WHERE id_telegram = %s", (chat_id,))  # E aqui
            cnx.commit()
        else:
            print(e)




async def main():
    criar_tabela()  # Cria a tabela se ela não existir
    await get_updates()
    await send_welcome_messages()
    await bot.session.close()

    # Fecha a conexão com o banco de dados
    cnx.close()
    cursor.close()

asyncio.run(main())

fim = datetime.now()

tempo_total = fim - inicio
print(tempo_total)