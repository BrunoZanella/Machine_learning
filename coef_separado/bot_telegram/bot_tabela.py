import mysql.connector
from telegram import Bot
from aiogram import Bot, types, exceptions
import asyncio

TOKEN = "6959786383:AAF6Ob3oZcUf3C0zmAOjjIr8337cV5ZkJX4"
#CHAT_ID = "6870017547"

bot = Bot(token=TOKEN)

# Armazena os IDs de chat e nomes
chat_ids = {}

# Lista de números de usinas para os quais enviar mensagens
usinas_para_enviar = [434]


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
        id_telegram BIGINT,
        bloqueado TINYINT DEFAULT 0,
        usina INT,
        data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)



# Função para inserir um usuário
def inserir_usuario(usuario, id_telegram, usina):
    # Verifica se o usuário já existe
    cursor.execute("SELECT * FROM usuarios_telegram WHERE id_telegram = %s", (id_telegram,))
    if cursor.fetchone() is not None:
        print(f"Usuário {usuario} já existe, não inserindo")
        return

    # Se o usuário não existir, insere no banco de dados
    query = "INSERT INTO usuarios_telegram (usuario, id_telegram, usina) VALUES (%s, %s, %s)"
    values = (usuario, id_telegram, usina)
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
            # Verifica se o ID do chat já foi impresso
            if chat_id not in chat_ids:
                user = update.message.from_user
                if user.first_name and user.last_name:
                    full_name = f"{user.first_name} {user.last_name}"
                    print(f"ID do chat: {chat_id}, Nome: {full_name}")
                    inserir_usuario(full_name, chat_id, 434)  # Insere o usuário no banco de dados
                else:
                    print(f"ID do chat: {chat_id}, Nome: {user.first_name}")
                    inserir_usuario(user.first_name, chat_id, 434)  # Insere o usuário no banco de dados
                chat_ids[chat_id] = full_name


async def send_welcome_messages():
    try:
        for usina in usinas_para_enviar:
            # Busca os IDs de chat para a usina
            cursor.execute("SELECT id_telegram, usuario FROM usuarios_telegram WHERE usina = %s", (usina,))
            usuarios = cursor.fetchall()
            
            for usuario in usuarios:
                chat_id = usuario[0]
                full_name = usuario[1]
                
                await bot.send_message(chat_id=chat_id, text=f"Bem-vindo, {full_name}!")
                # Se a mensagem foi enviada com sucesso, atualiza 'bloqueado' para 0
                cursor.execute("UPDATE usuarios_telegram SET bloqueado = 0 WHERE id_telegram = %s", (chat_id,))
                cnx.commit()
    except Exception as e:
        if 'Forbidden: bot was blocked by the user' in str(e):
            print(f"Erro: Bot bloqueado pelo usuário de ID: {chat_id}")  # Use chat_id aqui
            # Se o bot foi bloqueado pelo usuário, atualiza 'bloqueado' para 1
            cursor.execute("UPDATE usuarios_telegram SET bloqueado = 1 WHERE id_telegram = %s", (chat_id,))  # E aqui
            cnx.commit()
        else:
            print(e)



async def main():
    criar_tabela()  # Cria a tabela se ela não existir
    await get_updates()
    await send_welcome_messages()
    await bot.session.close()

asyncio.run(main())
