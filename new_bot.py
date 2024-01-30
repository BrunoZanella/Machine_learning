from aiogram import Bot, Dispatcher, types
import asyncio
import mysql.connector
import atexit
import pandas as pd
from datetime import datetime, timedelta, time

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

TOKEN = "6959786383:AAF6Ob3oZcUf3C0zmAOjjIr8337cV5ZkJX4"
#bot_tele = telebot.TeleBot(TOKEN)

bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())

#updater = Updater(token=TOKEN, use_context=True)

#chat_ids = {}


cnx = mysql.connector.connect(
  host="192.168.15.104",
  user="root",
  password="gridbancoteste",
  database="sup_geral"
)

cursor = cnx.cursor(buffered=True)

def criar_tabela():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS usuarios_telegram (
        id INT AUTO_INCREMENT PRIMARY KEY,
        usuario VARCHAR(100),
        nome_supervisorio VARCHAR(100),
        nome_telegram VARCHAR(100),
        id_telegram BIGINT,
        bloqueado TINYINT DEFAULT 0,
        primeiro_acesso TINYINT DEFAULT 1,
        cod_usuario INT,
        data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cnx.commit()

def criar_tabela_leituras():
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS leituras_consecutivas (
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
    cnx.commit()


def selecionar_GMG():
    cursor.execute("SELECT codigo, ativo FROM tipos_equipamentos WHERE classe = 'GMG'")
    resultados = cursor.fetchall()
    codigos = []

    for resultado in resultados:
        codigos.append(resultado[0])
    
    return codigos


tabelas = 'leituras'

def obter_equipamentos_validos(tabelas):
    codigos_GMG = selecionar_GMG()
    codigos_GMG_str = ', '.join(map(str, codigos_GMG))

    query_equipamentos = f"SELECT DISTINCT codigo FROM equipamentos WHERE cod_tipo_equipamento IN ({codigos_GMG_str}) AND ativo = 1"
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

cod_equipamentos = obter_equipamentos_validos(tabelas)

cod_campo_especificados = ['3', '114']


async def id_chat_grupo():
    cursor.execute("SELECT id_telegram FROM usuarios_telegram WHERE usuario = 'Grupo'")
    result = cursor.fetchone()
    if result is not None:
        ID_DO_CHAT_DO_GRUPO = result[0]
        return ID_DO_CHAT_DO_GRUPO
    else:
        print("Nenhum grupo encontrado")
        return None

async def id_chat_usuario(username):
    cursor.execute("SELECT id_telegram FROM usuarios_telegram WHERE usuario = %s", (username,))
    result = cursor.fetchone()
    if result is not None:
        id_usuario = result[0]
        return id_usuario
    else:
        print("Usuário não encontrado")
        return None

async def enviar_menu_grupo(chat_id):
    cursor.execute("SELECT usuario FROM usuarios_telegram WHERE id_telegram = %s", (chat_id,))
    result = cursor.fetchone()
    if result is not None and result[0] == 'Grupo':
        keyboard = InlineKeyboardMarkup(row_width=1)
        buttons = [
            InlineKeyboardButton("Digitar cod_usina", callback_data='1'),
            InlineKeyboardButton("Digitar cod_equipamento", callback_data='2'),
            InlineKeyboardButton("Inserir usuario", callback_data='3'),
        ]
        keyboard.add(*buttons)
        await bot.send_message(chat_id, "Escolha uma opção:", reply_markup=keyboard)

async def criar_menu():
    menu_button = types.KeyboardButton('/menu')
    teclado_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
    teclado_menu.add(menu_button)
    return teclado_menu

@dp.message_handler(commands=['teste'])
async def teste_menu(message: types.Message):
    print('clicou no teste')
    chat_id = message.chat.id
    print(chat_id)

    cursor.execute("SELECT usuario FROM usuarios_telegram WHERE id_telegram = %s", (chat_id,))
    result = cursor.fetchone()
    print(result)

    if result is not None:
        username = result[0]
        print(username)
        
        timestamp = int(time.mktime(message.date.timetuple()))
        
        user_message = types.Message(message_id=message.message_id, from_user=message.from_user, chat=message.chat, date=timestamp, text=username)
        
        await enviar_previsao_valor_equipamento(user_message, username)
    else:
        print("Usuário não encontrado.")

@dp.message_handler(commands=['menu'])
async def menu(message: types.Message):
    chat_id = message.chat.id

    await enviar_menu_grupo(chat_id)

class Form(StatesGroup):
    cod_usina = State()
    cod_equipamento = State()
    usuario = State()
    ask_supervisor_name = State()

@dp.callback_query_handler(lambda query: True, state=Form.usuario)
async def process_usuario_callback(query: CallbackQuery, state: FSMContext):
    nome_telegram = query.data

    print(f"Clicou no usuário com nome_telegram: {nome_telegram}")

    await query.answer(f"Você escolheu o usuário: {nome_telegram}")

    await state.update_data(selected_user_nome_telegram=nome_telegram)

    await Form.ask_supervisor_name.set()
    await bot.send_message(query.message.chat.id, f"Insira o nome de usuário do supervisorio para {nome_telegram}?")

@dp.message_handler(state=Form.ask_supervisor_name)
async def process_supervisor_name(message: types.Message, state: FSMContext):
    data = await state.get_data()
    user_nome_telegram = data.get('selected_user_nome_telegram')

    supervisor_name = message.text

    user_input = supervisor_name.lstrip('/')

    cursor.execute("UPDATE usuarios_telegram SET usuario = %s WHERE nome_telegram = %s", (user_input, user_nome_telegram))
    cnx.commit()

    cursor.execute("SELECT id_telegram FROM usuarios_telegram WHERE nome_telegram = %s", (user_nome_telegram,))
    id_telegram_result = cursor.fetchone()

    cursor.execute("SELECT codigo, nome FROM usuarios WHERE login = %s", (user_input,))
    codigo_result = cursor.fetchone()

    if codigo_result:
        codigo, nome = codigo_result

        cursor.execute("UPDATE usuarios_telegram SET cod_usuario = %s, nome_supervisorio = %s WHERE nome_telegram = %s", (codigo, nome, user_nome_telegram))
        cnx.commit()
        
    response_message_grupo = f"A informação foi atualizada.\n O usuário do supervisorio para {user_nome_telegram} é: {user_input}"
    await bot.send_message(message.chat.id, response_message_grupo)

    if id_telegram_result:
        id_telegram = id_telegram_result[0]

        response_message = f"A informação foi atualizada.\n O usuário do supervisorio para {user_nome_telegram} é: {user_input}"
        await bot.send_message(id_telegram, response_message)

        await boas_vindas(message, id_telegram)
        await enviar_previsao_valor_equipamento(message, id_telegram)

        await state.finish()
    else:
        await bot.send_message(message.chat.id, "Erro ao obter o ID do usuário atualizado.")


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
        await bot.send_message(chat_id=query.message.chat.id, text="Digite o cod_usina. Insira o número com '/' antes.")
        await Form.cod_usina.set()
    elif query.data == '2':
        await bot.send_message(chat_id=query.message.chat.id, text="Digite cod_equipamento. Insira o número com '/' antes.")
        await Form.cod_equipamento.set()
    elif query.data == '3':
        await process_usuario(query.message, state)
        await bot.send_message(chat_id=query.message.chat.id, text="Digite o usuário. Insira o usuário com '/' antes.")
        await Form.usuario.set()



@dp.message_handler(state=Form.cod_usina)
async def process_cod_usina(message: types.Message, state: FSMContext):
    chat_id = await id_chat_grupo()
    user_input = message.text.lstrip('/')

    if user_input.isdigit():
        cod_usina = int(user_input)

        cursor.execute("SELECT nome, cod_cliente, cod_usuario FROM usinas WHERE codigo = %s", (cod_usina,))
        usina_result = cursor.fetchone()

        if usina_result:
            nome_usina, cod_cliente, cod_usuario = usina_result

            cursor.execute("SELECT codigo, nome FROM equipamentos WHERE cod_usina = %s", (cod_usina,))
            equipamentos_result = cursor.fetchall()

            equipamentos_message = ""
            for equipamento in equipamentos_result:
                codigo, nome_equipamento = equipamento
                equipamentos_message += f"Código: {codigo}, Nome: {nome_equipamento}\n"

            keyboard = InlineKeyboardMarkup()
            fazer_previsao_button = InlineKeyboardButton("Fazer Previsão", callback_data=f"fazer_previsao_usina_{cod_usina}")
            keyboard.add(fazer_previsao_button)

            response_message = (
                f"Nome da Usina: {nome_usina}\n"
                f"Código do Cliente: {cod_cliente}\n"
                f"Código do Usuário: {cod_usuario}\n\n"
                f"Equipamentos:\n"
                f"{equipamentos_message}"
            )

            await bot.send_message(chat_id, response_message, reply_markup=keyboard)

        else:
            await bot.send_message(chat_id, "Usina não encontrada.")
    else:
        await bot.send_message(chat_id, "Por favor, insira um número.")

    await state.finish()

@dp.message_handler(state=Form.cod_equipamento)
async def process_cod_equipamento(message: types.Message, state: FSMContext):

    chat_id = await id_chat_grupo()
    user_input = message.text.lstrip('/')

    if user_input.isdigit():
        cod_equipamento = int(user_input)

        cursor.execute("SELECT codigo, nome, cod_usina FROM equipamentos WHERE codigo = %s", (cod_equipamento,))
        equipamento_result = cursor.fetchone()

        if equipamento_result:
            codigo, nome_equipamento, cod_usina = equipamento_result

            cursor.execute("SELECT nome FROM usinas WHERE codigo = %s", (cod_usina,))
            usina_result = cursor.fetchone()

            if usina_result:
                nome_usina = usina_result[0]

                response_message = (
                    f"Código do Equipamento: {codigo}\n"
                    f"Nome do Equipamento: {nome_equipamento}\n"
                    f"Código da Usina: {cod_usina}\n"
                    f"Nome da Usina: {nome_usina}\n"
                )
                
                fazer_previsao_button_equipamento = InlineKeyboardButton("Fazer Previsão", callback_data=f'fazer_previsao_equipamento_{cod_equipamento}')
                keyboard = InlineKeyboardMarkup().add(fazer_previsao_button_equipamento)

                await bot.send_message(chat_id, response_message, reply_markup=keyboard)

            else:
                await bot.send_message(chat_id, "Usina não encontrada.")
        else:
            await bot.send_message(chat_id, "Equipamento não encontrado.")
    else:
        await bot.send_message(chat_id, "Por favor, insira um número.")

    await state.finish()

@dp.message_handler(state=Form.usuario)
async def process_usuario(message: types.Message, state: FSMContext):
    chat_id = await id_chat_grupo()

    cursor.execute("SELECT id, nome_supervisorio, nome_telegram FROM usuarios_telegram WHERE usuario = '0'")
    usuarios_result = cursor.fetchall()

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



@dp.message_handler(commands=['start'])
async def enviar_boas_vindas(message: types.Message):
    chat_id = message.chat.id
    chat_type = message.chat.type
    
    primeiro_acesso = None
    usuario = None

    cursor.execute("SELECT primeiro_acesso, usuario FROM usuarios_telegram WHERE id_telegram = %s", (chat_id,))
    result = cursor.fetchone()
    if result is not None:
        primeiro_acesso, usuario = result

        if primeiro_acesso == 1:

            if chat_type in ['group', 'supergroup']:
                cursor.execute("SELECT * FROM usuarios_telegram WHERE id_telegram = %s", (chat_id,))
                result = cursor.fetchone()
                if result is None:
                    cursor.execute("INSERT INTO usuarios_telegram (usuario, nome_supervisorio, nome_telegram, id_telegram,primeiro_acesso, cod_usuario) VALUES (%s, %s, %s, %s, %s, %s)", 
                                ('Grupo', '0', message.chat.title, chat_id, '1', '0'))
                    await message.reply("Grupo salvo com sucesso!\nUse o botão /menu abaixo para acessar o menu.", reply_markup=teclado_menu)

                else:
                    cursor.execute("UPDATE usuarios_telegram SET usuario = %s, nome_supervisorio = %s, nome_telegram = %s WHERE id_telegram = %s", 
                                ('Grupo', '0', message.chat.title, chat_id))
                    await message.reply("Grupo atualizado com sucesso!\nUse o botão /menu abaixo para acessar o menu.", reply_markup=teclado_menu)
                cnx.commit()
            else:
                await bot.send_message(chat_id, 'Olá! \nQual é o seu usuário no supervisório da BRG?\n                 <a href="https://supervisorio.brggeradores.com.br/beta/index.php">Acessar supervisório</a>', parse_mode='HTML')

        else:
            await message.reply(f"Bem-vindo de volta, {usuario}!\nSeus equipamentos estão sendo atualizados constantemente. Qualquer intervenção, você será notificado aqui.")
            cursor.execute("UPDATE usuarios_telegram SET bloqueado = 0 WHERE id_telegram = %s", (chat_id,))
            cnx.commit()
    else:
        if chat_type in ['group', 'supergroup']:
            cursor.execute("SELECT * FROM usuarios_telegram WHERE id_telegram = %s", (chat_id,))
            result = cursor.fetchone()
            if result is None:
                cursor.execute("INSERT INTO usuarios_telegram (usuario, nome_supervisorio, nome_telegram, id_telegram,primeiro_acesso, cod_usuario) VALUES (%s, %s, %s, %s, %s, %s)", 
                            ('Grupo', '0', message.chat.title, chat_id, '1', '0'))
                await message.reply("Grupo salvo com sucesso!\nDigite /menu para opções")
            else:
                cursor.execute("UPDATE usuarios_telegram SET usuario = %s, nome_supervisorio = %s, nome_telegram = %s WHERE id_telegram = %s", 
                            ('Grupo', '0', message.chat.title, chat_id))
                await message.reply("Grupo atualizado com sucesso!\nDigite /menu para opções")
            cnx.commit()
        else:
            await bot.send_message(chat_id, 'Olá! \nQual é o seu usuário no supervisório da BRG?\n                 <a href="https://supervisorio.brggeradores.com.br/beta/index.php">Acessar supervisório</a>', parse_mode='HTML')



async def boas_vindas(message: types.Message, id_telegram=None):
    user_input = message.text.lstrip('/')
    username = user_input
    chat_id = message.chat.id
    print(username)
    print(chat_id)
    
    cursor.execute("SELECT nome, codigo FROM usuarios WHERE login = %s", (username,))
    result = cursor.fetchone()

    if result is not None:
        nome_supervisorio, cod_usuario = result

        cursor.execute("SELECT primeiro_acesso FROM usuarios_telegram WHERE usuario = %s", (username,))
        primeiro_acesso = cursor.fetchone()[0]
        if primeiro_acesso == 1:
            cursor.execute("SELECT cod_usina FROM usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
            cod_usinas = cursor.fetchall()

            nomes_usinas = []

            for cod_usina_tuple in cod_usinas:
                cod_usina = cod_usina_tuple[0]
                cursor.execute("SELECT nome FROM usinas WHERE codigo = %s", (cod_usina,))
                nome_usina = cursor.fetchone()[0]

                nomes_usinas.append(f"{cod_usina} - {nome_usina}")

            nomes_usinas_str = '\n'.join(nomes_usinas)

            if id_telegram:
                await bot.send_message(id_telegram, f"Aqui está todas as usinas que {nome_supervisorio} está cadastrado:\n{nomes_usinas_str}")
            else:
                await bot.send_message(message.chat.id, f"Aqui está todas as usinas que {nome_supervisorio} está cadastrado:\n{nomes_usinas_str}")

        else:
            await message.reply("Usuário já existente.")
    else:
        await message.reply("Usuário não encontrado.")

@dp.message_handler()
async def save_username(message: types.Message):

    user_input = message.text.lstrip('/')
    username = user_input
    chat_type = message.chat.type

    full_name = f"{message.from_user.first_name} {message.from_user.last_name}"
    name = f"{message.from_user.first_name}"
    chat_id = message.chat.id

    cursor.execute("SELECT nome, codigo FROM usuarios WHERE login = %s", (username,))
    result = cursor.fetchone()
    if result is not None:
        nome_supervisorio, cod_usuario = result

        cursor.execute("SELECT * FROM usuarios_telegram WHERE id_telegram = %s", (chat_id,))
        result = cursor.fetchone()
        if result is None:
            query = "INSERT INTO usuarios_telegram (usuario, nome_supervisorio, nome_telegram, id_telegram, cod_usuario) VALUES (%s, %s, %s, %s, %s)"
            values = (username, nome_supervisorio, full_name, chat_id, cod_usuario)
            cursor.execute(query, values)
            await message.reply("Usuário, nome do supervisorio, nome do Telegram e código do usuário salvos com sucesso!")
        else:
            cursor.execute("UPDATE usuarios_telegram SET usuario = %s, nome_supervisorio = %s, nome_telegram = %s, cod_usuario = %s WHERE id_telegram = %s", 
                           (username, nome_supervisorio, full_name, cod_usuario, chat_id))
            await message.reply("Usuário atualizado com sucesso!")
        cnx.commit()

        await boas_vindas(message)
        await enviar_previsao_valor_equipamento(message)

    else:
        cursor.execute("SELECT * FROM usuarios_telegram WHERE nome_telegram = %s", (full_name,))
        result = cursor.fetchone()

        if result is not None:

            if chat_type in ['group', 'supergroup']:
                cursor.execute("SELECT * FROM usuarios_telegram WHERE id_telegram = %s", (chat_id,))
                result = cursor.fetchone()
                if result is None:
                    cursor.execute("SELECT data_cadastro FROM usuarios_telegram WHERE nome_telegram = %s", (full_name,))
                    data_cadastro = cursor.fetchone()[0]
                    cursor.execute("UPDATE usuarios_telegram SET usuario = %s, nome_supervisorio = %s, id_telegram = %s, data_cadastro = %s WHERE nome_telegram = %s", 
                                (0, username, chat_id, data_cadastro, full_name))
                    cnx.commit()
                    await message.reply("Usuário atualizado com sucesso!")
                else:
                    await message.reply("Voce nao pode editar o usuario aqui, se quiser editar, digite /menu!")

        else:
            query = "INSERT INTO usuarios_telegram (usuario, nome_telegram, nome_supervisorio, id_telegram) VALUES (%s, %s, %s, %s)"
            values = (0, full_name, username, chat_id)
            cursor.execute(query, values)
            cnx.commit()
            await message.reply("Nome do usuário não encotrado, enviado para a adminstração!")
            id_grupo = await id_chat_grupo()
            print(id_grupo)
            await bot.send_message(id_grupo, f'Usuario {full_name} de ID ({chat_id}) nao encontrado no banco de dados, digite /menu e insira manualmente')


def verificar_e_obter_coeficiente(cod_equipamento):
    coeficiente_existente = 0.0
    intercepto_existente = 0.0
    acuracia_existente = 0.0

    cursor.execute(f"SELECT * FROM coeficiente_geradores WHERE cod_equipamento = {cod_equipamento}")
    resultado = cursor.fetchone()

    if resultado is not None:
        coeficiente_existente = resultado[2]
        intercepto_existente = resultado[3]
        acuracia_existente = resultado[4]
        
    return coeficiente_existente, intercepto_existente


def fazer_previsao_sempre(valores_atuais, coeficiente, intercepto, cod_equipamento_resultado):

    contagem_limites = 4

    cursor.execute("SELECT data_cadastro FROM leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 3", (int(cod_equipamento_resultado),))
    data_cadastro = cursor.fetchone()[0]
    
    agora = datetime.now()

    if data_cadastro is not None and (agora - data_cadastro <= timedelta(days=1)):

        coeficiente_existente, intercepto_existente = verificar_e_obter_coeficiente(cod_equipamento_resultado)

        if valores_atuais is not None:
            previsoes = [(valor * coeficiente_existente + intercepto_existente) for valor in valores_atuais]
        else:
            previsoes = []
            
        previsoes = [round(valor, 1) for valor in previsoes]

        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 114", (int(cod_equipamento_resultado),))
        valores_atuais_114 = cursor.fetchone()

        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 3", (int(cod_equipamento_resultado),))
        valores_atuais_3 = cursor.fetchone()

        if valores_atuais_3 is None or 0 in valores_atuais_3:
            return previsoes, False
        
        limite_mais = 10
        limite_menos = -10

        contagem_acima_do_limite = 0
        contagem_abaixo_do_limite = 0

        print('*****************************************************************************************************************')
        for i, valor in enumerate(valores_atuais_114):
            if valor == 0 or previsoes[i] == 0:
                return previsoes, False
            
            limite_porcentagem_mais = round(0.15 * valor, 2)
            limite_porcentagem_menos = round(-0.15 * valor, 2)

            calculo = round(valor - previsoes[i], 2)

            if calculo < limite_menos:
                contagem_abaixo_do_limite +=1
                print(cod_equipamento_resultado,'         ABAIXO', ' ',i,'  valor',valor,'     previsoes',previsoes[i], '     calculo =',calculo, '     limite_menos', limite_menos, '     contagem_abaixo_do_limite', contagem_abaixo_do_limite)

            elif abs(calculo) > limite_mais:
                contagem_acima_do_limite += 1
                print(cod_equipamento_resultado,'         ACIMA', ' ',i,'  valor',valor,'     previsoes',previsoes[i], '     calculo =',calculo, '     limite_mais', limite_mais, '     contagem_acima_do_limite', contagem_acima_do_limite)

        '''
        if contagem_acima_do_limite > contagem_limites or contagem_abaixo_do_limite > contagem_limites:
            cursor.execute("""
            UPDATE leituras_consecutivas
            SET alerta = 1
            WHERE cod_equipamento = %s
            """, (int(cod_equipamento_resultado),))
            cnx.commit()
        elif contagem_acima_do_limite <= contagem_limites or contagem_abaixo_do_limite <= contagem_limites:
            cursor.execute("""
            UPDATE leituras_consecutivas
            SET alerta = 0
            WHERE cod_equipamento = %s
            """, (int(cod_equipamento_resultado),))
            cnx.commit()
        '''
        
        return previsoes, contagem_acima_do_limite > contagem_limites or contagem_abaixo_do_limite > contagem_limites

    else:
        return 'NOT UPDATED BRO', False


def fazer_previsao_sempre_alerta(valores_atuais, coeficiente, intercepto, cod_equipamento_resultado):

    contagem_limites = 4

    cursor.execute("SELECT data_cadastro FROM leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 3", (int(cod_equipamento_resultado),))
    data_cadastro = cursor.fetchone()[0]
    
    agora = datetime.now()

    if data_cadastro is not None and (agora - data_cadastro <= timedelta(hours=1)):

        coeficiente_existente, intercepto_existente = verificar_e_obter_coeficiente(cod_equipamento_resultado)

        if valores_atuais is not None:
            previsoes = [(valor * coeficiente_existente + intercepto_existente) for valor in valores_atuais]
        else:
            previsoes = []
            
        previsoes = [round(valor, 1) for valor in previsoes]

        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 114", (int(cod_equipamento_resultado),))
        valores_atuais_114 = cursor.fetchone()

        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 3", (int(cod_equipamento_resultado),))
        valores_atuais_3 = cursor.fetchone()

        if valores_atuais_3 is None or 0 in valores_atuais_3:
            return previsoes, False, False
        
        limite_mais = 10
        limite_menos = -10

        contagem_acima_do_limite = 0
        contagem_abaixo_do_limite = 0

        print('*****************************************************************************************************************')
        for i, valor in enumerate(valores_atuais_114):
            if valor == 0 or previsoes[i] == 0:
                return previsoes, False, False
            
            limite_porcentagem_mais = round(0.15 * valor, 2)
            limite_porcentagem_menos = round(-0.15 * valor, 2)

            calculo = round(valor - previsoes[i], 2)

            if calculo < limite_menos:
                contagem_abaixo_do_limite +=1
                print(cod_equipamento_resultado,'         ABAIXO', ' ',i,'  valor',valor,'     previsoes',previsoes[i], '     calculo =',calculo, '     limite_menos', limite_menos, '     contagem_abaixo_do_limite', contagem_abaixo_do_limite)

            elif abs(calculo) > limite_mais:
                contagem_acima_do_limite += 1
                print(cod_equipamento_resultado,'         ACIMA', ' ',i,'  valor',valor,'     previsoes',previsoes[i], '     calculo =',calculo, '     limite_mais', limite_mais, '     contagem_acima_do_limite', contagem_acima_do_limite)

        if contagem_acima_do_limite > contagem_limites or contagem_abaixo_do_limite > contagem_limites:
            cursor.execute("""
            UPDATE leituras_consecutivas
            SET alerta = 1
            WHERE cod_equipamento = %s
            """, (int(cod_equipamento_resultado),))
            cnx.commit()
        elif contagem_acima_do_limite <= contagem_limites or contagem_abaixo_do_limite <= contagem_limites:
            cursor.execute("""
            UPDATE leituras_consecutivas
            SET alerta = 0
            WHERE cod_equipamento = %s
            """, (int(cod_equipamento_resultado),))
            cnx.commit()
        
        return previsoes, contagem_abaixo_do_limite > contagem_limites, contagem_acima_do_limite > contagem_limites

    else:
        return 'NOT UPDATED BRO', False, False


def fazer_previsao(valores_atuais, coeficiente, intercepto, cod_equipamento_resultado):

    contagem_limites = 4

    cursor.execute("SELECT data_cadastro FROM leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 3", (int(cod_equipamento_resultado),))
    data_cadastro = cursor.fetchone()[0]
    
    agora = datetime.now()

    if data_cadastro is not None and (agora - data_cadastro <= timedelta(hours=1)):

        coeficiente_existente, intercepto_existente = verificar_e_obter_coeficiente(cod_equipamento_resultado)

        if valores_atuais is not None:
            previsoes = [(valor * coeficiente_existente + intercepto_existente) for valor in valores_atuais]
        else:
            previsoes = []
            
        previsoes = [round(valor, 1) for valor in previsoes]

        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 114", (int(cod_equipamento_resultado),))
        valores_atuais_114 = cursor.fetchone()

        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 3", (int(cod_equipamento_resultado),))
        valores_atuais_3 = cursor.fetchone()

        if valores_atuais_3 is None or 0 in valores_atuais_3:
            return previsoes, False
        
        limite_mais = 10
        limite_menos = -10

        contagem_acima_do_limite = 0
        contagem_abaixo_do_limite = 0


        print('*****************************************************************************************************************')
        for i, valor in enumerate(valores_atuais_114):
            if valor == 0 or previsoes[i] == 0:
                return previsoes, False
            
            limite_porcentagem_mais = round(0.15 * previsoes[i], 2)
            limite_porcentagem_menos = round(-0.15 * previsoes[i], 2)

            calculo = round(valor - previsoes[i], 2)

            if calculo < limite_menos:
                contagem_abaixo_do_limite +=1
                print('         ABAIXO')

            elif abs(calculo) > limite_mais:
                contagem_acima_do_limite += 1
                print('         ACIMA')

            print(cod_equipamento_resultado, ' ',i,'  valor',valor,'     previsoes',previsoes[i], '     calculo =',calculo, '     limite_porcentagem_menos', limite_porcentagem_menos, '     limite_porcentagem_mais', limite_porcentagem_mais, '     contagem_abaixo_do_limite', contagem_abaixo_do_limite, '     contagem_acima_do_limite', contagem_acima_do_limite)


        
        return previsoes, contagem_acima_do_limite > contagem_limites or contagem_abaixo_do_limite > contagem_limites

    else:
        return 0, False

async def processar_equipamentos(cod_equipamentos, tabelas, cod_campo_especificados):
    while True:
        for cod_equipamento in cod_equipamentos:
            valores = {cod: [0, 0, 0, 0, 0] for cod in cod_campo_especificados}
            try:
                query = f"SELECT data_cadastro, valor, cod_campo FROM {tabelas} WHERE cod_equipamento = {cod_equipamento} AND cod_campo IN ({', '.join(cod_campo_especificados)})"
                cursor.execute(query)
                resultados = cursor.fetchall()

                df = pd.DataFrame(resultados, columns=['data_cadastro', 'valor', 'cod_campo'])

                df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])

                df['valor'] = pd.to_numeric(df['valor'], errors='coerce')

                for cod in cod_campo_especificados:
                    valores_cod_campo = df[df['cod_campo'] == int(cod)]['valor'].values
                    valores[cod] = list(valores_cod_campo[-5:])[::-1] + valores[cod][:5-len(valores_cod_campo[-5:])]

                    cursor.execute(f"SELECT data_cadastro FROM {tabelas} WHERE cod_equipamento = {cod_equipamento} AND cod_campo = {cod}")
                    data_cadastro = cursor.fetchone()
                    if data_cadastro is not None:
                        data_cadastro = data_cadastro[0]
                        data_cadastro_formatada = data_cadastro.strftime('%Y-%m-%d %H:%M:%S')

                        cursor.execute(f"SELECT COUNT(*) FROM leituras_consecutivas WHERE cod_equipamento = {cod_equipamento} AND cod_campo = {cod} AND data_cadastro = '{data_cadastro_formatada}'")
                        count = cursor.fetchone()[0]
                        if count == 0:
                            cursor.execute(f"""
                            INSERT INTO leituras_consecutivas (cod_equipamento, cod_campo, valor_1, valor_2, valor_3, valor_4, valor_5, data_cadastro)
                            VALUES ({cod_equipamento}, {cod}, {valores[cod][4]}, {valores[cod][3]}, {valores[cod][2]}, {valores[cod][1]}, {valores[cod][0]}, '{data_cadastro_formatada}')
                            ON DUPLICATE KEY UPDATE
                            valor_1 = leituras_consecutivas.valor_2,
                            valor_2 = leituras_consecutivas.valor_3,
                            valor_3 = leituras_consecutivas.valor_4,
                            valor_4 = leituras_consecutivas.valor_5,
                            valor_5 = {valores[cod][0]},
                            data_cadastro = '{data_cadastro_formatada}'
                            """)
                            cnx.commit()
                    
            except Exception as e:
                print(f"Erro ao processar o equipamento {cod_equipamento}: {str(e)}")
        await asyncio.sleep(60)



async def processar_valores(cod_equipamentos, tabelas, cod_campo_especificados):
    while True:
        for cod_equipamento in cod_equipamentos:
            try:
                query = f"SELECT data_cadastro, valor, cod_campo FROM {tabelas} WHERE cod_equipamento = {cod_equipamento} AND cod_campo IN (3, 114)"

                cursor.execute(query)
                resultados = cursor.fetchall()

                df = pd.DataFrame(resultados, columns=['data_cadastro', 'valor', 'cod_campo'])

                df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])
                df['valor'] = pd.to_numeric(df['valor'], errors='coerce')

                query_valores = f"SELECT cod_equipamento, cod_campo, valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = {cod_equipamento} ORDER BY cod_equipamento, cod_campo"
                cursor.execute(query_valores)

                resultados_valores = cursor.fetchall()

                for resultado in resultados_valores:
                    cod_equipamento_resultado, cod_campo, valor_1, valor_2, valor_3, valor_4, valor_5 = resultado
                    if cod_campo == 3:

                        print('\n----------------------------------------------------------------------------------------------------------------\n')
                        print(f'cod_equipamento: {cod_equipamento_resultado}, cod_campo: {cod_campo}, valores: {valor_1}, {valor_2}, {valor_3}, {valor_4}, {valor_5}')

                        coeficiente = 0.0
                        intercepto = 0.0
                        previsoes = fazer_previsao_sempre([valor_1, valor_2, valor_3, valor_4, valor_5], coeficiente, intercepto, cod_equipamento_resultado)

                        print(f'Previsões: {previsoes}')
                    if cod_campo == 114:
                        print(f'cod_campo: {cod_campo}, valores: {valor_1}, {valor_2}, {valor_3}, {valor_4}, {valor_5}')

            except Exception as e:
                print(f"Erro ao ler o equipamento {cod_equipamento}: {str(e)}")
        await asyncio.sleep(10)


ultimos_alertas = {}
alertas_enviados_previsao = set()
async def enviar_previsao_valor_equipamento_alerta(cod_equipamentos, tabelas, cod_campo_especificados):
    usuarios_bloqueados = set()
    alertas_enviados_acima = set()
    
    while True:
        alertas_por_usina = {}
        for cod_equipamento in cod_equipamentos:

            cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = %s", (int(cod_equipamento),))
            valores_atuais = cursor.fetchone()
            
            cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (int(cod_equipamento),))
            valores_atuais_114 = cursor.fetchone()
            
            coeficiente_existente, intercepto_existente = verificar_e_obter_coeficiente(int(cod_equipamento))
            previsoes, alerta_abaixo, alerta_acima = fazer_previsao_sempre_alerta(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento))

            if cod_equipamento not in ultimos_alertas:
                ultimos_alertas[cod_equipamento] = []
            ultimos_alertas[cod_equipamento].append(int(alerta_abaixo or alerta_acima))
            if len(ultimos_alertas[cod_equipamento]) > 5:
                ultimos_alertas[cod_equipamento].pop(0)

            if len(ultimos_alertas[cod_equipamento]) < 5:
                print(f'\n{cod_equipamento} ultimos_alertas',ultimos_alertas[cod_equipamento])

            if len(ultimos_alertas[cod_equipamento]) == 5:
                media_alerta = sum(ultimos_alertas[cod_equipamento]) / len(ultimos_alertas[cod_equipamento])
                if media_alerta > 0.1:
                    print(f'\n{cod_equipamento} ultimos_alertas',ultimos_alertas[cod_equipamento])
                    print('media_alerta',media_alerta)
                if media_alerta == 1 and cod_equipamento not in alertas_enviados_previsao:

                    cursor.execute("SELECT nome, cod_usina, cod_usuario FROM equipamentos WHERE codigo = %s", (cod_equipamento,))
                    result = cursor.fetchone()
                    if result is not None:
                        nome, cod_usina, cod_usuario = result

                        valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                        equipamentos_str = f'\n\nValores Atuais: {valores_atuais_str} \nValores Previstos: {previsoes}'
                        if alerta_abaixo:
                            mensagem = f"Equipamento: {cod_equipamento} ({nome}): {equipamentos_str}\n\nAlerta: Load Speed abaixo do previsto\n\n"
                        elif alerta_acima:
                            mensagem = f"Equipamento: {cod_equipamento} ({nome}): {equipamentos_str}\n\nAlerta: Load Speed acima do previsto\n\n"

                        if cod_usina not in alertas_por_usina:
                            alertas_por_usina[cod_usina] = []
                        alertas_por_usina[cod_usina].append(mensagem)

                    alertas_enviados_previsao.add(cod_equipamento)
                    
                elif media_alerta == 0 and cod_equipamento in alertas_enviados_previsao:
                    mensagem_previsao = f"Equipamento: {cod_equipamento} ({nome}): {equipamentos_str}\n\nAlerta removido"
                    print('alerta removido')
                    await bot.send_message(id_grupo, mensagem_previsao)
                    alertas_enviados_previsao.remove(cod_equipamento)
                    
        for cod_usina, mensagens in alertas_por_usina.items():

            cursor.execute("SELECT nome FROM usinas WHERE codigo = %s", (cod_usina,))
            nome_usina = cursor.fetchone()[0]
            mensagem_final = f'<b>ALERTA!</b> \n\nUsina: {cod_usina} - {nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n' + ''.join(mensagens)

            cursor.execute("SELECT cod_usuario FROM usuarios_ext_usinas WHERE cod_usuario != 0 AND cod_usina = %s", (cod_usina,))
            cod_usuarios = cursor.fetchall()

            for cod_usuario_tuple in cod_usuarios:
                cod_usuario = cod_usuario_tuple[0]
                cursor.execute("SELECT id_telegram, usuario FROM usuarios_telegram WHERE cod_usuario = %s", (cod_usuario,))
                result = cursor.fetchone()
                if result is not None:
                    id_telegram, nome_usuario = result
                    try:
                        await bot.send_message(id_telegram, mensagem_final, parse_mode='HTML')
    
                        id_grupo = await id_chat_grupo()    
                        mensagem_final_grupo = f'<b>Enviada para {nome_usuario}!</b> \n\nUsina: {cod_usina} - {nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n' + ''.join(mensagens)
                        await bot.send_message(id_grupo, mensagem_final_grupo, parse_mode='HTML')

                        if cod_usuario in usuarios_bloqueados:
                            usuarios_bloqueados.remove(cod_usuario)
                    except BotBlocked:
                        if cod_usuario not in usuarios_bloqueados:
                            id_grupo = await id_chat_grupo()
                            if id_grupo is not None:
                                await bot.send_message(id_grupo, f"O bot foi bloqueado pelo usuário {nome_usuario} ({cod_usuario})")
                            cursor.execute("UPDATE usuarios_telegram SET bloqueado = 1 WHERE cod_usuario = %s", (cod_usuario,))
                            usuarios_bloqueados.add(cod_usuario)
                            
        tempo_inicial = datetime.now()
        data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')
        print("\nultima verificacao",data_cadastro_formatada,'\n')
        await asyncio.sleep(300)


ultimos_valores = {}
alertas_enviados = set()

async def enviar_alerta_80_100(cod_equipamentos, tabelas, cod_campo_especificados):
    usuarios_bloqueados = set()
    alertas_enviados_acima = set()

    while True:
        alertas_por_usina = {}
        for cod_equipamento in cod_equipamentos:

            cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5, data_cadastro FROM leituras_consecutivas WHERE cod_equipamento = %s AND cod_campo = 114", (int(cod_equipamento),))
            result = cursor.fetchone()
            valores_atuais_114 = result[:-1]
            data_cadastro = result[-1]

            agora = datetime.now()

            if agora - data_cadastro > timedelta(hours=12):
                continue

            alerta_80 = False
            alerta_100 = False

            for valor in valores_atuais_114:

                if 80 <= valor < 100:
                    alerta_80 = True

                elif valor == 100:
                    alerta_100 = True

            if cod_equipamento not in ultimos_valores:
                ultimos_valores[cod_equipamento] = []
            ultimos_valores[cod_equipamento].append(valor)
            if len(ultimos_valores[cod_equipamento]) > 5:
                ultimos_valores[cod_equipamento].pop(0)

            if len(ultimos_valores[cod_equipamento]) < 5:
                print(f'\n{cod_equipamento} ultimos_valores - ',ultimos_valores[cod_equipamento])
                
            if len(ultimos_valores[cod_equipamento]) == 5:
                media = sum(ultimos_valores[cod_equipamento]) / len(ultimos_valores[cod_equipamento])
                if media > 0.1:
                    cursor.execute("SELECT nome, cod_usina, cod_usuario FROM equipamentos WHERE codigo = %s", (cod_equipamento,))
                    result = cursor.fetchone()
                    if result is not None:
                        nome, cod_usina, cod_usuario = result
                        
                    print(f'\n{cod_equipamento} - {cod_usina} ultimos_valores - ',ultimos_valores[cod_equipamento])
                    print('media - ',media)
                if 80 <= media < 100 and cod_equipamento not in alertas_enviados:
                    alertas_enviados_acima.add(cod_equipamento)
                    alertas_enviados.add(cod_equipamento)

                    cursor.execute("SELECT nome, cod_usina, cod_usuario FROM equipamentos WHERE codigo = %s", (cod_equipamento,))
                    result = cursor.fetchone()
                    if result is not None:
                        nome, cod_usina, cod_usuario = result

                        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = %s", (int(cod_equipamento),))
                        valores_atuais = cursor.fetchone()

                        coeficiente_existente, intercepto_existente = verificar_e_obter_coeficiente(int(cod_equipamento))
                        previsoes, alerta = fazer_previsao(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento))

                        valores_atuais_str = ', '.join(map(str, valores_atuais_114))
                        equipamentos_str = f'\n\nValores Atuais: {valores_atuais_str} \nValores Previstos: {previsoes}\n\n'
                        mensagem = f"Equipamento: {cod_equipamento} ({nome}): {equipamentos_str}"

                        if alerta_80:
                            mensagem += "O load speed está acima de 80%.\n\n"
                        if alerta_100:
                            mensagem += "O load speed é 100%.\n\n"

                        if cod_usina not in alertas_por_usina:
                            alertas_por_usina[cod_usina] = []
                        alertas_por_usina[cod_usina].append(mensagem)

                elif media < 80 and cod_equipamento in alertas_enviados:
                    alertas_enviados.remove(cod_equipamento)
                    if cod_equipamento in alertas_enviados_acima:
                        mensagem_previsao = f"Equipamento: {cod_equipamento} ({nome}): {equipamentos_str}\n\nAlerta de load speed removido"
                        print('alerta removido de load speed')
                        await bot.send_message(id_grupo, mensagem_previsao)
                        alertas_enviados_acima.remove(cod_equipamento)

        for cod_usina, mensagens in alertas_por_usina.items():
            
            cursor.execute("SELECT nome FROM usinas WHERE codigo = %s", (cod_usina,))
            nome_usina = cursor.fetchone()[0]
            mensagem_final = f'<b>ALERTA!</b> \n\nUsina: {cod_usina} - {nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n' + ''.join(mensagens)

            cursor.execute("SELECT cod_usuario FROM usuarios_ext_usinas WHERE cod_usuario != 0 AND cod_usina = %s", (cod_usina,))
            cod_usuarios = cursor.fetchall()

            for cod_usuario_tuple in cod_usuarios:
                cod_usuario = cod_usuario_tuple[0]
                cursor.execute("SELECT id_telegram, usuario FROM usuarios_telegram WHERE cod_usuario = %s", (cod_usuario,))
                result = cursor.fetchone()
                if result is not None:
                    id_telegram, nome_usuario = result
                    try:
                        if len(mensagem_final) > 4096:
                            for i in range(0, len(mensagem_final), 4096):
                                await bot.send_message(id_telegram, mensagem_final[i:i+4096], parse_mode='HTML')
                        else:
                            await bot.send_message(id_telegram, mensagem_final, parse_mode='HTML')

                        id_grupo = await id_chat_grupo()    
                        mensagem_final_grupo = f'<b>Enviada para {nome_usuario}!</b> \n\nUsina: {cod_usina} - {nome_usina} \n\n <a href="https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina={cod_usina}">Ir para usina</a>\n\n' + ''.join(mensagens)
                        await bot.send_message(id_grupo, mensagem_final_grupo, parse_mode='HTML')

                        if cod_usuario in usuarios_bloqueados:
                            usuarios_bloqueados.remove(cod_usuario)

                    except BotBlocked:
                        if cod_usuario not in usuarios_bloqueados:
                            id_grupo = await id_chat_grupo()
                            if id_grupo is not None:
                                await bot.send_message(id_grupo, f"O bot foi bloqueado pelo usuário {nome_usuario} ({cod_usuario})")
                            cursor.execute("UPDATE usuarios_telegram SET bloqueado = 1 WHERE cod_usuario = %s", (cod_usuario,))
                            usuarios_bloqueados.add(cod_usuario)

        tempo_inicial = datetime.now()
        data_cadastro_formatada = tempo_inicial.strftime('%d-%m-%Y %H:%M')
        print("\nultima verificacao do enviar_alerta_80_100 ",data_cadastro_formatada,'\n')
        await asyncio.sleep(250)

async def enviar_cod_equipamento_usuario(message: types.Message, id_telegram=None):

    user_input = message.text.lstrip('/')
    username = user_input
    
    chat_id = message.chat.id

    cursor.execute("SELECT nome, codigo FROM usuarios WHERE login = %s", (username,))
    result = cursor.fetchone()
    if result is not None:
        nome_supervisorio, cod_usuario = result

        cursor.execute("SELECT primeiro_acesso FROM usuarios_telegram WHERE usuario = %s", (username,))
        primeiro_acesso = cursor.fetchone()[0]
        if primeiro_acesso == 1:
            cursor.execute("SELECT cod_usina FROM usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
            cod_usinas = cursor.fetchall()

            mensagem = f"Usinas cadastradas de {nome_supervisorio} e seus equipamentos:\n"

            for cod_usina_tuple in cod_usinas:
                cod_usina = cod_usina_tuple[0]
                cursor.execute("SELECT codigo FROM equipamentos WHERE cod_usina = %s", (cod_usina,))
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

    cursor.execute("SELECT nome, codigo FROM usuarios WHERE login = %s", (username,))
    result = cursor.fetchone()
    if result is not None:
        nome_supervisorio, cod_usuario = result

        cursor.execute("SELECT primeiro_acesso FROM usuarios_telegram WHERE usuario = %s", (username,))
        primeiro_acesso = cursor.fetchone()[0]
        if primeiro_acesso == 1:
            cursor.execute("SELECT cod_usina FROM usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
            cod_usinas = cursor.fetchall()

            mensagem = f"Usina de {nome_supervisorio} e seus equipamentos:\n\n"

            for cod_usina_tuple in cod_usinas:
                cod_usina = cod_usina_tuple[0]
                cursor.execute("SELECT codigo FROM equipamentos WHERE cod_usina = %s AND ativo = 1", (cod_usina,))
                cod_equipamentos = cursor.fetchall()

                equipamentos_lista = [str(resultado[0]) for resultado in cod_equipamentos]

                for equipamento in equipamentos_lista:
                    cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = %s", (int(equipamento),))
                    valores_atuais = cursor.fetchone()
                    if valores_atuais is not None:
                        coeficiente_existente, intercepto_existente = verificar_e_obter_coeficiente(int(equipamento))
                        previsoes, alerta = fazer_previsao(valores_atuais, coeficiente_existente, intercepto_existente, int(equipamento))

                        cursor.execute("SELECT valor FROM leituras WHERE cod_equipamento = %s AND cod_campo = 114 ORDER BY data_cadastro DESC LIMIT 1", (int(equipamento),))
                        valor_real = cursor.fetchone()

                        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (int(equipamento),))
                        valores_atuais_114 = cursor.fetchone()
                        
                        if valor_real is not None:
                            valor_real = valor_real[0]
                        else:
                            valor_real = 'N/A'

                        valores_atuais_str = ', '.join(map(str, valores_atuais_114))

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

            cursor.execute("UPDATE usuarios_telegram SET primeiro_acesso = 0 WHERE usuario = %s", (username,))
            cnx.commit()

        else:
            if message.text == 'bruno.zanella':
                cursor.execute("SELECT cod_usina FROM usuarios_ext_usinas WHERE cod_usuario = %s", (cod_usuario,))
                cod_usinas = cursor.fetchall()
                mensagem_total_equipamentos = ""

                mensagem = f"Usina de {nome_supervisorio} e seus equipamentos:\n\n"
                total_equipamentos = 0

                for cod_usina_tuple in cod_usinas:
                    cod_usina = cod_usina_tuple[0]
                    
                    codigos_GMG = selecionar_GMG()

                    placeholders = ', '.join(['%s'] * len(codigos_GMG))
                    query = f"SELECT codigo FROM equipamentos WHERE cod_usina = %s AND cod_tipo_equipamento IN ({placeholders}) AND ativo = 1"
                    cursor.execute(query, [cod_usina] + list(codigos_GMG))

                    cod_equipamentos = cursor.fetchall()

                    equipamentos_lista = [str(resultado[0]) for resultado in cod_equipamentos]

                    for equipamento in equipamentos_lista:
                        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = %s", (int(equipamento),))
                        valores_atuais = cursor.fetchone()
                        if valores_atuais is not None:
                            if all(valor != 0 for valor in valores_atuais):
                                coeficiente_existente, intercepto_existente = verificar_e_obter_coeficiente(int(equipamento))
                                previsoes,alerta = fazer_previsao(valores_atuais, coeficiente_existente, intercepto_existente, int(equipamento))

                                if isinstance(previsoes, int):
                                    if previsoes != 0:
                                        total_equipamentos += 1
                            
                                        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (int(equipamento),))
                                        valores_atuais_114 = cursor.fetchone()

                                        valores_atuais_str = ', '.join(map(str, valores_atuais_114))

                                        equipamentos_str = f'\n\nValores Atuais: {valores_atuais_str} \nValores Previstos: {previsoes} \n{alerta}'

                                        mensagem += f"                   Usina: {cod_usina} - Equipamento: {equipamento}: {equipamentos_str}\n\n"

                                elif isinstance(previsoes, list):
                                    if not all(valor == 0 for valor in previsoes):
                                        total_equipamentos += 1
                    
                                        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (int(equipamento),))
                                        valores_atuais_114 = cursor.fetchone()

                                        valores_atuais_str = ', '.join(map(str, valores_atuais_114))

                                        equipamentos_str = f'\n\nValores Atuais: {valores_atuais_str} \nValores Previstos: {previsoes} \n{alerta}'

                                        mensagem += f"                   Usina: {cod_usina} - Equipamento: {equipamento}: {equipamentos_str}\n\n"


                cursor.execute("SELECT id_telegram FROM usuarios_telegram WHERE usuario = %s", (username,))
                id_telegram = cursor.fetchone()[0]
        
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
                await message.reply("Usuário já logado.")
        
            mensagem_total_equipamentos += f"\n\nTotal de equipamentos: {total_equipamentos} operando"
            print(f"\n\nTotal de equipamentos: {total_equipamentos} operando")
            await bot.send_message(chat_id, mensagem_total_equipamentos)

    else:
        await message.reply("Usuário não encontrado.")


async def enviar_previsao_valor_usina_menu(cod_usina, chat_id=None):
    codigos_GMG = selecionar_GMG()

    placeholders = ', '.join(['%s'] * len(codigos_GMG))
    query = f"SELECT codigo FROM equipamentos WHERE cod_usina = %s AND cod_tipo_equipamento IN ({placeholders}) AND ativo = 1"
    cursor.execute(query, [cod_usina] + list(codigos_GMG))

    cod_equipamentos = cursor.fetchall()
    equipamentos_lista = [str(resultado[0]) for resultado in cod_equipamentos]

    mensagem = f"Usina: {cod_usina} - Previsões para Equipamentos:\n\n"

    for equipamento in equipamentos_lista:
        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = %s", (equipamento,))
        valores_atuais = cursor.fetchone()

        cursor.execute("SELECT nome FROM equipamentos WHERE codigo = %s", (int(equipamento),))
        nome_equipamento = cursor.fetchone()[0]
        
        if valores_atuais is None:
            mensagem += f"Equipamento: {equipamento} ({nome_equipamento}) - Valores não encontrados na base de dados da tabela leituras.\n\n"
            continue

        coeficiente_existente, intercepto_existente = verificar_e_obter_coeficiente(equipamento)
        previsoes, alerta = fazer_previsao_sempre(valores_atuais, coeficiente_existente, intercepto_existente, int(equipamento))

        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (int(equipamento),))
        valores_atuais_114 = cursor.fetchone()

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
    cursor.execute("SELECT nome, cod_usina FROM equipamentos WHERE codigo = %s AND ativo = 1", (cod_equipamento,) )
    equipamento_info = cursor.fetchone()

    if equipamento_info:
        nome_equipamento, cod_usina = equipamento_info

        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_equipamento = %s", (cod_equipamento,))
        valores_atuais = cursor.fetchone()

        cursor.execute("SELECT valor_1, valor_2, valor_3, valor_4, valor_5 FROM leituras_consecutivas WHERE cod_campo = 114 AND cod_equipamento = %s", (cod_equipamento,))
        valores_atuais_114 = cursor.fetchone()
            
        if valores_atuais:
            coeficiente_existente, intercepto_existente = verificar_e_obter_coeficiente(cod_equipamento)
            previsoes, alerta = fazer_previsao_sempre(valores_atuais, coeficiente_existente, intercepto_existente, int(cod_equipamento))

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


def close_db():
    cursor.close()
    cnx.close()
    print('fechando conexao com o banco')

atexit.register(close_db)



if __name__ == '__main__':
    from aiogram import executor
    criar_tabela_leituras()
    criar_tabela()
    
    loop = asyncio.get_event_loop()
    loop.create_task(processar_equipamentos(cod_equipamentos, tabelas, cod_campo_especificados))
    loop.create_task(enviar_previsao_valor_equipamento_alerta(cod_equipamentos, tabelas, cod_campo_especificados))
    loop.create_task(enviar_alerta_80_100(cod_equipamentos, tabelas, cod_campo_especificados))

    teclado_menu = loop.run_until_complete(criar_menu())

    executor.start_polling(dp, skip_updates=True)