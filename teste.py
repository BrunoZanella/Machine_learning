import logging
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.contrib.middlewares.logging import LoggingMiddleware
import aiomysql
import asyncio
import google.generativeai as genai
import re

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
        password="@ssistente_2024"
    )



    
# Tabelas específicas para consultar
tables = {
    "machine_learning": [
        "coeficiente_geradores",
        "falhas_gerais",
        "leituras_consecutivas",
        "log_relatorio_quebras",
        "telegram_silenciar_bot",
        "usuarios_telegram",
        "valores_previsao"
    ],
    "sup_geral": [
        "lista_alarmes",
        "alarmes_ativos",
        "campos",
        "equipamentos",
        "campos",
        "clientes",
        "usinas",
        "usuarios",
        "leituras",
    ]
}

# Função para obter as colunas das tabelas
async def get_columns_for_tables():
    pool = await create_pool()
    tabelas_colunas = {}
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for schema, table_list in tables.items():
                    tabelas_colunas[schema] = {}
                    for table in table_list:
                        await cur.execute(f"USE {schema};")
                        await cur.execute(f"SHOW COLUMNS FROM {table};")
                        columns = await cur.fetchall()
                        column_names = [col[0] for col in columns]
                        tabelas_colunas[schema][table] = column_names
    except Exception as e:
        logger.error(f"Erro ao encontrar a tabela ou coluna no banco de dados: {str(e)}")
    finally:
        pool.close()
        await pool.wait_closed()
    
    return tabelas_colunas

# Função para dividir o relatório em partes de no máximo 4096 caracteres
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

# Função para manter o histórico de chat
chat_history = {}

# Função para responder no Telegram
@dp.message_handler()
async def handle_message(message: types.Message):
    query = message.text
    chat_id = message.chat.id

    # Configuração do modelo Gemini
    model = genai.GenerativeModel('gemini-1.5-flash-latest')
    chat = model.start_chat(history=[])

    # Inicializa o histórico de chat se ainda não existir
    if chat_id not in chat_history:
        chat_history[chat_id] = []

    # Adiciona a mensagem do usuário ao histórico
    chat_history[chat_id].append({"role": "user", "content": query})
    
    tabelas_colunas = await get_columns_for_tables()

    nome_tabela_context = (
        "Você é um assistente que ajuda com consultas a um banco de dados. "
        f"Interprete a pergunta do usuário, determine o nome das tabelas apropriadas de acordo com a {query} e pesquise os dados de acordo com a pergunta vendo se tem algum nome de coluna que possa ajudar a procurar em {tabelas_colunas}."
        "Sempre coloque a tabela usinas e equipamentos"
        "Inclua apenas os nomes das tabelas mais apropriadas geradas na resposta."
        "Se for mais de uma tabela, entregue o resultado enumerando as ordens das tabelas solicitadas. Apenas coloque o número um traço e o nome da tabela e mais nada, seja direto e entregue somente esse dado."
        "Caso não tenha nenhuma tabela na pergunta, entregue null."
    )
    prompt_tabela = f"{nome_tabela_context}\n\nPergunta: {query}"
    response_tabela = chat.send_message(prompt_tabela)
    nome_tabela = response_tabela.text.strip()

    schema_context = (
        "Você é um assistente que ajuda com consultas a um banco de dados. "
        f"Interprete a pergunta do usuário, determine os nomes dos schemas apropriados de acordo com a {query}, usando a resposta de {nome_tabela} para pesquisar os dados em {tabelas_colunas}."
        "Inclua apenas os schemas dos bancos mais apropriados gerados na resposta."
        "Para as tabelas usinas e equipamentos o schema é sup_geral"
        "Se for mais de um schema, entregue o resultado enumerando as ordens dos schemas solicitadas. Apenas coloque o número um traço e o nome do schema e mais nada, seja direto e entregue somente esse dado."
        "Caso não tenha um schema na pergunta, entregue null."
    )
    prompt_schema = f"{schema_context}\n\nPergunta: {query}"
    response_schema = chat.send_message(prompt_schema)
    nome_schema = response_schema.text.strip()

    db_names = [schema.split('- ')[1].strip() for schema in nome_schema.split('\n') if '- ' in schema]
    table_names = [table.split('- ')[1].strip() for table in nome_tabela.split('\n') if '- ' in table]
    print('\ndb_names',db_names,'table_names',table_names)

    reports = []
    try:
        resultado_nomes_str = []
        for db_name in db_names:
            for table_name in table_names:
                if db_name in tabelas_colunas and table_name in tabelas_colunas[db_name]:
                    column_names = tabelas_colunas[db_name][table_name]

                    column_in_query = None
                    for col in column_names:
                        if col.lower() in query.lower():
                            column_in_query = col
                            break

                    if column_in_query:
                        nomes_context = (
                            f"""
                            Você é um assistente que ajuda com consultas a um banco de dados. 
                            Nunca invente dado. 
                            Retorne no máximo 10 resultados. 
                            Use DISTINCT.
                            Tente sempre fazer um contador.
                            Sempre tente trazer o nome e o codigo da pergunta de {query}
                            Use o schema {db_name} e a tabela {table_name}. 
                            Use {tabelas_colunas} para os nomes das colunas para pesquisar a resposta de {query}. 
                            procure sempre por colunas ativo = 1, se existir, filtre.
                            Nao retorne valores zeros.
                            Preste atenção para fazer a consulta correta para o schema correto e a tabela correta. 
                            Cuidado com a sintaxe, veja os nomes corretos de cada coluna para cada tabela e schema.
                            Se pedir nome de algo, sempre pesquise por nomes parecidos com o da pergunta, como maiusculo, minusculo, acentos...
                            Se nao encontrar o nome, procure por nomes parecidos, proximos do escrito, como por exemplo:
                            - Fazenda = faz.
                            - Santa = Sta.
                            - santo = Sto. ou St.
                            - caso escreva o nome e nao tenha, como isabel, porem no banco so tem izabel, faça a relacao de procurar nomes com partes do nome parecido e dizer que nao encontrou o nome X porem o nome Y pode ser a opção.
                            se nao encontrar o nome, procure parecidos e de as opções de sugestão.
                            Caso tenha mais de uma, mostre as opções e diga no plural juntamente com o nome caso tenha
                            Caso pergunte de alerta, alerta deve ser 1 para alerta ativo e 0 para sem alerta.
                            ao pedir algo, veja se tem a coluna ativo e retorne apenas os ativos, dizendo X ativos e Y inativos e o total. Veja se essa pergunta tem mais relação com mais alguma tabela e veja se essa tabela estao ativos para os mesmos.
                            Construa a consulta SQL correta para obter os dados solicitados, colocando o nome do schema e o nome da tabela. 
                            Use apenas SELECT. 
                            A função DATE() não é adequada para calcular datas relativas. Em vez disso, você deve usar funções como CURDATE() e INTERVAL. 
                            "Interprete a pergunta do usuário e determine uma consulta SQL específica para encontrar os códigos e nomes apropriados. 
                            "Inclua apenas os códigos e nomes ou valores retornados mais apropriados gerados na resposta. 
                            "Caso não tenha um código ou nome na pergunta, entregue null.
                            """
                        )

                        prompt_nomes = f"{nomes_context}\n\nPergunta: {query}"
                        response_nomes = chat.send_message(prompt_nomes)
                        nomes_banco = response_nomes.text.strip()
                        nomes_banco = nomes_banco.replace("```sql", "").replace("```", "").strip()
                        print('\nnomes_banco\n',nomes_banco)

                        if nomes_banco:
                            pool = await create_pool()
                            async with pool.acquire() as conn:
                                async with conn.cursor() as cur:    
                                    await cur.execute(nomes_banco)
                                    resultado_nomes = await cur.fetchall()
                                    resultado_nomes_str = "\n".join([str(row) for row in resultado_nomes])
                                    print('\nresultado_nomes_str\n',resultado_nomes_str)

        resultado_nomes_context = (
            f"Você é um assistente que ajuda a procurar nomes e códigos na resposta {resultado_nomes_str}. "
            "Nunca invente dado. "
            "Retorne no máximo 10 resultados. "
            "Use DISTINCT. "
            f"Veja se a coluna da pergunta {query} tem relação com a resposta {resultado_nomes_str}. "
            "Interprete a pergunta do usuário e determine a resposta que mais faz sentido para encontrar os códigos e nomes apropriados. "
            "Inclua apenas os códigos e nomes retornados mais apropriados gerados na resposta. "
            f"Formate o resultado removendo parênteses das consultas SQL e entregando apenas o resultado e com uma palavra apenas explicando o resultado."
            f"Exemplo: veja o resultado de {resultado_nomes_str}, veja a pergunta de {query} para ver o contexto:"
            
            " - Se a pergunta for relacionada a código do equipamento, entregue a resposta da seguinte forma: código do equipamento é 2."
            " - Se a pergunta for relacionada a quantidade de equipamento, entregue a resposta da seguinte forma: A quantidade de equipamento é 2."
            " - Se a pergunta for relacionada a nome de equipamentos, entregue a resposta da seguinte forma: Os nomes dos equipamentos são ."

            " - Se a pergunta for relacionada a quantidade de usinas, entregue a resposta da seguinte forma: A quantidade de usinas é 2."
            " - Se a pergunta for relacionada a código de usinas, entregue a resposta da seguinte forma: O código da usina é 2."
            " - Se a pergunta for relacionada a nome de usinas, entregue a resposta da seguinte forma: Os nomes das usinas são ."
        
            " - Assim por diante..."
        )

        resultado_prompt_nomes = f"{resultado_nomes_context}\n\nPergunta: {resultado_nomes_str}"
        resultado_response_nomes = chat.send_message(resultado_prompt_nomes)
        resultado_nomes_banco = resultado_response_nomes.text.strip()
        print('\nresultado_nomes_banco',resultado_nomes_banco)

        reports.append(f"{resultado_nomes_banco}")

        final_report = "\n\n".join(reports)

    except Exception as e:
        await bot.send_message(chat_id, f"Ocorreu um erro ao consultar o banco de dados")
        print(f"Ocorreu um erro ao consultar o banco de dados: {str(e)}")
        return

    # Dividir o relatório em partes de no máximo 4096 caracteres
    parts = split_report(final_report)

    # Enviar cada parte do relatório no Telegram
    # for part in parts:
    #     await bot.send_message(chat_id, part)



    if db_names and table_names:
        try:
            consulta_banco = []
            result_str = []
            pool = await create_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    reports = []
                #    for db_name, table_name in zip(db_names, table_names):
                    for db_name in db_names:
                        for table_name in table_names:
                
                            await cur.execute(f"USE {db_name};")
                            await cur.execute(f"SHOW COLUMNS FROM {db_name}.{table_name}")
                            columns = await cur.fetchall()
                            column_names = [col[0] for col in columns]
                        #    print('Schema e tabela',db_name,table_name,'\n')

                            nomes_colunas_context = (
                                f"""
                                Você é um assistente que ajuda com consultas a um banco de dados.
                                Nunca invente dado.
                                Use a pesquisa sql anterior para referencia na proxima pesquisa {consulta_banco} e use a resposta anterior para criar a proxima consulta {result_str}
                                Sempre entregue o nome ou o codigo.
                                "Retorne no maximo 10 resultados. "
                                "Use DISTINCT. "
                                "Nao retorne valores zeros."
                                Use o resultado de {resultado_nomes_banco} para filtrar na consulta sql.
                                Interprete os nomes das colunas da tabela {db_name}.{table_name}, determine os nomes das colunas apropriadas usando {tabelas_colunas} para a consulta da pergunta em {query}, lembrando e procurando relação com a resposta da filtragem {resultado_nomes_banco}. 
                                Procure sempre por colunas ativo = 1, se existir, filtre.
                                Quando falar quantos, é quantidade
                                Quando falar quais é nome
                                Quando falar quantos e quais é a quantidade e os nomes respectivamente
                                Sempre relacione pela tabela sup_geral.usinas para ativo = 1 e relacionando o nome da usina a pergunta de {query}, entregando o resultado filtrado para as usinas ativas
                                Preste atenção para fazer a consulta correta para o schema correto e a tabela correta. 
                                Cuidado com a sintaxe, veja os nomes corretos de cada coluna para cada tabela e schema.
                                Se pedir nome de algo, sempre pesquise por nomes parecidos com o da pergunta, como maiusculo, minusculo, acentos...
                                Se nao encontrar o nome, procure por nomes parecidos, proximos do escrito, como por exemplo:
                                - Fazenda = faz.
                                - Santa = Sta.
                                - santo = Sto. ou St.
                                - caso escreva o nome e nao tenha, como isabel, porem no banco so tem izabel, faça a relacao de procurar nomes com partes do nome parecido e dizer que nao encontrou o nome X porem o nome Y pode ser a opção.
                                se nao encontrar o nome, procure parecidos e de as opções de sugestão.
                                Caso tenha mais de uma, mostre as opções e diga no plural juntamente com o nome caso tenha
                                Caso pergunte de alerta, alerta deve ser 1 para alerta ativo e 0 para sem alerta.
                                ao pedir algo, veja se tem a coluna ativo e retorne apenas os ativos, dizendo X ativos e Y inativos e o total. Veja se essa pergunta tem mais relação com mais alguma tabela e veja se essa tabela estao ativos para os mesmos.
                                Construa a consulta SQL correta para obter os dados solicitados, colocando o nome do schema e o nome da tabela. 
                                Use apenas SELECT. 
                                A função DATE() não é adequada para calcular datas relativas. Em vez disso, você deve usar funções como CURDATE() e INTERVAL. 
                                Inclua apenas a consulta SQL mais apropriada gerada na resposta, limpando o texto e deixando somente a consulta. 
                                Não entregue a resposta com ```sql, entregue a consulta apenas. 
                                Entregue apenas o resultado da consulta em forma de texto, nada mais. 
                                Não quero o resultado com ```sql.
                                """
                            )

                            prompt_nome = f"{nomes_colunas_context}\n\nPergunta: {column_names} e use o resultado da filtragem de {resultado_nomes_banco} para que filtre a consulta de {query}"
                            response_nome = chat.send_message(prompt_nome)
                            consulta_banco = response_nome.text
                            consulta_banco = consulta_banco.replace("```sql", "").replace("```", "").strip()  # Remover ```sql e ``` delimitadores
                        #    print('\nconsulta_banco 2\n',consulta_banco)

                            await cur.execute(consulta_banco)
                            results = await cur.fetchall()

                            result_str = "\n".join([str(row) for row in results])
                            print('\nresult_str\n',result_str)

                            resultado_consulta_context = (
                                "Você é um assistente que ajuda com consultas a um banco de dados."
                                "Nunca invente dado."
                                "Retorne no maximo 10 resultados. "
                                "Use DISTINCT. "
                                "Nao retorne valores zeros."
                                f"Entregue uma resposta formatada e descritiva do resultado {result_str} da consulta do sql {consulta_banco}, que esta no banco de dados {db_name}.{table_name}, usando a pergunta anterior {chat_history[chat_id]}"
                                "Envie em formato HTML, utilizando apenas as tags <b> e </b> para negrito para formatar o texto."
                                "Não crie tabela." 
                                "Não use a tag <p> </p>."
                                "Não use a tag <br> ou </br> ou <br/>"
                                "Inclua a resposta sem inventar dados."
                                "Não inclua o nome da tabela e nem do schema."
                                "Veja se teve mais algum pedido do usuario e complemente sem inventar dados."
                                "Pule linhas para ficar mais organizado"
                            )
                            prompt_resultado_consulta = f"{resultado_consulta_context}"
                            response_resultado_consulta = chat.send_message(prompt_resultado_consulta)
                            resultado_consulta = response_resultado_consulta.text

                            # Verifica se o resultado contém termos que indicam que não há resultados encontrados
                            if re.search(r'\b(não encontrado|não|indeterminado)\b', resultado_consulta, re.IGNORECASE):
                                print("resultado_consulta",resultado_consulta)
                                print("Não tem consulta")

                                nomes_colunas_context2 = (
                                    f"""
                                    Você é um assistente que ajuda com consultas a um banco de dados.
                                    Nao faça a mesma consulta de {result_str} 
                                    Interprete a consulta da pergunta {resultado_consulta}
                                    Nunca invente dado. apenas use a consulta ao banco.
                                    Veja se a consulta conseguiu algum resultado, caso não, faca uma nova consulta sql para wue procure algo parecido para a pergunta {query}, de sugestao e faça a consulta usando o banco de dados {db_name}.{table_name} 
                                    Se pedir nome de algo, sempre pesquise por nomes parecidos com o da pergunta, como maiusculo, minusculo, acentos, pedaços do nome destrinchando em pequenos pedaços...
                                    Se nao encontrar o nome, procure por nomes parecidos, proximos do escrito, como por exemplo:
                                        - Fazenda = faz.
                                        - Santa = Sta.
                                        - santo = Sto.
                                        - caso escreva o nome e nao tenha, como por exempo: isabel, porem no banco so tem izabel, faça a relacao de procurar nomes com partes do nome parecido e dizer que nao encontrou o nome X porem o nome Y pode ser a opção.
                                        - se entregar o total, fale o total para os nomes mais relacionado ao pedido ou o que mais apareceu e o total geral da consulta
                                        faça a consulta para que seja varios tipos de nomes possiveis, destrinchando o nome para varios nomes pequenos desse pedido.
                                    Caso tenha mais de uma, mostre as opções e diga no plural juntamente com o nome caso tenha
                                    ao pedir algo, veja se tem a coluna ativo e retorne apenas os ativos, dizendo X ativos e Y inativos e o total. Veja se essa pergunta tem mais relação com mais alguma tabela e veja se essa tabela estao ativos para os mesmos.
                                    Construa a consulta SQL correta para obter os dados solicitados, colocando o nome do schema e o nome da tabela. 
                                    Use apenas SELECT. 
                                    A função DATE() não é adequada para calcular datas relativas. Em vez disso, você deve usar funções como CURDATE() e INTERVAL. 
                                    Entregue sempre o nome.
                                    Inclua apenas a consulta SQL mais apropriada gerada na resposta, limpando o texto e deixando somente a consulta. 
                                    Não entregue a resposta com ```sql, entregue a consulta apenas. 
                                    Entregue apenas o resultado da consulta em forma de texto, nada mais. 
                                    Preste atenção para fazer a consulta correta para o schema correto e a tabela correta. 
                                    Cuidado com a sintaxe, veja os nomes corretos de cada coluna para cada tabela e schema.
                                    """
                                )

                                prompt_nome2 = f"{nomes_colunas_context2}\n\nPergunta: {column_names}"
                                response_nome2 = chat.send_message(prompt_nome2)
                                consulta_banco2 = response_nome2.text
                                consulta_banco2 = consulta_banco2.replace("```sql", "").replace("```", "").strip()  # Remover ```sql e ``` delimitadores
                            #    print('consulta_banco2\n',consulta_banco2)

                                await cur.execute(consulta_banco2)
                                results2 = await cur.fetchall()
                                print('results2\n',results2)
                                
                                
                                result_str2 = "\n".join([str(row) for row in results2])

                                resultado_consulta_context2 = (
                                    "Você é um assistente que ajuda com consultas a um banco de dados."
                                    f"Entregue uma resposta formatada e descritiva do resultado {result_str2} da consulta do sql {consulta_banco2}, que esta no banco de dados {db_name}.{table_name}, usando a pergunta anterior {chat_history[chat_id]}"
                                    "Envie em formato HTML, utilizando apenas as tags <b> e </b> para negrito para formatar o texto."
                                    "Não crie tabela." 
                                    "Não use a tag <p> </p>."
                                    "Não use a tag <br> ou </br> ou <br/>"
                                    "Inclua a resposta sem inventar dados."
                                    "Não inclua o nome da tabela e nem do schema."
                                    "Veja se teve mais algum pedido do usuario e complemente sem inventar dados."
                                    "Pule linhas para ficar mais organizado"
                                    "Caso o resultado entregue varios resultados, mostre o que mais teve relacao com a consulta ou que teve mais resultados parecidos."
                                    "Entregue uma boa identação"
                                )
                                prompt_resultado_consulta2 = f"{resultado_consulta_context2}"
                                response_resultado_consulta2 = chat.send_message(prompt_resultado_consulta2)
                                resultado_consulta = response_resultado_consulta2.text
                        

                    #    reports.append(f"<b>Schema: {db_name}</b>\n<b>Tabela: {table_name}</b>\n\n{resultado_consulta}")
                    reports.append(f"{resultado_consulta}")

                    chat_history[chat_id].append({"role": "assistant", "content": resultado_consulta})

                    # Junta todos os relatórios
                    final_report = "\n\n".join(reports)
                    print('final_report\n',final_report,'\n')

        except Exception as e:
            await bot.send_message(chat_id, f"Erro ao conectar com o banco de dados")
            print(f"Erro ao conectar com o banco de dados: {str(e)}")

            return
        finally:
            pool.close()
            await pool.wait_closed()
    else:
    #    final_report = "Desculpe, não consegui entender sua consulta. Poderia reformular sua pergunta?"

        erro_context = (
            "Você é um assistente que ajuda com perguntas em geral. "
            f"Interprete a pergunta do usuario em {query}"
    #        f"Interprete a pergunta do usuario em {query} usando a pergunta anterior {chat_history[chat_id]}"
    #        f"As perguntas e respostas estao em {chat_history[chat_id]}, use essas informacoes para formular uma resposta"
            f"Caso tenha a ver com banco de dados, interprete a pergunta e tente entregar a consulta sql para o usuario olhando em {tables} para entrgar a consulta com o schema e o nome da tabela"
            "Caso seja uma pergunta que nao tenha a ver com banco de dados, converse normalmente com o usuário, lembrando de perguntas anteriores para uma conversa mais organica."
        )
        prompt_erro = f"{erro_context}\n\nPergunta: {query}"
#        prompt_erro = f"perguntas e respostas estao em: {chat_history[chat_id]} \n\n{erro_context}\n\nPergunta: {query}"
        response_erro = chat.send_message(prompt_erro)
        resposta_erro = response_erro.text
        final_report = resposta_erro
        print(final_report)
        
    #    chat_history[chat_id].append({"role": "assistant", "content": resposta_erro})
            
    # Divide a mensagem em partes de no máximo 4096 caracteres
    parts = split_report(final_report)

    # Envia cada parte da mensagem separadamente
    for part in parts:
        await bot.send_message(chat_id, part, parse_mode='HTML')
        
        
# Inicialização do bot
if __name__ == '__main__':
    from aiogram import executor
    executor.start_polling(dp, skip_updates=True)
    
    







'''

import logging
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.contrib.middlewares.logging import LoggingMiddleware
import aiomysql
import asyncio
import google.generativeai as genai
import re

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

# Função para criar pool de conexão com o banco de dados
# async def create_pool():
#     return await aiomysql.create_pool(
#         host="192.168.4.50",
#         user="bruno",
#         password="superbancoml"
#     )

async def create_pool():
    return await aiomysql.create_pool(
        host="192.168.4.50",
        user="bot_consultas",
        password="@ssistente_2024"
    )
    
# Tabelas específicas para consultar
tables = {
    "machine_learning": [
        "coeficiente_geradores",
        "falhas_gerais",
        "leituras_consecutivas",
        "log_relatorio_quebras",
        "telegram_silenciar_bot",
        "usuarios_telegram",
        "valores_previsao"
    ],
    "sup_geral": [
        "lista_alarmes",
        "alarmes_ativos",
        "campos",
        "equipamentos",
        "campos",
        "clientes",
        "usinas",
        "usuarios",
        "leituras",
    ]
}



async def get_columns_for_tables():
    pool = await create_pool()
    tabelas_colunas = {}
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for schema, table_list in tables.items():
                    tabelas_colunas[schema] = {}
                    for table in table_list:
                        await cur.execute(f"USE {schema};")
                        await cur.execute(f"SHOW COLUMNS FROM {table};")
                        columns = await cur.fetchall()
                        column_names = [col[0] for col in columns]
                        tabelas_colunas[schema][table] = column_names
    except Exception as e:
        print(f"Erro ao encontrar a tabela ou coluna no banco de dados: {str(e)}")
    finally:
        pool.close()
        await pool.wait_closed()
    
    return tabelas_colunas

# Função para verificar se a tabela existe nas listas permitidas
def get_table_name(user_query):
    for db, table_list in tables.items():
        for table in table_list:
            if table.lower() in user_query.lower():
                return table, db
    return None, None

# Função para dividir o relatório em partes de no máximo 4096 caracteres
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


# Função para manter o histórico de chat
chat_history = {}

# Função para responder no Telegram
@dp.message_handler()
async def handle_message(message: types.Message):
    query = message.text
    chat_id = message.chat.id

    # Configuração do modelo Gemini
    model = genai.GenerativeModel('gemini-1.5-flash-latest')
    chat = model.start_chat(history=[])

    # Inicializa o histórico de chat se ainda não existir
    if chat_id not in chat_history:
        chat_history[chat_id] = []

    # Adiciona a mensagem do usuário ao histórico
    chat_history[chat_id].append({"role": "user", "content": query})

    tabelas_colunas = await get_columns_for_tables()
#    print(tabelas_colunas)
    
    nome_tabela_context = (
        "Você é um assistente que ajuda com consultas a um banco de dados. "
        f"Interprete a pergunta do usuário, determine o nome das tabelas apropriadas caso tenha mais de uma de acordo com a {query} e pesquise os dados de acordo com a pergunta vendo se tem algum nome de coluna que possa ajudar a procurar em {tabelas_colunas}"
        "Inclua apenas os nomes das tabelas mais apropriadas geradas na resposta."
        "Se for mais de uma tabela, entregue o resultado enumerando as ordens das tabelas solicitadas. apenas coloque o numero um traco e o nome da tabela e mais nada, seja direto e entregue somente esse dado"
        "Caso nao tenha nenhuma tabela na pergunta, entregue null."
    )
    prompt_tabela = f"{nome_tabela_context}\n\nPergunta: {query}"
    response_tabela = chat.send_message(prompt_tabela)

    nome_tabela = response_tabela.text
#    print('nome_tabela GEMINI: ',nome_tabela)

    chat_history[chat_id].append({"role": "assistant", "content": nome_tabela})


    schema_context = (
        "Você é um assistente que ajuda com consultas a um banco de dados. "
        f"Interprete a pergunta do usuário, determine os nomes dos schemas apropriados de acordo com a {query}, usando a resposta de {nome_tabela} para pesquisar os dados em {tabelas_colunas}"
        "Inclua apenas os schemas dos bancos mais apropriados gerados na resposta."
        "Se for mais de um schemas, entregue o resultado enumerando as ordens dos schemas solicitadas. apenas coloque o numero um traco e o nome do schemas e mais nada, seja direto e entregue somente esse dado"
        "Caso nao tenha um schema na pergunta, entregue null."
    )
    prompt_schema = f"{schema_context}\n\nPergunta: {query}"
    response_schema = chat.send_message(prompt_schema)
    nome_schema = response_schema.text
#    print('nome_schema GEMINI: ',nome_schema)
                        
    chat_history[chat_id].append({"role": "assistant", "content": nome_schema})

    # Organize as tabelas e schemas em listas
    db_names = [schema.split('- ')[1].strip() for schema in nome_schema.split('\n') if '- ' in schema]
    table_names = [table.split('- ')[1].strip() for table in nome_tabela.split('\n') if '- ' in table]


    resultado_nomes_str = []
    for db_name in db_names:
        for table_name in table_names:
            if db_name in tabelas_colunas and table_name in tabelas_colunas[db_name]:
                column_names = tabelas_colunas[db_name][table_name]
                # Verifique se qualquer coluna mencionada na consulta está na tabela atual
                column_in_query = None
                for col in column_names:
                    if col.lower() in query.lower():
                        column_in_query = col
                        break

                if column_in_query:
                #    print('\ndb_name', db_name, 'table_name', table_name)
                    nomes_context = (
                        "Você é um assistente que ajuda com consultas a um banco de dados. "
                        "Nunca invente dado. "
                        "Retorne no maximo 10 resultados. "
                        "Use DISTINCT. "
                        f"Veja se a coluna da pergunta {query} tem no schema {db_name} e a tabela {table_name} das colunas de {tabelas_colunas}, caso nao tenha, retorne null. "
                        f"de acordo com a {query}, e o schema {db_name} e a tabela {table_name} para os nomes das colunas de {tabelas_colunas} para pesquisar. "
                        f"Use o schema {db_name} e a tabela {table_name}. "
                        "Interprete a pergunta do usuário e determine uma consulta SQL específica para encontrar os códigos e nomes apropriados. "
                        "Inclua apenas os códigos e nomes retornados mais apropriados gerados na resposta. "
                        "Caso não tenha um código ou nome na pergunta, entregue null."
                    )

                    prompt_nomes = f"{nomes_context}\n\nPergunta: {query}"
                    response_nomes = chat.send_message(prompt_nomes)
                    nomes_banco = response_nomes.text
                    nomes_banco = nomes_banco.replace("```sql", "").replace("```", "").strip()
            #        print('nomes_banco\n', nomes_banco)
                    if nomes_banco:
                        pool = await create_pool()
                        async with pool.acquire() as conn:
                            async with conn.cursor() as cur:    
                                await cur.execute(nomes_banco)
                                resultado_nomes = await cur.fetchall()
                                resultado_nomes_str = "\n".join([str(row) for row in resultado_nomes])
                                print('\nresultado_nomes_str\n', resultado_nomes_str)

    resultado_nomes_context = (
        f"Você é um assistente que ajuda a procurar nomes e codigos na resposta {resultado_nomes_str}. "
        "Nunca invente dado. "
        "Retorne no maximo 10 resultados. "
        "Use DISTINCT. "
    #    f"Veja se a coluna da pergunta {query} tem relaçao com a resposta {resultado_nomes_str}, caso nao tenha, retorne null. "
        f"Veja se a coluna da pergunta {query} tem relaçao com a resposta {resultado_nomes_str}. "
        "Interprete a pergunta do usuário e determine a resposta que mais faz sentido para encontrar os códigos e nomes apropriados. "
        "Inclua apenas os códigos e nomes retornados mais apropriados gerados na resposta. "
        f"Formate o resultado removendo parenteses das consutas sql e entregando apenas o resultado e com uma palavra apenas explicando o resultado."
        f"Exemplo: veja o resultado de {resultado_nomes_str}, veja a pergunta de {query} para ver o contexto:"
        " - Se a pergunta for relacionada a codigo do equipamento, entrege a resposta da seguinte forma: codigo do equipamento e 2."
        " - Se a pergunta for relacionada a quantidade de equipamento, entrege a resposta da seguinte forma: A quantidade de equipamento e 2."
        " - Se a pergunta for relacionada a nome de equipamentos, entrege a resposta da seguinte forma: Os nomes dos equipamentos são ."
        " - Se a pergunta for relacionada a quantidade de usinas, entrege a resposta da seguinte forma: A quantidade de usinas e 2."
        " - Se a pergunta for relacionada a codigo de usinas, entrege a resposta da seguinte forma: O codigo da usina e 2."
        " - Se a pergunta for relacionada a nome de usinas, entrege a resposta da seguinte forma: Os nomes das usinas são ."
        " - Assim por diante..."
    )

    resultado_prompt_nomes = f"{resultado_nomes_context}\n\nPergunta: {resultado_nomes_str}"
    resultado_response_nomes = chat.send_message(resultado_prompt_nomes)
    resultado_nomes_banco = resultado_response_nomes.text
    print('resultado_nomes_banco\n', resultado_nomes_banco)


    print('\ndb_names',db_names,'table_names',table_names,'\n')

    if db_names and table_names:
        try:
            pool = await create_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    reports = []
                #    for db_name, table_name in zip(db_names, table_names):
                    for db_name in db_names:
                        for table_name in table_names:
                
                            await cur.execute(f"USE {db_name};")
                            await cur.execute(f"SHOW COLUMNS FROM {db_name}.{table_name}")
                            columns = await cur.fetchall()
                            column_names = [col[0] for col in columns]
                            print('Schema e tabela',db_name,table_name,'\n')

                            nomes_colunas_context = (
                                f"""
                                Você é um assistente que ajuda com consultas a um banco de dados.
                                Nunca invente dado.
                                Sempre entregue o nome ou o codigo.
                                "Retorne no maximo 10 resultados. "
                                "Use DISTINCT. "
                                "Nao retorne valores zeros."
                                Use o resultado de {resultado_nomes_banco} para filtrar na consulta sql.
                                Interprete os nomes das colunas da tabela {db_name}.{table_name}, determine os nomes das colunas apropriadas usando {tabelas_colunas} para a consulta da pergunta em {query}, lembrando e procurando relação com a resposta da filtragem {resultado_nomes_banco}. 
                                Preste atenção para fazer a consulta correta para o schema correto e a tabela correta. 
                                Cuidado com a sintaxe, veja os nomes corretos de cada coluna para cada tabela e schema.
                                Se pedir nome de algo, sempre pesquise por nomes parecidos com o da pergunta, como maiusculo, minusculo, acentos...
                                Se nao encontrar o nome, procure por nomes parecidos, proximos do escrito, como por exemplo:
                                - Fazenda = faz.
                                - Santa = Sta.
                                - santo = Sto. ou St.
                                - caso escreva o nome e nao tenha, como isabel, porem no banco so tem izabel, faça a relacao de procurar nomes com partes do nome parecido e dizer que nao encontrou o nome X porem o nome Y pode ser a opção.
                                se nao encontrar o nome, procure parecidos e de as opções de sugestão.
                                Caso tenha mais de uma, mostre as opções e diga no plural juntamente com o nome caso tenha
                                Caso pergunte de alerta, alerta deve ser 1 para alerta ativo e 0 para sem alerta.
                                ao pedir algo, veja se tem a coluna ativo e retorne apenas os ativos, dizendo X ativos e Y inativos e o total. Veja se essa pergunta tem mais relação com mais alguma tabela e veja se essa tabela estao ativos para os mesmos.
                                Construa a consulta SQL correta para obter os dados solicitados, colocando o nome do schema e o nome da tabela. 
                                Use apenas SELECT. 
                                A função DATE() não é adequada para calcular datas relativas. Em vez disso, você deve usar funções como CURDATE() e INTERVAL. 
                                Inclua apenas a consulta SQL mais apropriada gerada na resposta, limpando o texto e deixando somente a consulta. 
                                Não entregue a resposta com ```sql, entregue a consulta apenas. 
                                Entregue apenas o resultado da consulta em forma de texto, nada mais. 
                                Não quero o resultado com ```sql.
                                """
                            )

                            prompt_nome = f"{nomes_colunas_context}\n\nPergunta: {column_names} e use o resultado da filtragem de {resultado_nomes_banco} para que filtre a consulta de {query}"
                            response_nome = chat.send_message(prompt_nome)
                            consulta_banco = response_nome.text
                            consulta_banco = consulta_banco.replace("```sql", "").replace("```", "").strip()  # Remover ```sql e ``` delimitadores
                            print('consulta_banco\n',consulta_banco)

                            await cur.execute(consulta_banco)
                            results = await cur.fetchall()

                            result_str = "\n".join([str(row) for row in results])

                            resultado_consulta_context = (
                                "Você é um assistente que ajuda com consultas a um banco de dados."
                                "Nunca invente dado."
                                "Retorne no maximo 10 resultados. "
                                "Use DISTINCT. "
                                "Nao retorne valores zeros."
                                f"Entregue uma resposta formatada e descritiva do resultado {result_str} da consulta do sql {consulta_banco}, que esta no banco de dados {db_name}.{table_name}, usando a pergunta anterior {chat_history[chat_id]}"
                                "Envie em formato HTML, utilizando apenas as tags <b> e </b> para negrito para formatar o texto."
                                "Não crie tabela." 
                                "Não use a tag <p> </p>."
                                "Não use a tag <br> ou </br> ou <br/>"
                                "Inclua a resposta sem inventar dados."
                                "Não inclua o nome da tabela e nem do schema."
                                "Veja se teve mais algum pedido do usuario e complemente sem inventar dados."
                                "Pule linhas para ficar mais organizado"
                            )
                            prompt_resultado_consulta = f"{resultado_consulta_context}"
                            response_resultado_consulta = chat.send_message(prompt_resultado_consulta)
                            resultado_consulta = response_resultado_consulta.text

                            # Verifica se o resultado contém termos que indicam que não há resultados encontrados
                            if re.search(r'\b(não encontrado|não|indeterminado)\b', resultado_consulta, re.IGNORECASE):
                                print("resultado_consulta",resultado_consulta)
                                print("Não tem consulta")

                                nomes_colunas_context2 = (
                                    f"""
                                    Você é um assistente que ajuda com consultas a um banco de dados.
                                    Nao faça a mesma consulta de {result_str} 
                                    Interprete a consulta da pergunta {resultado_consulta}
                                    Nunca invente dado. apenas use a consulta ao banco.
                                    Veja se a consulta conseguiu algum resultado, caso não, faca uma nova consulta sql para wue procure algo parecido para a pergunta {query}, de sugestao e faça a consulta usando o banco de dados {db_name}.{table_name} 
                                    Se pedir nome de algo, sempre pesquise por nomes parecidos com o da pergunta, como maiusculo, minusculo, acentos, pedaços do nome destrinchando em pequenos pedaços...
                                    Se nao encontrar o nome, procure por nomes parecidos, proximos do escrito, como por exemplo:
                                        - Fazenda = faz.
                                        - Santa = Sta.
                                        - santo = Sto.
                                        - caso escreva o nome e nao tenha, como por exempo: isabel, porem no banco so tem izabel, faça a relacao de procurar nomes com partes do nome parecido e dizer que nao encontrou o nome X porem o nome Y pode ser a opção.
                                        - se entregar o total, fale o total para os nomes mais relacionado ao pedido ou o que mais apareceu e o total geral da consulta
                                        faça a consulta para que seja varios tipos de nomes possiveis, destrinchando o nome para varios nomes pequenos desse pedido.
                                    Caso tenha mais de uma, mostre as opções e diga no plural juntamente com o nome caso tenha
                                    ao pedir algo, veja se tem a coluna ativo e retorne apenas os ativos, dizendo X ativos e Y inativos e o total. Veja se essa pergunta tem mais relação com mais alguma tabela e veja se essa tabela estao ativos para os mesmos.
                                    Construa a consulta SQL correta para obter os dados solicitados, colocando o nome do schema e o nome da tabela. 
                                    Use apenas SELECT. 
                                    A função DATE() não é adequada para calcular datas relativas. Em vez disso, você deve usar funções como CURDATE() e INTERVAL. 
                                    Entregue sempre o nome.
                                    Inclua apenas a consulta SQL mais apropriada gerada na resposta, limpando o texto e deixando somente a consulta. 
                                    Não entregue a resposta com ```sql, entregue a consulta apenas. 
                                    Entregue apenas o resultado da consulta em forma de texto, nada mais. 
                                    Preste atenção para fazer a consulta correta para o schema correto e a tabela correta. 
                                    Cuidado com a sintaxe, veja os nomes corretos de cada coluna para cada tabela e schema.
                                    """
                                )

                                prompt_nome2 = f"{nomes_colunas_context2}\n\nPergunta: {column_names}"
                                response_nome2 = chat.send_message(prompt_nome2)
                                consulta_banco2 = response_nome2.text
                                consulta_banco2 = consulta_banco2.replace("```sql", "").replace("```", "").strip()  # Remover ```sql e ``` delimitadores
                                print('consulta_banco2\n',consulta_banco2)

                                await cur.execute(consulta_banco2)
                                results2 = await cur.fetchall()
                                
                                
                                result_str2 = "\n".join([str(row) for row in results2])

                                resultado_consulta_context2 = (
                                    "Você é um assistente que ajuda com consultas a um banco de dados."
                                    f"Entregue uma resposta formatada e descritiva do resultado {result_str2} da consulta do sql {consulta_banco2}, que esta no banco de dados {db_name}.{table_name}, usando a pergunta anterior {chat_history[chat_id]}"
                                    "Envie em formato HTML, utilizando apenas as tags <b> e </b> para negrito para formatar o texto."
                                    "Não crie tabela." 
                                    "Não use a tag <p> </p>."
                                    "Não use a tag <br> ou </br> ou <br/>"
                                    "Inclua a resposta sem inventar dados."
                                    "Não inclua o nome da tabela e nem do schema."
                                    "Veja se teve mais algum pedido do usuario e complemente sem inventar dados."
                                    "Pule linhas para ficar mais organizado"
                                    "Caso o resultado entregue varios resultados, mostre o que mais teve relacao com a consulta ou que teve mais resultados parecidos."
                                    "Entregue uma boa identação"
                                )
                                prompt_resultado_consulta2 = f"{resultado_consulta_context2}"
                                response_resultado_consulta2 = chat.send_message(prompt_resultado_consulta2)
                                resultado_consulta = response_resultado_consulta2.text
                        

                    #    reports.append(f"<b>Schema: {db_name}</b>\n<b>Tabela: {table_name}</b>\n\n{resultado_consulta}")
                    reports.append(f"{resultado_consulta}")

                    chat_history[chat_id].append({"role": "assistant", "content": resultado_consulta})

                    # Junta todos os relatórios
                    final_report = "\n\n".join(reports)
                    print('final_report\n',final_report,'\n')

        except Exception as e:
            await bot.send_message(chat_id, f"Erro ao conectar com o banco de dados")
            print(f"Erro ao conectar com o banco de dados: {str(e)}")

            return
        finally:
            pool.close()
            await pool.wait_closed()
    else:
    #    final_report = "Desculpe, não consegui entender sua consulta. Poderia reformular sua pergunta?"

        erro_context = (
            "Você é um assistente que ajuda com perguntas em geral. "
            f"Interprete a pergunta do usuario em {query}"
    #        f"Interprete a pergunta do usuario em {query} usando a pergunta anterior {chat_history[chat_id]}"
    #        f"As perguntas e respostas estao em {chat_history[chat_id]}, use essas informacoes para formular uma resposta"
            f"Caso tenha a ver com banco de dados, interprete a pergunta e tente entregar a consulta sql para o usuario olhando em {tables} para entrgar a consulta com o schema e o nome da tabela"
            "Caso seja uma pergunta que nao tenha a ver com banco de dados, converse normalmente com o usuário, lembrando de perguntas anteriores para uma conversa mais organica."
        )
        prompt_erro = f"{erro_context}\n\nPergunta: {query}"
#        prompt_erro = f"perguntas e respostas estao em: {chat_history[chat_id]} \n\n{erro_context}\n\nPergunta: {query}"
        response_erro = chat.send_message(prompt_erro)
        resposta_erro = response_erro.text
        final_report = resposta_erro
        print(final_report)
        
    #    chat_history[chat_id].append({"role": "assistant", "content": resposta_erro})
            
    # Divide a mensagem em partes de no máximo 4096 caracteres
    parts = split_report(final_report)

    # Envia cada parte da mensagem separadamente
    for part in parts:
        await bot.send_message(chat_id, part, parse_mode='HTML')

# Função principal para rodar o bot
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(dp.start_polling())
    
'''    
    
    
    
    


'''


import logging
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.contrib.middlewares.logging import LoggingMiddleware
import aiomysql
import asyncio
import google.generativeai as genai
import re

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

# Função para criar pool de conexão com o banco de dados
# async def create_pool():
#     return await aiomysql.create_pool(
#         host="192.168.4.50",
#         user="bruno",
#         password="superbancoml"
#     )

async def create_pool():
    return await aiomysql.create_pool(
        host="192.168.4.50",
        user="bot_consultas",
        password="@ssistente_2024"
    )
    
# Tabelas específicas para consultar
tables = {
    "machine_learning": [
        "coeficiente_geradores",
        "falhas_gerais",
        "leituras_consecutivas",
        "log_relatorio_quebras",
        "telegram_silenciar_bot",
        "usuarios_telegram",
        "valores_previsao"
    ],
    "sup_geral": [
        "lista_alarmes",
        "alarmes_ativos",
        "campos",
        "equipamentos",
        "campos",
        "clientes",
        "usinas",
        "usuarios",
        "leituras",
    ]
}


async def get_columns_for_tables():
    pool = await create_pool()
    tabelas_colunas = {}
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for schema, table_list in tables.items():
                    tabelas_colunas[schema] = []
                    for table in table_list:
                        await cur.execute(f"USE {schema};")
                        await cur.execute(f"SHOW COLUMNS FROM {table};")
                        columns = await cur.fetchall()
                        column_names = [col[0] for col in columns]
                        tabelas_colunas[schema].append(table)
                        tabelas_colunas[schema].append(", ".join(column_names))
    except Exception as e:
        print(f"Erro ao encontrar a tabela ou coluna no banco de dados: {str(e)}")
    finally:
        pool.close()
        await pool.wait_closed()
    
    return tabelas_colunas

# Função para verificar se a tabela existe nas listas permitidas
def get_table_name(user_query):
    for db, table_list in tables.items():
        for table in table_list:
            if table.lower() in user_query.lower():
                return table, db
    return None, None

# Função para dividir o relatório em partes de no máximo 4096 caracteres
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


# Função para manter o histórico de chat
chat_history = {}

# Função para responder no Telegram
@dp.message_handler()
async def handle_message(message: types.Message):
    query = message.text
    chat_id = message.chat.id

    # Configuração do modelo Gemini
    model = genai.GenerativeModel('gemini-1.5-flash-latest')
    chat = model.start_chat(history=[])

    # Inicializa o histórico de chat se ainda não existir
    if chat_id not in chat_history:
        chat_history[chat_id] = []

    # Adiciona a mensagem do usuário ao histórico
    chat_history[chat_id].append({"role": "user", "content": query})

    tabelas_colunas = await get_columns_for_tables()
#    print(tabelas_colunas)
    
    nome_tabela_context = (
        "Você é um assistente que ajuda com consultas a um banco de dados. "
        f"Interprete a pergunta do usuário, determine o nome das tabelas apropriadas caso tenha mais de uma de acordo com a {query} e pesquise os dados de acordo com a pergunta vendo se tem algum nome de coluna que possa ajudar a procurar em {tabelas_colunas}"
        "Inclua apenas os nomes das tabelas mais apropriadas geradas na resposta."
        "Se for mais de uma tabela, entregue o resultado enumerando as ordens das tabelas solicitadas. apenas coloque o numero um traco e o nome da tabela e mais nada, seja direto e entregue somente esse dado"
        "Caso nao tenha nenhuma tabela na pergunta, entregue null."
    )
    prompt_tabela = f"{nome_tabela_context}\n\nPergunta: {query}"
    response_tabela = chat.send_message(prompt_tabela)

    nome_tabela = response_tabela.text
#    print('nome_tabela GEMINI: ',nome_tabela)

    chat_history[chat_id].append({"role": "assistant", "content": nome_tabela})


    schema_context = (
        "Você é um assistente que ajuda com consultas a um banco de dados. "
        f"Interprete a pergunta do usuário, determine os nomes dos schemas apropriados de acordo com a {query}, usando a resposta de {nome_tabela} para pesquisar os dados em {tabelas_colunas}"
        "Inclua apenas os schemas dos bancos mais apropriados gerados na resposta."
        "Se for mais de um schemas, entregue o resultado enumerando as ordens dos schemas solicitadas. apenas coloque o numero um traco e o nome do schemas e mais nada, seja direto e entregue somente esse dado"
        "Caso nao tenha um schema na pergunta, entregue null."
    )
    prompt_schema = f"{schema_context}\n\nPergunta: {query}"
    response_schema = chat.send_message(prompt_schema)
    nome_schema = response_schema.text
#    print('nome_schema GEMINI: ',nome_schema)

    chat_history[chat_id].append({"role": "assistant", "content": nome_schema})

    # Organize as tabelas e schemas em listas
    db_names = [schema.split('- ')[1].strip() for schema in nome_schema.split('\n') if '- ' in schema]
    table_names = [table.split('- ')[1].strip() for table in nome_tabela.split('\n') if '- ' in table]

    print('\ndb_names',db_names,'table_names',table_names,'\n')

    if db_names and table_names:
        try:
            pool = await create_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    reports = []
                    for db_name, table_name in zip(db_names, table_names):

                        await cur.execute(f"USE {db_name};")
                        await cur.execute(f"SHOW COLUMNS FROM {db_name}.{table_name}")
                        columns = await cur.fetchall()
                        column_names = [col[0] for col in columns]
                        print('Schema e tabela',db_name,table_name,'\n')

                        nomes_colunas_context = (
                            f"""
                            Você é um assistente que ajuda com consultas a um banco de dados.
                            Nunca invente dado.
                            Interprete os nomes das colunas da tabela {db_name}.{table_name}, determine os nomes das colunas apropriadas usando {tabelas_colunas} para a consulta da pergunta em {query}, lembrando e procurando relação com a pergunta anterior {chat_history[chat_id]}. 
                            Preste atenção para fazer a consulta correta para o schema correto e a tabela correta. 
                            Cuidado com a sintaxe, veja os nomes corretos de cada coluna para cada tabela e schema.
                            Se pedir nome de algo, sempre pesquise por nomes parecidos com o da pergunta, como maiusculo, minusculo, acentos...
                            Se nao encontrar o nome, procure por nomes parecidos, proximos do escrito, como por exemplo:
                             - Fazenda = faz.
                             - Santa = Sta.
                             - santo = Sto. ou St.
                             - caso escreva o nome e nao tenha, como isabel, porem no banco so tem izabel, faça a relacao de procurar nomes com partes do nome parecido e dizer que nao encontrou o nome X porem o nome Y pode ser a opção.
                             se nao encontrar o nome, procure parecidos e de as opções de sugestão.
                            Caso tenha mais de uma, mostre as opções e diga no plural juntamente com o nome caso tenha
                            ao pedir algo, veja se tem a coluna ativo e retorne apenas os ativos, dizendo X ativos e Y inativos e o total. Veja se essa pergunta tem mais relação com mais alguma tabela e veja se essa tabela estao ativos para os mesmos.
                            Construa a consulta SQL correta para obter os dados solicitados, colocando o nome do schema e o nome da tabela. 
                            Use apenas SELECT. 
                            A função DATE() não é adequada para calcular datas relativas. Em vez disso, você deve usar funções como CURDATE() e INTERVAL. 
                            Inclua apenas a consulta SQL mais apropriada gerada na resposta, limpando o texto e deixando somente a consulta. 
                            Não entregue a resposta com ```sql, entregue a consulta apenas. 
                            Entregue apenas o resultado da consulta em forma de texto, nada mais. 
                            Não quero o resultado com ```sql.
                            """
                        )

                        prompt_nome = f"{nomes_colunas_context}\n\nPergunta: {column_names}"
                        response_nome = chat.send_message(prompt_nome)
                        consulta_banco = response_nome.text
                        consulta_banco = consulta_banco.replace("```sql", "").replace("```", "").strip()  # Remover ```sql e ``` delimitadores
                        print('consulta_banco\n',consulta_banco)

                        await cur.execute(consulta_banco)
                        results = await cur.fetchall()

                        result_str = "\n".join([str(row) for row in results])

                        resultado_consulta_context = (
                            "Você é um assistente que ajuda com consultas a um banco de dados."
                            "Nunca invente dado."                          
                            f"Entregue uma resposta formatada e descritiva do resultado {result_str} da consulta do sql {consulta_banco}, que esta no banco de dados {db_name}.{table_name}, usando a pergunta anterior {chat_history[chat_id]}"
                            "Envie em formato HTML, utilizando apenas as tags <b> e </b> para negrito para formatar o texto."
                            "Não crie tabela." 
                            "Não use a tag <p> </p>."
                            "Não use a tag <br> ou </br> ou <br/>"
                            "Inclua a resposta sem inventar dados."
                            "Não inclua o nome da tabela e nem do schema."
                            "Veja se teve mais algum pedido do usuario e complemente sem inventar dados."
                            "Pule linhas para ficar mais organizado"
                        )
                        prompt_resultado_consulta = f"{resultado_consulta_context}"
                        response_resultado_consulta = chat.send_message(prompt_resultado_consulta)
                        resultado_consulta = response_resultado_consulta.text

                        # Verifica se o resultado contém termos que indicam que não há resultados encontrados
                        if re.search(r'\b(não encontrado|não|indeterminado)\b', resultado_consulta, re.IGNORECASE):
                            print("resultado_consulta",resultado_consulta)
                            print("Não tem consulta")

                            nomes_colunas_context2 = (
                                f"""
                                Você é um assistente que ajuda com consultas a um banco de dados.
                                Nao faça a mesma consulta de {result_str} 
                                Interprete a consulta da pergunta {resultado_consulta}
                                Nunca invente dado. apenas use a consulta ao banco.
                                Veja se a consulta conseguiu algum resultado, caso não, faca uma nova consulta sql para wue procure algo parecido para a pergunta {query}, de sugestao e faça a consulta usando o banco de dados {db_name}.{table_name} 
                                Se pedir nome de algo, sempre pesquise por nomes parecidos com o da pergunta, como maiusculo, minusculo, acentos, pedaços do nome destrinchando em pequenos pedaços...
                                Se nao encontrar o nome, procure por nomes parecidos, proximos do escrito, como por exemplo:
                                    - Fazenda = faz.
                                    - Santa = Sta.
                                    - santo = Sto.
                                    - caso escreva o nome e nao tenha, como por exempo: isabel, porem no banco so tem izabel, faça a relacao de procurar nomes com partes do nome parecido e dizer que nao encontrou o nome X porem o nome Y pode ser a opção.
                                    - se entregar o total, fale o total para os nomes mais relacionado ao pedido ou o que mais apareceu e o total geral da consulta
                                    faça a consulta para que seja varios tipos de nomes possiveis, destrinchando o nome para varios nomes pequenos desse pedido.
                                Caso tenha mais de uma, mostre as opções e diga no plural juntamente com o nome caso tenha
                                ao pedir algo, veja se tem a coluna ativo e retorne apenas os ativos, dizendo X ativos e Y inativos e o total. Veja se essa pergunta tem mais relação com mais alguma tabela e veja se essa tabela estao ativos para os mesmos.
                                Construa a consulta SQL correta para obter os dados solicitados, colocando o nome do schema e o nome da tabela. 
                                Use apenas SELECT. 
                                A função DATE() não é adequada para calcular datas relativas. Em vez disso, você deve usar funções como CURDATE() e INTERVAL. 
                                Inclua apenas a consulta SQL mais apropriada gerada na resposta, limpando o texto e deixando somente a consulta. 
                                Não entregue a resposta com ```sql, entregue a consulta apenas. 
                                Entregue apenas o resultado da consulta em forma de texto, nada mais. 
                                Preste atenção para fazer a consulta correta para o schema correto e a tabela correta. 
                                Cuidado com a sintaxe, veja os nomes corretos de cada coluna para cada tabela e schema.
                                """
                            )

                            prompt_nome2 = f"{nomes_colunas_context2}\n\nPergunta: {column_names}"
                            response_nome2 = chat.send_message(prompt_nome2)
                            consulta_banco2 = response_nome2.text
                            consulta_banco2 = consulta_banco2.replace("```sql", "").replace("```", "").strip()  # Remover ```sql e ``` delimitadores
                            print('consulta_banco2\n',consulta_banco2)

                            await cur.execute(consulta_banco2)
                            results2 = await cur.fetchall()
                            
                            
                            result_str2 = "\n".join([str(row) for row in results2])

                            resultado_consulta_context2 = (
                                "Você é um assistente que ajuda com consultas a um banco de dados."
                                f"Entregue uma resposta formatada e descritiva do resultado {result_str2} da consulta do sql {consulta_banco2}, que esta no banco de dados {db_name}.{table_name}, usando a pergunta anterior {chat_history[chat_id]}"
                                "Envie em formato HTML, utilizando apenas as tags <b> e </b> para negrito para formatar o texto."
                                "Não crie tabela." 
                                "Não use a tag <p> </p>."
                                "Não use a tag <br> ou </br> ou <br/>"
                                "Inclua a resposta sem inventar dados."
                                "Não inclua o nome da tabela e nem do schema."
                                "Veja se teve mais algum pedido do usuario e complemente sem inventar dados."
                                "Pule linhas para ficar mais organizado"
                                "Caso o resultado entregue varios resultados, mostre o que mais teve relacao com a consulta ou que teve mais resultados parecidos."
                                "Entregue uma boa identação"
                            )
                            prompt_resultado_consulta2 = f"{resultado_consulta_context2}"
                            response_resultado_consulta2 = chat.send_message(prompt_resultado_consulta2)
                            resultado_consulta = response_resultado_consulta2.text
                    
                        else:
                            # Processamento adicional, se necessário
                            pass



                        

                    #    reports.append(f"<b>Schema: {db_name}</b>\n<b>Tabela: {table_name}</b>\n\n{resultado_consulta}")
                    reports.append(f"{resultado_consulta}")

                    chat_history[chat_id].append({"role": "assistant", "content": resultado_consulta})

                    # Junta todos os relatórios
                    final_report = "\n\n".join(reports)
                    print('final_report\n',final_report,'\n')

        except Exception as e:
            await bot.send_message(chat_id, f"Erro ao conectar com o banco de dados")
            print(f"Erro ao conectar com o banco de dados: {str(e)}")

            return
        finally:
            pool.close()
            await pool.wait_closed()
    else:
    #    final_report = "Desculpe, não consegui entender sua consulta. Poderia reformular sua pergunta?"

        erro_context = (
            "Você é um assistente que ajuda com perguntas em geral. "
            f"Interprete a pergunta do usuario em {query}"
    #        f"Interprete a pergunta do usuario em {query} usando a pergunta anterior {chat_history[chat_id]}"
    #        f"As perguntas e respostas estao em {chat_history[chat_id]}, use essas informacoes para formular uma resposta"
            f"Caso tenha a ver com banco de dados, interprete a pergunta e tente entregar a consulta sql para o usuario olhando em {tables} para entrgar a consulta com o schema e o nome da tabela"
            "Caso seja uma pergunta que nao tenha a ver com banco de dados, converse normalmente com o usuário, lembrando de perguntas anteriores para uma conversa mais organica."
        )
        prompt_erro = f"{erro_context}\n\nPergunta: {query}"
#        prompt_erro = f"perguntas e respostas estao em: {chat_history[chat_id]} \n\n{erro_context}\n\nPergunta: {query}"
        response_erro = chat.send_message(prompt_erro)
        resposta_erro = response_erro.text
        final_report = resposta_erro
        print(final_report)
        
    #    chat_history[chat_id].append({"role": "assistant", "content": resposta_erro})
            
    # Divide a mensagem em partes de no máximo 4096 caracteres
    parts = split_report(final_report)

    # Envia cada parte da mensagem separadamente
    for part in parts:
        await bot.send_message(chat_id, part, parse_mode='HTML')

# Função principal para rodar o bot
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(dp.start_polling())

'''