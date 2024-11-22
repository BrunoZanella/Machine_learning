
import requests
import aiomysql
import asyncio
import time

# Função para criar o pool de conexões MySQL
async def create_pool():
    pool = await aiomysql.create_pool(
        host="192.168.4.50",  # Endereço do banco de dados
        user="bruno",  # Usuário do banco
        password="superbancoml",  # Senha do banco
        db="machine_learning",  # Nome do banco de dados
        minsize=1,
        maxsize=10
    )
    return pool

# Função para obter os dados dos usuários que atendem as condições
async def get_usuarios_telegram(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # Consulta os dados da tabela usuarios_telegram com as condições desejadas
            await cursor.execute("""
                SELECT nome_telegram, telefone
                FROM usuarios_telegram
                WHERE ativo = 1 AND bloqueado = 0 AND somente_telefone = 1
            """)
            result = await cursor.fetchall()
            return result

# Função para enviar a mensagem
def start_chat(payload):
    url_base = "http://192.168.15.60:8080"
#    url = f"{url_base}/message/sendList/Suporte_BRG"
    url = f"{url_base}/message/sendText/Suporte_BRG"

    try:
        response = requests.request(
            "POST",
            url,
            json=payload,
            headers={
                "apikey": "k3v14ilstiguaumoz8nzt",  # Chave de API
                "Content-Type": "application/json"
            }
        )

        if response.status_code == 200:
            data = response.json()
            # Verifica se a mensagem está pendente
            if data.get("status") == "PENDING":
                print(f"A mensagem está na fila (PENDING). Aguardando entrega...")
                message_id = data['message']['id']
                check_message_status(message_id)
            else:
                print("Mensagem enviada com sucesso:", data)
        else:
            print(f"Erro ao enviar mensagem. Código: {response.status_code}, Resposta: {response.text}")

    except Exception as e:
        print("Erro ao enviar mensagem:", e)


# Função para verificar o status da mensagem após o envio
def check_message_status(message_id):
    url_base = "http://192.168.15.60:8080"
    url = f"{url_base}/message/status/{message_id}"

    # Espera um tempo antes de verificar o status novamente
    time.sleep(10)  # Aguarda 10 segundos (ajuste conforme necessário)

    try:
        # Faz uma nova requisição para verificar o status
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            status = data.get("status", "UNKNOWN")

            if status == "DELIVERED":
                print("Mensagem entregue com sucesso!")
            elif status == "PENDING":
                print("A mensagem ainda está na fila. Tentando novamente...")
                # Se o status ainda for PENDING, podemos verificar novamente após outro tempo
                check_message_status(message_id)
            else:
                print(f"Mensagem com status desconhecido: {status}")
        else:
            print(f"Erro ao verificar status. Código: {response.status_code}, Resposta: {response.text}")

    except Exception as e:
        print("Erro ao verificar status:", e)

# Função principal que conecta ao banco e envia mensagens
async def main():
    # Criar pool de conexões com o banco de dados
    pool = await create_pool()

    # Obter usuários que atendem às condições
    usuarios = await get_usuarios_telegram(pool)

    # Enviar mensagem para cada usuário
    for usuario in usuarios:
        nome_telegram, telefone = usuario

        telefone_formatado = telefone.strip().replace(" ", "").replace("-", "")
        if not telefone_formatado.startswith("+"):
            telefone_formatado = f"+{telefone_formatado}"
            
        payload = {
            "number": telefone_formatado,
            "textMessage": {
                "text": (
                #    f"Enviada para: {nome_telegram}\n\n"
                    f"Usina: 623 - FAZ. ÁGUA SANTA\n\n"
                    f"🔴‼️ Equipamento: 2722 (G21 - CLIENTE):\n\n"
                    f"Valores Atuais: 100.0, 100.0, 100.0, 100.0, 100.0\n"
                    f"Valores Previstos: 48.0, 48.0, 50.8, 49.1, 49.1\n\n"
                    f"Alerta: O load speed está em 100%."
                #    f"---\n"  # Separador visual para o rodapé
                #    f"Para acompanhar acesse o link:\n"
                #    f"https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina=623\n\n"
                ),
            }
        }



        # payload = {
        #     "number": telefone_formatado,
        #     "ticke_id": f"3",
        #     "listMessage": {
        #     "title": "Usina: 623 - FAZ. ÁGUA SANTA\n\n",
        #     "description": (
        #         "🔴‼️ Equipamento: 2722 (G21 - CLIENTE):\n\n"
        #         "Valores Atuais: 100.0, 100.0, 100.0, 100.0, 100.0\n"
        #         "Valores Previstos: 48.0, 48.0, 50.8, 49.1, 49.1\n\n"
        #         "Alerta: O load speed está em 100%.\n\n"
        #     ),
        #         "footerText": (
        #             "Para acompanhar acesse o link:\n"
        #             "https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina=623\n\n"
        #         ),
        #     "buttonText": "Lista dos equipamentos",
        #     "sections": [
        #         {
        #             "title": "Equipamentos da usina FAZ. ÁGUA SANTA:",
        #             "rows": [
        #                 {
        #                     "title": "Equipamento G24 - cliente",
        #                     "description": "https://supervisorio.brggeradores.com.br/beta/detalhesgmg.php?codUsina=623&codEquip=2725",
        #                     "rowId": f"2725" # passando o ticketId para recuperar mais fácil na hora de enviar a aprovação
        #                 },
        #                 {
        #                     "title": "Equipamento G10",
        #                     "description": "https://supervisorio.brggeradores.com.br/beta/detalhesgmg.php?codUsina=623&codEquip=2678",
        #                     "rowId": f"2678" # passando o ticketId para recuperar mais fácil na hora de enviar a aprovação
        #                 }
        #             ]
        #         }
        #     ]
        #     },
        #     "options": {
        #     "delay": 1200,
        #     "presence": "composing"
        #     },
        #     "quoted": {
        #         "key": {
        #             "fromMe": True,
        #             "type":"Chamado solucionado"
        #         }
        #     }
        # }
        
        
        start_chat(payload)

# Executa a função principal assíncrona
if __name__ == "__main__":
    asyncio.run(main())




'''
import requests
import aiomysql



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


def start_chat(payload):
    url_base = "http://192.168.15.60:8080"

    url = f"{url_base}/message/sendText/Suporte_BRG"

    try:
        response = requests.request(
            "POST",
            url,
            json=payload,
            headers={
                "apikey": "k3v14ilstiguaumoz8nzt", 
                "Content-Type": "application/json"
            }
        )
                
        if response.status_code == 200:
            data = response.json()
            print("Mensagem enviada com sucesso:", data)
        else:
            print(f"Erro ao enviar mensagem. Código: {response.status_code}, Resposta: {response.text}")
    
    except Exception as e:
        print("Erro ao enviar mensagem:", e)

if __name__ == "__main__":
    payload = {
        "number": "5562982957089",
        "textMessage": {  
            "text": "Olá, este é um teste de chat." 
        }
    }
    start_chat(payload)

'''