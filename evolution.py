
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
#    url = f"{url_base}/message/sendText/Suporte_BRG"
    url = f"{url_base}/message/sendList/Suporte_BRG"

    try:
        response = requests.post(
            url,
            json=payload,
            headers={
                "apikey": "k3v14ilstiguaumoz8nzt",
                "Content-Type": "application/json"
            }
        )

        if response.status_code in [200, 201]:
            print("Mensagem enviada com sucesso.")
        else:
            print(f"Erro ao enviar mensagem. Código: {response.status_code}, Resposta: {response.text}")
    except Exception as e:
        print("Erro ao enviar mensagem:", e)





# Função principal que conecta ao banco e envia mensagens
async def main():
    # Criar pool de conexões com o banco de dados
    pool = await create_pool()

    cod_usuario = 374  # Exemplo de cod_usuario, pode ser passado como parâmetro

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Verificar se a usina está ativa
                await cursor.execute("SELECT ativo FROM machine_learning.usuarios_telegram WHERE cod_usuario = %s", (cod_usuario,))
                usina_ativa_row = await cursor.fetchone()

                if usina_ativa_row and usina_ativa_row[0] == 1:
                    # Buscar dados do usuário específico
                    await cursor.execute(
                        "SELECT id_telegram, usuario, ativo, telefone "
                        "FROM machine_learning.usuarios_telegram WHERE cod_usuario = %s",
                        (cod_usuario,)
                    )
                    result = await cursor.fetchone()

                    if result:
                        id_telegram, nome_usuario, ativo, telefone = result

                        # Formatar telefone
                        telefone_formatado = telefone.strip().replace(" ", "").replace("-", "")
                        if not telefone_formatado.startswith("+"):
                            telefone_formatado = f"+{telefone_formatado}"

                        # Criar payload para envio

                        payload = {
                            "number": telefone_formatado,
                            "ticke_id": f"3",
                            "listMessage": {
                            "title": "Usina: 623 - FAZ. ÁGUA SANTA\n\n",
                            "description": (
                                "🔴‼️ Equipamento: 2722 (G21 - CLIENTE):\n\n"
                                "Valores Atuais: 100.0, 100.0, 100.0, 100.0, 100.0\n"
                                "Valores Previstos: 48.0, 48.0, 50.8, 49.1, 49.1\n\n"
                                "Alerta: O load speed está em 100%.\n\n"
                            ),
                                "footerText": (
                                    "Para acompanhar acesse o link:\n"
                                    "https://supervisorio.brggeradores.com.br/beta/detalhesusinaover.php?codUsina=623\n\n"
                                ),
                            "buttonText": "Lista dos equipamentos",
                            "sections": [
                                {
                                    "title": "Equipamentos da usina FAZ. ÁGUA SANTA:",
                                    "rows": [
                                        {
                                            "title": "Equipamento G24 - cliente",
                                            "description": "https://supervisorio.brggeradores.com.br/beta/detalhesgmg.php?codUsina=623&codEquip=2725",
                                            "rowId": f"2725" # passando o ticketId para recuperar mais fácil na hora de enviar a aprovação
                                        },
                                        {
                                            "title": "Equipamento G10",
                                            "description": "https://supervisorio.brggeradores.com.br/beta/detalhesgmg.php?codUsina=623&codEquip=2678",
                                            "rowId": f"2678" # passando o ticketId para recuperar mais fácil na hora de enviar a aprovação
                                        }
                                    ]
                                }
                            ]
                            },
                            "options": {
                            "delay": 1200,
                            "presence": "composing"
                            },
                            "quoted": {
                                "key": {
                                    "fromMe": True,
                                    "type":"Chamado solucionado"
                                }
                            }
                        }

                        print(f"Mensagem preparada para {nome_usuario} com telefone {telefone_formatado}")
                        # Aqui você pode chamar a função para enviar a mensagem usando o payload
                        # await send_message(payload)

                    else:
                        print(f"Nenhum usuário encontrado com cod_usuario {cod_usuario}")
                else:
                    print(f"Usina associada ao cod_usuario {cod_usuario} não está ativa.")
            except Exception as e:
                print(f"Erro ao processar usuário {cod_usuario}: {e}")

        
        start_chat(payload)

# Executa a função principal assíncrona
if __name__ == "__main__":
    asyncio.run(main())




# payload = {
#     "number": telefone_formatado,
#     "textMessage": {
#         "text": (
#             f"Usina: 623 - FAZ. ÁGUA SANTA\n\n"
#             f"🔴‼️ Equipamento: 2722 (G21 - CLIENTE):\n\n"
#             f"Valores Atuais: 100.0, 100.0, 100.0, 100.0, 100.0\n"
#             f"Valores Previstos: 48.0, 48.0, 50.8, 49.1, 49.1\n\n"
#             f"Alerta: O load speed está em 100%."
#         ),
#     },
# }
                        
# payload = {
#     "number": telefone_formatado,
#     "options": {
#         "delay": 1200,
#         "presence": "composing"
#     },
#     "contactMessage": [
#         {
#             "fullName": "Contact Name",
#             "wuid": "559999999999",
#             "phoneNumber": "+55 99 9 9999-9999",
#             "organization": "Company Name",
#             "email": "email",
#             "url": "url page"
#         },
#         {
#             "fullName": "Contact Name",
#             "wuid": "559911111111",
#             "phoneNumber": "+55 99 9 1111-1111",
#             "organization": "Company Name",
#             "email": "email",
#             "url": "url page"
#         }
#     ]
# }


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
        