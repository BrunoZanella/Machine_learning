# enviar_previsao.py
import sys
import json
from bot import principal, novo_buscar_cod_equipamentos, novo_cod_usina, Energia_Equipamento, create_pool
import aiomysql


async def main():

    # Criar conexão com o banco
    pool = await create_pool()
    await principal(pool)

# Rodar o processo assíncrono
if __name__ == '__main__':
    import asyncio
    asyncio.run(main())