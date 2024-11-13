# processar_equipamentos.py
import sys
import json
from bot import monitorar_leituras_consecutivas, create_pool


async def main():

    # Criar conexão com o banco
    pool = await create_pool()

    await monitorar_leituras_consecutivas(pool)

# Rodar o processo assíncrono
if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
