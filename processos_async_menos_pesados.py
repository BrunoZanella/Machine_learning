# processos_async_menos_pesados.py
import sys
import json
from bot import verificar_alarmes,verificar_e_excluir_linhas_expiradas, clean_temp_files, atualizar_usinas_usuario, adicionar_DataQuebra_FG, create_pool


async def main():

    # Criar conexão com o banco
    pool = await create_pool()

    await verificar_alarmes(pool)
    await verificar_e_excluir_linhas_expiradas(pool)
    await clean_temp_files(pool)
    await atualizar_usinas_usuario(pool)
    await adicionar_DataQuebra_FG(pool)

# Rodar o processo assíncrono
if __name__ == '__main__':
    import asyncio
    asyncio.run(main())