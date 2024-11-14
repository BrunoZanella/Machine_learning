# enviar_previsao.py
import sys
import json
from bot import enviar_previsao_valor_equipamento_alerta, create_pool, ultimos_alertas, alertas_enviados_previsao, hora_media_alerta_1, hora_media_alerta_saida, sem_mensagem_silenciado
import aiomysql

async def selecionar_GMG(pool):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT codigo, ativo FROM sup_geral.tipos_equipamentos WHERE classe = 'GMG'")
            resultados = await cursor.fetchall()
            codigos = [resultado['codigo'] for resultado in resultados]
        return codigos

async def obter_equipamentos_validos(tabelas, pool):
    codigos_GMG = await selecionar_GMG(pool)
    codigos_GMG_str = ', '.join(map(str, codigos_GMG))

    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            query_equipamentos = f"SELECT DISTINCT codigo FROM sup_geral.equipamentos WHERE cod_tipo_equipamento IN ({codigos_GMG_str}) AND ativo = 1"
            await cursor.execute(query_equipamentos)
            resultados_equipamentos = await cursor.fetchall()
            cod_equipamentos = [str(resultado['codigo']) for resultado in resultados_equipamentos]

            query_ultima_tabela = f"SELECT DISTINCT cod_equipamento FROM {tabelas}"
            await cursor.execute(query_ultima_tabela)
            resultados_ultima_tabela = await cursor.fetchall()
            cod_ultima_tabela = [str(resultado['cod_equipamento']) for resultado in resultados_ultima_tabela]

    cod_equipamentos_validos = list(set(cod_equipamentos) & set(cod_ultima_tabela))
    cod_equipamentos_validos = sorted([int(cod) for cod in cod_equipamentos_validos])

    return cod_equipamentos_validos

async def main():

    # Criar conexão com o banco
    pool = await create_pool()

    global ultimos_alertas
    global alertas_enviados_previsao
    global hora_media_alerta_1
    global hora_media_alerta_saida

    # Obtenha parâmetros necessários
    tabelas = 'sup_geral.leituras'
    cod_equipamentos = await obter_equipamentos_validos(tabelas, pool)
    cod_campo_especificados_processar_equipamentos = ['3','6','7','8','9','10', '11', '16', '19', '23', '24', '114', '21','76','25','20','77', '120']
    cod_campo_especificados = ['3', '114']

    # cod_equipamentos = json.loads(sys.argv[1])  # Carrega usando JSON
    # tabelas = sys.argv[2]
    # cod_campo_especificados = json.loads(sys.argv[3])
    
    await enviar_previsao_valor_equipamento_alerta(cod_equipamentos, tabelas, cod_campo_especificados, pool)

# Rodar o processo assíncrono
if __name__ == '__main__':
    import asyncio
    asyncio.run(main())