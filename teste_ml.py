import asyncio
import aiomysql
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import plotly.graph_objects as go
import streamlit as st
import pytz

# Configurar a página do Streamlit
st.set_page_config(
    page_title="Relatório de Quebras",
    page_icon=":bar_chart:",
    layout="wide",  # Pode ser "centered" ou "wide"
    initial_sidebar_state="expanded"  # Pode ser "expanded" ou "collapsed"
)

# Aplicar CSS personalizado
st.markdown("""
    <style>
    
    /* Centraliza a página inteira */
    .main-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        background-color: #0E1117;
        color: #FAFAFA;
    }

    /* Estilo da tabela */
    table {
        width: 80px;
        border-collapse: collapse;
        margin-top: 20px;
    }

    th, td {
        padding: 10px;
        text-align: center;
        border: 1px solid #ddd;
        white-space: nowrap; /* Garante que os textos caibam em uma linha */
        color: #FAFAFA;
    }

    th {
        background-color: #262730;
    }

    td.estado-parada {
        background-color: #FF4B4B;
        color: #FAFAFA;
    }

    td.estado-sem-parada {
        background-color: #97FF4B;
        color: #000000; /* Preto para 'Sem parada' */
    }

    td.estado-em-funcionamento {
        background-color: #FFFF00; /* Amarelo para 'Em funcionamento' */
        color: #000000; /* Preto para contraste */
    }

    /* Remove scroll da tabela */
    .dataframe-container {
        overflow-x: hidden;
    }

    /* Título */
    .stMarkdown h1 {
        color: #FF4B4B;
        text-align: center;
        font-size: 14px;  /* Ajuste o tamanho da fonte aqui */

    }

    /* Atualização */
    .stMarkdown p {
        text-align: center;
        color: #FAFAFA;
    }

    /* Background da sidebar */
    .css-1d391kg {
        background-color: #262730 !important;
    }

    /* Esconder cabeçalho padrão do Streamlit */
    header, .stApp > header {
        display: none;
    }

    /* Esconder rodapé padrão do Streamlit */
    footer, .stApp > footer {
        display: none;
    }

    </style>
    """, unsafe_allow_html=True)


async def fetch_data():

    # pool = await aiomysql.create_pool(
    #     host="192.168.4.50",
    #     user="bruno",
    #     password="superbancoml",
    #     db="machine_learning",
    #     minsize=1,
    #     maxsize=10
    # )

    # Cria o pool de conexão assíncrono usando as configurações do secrets.toml
    pool = await aiomysql.create_pool(
        host=st.secrets["mysql"]["host"],
        port=int(st.secrets["mysql"]["port"]),
        user=st.secrets["mysql"]["user"],
        password=st.secrets["mysql"]["password"],
        db=st.secrets["mysql"]["database"],
        minsize=st.secrets["mysql"]["minsize"],
        maxsize=st.secrets["mysql"]["maxsize"]
    )

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:

            # Calcular o horário de uma hora atrás
        #    one_hour_ago = datetime.now() - timedelta(hours=1)

            # Definir o fuso horário de São Paulo
            sao_paulo_tz = pytz.timezone('America/Sao_Paulo')

            # Calcular o horário de uma hora atrás no fuso horário de São Paulo
            now = datetime.now(sao_paulo_tz)
            one_hour_ago = now - timedelta(hours=1)
            now = now.strftime('%Y-%m-%d %H:%M:%S')
            
            # Query para obter o valor máximo das leituras nas últimas 1 hora
            query_max_value = f"""
                SELECT COUNT(COALESCE(valor_1, 0) + COALESCE(valor_2, 0) + COALESCE(valor_3, 0) + COALESCE(valor_4, 0) + COALESCE(valor_5, 0)) AS max_value
                FROM leituras_consecutivas
                WHERE cod_campo = 114 
                AND valor_1 > 0 
                AND valor_2 > 0 
                AND valor_3 > 0 
                AND valor_4 > 0 
                AND valor_5 > 0
                AND data_cadastro <= '{one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')}'
            """
            await cursor.execute(query_max_value)
            max_value_result = await cursor.fetchone()
            max_value = max_value_result[0] if max_value_result and max_value_result[0] not in (0, None) else 100

            # Query para contar a quantidade de equipamentos com data_cadastro_previsto ou data_cadastro_quebra no dia atual
            query_count_previsto_quebra = """
                SELECT COUNT(*) AS count_previsto_quebra
                FROM log_relatorio_quebras
                WHERE DATE(data_cadastro_previsto) = CURDATE()
                   OR DATE(data_cadastro_quebra) = CURDATE()
            """
            await cursor.execute(query_count_previsto_quebra)
            count_previsto_quebra_result = await cursor.fetchone()
            count_previsto = count_previsto_quebra_result[0] if count_previsto_quebra_result and count_previsto_quebra_result[0] is not None else 0

            # Query to obtain today's data from log_relatorio_quebras
            query_log = """
                SELECT CAST(cod_usina AS CHAR) AS cod_usina, 
                       CAST(cod_equipamento AS CHAR) AS cod_equipamento, 
                       data_cadastro_previsto, 
                       data_cadastro_quebra
                FROM log_relatorio_quebras
                WHERE DATE(data_cadastro_previsto) = CURDATE()
                   OR DATE(data_cadastro_quebra) = CURDATE()
            """
            await cursor.execute(query_log)
            log_data = await cursor.fetchall() or []

            # If log_data is empty, return empty DataFrame with columns and default values
            if not log_data:
                return pd.DataFrame(columns=['estado', 'alerta', 'tipo_alerta', 'nome_usina', 'nome_equipamento', 'cod_equipamento', 'data_cadastro_previsto', 'data_cadastro_quebra']), 0, max_value, count_previsto

            # Extracting unique cod_usina and cod_equipamento
            cod_usinas = {row[0] for row in log_data}
            cod_equipamentos = {row[1] for row in log_data}
            data_cadastro_previsto_map = {row[1]: row[2] for row in log_data}
            data_cadastro_quebra_map = {row[1]: row[3] for row in log_data}

            # Query to obtain names of usinas
            query_usinas = f"""
                SELECT CAST(codigo AS CHAR) AS codigo, nome AS nome_usina
                FROM sup_geral.usinas
                WHERE codigo IN %s
            """
            if cod_usinas:
                await cursor.execute(query_usinas, (tuple(cod_usinas),))
                usinas_data = await cursor.fetchall() or []
            else:
                usinas_data = []

                
            # Query to obtain names of equipamentos
            query_equipamentos = f"""
                SELECT CAST(codigo AS CHAR) AS codigo, nome AS nome_equipamento
                FROM sup_geral.equipamentos
                WHERE codigo IN %s
            """
            await cursor.execute(query_equipamentos, (tuple(cod_equipamentos),))
            equipamentos_data = await cursor.fetchall() or []


            # # Query para verificar os alertas e previsões em um intervalo de 300 segundos
            # tipo_alerta_data = []
            # query_tipo_alertas = """
            #     SELECT CAST(cod_equipamento AS CHAR) AS cod_equipamento, 
            #            data_cadastro_previsto, 
            #            alerta_80, 
            #            alerta_100, 
            #            previsao
            #     FROM valores_previsao
            #     WHERE cod_equipamento = %s 
            #       AND ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300
            # """
            # for cod_equipamento, data_previsto in data_cadastro_previsto_map.items():
            #     await cursor.execute(query_tipo_alertas, (cod_equipamento, data_previsto))
            #     result = await cursor.fetchall() or []
                
            #     # Verificação e formatação do alerta
            #     for row in result:
            #         alerta = None
            #         if row[2] == 1:  # alerta_80
            #             alerta = "80%"
            #         elif row[3] == 1:  # alerta_100
            #             alerta = "100%"
            #         elif row[4] == 1:  # previsao
            #             alerta = "Previsão"
                    
            #         if alerta:
            #             tipo_alerta_data.append({
            #                 'cod_equipamento': row[0],
            #             #    'data_cadastro_previsto': row[1],
            #                 'alerta': alerta
            #             })
        #    print('tipo_alerta_data',tipo_alerta_data)







            # Lista para acumular os alertas por equipamento
            # tipo_alerta_data = []


            # # Loop para verificar os alertas e previsões
            # for cod_equipamento, data_previsto in data_cadastro_previsto_map.items():
            #     data_quebra = data_cadastro_quebra_map.get(cod_equipamento)
                
            #     if data_quebra:  # Se data_cadastro_quebra existir
            #         query_tipo_alertas = """
            #             SELECT CAST(cod_equipamento AS CHAR) AS cod_equipamento, 
            #                 data_cadastro_previsto, 
            #                 alerta_80, 
            #                 alerta_100, 
            #                 previsao
            #             FROM valores_previsao
            #             WHERE cod_equipamento = %s 
            #             AND (ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300 
            #                 OR ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300)
            #             ORDER BY data_cadastro_previsto
            #         """
            #         await cursor.execute(query_tipo_alertas, (cod_equipamento, data_previsto, data_quebra))
            #     else:  # Se data_cadastro_quebra não existir, use a hora atual como limite
            #         query_tipo_alertas = """
            #             SELECT CAST(cod_equipamento AS CHAR) AS cod_equipamento, 
            #                 data_cadastro_previsto, 
            #                 alerta_80, 
            #                 alerta_100, 
            #                 previsao
            #             FROM valores_previsao
            #             WHERE cod_equipamento = %s 
            #             AND (ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300 
            #                 OR ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300)
            #             ORDER BY data_cadastro_previsto
            #         """
            #         await cursor.execute(query_tipo_alertas, (cod_equipamento, data_previsto, now))

            #     result = await cursor.fetchall() or []
                
            #     # Verificação e acumulação dos alertas únicos
            #     alertas = set()  # Usar um set para garantir que cada alerta apareça apenas uma vez
            #     for row in result:
            #         if row[2] == 1:  # alerta_80
            #             alertas.add("80%")
            #         if row[3] == 1:  # alerta_100
            #             alertas.add("100%")
            #         if row[4] == 1:  # previsao
            #             alertas.add("Previsão")
                
            #     # Adiciona os alertas, se existirem
            #     if alertas:
            #         tipo_alerta_data.append({
            #             'cod_equipamento': cod_equipamento,
            #             'alerta': ', '.join(sorted(alertas))  # Junta os alertas em ordem alfabética
            #         })



            # Dicionário para acumular os alertas por equipamento
            equipamento_alertas = {}

            # Loop para verificar os alertas e previsões
            for cod_equipamento, data_previsto in data_cadastro_previsto_map.items():
                data_quebra = data_cadastro_quebra_map.get(cod_equipamento)

                if data_quebra:  # Se data_cadastro_quebra existir
                    query_tipo_alertas = """
                        SELECT CAST(cod_equipamento AS CHAR) AS cod_equipamento, 
                            data_cadastro_previsto, 
                            alerta_80, 
                            alerta_100, 
                            previsao
                        FROM valores_previsao
                        WHERE cod_equipamento = %s 
                        AND (ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300 
                            OR ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300)
                        ORDER BY data_cadastro_previsto
                    """
                    await cursor.execute(query_tipo_alertas, (cod_equipamento, data_previsto, data_quebra))
                else:  # Se data_cadastro_quebra não existir, use a hora atual como limite
                    query_tipo_alertas = """
                        SELECT CAST(cod_equipamento AS CHAR) AS cod_equipamento, 
                            data_cadastro_previsto, 
                            alerta_80, 
                            alerta_100, 
                            previsao
                        FROM valores_previsao
                        WHERE cod_equipamento = %s 
                        AND data_cadastro_previsto BETWEEN (%s - INTERVAL 300 SECOND) AND (%s + INTERVAL 300 SECOND)
                        ORDER BY data_cadastro_previsto
                    """
                    await cursor.execute(query_tipo_alertas, (cod_equipamento, data_previsto, now))

                result = await cursor.fetchall() or []

                # Inicializa a lista de alertas para o equipamento se não existir
                if cod_equipamento not in equipamento_alertas:
                    equipamento_alertas[cod_equipamento] = set()

                # Adiciona os alertas ao set do equipamento
                for row in result:
                    if row[2] == 1:  # alerta_80
                        equipamento_alertas[cod_equipamento].add("80%")
                    if row[3] == 1:  # alerta_100
                        equipamento_alertas[cod_equipamento].add("100%")
                    if row[4] == 1:  # previsão
                        equipamento_alertas[cod_equipamento].add("Previsão")

                # Converta o set em string após o loop
                equipamento_alertas[cod_equipamento] = ', '.join(sorted(equipamento_alertas[cod_equipamento]))

            # Criar um dicionário para mapear cod_equipamento ao valor do alerta finalizado
            alerta_dict = {cod_equipamento: ', '.join(sorted(alertas)) if isinstance(alertas, set) else alertas
                            for cod_equipamento, alertas in equipamento_alertas.items()}



            # Query para obter os equipamentos com alerta = 1 e cod_campo = 114
            query_alertas = """
                SELECT CAST(cod_equipamento AS CHAR) AS cod_equipamento
                FROM leituras_consecutivas
                WHERE alerta = 1 AND cod_campo = 114
            """
            await cursor.execute(query_alertas)
            alertas_data = await cursor.fetchall() or []

            # Convertendo resultados para DataFrame
            df_log = pd.DataFrame(log_data, columns=['cod_usina', 'cod_equipamento', 'data_cadastro_previsto', 'data_cadastro_quebra'])
            df_usinas = pd.DataFrame(usinas_data, columns=['codigo', 'nome_usina'])
            df_equipamentos = pd.DataFrame(equipamentos_data, columns=['codigo', 'nome_equipamento'])
            df_alertas = pd.DataFrame(alertas_data, columns=['cod_equipamento'])

            # Converting cod_equipamento to the same type in both dataframes before merging
            df_log['cod_equipamento'] = df_log['cod_equipamento'].astype(int)
            df_equipamentos['codigo'] = df_equipamentos['codigo'].astype(int)
            df_alertas['cod_equipamento'] = df_alertas['cod_equipamento'].astype(int)

            # Mesclar para adicionar os nomes correspondentes e ocultar 'cod_usina'
            df_log = df_log.merge(df_usinas, how='left', left_on='cod_usina', right_on='codigo').drop(columns=['cod_usina', 'codigo'])
            df_log = df_log.merge(df_equipamentos, how='left', left_on='cod_equipamento', right_on='codigo').drop(columns=['codigo'])

            # Adicionar a coluna 'estado'
            df_log['estado'] = df_log['data_cadastro_quebra'].apply(lambda x: 'Parada' if pd.notna(x) else 'Sem parada')

            # Verificar se o equipamento está na lista de alertas
            df_log['alerta'] = df_log['cod_equipamento'].apply(lambda x: 'Sim' if x in df_alertas['cod_equipamento'].values else 'Não')

            # Ordenar os dados por data_cadastro_previsto (da mais antiga para a mais recente)
            df_log['data_cadastro_previsto'] = pd.to_datetime(df_log['data_cadastro_previsto'], errors='coerce')
            df_log = df_log.sort_values(by=['data_cadastro_previsto'], ascending=True)

            # Marcar alerta apenas na última linha de cada equipamento, se o equipamento estiver na lista de alertas
            def update_alerta(group):
                if group['alerta'].iloc[-1] == 'Sim':
                    group['alerta'] = 'Não'
                    group['alerta'].iloc[-1] = 'Sim'
                return group

            df_log = df_log.groupby('cod_equipamento').apply(update_alerta).reset_index(drop=True)

            # Adicionar a coluna 'tipo_alerta' ao DataFrame
            df_log['tipo_alerta'] = df_log['cod_equipamento'].astype(str).map(alerta_dict)


            # Ordenar as colunas na ordem desejada, incluindo 'tipo_alerta'
            df_log = df_log[['estado', 'alerta', 'tipo_alerta', 'nome_usina', 'nome_equipamento', 'cod_equipamento', 'data_cadastro_previsto', 'data_cadastro_quebra']]

            # Contar a quantidade de equipamentos com alerta = 1 e cod_campo = 114
            alerta_count = len(df_alertas)

    pool.close()
    await pool.wait_closed()

    # Retornar os valores de forma segura, com valores padrão se necessário
    return df_log, alerta_count, max_value, count_previsto



async def main():
    # Espaços reservados para os elementos que serão atualizados
    placeholder_logo = st.empty()
    placeholder_gauge = st.empty()
    placeholder_table = st.empty()

    # Definir o fuso horário de São Paulo
    tz_sao_paulo = pytz.timezone('America/Sao_Paulo')

    # Adicionar a logo no canto esquerdo
    with placeholder_logo.container():
        st.image('log_brg_novo_branco_2.png', width=150)

    while True:
        # Recuperar os dados
        data, alerta_count, max_value, count_previsto = await fetch_data()
        
        # Atualizar o gauge
        with placeholder_gauge.container():
            st.markdown('<div class="main-container">', unsafe_allow_html=True)
            st.markdown('<h2>Relatório de Quebras Diário</h2>', unsafe_allow_html=True)

            # Obter a hora atual em São Paulo
            now = datetime.now(tz_sao_paulo)
            st.write(f"Atualizado em: {now.strftime('%Y-%m-%d %H:%M:%S')}")

            fig = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=alerta_count,
                title={'text': "Equipamentos com Alerta"},
                gauge={
                    'axis': {'range': [0, max_value]},  # Intervalo do gauge com o valor máximo
                    'bar': {'color': "#FF4B4B"},  # Cor da barra do gauge
                    'bgcolor': "#262730",  # Cor de fundo
                    'steps': [
                        {'range': [0, max_value], 'color': "#262730"}  # Cor de fundo do gauge
                    ]
                },
                number={'font': {'size': 20, 'color': "#FAFAFA"}},  # Cor do texto do número
                delta={'reference': max_value, 'relative': True, 'position': "top"}  # Adiciona uma referência ao valor máximo

            ))
            # Adicionar uma anotação para o valor máximo
            fig.add_annotation(
                x=0.5,
                y=0.5,
                text=f"Máx: {max_value}",
                showarrow=False,
                font=dict(size=14, color="white"),
                align="center",
                xref="paper",
                yref="paper",
                opacity=0.8
            )


            # Ajustar o layout do gráfico para controlar o tamanho
            fig.update_layout(
                autosize=False,
                width=300,  # Ajuste a largura do gráfico
                height=150,  # Ajuste a altura do gráfico
                margin=dict(l=20, r=20, t=20, b=20)
            )

            st.plotly_chart(fig, use_container_width=True)


            # Exibir o número de equipamentos com data de cadastro prevista no dia atual
            st.markdown(f'**Quantidade de alerta diário:**<br>{count_previsto}', unsafe_allow_html=True)


        # Verificar se 'data' é um DataFrame
        if isinstance(data, pd.DataFrame):
            # Renomear as colunas
            data = data.rename(columns={
                'alerta': 'Em Alerta',
                'cod_equipamento': 'Código Equipamento',
                'nome_equipamento': 'Equipamento',
                'nome_usina': 'Usina',
                'data_cadastro_previsto': 'Data Cadastro Previsto',
                'data_cadastro_quebra': 'Data Cadastro Quebra',
                'estado': 'Estado',
                'tipo_alerta': 'Alerta'
            })

            # Atualizar a coluna 'Estado' para 'Em funcionamento' quando 'Alerta' for 'Sim'
            data['Estado'] = data.apply(lambda row: 'Em funcionamento' if row['Em Alerta'] == 'Sim' else row['Estado'], axis=1)

            # Remover as colunas 'Alerta', 'Data Cadastro Previsto', 'Data Cadastro Quebra'
            data = data.drop(columns=['Em Alerta', 'Data Cadastro Previsto', 'Data Cadastro Quebra'])

            # Remover o índice do DataFrame
            data.reset_index(drop=True, inplace=True)

            # Aplicar estilos condicionais
            def apply_styles(df):
                def color_estado(val):
                    if val == 'Parada':
                        return 'background-color: #FF4B4B; color: #FAFAFA;'  # Vermelho
                    elif val == 'Sem parada':
                        return 'background-color: #97FF4B; color: #000000;'  # Verde
                    elif val == 'Em funcionamento':
                        return 'background-color: #FFFF00; color: #000000;'  # Amarelo
                    else:
                        return ''

                # Aplica o estilo de cor de fundo para a coluna 'Estado'
                styled_df = df.style.applymap(color_estado, subset=['Estado'])

                # Centraliza o texto de todas as células
                styled_df = styled_df.set_properties(**{'text-align': 'center'})

                # Centraliza o texto do cabeçalho da tabela
                styled_df = styled_df.set_table_styles([{
                    'selector': 'th',
                    'props': [('text-align', 'center')]
                }])

                return styled_df

            # Atualizar a tabela existente

            with placeholder_table.container():
                st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            #    st.dataframe(apply_styles(data), use_container_width=True)
                st.table(apply_styles(data))
                st.markdown('</div>', unsafe_allow_html=True)


        # Atualiza os dados a cada 1 minuto
        await asyncio.sleep(60)

# Rodar o main loop de forma assíncrona
if __name__ == "__main__":
    asyncio.run(main())