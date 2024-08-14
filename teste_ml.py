import asyncio
import aiomysql
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import plotly.graph_objects as go
import streamlit as st

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
        color: #000000; /* Preto para 'sem parada' */
    }

    td.estado-em-funcionamento {
        background-color: #FFFF00; /* Amarelo para 'em funcionamento' */
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

            if not log_data:
                return pd.DataFrame(columns=['estado', 'alerta', 'cod_equipamento', 'nome_equipamento', 'nome_usina', 'data_cadastro_previsto', 'data_cadastro_quebra']), 0, 100, 0

            # Extracting unique cod_usina and cod_equipamento
            cod_usinas = {row[0] for row in log_data}
            cod_equipamentos = {row[1] for row in log_data}
            data_cadastro_previsto_map = {row[1]: row[2] for row in log_data}

            # Query to obtain names of usinas
            query_usinas = f"""
                SELECT CAST(codigo AS CHAR) AS codigo, nome AS nome_usina
                FROM sup_geral.usinas
                WHERE codigo IN %s
            """
            await cursor.execute(query_usinas, (tuple(cod_usinas),))
            usinas_data = await cursor.fetchall() or []
                
            # Query to obtain names of equipamentos
            query_equipamentos = f"""
                SELECT CAST(codigo AS CHAR) AS codigo, nome AS nome_equipamento
                FROM sup_geral.equipamentos
                WHERE codigo IN %s
            """
            await cursor.execute(query_equipamentos, (tuple(cod_equipamentos),))
            equipamentos_data = await cursor.fetchall() or []


            # Query para verificar os alertas e previsões em um intervalo de 300 segundos
            tipo_alerta_data = []
            query_tipo_alertas = """
                SELECT CAST(cod_equipamento AS CHAR) AS cod_equipamento, 
                       data_cadastro_previsto, 
                       alerta_80, 
                       alerta_100, 
                       previsao
                FROM valores_previsao
                WHERE cod_equipamento = %s 
                  AND ABS(TIMESTAMPDIFF(SECOND, data_cadastro_previsto, %s)) < 300
            """
            for cod_equipamento, data_previsto in data_cadastro_previsto_map.items():
                await cursor.execute(query_tipo_alertas, (cod_equipamento, data_previsto))
                result = await cursor.fetchall() or []
                
                # Verificação e formatação do alerta
                for row in result:
                    alerta = None
                    if row[2] == 1:  # alerta_80
                        alerta = "80%"
                    elif row[3] == 1:  # alerta_100
                        alerta = "100%"
                    elif row[4] == 1:  # previsao
                        alerta = "Previsão"
                    
                    if alerta:
                        tipo_alerta_data.append({
                            'cod_equipamento': row[0],
                        #    'data_cadastro_previsto': row[1],
                            'alerta': alerta
                        })
                        
            print('tipo_alerta_data',tipo_alerta_data)
            
            # Query para obter os equipamentos com alerta = 1 e cod_campo = 114
            query_alertas = """
                SELECT CAST(cod_equipamento AS CHAR) AS cod_equipamento
                FROM leituras_consecutivas
                WHERE alerta = 1 AND cod_campo = 114
            """
            await cursor.execute(query_alertas)
            alertas_data = await cursor.fetchall() or []

            # Calcular o horário de uma hora atrás
            one_hour_ago = datetime.now() - timedelta(hours=1)

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
                AND data_cadastro >= '{one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')}'
            """
            await cursor.execute(query_max_value)
            max_value_result = await cursor.fetchone()
            max_value = max_value_result[0] if max_value_result and max_value_result[0] is not None else 100  # Padrão para 100 se não houver resultados

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
            df_log['estado'] = df_log['data_cadastro_quebra'].apply(lambda x: 'parada' if pd.notna(x) else 'sem parada')

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

            # Ordenar as colunas na ordem desejada
            df_log = df_log[['estado', 'alerta', 'cod_equipamento', 'nome_equipamento', 'nome_usina', 'data_cadastro_previsto', 'data_cadastro_quebra']]

            # Contar a quantidade de equipamentos com alerta = 1 e cod_campo = 114
            alerta_count = len(df_alertas)

    pool.close()
    await pool.wait_closed()

    # Retornar os valores de forma segura, com valores padrão se necessário
    return df_log, alerta_count, max_value, count_previsto



async def main():
    placeholder = st.empty()
    while True:
        # Recuperar os dados
        data, alerta_count, max_value, count_previsto = await fetch_data()

        # Sobrescrever a exibição no Streamlit
        with placeholder.container():
            st.markdown('<div class="main-container">', unsafe_allow_html=True)
            st.markdown('<h2>Relatório de Quebras Diário</h2>', unsafe_allow_html=True)
            st.write(f"Atualizado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


            # Exibir o gauge com a quantidade de alertas
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=alerta_count,
                title={'text': "Equipamentos com Alerta"},
                gauge={
                    'axis': {'range': [0, max_value]},  # Ajuste o intervalo do gauge conforme o valor máximo calculado
                    'bar': {'color': "#FF4B4B"},
                    'bgcolor': "#262730",
                    'steps': [
                        {'range': [0, max_value * 0.5], 'color': "#FF4B4B"},
                        {'range': [alerta_count , max_value], 'color': "#FF7F7F"}
                    ]
                },
                number={'font': {'size': 20, 'color': "#FAFAFA"}}
            ))

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
        

            # Aplicar estilos condicionais usando o estilo de DataFrame
            def apply_styles(df):
                def color_estado(val):
                    if val == 'parada':
                        return 'background-color: #FF4B4B; color: #FAFAFA;'  # Vermelho
                    elif val == 'sem parada':
                        return 'background-color: #97FF4B; color: #000000;'  # Verde
                    elif val == 'em funcionamento':
                        return 'background-color: #FFFF00; color: #000000;'  # Amarelo
                    else:
                        return ''

                # Aplica o estilo de cor de fundo para a coluna 'estado'
                styled_df = df.style.applymap(color_estado, subset=['estado'])

                # Centraliza o texto de todas as células
                styled_df = styled_df.set_properties(**{'text-align': 'center'})

                # Centraliza o texto do cabeçalho da tabela
                styled_df = styled_df.set_table_styles([{
                    'selector': 'th',
                    'props': [('text-align', 'center')]
                }])

                return styled_df


            # Atualizar a coluna 'estado' para 'em funcionamento' quando alerta for 'Sim'
            data['estado'] = data.apply(lambda row: 'em funcionamento' if row['alerta'] == 'Sim' else row['estado'], axis=1)

            # Remover a coluna 'alerta'
            data = data.drop(columns=['alerta', 'data_cadastro_previsto', 'data_cadastro_quebra'])

            # Exibir a tabela
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(apply_styles(data), use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

            # Atualiza os dados a cada 1 minuto
            await asyncio.sleep(60)

# Rodar o main loop de forma assíncrona
if __name__ == "__main__":
    asyncio.run(main())


