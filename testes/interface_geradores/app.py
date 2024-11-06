from flask import Flask, render_template, jsonify
import mysql.connector
import re

from datetime import datetime
import time
from threading import Thread  # Importa Thread para execução paralela

app = Flask(__name__)

# Configuração do banco de dados
db_config = {
    'host': '192.168.4.50',
    'user': 'bruno',
    'password': 'superbancoml',
    'database': 'machine_learning'
}

# Mapeamento dos códigos dos campos para seus nomes
CAMPOS = {
    3: {'nome': 'Potência Ativa', 'min': None, 'max': None, 'ideal': None},
    320: {'nome': 'Potência Ativa previsto', 'min': None, 'max': None, 'ideal': None},
    16: {'nome': 'Frequência', 'min': 0.0, 'ideal': (59.8, 60.2), 'max': 61.5}, 
    19: {'nome': 'Tensão da Bateria', 'min': 9, 'max': 30, 'ideal': (12.5, 29.5)},
    20: {'nome': 'RPM', 'min': 0, 'ideal': (1799, 1801), 'max': 1850}, 
    21: {'nome': 'Pressão do Óleo', 'min': 0, 'max': 6, 'ideal': (4.5, 5.5)},
    23: {'nome': 'Consumo Instantâneo', 'min': None, 'max': None, 'ideal': None},
    24: {'nome': 'Horas de Operação', 'min': None, 'max': None, 'ideal': None},
    25: {'nome': 'Temperatura', 'min': 0, 'ideal': (30, 92), 'max': 102},
    76: {'nome': 'Temperatura do Ar de Admissão', 'min': None, 'max': None, 'ideal': None},
    77: {'nome': 'Pressão da Admissão', 'min': None, 'max': None, 'ideal': None},
    114: {'nome': 'Load Speed', 'min': None, 'max': None, 'ideal': None},
    411: {'nome': 'Load Speed previsto', 'min': None, 'max': None, 'ideal': None},
    120: {'nome': 'Potência Nominal', 'min': None, 'max': None, 'ideal': None}
}


def limpar_motor(marca, motor):
    """
    Limpa e padroniza o nome do motor e marca para combiná-lo com a tabela de parâmetros.
    Exemplo: Volvo Penta VOLVO TAD 1344GE -> Volvo TAD 13
    """
    # Ajuste condicional para a marca
    if "Volvo Penta" in marca:
        marca_limpa = "Volvo"  # Sempre padronizar como "Volvo"
    else:
        marca_limpa = marca.strip().upper()  # Mantém a marca em maiúsculas para os outros casos

    # Limpar o nome do motor de espaços em branco extras, hifens e colocar em maiúsculas
    motor_limpo = motor.strip().upper()

    # Remover duplicações de marca no motor (ex: Volvo Penta VOLVO TAD 1344 -> TAD 1344)
    motor_limpo = motor_limpo.replace(marca_limpa.upper(), "").strip()

    # Remover hífen ou outros caracteres especiais entre a marca e o motor
    motor_limpo = re.sub(r'[-]', ' ', motor_limpo).strip()

    # Remover caracteres extras após os primeiros dois dígitos principais do código do motor (ex: TAD1344GE -> TAD13)
    motor_limpo = re.sub(r'(\d{2})\d*[A-Za-z]*$', r'\1', motor_limpo)

    # Adicionar espaço entre letras e números (TAD13 -> TAD 13)
    motor_limpo = re.sub(r'([A-Za-z]+)(\d+)', r'\1 \2', motor_limpo)

    # Remover caracteres especiais e espaços em branco extras
    motor_limpo = re.sub(r'\s+', ' ', motor_limpo).strip()

    # Corrigir padrões específicos para Scania
    if 'SCANIA' in marca_limpa.upper():
        if re.match(r'^13$', motor_limpo):
            motor_limpo = 'DC 13'
        elif re.match(r'^SACANIA DC 1253 A$', motor_limpo) or re.match(r'^DC 1253 A$', motor_limpo):
            motor_limpo = 'DC 12'
        elif re.match(r'^DC 13\d{2}', motor_limpo):
            motor_limpo = 'DC 13'
        elif re.match(r'^DC 13 \d{2}', motor_limpo):
            motor_limpo = 'DC 13'
    
    # Corrigir padrões específicos para Volvo
    if 'VOLVO' in marca_limpa.upper():
        if re.match(r'^TAD 13', motor_limpo):
            motor_limpo = 'TAD 13'
        elif re.match(r'^TAD 16', motor_limpo):
            motor_limpo = 'TAD 16'

    # Combinar a marca e o motor de maneira padronizada
    motor_padronizado = f"{marca_limpa} {motor_limpo}".strip()

    return motor_padronizado

# Lista para armazenar os dados dos equipamentos
equipamentos_ativos = []

def carregar_equipamentos_ativos():
    """
    Função para carregar os equipamentos ativos do banco de dados e armazenar na lista `equipamentos_ativos`.
    """
    try:
        # Realiza a conexão com o banco de dados
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        # Consulta SQL para buscar os equipamentos ativos
        cursor.execute("""
            SELECT 
                e.codigo AS codigo_equipamento, 
                e.cod_usina, 
                u.nome AS nome_usina,  
                e.nome AS nome_equipamento,  
                e.motor, 
                COALESCE(
                    CASE 
                        WHEN e.motor = 'N/I' THEN '' 
                        WHEN e.motor LIKE '%TAD%' THEN 'Volvo'
                        WHEN e.motor LIKE '%TWD%' THEN 'Volvo'
                        WHEN e.motor LIKE '%DC%' THEN 'Scania'
                        WHEN e.motor LIKE '%NEF%' THEN 'FPT'
                        WHEN e.motor LIKE '%MWM%' THEN 'MWM'
                        WHEN e.motor LIKE '%PERKINS%' THEN 'Perkins'
                        ELSE ''
                    END, 
                    ''
                ) AS marca,
                e.potencia,  
                e.tensao     
            FROM 
                sup_geral.equipamentos e
            JOIN 
                sup_geral.usinas u ON e.cod_usina = u.codigo
            WHERE 
                u.ativo = 1
                AND e.motor IS NOT NULL
                AND e.ativo = 1
            GROUP BY 
                e.codigo, e.cod_usina, u.nome, e.nome, e.motor, e.potencia, e.tensao;
        """)

        # Armazenar o resultado da consulta na lista `equipamentos_ativos`
        equipamentos_ativos.clear()

        resultados = cursor.fetchall()

        for equipamento in resultados:
            # Desempacotando os valores, incluindo nome_equipamento
            codigo_equipamento, cod_usina, nome_usina, nome_equipamento, motor, marca, potencia, tensao = equipamento
            # Limpar e padronizar o nome do motor e da marca
            motor_padronizado = limpar_motor(marca, motor)
            # Armazenar os dados limpos na lista
            equipamentos_ativos.append((codigo_equipamento, cod_usina, nome_usina, nome_equipamento, motor_padronizado, marca, potencia, tensao))

        print("Equipamentos ativos carregados com sucesso!")

    except mysql.connector.Error as err:
        print(f"Erro ao carregar equipamentos: {err}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Armazenar parâmetros min/max globalmente
parametros_min_max_motores = {}

def carregar_parametros_min_max():
    """
    Carrega os parâmetros de min e max da tabela 'parametros_min_max_motores'.
    """
    global parametros_min_max_motores
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = """
            SELECT motor, parametro, min_alerta, max_alerta 
            FROM machine_learning.parametros_min_max_motores
        """
        cursor.execute(query)
        resultados = cursor.fetchall()

        # Armazenar resultados em um dicionário estruturado
        parametros_min_max_motores.clear()
        for motor, parametro, min_valor, max_valor in resultados:
            if motor not in parametros_min_max_motores:
                parametros_min_max_motores[motor] = {}
            parametros_min_max_motores[motor][parametro] = {
                'min': min_valor,
                'max': max_valor
            }

        print("Parâmetros min/max carregados com sucesso!")

    except mysql.connector.Error as err:
        print(f"Erro ao carregar parâmetros: {err}")

  
  
        
def conectar_bd():
    """Estabelece e retorna a conexão com o banco de dados."""
    return mysql.connector.connect(**db_config)

def inserir_valores_equipamentos_alerta():
    """Função que insere novos valores apenas se a data_cadastro for diferente e realiza previsões."""
    datas_previstas = set()  # Rastreia previsões já realizadas
    datas_previstas_320 = set()  # Rastreia previsões já realizadas

    while True:
        try:
            conn = conectar_bd()
            cursor = conn.cursor(dictionary=True, buffered=True)  # Usando cursor buffered

            # Busca os registros com alerta ativo
            cursor.execute("""
                SELECT cod_equipamento, cod_campo, valor_5, data_cadastro
                FROM leituras_consecutivas
                WHERE alerta = 1
            """)
            leituras = cursor.fetchall()  # Consome todos os resultados

            for leitura in leituras:
                cod_equipamento = leitura['cod_equipamento']
                cod_campo = leitura['cod_campo']
                valor_5 = leitura['valor_5']
                data_cadastro = leitura['data_cadastro']

                # Verifica se já existe um registro com a mesma data_cadastro
                cursor.execute("""
                    SELECT 1 FROM valores_equipamentos_alerta 
                    WHERE cod_equipamento = %s 
                    AND cod_campo = %s 
                    AND data_cadastro = %s
                """, (cod_equipamento, cod_campo, data_cadastro))
                registro_existe = cursor.fetchone()

                if not registro_existe:
                    cursor.execute("""
                        INSERT INTO valores_equipamentos_alerta 
                        (cod_equipamento, cod_campo, valor, data_cadastro)
                        VALUES (%s, %s, %s, %s)
                    """, (cod_equipamento, cod_campo, valor_5, data_cadastro))
                    conn.commit()

                    if cod_campo == 3 and (cod_equipamento, data_cadastro) not in datas_previstas:
                        cursor.execute("""
                            SELECT coeficiente, intercepto 
                            FROM coeficiente_geradores 
                            WHERE cod_equipamento = %s
                        """, (cod_equipamento,))
                        coeficientes = cursor.fetchone()

                        if coeficientes:
                            coeficiente = coeficientes['coeficiente']
                            intercepto = coeficientes['intercepto']

                            # Previsão: y = mx + b
                            valor_previsto = valor_5 * coeficiente + intercepto
                        #    print('previsao 411 -','cod_equipamento', cod_equipamento, 'valor_previsto', valor_previsto, 'valor_5', valor_5)

                            # Limita o valor previsto entre 0 e 100
                            valor_previsto = min(max(valor_previsto, 0), 100)

                            # Insere o valor previsto na tabela
                            cursor.execute("""
                                INSERT INTO valores_equipamentos_alerta 
                                (cod_equipamento, cod_campo, valor, data_cadastro)
                                VALUES (%s, %s, %s, %s)
                            """, (cod_equipamento, 411, valor_previsto, data_cadastro))
                            conn.commit()

                            # Marca a previsão como feita para esta data específica
                            datas_previstas.add((cod_equipamento, data_cadastro))


                    # Previsão para cod_campo = 25
                    if cod_campo == 25:
                        cursor.execute("""
                            SELECT coeficiente, intercepto 
                            FROM novos_coeficiente_geradores 
                            WHERE cod_equipamento = %s AND X = %s AND Y = %s
                        """, (cod_equipamento, 25, 3))
                        novos_coeficientes = cursor.fetchone()

                        if novos_coeficientes:
                            coeficiente = novos_coeficientes['coeficiente']
                            intercepto = novos_coeficientes['intercepto']

                            # Previsão: y = mx + b
                            valor_previsto = valor_5 * coeficiente + intercepto
                    #        print('previsao 320 -','cod_equipamento', cod_equipamento, 'valor_previsto', valor_previsto, 'valor_5', valor_5)

                            valor_previsto = max(valor_previsto, 0)

                            # Insere o valor previsto na tabela
                            cursor.execute("""
                                INSERT INTO valores_equipamentos_alerta 
                                (cod_equipamento, cod_campo, valor, data_cadastro)
                                VALUES (%s, %s, %s, %s)
                            """, (cod_equipamento, 320, valor_previsto, data_cadastro))
                            conn.commit()

                            # Marca a previsão como feita para esta data específica
                            datas_previstas_320.add((cod_equipamento, data_cadastro))
                            

            time.sleep(5)  # Aguardar 5 segundos antes de repetir

        except mysql.connector.Error as err:
            print(f"Erro ao inserir na tabela: {err}")
            time.sleep(5)  # Espera antes de tentar novamente

        finally:
            # Fecha o cursor e a conexão ao final de cada iteração
            if cursor:
                cursor.close()
            if conn and conn.is_connected():
                conn.close()



'''
@app.route('/equipamentos', methods=['GET']) 
def get_equipamentos_alerta():
    try:
        # Conectar ao banco de dados
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        # Converter as chaves do dicionário CAMPOS para uma tupla para a query SQL
        campos_tuple = tuple(CAMPOS.keys())

        # 1. Query para pegar os equipamentos em alerta
        query_alerta = """
            SELECT cod_equipamento
            FROM machine_learning.leituras_consecutivas
            WHERE alerta = 1;
        """

        # Executar a consulta para equipamentos em alerta
        cursor.execute(query_alerta)
        equipamentos_em_alerta = cursor.fetchall()

        # Se não houver equipamentos em alerta, retornar uma resposta vazia
        if not equipamentos_em_alerta:
            return jsonify({'ultimo_valor': [], 'historico': []}), 200

        # Extrair os códigos dos equipamentos em alerta
        cod_equipamentos_em_alerta = tuple(eq['cod_equipamento'] for eq in equipamentos_em_alerta)

        # 2. Query para pegar o último valor por campo e equipamento, incluindo o nome da usina
        query_ultimo_valor = f"""
            SELECT 
                eq.codigo AS cod_equipamento,
                eq.nome AS nome_equipamento,  -- Adiciona o nome do equipamento
                usina.nome AS nome_usina,      -- Adiciona o nome da usina
                vea.cod_campo, 
                vea.valor, 
                vea.data_cadastro
            FROM 
                machine_learning.valores_equipamentos_alerta AS vea
            INNER JOIN 
                sup_geral.equipamentos AS eq ON eq.codigo = vea.cod_equipamento
            INNER JOIN 
                sup_geral.usinas AS usina ON usina.codigo = eq.cod_usina  -- Assumindo que a tabela sup_geral.usinas existe
            INNER JOIN (
                SELECT cod_equipamento, cod_campo, MAX(data_cadastro) AS max_data
                FROM machine_learning.valores_equipamentos_alerta
                WHERE DATE(data_cadastro) = CURDATE()
                GROUP BY cod_equipamento, cod_campo
            ) AS ultimos ON vea.cod_equipamento = ultimos.cod_equipamento 
                        AND vea.cod_campo = ultimos.cod_campo 
                        AND vea.data_cadastro = ultimos.max_data
            WHERE 
                eq.ativo = 1 
                AND eq.codigo IN {cod_equipamentos_em_alerta}
                AND vea.cod_campo IN {campos_tuple};
        """

        # 3. Query para pegar o histórico do dia para gráficos, incluindo o nome da usina
        query_historico = f"""
            SELECT 
                eq.codigo AS cod_equipamento,
                eq.cod_usina AS cod_usina,
                eq.nome AS nome_equipamento,  -- Adiciona o nome do equipamento
                usina.nome AS nome_usina,      -- Adiciona o nome da usina
                vea.cod_campo, 
                vea.valor, 
                vea.data_cadastro
            FROM 
                machine_learning.valores_equipamentos_alerta AS vea
            INNER JOIN 
                sup_geral.equipamentos AS eq ON eq.codigo = vea.cod_equipamento
            INNER JOIN 
                sup_geral.usinas AS usina ON usina.codigo = eq.cod_usina  -- Assumindo que a tabela sup_geral.usinas existe
            WHERE 
                eq.ativo = 1 
                AND eq.codigo IN {cod_equipamentos_em_alerta}
                AND DATE(vea.data_cadastro) = CURDATE()
                AND vea.cod_campo IN {campos_tuple}
            ORDER BY vea.data_cadastro;
        """

        # Executar as queries
        cursor.execute(query_ultimo_valor)
        ultimo_valores = cursor.fetchall()

        cursor.execute(query_historico)
        historico_valores = cursor.fetchall()

        # Enriquecer os dados de último valor com 'min', 'max' e 'ideal'
        potencia_nominal = None

        for valor in ultimo_valores:
            cod_campo = valor['cod_campo']
            if cod_campo in CAMPOS:
                campo_info = CAMPOS[cod_campo]
                valor['nome'] = campo_info['nome']
                valor['min'] = campo_info['min']
                valor['max'] = campo_info['max']
                valor['ideal'] = campo_info['ideal']
                
                # Adiciona o nome da usina e do equipamento ao valor
                valor['titulo'] = f"Usina: {valor['nome_usina']} - Equipamento: {valor['nome_equipamento']}"

                # Se o campo for Potência Nominal, armazena seu valor
                if cod_campo == 120:  # Potência Nominal
                    potencia_nominal = valor['valor']

                # Ajusta o max da Potência Ativa se Potência Nominal estiver disponível
                if cod_campo == 3 and potencia_nominal is not None:  # Potência Ativa
                    valor['max'] = 0.9 * float(potencia_nominal)  # Define o max como 90% da Potência Nominal

                    # Define o ideal como entre 50% a 80% da Potência Nominal
                    valor['ideal'] = [
                        0.3 * float(potencia_nominal),  # 50% do valor da Potência Nominal
                        0.8 * float(potencia_nominal)   # 80% do valor da Potência Nominal
                    ]

        # Organizar os dados para o frontend
        dados = {
            'ultimo_valor': ultimo_valores,
            'historico': historico_valores,
        }

        return jsonify(dados), 200

    except mysql.connector.Error as err:
        return jsonify({'error no get': str(err)}), 500

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
'''


@app.route('/equipamentos', methods=['GET']) 
def get_equipamentos_alerta():
    try:
        # Conectar ao banco de dados
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        # Converter as chaves do dicionário CAMPOS para uma tupla para a query SQL
        campos_tuple = tuple(CAMPOS.keys())

        # 1. Query para pegar os equipamentos em alerta
        query_alerta = """
            SELECT cod_equipamento
            FROM machine_learning.leituras_consecutivas
            WHERE alerta = 1;
        """

        # Executar a consulta para equipamentos em alerta
        cursor.execute(query_alerta)
        equipamentos_em_alerta = cursor.fetchall()

        # Se não houver equipamentos em alerta, retornar uma resposta vazia
        if not equipamentos_em_alerta:
            return jsonify({'ultimo_valor': [], 'historico': []}), 200

        # Extrair os códigos dos equipamentos em alerta
        cod_equipamentos_em_alerta = tuple(eq['cod_equipamento'] for eq in equipamentos_em_alerta)

        # 2. Query para pegar o último valor por campo e equipamento, incluindo o nome da usina
        query_ultimo_valor = f"""
            SELECT 
                eq.codigo AS cod_equipamento,
                eq.nome AS nome_equipamento,  -- Adiciona o nome do equipamento
                usina.nome AS nome_usina,      -- Adiciona o nome da usina
                vea.cod_campo, 
                vea.valor, 
                vea.data_cadastro
            FROM 
                machine_learning.valores_equipamentos_alerta AS vea
            INNER JOIN 
                sup_geral.equipamentos AS eq ON eq.codigo = vea.cod_equipamento
            INNER JOIN 
                sup_geral.usinas AS usina ON usina.codigo = eq.cod_usina  -- Assumindo que a tabela sup_geral.usinas existe
            INNER JOIN (
                SELECT cod_equipamento, cod_campo, MAX(data_cadastro) AS max_data
                FROM machine_learning.valores_equipamentos_alerta
                WHERE DATE(data_cadastro) = CURDATE()
                GROUP BY cod_equipamento, cod_campo
            ) AS ultimos ON vea.cod_equipamento = ultimos.cod_equipamento 
                        AND vea.cod_campo = ultimos.cod_campo 
                        AND vea.data_cadastro = ultimos.max_data
            WHERE 
                eq.ativo = 1 
                AND eq.codigo IN {cod_equipamentos_em_alerta}
                AND vea.cod_campo IN {campos_tuple};
        """

        # 3. Query para pegar o histórico do dia para gráficos, incluindo o nome da usina
        query_historico = f"""
            SELECT 
                eq.codigo AS cod_equipamento,
                eq.cod_usina AS cod_usina,
                eq.nome AS nome_equipamento,  -- Adiciona o nome do equipamento
                usina.nome AS nome_usina,      -- Adiciona o nome da usina
                vea.cod_campo, 
                vea.valor, 
                vea.data_cadastro
            FROM 
                machine_learning.valores_equipamentos_alerta AS vea
            INNER JOIN 
                sup_geral.equipamentos AS eq ON eq.codigo = vea.cod_equipamento
            INNER JOIN 
                sup_geral.usinas AS usina ON usina.codigo = eq.cod_usina  -- Assumindo que a tabela sup_geral.usinas existe
            WHERE 
                eq.ativo = 1 
                AND eq.codigo IN {cod_equipamentos_em_alerta}
                AND DATE(vea.data_cadastro) = CURDATE()
                AND vea.cod_campo IN {campos_tuple}
            ORDER BY vea.data_cadastro;
        """

        # Executar as queries
        cursor.execute(query_ultimo_valor)
        ultimo_valores = cursor.fetchall()

        cursor.execute(query_historico)
        historico_valores = cursor.fetchall()

        # Dicionário para armazenar os min, max e ideais por equipamento e campo
        referencia_por_campo = {}

        # Enriquecer os dados de último valor com 'min', 'max' e 'ideal'
        potencia_nominal = None

        for valor in ultimo_valores:
            cod_equipamento = valor['cod_equipamento']
            cod_campo = valor['cod_campo']

            # Verifica se os valores de referência já foram calculados para este campo
            if cod_campo not in referencia_por_campo:
                if cod_campo in CAMPOS:
                    campo_info = CAMPOS[cod_campo]
                    referencia_por_campo[cod_campo] = {
                        'nome': campo_info['nome'],
                        'min': campo_info['min'],
                        'max': campo_info['max'],
                        'ideal': campo_info['ideal']
                    }

            # Adiciona os dados de referência ao valor atual
            if cod_campo in referencia_por_campo:
                valor.update(referencia_por_campo[cod_campo])

            # Adiciona o nome da usina e do equipamento ao valor
            valor['titulo'] = f"Usina: {valor['nome_usina']} - Equipamento: {valor['nome_equipamento']}"

            # Se o campo for Potência Nominal, armazena seu valor
            if cod_campo == 120:  # Potência Nominal
                potencia_nominal = valor['valor']

            # Ajusta o max da Potência Ativa se Potência Nominal estiver disponível
            if cod_campo == 3 and potencia_nominal is not None:  # Potência Ativa
                valor['max'] = 0.9 * float(potencia_nominal)  # Define o max como 90% da Potência Nominal
                valor['ideal'] = [
                    0.3 * float(potencia_nominal),  # 50% do valor da Potência Nominal
                    0.8 * float(potencia_nominal)   # 80% do valor da Potência Nominal
                ]

        # Organizar os dados para o frontend
        dados = {
            'ultimo_valor': ultimo_valores,
            'historico': historico_valores,
        }

        return jsonify(dados), 200

    except mysql.connector.Error as err:
        return jsonify({'error no get': str(err)}), 500

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()



@app.route('/')
def index():
    """Carrega a página principal."""
    return render_template('index.html')
#    return render_template('index_copy.html')

def iniciar_insercao_em_thread():
    """Inicia a função de inserção em uma thread separada."""
    thread = Thread(target=inserir_valores_equipamentos_alerta)
    thread.daemon = True  # A thread será finalizada quando o programa principal encerrar
    thread.start()

if __name__ == '__main__':
    iniciar_insercao_em_thread()  # Inicia o processo paralelo
    app.run(host='0.0.0.0', port=5000, debug=True)