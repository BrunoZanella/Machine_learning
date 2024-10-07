import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, render_template
import time
import threading
import sqlite3
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# Flask app
app = Flask(__name__)

# URLs dos sites das loterias
urls = {
    "mega_sena": "https://loterias.caixa.gov.br/Paginas/Mega-Sena.aspx",
    "lotofacil": "https://loterias.caixa.gov.br/Paginas/Lotofacil.aspx",
    "quina": "https://loterias.caixa.gov.br/Paginas/Quina.aspx",
    "somatematica": "https://www.somatematica.com.br/megasenaResultados.php"
}

# Função para enviar email quando houver mudanças
def send_email(subject, body):
    message = MIMEMultipart()
    message["From"] = "zanellabruno7@gmail.com"
    message["To"] = "zanellabruno7@gmail.com"
    message["Subject"] = subject

    message.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login("zanellabruno7@gmail.com", "wron fcmr ugbj ufhb")
            server.sendmail(message["From"], message["To"], message.as_string())
            print("E-mail enviado com sucesso!")
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")



# Função para buscar os dados de um site específico
def fetch_data(site_name):
    url = urls[site_name]
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    print('url',url)
    print('response',response)
    print('soup',soup)

    # Extração do número do concurso e data
    concurso_info = soup.find('h2', class_='title')
    print('concurso_info',concurso_info)
    if concurso_info:
        concurso_text = concurso_info.text.strip()
        concurso_parts = concurso_text.split(' ')
        numero_concurso = concurso_parts[1] if len(concurso_parts) > 1 else "Concurso não encontrado"
        data_concurso = concurso_parts[2].strip('()') if len(concurso_parts) > 2 else "Data não encontrada"
    else:
        numero_concurso = "Concurso não encontrado"
        data_concurso = "Data não encontrada"

    # Extração dos números sorteados (verifique a classe correta no HTML)
    numeros = soup.find_all('li', class_='numero-sorteado')  # Atualize esta classe conforme o HTML real
    print('numeros',numeros)
    if numeros:
        numeros = [numero.text.strip() for numero in numeros]
    else:
        numeros = ["Números não encontrados"]

    # Verificação se acumulou
    acumulado = soup.find('h3', class_='resultado')
    print('acumulado',acumulado)
    if acumulado and "Acumulou" in acumulado.text:
        acumulado = "Acumulou!"
    else:
        acumulado = "Não acumulou"

    # Estimativa do prêmio
    premio_info = soup.find('p', class_='premio')
    print('premio_info',premio_info)
    if premio_info:
        premio_info = premio_info.text.strip()
    else:
        premio_info = "Prêmio não encontrado"

    return {
        "concurso": numero_concurso,
        "data": data_concurso,
        "numeros": numeros,
        "acumulado": acumulado,
        "premio": premio_info
    }

# Testando a função
result = fetch_data('mega_sena')
print(result)




# Função para configurar o banco de dados SQLite
def setup_database():
    conn = sqlite3.connect('loterias.db')
    cursor = conn.cursor()

    # Cria a tabela se não existir
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS concursos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            concurso TEXT NOT NULL,
            data TEXT NOT NULL,
            numeros TEXT NOT NULL,
            acumulado TEXT NOT NULL,
            premio TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

import json


def send_email_with_table(concursos):
    message = MIMEMultipart()
    message["From"] = "zanellabruno7@gmail.com"
    message["To"] = "zanellabruno7@gmail.com"
    message["Subject"] = "Resultados Atualizados das Loterias"

    # HTML com tabela formatada
    html = """
    <html>
    <head>
        <style>
            table {
                width: 100%;
                border-collapse: collapse;
            }
            th, td {
                padding: 10px;
                border: 1px solid #ddd;
                text-align: left;
            }
            th {
                background-color: #4CAF50;
                color: white;
            }
            tr:nth-child(even) {
                background-color: #f2f2f2;
            }
        </style>
    </head>
    <body>
        <h2>Resultados das Loterias</h2>
        <table>
            <thead>
                <tr>
                    <th>Nome</th>
                    <th>Concurso</th>
                    <th>Data</th>
                    <th>Números</th>
                    <th>Acumulado</th>
                    <th>Prêmio</th>
                </tr>
            </thead>
            <tbody>
    """

    # Adicionando os dados de cada concurso à tabela
    for site, data in concursos:
        numeros_str = ', '.join(data['numeros'])  # Converte lista de números em string
        html += f"""
        <tr>
            <td>{site.capitalize()}</td>
            <td>{data['concurso']}</td>
            <td>{data['data']}</td>
            <td>{numeros_str}</td>
            <td>{data['acumulado']}</td>
            <td>{data['premio']}</td>
        </tr>
        """

    # Finalizando o HTML
    html += """
            </tbody>
        </table>
    </body>
    </html>
    """

    # Anexar o corpo HTML ao e-mail
    message.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login("zanellabruno7@gmail.com", "wron fcmr ugbj ufhb")
            server.sendmail(message["From"], message["To"], message.as_string())
            print("E-mail enviado com sucesso!")
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")


# Função para salvar o concurso no banco de dados, sem enviar email
def save_concurso(site_name, data):
    conn = sqlite3.connect('loterias.db')
    cursor = conn.cursor()

    # Verifica se o concurso já existe no banco
    cursor.execute('SELECT numeros, acumulado, premio FROM concursos WHERE nome = ? AND concurso = ?', (site_name, data["concurso"]))
    result = cursor.fetchone()

    # Converte a lista de números em uma string (JSON formatado)
    numeros_str = json.dumps(data["numeros"])

    if result is None:
        # Insere um novo concurso, pois ele ainda não existe no banco de dados
        cursor.execute('''
            INSERT INTO concursos (nome, concurso, data, numeros, acumulado, premio)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (site_name, data["concurso"], data["data"], numeros_str, data["acumulado"], data["premio"]))
        conn.commit()
        conn.close()

        # Retorna que houve uma mudança (novo concurso)
        return True

    else:
        # Verifica se os dados do concurso mudaram (números, acumulado ou prêmio)
        numeros_db, acumulado_db, premio_db = result
        if numeros_str != numeros_db or data["acumulado"] != acumulado_db or data["premio"] != premio_db:
            # Atualiza o concurso com os novos dados, pois houve uma alteração
            cursor.execute('''
                UPDATE concursos
                SET numeros = ?, acumulado = ?, premio = ?
                WHERE nome = ? AND concurso = ?
            ''', (numeros_str, data["acumulado"], data["premio"], site_name, data["concurso"]))
            conn.commit()
            conn.close()

            # Retorna que houve uma alteração
            return True

    conn.close()
    # Retorna que não houve alteração
    return False


# Função para monitorar mudanças nos três sites e enviar um único e-mail
def monitor_sites():
    while True:
        concursos_list = []  # Lista para armazenar concursos alterados

        for site in urls:
            current_data = fetch_data(site)

            if current_data:
                # Salva o concurso no banco de dados e verifica se houve mudança
                concurso_alterado = save_concurso(site, current_data)

                if concurso_alterado:
                    # Se houve alteração, acumula os dados para enviar no e-mail
                    concursos_list.append((site, current_data))

        # Enviar o e-mail apenas se houver concursos com mudanças
        if concursos_list:
            send_email_with_table(concursos_list)

        # Pausa o monitoramento por 60 segundos
        time.sleep(60)




# Rota principal para exibir a interface HTML com os resultados
@app.route('/')
def index():
    conn = sqlite3.connect('loterias.db')
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM concursos ORDER BY id DESC LIMIT 10')
    concursos = cursor.fetchall()
    conn.close()

    return render_template('index.html', concursos=concursos)

# Rota da API para consultar os últimos resultados em formato JSON
@app.route('/status', methods=['GET'])
def get_status():
    conn = sqlite3.connect('loterias.db')
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM concursos ORDER BY id DESC LIMIT 3')
    concursos = cursor.fetchall()
    conn.close()

    return jsonify({"concursos": concursos})

# Thread para executar o monitoramento sem bloquear o Flask
def start_monitoring():
    monitor_thread = threading.Thread(target=monitor_sites)
    monitor_thread.start()

if __name__ == '__main__':
    setup_database()  # Configura o banco de dados antes de iniciar o monitoramento
    start_monitoring()

    # Executa o servidor Flask
    app.run(debug=True, use_reloader=False)
