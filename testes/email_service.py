import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def enviar_email_tabela(dados):
    remetente = "zanellabruno7@gmail.com"
    destinatario = "zanellabruno7@gmail.com"
    senha = "wron fcmr ugbj ufhb"
    
    # Montar o conteúdo do email
    mensagem = MIMEMultipart()
    mensagem['From'] = remetente
    mensagem['To'] = destinatario
    mensagem['Subject'] = "Atualização dos Resultados das Loterias"
    
    # Corpo do email com tabela em HTML, estilizado
    tabela_html = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background-color: #f9f9f9;
                color: #333333;
                padding: 20px;
            }}
            h2 {{
                color: #2c3e50;
                font-size: 24px;
                text-align: center;
                margin-bottom: 20px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 0 auto;
                background-color: #ffffff;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
                border-radius: 8px;
                overflow: hidden;
            }}
            th {{
                background-color: #2980b9;
                color: #ffffff;
                text-transform: uppercase;
                padding: 12px 15px;
                text-align: center;
            }}
            td {{
                padding: 10px 15px;
                text-align: center;
                border-bottom: 1px solid #dddddd;
                font-size: 14px;
            }}
            tr:nth-child(even) {{
                background-color: #f2f2f2;
            }}
            tr:hover {{
                background-color: #d9edf7;
            }}
            .footer {{
                text-align: center;
                margin-top: 20px;
                font-size: 12px;
                color: #999999;
            }}
        </style>
    </head>
    <body>
        <h2>Atualização dos Concursos de Loterias</h2>
        <table>
            <thead>
                <tr>
                    <th>Tipo Jogo</th>
                    <th>Número</th>
                    <th>Data Apuração</th>
                    <th>Dezenas Sorteadas</th>
                    <th>Próximo concurso</th>
                    <th>Acumulou</th>

                </tr>
            </thead>
            <tbody>
                {"".join([f"<tr><td>{d['tipo_jogo']}</td><td>{d['numero_concurso']}</td><td>{d['data_apuracao']}</td><td>{d['dezenas_sorteadas']}</td><td>{d['proximo_concurso']}</td><td>{d['acumulou']}</td></tr>" for d in dados])}
            </tbody>
        </table>

        <div class="footer">
            <p>Este email foi enviado automaticamente. Por favor, não responda.</p>
            <p>&copy; 2024 Loterias. Todos os direitos reservados.</p>
        </div>
    </body>
    </html>
    """

    mensagem.attach(MIMEText(tabela_html, 'html'))

    # Enviar o email
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(remetente, senha)
        server.sendmail(remetente, destinatario, mensagem.as_string())
