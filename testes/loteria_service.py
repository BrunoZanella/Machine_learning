

'''
from loteria_caixa import MegaSena, LotoFacil, Quina
from models import db, Concurso
from email_service import enviar_email_tabela
from datetime import datetime

def buscar_e_atualizar_dados():
    from app import app

    with app.app_context():
        concursos = [MegaSena(), LotoFacil(), Quina()]
        dados_atualizados = []
        houve_mudanca = False  # Flag para verificar se houve alteração

        for concurso in concursos:
            print('concurso',concurso,'dados_atualizados',dados_atualizados,'houve_mudanca',houve_mudanca)
            tipo_jogo = concurso.tipoJogo()
            numero_concurso = concurso.numero()
            data_apuracao = concurso.dataApuracao()
            dezenas_sorteadas = ', '.join(concurso.listaDezenas())
            proximo_concurso = concurso.dataProximoConcurso()
            acumulou = "Acumulou" if concurso.acumulado() else "Não"

            # Verifica se o concurso já existe no banco pelo tipo_jogo e número do concurso
            concurso_db = Concurso.query.filter_by(tipo_jogo=tipo_jogo, numero_concurso=numero_concurso).first()

            if concurso_db:
                # Verifica se algum dado mudou
                if (concurso_db.dezenas_sorteadas != dezenas_sorteadas or
                    concurso_db.data_apuracao != data_apuracao or
                    concurso_db.proximo_concurso != proximo_concurso or
                    concurso_db.acumulou != acumulou):
                    
                    # Atualiza os dados existentes
                    concurso_db.dezenas_sorteadas = dezenas_sorteadas
                    concurso_db.data_apuracao = data_apuracao
                    concurso_db.proximo_concurso = proximo_concurso
                    concurso_db.acumulou = acumulou
                    concurso_db.ultima_atualizacao = datetime.now()

                    houve_mudanca = True  # Marcar que houve uma alteração
                    dados_atualizados.append({
                        'tipo_jogo': tipo_jogo,
                        'numero_concurso': numero_concurso,
                        'data_apuracao': data_apuracao,
                        'dezenas_sorteadas': dezenas_sorteadas,
                        'proximo_concurso': proximo_concurso,
                        'acumulou': acumulou,
                    })
            else:
                # Adicionar um novo concurso caso não exista
                novo_concurso = Concurso(
                    tipo_jogo=tipo_jogo,
                    numero_concurso=numero_concurso,
                    data_apuracao=data_apuracao,
                    dezenas_sorteadas=dezenas_sorteadas,
                    proximo_concurso=proximo_concurso,
                    acumulou=acumulou,
                )
                db.session.add(novo_concurso)
                houve_mudanca = True  # Concurso novo, houve alteração
                dados_atualizados.append({
                    'tipo_jogo': tipo_jogo,
                    'numero_concurso': numero_concurso,
                    'data_apuracao': data_apuracao,
                    'dezenas_sorteadas': dezenas_sorteadas,
                    'proximo_concurso': proximo_concurso,
                    'acumulou': acumulou,
                })

        # Commit no banco de dados para salvar as alterações
        db.session.commit()

        # Enviar email apenas se houve mudanças
        if houve_mudanca:
            print('email enviado')
            enviar_email_tabela(dados_atualizados)

'''

from loteria_caixa import MegaSena, LotoFacil, Quina
from models import db, Concurso
from email_service import enviar_email_tabela
from datetime import datetime

def buscar_e_atualizar_dados():
    from app import app

    with app.app_context():
        concursos = [MegaSena(), LotoFacil(), Quina()]
        dados_atualizados = []
        houve_mudanca = False  # Flag para verificar se houve alteração

        for concurso in concursos:
            tipo_jogo = concurso.tipoJogo()
            numero_concurso = concurso.numero()
            data_apuracao = concurso.dataApuracao()
            dezenas_sorteadas = ', '.join(concurso.listaDezenas())
            proximo_concurso = concurso.dataProximoConcurso()
            acumulou = "Acumulou" if concurso.acumulado() else "Não"

            # Verifica se o concurso já existe no banco pelo tipo_jogo e número do concurso
        #    concurso_db = Concurso.query.filter_by(tipo_jogo=tipo_jogo, numero_concurso=numero_concurso).first()

            # Verifica se o concurso já existe no banco pelo tipo_jogo (somente o tipo de jogo)
            concurso_db = Concurso.query.filter_by(numero_concurso=numero_concurso).first()

            if concurso_db:
                print('concurso',concurso,'dados_atualizados',dados_atualizados,'houve_mudanca',houve_mudanca)
                # Verifica se algum dado mudou
                if (concurso_db.dezenas_sorteadas != dezenas_sorteadas or
                    concurso_db.data_apuracao != data_apuracao or
                    concurso_db.proximo_concurso != proximo_concurso or
                    concurso_db.acumulou != acumulou):
                    
                    # Atualiza os dados existentes
                    concurso_db.dezenas_sorteadas = dezenas_sorteadas
                    concurso_db.data_apuracao = data_apuracao
                    concurso_db.proximo_concurso = proximo_concurso
                    concurso_db.acumulou = acumulou
                    concurso_db.ultima_atualizacao = datetime.now()

                    houve_mudanca = True  # Marcar que houve uma alteração
                    dados_atualizados.append({
                        'tipo_jogo': tipo_jogo,
                        'numero_concurso': numero_concurso,
                        'data_apuracao': data_apuracao,
                        'dezenas_sorteadas': dezenas_sorteadas,
                        'proximo_concurso': proximo_concurso,
                        'acumulou': acumulou,
                    })
            else:
                # Adicionar um novo concurso caso não exista
                novo_concurso = Concurso(
                    tipo_jogo=tipo_jogo,
                    numero_concurso=numero_concurso,
                    data_apuracao=data_apuracao,
                    dezenas_sorteadas=dezenas_sorteadas,
                    proximo_concurso=proximo_concurso,
                    acumulou=acumulou,
                )
                db.session.add(novo_concurso)
                houve_mudanca = True  # Concurso novo, houve alteração
                dados_atualizados.append({
                    'tipo_jogo': tipo_jogo,
                    'numero_concurso': numero_concurso,
                    'data_apuracao': data_apuracao,
                    'dezenas_sorteadas': dezenas_sorteadas,
                    'proximo_concurso': proximo_concurso,
                    'acumulou': acumulou,
                })

        # Commit no banco de dados para salvar as alterações
        db.session.commit()

        # Enviar email apenas se houve mudanças
        if houve_mudanca:
            print('email enviado')
            enviar_email_tabela(dados_atualizados)

