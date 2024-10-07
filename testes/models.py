from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from flask_login import UserMixin

db = SQLAlchemy()

class Concurso(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tipo_jogo = db.Column(db.String(50))
    numero_concurso = db.Column(db.String(50))
    data_apuracao = db.Column(db.String(50))
    dezenas_sorteadas = db.Column(db.String(255))
    ultima_atualizacao = db.Column(db.DateTime, default=datetime.utcnow)
    proximo_concurso = db.Column(db.String(255))
    acumulou = db.Column(db.String(255))

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password = db.Column(db.String(60), nullable=False)