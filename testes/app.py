from flask import Flask, render_template, jsonify
from flask_sqlalchemy import SQLAlchemy
from loteria_service import buscar_e_atualizar_dados
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, render_template, redirect, url_for, flash
from flask_bcrypt import Bcrypt
from flask_wtf import FlaskForm
from models import db, User, Concurso  # Ajuste o modelo de User para incluir seu modelo
from forms import LoginForm, RegisterForm  # Importe os formulários que você criou

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///loterias.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = 'zanella'  # Necessário para Flask-WTF

db.init_app(app)
bcrypt = Bcrypt(app)

# Função para criar o banco de dados, se ainda não existir
with app.app_context():
    db.create_all()

@app.route('/register', methods=['GET', 'POST'])
def register():
    form = RegisterForm()
    if form.validate_on_submit():
        hashed_password = bcrypt.generate_password_hash(form.password.data).decode('utf-8')
        new_user = User(email=form.email.data, password=hashed_password)
        db.session.add(new_user)
        db.session.commit()
        flash('Sua conta foi criada! Agora você pode fazer login.', 'success')
        return redirect(url_for('login'))
    return render_template('register.html', form=form)

@app.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if user and bcrypt.check_password_hash(user.password, form.password.data):
            flash('Login realizado com sucesso!', 'success')
            return redirect(url_for('index'))
        else:
            flash('Falha no login. Verifique suas credenciais.', 'danger')
    return render_template('login.html', form=form)

@app.route('/atualizar_concursos', methods=['GET'])
def atualizar_concursos():
    buscar_e_atualizar_dados()
    return jsonify({"message": "Concursos atualizados e email enviado, se houver mudanças."})

@app.route('/')
def index():
    concursos = Concurso.query.all()
    return render_template('index2.html', concursos=concursos)

# Função para agendar a atualização automática
def agendar_atualizacao():
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=buscar_e_atualizar_dados, trigger="interval", minutes=5)
    scheduler.start()

    # Impedir o scheduler de parar junto com a aplicação Flask
    import atexit
    atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    agendar_atualizacao()
    app.run(debug=True)
#    app.run(host='192.168.2.2', port=5000, debug=True)

