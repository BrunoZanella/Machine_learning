import os
import pickle
from sklearn.linear_model import LinearRegression
import pandas as pd
import numpy as np
import pickle
from sklearn.linear_model import LinearRegression



# Função para carregar o modelo da pasta com X e Y
def carregar_modelo(cod_equipamento, cod_campo, cod_campo_secundario):
    nome_arquivo = f"modelos_de_ia/modelo_equipamento_2170_X_3_Y_114.pkl"
    if os.path.exists(nome_arquivo):
        with open(nome_arquivo, 'rb') as f:
            modelo_serializado = pickle.load(f)
        return modelo_serializado
    return None

# Carregar o modelo
modelo = carregar_modelo(1, 3, 4)

# Criar um DataFrame de exemplo com as colunas esperadas
X_test = pd.DataFrame({
    'valor_cod_campo_20': np.random.rand(100),
    'valor_cod_campo_21': np.random.rand(100),
    'valor_cod_campo_25': np.random.rand(100),
    'valor_cod_campo_76': np.random.rand(100),
    'valor_cod_campo_114': np.random.rand(100),
    'rounded_time': np.linspace(0, 10, 100)  # Criando a coluna 'rounded_time'
})

# Converter 'rounded_time' para o tipo datetime64, se necessário
X_test['rounded_time'] = pd.to_datetime(X_test['rounded_time'], unit='s')

# Verificar se o modelo foi carregado corretamente
if modelo is None:
    print("Modelo não foi carregado corretamente.")
else:
    if isinstance(modelo, LinearRegression):
        print("Coeficientes do modelo:", modelo.coef_)
        print("Intercepto do modelo:", modelo.intercept_)
    
    # Verificar se as colunas esperadas estão presentes no DataFrame
    colunas_esperadas = ['valor_cod_campo_20', 'valor_cod_campo_21', 'valor_cod_campo_25', 'valor_cod_campo_76', 'valor_cod_campo_114', 'rounded_time']
    
    if all(coluna in X_test.columns for coluna in colunas_esperadas):
        # Fazer a previsão com o modelo
        y_pred = modelo.predict(X_test[colunas_esperadas])
        
        # Plotar a previsão
        plt.figure(figsize=(10, 6))
        plt.plot(X_test['rounded_time'], y_pred, label='Previsão do Modelo', color='blue')
        plt.title('Visualização da Previsão do Modelo')
        plt.xlabel('rounded_time')
        plt.ylabel('Previsão')
        plt.legend()
        plt.show()
    else:
        print("As colunas esperadas não foram encontradas no DataFrame de entrada.")



# Agora vamos usar o modelo para fazer previsões e plotar os gráficos
import numpy as np
import matplotlib.pyplot as plt

# Dados de exemplo para fazer previsões
X_test = np.linspace(0, 10, 100).reshape(-1, 1)  # Criando alguns dados de teste
y_pred = modelo.predict(X_test)

# Plotar a previsão do modelo
plt.figure(figsize=(10, 6))
plt.plot(X_test, y_pred, label='Previsão do Modelo', color='blue')
plt.title('Visualização da Previsão do Modelo')
plt.xlabel('X_test')
plt.ylabel('Previsão')
plt.legend()
plt.show()


from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Suponha que você tenha os valores reais y_test
y_test = np.sin(X_test)  # Apenas um exemplo de valores reais

# Avaliar o modelo com métricas
r2 = r2_score(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)

print(f"R²: {r2}")
print(f"MAE: {mae}")
print(f"MSE: {mse}")
print(f"RMSE: {rmse}")

# Visualizar os erros em um gráfico
plt.figure(figsize=(10, 6))
plt.scatter(X_test, y_test, label="Valores Reais", color="red")
plt.plot(X_test, y_pred, label="Previsão do Modelo", color='blue')
plt.title('Comparação entre valores reais e previstos')
plt.xlabel('X_test')
plt.ylabel('Valores')
plt.legend()
plt.show()


from mpl_toolkits.mplot3d import Axes3D

# Exemplo de dados 3D
X_test_3d = np.linspace(0, 10, 100)
Y_test_3d = np.linspace(0, 5, 100)
X_test_3d, Y_test_3d = np.meshgrid(X_test_3d, Y_test_3d)

Z_pred = modelo.predict(np.c_[X_test_3d.ravel(), Y_test_3d.ravel()]).reshape(X_test_3d.shape)

# Criar um gráfico 3D
fig = plt.figure(figsize=(10, 6))
ax = fig.add_subplot(111, projection='3d')

# Plotar os dados e as previsões
ax.plot_surface(X_test_3d, Y_test_3d, Z_pred, cmap='viridis')

ax.set_xlabel('X1')
ax.set_ylabel('X2')
ax.set_zlabel('Previsão')
plt.title('Visualização em 3D do Modelo')
plt.show()


# Exemplo básico para registrar a perda durante o treinamento
losses = []

# Suponha que 'modelo' seja uma rede neural, e a função 'fit' aceita callbacks
for epoch in range(100):
    modelo.fit(X_train, y_train)
    
    # Simular uma função que retorna a perda
    loss = mean_squared_error(y_train, modelo.predict(X_train))
    losses.append(loss)

# Plotar a perda ao longo do treinamento
plt.figure(figsize=(10, 6))
plt.plot(losses)
plt.title('Evolução da perda durante o treinamento')
plt.xlabel('Épocas')
plt.ylabel('Perda')
plt.show()
