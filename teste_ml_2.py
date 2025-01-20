from transformers import pipeline
from PIL import Image
import requests
from io import BytesIO

# Configurando o pipeline
nlp = pipeline("document-question-answering", model="impira/layoutlm-document-qa")

# URL da imagem
image_url = "https://miro.medium.com/max/787/1*iECQRIiOGTmEFLdWkVIH2g.jpeg"

# Baixando a imagem
response = requests.get(image_url)
response.raise_for_status()  # Verifica se o download foi bem-sucedido

# Convertendo a imagem para um formato compatível
image = Image.open(BytesIO(response.content)).convert("RGBA")

# Executando o pipeline
result = nlp(image, "Qual é o valor da compra?")
print(result)


'''

from transformers import pipeline

nlp = pipeline(
    "document-question-answering",
    model="impira/layoutlm-document-qa",
)

nlp(
    "https://templates.invoicehome.com/invoice-template-us-neat-750px.png",
    "What is the invoice number?"
)
# {'score': 0.9943977, 'answer': 'us-001', 'start': 15, 'end': 15}

nlp(
    "https://miro.medium.com/max/787/1*iECQRIiOGTmEFLdWkVIH2g.jpeg",
    "What is the purchase amount?"
)
# {'score': 0.9912159, 'answer': '$1,000,000,000', 'start': 97, 'end': 97}

nlp(
    "https://www.accountingcoach.com/wp-content/uploads/2013/10/income-statement-example@2x.png",
    "What are the 2020 net sales?"
)
# {'score': 0.59147286, 'answer': '$ 3,750', 'start': 19, 'end': 20}
'''