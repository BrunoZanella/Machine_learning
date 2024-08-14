import streamlit as st
from pdf2image import convert_from_bytes
import os
import google.generativeai as genai
from PIL import Image


API_KEY = 'AIzaSyDfcjDbVZ2BbPJEuSpJ4wF_ATguRuffikA'
genai.configure(api_key=API_KEY)

def get_gemini_response(input, images, query=None):
    model = genai.GenerativeModel("gemini-pro-vision")
    if query is not None:
        response = model.generate_content([input] + images + [query])
    else:
        response = model.generate_content([input] + images)

    return response.text

def input_resumedata(uploaded_file):
    if uploaded_file is not None:
        pdf_bytes = uploaded_file.read()
        images = convert_from_bytes(pdf_bytes)
        return images
    else:
        raise FileNotFoundError("Nenhum arquivo enviado")

st.set_page_config(page_title="Contrados BRG Geradores")

st.header("Leitor de contrados BRG Geradores")

uploaded_file = st.file_uploader("Escolha um arquivo...", type=["pdf"])

image_data = None

if uploaded_file is not None:
    image_data = input_resumedata(uploaded_file)
    for i, img in enumerate(image_data):
        st.image(img, caption=f'Página {i + 1}', use_column_width=True)
    query = st.text_input(label="Quais detalhes você deseja do contrado?")
    query_button = st.button("Obter resposta")

    submit = st.button("Extraia todos os dados")

    input_prompt = """
        Você é um analisador de contrado de geradores. Você deve extrair os dados das imagens do contrado.
        Você terá que responder às perguntas com base nas imagens do contrado inserido, retornando dados importantes sobre o contrado.
        retorne preco data da emissao, numero da nota, cliente, vendedor.
    """

    if submit and image_data is not None:
        response = get_gemini_response(input_prompt, image_data)
        st.subheader("A resposta é:")
        st.write(response)

    if query_button and image_data is not None:
        response = get_gemini_response(input_prompt, image_data, query)
        st.subheader("A resposta é:")
        st.write(response)
