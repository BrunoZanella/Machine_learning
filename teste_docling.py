import os
import logging
import time
from pathlib import Path
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from markdownify import markdownify as md
import fitz  # PyMuPDF
import tensorflow as tf

# Limitar o uso de GPU (se houver)
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"  # Desabilita o uso de GPU, caso contrário, limita a memória
os.environ["OMP_NUM_THREADS"] = "2"  # Limita para 2 núcleos de CPU
os.environ["MKL_NUM_THREADS"] = "2"


# Limita o uso de memória da GPU se a GPU estiver ativa
gpus = tf.config.experimental.list_physical_devices('GPU')
if gpus:
    try:
        tf.config.experimental.set_virtual_device_configuration(
            gpus[0],
            [tf.config.experimental.VirtualDeviceConfiguration(memory_limit=4096)])  # Limita a 4 GB de memória
    except RuntimeError as e:
        print(e)
        
logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)

# Caminho da pasta de entrada e saída
input_folder = r"/home/bruno/documentos"
output_folder = r"/home/bruno/documentos_docling"
os.makedirs(output_folder, exist_ok=True)

# Lista de arquivos PDF
pdf_files = [
    # "Apostila 2  - markdown.pdf",
    # "Apostila 3 - Gerado.pdf",
    # "Apostila 6 - Gerado.pdf",
    # "Alarmes e Possíveis Causas.pdf",
    # "Alarmes do motor.pdf",
    "47704191_BR Elétrico.pdf",
]

# Configuração do Docling
pipeline_options = PdfPipelineOptions()
pipeline_options.do_ocr = True
pipeline_options.do_table_structure = True
pipeline_options.table_structure_options.do_cell_matching = True
pipeline_options.ocr_options.lang = ["pt"]
#pipeline_options.table_structure_options.mode.TableFormerMode.ACCURATE

doc_converter = DocumentConverter(
    format_options={
        InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
    }
)

def convert_with_docling(pdf_path, output_path):
    """Converte PDF em Markdown usando o Docling."""
    try:
        start_time = time.time()
        result = doc_converter.convert(pdf_path)
        end_time = time.time() - start_time
        _log.info(f"Convertido com Docling: {pdf_path} ({end_time:.2f}s)")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(result.document.export_to_markdown())
    except Exception as e:
        _log.error(f"Erro ao processar {pdf_path} com Docling: {e}")

def convert_with_fitz(pdf_path, output_path):
    """Converte PDF em Markdown usando PyMuPDF e markdownify."""
    try:
        pdf_document = fitz.open(pdf_path)
        markdown_content = ""
        for page_num in range(len(pdf_document)):
            page = pdf_document[page_num]
            text = page.get_text("text")
            markdown_content += f"\n## Página {page_num + 1}\n"
            markdown_content += md(text)
        with open(output_path, "w", encoding="utf-8") as md_file:
            md_file.write(markdown_content)
        _log.info(f"Convertido com Fitz: {pdf_path}")
    except Exception as e:
        _log.error(f"Erro ao processar {pdf_path} com Fitz: {e}")

# Processa cada arquivo PDF
for file_name in pdf_files:
    pdf_path = os.path.join(input_folder, file_name)
    output_path = os.path.join(output_folder, f"{Path(file_name).stem}.md")
    
    if os.path.exists(pdf_path):
        # Tenta converter com Docling
        convert_with_docling(pdf_path, output_path)
        
        # Se Docling falhar, tenta com Fitz
        if not os.path.exists(output_path):
            convert_with_fitz(pdf_path, output_path)
    else:
        _log.warning(f"Arquivo não encontrado: {pdf_path}")

print("Processamento concluído!")
