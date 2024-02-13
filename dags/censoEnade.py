from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime

import os
import requests
import shutil
import zipfile
import pandas as pd

from datetime import datetime

# Definição de variáveis globais
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
DATA_ATUAL = "{{ ds }}"
CAMINHO_DATALAKE = f'{AIRFLOW_HOME}/datalake'
CAMADA_BRONZE = f'{CAMINHO_DATALAKE}/bronze/date={DATA_ATUAL}'
CAMADA_PRATA = f'{CAMINHO_DATALAKE}/silver/date={DATA_ATUAL}'
CAMADA_OURO = f'{CAMINHO_DATALAKE}/gold/date={DATA_ATUAL}'

# Argumentos padrão da DAG
default_args = {
    'owner': 'Néfi Fernandes',
    'depends_on_past': False,
    'retries': 0,
    'start_date': datetime(2024, 2, 6),
    'schedule_interval': '0 0 1 * *',
}

# Definição das funções

def download_zip_file(url, output_dir, zip_file_name):
    # Função para fazer o download de um arquivo zipado
    os.makedirs(output_dir, exist_ok=True)
    response = requests.get(url, verify=False)
    zip_file_path = os.path.join(output_dir, zip_file_name)
    with open(zip_file_path, 'wb') as f:
        f.write(response.content)
    return zip_file_path

def extract_zip_file(zip_file_path, extract_dir):
    # Função para extrair os arquivos zipados
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

def select_files(input_dir, output_dir, subfolder, files_to_select):
    # Função para selecionar os arquivos de interesse
    subfolder_path = os.path.join(input_dir, subfolder)
    for file_name in files_to_select:
        source_file_path = os.path.join(subfolder_path, file_name)
        if os.path.exists(source_file_path):
            shutil.copy(source_file_path, output_dir)

def download_xlsx_file(url, output_dir, year):
    # Função para fazer o download do arquivo XLSX
    os.makedirs(output_dir, exist_ok=True)
    response = requests.get(url, verify=False)
    xlsx_file_path = os.path.join(output_dir, f'conceito_enade_{year}.xlsx')
    with open(xlsx_file_path, 'wb') as f:
        f.write(response.content)
    return xlsx_file_path

def rename_files_in_directory(input_dir, output_dir, file_prefix):
    # Função para renomear arquivos em um diretório
    files = os.listdir(input_dir)
    for file in files:
        if file.startswith(file_prefix):
            input_file_path = os.path.join(input_dir, file)
            output_file_path = os.path.join(output_dir, file.replace('.txt', '.csv'))
            os.rename(input_file_path, output_file_path)
            print(f'O arquivo {file} foi renomeado para {file.replace(".txt", ".csv")}.')

def convert_xlsx_to_csv(input_file, output_file):
    # Função para converter arquivo XLSX em CSV
    df = pd.read_excel(input_file)
    df.to_csv(output_file, index=False)

def rename_censo_dir(input_dir, output_dir, year):
    # Função para renomear o diretório do censo
    new_dir_name = f'censo_ed_superior_{year}'
    if os.path.exists(os.path.join(output_dir, new_dir_name)):
        print(f'O diretório {new_dir_name} já existe. Continuando para a próxima etapa.')
        return
    files = os.listdir(input_dir)
    for file in files:
        if 'Microdados do Censo da Educação Superior' in file:
            for item in os.listdir(input_dir):
                item_path = os.path.join(input_dir, item)
                if os.path.isfile(item_path):
                    os.unlink(item_path)
                else:
                    shutil.rmtree(item_path)
            os.rename(os.path.join(input_dir, file), os.path.join(output_dir, new_dir_name))
            break

# Definição da DAG
with DAG('censoEnade', default_args=default_args) as dag:

    # Criação dos diretórios
    cria_diretorios_2021 = BashOperator(
        task_id='cria_diretorios_2021',
        bash_command=f"mkdir -p {CAMADA_BRONZE} {CAMADA_PRATA} {CAMADA_OURO}",
    )

    # Definição das tarefas
    download_enade_2021 = PythonOperator(
        task_id='download_enade_2021',
        python_callable=download_zip_file,
        op_kwargs={'url': 'https://download.inep.gov.br/microdados/microdados_enade_2021.zip', 'output_dir': '/opt/airflow/dataset', 'zip_file_name': 'microdados_enade_2021.zip'},
    )

    download_censo_2021 = PythonOperator(
        task_id='download_censo_2021',
        python_callable=download_zip_file,
        op_kwargs={'url': 'https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2021.zip', 'output_dir': '/opt/airflow/dataset', 'zip_file_name': 'microdados_censo_da_educacao_superior_2021.zip'},
    )

    download_conceito_2021_xlsx = PythonOperator(
        task_id='download_conceito_2021',
        python_callable=download_xlsx_file,
        op_kwargs={'url': 'https://download.inep.gov.br/educacao_superior/indicadores/resultados/2021/conceito_enade_2021.xlsx', 'output_dir': '/opt/airflow/dataset', 'year': 2021},
    )

    extract_enade_2021 = PythonOperator(
        task_id='extract_enade_2021',
        python_callable=extract_zip_file,
        op_kwargs={'zip_file_path': '/opt/airflow/dataset/microdados_enade_2021.zip', 'extract_dir': '/opt/airflow/dataset/diretorio_de_extracao'},
    )

    extract_censo_2021 = PythonOperator(
        task_id='extract_censo_2021',
        python_callable=extract_zip_file,
        op_kwargs={'zip_file_path': '/opt/airflow/dataset/microdados_censo_da_educacao_superior_2021.zip', 'extract_dir': '/opt/airflow/dataset/diretorio_de_extracao'},
    )

    rename_censo_dir_task = PythonOperator(
        task_id='rename_censo_dir',
        python_callable=rename_censo_dir,
        op_kwargs={'input_dir': '/opt/airflow/dataset/diretorio_de_extracao', 'output_dir': '/opt/airflow/dataset/diretorio_de_extracao', 'year': 2021},
    )

    select_enade_2021 = PythonOperator(
        task_id='select_enade_2021',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/dataset/diretorio_de_extracao/microdados_Enade_2021_LGPD','subfolder': '2.DADOS', 'files_to_select': ['microdados2021_arq1.txt', 'microdados2021_arq2.txt'], 'output_dir': '/opt/airflow/dataset/diretorio_de_selecao',},
    )  

    select_censo_2021 = PythonOperator(
        task_id='select_censo_2021',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/dataset/diretorio_de_extracao/censo_ed_superior_2021', 'output_dir': '/opt/airflow/dataset/diretorio_de_selecao', 'subfolder': 'dados', 'files_to_select': ['MICRODADOS_CADASTRO_IES_2021.CSV', 'MICRODADOS_CADASTRO_CURSOS_2021.CSV']},
    ) 

    rename_files_in_select_dir_task = PythonOperator(
        task_id='rename_files_in_select_dir',
        python_callable=rename_files_in_directory,
        op_kwargs={'input_dir': '/opt/airflow/dataset/diretorio_de_selecao', 'output_dir': '/opt/airflow/dataset/diretorio_de_selecao', 'file_prefix': 'microdados2021_arq'},
    )

    convert_xlsx_to_csv_task = PythonOperator(
        task_id='convert_xlsx_to_csv',
        python_callable=convert_xlsx_to_csv,
        op_kwargs={'input_file': '/opt/airflow/dataset/conceito_enade_2021.xlsx', 'output_file': '/opt/airflow/dataset/diretorio_de_selecao_conceito_enade_2021.csv'},
    )

    # Define as dependências entre as tarefas
    cria_diretorios_2021 >> [download_enade_2021, download_censo_2021, download_conceito_2021_xlsx]
    download_enade_2021 >> extract_enade_2021
    download_censo_2021 >> extract_censo_2021
    download_conceito_2021_xlsx >> convert_xlsx_to_csv_task
    extract_enade_2021 >> select_enade_2021
    extract_censo_2021 >> rename_censo_dir_task >> select_censo_2021
    select_enade_2021 >> rename_files_in_select_dir_task
