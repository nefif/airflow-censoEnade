from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime

import os
import requests
import shutil
import zipfile
import pandas as pd
import logging
import boto3

from datetime import datetime, timedelta

data_atual = datetime.now().strftime("%Y-%m-%d")

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
    'start_date': datetime(2024, 2, 16),
    'schedule_interval': '0 0 1* *',
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

def unificacao_limpeza_dir(year):
    
    # Usar a data atual para construir o caminho para os arquivos
    caminho_base = '/opt/airflow/datalake/silver'
    caminho_arquivo_instituicao_ensino = f'{caminho_base}/date={data_atual}/microdados{year}_arq1.csv'
    caminho_arquivo_sexo = f'{caminho_base}/date={data_atual}/microdados{year}_arq5.csv'
    caminho_arquivo_notas = f'{caminho_base}/date={data_atual}/microdados{year}_arq3.csv'
    caminho_arquivo_idade = f'{caminho_base}/date={data_atual}/microdados{year}_arq6.csv'
    caminho_arquivo_estado_civil = f'{caminho_base}/date={data_atual}/microdados{year}_arq7.csv'
    caminho_arquivo_cor = f'{caminho_base}/date={data_atual}/microdados{year}_arq8.csv'
    caminho_arquivo_info_residencia = f'{caminho_base}/date={data_atual}/microdados{year}_arq12.csv'
    caminho_arquivo_quantidade_residencia = f'{caminho_base}/date={data_atual}/microdados{year}_arq13.csv'
    caminho_arquivo_renda = f'{caminho_base}/date={data_atual}/microdados{year}_arq14.csv'
    caminho_arquivo_financeiro = f'{caminho_base}/date={data_atual}/microdados{year}_arq15.csv'
    caminho_arquivo_trabalho = f'{caminho_base}/date={data_atual}/microdados{year}_arq16.csv'
    caminho_arquivo_financiamento = f'{caminho_base}/date={data_atual}/microdados{year}_arq17.csv'
    caminho_arquivo_auxilio = f'{caminho_base}/date={data_atual}/microdados{year}_arq18.csv'
    caminho_arquivo_bolsa = f'{caminho_base}/date={data_atual}/microdados{year}_arq19.csv'
    caminho_arquivo_exterior= f'{caminho_base}/date={data_atual}/microdados{year}_arq20.csv'
    caminho_arquivo_politica_afirmativa = f'{caminho_base}/date={data_atual}/microdados{year}_arq21.csv'
    caminho_arquivo_horas_estudo = f'{caminho_base}/date={data_atual}/microdados{year}_arq29.csv'
    caminho_arquivo_censo_cursos = f'{caminho_base}/date={data_atual}/MICRODADOS_CADASTRO_CURSOS_{year}.CSV'
    caminho_arquivo_censo_ies = f'{caminho_base}/date={data_atual}/MICRODADOS_CADASTRO_IES_{year}.CSV'
    
    # Adicione os outros caminhos de arquivo conforme necessário

    # Carregar os DataFrames
    df_instituicao_ensino = pd.read_csv(caminho_arquivo_instituicao_ensino, delimiter=";", encoding='latin1',low_memory=False)
    df_sexo = pd.read_csv(caminho_arquivo_sexo, delimiter=";", encoding='latin1',low_memory=False)
    df_notas = pd.read_csv(caminho_arquivo_notas, delimiter=";", encoding='latin1',low_memory=False)
    df_idade = pd.read_csv(caminho_arquivo_idade, delimiter=";", encoding='latin1',low_memory=False)
    df_estado_civil = pd.read_csv(caminho_arquivo_estado_civil, delimiter=";", encoding='latin1',low_memory=False)
    df_cor = pd.read_csv(caminho_arquivo_cor, delimiter=";", encoding='latin1',low_memory=False)
    df_info_residencia = pd.read_csv(caminho_arquivo_info_residencia, delimiter=";", encoding='latin1',low_memory=False)
    df_quantidade_residencia = pd.read_csv(caminho_arquivo_quantidade_residencia, delimiter=";", encoding='latin1',low_memory=False)
    df_renda = pd.read_csv(caminho_arquivo_renda, delimiter=";", encoding='latin1',low_memory=False)
    df_financeiro = pd.read_csv(caminho_arquivo_financeiro, delimiter=";", encoding='latin1',low_memory=False)
    df_trabalho = pd.read_csv(caminho_arquivo_trabalho, delimiter=";", encoding='latin1',low_memory=False)
    df_financiamento = pd.read_csv(caminho_arquivo_financiamento, delimiter=";", encoding='latin1',low_memory=False)
    df_auxilio = pd.read_csv(caminho_arquivo_auxilio, delimiter=";", encoding='latin1',low_memory=False)
    df_bolsa = pd.read_csv(caminho_arquivo_bolsa, delimiter=";", encoding='latin1',low_memory=False)
    df_exterior = pd.read_csv(caminho_arquivo_exterior, delimiter=";", encoding='latin1',low_memory=False)
    df_politica_afirmativa = pd.read_csv(caminho_arquivo_politica_afirmativa, delimiter=";", encoding='latin1',low_memory=False)
    df_horas_estudo = pd.read_csv(caminho_arquivo_horas_estudo, delimiter=";", encoding='latin1',low_memory=False)
    df_censo_cursos = pd.read_csv(caminho_arquivo_censo_cursos, delimiter=";", encoding='latin1',low_memory=False)
    df_censo_ies = pd.read_csv(caminho_arquivo_censo_ies, delimiter=";", encoding='latin1',low_memory=False)

    colunas_selecionadas_cursos = ["NU_ANO_CENSO",
                                        "NO_REGIAO",
                                        "CO_REGIAO",
                                        "NO_UF",
                                        "SG_UF",
                                        "CO_UF",
                                        "NO_MUNICIPIO",
                                        "CO_MUNICIPIO",
                                        "IN_CAPITAL",
                                        "TP_DIMENSAO",
                                        "TP_ORGANIZACAO_ACADEMICA",
                                        "TP_CATEGORIA_ADMINISTRATIVA",
                                        "TP_REDE",
                                        "CO_IES",
                                        "NO_CURSO",
                                        "CO_CURSO",
                                        "NO_CINE_ROTULO",
                                        "CO_CINE_ROTULO",
                                        "CO_CINE_AREA_GERAL",
                                        "NO_CINE_AREA_GERAL",]
    colunas_selecionadas_ies = ["NU_ANO_CENSO",
                                "NO_REGIAO_IES",
                                "CO_REGIAO_IES",
                                "NO_UF_IES",
                                "SG_UF_IES",
                                "CO_UF_IES",
                                "NO_MUNICIPIO_IES",
                                "CO_MUNICIPIO_IES",
                                "IN_CAPITAL_IES",
                                "TP_ORGANIZACAO_ACADEMICA",
                                "TP_CATEGORIA_ADMINISTRATIVA",
                                "NO_MANTENEDORA",
                                "CO_MANTENEDORA",
                                "CO_IES",
                                "NO_IES",
                                "SG_IES"]

    # Criar DataFrames df_cursos e df_ies
    df_cursos = df_censo_cursos[colunas_selecionadas_cursos].drop_duplicates(subset="CO_CURSO")
    df_censo_ies = df_censo_ies[colunas_selecionadas_ies].drop_duplicates(subset="CO_IES")

    # Agrupando os cursos por IES
    cursos_ies = pd.merge(df_cursos, df_censo_ies, on="CO_IES")

    
    # Media das Notas por Curso
    notas_agrupadas = (
        df_notas.groupby("CO_CURSO")[['NT_GER', 'NT_FG', 'NT_CE']]
        .mean()
        .round(2)
        .sort_values(by="NT_GER", ascending=False)
    )
    
    notas_agrupadas = notas_agrupadas.fillna(0)

    # Media, Mediana e Desvio Padrão das Idades dos Inscritos por Curso
    idade_estatisticos_2021 = (
        df_idade.groupby("CO_CURSO")["NU_IDADE"]
        .agg(["mean", "median", "std"])
        .reset_index()
    )
    # Renomeia as colunas para tornar o resultado mais descritivo
    idade_estatisticos_2021.columns = [
        "CO_CURSO",
        "MEDIA_IDADE",
        "MEDIANA_IDADE",
        "DESVIO_PADRAO_IDADE",
    ]

    # Criar uma tabela dinâmica a partir do DataFrame, esta tabela contém contagens de alunos por sexo
    df_sexo["TP_SEXO"] = df_sexo["TP_SEXO"].fillna("N")
    info_sexo_modififcado = pd.pivot_table(
        df_sexo,
        values="NU_ANO",
        index="CO_CURSO",
        columns="TP_SEXO",
        aggfunc="count",
        fill_value=0,
    )


    df_estado_civil["QE_I01"] = df_estado_civil["QE_I01"].fillna("N")
    df_estado_civil = pd.pivot_table(
        df_estado_civil,
        values="NU_ANO",
        index="CO_CURSO",
        columns="QE_I01",
        aggfunc="count",
        fill_value=0,
    )
    df_estado_civil.columns = [
        "SOLTEIRO",
        "CASADO",
        "SEPARADO JUDICIALMENTE/DIVORCIADO",
        "VIÚVO",
        "OUTRO",
        "INFO_ESTADO_CIVIL_NÃO RESPONDEU",
    ]
    info_estado_civil = pd.merge(
        df_estado_civil, cursos_ies, on="CO_CURSO", how="left"
    )


    df_cor["QE_I02"] = df_cor["QE_I02"].fillna("N")
    df_cor = pd.pivot_table(
        df_cor,
        values="NU_ANO",
        index="CO_CURSO",
        columns="QE_I02",
        aggfunc="count",
        fill_value=0,
    )
    df_cor.columns = [
        "BRANCA",
        "PRETA",
        "AMARELA",
        "PARDA",
        "INDÍGINA",
        "NÃO QUERO DECLARAR",
        "INFO_COR_NÃO RESPONDEU",
    ]
    info_cor = pd.merge(
        df_cor, cursos_ies, on="CO_CURSO", how="left"
    )


    df_info_residencia["QE_I06"] = df_info_residencia["QE_I06"].fillna("N")
    df_info_residencia = pd.pivot_table(
        df_info_residencia,
        values="NU_ANO",
        index="CO_CURSO",
        columns="QE_I06",
        aggfunc="count",
        fill_value=0,
    )
    df_info_residencia.columns = [
        "Em casa ou apartamento, sozinho.",
        "Em casa ou apartamento, com pais e/ou parentes.",
        "Em casa ou apartamento, com cônjuge e/ou filhos.",
        "Em casa ou apartamento, com outras pessoas (incluindo república).",
        "Em alojamento universitário da própria instituição.",
        "Em outros tipos de habitação individual ou coletiva (hotel, hospedaria, pensão ou outro).",
        "INFO_RESIDENCIA_NÃO RESPONDEU",
    ]
    info_residencia = pd.merge(
        df_info_residencia, cursos_ies, on="CO_CURSO", how="left"
    )


    df_quantidade_residencia["QE_I07"] = df_quantidade_residencia["QE_I07"].fillna("N")
    df_quantidade_residencia = pd.pivot_table(
        df_quantidade_residencia,
        values="NU_ANO",
        index="CO_CURSO",
        columns="QE_I07",
        aggfunc="count",
        fill_value=0,
    )
    df_quantidade_residencia.columns = [
        "Nenhuma",
        "Uma",
        "Duas",
        "Três",
        "Quatro",
        "Cinco",
        "Seis",
        "Sete ou mais",
        "INFO_QUANTIDADE_RESIDENCIA_NÃO RESPONDEU",
    ]
    info_quantidade_residencia = pd.merge(
        df_quantidade_residencia, cursos_ies, on="CO_CURSO", how="left"
    )


    df_renda["QE_I08"] = df_renda["QE_I08"].fillna("N")
    df_renda = pd.pivot_table(
        df_renda,
        values="NU_ANO",
        index="CO_CURSO",
        columns="QE_I08",
        aggfunc="count",
        fill_value=0,
    )
    df_renda.columns = [
        "Até 1,5 salário mínimo (até R$ 1.650,00)",
        "De 1,5 a 3 salários mínimos (R$ 1.650,01 a R$ 3.300,00)",
        "De 3 a 4,5 salários mínimos (R$ 3.300,01 a R$ 4.950,00)",
        "De 4,5 a 6 salários mínimos (R$ 4.950,01 a R$ 6.600,00)",
        "De 6 a 10 salários mínimos (R$ 6.600,01 a R$ 11.000,00)",
        "De 10 a 30 salários mínimos (R$ 11.000,01 a R$ 33.000,00)",
        "Acima de 30 salários mínimos (mais de R$ 33.000,00)",
        "INFO_RENDA_NÃO RESPONDEU",
    ]
    info_renda = pd.merge(
        df_renda, cursos_ies, on="CO_CURSO", how="left"
    )


    df_financeiro["QE_I09"] = df_financeiro["QE_I09"].fillna("N")
    df_financeiro = pd.pivot_table(
        df_financeiro,
        values="NU_ANO",
        index="CO_CURSO",
        columns="QE_I09",
        aggfunc="count",
        fill_value=0,
    )
    df_financeiro.columns = [
        "Não tenho renda e meus gastos são financiados por programas governamentais",
        "Não tenho renda e meus gastos são financiados pela minha família ou por outras pessoas",
        "Tenho renda, mas recebo ajuda da família ou de outras pessoas para financiar meus gastos",
        "Tenho renda e não preciso de ajuda para financiar meus gastos",
        "Tenho renda e contribuo com o sustento da família",
        "Sou o principal responsável pelo sustento da família",
        "INFO_FINANCEIRO_NÃO RESPONDEU",
    ]
    info_financeiro = pd.merge(
        df_financeiro, cursos_ies, on="CO_CURSO", how="left"
    )


    df_trabalho["QE_I10"] = df_trabalho["QE_I10"].fillna("N")
    df_trabalho = pd.pivot_table(
        df_trabalho,
        values="NU_ANO",
        index="CO_CURSO",
        columns="QE_I10",
        aggfunc="count",
        fill_value=0,
    )
    df_trabalho.columns = [
        "Não estou trabalhando",
        "Trabalho eventualmente",
        "Trabalho até 20 horas semanais",
        "Trabalho de 21 a 39 horas semanais",
        "Trabalho 40 horas semanais ou mais",
        "INFO_TRABALHO_NÃO RESPONDEU",
    ]
    info_trabalho = pd.merge(
        df_trabalho, cursos_ies, on="CO_CURSO", how="left"
    )


    df_financiamento["QE_I11"] = df_financiamento["QE_I11"].fillna("N")
    df_financiamento = pd.pivot_table(
        df_financiamento,
        values="NU_ANO",
        index="CO_CURSO",
        columns="QE_I11",
        aggfunc="count",
        fill_value=0,
    )
    df_financiamento.columns = [
        "Nenhum, pois meu curso é gratuito",
        "Nenhum, embora meu curso não seja gratuito",
        "ProUni Integral",
        "ProUni parcial, apenas",
        "FIES, apenas",
        "ProUni Parcial e FIES",
        "Bolsa oferecida por governo estadual, distrital ou municipal",
        "Bolsa oferecida pela própria instituição",
        "Bolsa oferecida por outra entidade (empresa, ONG, outra)",
        "Financiamento oferecido pela própria instituição",
        "Financiamento bancário",
        "INFO_FINANCIAMENTO_NÃO RESPONDEU",
    ]
    info_financiamento = pd.merge(
        df_financiamento, cursos_ies, on="CO_CURSO", how="left"
    )


    df_auxilio["QE_I12"] = df_auxilio["QE_I12"].fillna("N")
    df_auxilio = pd.pivot_table(
        df_auxilio,
        values="NU_ANO",
        index="CO_CURSO",
        columns="QE_I12",
        aggfunc="count",
        fill_value=0,
    )
    df_auxilio.columns = [
        "Nenhum",
        "Auxílio moradia",
        "Auxílio alimentação",
        "Auxílio moradia e alimentação",
        "Auxílio Permanência",
        "Outro tipo de auxílio",
        "INFO_AUXILIO_NÃO RESPONDEU",
    ]
    info_auxilio = pd.merge(
        df_auxilio, cursos_ies, on="CO_CURSO", how="left"
    )


    df_bolsa["QE_I13"] = df_bolsa["QE_I13"].fillna("N")
    df_bolsa = pd.pivot_table(
        df_bolsa,
        values="NU_ANO",
        index="CO_CURSO",
        columns="QE_I13",
        aggfunc="count",
        fill_value=0,
    )
    df_bolsa.columns = [
        "Nenhum",
        "Bolsa de iniciação científica",
        "Bolsa de extensão",
        "Bolsa de monitoria/tutoria",
        "Bolsa PET",
        "Outro tipo de bolsa acadêmica",
        "INFO_BOLSA_NÃO RESPONDEU",
    ]
    info_bolsa = pd.merge(
        df_bolsa, cursos_ies, on="CO_CURSO", how="left"
    )


    df_exterior["QE_I14"] = df_exterior["QE_I14"].fillna("N")
    df_exterior = pd.pivot_table(
        df_exterior,
        values="NU_ANO",
        index="CO_CURSO",
        columns="QE_I14",
        aggfunc="count",
        fill_value=0,
    )
    df_exterior.columns = [
        "Não participei",
        "Sim, Programa Ciência sem Fronteiras",
        "Sim, programa de intercâmbio financiado pelo Governo Federal (Marca; Brafitec; PLI; outro)",
        "Sim, programa de intercâmbio financiado pelo Governo Estadual",
        "Sim, programa de intercâmbio da minha instituição",
        "Sim, outro intercâmbio não institucional",
        "INFO_EXTERIOR_NÃO RESPONDEU",
    ]
    info_exterior = pd.merge(
        df_exterior, cursos_ies, on="CO_CURSO", how="left"
    )


    df_politica_afirmativa["QE_I15"] = df_politica_afirmativa["QE_I15"].fillna("N")
    df_politica_afirmativa = pd.pivot_table(
        df_politica_afirmativa,
        values="NU_ANO",
        index="CO_CURSO",
        columns="QE_I15",
        aggfunc="count",
        fill_value=0,
    )
    df_politica_afirmativa.columns = [
        "Não",
        "Por critério étnico-racial",
        "Por critério de renda",
        "Por ter estudado em escola pública ou particular com bolsa de estudos",
        "Por sistema que combina dois ou mais critérios anteriores",
        "Por sistema diferente dos anteriores",
        "INFO_POLITICA AFIRMATIVA_NÃO RESPONDEU",
    ]
    info_politica_afirmativa = pd.merge(
        df_politica_afirmativa, cursos_ies, on="CO_CURSO", how="left"
    )

    df_horas_estudo["QE_I23"] = df_horas_estudo["QE_I23"].fillna("N")
    df_horas_estudo = pd.pivot_table(
        df_horas_estudo,
        values="NU_ANO",
        index="CO_CURSO",
        columns="QE_I23",
        aggfunc="count",
        fill_value=0,
    )
    df_horas_estudo.columns = [
        "Nenhuma, apenas assisto às aulas",
        "De uma a três",
        "De quatro a sete",
        "De oito a doze",
        "Mais de doze",
        "INFO_HORAS_ESTUDO_NÃO RESPONDEU",
    ]
    info_horas_estudo = pd.merge(
        df_horas_estudo, cursos_ies, on="CO_CURSO", how="left"
    )


    notas_cursos_ies = pd.merge(notas_agrupadas, cursos_ies, on="CO_CURSO", how="left")
    info_sexo_cursos = pd.merge(
        info_sexo_modififcado, cursos_ies, on="CO_CURSO", how="left"
    )
    idade_estatisticos_cursos = pd.merge(
        idade_estatisticos_2021, cursos_ies, on="CO_CURSO", how="left"
    )
    notas_agrupadas_ies_media = (
        notas_cursos_ies.groupby("CO_IES")[["NT_GER", "NT_FG", "NT_CE"]]
        .mean()
        .round(2)
        .sort_values(by="NT_GER", ascending=False)
    )

    sexo_agrupado_curos = info_sexo_cursos.groupby("CO_CURSO")[["M", "F", "N"]].sum()
    estado_civil_agrupado_cursos = info_estado_civil.groupby("CO_CURSO")[["SOLTEIRO",
        "CASADO",
        "SEPARADO JUDICIALMENTE/DIVORCIADO",
        "VIÚVO",
        "OUTRO",
        "INFO_ESTADO_CIVIL_NÃO RESPONDEU",]].sum()
    cor_agrupado_cursos = info_cor.groupby("CO_CURSO")[["BRANCA",
        "PRETA",
        "AMARELA",
        "PARDA",
        "INDÍGINA",
        "NÃO QUERO DECLARAR",
        "INFO_COR_NÃO RESPONDEU",]].sum()
    info_residencia_agrupado_cursos = info_residencia.groupby("CO_CURSO")[["Em casa ou apartamento, sozinho.",
        "Em casa ou apartamento, com pais e/ou parentes.",
        "Em casa ou apartamento, com cônjuge e/ou filhos.",
        "Em casa ou apartamento, com outras pessoas (incluindo república).",
        "Em alojamento universitário da própria instituição.",
        "Em outros tipos de habitação individual ou coletiva (hotel, hospedaria, pensão ou outro).",
        "INFO_RESIDENCIA_NÃO RESPONDEU",]].sum()
    info_quantidade_residencia_agrupado_cursos = info_quantidade_residencia.groupby("CO_CURSO")[["Nenhuma",
        "Uma",
        "Duas",
        "Três",
        "Quatro",
        "Cinco",
        "Seis",
        "Sete ou mais",
        "INFO_QUANTIDADE_RESIDENCIA_NÃO RESPONDEU",]].sum()
    info_renda_agrupado_cursos = info_renda.groupby("CO_CURSO")[["Até 1,5 salário mínimo (até R$ 1.650,00)",
        "De 1,5 a 3 salários mínimos (R$ 1.650,01 a R$ 3.300,00)",
        "De 3 a 4,5 salários mínimos (R$ 3.300,01 a R$ 4.950,00)",
        "De 4,5 a 6 salários mínimos (R$ 4.950,01 a R$ 6.600,00)",
        "De 6 a 10 salários mínimos (R$ 6.600,01 a R$ 11.000,00)",
        "De 10 a 30 salários mínimos (R$ 11.000,01 a R$ 33.000,00)",
        "Acima de 30 salários mínimos (mais de R$ 33.000,00)",
        "INFO_RENDA_NÃO RESPONDEU",]].sum()
    info_financeiro_agrupado_cursos = info_financeiro.groupby("CO_CURSO")[
        [
            "Não tenho renda e meus gastos são financiados por programas governamentais",
            "Não tenho renda e meus gastos são financiados pela minha família ou por outras pessoas",
            "Tenho renda, mas recebo ajuda da família ou de outras pessoas para financiar meus gastos",
            "Tenho renda e não preciso de ajuda para financiar meus gastos",
            "Tenho renda e contribuo com o sustento da família",
            "Sou o principal responsável pelo sustento da família",
            "INFO_FINANCEIRO_NÃO RESPONDEU",
        ]
    ].sum()
    info_trabalho_agrupado_cursos = info_trabalho.groupby("CO_CURSO")[["Não estou trabalhando",
        "Trabalho eventualmente",
        "Trabalho até 20 horas semanais",
        "Trabalho de 21 a 39 horas semanais",
        "Trabalho 40 horas semanais ou mais",
        "INFO_TRABALHO_NÃO RESPONDEU",]].sum()
    info_financiamento_agrupado_cursos = info_financiamento.groupby("CO_CURSO")[["Nenhum, pois meu curso é gratuito",
        "Nenhum, embora meu curso não seja gratuito",
        "ProUni Integral",
        "ProUni parcial, apenas",
        "FIES, apenas",
        "ProUni Parcial e FIES",
        "Bolsa oferecida por governo estadual, distrital ou municipal",
        "Bolsa oferecida pela própria instituição",
        "Bolsa oferecida por outra entidade (empresa, ONG, outra)",
        "Financiamento oferecido pela própria instituição",
        "Financiamento bancário",
        "INFO_FINANCIAMENTO_NÃO RESPONDEU",]].sum()
    info_auxilio_agrupado_cursos = info_auxilio.groupby("CO_CURSO")[["Nenhum",
        "Auxílio moradia",
        "Auxílio alimentação",
        "Auxílio moradia e alimentação",
        "Auxílio Permanência",
        "Outro tipo de auxílio",
        "INFO_AUXILIO_NÃO RESPONDEU",]].sum()
    info_bolsa_agrupado_cursos = info_bolsa.groupby("CO_CURSO")[["Nenhum",
        "Bolsa de iniciação científica",
        "Bolsa de extensão",
        "Bolsa de monitoria/tutoria",
        "Bolsa PET",
        "Outro tipo de bolsa acadêmica",
        "INFO_BOLSA_NÃO RESPONDEU",]].sum()
    info_exterior_agrupado_cursos = info_exterior.groupby("CO_CURSO")[["Não participei",
        "Sim, Programa Ciência sem Fronteiras",
        "Sim, programa de intercâmbio financiado pelo Governo Federal (Marca; Brafitec; PLI; outro)",
        "Sim, programa de intercâmbio financiado pelo Governo Estadual",
        "Sim, programa de intercâmbio da minha instituição",
        "Sim, outro intercâmbio não institucional",
        "INFO_EXTERIOR_NÃO RESPONDEU",]].sum()
    info_politica_afirmativa_agrupado_cursos = info_politica_afirmativa.groupby("CO_CURSO")[
        [
            "Não",
            "Por critério étnico-racial",
            "Por critério de renda",
            "Por ter estudado em escola pública ou particular com bolsa de estudos",
            "Por sistema que combina dois ou mais critérios anteriores",
            "Por sistema diferente dos anteriores",
            "INFO_POLITICA AFIRMATIVA_NÃO RESPONDEU",
        ]
    ].sum()
    info_horas_estudo_agrupado_cursos = info_horas_estudo.groupby("CO_CURSO")[["Nenhuma, apenas assisto às aulas",
        "De uma a três",
        "De quatro a sete",
        "De oito a doze",
        "Mais de doze",
        "INFO_HORAS_ESTUDO_NÃO RESPONDEU",]].sum()
    idade_agrupada_cursos = (
        idade_estatisticos_cursos.groupby("CO_CURSO")[
            [
                "MEDIA_IDADE",
                "MEDIANA_IDADE",
                "DESVIO_PADRAO_IDADE",
            ]
        ]
        .mean()
        .reset_index()
    )

    sexo_agrupado_ies = info_sexo_cursos.groupby("CO_IES")[["M", "F", "N"]].sum()
    estado_civil_agrupado_ies = info_estado_civil.groupby("CO_IES")[["SOLTEIRO",
        "CASADO",
        "SEPARADO JUDICIALMENTE/DIVORCIADO",
        "VIÚVO",
        "OUTRO",
        "INFO_ESTADO_CIVIL_NÃO RESPONDEU",]].sum()
    cor_agrupado_ies = info_cor.groupby("CO_IES")[["BRANCA",
        "PRETA",
        "AMARELA",
        "PARDA",
        "INDÍGINA",
        "NÃO QUERO DECLARAR",
        "INFO_COR_NÃO RESPONDEU",]].sum()
    df_info_residencia_agrupado_ies = info_residencia.groupby("CO_IES")[["Em casa ou apartamento, sozinho.",
        "Em casa ou apartamento, com pais e/ou parentes.",
        "Em casa ou apartamento, com cônjuge e/ou filhos.",
        "Em casa ou apartamento, com outras pessoas (incluindo república).",
        "Em alojamento universitário da própria instituição.",
        "Em outros tipos de habitação individual ou coletiva (hotel, hospedaria, pensão ou outro).",
        "INFO_RESIDENCIA_NÃO RESPONDEU",]].sum()
    df_info_quantidade_residencia_agrupado_ies = info_quantidade_residencia.groupby("CO_IES")[["Nenhuma",
        "Uma",
        "Duas",
        "Três",
        "Quatro",
        "Cinco",
        "Seis",
        "Sete ou mais",
        "INFO_QUANTIDADE_RESIDENCIA_NÃO RESPONDEU",]].sum()
    renda_agrupada_ies = info_renda.groupby("CO_IES")[["Até 1,5 salário mínimo (até R$ 1.650,00)",
        "De 1,5 a 3 salários mínimos (R$ 1.650,01 a R$ 3.300,00)",
        "De 3 a 4,5 salários mínimos (R$ 3.300,01 a R$ 4.950,00)",
        "De 4,5 a 6 salários mínimos (R$ 4.950,01 a R$ 6.600,00)",
        "De 6 a 10 salários mínimos (R$ 6.600,01 a R$ 11.000,00)",
        "De 10 a 30 salários mínimos (R$ 11.000,01 a R$ 33.000,00)",
        "Acima de 30 salários mínimos (mais de R$ 33.000,00)",
        "INFO_RENDA_NÃO RESPONDEU",]].sum()
    financeiro_agrupado_ies = info_financeiro.groupby("CO_IES")[
        [
            "Não tenho renda e meus gastos são financiados por programas governamentais",
            "Não tenho renda e meus gastos são financiados pela minha família ou por outras pessoas",
            "Tenho renda, mas recebo ajuda da família ou de outras pessoas para financiar meus gastos",
            "Tenho renda e não preciso de ajuda para financiar meus gastos",
            "Tenho renda e contribuo com o sustento da família",
            "Sou o principal responsável pelo sustento da família",
            "INFO_FINANCEIRO_NÃO RESPONDEU",
        ]
    ].sum()
    trabalho_agrupado_ies = info_trabalho.groupby("CO_IES")[["Não estou trabalhando",
        "Trabalho eventualmente",
        "Trabalho até 20 horas semanais",
        "Trabalho de 21 a 39 horas semanais",
        "Trabalho 40 horas semanais ou mais",
        "INFO_TRABALHO_NÃO RESPONDEU",]].sum()
    financiamento_agrupado_ies = info_financiamento.groupby("CO_IES")[["Nenhum, pois meu curso é gratuito",
        "Nenhum, embora meu curso não seja gratuito",
        "ProUni Integral",
        "ProUni parcial, apenas",
        "FIES, apenas",
        "ProUni Parcial e FIES",
        "Bolsa oferecida por governo estadual, distrital ou municipal",
        "Bolsa oferecida pela própria instituição",
        "Bolsa oferecida por outra entidade (empresa, ONG, outra)",
        "Financiamento oferecido pela própria instituição",
        "Financiamento bancário",
        "INFO_FINANCIAMENTO_NÃO RESPONDEU",]].sum()
    auxilio_agrupado_ies = info_auxilio.groupby("CO_IES")[["Nenhum",
        "Auxílio moradia",
        "Auxílio alimentação",
        "Auxílio moradia e alimentação",
        "Auxílio Permanência",
        "Outro tipo de auxílio",
        "INFO_AUXILIO_NÃO RESPONDEU",]].sum()
    bolsa_agrupado_ies = info_bolsa.groupby("CO_IES")[["Nenhum",
        "Bolsa de iniciação científica",
        "Bolsa de extensão",
        "Bolsa de monitoria/tutoria",
        "Bolsa PET",
        "Outro tipo de bolsa acadêmica",
        "INFO_BOLSA_NÃO RESPONDEU",]].sum()
    exterior_agrupado_ies = info_exterior.groupby("CO_IES")[["Não participei",
        "Sim, Programa Ciência sem Fronteiras",
        "Sim, programa de intercâmbio financiado pelo Governo Federal (Marca; Brafitec; PLI; outro)",
        "Sim, programa de intercâmbio financiado pelo Governo Estadual",
        "Sim, programa de intercâmbio da minha instituição",
        "Sim, outro intercâmbio não institucional",
        "INFO_EXTERIOR_NÃO RESPONDEU",]].sum()
    politica_afirmativa_agrupado_ies = info_politica_afirmativa.groupby("CO_IES")[["Não",
        "Por critério étnico-racial",
        "Por critério de renda",
        "Por ter estudado em escola pública ou particular com bolsa de estudos",
        "Por sistema que combina dois ou mais critérios anteriores",
        "Por sistema diferente dos anteriores",
        "INFO_POLITICA AFIRMATIVA_NÃO RESPONDEU",]].sum()
    horas_estudo_agrupado_ies = info_horas_estudo.groupby("CO_IES")[["Nenhuma, apenas assisto às aulas",
        "De uma a três",
        "De quatro a sete",
        "De oito a doze",
        "Mais de doze",
        "INFO_HORAS_ESTUDO_NÃO RESPONDEU",]].sum()
    idade_agrupado_ies = (
        idade_estatisticos_cursos.groupby("CO_IES")[
            [
                "MEDIA_IDADE",
                "MEDIANA_IDADE",
                "DESVIO_PADRAO_IDADE",
            ]
        ]
        .mean()
        .reset_index()
    )


    # Agrupamento das Informações dos Cursos
    info_cursos = pd.merge(notas_cursos_ies, sexo_agrupado_curos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, estado_civil_agrupado_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, cor_agrupado_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, info_residencia_agrupado_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, info_quantidade_residencia_agrupado_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, info_renda_agrupado_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, info_financeiro_agrupado_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, info_trabalho_agrupado_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, info_financiamento_agrupado_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, info_auxilio_agrupado_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, info_bolsa_agrupado_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, info_exterior_agrupado_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, info_politica_afirmativa_agrupado_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, info_horas_estudo_agrupado_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)

    info_cursos = pd.merge(info_cursos, idade_agrupada_cursos, on="CO_CURSO", how="left")
    cols_to_drop = [col for col in info_cursos.columns if "_y" in col]
    info_ies_v2 = info_cursos.drop(cols_to_drop, axis=1)



    # Agrupamento das Informações das IES
    medias_notas_agrupadas_ies = pd.merge(notas_agrupadas_ies_media, df_censo_ies, on="CO_IES", how="left")
    info_sexo_agrupado_ies = pd.merge(sexo_agrupado_ies, df_censo_ies, on="CO_IES", how="left")
    info_idade_agrupado_ies = pd.merge(idade_agrupado_ies, df_censo_ies, on="CO_IES", how="left")
    info_estado_civil_agrupado_ies = pd.merge(estado_civil_agrupado_ies,df_censo_ies, on="CO_IES", how="left")
    info_cor_agrupado_ies = pd.merge(cor_agrupado_ies,df_censo_ies, on="CO_IES", how="left")
    info_residencia_agrupado_ies = pd.merge(df_info_residencia_agrupado_ies,df_censo_ies, on="CO_IES", how="left")
    info_quantidade_residencia_agrupado_ies = pd.merge(df_info_quantidade_residencia_agrupado_ies,df_censo_ies, on="CO_IES", how="left")
    info_renda_agrupado_ies = pd.merge(renda_agrupada_ies,df_censo_ies, on="CO_IES", how="left")
    info_financeiro_agrupado_ies = pd.merge(financeiro_agrupado_ies,df_censo_ies, on="CO_IES", how="left")
    info_trabalho_agrupado_ies = pd.merge(trabalho_agrupado_ies,df_censo_ies, on="CO_IES", how="left")
    info_financiamento_agrupado_ies = pd.merge(financiamento_agrupado_ies,df_censo_ies, on="CO_IES", how="left")
    info_auxilio_agrupado_ies = pd.merge(auxilio_agrupado_ies,df_censo_ies, on="CO_IES", how="left")
    info_bolsa_agrupado_ies = pd.merge(bolsa_agrupado_ies,df_censo_ies, on="CO_IES", how="left")
    info_exterior_agrupado_ies = pd.merge(exterior_agrupado_ies,df_censo_ies, on="CO_IES", how="left")
    info_politica_afirmativa_agrupado_ies = pd.merge(politica_afirmativa_agrupado_ies,df_censo_ies, on="CO_IES", how="left")
    info_horas_estudo_agrupado_ies = pd.merge(horas_estudo_agrupado_ies,df_censo_ies, on="CO_IES", how="left")

    info_ies = pd.merge(medias_notas_agrupadas_ies, info_sexo_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_idade_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_estado_civil_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_cor_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_residencia_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_quantidade_residencia_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_renda_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_financeiro_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_trabalho_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_financiamento_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_auxilio_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_bolsa_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_exterior_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_politica_afirmativa_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)

    info_ies = pd.merge(info_ies, info_horas_estudo_agrupado_ies, on="CO_IES", how="left")
    cols_to_drop = [col for col in info_ies.columns if "_y" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)
    cols_to_drop = [col for col in info_ies.columns if "_x" in col]
    info_ies = info_ies.drop(cols_to_drop, axis=1)


    info_ies = pd.merge(info_ies, df_censo_ies, on="CO_IES", how="left")
    

    output_dir = "/opt/airflow/dataset"

    # Garanta que o diretório exista
    os.makedirs(output_dir, exist_ok=True)

    # Caminho completo para o arquivo CSV
    file_path_cursos = os.path.join(output_dir, f"info_cursos_enade_censo_{year}.csv")
    file_path_ies = os.path.join(output_dir, f"info_ies_enade_censo_{year}.csv")

    # Salvar o DataFrame como um arquivo CSV no caminho especificado
    info_cursos.to_csv(file_path_cursos, index=False)
    info_ies.to_csv(file_path_ies, index=False)

def process_and_concat_csv(directory):
    # Lista todos os arquivos CSV no diretório
    csv_files = [file for file in os.listdir(directory) if file.endswith('.csv')]
    
    # Inicializa uma lista vazia para armazenar os DataFrames de cada arquivo
    ies_dfs = []
    cursos_dfs = []
    
    # Itera sobre cada arquivo CSV encontrado no diretório
    for file in csv_files:
        # Lê o arquivo CSV e adiciona o DataFrame à lista correspondente
        df = pd.read_csv(os.path.join(directory, file), low_memory=False, encoding='latin1')
        if 'info_ies_enade_censo' in file:
            ies_dfs.append(df)
        elif 'info_cursos_enade_censo' in file:
            cursos_dfs.append(df)
    
    # Concatena todos os DataFrames de IES da lista em um único DataFrame
    ies_concatenated_df = pd.concat(ies_dfs, ignore_index=True, axis=0)
    
    # Mover a coluna 'NU_ANO_CENSO' para a primeira posição
    ies_concatenated_df = ies_concatenated_df[["NU_ANO_CENSO"] + [col for col in ies_concatenated_df.columns if col != "NU_ANO_CENSO"]]
    
    # Concatena todos os DataFrames de cursos da lista em um único DataFrame
    cursos_concatenated_df = pd.concat(cursos_dfs, ignore_index=True, axis=0)
    
    # Mover a coluna 'NU_ANO_CENSO' para a primeira posição
    cursos_concatenated_df = cursos_concatenated_df[["NU_ANO_CENSO_x"] + [col for col in cursos_concatenated_df.columns if col != "NU_ANO_CENSO_x"]]
    
    output_dir = "/opt/airflow/dataset"

    # Garanta que o diretório exista
    os.makedirs(output_dir, exist_ok=True)

    # Caminho completo para o arquivo CSV
    file_path_cursos = os.path.join(output_dir, "cursos_concatenated.csv")
    file_path_ies = os.path.join(output_dir, "ies_concatenated.csv")
    
    # Salva os DataFrames concatenados em arquivos CSV separados
    ies_concatenated_df.to_csv(file_path_ies, index=False)
    cursos_concatenated_df.to_csv(file_path_cursos, index=False)

def upload_arquivos_para_s3(bucket_name, dir_to_update, endpoint_url, localstack_auth_token=None):
    if not os.path.exists(dir_to_update):
        logging.error(f"Diretório '{dir_to_update}' não encontrado.")
        return
    
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id='mock_access_key_id',  # Não é necessário para o LocalStack
        aws_secret_access_key='mock_secret_access_key',  # Não é necessário para o LocalStack
        aws_session_token=localstack_auth_token  # Passa o token de autenticação
    )
    
    try:
        for root, _, files in os.walk(dir_to_update):
            for file in files:
                local_path = os.path.join(root, file)
                s3_key = os.path.relpath(local_path, dir_to_update)
                
                try:
                    s3_client.upload_file(local_path, bucket_name, s3_key)
                    logging.info(f"Arquivo '{local_path}' enviado para o bucket '{bucket_name}' como '{s3_key}'.")
                    os.remove(local_path)
                    logging.info(f"Arquivo '{local_path}' excluído localmente após o envio.")
                except Exception as e:
                    logging.error(f"Erro ao enviar o arquivo '{local_path}' para o bucket '{bucket_name}': {e}")
        
        _deletar_diretorios_vazios(dir_to_update)
        
    except Exception as e:
        logging.error(f"Erro ao percorrer o diretório '{dir_to_update}': {e}")

def _deletar_diretorios_vazios(base_dir):
    for root, dirs, _ in os.walk(base_dir, topdown=False):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)
                logging.info(f"Diretório vazio '{dir_path}' excluído localmente.")

def sync_to_s3(directory):
    localstack_auth_token = os.environ.get("LOCALSTACK_AUTH_TOKEN")  # Obtém o token de autenticação do ambiente
    
    upload_arquivos_para_s3(
        bucket_name='datalake-enade-censo',
        dir_to_update=directory,
        endpoint_url='https://localhost.localstack.cloud:4566',  # Corrigido o endpoint
        localstack_auth_token="ls-gaWeGuxo-6575-0400-jazo-GuqeXOgI4203"  # Passa o token de autenticação
    )

   



# Definição da DAG
with DAG('censoEnade', default_args=default_args) as dag:

    # Criação dos diretórios
    cria_diretorios = BashOperator(
        task_id='cria_diretorios',
        bash_command=f"mkdir -p {CAMADA_BRONZE} {CAMADA_PRATA} {CAMADA_OURO}",
    )

    # Definição das tarefas

    #2021
    #ENADE
    download_enade_2021 = PythonOperator(
        task_id='download_enade_2021',
        python_callable=download_zip_file,
        op_kwargs={'url': 'https://download.inep.gov.br/microdados/microdados_enade_2021.zip', 'output_dir': '/opt/airflow/dataset', 'zip_file_name': 'microdados_enade_2021.zip'},
        
    )

    extract_enade_2021 = PythonOperator(
        task_id='extract_enade_2021',
        python_callable=extract_zip_file,
        op_kwargs={'zip_file_path': '/opt/airflow/dataset/microdados_enade_2021.zip', 'extract_dir': '/opt/airflow/dataset/diretorio_de_extracao'},
    )

    select_enade_2021 = PythonOperator(
        task_id='select_enade_2021',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/dataset/diretorio_de_extracao/microdados_Enade_2021_LGPD/2.DADOS','subfolder': '', 'files_to_select': ['microdados2021_arq1.txt', 
                                                                                                                                                    'microdados2021_arq2.txt', 
                                                                                                                                                    'microdados2021_arq3.txt', 
                                                                                                                                                    'microdados2021_arq5.txt',
                                                                                                                                                    'microdados2021_arq6.txt',
                                                                                                                                                    'microdados2021_arq7.txt',
                                                                                                                                                    'microdados2021_arq8.txt',
                                                                                                                                                    'microdados2021_arq12.txt',
                                                                                                                                                    'microdados2021_arq13.txt',
                                                                                                                                                    'microdados2021_arq14.txt',
                                                                                                                                                    'microdados2021_arq15.txt',
                                                                                                                                                    'microdados2021_arq16.txt',
                                                                                                                                                    'microdados2021_arq17.txt',
                                                                                                                                                    'microdados2021_arq18.txt',
                                                                                                                                                    'microdados2021_arq19.txt',
                                                                                                                                                    'microdados2021_arq20.txt',
                                                                                                                                                    'microdados2021_arq21.txt',
                                                                                                                                                    'microdados2021_arq29.txt',], 'output_dir': '/opt/airflow/datalake/bronze/date={{ ds }}',},
    )  

    rename_files_in_select_dir_task_2021 = PythonOperator(
        task_id='rename_files_in_select_dir_2021',
        python_callable=rename_files_in_directory,
        op_kwargs={'input_dir': '/opt/airflow/datalake/bronze/date={{ ds }}', 'output_dir': '/opt/airflow/datalake/silver/date={{ ds }}', 'file_prefix': 'microdados2021_arq'},
    )

    #CENSO
    download_censo_2021 = PythonOperator(
        task_id='download_censo_2021',
        python_callable=download_zip_file,
        op_kwargs={'url': 'https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2021.zip', 'output_dir': '/opt/airflow/dataset', 'zip_file_name': 'microdados_censo_da_educacao_superior_2021.zip'},
    )    

    extract_censo_2021 = PythonOperator(
        task_id='extract_censo_2021',
        python_callable=extract_zip_file,
        op_kwargs={'zip_file_path': '/opt/airflow/dataset/microdados_censo_da_educacao_superior_2021.zip', 'extract_dir': '/opt/airflow/datalake/bronze/date={{ ds }}'},
    )

    select_censo_2021 = PythonOperator(
        task_id='select_censo_2021',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/datalake/bronze/date={{ ds }}/Microdados do Censo da Educaç╞o Superior 2021', 'output_dir': '/opt/airflow/datalake/silver/date={{ ds }}', 'subfolder': 'dados', 'files_to_select': ['MICRODADOS_CADASTRO_IES_2021.CSV', 'MICRODADOS_CADASTRO_CURSOS_2021.CSV']},
    ) 
 
    #UNIFICAR
    unificar_arquivos_2021 = PythonOperator(
        task_id='unificar_arquivos_2021',
        python_callable = unificacao_limpeza_dir,
        op_kwargs={'year': 2021}
    )
    
    #CAMADA GOLD
    select_info_cursos_ies_2021 = PythonOperator(
        task_id='select_info_cursos_2021',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/dataset', 'output_dir': '/opt/airflow/datalake/gold/date={{ ds }}','subfolder': '', 'files_to_select': ['info_cursos_enade_censo_2021.csv', 'info_ies_enade_censo_2021.csv']},
    )
    
    
    #2019
    #ENADE
    download_enade_2019 = PythonOperator(
        task_id='download_enade_2019',
        python_callable=download_zip_file,
        op_kwargs={'url': 'https://download.inep.gov.br/microdados/microdados_enade_2019_LGPD.zip', 'output_dir': '/opt/airflow/dataset', 'zip_file_name': 'microdados_enade_2019.zip'},
        
    )

    extract_enade_2019 = PythonOperator(
        task_id='extract_enade_2019',
        python_callable=extract_zip_file,
        op_kwargs={'zip_file_path': '/opt/airflow/dataset/microdados_enade_2019.zip', 'extract_dir': '/opt/airflow/dataset/diretorio_de_extracao'},
    )

    select_enade_2019 = PythonOperator(
        task_id='select_enade_2019',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/dataset/diretorio_de_extracao/microdados_Enade_2019_LGPD/2. DADOS','subfolder': '', 'files_to_select': ['microdados2019_arq1.txt', 
                                                                                                                                                    'microdados2019_arq2.txt', 
                                                                                                                                                    'microdados2019_arq3.txt', 
                                                                                                                                                    'microdados2019_arq5.txt',
                                                                                                                                                    'microdados2019_arq6.txt',
                                                                                                                                                    'microdados2019_arq7.txt',
                                                                                                                                                    'microdados2019_arq8.txt',
                                                                                                                                                    'microdados2019_arq12.txt',
                                                                                                                                                    'microdados2019_arq13.txt',
                                                                                                                                                    'microdados2019_arq14.txt',
                                                                                                                                                    'microdados2019_arq15.txt',
                                                                                                                                                    'microdados2019_arq16.txt',
                                                                                                                                                    'microdados2019_arq17.txt',
                                                                                                                                                    'microdados2019_arq18.txt',
                                                                                                                                                    'microdados2019_arq19.txt',
                                                                                                                                                    'microdados2019_arq20.txt',
                                                                                                                                                    'microdados2019_arq21.txt',
                                                                                                                                                    'microdados2019_arq29.txt',], 'output_dir': '/opt/airflow/datalake/bronze/date={{ ds }}',},
    )  

    rename_files_in_select_dir_task_2019 = PythonOperator(
        task_id='rename_files_in_select_dir_2019',
        python_callable=rename_files_in_directory,
        op_kwargs={'input_dir': '/opt/airflow/datalake/bronze/date={{ ds }}', 'output_dir': '/opt/airflow/datalake/silver/date={{ ds }}', 'file_prefix': 'microdados2019_arq'},
    )

    #CENSO
    download_censo_2019 = PythonOperator(
        task_id='download_censo_2019',
        python_callable=download_zip_file,
        op_kwargs={'url': 'https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2019.zip', 'output_dir': '/opt/airflow/dataset', 'zip_file_name': 'microdados_censo_da_educacao_superior_2019.zip'},
    )    

    extract_censo_2019 = PythonOperator(
        task_id='extract_censo_2019',
        python_callable=extract_zip_file,
        op_kwargs={'zip_file_path': '/opt/airflow/dataset/microdados_censo_da_educacao_superior_2019.zip', 'extract_dir': '/opt/airflow/datalake/bronze/date={{ ds }}'},
    )

    select_censo_2019 = PythonOperator(
        task_id='select_censo_2019',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/datalake/bronze/date={{ ds }}/Microdados do Censo da Educaç╞o Superior 2019', 'output_dir': '/opt/airflow/datalake/silver/date={{ ds }}', 'subfolder': 'dados', 'files_to_select': ['MICRODADOS_CADASTRO_IES_2019.CSV', 'MICRODADOS_CADASTRO_CURSOS_2019.CSV']},
    ) 

  
    #UNIFICAR
    unificar_arquivos_2019 = PythonOperator(
        task_id='unificar_arquivos_2019',
        python_callable = unificacao_limpeza_dir,
        op_kwargs={'year': 2019}
    )
    
    #CAMADA GOLD
    select_info_cursos_ies_2019 = PythonOperator(
        task_id='select_info_cursos_2019',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/dataset', 'output_dir': '/opt/airflow/datalake/gold/date={{ ds }}','subfolder': '', 'files_to_select': ['info_cursos_enade_censo_2019.csv', 'info_ies_enade_censo_2019.csv']},
    )
    
    
    #2018
    #ENADE
    download_enade_2018 = PythonOperator(
        task_id='download_enade_2018',
        python_callable=download_zip_file,
        op_kwargs={'url': 'https://download.inep.gov.br/microdados/microdados_enade_2018_LGPD.zip', 'output_dir': '/opt/airflow/dataset', 'zip_file_name': 'microdados_enade_2018.zip'},
        
    )

    extract_enade_2018 = PythonOperator(
        task_id='extract_enade_2018',
        python_callable=extract_zip_file,
        op_kwargs={'zip_file_path': '/opt/airflow/dataset/microdados_enade_2018.zip', 'extract_dir': '/opt/airflow/dataset/diretorio_de_extracao'},
    )

    select_enade_2018 = PythonOperator(
        task_id='select_enade_2018',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/dataset/diretorio_de_extracao/microdados_Enade_2018_LGPD/2.DADOS','subfolder': '', 'files_to_select': ['microdados2018_arq1.txt', 
                                                                                                                                                    'microdados2018_arq2.txt', 
                                                                                                                                                    'microdados2018_arq3.txt', 
                                                                                                                                                    'microdados2018_arq5.txt',
                                                                                                                                                    'microdados2018_arq6.txt',
                                                                                                                                                    'microdados2018_arq7.txt',
                                                                                                                                                    'microdados2018_arq8.txt',
                                                                                                                                                    'microdados2018_arq12.txt',
                                                                                                                                                    'microdados2018_arq13.txt',
                                                                                                                                                    'microdados2018_arq14.txt',
                                                                                                                                                    'microdados2018_arq15.txt',
                                                                                                                                                    'microdados2018_arq16.txt',
                                                                                                                                                    'microdados2018_arq17.txt',
                                                                                                                                                    'microdados2018_arq18.txt',
                                                                                                                                                    'microdados2018_arq19.txt',
                                                                                                                                                    'microdados2018_arq20.txt',
                                                                                                                                                    'microdados2018_arq21.txt',
                                                                                                                                                    'microdados2018_arq29.txt',], 'output_dir': '/opt/airflow/datalake/bronze/date={{ ds }}',},
    )  

    rename_files_in_select_dir_task_2018 = PythonOperator(
        task_id='rename_files_in_select_dir_2018',
        python_callable=rename_files_in_directory,
        op_kwargs={'input_dir': '/opt/airflow/datalake/bronze/date={{ ds }}', 'output_dir': '/opt/airflow/datalake/silver/date={{ ds }}', 'file_prefix': 'microdados2018_arq'},
    )

    #CENSO
    download_censo_2018 = PythonOperator(
        task_id='download_censo_2018',
        python_callable=download_zip_file,
        op_kwargs={'url': 'https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2018.zip', 'output_dir': '/opt/airflow/dataset', 'zip_file_name': 'microdados_censo_da_educacao_superior_2018.zip'},
    )    

    extract_censo_2018 = PythonOperator(
        task_id='extract_censo_2018',
        python_callable=extract_zip_file,
        op_kwargs={'zip_file_path': '/opt/airflow/dataset/microdados_censo_da_educacao_superior_2018.zip', 'extract_dir': '/opt/airflow/datalake/bronze/date={{ ds }}'},
    )

    select_censo_2018 = PythonOperator(
        task_id='select_censo_2018',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/datalake/bronze/date={{ ds }}/Microdados do Censo da Educaç╞o Superior 2018', 'output_dir': '/opt/airflow/datalake/silver/date={{ ds }}', 'subfolder': 'dados', 'files_to_select': ['MICRODADOS_CADASTRO_IES_2018.CSV', 'MICRODADOS_CADASTRO_CURSOS_2018.CSV']},
    ) 

 
    #UNIFICAR
    unificar_arquivos_2018 = PythonOperator(
        task_id='unificar_arquivos_2018',
        python_callable = unificacao_limpeza_dir,
        op_kwargs={'year': 2018}
    )
    
    #CAMADA GOLD
    select_info_cursos_ies_2018 = PythonOperator(
        task_id='select_info_cursos_2018',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/dataset', 'output_dir': '/opt/airflow/datalake/gold/date={{ ds }}','subfolder': '', 'files_to_select': ['info_cursos_enade_censo_2018.csv', 'info_ies_enade_censo_2018.csv']},
    )    
    
    #2017
    #ENADE
    download_enade_2017 = PythonOperator(
        task_id='download_enade_2017',
        python_callable=download_zip_file,
        op_kwargs={'url': 'https://download.inep.gov.br/microdados/microdados_enade_2017_LGPD.zip', 'output_dir': '/opt/airflow/dataset', 'zip_file_name': 'microdados_enade_2017.zip'},
        
    )

    extract_enade_2017 = PythonOperator(
        task_id='extract_enade_2017',
        python_callable=extract_zip_file,
        op_kwargs={'zip_file_path': '/opt/airflow/dataset/microdados_enade_2017.zip', 'extract_dir': '/opt/airflow/dataset/diretorio_de_extracao'},
    )

    select_enade_2017 = PythonOperator(
        task_id='select_enade_2017',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/dataset/diretorio_de_extracao/microdados_Enade_2017_LGPD/2.DADOS','subfolder': '', 'files_to_select': ['microdados2017_arq1.txt', 
                                                                                                                                                    'microdados2017_arq2.txt', 
                                                                                                                                                    'microdados2017_arq3.txt', 
                                                                                                                                                    'microdados2017_arq5.txt',
                                                                                                                                                    'microdados2017_arq6.txt',
                                                                                                                                                    'microdados2017_arq7.txt',
                                                                                                                                                    'microdados2017_arq8.txt',
                                                                                                                                                    'microdados2017_arq12.txt',
                                                                                                                                                    'microdados2017_arq13.txt',
                                                                                                                                                    'microdados2017_arq14.txt',
                                                                                                                                                    'microdados2017_arq15.txt',
                                                                                                                                                    'microdados2017_arq16.txt',
                                                                                                                                                    'microdados2017_arq17.txt',
                                                                                                                                                    'microdados2017_arq18.txt',
                                                                                                                                                    'microdados2017_arq19.txt',
                                                                                                                                                    'microdados2017_arq20.txt',
                                                                                                                                                    'microdados2017_arq21.txt',
                                                                                                                                                    'microdados2017_arq29.txt',], 'output_dir': '/opt/airflow/datalake/bronze/date={{ ds }}',},
    )  

    rename_files_in_select_dir_task_2017 = PythonOperator(
        task_id='rename_files_in_select_dir_2017',
        python_callable=rename_files_in_directory,
        op_kwargs={'input_dir': '/opt/airflow/datalake/bronze/date={{ ds }}', 'output_dir': '/opt/airflow/datalake/silver/date={{ ds }}', 'file_prefix': 'microdados2017_arq'},
    )

    #CENSO
    download_censo_2017 = PythonOperator(
        task_id='download_censo_2017',
        python_callable=download_zip_file,
        op_kwargs={'url': 'https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2017.zip', 'output_dir': '/opt/airflow/dataset', 'zip_file_name': 'microdados_censo_da_educacao_superior_2017.zip'},
    )    

    extract_censo_2017 = PythonOperator(
        task_id='extract_censo_2017',
        python_callable=extract_zip_file,
        op_kwargs={'zip_file_path': '/opt/airflow/dataset/microdados_censo_da_educacao_superior_2017.zip', 'extract_dir': '/opt/airflow/datalake/bronze/date={{ ds }}'},
    )

    select_censo_2017 = PythonOperator(
        task_id='select_censo_2017',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/datalake/bronze/date={{ ds }}/Microdados do Censo da Educaç╞o Superior 2017', 'output_dir': '/opt/airflow/datalake/silver/date={{ ds }}', 'subfolder': 'dados', 'files_to_select': ['MICRODADOS_CADASTRO_IES_2017.CSV', 'MICRODADOS_CADASTRO_CURSOS_2017.CSV']},
    ) 

 
    #UNIFICAR
    unificar_arquivos_2017 = PythonOperator(
        task_id='unificar_arquivos_2017',
        python_callable = unificacao_limpeza_dir,
        op_kwargs={'year': 2017}
    )
    
    #CAMADA GOLD
    select_info_cursos_ies_2017 = PythonOperator(
        task_id='select_info_cursos_2017',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/dataset', 'output_dir': '/opt/airflow/datalake/gold/date={{ ds }}','subfolder': '', 'files_to_select': ['info_cursos_enade_censo_2017.csv', 'info_ies_enade_censo_2017.csv']},
    )
    
    
    #CONCATENAR TODOS OS ANOS     
    process_and_concat_csv_task = PythonOperator(
        task_id = 'process_and_concat_csv',
        python_callable=process_and_concat_csv,
        op_kwargs = {'directory': '/opt/airflow/datalake/gold/date={{ ds }}'}
    )
    
    select_info_cursos_ies_concatenado = PythonOperator(
        task_id='select_info_cursos_ies_concatenado',
        python_callable=select_files,
        op_kwargs={'input_dir': '/opt/airflow/dataset', 'output_dir': '/opt/airflow/datalake/gold/date={{ ds }}','subfolder': '', 'files_to_select': ['cursos_concatenated.csv', 'ies_concatenated.csv']},
    )
    
    #EXPORTAR DATALAKE
    sync_bronze_to_s3 = PythonOperator(
        task_id='sync_bronze_to_s3',
        python_callable=sync_to_s3,
        op_kwargs={'directory': '/opt/airflow/datalake/bronze/date={{ ds }}'}
    )

    sync_silver_to_s3 = PythonOperator(
        task_id='sync_silver_to_s3',
        python_callable=sync_to_s3,
        op_kwargs={'directory': '/opt/airflow/datalake/silver/date={{ ds }}'}
    )

    sync_gold_to_s3 = PythonOperator(
        task_id='sync_gold_to_s3',
        python_callable=sync_to_s3,
        op_kwargs={'directory': '/opt/airflow/datalake/gold/date={{ ds }}'}
    ) 
    
    # Define as dependências entre as tarefas
    #2021
    cria_diretorios >> [download_enade_2021, download_censo_2021]
    download_enade_2021 >> extract_enade_2021
    download_censo_2021 >> extract_censo_2021
    extract_enade_2021 >> select_enade_2021
    extract_censo_2021 >> select_censo_2021
    select_enade_2021 >> rename_files_in_select_dir_task_2021
     
    #2019
    cria_diretorios >> [download_enade_2019, download_censo_2019]
    download_enade_2019 >> extract_enade_2019
    download_censo_2019 >> extract_censo_2019
    extract_enade_2019 >> select_enade_2019
    extract_censo_2019 >> select_censo_2019
    select_enade_2019 >> rename_files_in_select_dir_task_2019
    
    #2018
    cria_diretorios >> [download_enade_2018, download_censo_2018]
    download_enade_2018 >> extract_enade_2018
    download_censo_2018 >> extract_censo_2018
    extract_enade_2018 >> select_enade_2018
    extract_censo_2018 >> select_censo_2018
    select_enade_2018 >> rename_files_in_select_dir_task_2018
        
    #2017
    cria_diretorios >> [download_enade_2017, download_censo_2017]
    download_enade_2017 >> extract_enade_2017
    download_censo_2017 >> extract_censo_2017
    extract_enade_2017 >> select_enade_2017
    extract_censo_2017 >> select_censo_2017
    select_enade_2017 >> rename_files_in_select_dir_task_2017
  
    
    [select_censo_2017,rename_files_in_select_dir_task_2017,select_censo_2018,rename_files_in_select_dir_task_2018, select_censo_2019,rename_files_in_select_dir_task_2019, select_censo_2021,rename_files_in_select_dir_task_2021] >> unificar_arquivos_2017 >> unificar_arquivos_2018 >> unificar_arquivos_2019 >> unificar_arquivos_2021
    unificar_arquivos_2017 >> select_info_cursos_ies_2017
    unificar_arquivos_2018 >> select_info_cursos_ies_2018
    unificar_arquivos_2019 >> select_info_cursos_ies_2019
    unificar_arquivos_2021 >> select_info_cursos_ies_2021
    [select_info_cursos_ies_2017, select_info_cursos_ies_2018, select_info_cursos_ies_2019, select_info_cursos_ies_2021] >> process_and_concat_csv_task 
    process_and_concat_csv_task >> select_info_cursos_ies_concatenado
    select_info_cursos_ies_concatenado >> [sync_bronze_to_s3,sync_silver_to_s3, sync_gold_to_s3]