from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Importamos as funções de Extração, Transformação e Carga (E, T, L)
# No Airflow, a pasta 'dags' precisa ter acesso ao código em 'src'.
# O Airflow faria isso automaticamente se o código estivesse configurado no ambiente Python.

# Importamos as funções diretamente
# ASSUMIMOS QUE VOCÊ CONFIGUROU O PYTHON PATH DO AIRFLOW PARA INCLUIR 'src'
# OU COPIOU ESTES ARQUIVOS PARA UM DIRETÓRIO ACESSÍVEL PELO CONTAINER DO AIRFLOW.
# Para manter a clareza, vamos chamar as funções diretamente:

import sys
import os

# Adiciona o diretório raiz do projeto ao path para importar 'src'
# (Isto é necessário se você estiver rodando o DAG localmente fora do ambiente Airflow. 
# No ambiente Airflow, isso é geralmente feito através da configuração do Docker ou PYTHONPATH)

# Nota: Em um ambiente Docker Airflow, é melhor garantir que 'src' esteja acessível 
# diretamente, mapeando o volume do projeto.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.extract.api_extractor import fetch_weather_data
from src.transform.data_transformer import transform_weather_data
from src.load.data_loader import load_data_to_db

# --- Configuração Padrão do DAG ---
default_args = {
    'owner': 'seu_nome',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email': ['seu.email@exemplo.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- Definição da DAG ---
with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL de dados de clima da OpenWeatherMap',
    # Agenda para rodar diariamente à 1:00 AM
    schedule_interval='0 1 * * *', 
    catchup=False,
    tags=['etl', 'weather', 'data-engineering'],
) as dag:

    # --------------------------------------------------------
    # Task 1: Extração (E)
    # --------------------------------------------------------
    # Esta função extrai os dados da API e salva o JSON bruto.
    # Usamos o 'xcom_push' para que o Airflow armazene o caminho do arquivo JSON
    # e o passe para a próxima tarefa.
    extract_task = PythonOperator(
        task_id='extract_raw_data',
        python_callable=fetch_weather_data,
        do_xcom_push=True, # Garante que o retorno da função seja armazenado
    )

    # --------------------------------------------------------
    # Task 2: Transformação (T)
    # --------------------------------------------------------
    # Esta função recebe o caminho do arquivo bruto da Task 1 via XCom
    def transform_wrapper(**kwargs):
        ti = kwargs['ti']
        # Puxa o caminho do arquivo JSON que foi retornado pela 'extract_task'
        raw_file_path = ti.xcom_pull(task_ids='extract_raw_data')
        
        if not raw_file_path:
            raise ValueError("Caminho do arquivo bruto não encontrado (XCom falhou).")
            
        # Retorna os caminhos dos CSVs limpos
        return transform_weather_data(raw_file_path) 
        
    transform_task = PythonOperator(
        task_id='transform_data_to_csv',
        python_callable=transform_wrapper,
        do_xcom_push=True, # Retorna os caminhos dos dois CSVs (current e forecast)
    )

    # --------------------------------------------------------
    # Task 3: Carga (L)
    # --------------------------------------------------------
    # Esta função recebe os caminhos dos CSVs limpos da Task 2 via XCom
    def load_wrapper(**kwargs):
        ti = kwargs['ti']
        # Puxa a tupla (current_csv, forecast_csv) que foi retornada pela 'transform_task'
        csv_paths = ti.xcom_pull(task_ids='transform_data_to_csv')
        
        if not csv_paths or len(csv_paths) != 2:
            raise ValueError("Caminhos dos arquivos CSV limpos não encontrados (XCom falhou).")
            
        current_csv, forecast_csv = csv_paths
        
        # Chama a função de carga
        load_data_to_db(current_csv, forecast_csv) 
        
    load_task = PythonOperator(
        task_id='load_cleaned_data_to_db',
        python_callable=load_wrapper,
    )

    # --- Definição do Fluxo (Ordem de Execução) ---
    extract_task >> transform_task >> load_task
    
    # No final de src/transform/data_transformer.py

def transform_weather_data(file_path):
    # ... (todo o código de transformação) ...
    
    # ... (criação dos caminhos current_output_path e forecast_output_path) ...
    
    df_current.to_csv(current_output_path, index=False)
    df_forecast.to_csv(forecast_output_path, index=False)
    
    print(f"\nDados limpos salvos:\n- Clima Atual: {current_output_path}\n- Previsão: {forecast_output_path}")
    
    # MUDANÇA ESSENCIAL: Retorna os dois caminhos
    return current_output_path, forecast_output_path