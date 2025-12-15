import pandas as pd
from sqlalchemy import create_engine
import os

# Define o nome do arquivo do banco de dados que será criado na pasta 'data'
DATABASE_FILE = 'data/weather_data_warehouse.db'
SQLITE_URL = f"sqlite:///{DATABASE_FILE}"

def load_data_to_db(current_csv_path, forecast_csv_path):
    """
    Carrega dados de arquivos CSV em um banco de dados SQLite.
    Cria a engine do banco de dados e carrega dois DataFrames em duas tabelas diferentes.
    """
    print(f"Iniciando a carga de dados para: {DATABASE_FILE}")
    
    # 1. Cria a engine de conexão com o banco de dados
    # O comando create_engine cria o arquivo .db se ele não existir
    try:
        engine = create_engine(SQLITE_URL)
        print("Conexão com o banco de dados estabelecida.")
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return

    # 2. Carrega os DataFrames
    try:
        df_current = pd.read_csv(current_csv_path)
        df_forecast = pd.read_csv(forecast_csv_path)
        print("Arquivos CSV lidos com sucesso.")
    except Exception as e:
        print(f"Erro ao ler arquivos CSV: {e}")
        return

    # 3. Carga dos dados de Clima Atual
    # if_exists='append' garante que novos dados sejam adicionados a cada execução
    try:
        df_current.to_sql(
            name='current_weather', # Nome da tabela no DB
            con=engine, 
            if_exists='append', 
            index=False
        )
        print(f"✅ Sucesso: {len(df_current)} registros de Clima Atual carregados na tabela 'current_weather'.")
    except Exception as e:
        print(f"Erro ao carregar dados de Clima Atual: {e}")

    # 4. Carga dos dados de Previsão
    try:
        df_forecast.to_sql(
            name='forecast_weather', # Nome da tabela no DB
            con=engine, 
            if_exists='append', 
            index=False
        )
        print(f"✅ Sucesso: {len(df_forecast)} registros de Previsão carregados na tabela 'forecast_weather'.")
    except Exception as e:
        print(f"Erro ao carregar dados de Previsão: {e}")

    print("\nProcesso de Carga (L) concluído com sucesso!")


if __name__ == "__main__":
    # --- LÓGICA DE EXECUÇÃO ---
    # Precisamos encontrar os arquivos CSV mais recentes na pasta 'data/cleaned'
    
    cleaned_dir = os.path.join("data", "cleaned")
    
    # Lista todos os arquivos CSV na pasta 'cleaned'
    csv_files = [os.path.join(cleaned_dir, f) for f in os.listdir(cleaned_dir) if f.endswith('.csv')]
    
    if len(csv_files) >= 2:
        # Encontra o arquivo de Clima Atual e Previsão mais recentes com base no prefixo do nome
        current_file = max([f for f in csv_files if 'current_weather' in f], key=os.path.getctime)
        forecast_file = max([f for f in csv_files if 'forecast_weather' in f], key=os.path.getctime)
        
        load_data_to_db(current_file, forecast_file)
    else:
        print("Erro: Não foi possível encontrar os dois arquivos CSV de clima (current e forecast) mais recentes em 'data/cleaned'.")
        print("Execute os scripts de Extração (E) e Transformação (T) primeiro.")