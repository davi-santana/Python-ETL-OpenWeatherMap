import pandas as pd
import json
import os
from datetime import datetime

def transform_weather_data(file_path):
    """
    Carrega o JSON bruto e o transforma em DataFrames tabulares limpos
    para Clima Atual e Previsão.
    Retorna os caminhos dos CSVs limpos.
    """
    print(f"Iniciando transformação para o arquivo: {file_path}")
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Erro: Arquivo não encontrado em {file_path}")
        return None, None
        
    # Separa os dados brutos por tipo de fonte
    current_data = [item for item in data if item.get('source_type') == 'current_weather']
    forecast_data = [item for item in data if item.get('source_type') == 'forecast_5day']
    
    # --- 1. Transformação dos Dados de Clima Atual ---
    df_current = pd.json_normalize(
        current_data,
        sep='_',
        meta=['city_name', 'extraction_timestamp', 'dt', 'id', 'name']
    )
    
    # Seleção e Limpeza
    df_current = df_current[[
        'city_name', 'extraction_timestamp', 'dt', 'id', 'name', 
        'main_temp', 'main_feels_like', 'main_temp_min', 'main_temp_max', 
        'main_pressure', 'main_humidity', 'visibility', 
        'wind_speed', 'wind_deg', 'clouds_all', 'sys_sunrise', 'sys_sunset', 
        'coord_lat', 'coord_lon', 'timezone', 'weather' # Adicionamos 'weather' para normalizar
    ]].copy()
    
    # CORREÇÃO 1: Conversão de Timestamps (com formato explícito)
    df_current['dt_iso'] = pd.to_datetime(df_current['dt'], unit='s', utc=True)
    df_current['extraction_dt_iso'] = pd.to_datetime(
        df_current['extraction_timestamp'], 
        format='%Y%m%d_%H%M%S'
    )
    df_current['sys_sunrise_iso'] = pd.to_datetime(df_current['sys_sunrise'], unit='s', utc=True)
    df_current['sys_sunset_iso'] = pd.to_datetime(df_current['sys_sunset'], unit='s', utc=True)
    
    # CORREÇÃO 2: Normalização do campo 'weather' de forma segura
    # Pega o primeiro dicionário da lista 'weather' para extrair 'main' e 'description'
    weather_flat_current = pd.json_normalize(df_current['weather'].str[0]) 
    
    df_current['weather_main'] = weather_flat_current.get('main', None)
    df_current['weather_description'] = weather_flat_current.get('description', None)
    
    df_current = df_current.drop(columns=['dt', 'sys_sunrise', 'sys_sunset', 'id', 'extraction_timestamp', 'weather'])
    
    print("Transformação de Clima Atual concluída.")
    
    # --- 2. Transformação dos Dados de Previsão ---
    df_forecast = pd.json_normalize(
        forecast_data,
        record_path=['list'],
        meta=['city_name', 'extraction_timestamp'],
        sep='_'
    )
    
    # Seleção e Limpeza
    df_forecast = df_forecast[[
        'city_name', 'extraction_timestamp', 'dt', 'dt_txt', 
        'main_temp', 'main_feels_like', 'main_temp_min', 'main_temp_max', 
        'main_pressure', 'main_humidity', 'wind_speed', 'wind_deg', 
        'clouds_all', 'pop', 'sys_pod', 'weather' # Garantimos 'weather' na seleção
    ]].copy()
    
    # CORREÇÃO 3: Conversão de Timestamps (com formato explícito)
    df_forecast['dt_iso'] = pd.to_datetime(df_forecast['dt'], unit='s', utc=True)
    df_forecast['dt_txt'] = pd.to_datetime(df_forecast['dt_txt'])
    df_forecast['extraction_dt_iso'] = pd.to_datetime(
        df_forecast['extraction_timestamp'],
        format='%Y%m%d_%H%M%S'
    )
    
    # CORREÇÃO 4: Normalização do campo 'weather' de forma segura
    weather_flat_forecast = pd.json_normalize(df_forecast['weather'].str[0])
    
    df_forecast['weather_main'] = weather_flat_forecast.get('main', None)
    df_forecast['weather_description'] = weather_flat_forecast.get('description', None)
    
    df_forecast = df_forecast.drop(columns=['dt', 'weather', 'extraction_timestamp'])
    
    print("Transformação de Previsão concluída.")

    # --- 3. Salvar os resultados transformados ---
    output_dir = os.path.join("data", "cleaned")
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp_prefix = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    current_output_path = os.path.join(output_dir, f"current_weather_{timestamp_prefix}.csv")
    forecast_output_path = os.path.join(output_dir, f"forecast_weather_{timestamp_prefix}.csv")
    
    df_current.to_csv(current_output_path, index=False)
    df_forecast.to_csv(forecast_output_path, index=False)
    
    print(f"\nDados limpos salvos:\n- Clima Atual: {current_output_path}\n- Previsão: {forecast_output_path}")
    
    # Retorno para o Airflow (XCom)
    return current_output_path, forecast_output_path


if __name__ == "__main__":
    # Lógica para rodar fora do Airflow
    raw_dir = os.path.join("data", "raw")
    if os.path.exists(raw_dir):
        files = [os.path.join(raw_dir, f) for f in os.listdir(raw_dir) if f.endswith('.json')]
        if files:
            latest_file = max(files, key=os.path.getctime)
            transform_weather_data(latest_file)
        else:
            print("Nenhum arquivo JSON encontrado. Execute a extração.")
    else:
        print("Pasta 'data/raw' não existe.")