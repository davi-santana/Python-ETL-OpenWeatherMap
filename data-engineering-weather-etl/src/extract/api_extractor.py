import requests
import os
import json
from datetime import datetime
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
BASE_URL = "https://api.openweathermap.org/data/2.5"

# Lista de cidades (latitude e longitude) para coleta
CITIES = [
    {"name": "Sao_Paulo", "lat": -23.5505, "lon": -46.6333},
    {"name": "New_York", "lat": 40.7128, "lon": -74.0060},
    {"name": "London", "lat": 51.5074, "lon": 0.1278},
]

def fetch_weather_data():
    """Coleta dados de clima atual e previsão para as cidades."""
    all_data = []
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for city in CITIES:
        print(f"Coletando dados para {city['name']}...")

        # 1. Clima Atual (Endpoint /weather)
        current_url = f"{BASE_URL}/weather?lat={city['lat']}&lon={city['lon']}&appid={API_KEY}&units=metric"
        current_response = requests.get(current_url)

        if current_response.status_code == 200:
            # Adiciona metadados de cidade e timestamp para rastreamento
            current_data = current_response.json()
            current_data['source_type'] = 'current_weather'
            current_data['city_name'] = city['name']
            current_data['extraction_timestamp'] = timestamp
            all_data.append(current_data)
        else:
            print(f"Erro ao coletar clima atual para {city['name']}: {current_response.status_code}")

        # 2. Previsão de 5 dias (Endpoint /forecast)
        forecast_url = f"{BASE_URL}/forecast?lat={city['lat']}&lon={city['lon']}&appid={API_KEY}&units=metric"
        forecast_response = requests.get(forecast_url)

        if forecast_response.status_code == 200:
            forecast_data = forecast_response.json()
            forecast_data['source_type'] = 'forecast_5day'
            forecast_data['city_name'] = city['name']
            forecast_data['extraction_timestamp'] = timestamp
            all_data.append(forecast_data)
        else:
            print(f"Erro ao coletar previsão para {city['name']}: {forecast_response.status_code}")

    # Salva todos os dados brutos em um único arquivo JSON
    output_path = os.path.join("data", "raw", f"weather_raw_{timestamp}.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(all_data, f, indent=4)

    print(f"\nExtração concluída! Dados salvos em: {output_path}")

if __name__ == "__main__":
    fetch_weather_data()