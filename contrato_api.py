import os
from supabase import create_client, Client
import json

supabase_url = "https://yrgxpdcqlkypnxsfozrz.supabase.co"
supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlyZ3hwZGNxbGt5cG54c2ZvenJ6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzUyMTQxNTAsImV4cCI6MjA1MDc5MDE1MH0.tKTiR2771WIuzVgmbRqE4x-2O6h86JA5Tc_Z3ICmVbg"


def fetch_contrato_data_row_by_row(supabase_url, supabase_key):
    """
    Busca registros da tabela 'contrato' do Supabase um por um e os salva em um JSON na pasta Data.
    """
    # Conecta ao Supabase
    supabase_client: Client = create_client(supabase_url, supabase_key)
    print(f"Conectado ao Supabase: {supabase_url}")

    # Inicializa a lista para armazenar os registros
    all_data = []

    # Contador para buscar registros
    row_index = 0
    while True:
        # Busca um registro por vez da tabela 'contrato'
        try:
            response = supabase_client.table("contrato").select("*").range(row_index, row_index).execute()

            # Verifica se o registro existe
            if response.data:
                all_data.append(response.data[0])  # Adiciona o registro à lista
                print(f"Registro {row_index + 1} obtido: {response.data[0]}")
                row_index += 1  # Avança para o próximo registro
            else:
                print("Todos os registros da tabela 'contrato' foram processados.")
                break
        except Exception as e:
            print(f"Erro ao buscar registro {row_index + 1}: {e}")
            break

    # Salva todos os dados em um JSON na pasta Data
    if all_data:
        # Definir o diretório 'Data' onde os arquivos serão salvos
        data_dir = os.path.join(os.getcwd(), 'Data')
        # Cria o diretório 'Data' caso não exista
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        # Caminho para o arquivo JSON
        json_path = os.path.join(data_dir, "data_contrato_supabase.json")

        # Salva os dados em um arquivo JSON na pasta 'Data'
        with open(json_path, 'w') as temp_file:
            json.dump(all_data, temp_file, indent=4)
        print(f"Todos os dados da tabela 'contrato' salvos em: {json_path}")
        return json_path
    else:
        print("Nenhum dado encontrado na tabela 'contrato'.")
        return None


# Teste: Lendo dados da tabela 'contrato' registro por registro e salvando no JSON
if __name__ == "__main__":
    json_file_path = fetch_contrato_data_row_by_row(supabase_url, supabase_key)
    if json_file_path:
        # Lê o JSON salvo na pasta 'Data' para exibição
        with open(json_file_path, "r") as file:
            data = json.load(file)
            print("Dados lidos do JSON:")
            print(data)
