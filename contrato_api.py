import os
from supabase import create_client, Client
import json
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

# Obtendo as credenciais do Supabase do arquivo .env
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def fetch_contrato_data_row_by_row():
    """
    Busca registros da tabela 'contrato' do Supabase um por um e os salva em um JSON na pasta Data.
    """
    # Verifica se as credenciais foram carregadas corretamente
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Erro: As credenciais do Supabase não foram carregadas corretamente.")
        return None

    # Conecta ao Supabase
    supabase_client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print(f"Conectado ao Supabase: {SUPABASE_URL}")

    # Inicializa a lista para armazenar os registros
    all_data = []

    # Contador para buscar registros
    row_index = 0
    while True:
        try:
            # Busca um registro por vez da tabela 'contrato'
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
    json_file_path = fetch_contrato_data_row_by_row()
    if json_file_path:
        with open(json_file_path, "r") as file:
            data = json.load(file)
            print("Dados lidos do JSON:")
            print(data)
