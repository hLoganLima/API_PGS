import os
import json
from tempfile import NamedTemporaryFile
from supabase import create_client, Client

# Configurando o Supabase
SUPABASE_URL = "https://yrgxpdcqlkypnxsfozrz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlyZ3hwZGNxbGt5cG54c2ZvenJ6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzUyMTQxNTAsImV4cCI6MjA1MDc5MDE1MH0.tKTiR2771WIuzVgmbRqE4x-2O6h86JA5Tc_Z3ICmVbg"

# Diretório padrão para arquivos temporários
TEMP_DIR = os.getcwd()

# Funções comuns

def save_to_temp_json(data, filename):
    """
    Salva dados em um arquivo JSON no diretório TEMP_DIR.

    Args:
        data (list/dict): Dados a serem salvos.
        filename (str): Nome do arquivo.

    Returns:
        str: Caminho completo do arquivo JSON salvo.
    """
    if not data:
        return None

    json_path = os.path.join(TEMP_DIR, filename)
    with open(json_path, 'w') as temp_file:
        json.dump(data, temp_file, indent=4)

    return json_path

def load_json(json_path):
    """Carrega dados de um arquivo JSON."""
    with open(json_path, 'r') as file:
        return json.load(file)

def find_differences(local_data, api_data, unique_keys):
    """
    Compara os dados locais com os dados da API e identifica inserções e atualizações.

    Args:
        local_data (list): Dados locais.
        api_data (list): Dados da API.
        unique_keys (list): Lista de chaves únicas para identificar registros.

    Returns:
        dict: Um dicionário contendo listas de inserções e atualizações.
    """
    api_data_dict = {tuple(item[key] for key in unique_keys): item for item in api_data}

    to_insert = []
    to_update = []

    for local_record in local_data:
        unique_key = tuple(local_record[key] for key in unique_keys)

        if unique_key not in api_data_dict:
            to_insert.append(local_record)
        else:
            api_record = api_data_dict[unique_key]
            if local_record != api_record:
                to_update.append(local_record)

    return {"to_insert": to_insert, "to_update": to_update}

def fetch_data_from_supabase(table_name):
    """
    Busca registros de uma tabela no Supabase.

    Args:
        table_name (str): Nome da tabela.

    Returns:
        list: Dados obtidos da tabela.
    """
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    response = supabase.table(table_name).select("*").execute()

    if response.data:
        return response.data
    else:
        return []

# Funções por módulo

def cliente_api():
    """Processa dados da API de clientes e salva no JSON temporário."""
    api_data = fetch_data_from_supabase("cliente")
    return save_to_temp_json(api_data, "cliente_api_temp.json")

def produto_api():
    """Processa dados da API de produtos e salva no JSON temporário."""
    api_data = fetch_data_from_supabase("produto")
    return save_to_temp_json(api_data, "produto_api_temp.json")

def organizador():
    """Organiza dados locais (mock para este exemplo)."""
    local_cliente = [
        {"id_siger_cliente": 1, "nome": "Cliente A", "ativo": True},
        {"id_siger_cliente": 2, "nome": "Cliente B", "ativo": True}
    ]
    return save_to_temp_json(local_cliente, "cliente_local.json")

def comparador():
    """Compara dados locais e da API, gerando arquivos JSON para inserções/atualizações."""
    local_cliente = load_json(os.path.join(TEMP_DIR, "cliente_local.json"))
    api_cliente = load_json(os.path.join(TEMP_DIR, "cliente_api_temp.json"))

    diff = find_differences(local_cliente, api_cliente, unique_keys=["id_siger_cliente"])

    cliente_insert_path = save_to_temp_json(diff["to_insert"], "cliente_insert.json")
    cliente_update_path = save_to_temp_json(diff["to_update"], "cliente_update.json")

    return {
        "cliente_insert": cliente_insert_path,
        "cliente_update": cliente_update_path
    }

# Execução principal
if __name__ == "__main__":
    print("Organizando dados locais...")
    organizador()

    print("Buscando dados da API de clientes...")
    cliente_api()

    print("Comparando dados...")
    paths = comparador()

    for key, path in paths.items():
        if path:
            print(f"{key} salvo em: {path}")
