import json
import os


def load_json(json_path):
    """Carrega dados de um arquivo JSON."""
    with open(json_path, 'r') as file:
        return json.load(file)


def find_differences(local_data, api_data, unique_keys):
    """
    Compara os dados locais com os dados da API e identifica inserções e atualizações.

    Args:
        local_data (list): Dados gerados localmente.
        api_data (list): Dados da API.
        unique_keys (list): Lista de chaves únicas para identificar os registros.

    Returns:
        dict: Um dicionário contendo listas de inserções e atualizações.
    """
    api_data_dict = {tuple(item[key] for key in unique_keys): item for item in api_data}

    to_insert = []
    to_update = []

    for local_record in local_data:
        unique_key = tuple(local_record[key] for key in unique_keys)

        if unique_key not in api_data_dict:
            # Registro existe localmente, mas não na API -> Inserção
            to_insert.append(local_record)
        else:
            # Registro existe na API -> Verificar necessidade de atualização
            api_record = api_data_dict[unique_key]
            if local_record != api_record:
                to_update.append(local_record)

    return {"to_insert": to_insert, "to_update": to_update}


def save_to_temp_json(data, prefix):
    """
    Salva dados em um arquivo JSON temporário.
    """
    if not data:
        return None

    # Caminho para salvar na pasta 'Data' no diretório atual
    data_folder = os.path.join(os.getcwd(), "Data")
    os.makedirs(data_folder, exist_ok=True)

    # Nome do arquivo JSON baseado no prefixo
    json_file_path = os.path.join(data_folder, f"{prefix}.json")

    # Salvar os dados no arquivo JSON
    with open(json_file_path, 'w') as temp_file:
        json.dump(data, temp_file, indent=4)

    return json_file_path


def compare_and_generate_updates():
    """
    Compara os dados locais com os dados das APIs e gera JSONs contendo
    os registros a serem inseridos ou atualizados.
    """
    # Caminhos dos JSONs gerados na pasta 'Data'
    paths = {
        "local_cliente": os.path.join(os.getcwd(), "Data", "cliente_local.json"),  # Gerado pelo organizador.py
        "api_cliente": os.path.join(os.getcwd(), "Data", "data_cliente_supabase.json"),  # Gerado pelo cliente_api.py
        "local_contrato": os.path.join(os.getcwd(), "Data", "contrato_local.json"),
        "api_contrato": os.path.join(os.getcwd(), "Data", "data_contrato_supabase.json"),
        "local_produto": os.path.join(os.getcwd(), "Data", "produto_local.json"),
        "api_produto": os.path.join(os.getcwd(), "Data", "data_produto_supabase.json")
    }

    # Carregar os dados
    local_cliente = load_json(paths["local_cliente"])
    api_cliente = load_json(paths["api_cliente"])
    local_contrato = load_json(paths["local_contrato"])
    api_contrato = load_json(paths["api_contrato"])
    local_produto = load_json(paths["local_produto"])
    api_produto = load_json(paths["api_produto"])

    # Comparar e identificar inserções/atualizações
    cliente_diff = find_differences(local_cliente, api_cliente, unique_keys=["id_siger_cliente"])
    contrato_diff = find_differences(local_contrato, api_contrato, unique_keys=["id_contrato"])
    produto_diff = find_differences(local_produto, api_produto, unique_keys=["id_produto_siger"])

    # Salvar os resultados em arquivos JSON na pasta 'Data'
    cliente_insert_path = save_to_temp_json(cliente_diff["to_insert"], "cliente_insert")
    cliente_update_path = save_to_temp_json(cliente_diff["to_update"], "cliente_update")
    contrato_insert_path = save_to_temp_json(contrato_diff["to_insert"], "contrato_insert")
    contrato_update_path = save_to_temp_json(contrato_diff["to_update"], "contrato_update")
    produto_insert_path = save_to_temp_json(produto_diff["to_insert"], "produto_insert")
    produto_update_path = save_to_temp_json(produto_diff["to_update"], "produto_update")

    # Retornar os caminhos dos arquivos temporários
    return {
        "cliente_insert": cliente_insert_path,
        "cliente_update": cliente_update_path,
        "contrato_insert": contrato_insert_path,
        "contrato_update": contrato_update_path,
        "produto_insert": produto_insert_path,
        "produto_update": produto_update_path
    }


if __name__ == "__main__":
    json_paths = compare_and_generate_updates()
    for key, path in json_paths.items():
        if path:
            print(f"{key} JSON salvo em: {path}")
