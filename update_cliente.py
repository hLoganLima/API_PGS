import os
import json
from supabase import create_client, Client


def connect_to_supabase():
    """Conecta ao Supabase usando as credenciais fornecidas."""
    SUPABASE_URL = "https://yrgxpdcqlkypnxsfozrz.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlyZ3hwZGNxbGt5cG54c2ZvenJ6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzUyMTQxNTAsImV4cCI6MjA1MDc5MDE1MH0.tKTiR2771WIuzVgmbRqE4x-2O6h86JA5Tc_Z3ICmVbg"
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def load_update_json(file_name):
    """
    Carrega o arquivo JSON contendo os registros de atualização.

    Args:
        file_name (str): Nome do arquivo JSON.

    Returns:
        list: Dados do arquivo JSON.
    """
    # Corrigindo o caminho para evitar duplicação de 'Data'
    current_dir = os.getcwd()  # Diretório atual
    data_folder = os.path.join(current_dir, "Data")  # Pasta 'Data'
    file_path = os.path.normpath(os.path.join(data_folder, file_name))  # Caminho normalizado

    if not os.path.exists(file_path):
        print(f"Arquivo {file_name} não encontrado em {file_path}.")
        exit(1)

    with open(file_path, 'r') as file:
        return json.load(file)


def update_cliente_table_line_by_line(supabase_client, update_data):
    """
    Atualiza a tabela 'cliente' do Supabase linha por linha.

    Args:
        supabase_client (Client): Cliente do Supabase.
        update_data (list): Dados a serem atualizados.
    """
    errors = []
    total_records = len(update_data)
    print(f"Iniciando atualização linha por linha. Total de registros: {total_records}")

    for idx, record in enumerate(update_data, start=1):
        try:
            # Atualizar o registro individualmente
            response = supabase_client.table("cliente").upsert(record).execute()
            if response.status_code != 200:
                errors.append({"record": record, "error": response.error_message})
                print(f"Erro ao atualizar registro {idx}: {response.error_message}")
            else:
                print(f"Registro {idx}/{total_records} atualizado com sucesso.")
        except Exception as e:
            errors.append({"record": record, "error": str(e)})
            print(f"Erro inesperado ao atualizar registro {idx}: {e}")

    return errors


def main():
    # Conecta ao Supabase
    supabase_client = connect_to_supabase()

    # Carrega os dados do arquivo produto_update.json
    update_data = load_update_json("cliente_update.json")  # Apenas o nome do arquivo, sem 'Data/'

    # Atualiza os registros no Supabase linha por linha
    errors = update_cliente_table_line_by_line(supabase_client, update_data)

    # Exibe o resumo
    print(f"\nProcesso concluído. Total de registros processados: {len(update_data)}")
    print(f"Total de erros: {len(errors)}")
    if errors:
        print("\nErros encontrados:")
        for error in errors:
            print(f"Registro com erro: {error['record']}, Erro: {error['error']}")


if __name__ == "__main__":
    main()
