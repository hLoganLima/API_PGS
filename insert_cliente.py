import os
import json
from supabase import create_client
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

# Obtendo as credenciais do Supabase do arquivo .env
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def connect_to_supabase():
    """Conecta ao Supabase usando as credenciais do .env."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Erro: As credenciais do Supabase não foram carregadas corretamente.")
        exit(1)

    return create_client(SUPABASE_URL, SUPABASE_KEY)

def load_insert_json(file_name):
    """
    Carrega o arquivo JSON contendo os registros para inserção.

    Args:
        file_name (str): Nome do arquivo JSON.

    Returns:
        list: Dados do arquivo JSON.
    """
    data_folder = os.path.join(os.getcwd(), "Data")
    file_path = os.path.join(data_folder, file_name)

    if not os.path.exists(file_path):
        print(f"Arquivo {file_name} não encontrado em {file_path}.")
        exit(1)

    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)

def insert_cliente_table(supabase_client, insert_data):
    """
    Insere novos registros na tabela 'cliente' do Supabase.
    """
    errors = []
    total_records = len(insert_data)
    print(f"Iniciando inserção. Total de registros: {total_records}")

    for index, record in enumerate(insert_data, start=1):
        try:
            # Enviar requisição de INSERT
            response = supabase_client.from_("cliente").insert(record).execute()

            # Verificar se houve erro
            if hasattr(response, 'error') and response.error:
                error_message = response.error.get("message", "Erro desconhecido")
                errors.append({"record": record, "error": error_message})
                print(f"[ERRO] Registro {record['cnpj']}: {error_message}")
            elif isinstance(response, dict) and "error" in response:
                error_message = response["error"].get("message", "Erro desconhecido")
                errors.append({"record": record, "error": error_message})
                print(f"[ERRO] Registro {record['cnpj']}: {error_message}")
            else:
                print(f"[OK] Registro {record['cnpj']} inserido ({index}/{total_records}).")

        except KeyError as ke:
            errors.append({"record": record, "error": str(ke)})
            print(f"[CHAVE FALTANDO] {ke} - Registro: {record}")
        except Exception as e:
            errors.append({"record": record, "error": str(e)})
            print(f"[EXCEPTION] Erro ao inserir registro {record.get('cnpj', 'N/A')}: {e}")

    return errors

def main():
    # Conecta ao Supabase
    supabase_client = connect_to_supabase()

    # Carrega o JSON de inserção (por exemplo "cliente_insert.json")
    insert_data = load_insert_json("cliente_insert.json")

    # Insere os registros no Supabase, um por vez
    errors = insert_cliente_table(supabase_client, insert_data)

    # Exibe o resumo
    print(f"\nProcesso concluído. Total de registros processados: {len(insert_data)}")
    print(f"Total de erros: {len(errors)}")

    if errors:
        print("\nErros encontrados:")
        for error in errors:
            print(f"Registro com erro: {error['record']}")
            print(f"Detalhes do erro: {error['error']}\n")

if __name__ == "__main__":
    main()
