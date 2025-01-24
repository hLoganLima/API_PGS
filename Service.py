import subprocess
import os

def execute_script(script_name):
    """
    Executa um script Python como subprocesso.
    Args:
        script_name (str): Nome do script Python a ser executado.
    """
    try:
        print(f"Iniciando {script_name}...")
        subprocess.run(["python", script_name], check=True)
        print(f"{script_name} concluído com sucesso!\n")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar {script_name}: {e}\n")

def clean_data_folder():
    """
    Remove todos os arquivos JSON da pasta Data.
    """
    data_folder = os.path.join(os.getcwd(), "Data")
    if os.path.exists(data_folder):
        for file_name in os.listdir(data_folder):
            if file_name.endswith(".json"):
                file_path = os.path.join(data_folder, file_name)
                try:
                    os.remove(file_path)
                    print(f"Removido: {file_path}")
                except Exception as e:
                    print(f"Erro ao remover {file_path}: {e}")
    else:
        print("Pasta Data não encontrada.")

def main():
    # Etapa 1: Executar os scripts de API e organizador
    api_and_organizer_scripts = [
        "cliente_api.py",
        "contrato_api.py",
        "produto_api.py",
        "organizador.py"
    ]

    print("Etapa 1: Executando scripts de API e organizador...")
    processes = [subprocess.Popen(["python", script]) for script in api_and_organizer_scripts]
    for process in processes:
        process.wait()

    # Etapa 2: Executar o comparador
    print("\nEtapa 2: Executando o comparador...")
    execute_script("comparador.py")

    # Etapa 3: Executar os updates
    update_scripts = [
        "update_cliente.py",
        "update_contrato.py",
        "update_produto.py"
    ]

    print("\nEtapa 3: Executando updates...")
    processes = [subprocess.Popen(["python", script]) for script in update_scripts]
    for process in processes:
        process.wait()

    # Etapa 4: Executar os inserts
    insert_scripts = [
        "insert_cliente.py",
        "insert_contrato.py",
        "insert_produto.py"
    ]

    print("\nEtapa 4: Executando inserts...")
    processes = [subprocess.Popen(["python", script]) for script in insert_scripts]
    for process in processes:
        process.wait()

    # Etapa 5: Limpar a pasta Data
    print("\nEtapa 5: Limpando a pasta Data...")
    clean_data_folder()

    print("\nProcesso completo! Todos os scripts foram executados com sucesso.")

if __name__ == "__main__":
    main()
