import threading
import runpy

# Importa os módulos dos scripts.
# Certifique-se de que os módulos estejam no mesmo diretório ou no PYTHONPATH.
import organizador
import cliente_api
import contrato_api
import produto_api
import comparador
import update_cliente
import update_contrato
import update_produto
import insert_cliente
import insert_contrato
import insert_produto


def run_module(module):
    """
    Executa o módulo de duas maneiras:
      - Se o módulo possuir uma função main(), ela é chamada.
      - Caso contrário, utiliza runpy para executar o módulo como se ele fosse o script principal,
        rodando o bloco 'if __name__ == "__main__":'.
    """
    if hasattr(module, "main"):
        try:
            print(f"Iniciando {module.__name__}.main()")
            module.main()
            print(f"Finalizado {module.__name__}.main()")
        except Exception as e:
            print(f"Ocorreu um erro ao executar {module.__name__}.main(): {e}")
    else:
        print(f"O módulo {module.__name__} não possui uma função main(); executando via runpy.run_module()")
        try:
            runpy.run_module(module.__name__, run_name="__main__")
            print(f"Finalizado {module.__name__} (via runpy)")
        except Exception as e:
            print(f"Ocorreu um erro ao executar {module.__name__} (via runpy): {e}")


def run_group_concurrently(modules):
    """
    Recebe uma lista de módulos e os executa em paralelo utilizando threads.
    Aguarda a finalização de todas as threads antes de retornar.
    """
    threads = []
    for mod in modules:
        thread = threading.Thread(target=run_module, args=(mod,))
        thread.start()
        threads.append(thread)
    # Aguarda todas as threads terminarem
    for thread in threads:
        thread.join()


def main():
    # Grupo 1: Executa os módulos em paralelo.
    grupo1 = [
        organizador,
        cliente_api,
        contrato_api,
        produto_api
    ]
    print("=== Executando Grupo 1 (execução paralela) ===")
    run_group_concurrently(grupo1)

    # Grupo 2: Executa o comparador de forma sequencial.
    print("\n=== Executando comparador (execução sequencial) ===")
    run_module(comparador)

    # Grupo 3: Executa os módulos de update/insert em paralelo.
    grupo3 = [
        update_cliente,
        update_contrato,
        update_produto,
        insert_cliente,
        insert_contrato,
        insert_produto
    ]
    print("\n=== Executando Grupo 3 (execução paralela) ===")
    run_group_concurrently(grupo3)

    print("\nTodas as execuções foram finalizadas.")


if __name__ == "__main__":
    main()
