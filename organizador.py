import os
import json
import pandas as pd
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

# Obtendo o caminho do arquivo CSV do arquivo .env
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH")

def load_csv(file_path, delimiter=";"):
    """Carrega um arquivo CSV."""
    try:
        return pd.read_csv(file_path, delimiter=delimiter)
    except FileNotFoundError:
        print(f"Erro: Arquivo CSV não encontrado em {file_path}.")
        exit(1)
    except Exception as e:
        print(f"Erro ao carregar CSV: {e}")
        exit(1)

def save_to_temp_json(dataframe, prefix, output_dir="Data/"):
    """
    Salva um DataFrame em um arquivo JSON no diretório especificado.
    Retorna o caminho do arquivo salvo.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Ajusta o nome do arquivo para corresponder ao esperado pelo comparador
    temp_file_path = os.path.join(output_dir, f"{prefix}local.json")
    try:
        dataframe.to_json(temp_file_path, orient='records', indent=4)
        print(f"Arquivo JSON salvo: {temp_file_path}")
        return temp_file_path
    except Exception as e:
        print(f"Erro ao salvar JSON: {e}")
        exit(1)

def process_csv(input_csv_path):
    """
    Processa o arquivo CSV e cria arquivos JSON no diretório 'Data' para cliente, contrato e produto.
    """
    # Carregar o CSV
    df = load_csv(input_csv_path)

    # Remover espaços nos cabeçalhos das colunas, se necessário
    df.columns = df.columns.str.strip()

    # Processar tabela `cliente`
    cliente_csv = df[['Cód', 'Razão social', 'CNPJ/CPF']].drop_duplicates()
    cliente_csv = cliente_csv.rename(
        columns={
            'Cód': 'id_siger_cliente',
            'Razão social': 'nome_cliente',
            'CNPJ/CPF': 'cnpj'
        }
    )
    cliente_csv['deletado'] = False  # Adicionar coluna padrão
    cliente_json_path = save_to_temp_json(cliente_csv, 'cliente_')

    # Processar tabela `contrato`
    contrato_csv = df[['Núm.contrato', 'Cód', 'Dt.inc.cont', 'Dt.vig.inic', 'Dt.vig.final']].drop_duplicates()
    contrato_csv = contrato_csv.rename(
        columns={
            'Núm.contrato': 'id_contrato',
            'Cód': 'id_siger_cliente',
            'Dt.inc.cont': 'dt_inic_cont',
            'Dt.vig.inic': 'dt_vig_inic',
            'Dt.vig.final': 'dt_vig_final'
        }
    )
    contrato_csv['deletado'] = False  # Adicionar coluna padrão
    # Preencher valores padrão para `dt_vig_final` se estiverem vazios
    contrato_csv['dt_vig_final'] = contrato_csv['dt_vig_final'].fillna('2099-01-01')
    contrato_json_path = save_to_temp_json(contrato_csv, 'contrato_')

    # Processar tabela `produto`
    produto_csv = df[
        ['Código', 'Desc.item', 'Descrição', 'Núm.lote forn', 'Núm.lote', 'Descr.Sit.item cont.(enumerado)',
         'Núm.contrato', 'Cód']].drop_duplicates()
    produto_csv = produto_csv.rename(
        columns={
            'Código': 'id_produto_siger',
            'Desc.item': 'nome_produto',
            'Descrição': 'tipo_produto',
            'Núm.lote forn': 'num_serie',
            'Núm.lote': 'num_lote',
            'Descr.Sit.item cont.(enumerado)': 'ativo',
            'Núm.contrato': 'id_contrato',
            'Cód': 'id_cliente_siger'
        }
    )
    # Mapear os valores de "ativo" (exemplo: 1 = True, 0 = False)
    produto_csv['ativo'] = produto_csv['ativo'].apply(lambda x: True if str(x).strip() == '1' else False)
    produto_csv['deletado'] = False  # Adicionar coluna padrão
    produto_json_path = save_to_temp_json(produto_csv, 'produto_')

    return cliente_json_path, contrato_json_path, produto_json_path

def main():
    if not CSV_FILE_PATH:
        print("Erro: O caminho do arquivo CSV não foi configurado no .env.")
        exit(1)

    cliente_json, contrato_json, produto_json = process_csv(CSV_FILE_PATH)

    print(f"Dados de cliente salvos em: {cliente_json}")
    print(f"Dados de contrato salvos em: {contrato_json}")
    print(f"Dados de produto salvos em: {produto_json}")

    # Exemplo de leitura dos arquivos JSON temporários
    for json_file in [cliente_json, contrato_json, produto_json]:
        with open(json_file, 'r') as f:
            data = json.load(f)
            print(f"Número de registros em {json_file}: {len(data)}")

    # Remover arquivos JSON temporários após leitura (opcional)
    # for json_file in [cliente_json, contrato_json, produto_json]:
    #    os.remove(json_file)
    #    print(f"Arquivo temporário removido: {json_file}")

if __name__ == "__main__":
    main()
