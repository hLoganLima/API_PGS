from supabase import create_client
import pandas as pd


# Função para carregar um arquivo CSV
def load_csv(file_path):
    """Carrega um arquivo CSV."""
    return pd.read_csv(file_path)


# Função para verificar se um registro já existe no Supabase
def record_exists(supabase_client, table, unique_column, value):
    """
    Verifica se um registro já existe no Supabase.
    :param supabase_client: Cliente do Supabase.
    :param table: Nome da tabela.
    :param unique_column: Coluna única para verificar existência.
    :param value: Valor da coluna para verificar.
    :return: True se o registro existir, False caso contrário.
    """
    response = supabase_client.table(table).select("*").eq(unique_column, value).execute()
    return len(response.data) > 0


# Função para converter datas no DataFrame para o formato ISO 8601 (YYYY-MM-DD)
def format_date_columns(dataframe, date_columns):
    """
    Converte colunas de data no DataFrame para o formato ISO 8601.
    :param dataframe: DataFrame com os dados.
    :param date_columns: Lista de nomes das colunas de data a serem formatadas.
    """
    for col in date_columns:
        if col in dataframe.columns:
            dataframe[col] = pd.to_datetime(dataframe[col], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
    return dataframe


# Função para realizar update ou insert
def upsert_row_by_row(dataframe, table, supabase_url, supabase_key, unique_column, date_columns=None):
    """
    Realiza updates ou inserts no Supabase, linha a linha.
    :param dataframe: DataFrame com os dados a serem enviados.
    :param table: Nome da tabela no Supabase.
    :param supabase_url: URL do Supabase.
    :param supabase_key: Chave de API do Supabase.
    :param unique_column: Coluna única usada para verificar existência do registro.
    :param date_columns: Lista de colunas de data a serem formatadas (opcional).
    """
    # Criar cliente Supabase
    supabase_client = create_client(supabase_url, supabase_key)

    # Verificar se há registros no DataFrame
    if dataframe.empty:
        print(f"[{table}] Nenhum registro encontrado no arquivo.")
        return

    # Formatar colunas de data, se aplicável
    if date_columns:
        dataframe = format_date_columns(dataframe, date_columns)

    # Iterar por cada linha no DataFrame
    for _, row in dataframe.iterrows():
        # Converter a linha para dicionário
        record = row.to_dict()
        unique_value = record[unique_column]

        # Verificar se o registro já existe
        if record_exists(supabase_client, table, unique_column, unique_value):
            # Fazer update
            response = supabase_client.table(table).update(record).eq(unique_column, unique_value).execute()
            operation = "Update"
        else:
            # Fazer insert
            response = supabase_client.table(table).insert(record).execute()
            operation = "Insert"

        # Verifica se houve erro ou sucesso
        if response.data:
            print(f"[{table}] {operation} bem-sucedido para o registro: {unique_value}.")
        else:
            print(f"[{table}] Erro no {operation} do registro {unique_value}: {response.error}")


# Função principal para processar os dados e enviar para o Supabase
def main():
    # Credenciais do Supabase
    supabase_url = "https://yrgxpdcqlkypnxsfozrz.supabase.co"
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlyZ3hwZGNxbGt5cG54c2ZvenJ6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzUyMTQxNTAsImV4cCI6MjA1MDc5MDE1MH0.tKTiR2771WIuzVgmbRqE4x-2O6h86JA5Tc_Z3ICmVbg"

    # Configuração das tabelas
    tables_config = {
        "cliente": {
            "file_path": "finalCliente.csv",
            "unique_column": "cnpj",
            "date_columns": []  # Não há colunas de data na tabela cliente
        },
        "contrato": {
            "file_path": "finalContrato.csv",
            "unique_column": "id_contrato",
            "date_columns": ["dt_inic_cont", "dt_vig_inic", "dt_vig_final"]  # Colunas de data
        },
        "produto": {
            "file_path": "finalProduto.csv",
            "unique_column": "id_produto_siger",
            "date_columns": []  # Não há colunas de data na tabela produto
        }
    }

    # Processar cada tabela
    for table_name, config in tables_config.items():
        print(f"Processando tabela: {table_name}")

        # Carregar os dados do arquivo CSV
        try:
            dataframe = load_csv(config["file_path"])
        except FileNotFoundError:
            print(f"[{table_name}] Arquivo {config['file_path']} não encontrado. Pulando.")
            continue

        # Enviar os dados para o Supabase linha a linha (insert ou update)
        upsert_row_by_row(
            dataframe=dataframe,
            table=table_name,
            supabase_url=supabase_url,
            supabase_key=supabase_key,
            unique_column=config["unique_column"],
            date_columns=config["date_columns"]
        )


if __name__ == "__main__":
    main()
