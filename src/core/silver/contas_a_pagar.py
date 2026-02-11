import pandas as pd
import yaml
from google.cloud import storage
from datetime import datetime
import os
import json


def carregar_config():
    """
    Carrega as configurações do arquivo config.yaml

    Returns:
        dict: Dicionário com as configurações
    """
    config_path = os.path.join(os.path.dirname(
        __file__), '..', '..', '..', 'config', 'config.yaml')
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def ler_do_gcs(bucket_name, caminho_gcs, credentials_path):
    """
    Lê um arquivo parquet do Google Cloud Storage

    Args:
        bucket_name: Nome do bucket GCS
        caminho_gcs: Caminho completo do arquivo no bucket
        credentials_path: Caminho para o arquivo de credenciais

    Returns:
        pd.DataFrame: DataFrame com os dados lidos ou None em caso de erro
    """
    try:
        # Configurar credenciais
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        # Criar cliente do GCS
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Criar blob
        blob = bucket.blob(caminho_gcs)

        # Baixar para arquivo temporário
        import tempfile
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, 'temp_contas_a_pagar.parquet')

        blob.download_to_filename(temp_file)

        # Ler o arquivo parquet
        df = pd.read_parquet(temp_file, engine='pyarrow')

        print(
            f"[CONTAS A PAGAR SILVER] Arquivo lido do GCS: gs://{bucket_name}/{caminho_gcs}")
        print(f"[CONTAS A PAGAR SILVER] Total de registros lidos: {len(df)}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return df

    except Exception as e:
        print(f"[CONTAS A PAGAR SILVER] Erro ao ler do GCS: {e}")
        return None


def salvar_no_gcs(df, bucket_name, caminho_gcs, credentials_path, nome_arquivo):
    """
    Salva o DataFrame no Google Cloud Storage em formato parquet

    Args:
        df: DataFrame a ser salvo
        bucket_name: Nome do bucket GCS
        caminho_gcs: Caminho dentro do bucket
        credentials_path: Caminho para o arquivo de credenciais
        nome_arquivo: Nome do arquivo a ser salvo

    Returns:
        str: Caminho completo do arquivo salvo ou None em caso de erro
    """
    try:
        # Configurar credenciais
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        # Criar cliente do GCS
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Criar caminho completo
        caminho_completo = f"{caminho_gcs}/{nome_arquivo}"

        # Salvar DataFrame temporariamente como parquet
        import tempfile
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, nome_arquivo)

        df.to_parquet(temp_file, index=False, engine='pyarrow')

        # Upload para o GCS
        blob = bucket.blob(caminho_completo)
        blob.upload_from_filename(temp_file)

        print(
            f"[CONTAS A PAGAR SILVER] Arquivo salvo no GCS: gs://{bucket_name}/{caminho_completo}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return caminho_completo

    except Exception as e:
        print(f"[CONTAS A PAGAR SILVER] Erro ao salvar no GCS: {e}")
        return None


def normalizar_categorias(df):
    """
    Normaliza a coluna categorias do DataFrame duplicando linhas.

    A coluna categorias contém uma lista/array de categorias. 
    Para cada registro com múltiplas categorias, cria uma linha para cada categoria.

    Args:
        df: DataFrame com a coluna categorias

    Returns:
        pd.DataFrame: DataFrame com linhas duplicadas para cada categoria
    """
    try:
        print(f"[CONTAS A PAGAR SILVER] Iniciando normalização da coluna categorias...")

        # Verificar se a coluna existe
        if 'categorias' not in df.columns:
            print(
                f"[CONTAS A PAGAR SILVER] AVISO: Coluna 'categorias' não encontrada no DataFrame")
            return df

        # Criar cópia do DataFrame
        df_normalizado = df.copy()

        # Verificar se há dados na coluna categorias
        dados_validos = df_normalizado['categorias'].notna().sum()
        print(
            f"[CONTAS A PAGAR SILVER] Registros com categorias válidas: {dados_validos}/{len(df_normalizado)}")

        if dados_validos == 0:
            print(
                f"[CONTAS A PAGAR SILVER] Nenhum dado válido na coluna categorias para normalizar")
            return df_normalizado

        # Parsear JSON da coluna categorias (se for string)
        def parse_categorias(valor):
            if pd.isna(valor):
                return []
            if isinstance(valor, str):
                try:
                    parsed = json.loads(valor)
                    # Se for um dict, retorna como lista com um elemento
                    if isinstance(parsed, dict):
                        return [parsed]
                    # Se for uma lista, retorna a lista
                    elif isinstance(parsed, list):
                        return parsed
                    else:
                        return []
                except:
                    return []
            # Se já for lista ou dict
            if isinstance(valor, list):
                return valor
            if isinstance(valor, dict):
                return [valor]
            return []

        df_normalizado['categorias_parsed'] = df_normalizado['categorias'].apply(
            parse_categorias)

        # Contar total de categorias antes da explosão
        total_categorias = df_normalizado['categorias_parsed'].apply(len).sum()
        print(
            f"[CONTAS A PAGAR SILVER] Total de categorias a expandir: {total_categorias}")

        # Explodir o DataFrame - criar uma linha para cada categoria
        df_exploded = df_normalizado.explode(
            'categorias_parsed', ignore_index=True)

        # Normalizar os dados da categoria (se for dict)
        categorias_df = pd.json_normalize(
            df_exploded['categorias_parsed'].dropna())

        # Se houver dados normalizados, adicionar ao DataFrame
        if not categorias_df.empty:
            # Adicionar prefixo 'categoria_' nas colunas
            categorias_df.columns = ['categoria_' +
                                     str(col) for col in categorias_df.columns]

            # Resetar índice do categorias_df para combinar corretamente
            categorias_df.index = df_exploded[df_exploded['categorias_parsed'].notna(
            )].index

            # Combinar com o DataFrame explodido
            df_exploded = df_exploded.join(categorias_df)

        # Remover colunas temporárias
        df_exploded = df_exploded.drop(
            columns=['categorias', 'categorias_parsed'])

        print(f"[CONTAS A PAGAR SILVER] Normalização concluída!")
        print(
            f"[CONTAS A PAGAR SILVER] Registros antes: {len(df_normalizado)}")
        print(f"[CONTAS A PAGAR SILVER] Registros depois: {len(df_exploded)}")
        if not categorias_df.empty:
            print(
                f"[CONTAS A PAGAR SILVER] Colunas de categoria criadas: {list(categorias_df.columns)}")

        return df_exploded

    except Exception as e:
        print(f"[CONTAS A PAGAR SILVER] Erro ao normalizar categorias: {e}")
        import traceback
        print(
            f"[CONTAS A PAGAR SILVER] Detalhes do erro:\n{traceback.format_exc()}")
        return df


def processar_contas_a_pagar_silver(config_yaml):
    """
    Processa os dados da camada bronze para a camada silver.

    Args:
        config_yaml: Dicionário com as configurações

    Returns:
        bool: True se o processo foi bem-sucedido, False caso contrário
    """
    try:
        # Configurações do GCS
        bucket_name = config_yaml['gcs']['bronze']['bucket']
        caminho_bronze = f"{config_yaml['gcs']['bronze']['folder']}/mysql_omie/contas_a_pagar.parquet"
        credentials_path = config_yaml['credentials-path']

        # Ler dados da camada bronze
        print(f"[CONTAS A PAGAR SILVER] Lendo dados da camada bronze...")
        df = ler_do_gcs(bucket_name, caminho_bronze, credentials_path)

        if df is None or len(df) == 0:
            print(f"[CONTAS A PAGAR SILVER] Nenhum dado encontrado na camada bronze.")
            return False

        # Normalizar a coluna categorias
        df_silver = normalizar_categorias(df)

        # Salvar na camada silver
        bucket_name_silver = config_yaml['gcs']['silver']['bucket']
        caminho_silver = f"{config_yaml['gcs']['silver']['folder']}/mysql_omie"

        resultado = salvar_no_gcs(
            df_silver,
            bucket_name_silver,
            caminho_silver,
            credentials_path,
            "contas_a_pagar.parquet"
        )

        return resultado is not None

    except Exception as e:
        print(f"[CONTAS A PAGAR SILVER] Erro no processamento: {e}")
        return False


if __name__ == "__main__":
    # Teste individual do módulo
    print("="*80)
    print("PROCESSAMENTO SILVER - CONTAS A PAGAR")
    print("="*80)

    config_yaml = carregar_config()

    try:
        sucesso = processar_contas_a_pagar_silver(config_yaml)
        print(f"\n{'='*80}")
        print(f"Processo {'concluído com sucesso' if sucesso else 'falhou'}!")
        print(f"{'='*80}")

    except Exception as e:
        print(f"Erro durante o processamento: {e}")
