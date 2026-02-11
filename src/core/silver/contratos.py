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
        temp_file = os.path.join(temp_dir, 'temp_contratos.parquet')

        blob.download_to_filename(temp_file)

        # Ler o arquivo parquet
        df = pd.read_parquet(temp_file, engine='pyarrow')

        print(
            f"[CONTRATOS SILVER] Arquivo lido do GCS: gs://{bucket_name}/{caminho_gcs}")
        print(f"[CONTRATOS SILVER] Total de registros lidos: {len(df)}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return df

    except Exception as e:
        print(f"[CONTRATOS SILVER] Erro ao ler do GCS: {e}")
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
            f"[CONTRATOS SILVER] Arquivo salvo no GCS: gs://{bucket_name}/{caminho_completo}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return caminho_completo

    except Exception as e:
        print(f"[CONTRATOS SILVER] Erro ao salvar no GCS: {e}")
        return None


def normalizar_itens_contrato(df):
    """
    Normaliza a coluna itenscontrato do DataFrame duplicando linhas.

    A coluna itenscontrato contém uma lista/array de itens. 
    Para cada registro com múltiplos itens, cria uma linha para cada item.

    Args:
        df: DataFrame com a coluna itenscontrato

    Returns:
        pd.DataFrame: DataFrame com linhas duplicadas para cada item
    """
    try:
        print(
            f"[CONTRATOS SILVER] Iniciando normalização da coluna itenscontrato...")

        # Verificar se a coluna existe
        if 'itenscontrato' not in df.columns:
            print(
                f"[CONTRATOS SILVER] AVISO: Coluna 'itenscontrato' não encontrada no DataFrame")
            return df

        # Criar cópia do DataFrame
        df_normalizado = df.copy()

        # Verificar se há dados na coluna itenscontrato
        dados_validos = df_normalizado['itenscontrato'].notna().sum()
        print(
            f"[CONTRATOS SILVER] Registros com itens válidos: {dados_validos}/{len(df_normalizado)}")

        if dados_validos == 0:
            print(
                f"[CONTRATOS SILVER] Nenhum dado válido na coluna itenscontrato para normalizar")
            return df_normalizado

        # Parsear JSON da coluna itenscontrato (se for string)
        def parse_itens_contrato(valor):
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

        df_normalizado['itenscontrato_parsed'] = df_normalizado['itenscontrato'].apply(
            parse_itens_contrato)

        # Contar total de itens antes da explosão
        total_itens = df_normalizado['itenscontrato_parsed'].apply(len).sum()
        print(
            f"[CONTRATOS SILVER] Total de itens a expandir: {total_itens}")

        # Explodir o DataFrame - criar uma linha para cada item
        df_exploded = df_normalizado.explode(
            'itenscontrato_parsed', ignore_index=True)

        # Normalizar os dados do item (se for dict)
        itens_df = pd.json_normalize(
            df_exploded['itenscontrato_parsed'].dropna())

        # Se houver dados normalizados, adicionar ao DataFrame
        if not itens_df.empty:
            # Adicionar prefixo 'item_' nas colunas
            itens_df.columns = ['item_' +
                                str(col) for col in itens_df.columns]

            # Resetar índice do itens_df para combinar corretamente
            itens_df.index = df_exploded[df_exploded['itenscontrato_parsed'].notna(
            )].index

            # Combinar com o DataFrame explodido
            df_exploded = df_exploded.join(itens_df)

        # Remover colunas temporárias
        df_exploded = df_exploded.drop(
            columns=['itenscontrato', 'itenscontrato_parsed'])

        print(f"[CONTRATOS SILVER] Normalização concluída!")
        print(
            f"[CONTRATOS SILVER] Registros antes: {len(df_normalizado)}")
        print(
            f"[CONTRATOS SILVER] Registros depois: {len(df_exploded)}")
        if not itens_df.empty:
            print(
                f"[CONTRATOS SILVER] Colunas de item criadas: {list(itens_df.columns)}")

        return df_exploded

    except Exception as e:
        print(f"[CONTRATOS SILVER] Erro ao normalizar itenscontrato: {e}")
        import traceback
        print(
            f"[CONTRATOS SILVER] Detalhes do erro:\n{traceback.format_exc()}")
        return df


def processar_contratos_silver(config_yaml):
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
        caminho_bronze = f"{config_yaml['gcs']['bronze']['folder']}/mysql_omie/contratos.parquet"
        credentials_path = config_yaml['credentials-path']

        # Ler dados da camada bronze
        print(f"[CONTRATOS SILVER] Lendo dados da camada bronze...")
        df = ler_do_gcs(bucket_name, caminho_bronze, credentials_path)

        if df is None or len(df) == 0:
            print(
                f"[CONTRATOS SILVER] Nenhum dado encontrado na camada bronze.")
            return False

        # Normalizar a coluna itenscontrato
        df_silver = normalizar_itens_contrato(df)

        # Salvar na camada silver
        bucket_name_silver = config_yaml['gcs']['silver']['bucket']
        caminho_silver = f"{config_yaml['gcs']['silver']['folder']}/mysql_omie"

        resultado = salvar_no_gcs(
            df_silver,
            bucket_name_silver,
            caminho_silver,
            credentials_path,
            "contratos.parquet"
        )

        return resultado is not None

    except Exception as e:
        print(f"[CONTRATOS SILVER] Erro no processamento: {e}")
        return False


if __name__ == "__main__":
    # Teste individual do módulo
    print("="*80)
    print("PROCESSAMENTO SILVER - CONTRATOS")
    print("="*80)

    config_yaml = carregar_config()

    try:
        sucesso = processar_contratos_silver(config_yaml)
        print(f"\n{'='*80}")
        print(f"Processo {'concluído com sucesso' if sucesso else 'falhou'}!")
        print(f"{'='*80}")

    except Exception as e:
        print(f"Erro durante o processamento: {e}")
