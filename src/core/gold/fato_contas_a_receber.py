import pandas as pd
import yaml
from google.cloud import storage
from datetime import datetime
import os


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
        temp_file = os.path.join(temp_dir, 'temp_contas_a_receber.parquet')

        blob.download_to_filename(temp_file)

        # Ler o arquivo parquet
        df = pd.read_parquet(temp_file, engine='pyarrow')

        print(
            f"[CONTAS A RECEBER GOLD] Arquivo lido do GCS: gs://{bucket_name}/{caminho_gcs}")
        print(f"[CONTAS A RECEBER GOLD] Total de registros lidos: {len(df)}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return df

    except Exception as e:
        print(f"[CONTAS A RECEBER GOLD] Erro ao ler do GCS: {e}")
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
            f"[CONTAS A RECEBER GOLD] Arquivo salvo no GCS: gs://{bucket_name}/{caminho_completo}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return caminho_completo

    except Exception as e:
        print(f"[CONTAS A RECEBER GOLD] Erro ao salvar no GCS: {e}")
        return None


def processar_fato_contas_a_receber_gold(config_yaml):
    """
    Processa os dados da camada silver para a camada gold.

    Args:
        config_yaml: Dicionário com as configurações

    Returns:
        bool: True se o processo foi bem-sucedido, False caso contrário
    """
    try:
        # Configurações do GCS - lendo da camada silver
        bucket_name = config_yaml['gcs']['silver']['bucket']
        caminho_silver = f"{config_yaml['gcs']['silver']['folder']}/mysql_omie/contas_a_receber.parquet"
        credentials_path = config_yaml['credentials-path']

        # Ler dados da camada silver
        print(f"[CONTAS A RECEBER GOLD] Lendo dados da camada silver...")
        df = ler_do_gcs(bucket_name, caminho_silver, credentials_path)

        if df is None or len(df) == 0:
            print(f"[CONTAS A RECEBER GOLD] Nenhum dado encontrado na camada silver.")
            return False

        print(
            f"[CONTAS A RECEBER GOLD] Total de registros para processar: {len(df)}")
        print(f"[CONTAS A RECEBER GOLD] Colunas: {list(df.columns)}")

        # Função auxiliar para converter numéricos para string preservando NULL
        def converter_para_string_seguro(serie):
            """Converte série numérica para string, preservando NULL como NULL (não string vazia)"""
            # Converter para string, mas manter NULL como NULL (pd.NA)
            serie_str = serie.astype(str)
            # Substituir valores 'nan', 'None', 'NaN' por NULL (pd.NA) para preservar no Parquet
            serie_str = serie_str.replace(['nan', 'None', 'NaN'], pd.NA)
            return serie_str

        # Converter colunas categoria_* numéricas para string (BigQuery espera STRING, não DOUBLE)
        colunas_categoria = [
            col for col in df.columns if col.startswith('categoria_')]
        colunas_categoria_numericas = [
            col for col in colunas_categoria if df[col].dtype in ['float64', 'float32', 'int64', 'int32']]

        if colunas_categoria_numericas:
            print(
                f"[CONTAS A RECEBER GOLD] Convertendo {len(colunas_categoria_numericas)} colunas categoria_* numéricas para string: {colunas_categoria_numericas}")
            for col in colunas_categoria_numericas:
                df[col] = converter_para_string_seguro(df[col])

        # Converter colunas codigo_* numéricas para string (BigQuery espera STRING, não INT64)
        colunas_codigo = [
            col for col in df.columns if 'codigo' in col.lower()]
        colunas_codigo_numericas = [
            col for col in colunas_codigo if df[col].dtype in ['int64', 'int32', 'float64', 'float32']]

        if colunas_codigo_numericas:
            print(
                f"[CONTAS A RECEBER GOLD] Convertendo {len(colunas_codigo_numericas)} colunas codigo_* numéricas para string: {colunas_codigo_numericas}")
            for col in colunas_codigo_numericas:
                df[col] = converter_para_string_seguro(df[col])

        # Converter todas as colunas object para string (podem conter valores string misturados)
        # Preservando NULL como NULL (não string vazia) para permitir PARSE_DATE funcionar
        colunas_object = df.select_dtypes(include=['object']).columns
        if len(colunas_object) > 0:
            print(
                f"[CONTAS A RECEBER GOLD] Convertendo {len(colunas_object)} colunas object para string...")
            for col in colunas_object:
                # Converter para string preservando NULL
                df[col] = df[col].astype(str)
                # Substituir 'nan', 'None', 'NaN' por NULL (pd.NA)
                df[col] = df[col].replace(['nan', 'None', 'NaN'], pd.NA)

        # Salvar na camada gold
        bucket_name_gold = config_yaml['gcs']['gold']['bucket']
        caminho_gold = f"{config_yaml['gcs']['gold']['folder']}/mysql_omie"

        resultado = salvar_no_gcs(
            df,
            bucket_name_gold,
            caminho_gold,
            credentials_path,
            "fato_contas_a_receber.parquet"
        )

        return resultado is not None

    except Exception as e:
        print(f"[CONTAS A RECEBER GOLD] Erro no processamento: {e}")
        import traceback
        print(
            f"[CONTAS A RECEBER GOLD] Detalhes do erro:\n{traceback.format_exc()}")
        return False


if __name__ == "__main__":
    # Teste individual do módulo
    print("="*80)
    print("PROCESSAMENTO GOLD - FATO CONTAS A RECEBER")
    print("="*80)

    config_yaml = carregar_config()

    try:
        sucesso = processar_fato_contas_a_receber_gold(config_yaml)
        print(f"\n{'='*80}")
        print(f"Processo {'concluído com sucesso' if sucesso else 'falhou'}!")
        print(f"{'='*80}")

    except Exception as e:
        print(f"Erro durante o processamento: {e}")
