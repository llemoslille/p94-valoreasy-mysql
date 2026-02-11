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
        temp_file = os.path.join(temp_dir, 'temp_categorias.parquet')

        blob.download_to_filename(temp_file)

        # Ler o arquivo parquet
        df = pd.read_parquet(temp_file, engine='pyarrow')

        print(
            f"[CATEGORIAS GOLD] Arquivo lido do GCS: gs://{bucket_name}/{caminho_gcs}")
        print(f"[CATEGORIAS GOLD] Total de registros lidos: {len(df)}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return df

    except Exception as e:
        print(f"[CATEGORIAS GOLD] Erro ao ler do GCS: {e}")
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
            f"[CATEGORIAS GOLD] Arquivo salvo no GCS: gs://{bucket_name}/{caminho_completo}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return caminho_completo

    except Exception as e:
        print(f"[CATEGORIAS GOLD] Erro ao salvar no GCS: {e}")
        return None


def transformar_categorias(df):
    """
    Transforma o DataFrame de categorias selecionando, renomeando e aplicando transformações.

    Args:
        df: DataFrame com os dados brutos de categorias

    Returns:
        pd.DataFrame: DataFrame transformado para a dimensão de categorias
    """
    try:
        print(f"[CATEGORIAS GOLD] Iniciando transformação dos dados...")

        # Verificar se as colunas necessárias existem
        colunas_necessarias = [
            'empresa_id',
            'categoria_superior',
            'codigo',
            'codigo_dre',
            'descricao',
            'descricao_padrao',
            'dadosdre_codigodre',
            'dadosdre_descricaodre'
        ]
        colunas_faltantes = [
            col for col in colunas_necessarias if col not in df.columns]

        if colunas_faltantes:
            print(
                f"[CATEGORIAS GOLD] AVISO: Colunas faltantes no DataFrame: {colunas_faltantes}")
            print(f"[CATEGORIAS GOLD] Colunas disponíveis: {list(df.columns)}")

        # Selecionar as colunas
        df_transformado = df[[
            'empresa_id',
            'categoria_superior',
            'codigo',
            'codigo_dre',
            'descricao',
            'descricao_padrao',
            'dadosdre_codigodre',
            'dadosdre_descricaodre',
            'conta_inativa'
        ]].copy()

        # Renomear a coluna empresa_id
        df_transformado = df_transformado.rename(columns={
            'empresa_id': 'cod_empresa'
        })

        # Aplicar UPPER nas colunas de texto
        df_transformado['descricao'] = df_transformado['descricao'].str.upper()
        df_transformado['descricao_padrao'] = df_transformado['descricao_padrao'].str.upper()
        df_transformado['dadosdre_descricaodre'] = df_transformado['dadosdre_descricaodre'].str.upper()

        # Adicionar sufixo "(INATIVA)" quando conta_inativa == "S"
        mask_inativa = df_transformado['conta_inativa'] == 'S'
        if mask_inativa.any():
            df_transformado.loc[mask_inativa, 'descricao'] = (
                df_transformado.loc[mask_inativa, 'descricao'] + ' (INATIVA)'
            )
            df_transformado.loc[mask_inativa, 'descricao_padrao'] = (
                df_transformado.loc[mask_inativa,
                                    'descricao_padrao'] + ' (INATIVA)'
            )
            print(
                f"[CATEGORIAS GOLD] {mask_inativa.sum()} registros marcados como inativos")

        # Remover a coluna conta_inativa após o processamento (não é necessária no resultado final)
        if 'conta_inativa' in df_transformado.columns:
            df_transformado = df_transformado.drop(columns=['conta_inativa'])

        print(f"[CATEGORIAS GOLD] Transformação concluída!")
        print(
            f"[CATEGORIAS GOLD] Registros transformados: {len(df_transformado)}")
        print(
            f"[CATEGORIAS GOLD] Colunas finais: {list(df_transformado.columns)}")

        return df_transformado

    except Exception as e:
        print(f"[CATEGORIAS GOLD] Erro ao transformar dados: {e}")
        import traceback
        print(f"[CATEGORIAS GOLD] Detalhes do erro:\n{traceback.format_exc()}")
        return None


def processar_categorias_gold(config_yaml):
    """
    Processa os dados da camada bronze para a camada gold.

    Args:
        config_yaml: Dicionário com as configurações

    Returns:
        bool: True se o processo foi bem-sucedido, False caso contrário
    """
    try:
        # Configurações do GCS
        bucket_name = config_yaml['gcs']['bronze']['bucket']
        caminho_bronze = f"{config_yaml['gcs']['bronze']['folder']}/mysql_omie/categorias.parquet"
        credentials_path = config_yaml['credentials-path']

        # Ler dados da camada bronze
        print(f"[CATEGORIAS GOLD] Lendo dados da camada bronze...")
        df = ler_do_gcs(bucket_name, caminho_bronze, credentials_path)

        if df is None or len(df) == 0:
            print(f"[CATEGORIAS GOLD] Nenhum dado encontrado na camada bronze.")
            return False

        # Transformar os dados
        df_gold = transformar_categorias(df)

        if df_gold is None or len(df_gold) == 0:
            print(f"[CATEGORIAS GOLD] Erro na transformação dos dados.")
            return False

        # Salvar na camada gold
        bucket_name_gold = config_yaml['gcs']['gold']['bucket']
        caminho_gold = f"{config_yaml['gcs']['gold']['folder']}/mysql_omie"

        resultado = salvar_no_gcs(
            df_gold,
            bucket_name_gold,
            caminho_gold,
            credentials_path,
            "dim_categorias.parquet"
        )

        return resultado is not None

    except Exception as e:
        print(f"[CATEGORIAS GOLD] Erro no processamento: {e}")
        return False


if __name__ == "__main__":
    # Teste individual do módulo
    print("="*80)
    print("PROCESSAMENTO GOLD - DIMENSÃO CATEGORIAS")
    print("="*80)

    config_yaml = carregar_config()

    try:
        sucesso = processar_categorias_gold(config_yaml)
        print(f"\n{'='*80}")
        print(f"Processo {'concluído com sucesso' if sucesso else 'falhou'}!")
        print(f"{'='*80}")

    except Exception as e:
        print(f"Erro durante o processamento: {e}")
