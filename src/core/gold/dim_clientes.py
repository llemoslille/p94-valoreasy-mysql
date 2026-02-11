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
        temp_file = os.path.join(temp_dir, 'temp_clientes.parquet')

        blob.download_to_filename(temp_file)

        # Ler o arquivo parquet
        df = pd.read_parquet(temp_file, engine='pyarrow')

        print(
            f"[CLIENTES GOLD] Arquivo lido do GCS: gs://{bucket_name}/{caminho_gcs}")
        print(f"[CLIENTES GOLD] Total de registros lidos: {len(df)}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return df

    except Exception as e:
        print(f"[CLIENTES GOLD] Erro ao ler do GCS: {e}")
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
            f"[CLIENTES GOLD] Arquivo salvo no GCS: gs://{bucket_name}/{caminho_completo}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return caminho_completo

    except Exception as e:
        print(f"[CLIENTES GOLD] Erro ao salvar no GCS: {e}")
        return None


def transformar_clientes(df):
    """
    Transforma o DataFrame de clientes selecionando e renomeando colunas.
    GARANTE que TODOS os registros sejam mantidos, mesmo com valores nulos.

    Args:
        df: DataFrame com os dados brutos de clientes

    Returns:
        pd.DataFrame: DataFrame transformado para a dimensão de clientes (TODOS os registros)
    """
    try:
        print(f"[CLIENTES GOLD] Iniciando transformação dos dados...")
        print(f"[CLIENTES GOLD] Total de registros recebidos: {len(df)}")

        # Verificar se as colunas necessárias existem
        colunas_necessarias = [
            'empresa_id', 'codigo_cliente_omie', 'nome_fantasia', 'razao_social', 'cnpj_cpf']
        colunas_faltantes = [
            col for col in colunas_necessarias if col not in df.columns]

        if colunas_faltantes:
            print(
                f"[CLIENTES GOLD] ERRO: Colunas faltantes no DataFrame: {colunas_faltantes}")
            print(f"[CLIENTES GOLD] Colunas disponíveis: {list(df.columns)}")
            raise ValueError(
                f"Colunas necessárias não encontradas: {colunas_faltantes}")

        # Criar cópia do DataFrame para não modificar o original
        df_work = df.copy()

        # Garantir que TODOS os registros sejam mantidos
        # Preencher valores nulos com valores padrão apenas para evitar erros, mas manter registros
        # NÃO usar dropna() que excluiria registros

        # Selecionar as colunas necessárias (mantém TODOS os registros)
        df_transformado = df_work[[
            'empresa_id',
            'codigo_cliente_omie',
            'nome_fantasia',
            'razao_social',
            'cnpj_cpf'
        ]].copy()

        # Verificar quantos registros temos antes da transformação
        registros_antes = len(df_transformado)

        # Renomear as colunas
        df_transformado = df_transformado.rename(columns={
            'empresa_id': 'cod_empresa',
            'codigo_cliente_omie': 'pk_cliente',
            'nome_fantasia': 'nm_cliente',
            'razao_social': 'razao_social',
            'cnpj_cpf': 'cnpj_cpf'
        })

        # Verificar quantos registros temos depois da transformação
        registros_depois = len(df_transformado)

        # Validar que nenhum registro foi perdido
        if registros_antes != registros_depois:
            print(f"[CLIENTES GOLD] ERRO: Perda de registros durante transformação!")
            print(
                f"[CLIENTES GOLD] Registros antes: {registros_antes}, depois: {registros_depois}")
            raise ValueError(
                "Registros foram perdidos durante a transformação!")

        print(f"[CLIENTES GOLD] Transformação concluída!")
        print(
            f"[CLIENTES GOLD] Registros transformados: {len(df_transformado)} (TODOS mantidos)")
        print(
            f"[CLIENTES GOLD] Colunas finais: {list(df_transformado.columns)}")

        # Informar sobre valores nulos (apenas informativo)
        for col in df_transformado.columns:
            nulos = df_transformado[col].isna().sum()
            if nulos > 0:
                print(
                    f"[CLIENTES GOLD] INFO: Coluna '{col}' tem {nulos} valores nulos (registros mantidos)")

        return df_transformado

    except Exception as e:
        print(f"[CLIENTES GOLD] Erro ao transformar dados: {e}")
        import traceback
        print(f"[CLIENTES GOLD] Detalhes do erro:\n{traceback.format_exc()}")
        return None


def processar_clientes_gold(config_yaml):
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
        caminho_bronze = f"{config_yaml['gcs']['bronze']['folder']}/mysql_omie/clientes.parquet"
        credentials_path = config_yaml['credentials-path']

        # Ler dados da camada bronze
        print(f"[CLIENTES GOLD] Lendo dados da camada bronze...")
        df = ler_do_gcs(bucket_name, caminho_bronze, credentials_path)

        if df is None or len(df) == 0:
            print(f"[CLIENTES GOLD] ERRO: Nenhum dado encontrado na camada bronze.")
            return False

        print(f"[CLIENTES GOLD] Total de registros lidos da bronze: {len(df)}")

        # Transformar os dados (garante que TODOS sejam mantidos)
        df_gold = transformar_clientes(df)

        if df_gold is None:
            print(f"[CLIENTES GOLD] ERRO: Falha na transformação dos dados.")
            return False

        if len(df_gold) == 0:
            print(f"[CLIENTES GOLD] ERRO: Nenhum registro após transformação.")
            return False

        # Validar que todos os registros foram mantidos
        if len(df_gold) != len(df):
            print(f"[CLIENTES GOLD] ERRO: Perda de registros!")
            print(
                f"[CLIENTES GOLD] Registros bronze: {len(df)}, gold: {len(df_gold)}")
            return False

        print(
            f"[CLIENTES GOLD] Validado: Todos os {len(df_gold)} registros foram mantidos na transformação.")

        # Salvar na camada gold
        bucket_name_gold = config_yaml['gcs']['gold']['bucket']
        caminho_gold = f"{config_yaml['gcs']['gold']['folder']}/mysql_omie"

        resultado = salvar_no_gcs(
            df_gold,
            bucket_name_gold,
            caminho_gold,
            credentials_path,
            "dim_clientes.parquet"
        )

        return resultado is not None

    except Exception as e:
        print(f"[CLIENTES GOLD] Erro no processamento: {e}")
        return False


if __name__ == "__main__":
    # Teste individual do módulo
    print("="*80)
    print("PROCESSAMENTO GOLD - DIMENSÃO CLIENTES")
    print("="*80)

    config_yaml = carregar_config()

    try:
        sucesso = processar_clientes_gold(config_yaml)
        print(f"\n{'='*80}")
        print(f"Processo {'concluído com sucesso' if sucesso else 'falhou'}!")
        print(f"{'='*80}")

    except Exception as e:
        print(f"Erro durante o processamento: {e}")
