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
            f"[EMPRESAS GOLD] Arquivo salvo no GCS: gs://{bucket_name}/{caminho_completo}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return caminho_completo

    except Exception as e:
        print(f"[EMPRESAS GOLD] Erro ao salvar no GCS: {e}")
        return None


def criar_dim_empresas():
    """
    Cria o DataFrame da dimensão de empresas com os dados hardcoded.

    Returns:
        pd.DataFrame: DataFrame com os dados das empresas
    """
    try:
        print(f"[EMPRESAS GOLD] Criando dimensão de empresas...")

        # Dados das empresas
        dados_empresas = {
            'cod_empresa': [
                'P940001', 'P940002', 'P940003', 'P940004', 'P940005',
                'P940006', 'P940007', 'P940008', 'P940009', 'P940010',
                'P940011', 'P940012', 'P940013', 'P940014', 'P940015',
                'P940016', 'P940017', 'P940018', 'P940019', 'P940020',
                'P940021', 'P940022', 'P940023', 'P940024', 'P940025',
                'P940026', 'P940027', 'P940028', 'P940029', 'P940030',
                'P940031', 'P940032'
            ],
            'de_empresa': [
                'School of Rock Perdizes',
                'Tiro Certo Esportes',
                'Decsigner Comunicação Visual',
                'RPP Advocacia',
                'Agência Xpace',
                'B12 Filmes',
                'Braptec',
                'Mediar Audiologia',
                'Espaço Arrabal Benetti',
                'GCDB Serviços Ltda',
                'Grano Studio',
                'HS Contabil Ltda',
                'HS Soluções Empresariais Ltda - EPP',
                'Instituto Resgatando Vidas - IRV',
                'Klabin Glamour',
                'MABE',
                'Orgânica Digital',
                'Organica Digital - Filial',
                'Tekee Engenharia e Serviços',
                'E-Saúde Marketing Digital',
                'Cria - Brincar e Treinar',
                'Shift Mobilidade Corporativa',
                'Black Beans',
                'Agência Ili',
                'ClickEvolue',
                'School of Rock Panamby',
                'Plásticos Piauí',
                'School of Rock Morumbi',
                'T-Proxy',
                'Ipsis Sistemas',
                'Lustres Gênesis',
                'Toclick'
            ]
        }

        # Criar DataFrame
        df_empresas = pd.DataFrame(dados_empresas)

        print(f"[EMPRESAS GOLD] Dimensão criada com sucesso!")
        print(f"[EMPRESAS GOLD] Total de registros: {len(df_empresas)}")
        print(f"[EMPRESAS GOLD] Colunas: {list(df_empresas.columns)}")

        return df_empresas

    except Exception as e:
        print(f"[EMPRESAS GOLD] Erro ao criar dimensão de empresas: {e}")
        import traceback
        print(f"[EMPRESAS GOLD] Detalhes do erro:\n{traceback.format_exc()}")
        return None


def processar_dim_empresas_gold(config_yaml):
    """
    Processa e salva a dimensão de empresas na camada gold.

    Args:
        config_yaml: Dicionário com as configurações

    Returns:
        bool: True se o processo foi bem-sucedido, False caso contrário
    """
    try:
        # Criar a dimensão de empresas
        df_empresas = criar_dim_empresas()

        if df_empresas is None or len(df_empresas) == 0:
            print(f"[EMPRESAS GOLD] Erro ao criar dimensão de empresas.")
            return False

        # Salvar na camada gold
        bucket_name_gold = config_yaml['gcs']['gold']['bucket']
        caminho_gold = f"{config_yaml['gcs']['gold']['folder']}/mysql_omie"
        credentials_path = config_yaml['credentials-path']

        resultado = salvar_no_gcs(
            df_empresas,
            bucket_name_gold,
            caminho_gold,
            credentials_path,
            "dim_empresas.parquet"
        )

        return resultado is not None

    except Exception as e:
        print(f"[EMPRESAS GOLD] Erro no processamento: {e}")
        import traceback
        print(f"[EMPRESAS GOLD] Detalhes do erro:\n{traceback.format_exc()}")
        return False


if __name__ == "__main__":
    # Teste individual do módulo
    print("="*80)
    print("PROCESSAMENTO GOLD - DIMENSÃO EMPRESAS")
    print("="*80)

    config_yaml = carregar_config()

    try:
        sucesso = processar_dim_empresas_gold(config_yaml)
        print(f"\n{'='*80}")
        print(f"Processo {'concluído com sucesso' if sucesso else 'falhou'}!")
        print(f"{'='*80}")

    except Exception as e:
        print(f"Erro durante o processamento: {e}")
