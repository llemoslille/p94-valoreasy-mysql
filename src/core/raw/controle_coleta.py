import mysql.connector
from mysql.connector import Error
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

        print(f"[CONTROLE COLETA] Arquivo salvo no GCS: gs://{bucket_name}/{caminho_completo}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return caminho_completo

    except Exception as e:
        print(f"[CONTROLE COLETA] Erro ao salvar no GCS: {e}")
        return None


def coletar_controle_coleta(connection):
    """
    Coleta os dados da tabela omie_controle_coleta do banco de dados MySQL.

    Args:
        connection: Conexão ativa com o banco de dados MySQL

    Returns:
        pd.DataFrame: DataFrame com os dados controle de coleta ou None em caso de erro
    """
    try:
        if connection.is_connected():
            # Criar cursor e executar consulta
            cursor = connection.cursor()
            query = "SELECT * FROM omie_controle_coleta"
            cursor.execute(query)

            # Obter os nomes das colunas
            colunas = [desc[0] for desc in cursor.description]

            # Obter todos os registros
            registros = cursor.fetchall()

            # Criar DataFrame
            df = pd.DataFrame(registros, columns=colunas)

            print(f"[CONTROLE COLETA] Total de registros coletados: {len(df)}")

            cursor.close()
            return df

    except Error as e:
        print(f"[CONTROLE COLETA] Erro ao coletar dados: {e}")
        return None


def processar_controle_coleta(connection, config_yaml):
    """
    Processa a coleta e salvamento dos dados.
    
    Args:
        connection: Conexão ativa com o banco de dados
        config_yaml: Dicionário com as configurações
    
    Returns:
        bool: True se o processo foi bem-sucedido, False caso contrário
    """
    try:
        # Coletar dados
        df = coletar_controle_coleta(connection)

        if df is not None and len(df) > 0:
            # Salvar no GCS
            bucket_name = config_yaml['gcs']['bronze']['bucket']
            caminho_gcs = f"{config_yaml['gcs']['bronze']['folder']}/mysql_omie"
            credentials_path = config_yaml['credentials-path']

            resultado = salvar_no_gcs(
                df, bucket_name, caminho_gcs, credentials_path, "controle_coleta.parquet")

            return resultado is not None
        else:
            print(f"[CONTROLE COLETA] Nenhum dado coletado.")
            return False

    except Exception as e:
        print(f"[CONTROLE COLETA] Erro no processamento: {e}")
        return False


if __name__ == "__main__":
    # Teste individual do módulo
    config_yaml = carregar_config()
    
    connection = None
    try:
        connection = mysql.connector.connect(
            host='45.179.90.60',
            port=7513,
            user='lille',
            password='lille@datime2026',
            database='lille'
        )
        
        if connection.is_connected():
            print("Conectado ao servidor MySQL")
            sucesso = processar_controle_coleta(connection, config_yaml)
            print(f"\nProcesso {'concluído com sucesso' if sucesso else 'falhou'}!")
            
    except Error as e:
        print(f"Erro ao conectar: {e}")
    finally:
        if connection is not None and connection.is_connected():
            connection.close()
            print("Conexão encerrada.")
