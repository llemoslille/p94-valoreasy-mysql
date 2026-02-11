import mysql.connector
from mysql.connector import Error
import pandas as pd
import yaml
from google.cloud import storage
from datetime import datetime
import os
import time
from google.cloud.exceptions import GoogleCloudError


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


def salvar_no_gcs(df, bucket_name, caminho_gcs, credentials_path, nome_arquivo, max_retries=3, chunk_size=None):
    """
    Salva o DataFrame no Google Cloud Storage em formato parquet com retry e suporte a chunks

    Args:
        df: DataFrame a ser salvo
        bucket_name: Nome do bucket GCS
        caminho_gcs: Caminho dentro do bucket
        credentials_path: Caminho para o arquivo de credenciais
        nome_arquivo: Nome do arquivo a ser salvo
        max_retries: Número máximo de tentativas (padrão: 3)
        chunk_size: Se fornecido, divide o DataFrame em chunks deste tamanho (padrão: None)
    """
    try:
        # Configurar credenciais
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        # Criar cliente do GCS com timeout maior
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Se chunk_size for fornecido, dividir em chunks (sempre particionar)
        if chunk_size:
            print(
                f"[MOVIMENTOS FINANCEIROS] Dividindo DataFrame ({len(df)} registros) em chunks de {chunk_size}...")
            total_chunks = (len(df) // chunk_size) + \
                (1 if len(df) % chunk_size > 0 else 0)

            caminhos_enviados = []
            for i in range(0, len(df), chunk_size):
                chunk_num = (i // chunk_size) + 1
                df_chunk = df.iloc[i:i + chunk_size]
                nome_chunk = nome_arquivo.replace(
                    '.parquet', f'_part_{chunk_num:03d}.parquet')
                caminho_chunk = salvar_no_gcs(
                    df_chunk, bucket_name, caminho_gcs, credentials_path,
                    nome_chunk, max_retries=max_retries, chunk_size=None
                )
                if caminho_chunk:
                    caminhos_enviados.append(caminho_chunk)
                    print(
                        f"[MOVIMENTOS FINANCEIROS] Chunk {chunk_num}/{total_chunks} enviado com sucesso")
                else:
                    print(
                        f"[MOVIMENTOS FINANCEIROS] Erro ao enviar chunk {chunk_num}/{total_chunks}")

            if len(caminhos_enviados) == total_chunks:
                print(
                    f"[MOVIMENTOS FINANCEIROS] Todos os {total_chunks} chunks foram enviados com sucesso")
                return f"{caminho_gcs}/{nome_arquivo}"
            else:
                print(
                    f"[MOVIMENTOS FINANCEIROS] Apenas {len(caminhos_enviados)}/{total_chunks} chunks foram enviados")
                return None

        # Criar caminho completo
        caminho_completo = f"{caminho_gcs}/{nome_arquivo}"

        # Salvar DataFrame temporariamente como parquet
        import tempfile
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, nome_arquivo)

        print(f"[MOVIMENTOS FINANCEIROS] Salvando DataFrame em arquivo temporário...")
        df.to_parquet(temp_file, index=False, engine='pyarrow')

        # Obter tamanho do arquivo
        file_size_mb = os.path.getsize(temp_file) / (1024 * 1024)
        print(
            f"[MOVIMENTOS FINANCEIROS] Arquivo temporário criado: {file_size_mb:.2f} MB")

        # Upload para o GCS com retry
        blob = bucket.blob(caminho_completo)

        for tentativa in range(1, max_retries + 1):
            try:
                print(
                    f"[MOVIMENTOS FINANCEIROS] Tentativa {tentativa}/{max_retries} de upload para o GCS...")

                # Upload para o GCS (timeout de 10 minutos)
                # Algumas versões do cliente GCS não suportam timeout diretamente
                try:
                    blob.upload_from_filename(temp_file, timeout=600)
                except TypeError:
                    # Se timeout não for suportado, tenta sem timeout
                    blob.upload_from_filename(temp_file)

                print(
                    f"[MOVIMENTOS FINANCEIROS] Arquivo salvo no GCS: gs://{bucket_name}/{caminho_completo}")
                break

            except (GoogleCloudError, TimeoutError, ConnectionError, OSError, Exception) as e:
                error_msg = str(e)
                error_type = type(e).__name__

                if tentativa < max_retries:
                    wait_time = 2 ** tentativa  # Backoff exponencial
                    print(
                        f"[MOVIMENTOS FINANCEIROS] Erro na tentativa {tentativa}/{max_retries} ({error_type}): {error_msg}")
                    print(
                        f"[MOVIMENTOS FINANCEIROS] Aguardando {wait_time} segundos antes de tentar novamente...")
                    time.sleep(wait_time)
                else:
                    print(
                        f"[MOVIMENTOS FINANCEIROS] Erro ao salvar no GCS após {max_retries} tentativas ({error_type}): {error_msg}")
                    # Remover arquivo temporário antes de retornar
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                    return None

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return caminho_completo

    except Exception as e:
        print(f"[MOVIMENTOS FINANCEIROS] Erro ao salvar no GCS: {e}")
        return None


def coletar_movimentos_financeiros(connection):
    """
    Coleta os dados da tabela omie_movimentos_financeiros do banco de dados MySQL.

    Args:
        connection: Conexão ativa com o banco de dados MySQL

    Returns:
        pd.DataFrame: DataFrame com os dados movimentos financeiros ou None em caso de erro
    """
    try:
        if connection.is_connected():
            # Criar cursor e executar consulta
            cursor = connection.cursor()
            query = "SELECT * FROM omie_movimentos_financeiros"
            cursor.execute(query)

            # Obter os nomes das colunas
            colunas = [desc[0] for desc in cursor.description]

            # Obter todos os registros
            registros = cursor.fetchall()

            # Criar DataFrame
            df = pd.DataFrame(registros, columns=colunas)

            print(
                f"[MOVIMENTOS FINANCEIROS] Total de registros coletados: {len(df)}")

            cursor.close()
            return df

    except Error as e:
        print(f"[MOVIMENTOS FINANCEIROS] Erro ao coletar dados: {e}")
        return None


def processar_movimentos_financeiros(connection, config_yaml):
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
        df = coletar_movimentos_financeiros(connection)

        if df is not None and len(df) > 0:
            # Salvar no GCS
            bucket_name = config_yaml['gcs']['bronze']['bucket']
            caminho_gcs = f"{config_yaml['gcs']['bronze']['folder']}/mysql_omie"
            credentials_path = config_yaml['credentials-path']

            # Sempre particionar em chunks de 500k registros (padrão para arquivos grandes)
            chunk_size = 500000

            resultado = salvar_no_gcs(
                df, bucket_name, caminho_gcs, credentials_path,
                "movimentos_financeiros.parquet",
                max_retries=3,
                chunk_size=chunk_size
            )

            return resultado is not None
        else:
            print(f"[MOVIMENTOS FINANCEIROS] Nenhum dado coletado.")
            return False

    except Exception as e:
        print(f"[MOVIMENTOS FINANCEIROS] Erro no processamento: {e}")
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
            sucesso = processar_movimentos_financeiros(connection, config_yaml)
            print(
                f"\nProcesso {'concluído com sucesso' if sucesso else 'falhou'}!")

    except Error as e:
        print(f"Erro ao conectar: {e}")
    finally:
        if connection is not None and connection.is_connected():
            connection.close()
            print("Conexão encerrada.")
