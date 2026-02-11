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

        print(
            f"[CLIENTES] Arquivo salvo no GCS: gs://{bucket_name}/{caminho_completo}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return caminho_completo

    except Exception as e:
        print(f"[CLIENTES] Erro ao salvar no GCS: {e}")
        return None


def buscar_cliente_por_nome(connection, nome_busca):
    """
    Busca clientes por nome parcial (case-insensitive) na tabela omie_clientes.

    Args:
        connection: Conexão ativa com o banco de dados MySQL
        nome_busca: Nome ou parte do nome a buscar

    Returns:
        pd.DataFrame: DataFrame com os clientes encontrados ou None em caso de erro
    """
    try:
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Buscar em nome_fantasia e razao_social
            query = """
                SELECT * FROM omie_clientes 
                WHERE LOWER(nome_fantasia) LIKE LOWER(%s) 
                   OR LOWER(razao_social) LIKE LOWER(%s)
                ORDER BY nome_fantasia, razao_social
            """
            
            termo_busca = f"%{nome_busca}%"
            cursor.execute(query, (termo_busca, termo_busca))
            
            colunas = [desc[0] for desc in cursor.description]
            registros = cursor.fetchall()
            
            df_resultado = pd.DataFrame(registros, columns=colunas)
            
            print(f"[CLIENTES] Busca por '{nome_busca}': {len(df_resultado)} registro(s) encontrado(s)")
            
            if len(df_resultado) > 0:
                # Mostrar primeiras linhas relevantes
                colunas_nome = [col for col in df_resultado.columns if 'nome' in col.lower() or 'razao' in col.lower() or 'fantasia' in col.lower()]
                if colunas_nome:
                    print(f"\n[CLIENTES] Primeiros resultados:")
                    for idx, row in df_resultado.head(10).iterrows():
                        nome_exibido = str(row[colunas_nome[0]]) if len(colunas_nome) > 0 and pd.notna(row[colunas_nome[0]]) else 'N/A'
                        print(f"  - {nome_exibido}")
            
            cursor.close()
            return df_resultado

    except Error as e:
        print(f"[CLIENTES] Erro ao buscar cliente: {e}")
        import traceback
        print(traceback.format_exc())
        return None


def listar_colunas_tabela(connection):
    """
    Lista todas as colunas da tabela omie_clientes.

    Args:
        connection: Conexão ativa com o banco de dados MySQL

    Returns:
        list: Lista com os nomes das colunas
    """
    try:
        if connection.is_connected():
            cursor = connection.cursor()
            query = "DESCRIBE omie_clientes"
            cursor.execute(query)
            
            colunas_info = cursor.fetchall()
            colunas = [col[0] for col in colunas_info]
            
            print(f"[CLIENTES] Colunas da tabela omie_clientes: {colunas}")
            
            cursor.close()
            return colunas

    except Error as e:
        print(f"[CLIENTES] Erro ao listar colunas: {e}")
        return None


def coletar_clientes(connection):
    """
    Coleta TODOS os dados da tabela omie_clientes do banco de dados MySQL.
    Não aplica nenhum filtro para garantir que todos os clientes sejam coletados.

    Args:
        connection: Conexão ativa com o banco de dados MySQL

    Returns:
        pd.DataFrame: DataFrame com TODOS os dados dos clientes ou None em caso de erro
    """
    try:
        if connection.is_connected():
            # Criar cursor e executar consulta SEM FILTROS
            cursor = connection.cursor()
            # Garantir que busca TODOS os registros sem nenhuma condição WHERE
            query = "SELECT * FROM omie_clientes"
            cursor.execute(query)

            # Obter os nomes das colunas
            colunas = [desc[0] for desc in cursor.description]

            # Obter TODOS os registros
            registros = cursor.fetchall()

            # Criar DataFrame com TODOS os registros
            df_clientes = pd.DataFrame(registros, columns=colunas)

            print(f"[CLIENTES] Total de registros coletados: {len(df_clientes)}")
            print(f"[CLIENTES] Total de colunas: {len(colunas)}")
            
            # Verificar se há registros duplicados por codigo_cliente_omie
            if 'codigo_cliente_omie' in df_clientes.columns:
                duplicados = df_clientes['codigo_cliente_omie'].duplicated().sum()
                if duplicados > 0:
                    print(f"[CLIENTES] AVISO: {duplicados} códigos de cliente duplicados encontrados")
            
            # Mostrar informações sobre colunas de nome
            colunas_nome = [col for col in colunas if 'nome' in col.lower() or 'razao' in col.lower() or 'fantasia' in col.lower()]
            if colunas_nome:
                print(f"[CLIENTES] Colunas de nome disponíveis: {colunas_nome}")
                # Verificar se há valores nulos (apenas informativo, não exclui registros)
                for col in colunas_nome:
                    nulos = df_clientes[col].isna().sum()
                    if nulos > 0:
                        print(f"[CLIENTES] INFO: Coluna '{col}' tem {nulos} valores nulos (registros mantidos)")

            cursor.close()
            return df_clientes

    except Error as e:
        print(f"[CLIENTES] Erro ao coletar dados: {e}")
        import traceback
        print(traceback.format_exc())
        return None


def processar_clientes(connection, config):
    """
    Processa a coleta e salvamento dos dados de clientes.

    Args:
        connection: Conexão ativa com o banco de dados
        config: Dicionário com as configurações

    Returns:
        bool: True se o processo foi bem-sucedido, False caso contrário
    """
    try:
        # Coletar dados
        df = coletar_clientes(connection)

        if df is not None and len(df) > 0:
            # Salvar no GCS
            bucket_name = config['gcs']['bronze']['bucket']
            caminho_gcs = f"{config['gcs']['bronze']['folder']}/mysql_omie"
            credentials_path = config['credentials-path']

            resultado = salvar_no_gcs(
                df, bucket_name, caminho_gcs, credentials_path, "clientes.parquet")

            return resultado is not None
        else:
            print("[CLIENTES] Nenhum dado coletado.")
            return False

    except Exception as e:
        print(f"[CLIENTES] Erro no processamento: {e}")
        return False


if __name__ == "__main__":
    # Teste individual do módulo
    config = carregar_config()

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
            
            # Listar colunas da tabela
            print("\n" + "="*80)
            print("INFORMAÇÕES DA TABELA")
            print("="*80)
            listar_colunas_tabela(connection)
            
            # Buscar especificamente pelo cliente Bionexo
            print("\n" + "="*80)
            print("BUSCA ESPECIFICA - BIONEXO DO BRASIL SOLUCOES DIGITAIS EIRELI")
            print("="*80)
            
            # Buscar por Bionexo
            print("\n--- Buscando por: 'Bionexo' ---")
            resultado_bionexo = buscar_cliente_por_nome(connection, "Bionexo")
            
            if resultado_bionexo is not None and len(resultado_bionexo) > 0:
                print(f"\n[OK] Encontrado {len(resultado_bionexo)} registro(s) com 'Bionexo'")
                print("\n" + "-"*80)
                
                # Mostrar todas as informações relevantes
                colunas_nome = [col for col in resultado_bionexo.columns if 'nome' in col.lower() or 'razao' in col.lower() or 'fantasia' in col.lower()]
                colunas_importantes = ['codigo_cliente_omie', 'cnpj_cpf', 'email', 'telefone1_numero', 'telefone1_ddd', 'cidade', 'estado']
                
                for idx, row in resultado_bionexo.iterrows():
                    print(f"\n>>> REGISTRO {idx + 1} <<<")
                    print("-"*80)
                    # Mostrar colunas de nome
                    for col in colunas_nome:
                        if pd.notna(row[col]) and str(row[col]).strip():
                            print(f"  {col}: {row[col]}")
                    # Mostrar outras informações importantes
                    for col in colunas_importantes:
                        if col in resultado_bionexo.columns and pd.notna(row[col]) and str(row[col]).strip():
                            print(f"  {col}: {row[col]}")
                    print("-"*80)
            else:
                print("\n[AVISO] Nenhum registro encontrado com 'Bionexo'")
            
            # Buscar também por termos relacionados
            print("\n--- Buscando por termos relacionados ---")
            termos_relacionados = ["Solucoes Digitais", "Digitais Eireli"]
            for termo in termos_relacionados:
                resultado = buscar_cliente_por_nome(connection, termo)
                if resultado is not None and len(resultado) > 0:
                    print(f"\n[OK] Encontrado {len(resultado)} registro(s) com '{termo}'")
                    colunas_nome = [col for col in resultado.columns if 'nome' in col.lower() or 'razao' in col.lower() or 'fantasia' in col.lower()]
                    for idx, row in resultado.head(5).iterrows():
                        for col in colunas_nome:
                            if pd.notna(row[col]) and str(row[col]).strip():
                                print(f"  - {row[col]}")
                                break
            
            # Processar normalmente
            print("\n" + "="*80)
            print("PROCESSAMENTO NORMAL")
            print("="*80)
            sucesso = processar_clientes(connection, config)
            print(
                f"\nProcesso {'concluído com sucesso' if sucesso else 'falhou'}!")

    except Error as e:
        print(f"Erro ao conectar: {e}")
        import traceback
        print(traceback.format_exc())
    finally:
        if connection is not None and connection.is_connected():
            connection.close()
            print("Conexão encerrada.")
