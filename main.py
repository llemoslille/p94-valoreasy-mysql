import mysql.connector
from mysql.connector import Error
import yaml
import os
import sys
from datetime import datetime
import traceback as tb

# Importar módulo de notificação por e-mail
from src.utils.email_notifier import enviar_email, criar_corpo_email_sucesso, criar_corpo_email_erro, enviar_email_erro_processamento

# Importar módulos da camada raw
from src.core.raw.clientes_mysql import processar_clientes
from src.core.raw.cadastro_dre import processar_cadastro_dre
from src.core.raw.categorias import processar_categorias
from src.core.raw.contas_a_pagar import processar_contas_a_pagar
from src.core.raw.contas_a_receber import processar_contas_a_receber
from src.core.raw.contas_correntes import processar_contas_correntes
from src.core.raw.contratos import processar_contratos
from src.core.raw.controle_coleta import processar_controle_coleta
from src.core.raw.extrato_financeiro import processar_extrato_financeiro
from src.core.raw.movimentos_financeiros import processar_movimentos_financeiros
from src.core.raw.ordens_servico import processar_ordens_servico
from src.core.raw.pedidos import processar_pedidos
from src.core.raw.produtos import processar_produtos
from src.core.raw.projetos import processar_projetos
from src.core.raw.servicos import processar_servicos
from src.core.raw.tipo_fat_contrato import processar_tipo_fat_contrato
from src.core.raw.vendedores import processar_vendedores

# Importar módulos da camada silver
from src.core.silver.contas_a_pagar import processar_contas_a_pagar_silver
from src.core.silver.contas_a_receber import processar_contas_a_receber_silver

# Importar módulos da camada gold
from src.core.gold.dim_categorias import processar_categorias_gold
from src.core.gold.dim_clientes import processar_clientes_gold
from src.core.gold.dim_conta_corrente import processar_conta_corrente_gold
from src.core.gold.dim_empresas import processar_dim_empresas_gold
from src.core.gold.fato_contas_a_pagar import processar_fato_contas_a_pagar_gold
from src.core.gold.fato_contas_a_receber import processar_fato_contas_a_receber_gold
from src.core.gold.fato_extrato import processar_fato_extrato_gold
from src.core.gold.fato_movimentos_financeiro import processar_fato_movimentos_financeiro_gold


def carregar_config():
    """
    Carrega as configurações do arquivo config.yaml

    Returns:
        dict: Dicionário com as configurações
    """
    config_path = os.path.join(os.path.dirname(
        __file__), 'config', 'config.yaml')
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def conectar_mysql():
    """
    Estabelece conexão com o banco de dados MySQL.

    Returns:
        connection: Objeto de conexão MySQL ou None em caso de erro
    """
    try:
        connection = mysql.connector.connect(
            host='45.179.90.60',
            port=7513,
            user='lille',
            password='lille@datime2026',
            database='lille'
        )

        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"[OK] Conectado ao servidor MySQL versão: {db_info}")
            cursor = connection.cursor()
            cursor.execute("SELECT database();")
            record = cursor.fetchone()
            print(f"[OK] Banco de dados ativo: {record[0]}\n")
            cursor.close()
            return connection

    except Error as e:
        print(f"[ERRO] Erro ao conectar ao MySQL: {e}")
        return None


def processar_camada_raw(connection, config):
    """
    Processa todas as tabelas da camada raw.

    Args:
        connection: Conexão ativa com o banco de dados
        config: Dicionário com as configurações

    Returns:
        dict: Dicionário com o resultado de cada processamento
    """
    inicio_raw = datetime.now()
    print("="*80)
    print("INICIANDO PROCESSAMENTO DA CAMADA RAW")
    print(f"Início: {inicio_raw.strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*80)

    # Lista de processadores
    processadores = [
        ("Clientes", processar_clientes),
        ("Cadastro DRE", processar_cadastro_dre),
        ("Categorias", processar_categorias),
        ("Contas a Pagar", processar_contas_a_pagar),
        ("Contas a Receber", processar_contas_a_receber),
        ("Contas Correntes", processar_contas_correntes),
        ("Contratos", processar_contratos),
        ("Controle de Coleta", processar_controle_coleta),
        ("Extrato Financeiro", processar_extrato_financeiro),
        ("Movimentos Financeiros", processar_movimentos_financeiros),
        ("Ordens de Serviço", processar_ordens_servico),
        ("Pedidos", processar_pedidos),
        ("Produtos", processar_produtos),
        ("Projetos", processar_projetos),
        ("Serviços", processar_servicos),
        ("Tipo Fat Contrato", processar_tipo_fat_contrato),
        ("Vendedores", processar_vendedores),
    ]

    resultados = {}
    total = len(processadores)
    sucesso_count = 0
    erro_count = 0

    for idx, (nome, processador) in enumerate(processadores, 1):
        inicio_processamento = datetime.now()
        print(f"\n[{idx}/{total}] Processando {nome}...")
        print(
            f"Início: {inicio_processamento.strftime('%d/%m/%Y às %H:%M:%S')}")
        print("-" * 80)

        try:
            sucesso = processador(connection, config)
            resultados[nome] = sucesso

            if sucesso:
                sucesso_count += 1
                print(f"[OK] {nome} processado com sucesso!")
            else:
                erro_count += 1
                print(f"[ERRO] {nome} falhou no processamento.")
                # Enviar e-mail de erro para processamento específico
                try:
                    erro_msg = f"O processamento '{nome}' retornou False (falhou sem exceção)"
                    enviar_email_erro_processamento(
                        config, nome, "RAW", inicio_processamento, erro_msg
                    )
                except Exception as email_err:
                    print(
                        f"[EMAIL] Erro ao enviar e-mail de erro para {nome}: {email_err}")

        except Exception as e:
            erro_count += 1
            resultados[nome] = False
            erro_msg = str(e)
            traceback_str = tb.format_exc()
            print(f"[ERRO] Erro ao processar {nome}: {e}")
            # Enviar e-mail de erro para processamento específico
            try:
                enviar_email_erro_processamento(
                    config, nome, "RAW", inicio_processamento, erro_msg, traceback_str
                )
            except Exception as email_err:
                print(
                    f"[EMAIL] Erro ao enviar e-mail de erro para {nome}: {email_err}")

    # Resumo final
    print("\n" + "="*80)
    print("RESUMO DO PROCESSAMENTO")
    print("="*80)
    print(f"Total de tabelas: {total}")
    print(f"[OK] Sucesso: {sucesso_count}")
    print(f"[ERRO] Erros: {erro_count}")
    print("="*80)

    # Detalhamento
    print("\nDETALHAMENTO:")
    for nome, sucesso in resultados.items():
        status = "[OK] OK" if sucesso else "[ERRO] FALHOU"
        print(f"  {status} - {nome}")

    # Tempo total da camada raw
    fim_raw = datetime.now()
    duracao_raw = fim_raw - inicio_raw
    print("\n" + "="*80)
    print(f"Fim: {fim_raw.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"Duração da camada RAW: {duracao_raw}")
    print("="*80)

    return resultados


def processar_camada_silver(config):
    """
    Processa todas as tabelas da camada silver.

    Args:
        config: Dicionário com as configurações

    Returns:
        dict: Dicionário com o resultado de cada processamento
    """
    print("="*80)
    print("INICIANDO PROCESSAMENTO DA CAMADA SILVER")
    print("="*80)

    # Lista de processadores
    processadores = [
        ("Contas a Pagar", processar_contas_a_pagar_silver),
        ("Contas a Receber", processar_contas_a_receber_silver),
    ]

    resultados = {}
    total = len(processadores)
    sucesso_count = 0
    erro_count = 0

    for idx, (nome, processador) in enumerate(processadores, 1):
        inicio_processamento = datetime.now()
        print(f"\n[{idx}/{total}] Processando {nome}...")
        print(
            f"Início: {inicio_processamento.strftime('%d/%m/%Y às %H:%M:%S')}")
        print("-" * 80)

        try:
            sucesso = processador(config)
            resultados[nome] = sucesso

            if sucesso:
                sucesso_count += 1
                print(f"[OK] {nome} processado com sucesso!")
            else:
                erro_count += 1
                print(f"[ERRO] {nome} falhou no processamento.")
                # Enviar e-mail de erro para processamento específico
                try:
                    erro_msg = f"O processamento '{nome}' retornou False (falhou sem exceção)"
                    enviar_email_erro_processamento(
                        config, nome, "GOLD", inicio_processamento, erro_msg
                    )
                except Exception as email_err:
                    print(
                        f"[EMAIL] Erro ao enviar e-mail de erro para {nome}: {email_err}")

        except Exception as e:
            erro_count += 1
            resultados[nome] = False
            erro_msg = str(e)
            traceback_str = tb.format_exc()
            print(f"[ERRO] Erro ao processar {nome}: {e}")
            # Enviar e-mail de erro para processamento específico
            try:
                enviar_email_erro_processamento(
                    config, nome, "GOLD", inicio_processamento, erro_msg, traceback_str
                )
            except Exception as email_err:
                print(
                    f"[EMAIL] Erro ao enviar e-mail de erro para {nome}: {email_err}")

    # Resumo final
    print("\n" + "="*80)
    print("RESUMO DO PROCESSAMENTO")
    print("="*80)
    print(f"Total de tabelas: {total}")
    print(f"[OK] Sucesso: {sucesso_count}")
    print(f"[ERRO] Erros: {erro_count}")
    print("="*80)

    # Detalhamento
    print("\nDETALHAMENTO:")
    for nome, sucesso in resultados.items():
        status = "[OK] OK" if sucesso else "[ERRO] FALHOU"
        print(f"  {status} - {nome}")

    return resultados


def processar_camada_gold(config):
    """
    Processa todas as tabelas da camada gold.

    Args:
        config: Dicionário com as configurações

    Returns:
        dict: Dicionário com o resultado de cada processamento
    """
    print("="*80)
    print("INICIANDO PROCESSAMENTO DA CAMADA GOLD")
    print("="*80)

    # Lista de processadores
    processadores = [
        ("Dim Categorias", processar_categorias_gold),
        ("Dim Clientes", processar_clientes_gold),
        ("Dim Conta Corrente", processar_conta_corrente_gold),
        ("Dim Empresas", processar_dim_empresas_gold),
        ("Fato Contas a Pagar", processar_fato_contas_a_pagar_gold),
        ("Fato Contas a Receber", processar_fato_contas_a_receber_gold),
        ("Fato Extrato", processar_fato_extrato_gold),
        ("Fato Movimentos Financeiro", processar_fato_movimentos_financeiro_gold),
    ]

    resultados = {}
    total = len(processadores)
    sucesso_count = 0
    erro_count = 0

    for idx, (nome, processador) in enumerate(processadores, 1):
        inicio_processamento = datetime.now()
        print(f"\n[{idx}/{total}] Processando {nome}...")
        print(
            f"Início: {inicio_processamento.strftime('%d/%m/%Y às %H:%M:%S')}")
        print("-" * 80)

        try:
            sucesso = processador(config)
            resultados[nome] = sucesso

            if sucesso:
                sucesso_count += 1
                print(f"[OK] {nome} processado com sucesso!")
            else:
                erro_count += 1
                print(f"[ERRO] {nome} falhou no processamento.")
                # Enviar e-mail de erro para processamento específico
                try:
                    erro_msg = f"O processamento '{nome}' retornou False (falhou sem exceção)"
                    enviar_email_erro_processamento(
                        config, nome, "GOLD", inicio_processamento, erro_msg
                    )
                except Exception as email_err:
                    print(
                        f"[EMAIL] Erro ao enviar e-mail de erro para {nome}: {email_err}")

        except Exception as e:
            erro_count += 1
            resultados[nome] = False
            erro_msg = str(e)
            traceback_str = tb.format_exc()
            print(f"[ERRO] Erro ao processar {nome}: {e}")
            # Enviar e-mail de erro para processamento específico
            try:
                enviar_email_erro_processamento(
                    config, nome, "GOLD", inicio_processamento, erro_msg, traceback_str
                )
            except Exception as email_err:
                print(
                    f"[EMAIL] Erro ao enviar e-mail de erro para {nome}: {email_err}")

    # Resumo final
    print("\n" + "="*80)
    print("RESUMO DO PROCESSAMENTO")
    print("="*80)
    print(f"Total de tabelas: {total}")
    print(f"[OK] Sucesso: {sucesso_count}")
    print(f"[ERRO] Erros: {erro_count}")
    print("="*80)

    # Detalhamento
    print("\nDETALHAMENTO:")
    for nome, sucesso in resultados.items():
        status = "[OK] OK" if sucesso else "[ERRO] FALHOU"
        print(f"  {status} - {nome}")

    return resultados


def main_raw():
    """
    Função principal para executar apenas a camada raw.
    """
    inicio = datetime.now()
    print(f"\n{'='*80}")
    print(f"PIPELINE - CAMADA RAW")
    print(f"Início: {inicio.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'='*80}\n")

    # Carregar configurações
    print("Carregando configurações...")
    config = carregar_config()
    print(f"[OK] Configurações carregadas\n")

    connection = None
    resultados_raw = None
    erro_ocorrido = None
    traceback_str = None

    try:
        # Conectar ao MySQL (necessário para a camada raw)
        print("Conectando ao banco de dados MySQL...")
        connection = conectar_mysql()

        if connection is None:
            erro_msg = "Não foi possível estabelecer conexão com o banco de dados."
            print(f"\n[ERRO] {erro_msg}")
            print("Pipeline abortado.")
            erro_ocorrido = erro_msg
            raise Exception(erro_msg)

        # Processar camada raw
        resultados_raw = processar_camada_raw(connection, config)

        print("\n" + "="*80)
        print("PIPELINE FINALIZADO - Camada RAW processada")
        print("="*80)

    except Exception as e:
        erro_ocorrido = e
        traceback_str = tb.format_exc()
        print(f"\n[ERRO] Erro durante o processamento: {e}")
        print(f"\nDetalhes do erro:\n{traceback_str}")

    finally:
        # Desconectar do banco de dados
        if connection is not None and connection.is_connected():
            connection.close()
            print(f"\n{'='*80}")
            print("[OK] Conexão com o banco de dados encerrada.")
            print(f"{'='*80}")

        # Tempo total
        fim = datetime.now()
        duracao = fim - inicio
        print(f"\nFim: {fim.strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"Duração total: {duracao}")
        print(f"\n{'='*80}\n")

        # Enviar e-mail de notificação
        try:
            if erro_ocorrido:
                # E-mail de erro
                assunto = f"[ERRO] Erro no Pipeline ETL - RAW"
                corpo_html, corpo_texto = criar_corpo_email_erro(
                    "RAW", inicio, erro_ocorrido, traceback_str
                )
                enviar_email(config, assunto, corpo_html,
                             corpo_texto, erro_ocorrido)
            else:
                # E-mail de sucesso
                assunto = f"[OK] Pipeline ETL Concluído - RAW"
                corpo_html, corpo_texto = criar_corpo_email_sucesso(
                    "RAW", inicio, fim, duracao, resultados_raw=resultados_raw
                )
                enviar_email(config, assunto, corpo_html, corpo_texto)
        except Exception as email_error:
            print(
                f"[EMAIL] Erro ao tentar enviar e-mail de notificação: {email_error}")


def main_silver_gold():
    """
    Função que executa apenas as camadas silver e gold.
    """
    inicio = datetime.now()
    print(f"\n{'='*80}")
    print(f"PIPELINE - SILVER -> GOLD")
    print(f"Início: {inicio.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'='*80}\n")

    # Carregar configurações
    print("Carregando configurações...")
    config = carregar_config()
    print(f"[OK] Configurações carregadas\n")

    resultados_silver = None
    resultados_gold = None
    erro_ocorrido = None
    traceback_str = None

    try:
        # Processar camada silver
        print("\n" + "="*80)
        resultados_silver = processar_camada_silver(config)

        # Processar camada gold
        print("\n" + "="*80)
        resultados_gold = processar_camada_gold(config)

        # Resumo geral do pipeline
        print("\n" + "="*80)
        print("RESUMO GERAL DO PIPELINE")
        print("="*80)

        total_silver = len(resultados_silver)
        sucesso_silver = sum(1 for v in resultados_silver.values() if v)
        total_gold = len(resultados_gold)
        sucesso_gold = sum(1 for v in resultados_gold.values() if v)

        print(
            f"SILVER: {sucesso_silver}/{total_silver} processados com sucesso")
        print(f"GOLD: {sucesso_gold}/{total_gold} processados com sucesso")
        print("="*80)

    except Exception as e:
        erro_ocorrido = e
        traceback_str = tb.format_exc()
        print(f"\n[ERRO] Erro durante o processamento: {e}")
        print(f"\nDetalhes do erro:\n{traceback_str}")

    # Tempo total
    fim = datetime.now()
    duracao = fim - inicio
    print(f"\nFim: {fim.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"Duração total: {duracao}")
    print(f"\n{'='*80}\n")

    # Enviar e-mail de notificação
    try:
        if erro_ocorrido:
            # E-mail de erro
            assunto = f"[ERRO] Erro no Pipeline ETL - SILVER -> GOLD"
            corpo_html, corpo_texto = criar_corpo_email_erro(
                "SILVER -> GOLD", inicio, erro_ocorrido, traceback_str
            )
            enviar_email(config, assunto, corpo_html,
                         corpo_texto, erro_ocorrido)
        else:
            # E-mail de sucesso
            assunto = f"[OK] Pipeline ETL Concluído - SILVER -> GOLD"
            corpo_html, corpo_texto = criar_corpo_email_sucesso(
                "SILVER -> GOLD", inicio, fim, duracao,
                resultados_silver=resultados_silver, resultados_gold=resultados_gold
            )
            enviar_email(config, assunto, corpo_html, corpo_texto)
    except Exception as email_error:
        print(
            f"[EMAIL] Erro ao tentar enviar e-mail de notificação: {email_error}")


def main():
    """
    Função principal que orquestra o pipeline completo: RAW -> SILVER -> GOLD.
    Executa todas as etapas em sequência.
    """
    inicio = datetime.now()
    print(f"\n{'='*80}")
    print(f"PIPELINE COMPLETO - RAW -> SILVER -> GOLD")
    print(f"Início: {inicio.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'='*80}\n")

    # Carregar configurações
    print("Carregando configurações...")
    config = carregar_config()
    print(f"[OK] Configurações carregadas\n")

    connection = None
    resultados_raw = None
    resultados_silver = None
    resultados_gold = None
    erro_ocorrido = None
    traceback_str = None

    try:
        # ========== ETAPA 1: CAMADA RAW ==========
        print("Conectando ao banco de dados MySQL...")
        connection = conectar_mysql()

        if connection is None:
            erro_msg = "Não foi possível estabelecer conexão com o banco de dados."
            print(f"\n[ERRO] {erro_msg}")
            print("Pipeline abortado.")
            erro_ocorrido = erro_msg
            raise Exception(erro_msg)

        resultados_raw = processar_camada_raw(connection, config)

        # Fechar conexão após processar RAW
        if connection is not None and connection.is_connected():
            connection.close()
            print(f"\n{'='*80}")
            print("[OK] Conexão com o banco de dados encerrada.")
            print(f"{'='*80}\n")
            connection = None

        # ========== ETAPA 2: CAMADA SILVER ==========
        resultados_silver = processar_camada_silver(config)

        # ========== ETAPA 3: CAMADA GOLD ==========
        resultados_gold = processar_camada_gold(config)

        # ========== RESUMO GERAL DO PIPELINE ==========
        print("\n" + "="*80)
        print("RESUMO GERAL DO PIPELINE COMPLETO")
        print("="*80)

        total_raw = len(resultados_raw)
        sucesso_raw = sum(1 for v in resultados_raw.values() if v)
        total_silver = len(resultados_silver)
        sucesso_silver = sum(1 for v in resultados_silver.values() if v)
        total_gold = len(resultados_gold)
        sucesso_gold = sum(1 for v in resultados_gold.values() if v)

        print(f"RAW: {sucesso_raw}/{total_raw} processados com sucesso")
        print(
            f"SILVER: {sucesso_silver}/{total_silver} processados com sucesso")
        print(f"GOLD: {sucesso_gold}/{total_gold} processados com sucesso")
        print("="*80)

        # Estatísticas gerais
        total_geral = total_raw + total_silver + total_gold
        sucesso_geral = sucesso_raw + sucesso_silver + sucesso_gold
        erro_geral = total_geral - sucesso_geral

        print(
            f"\nTOTAL GERAL: {sucesso_geral}/{total_geral} processados com sucesso")
        print(f"ERROS: {erro_geral}")
        print("="*80)

    except Exception as e:
        erro_ocorrido = e
        traceback_str = tb.format_exc()
        print(f"\n[ERRO] Erro durante o processamento: {e}")
        print(f"\nDetalhes do erro:\n{traceback_str}")

    finally:
        # Garantir que a conexão seja fechada
        if connection is not None and connection.is_connected():
            connection.close()
            print(f"\n{'='*80}")
            print("[OK] Conexão com o banco de dados encerrada.")
            print(f"{'='*80}")

        # Tempo total
        fim = datetime.now()
        duracao = fim - inicio
        print(f"\nFim: {fim.strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"Duração total: {duracao}")
        print(f"\n{'='*80}\n")

        # Enviar e-mail de notificação
        try:
            if erro_ocorrido:
                # E-mail de erro
                assunto = f"✗ Valoreasy - Projeto Financeiro OMIE - Erro em {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
                corpo_html, corpo_texto = criar_corpo_email_erro(
                    "RAW -> SILVER -> GOLD", inicio, erro_ocorrido, traceback_str
                )
                enviar_email(config, assunto, corpo_html,
                             corpo_texto, erro_ocorrido)
            else:
                # E-mail de sucesso
                assunto = f"✓ Valoreasy - Projeto Financeiro OMIE - Sucesso em {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
                corpo_html, corpo_texto = criar_corpo_email_sucesso(
                    "RAW -> SILVER -> GOLD", inicio, fim, duracao,
                    resultados_raw=resultados_raw,
                    resultados_silver=resultados_silver,
                    resultados_gold=resultados_gold
                )
                enviar_email(config, assunto, corpo_html, corpo_texto)
        except Exception as email_error:
            print(
                f"[EMAIL] Erro ao tentar enviar e-mail de notificação: {email_error}")


if __name__ == "__main__":
    # Para executar todas as etapas (RAW -> SILVER -> GOLD), use: python main.py
    # Para executar apenas a camada RAW, use: python main.py raw
    # Para executar apenas SILVER e GOLD, use: python main.py silver_gold

    if len(sys.argv) > 1:
        comando = sys.argv[1].lower()
        if comando in ['raw', 'main_raw']:
            main_raw()
        elif comando in ['silver_gold', 'silver', 'gold']:
            main_silver_gold()
        else:
            print(f"Comando desconhecido: {comando}")
            print("Comandos disponíveis:")
            print(
                "  - python main.py          : Executa todas as etapas (RAW -> SILVER -> GOLD)")
            print("  - python main.py raw      : Executa apenas a camada RAW")
            print(
                "  - python main.py silver_gold : Executa apenas as camadas SILVER e GOLD")
            sys.exit(1)
    else:
        # Executa todas as etapas por padrão
        main()
