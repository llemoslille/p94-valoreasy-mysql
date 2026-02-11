"""
Script de Teste - Sistema de Notificacoes por Email
ETL Vanguard - Projeto Financeiro

Este script envia emails de teste para validar o sistema de notificacoes.
"""

import time
from email_notifier import enviar_notificacao_sucesso, enviar_notificacao_erro
from config_email import EMAIL_DESTINATARIO
from logger_config import obter_logger

# Configurar logger
logger = obter_logger('teste_email')


def teste_email_sucesso():
    """Testa o envio de email de sucesso"""
    print("\n" + "=" * 70)
    print("TESTE 1: EMAIL DE SUCESSO")
    print("=" * 70)

    logger.info("Iniciando teste de email de sucesso...")

    # Dados de teste
    tempo_execucao = "0:05:32"
    diagnostico = {
        'Scripts Executados': '11/11',
        'Status': 'Concluido com sucesso - TESTE',
        'Tabelas Atualizadas': 'Todas as tabelas do BigQuery (TESTE)',
        'Observacao': 'Este e um email de TESTE do sistema'
    }

    # Enviar para cada destinatario
    if isinstance(EMAIL_DESTINATARIO, list):
        destinatarios = EMAIL_DESTINATARIO
    else:
        destinatarios = [EMAIL_DESTINATARIO]

    print(
        f"\n[INFO] Enviando email de SUCESSO para {len(destinatarios)} destinatario(s)...")

    for i, email in enumerate(destinatarios, 1):
        try:
            print(f"\n[{i}/{len(destinatarios)}] Enviando para: {email}")
            logger.info(f"Enviando email de sucesso para: {email}")

            enviar_notificacao_sucesso(tempo_execucao, email, diagnostico)

            print(f"[OK] Email enviado com sucesso para {email}")
            logger.info(f"Email de sucesso enviado para: {email}")

            # Pequeno delay entre envios
            if i < len(destinatarios):
                time.sleep(1)

        except Exception as e:
            print(f"[ERRO] Falha ao enviar para {email}: {str(e)}")
            logger.error(f"Erro ao enviar email para {email}: {str(e)}")

    print("\n[OK] Teste de email de SUCESSO concluido!")
    logger.info("Teste de email de sucesso concluido")


def teste_email_erro():
    """Testa o envio de email de erro"""
    print("\n" + "=" * 70)
    print("TESTE 2: EMAIL DE ERRO")
    print("=" * 70)

    logger.info("Iniciando teste de email de erro...")

    # Mensagem de erro de teste
    erro_mensagem = """
Traceback (most recent call last):
  File "main.py", line 89, in executar_script
    funcao_main()
  File "fAgencia.py", line 45, in main
    response = requests.get(url, timeout=30)
  File "requests/api.py", line 73, in get
    return request('get', url, params=params, **kwargs)
ConnectionError: HTTPSConnectionPool(host='api.exemplo.com', port=443): 
Max retries exceeded with url: /endpoint (Caused by NewConnectionError)

ESTE E UM ERRO DE TESTE DO SISTEMA DE NOTIFICACOES
O ETL nao falhou de verdade - este e apenas um teste!
"""

    # Enviar para cada destinatario
    if isinstance(EMAIL_DESTINATARIO, list):
        destinatarios = EMAIL_DESTINATARIO
    else:
        destinatarios = [EMAIL_DESTINATARIO]

    print(
        f"\n[INFO] Enviando email de ERRO para {len(destinatarios)} destinatario(s)...")

    for i, email in enumerate(destinatarios, 1):
        try:
            print(f"\n[{i}/{len(destinatarios)}] Enviando para: {email}")
            logger.info(f"Enviando email de erro para: {email}")

            enviar_notificacao_erro(erro_mensagem, email)

            print(f"[OK] Email enviado com sucesso para {email}")
            logger.info(f"Email de erro enviado para: {email}")

            # Pequeno delay entre envios
            if i < len(destinatarios):
                time.sleep(1)

        except Exception as e:
            print(f"[ERRO] Falha ao enviar para {email}: {str(e)}")
            logger.error(f"Erro ao enviar email para {email}: {str(e)}")

    print("\n[OK] Teste de email de ERRO concluido!")
    logger.info("Teste de email de erro concluido")


def main():
    """Executa todos os testes de email"""
    print("\n" + "=" * 70)
    print("TESTE DO SISTEMA DE NOTIFICACOES POR EMAIL")
    print("ETL Vanguard - Projeto Financeiro")
    print("=" * 70)

    logger.info("=" * 60)
    logger.info("=== INICIANDO TESTES DE EMAIL ===")
    logger.info("=" * 60)

    # Verificar destinatarios
    if isinstance(EMAIL_DESTINATARIO, list):
        destinatarios = EMAIL_DESTINATARIO
    else:
        destinatarios = [EMAIL_DESTINATARIO]

    print(f"\n[INFO] Destinatarios configurados: {len(destinatarios)}")
    for i, email in enumerate(destinatarios, 1):
        print(f"       {i}. {email}")

    print("\n[INFO] Serao enviados 2 emails para cada destinatario:")
    print("       1. Email de SUCESSO (teste)")
    print("       2. Email de ERRO (teste)")
    print(f"\n[INFO] Total de emails a enviar: {len(destinatarios) * 2}")

    print("\n" + "-" * 70)
    print("Iniciando envio dos emails de teste em 2 segundos...")
    print("-" * 70)
    time.sleep(2)

    try:
        # Teste 1: Email de Sucesso
        teste_email_sucesso()

        # Delay entre testes
        print("\n[INFO] Aguardando 3 segundos antes do proximo teste...")
        time.sleep(3)

        # Teste 2: Email de Erro
        teste_email_erro()

        # Resumo final
        print("\n" + "=" * 70)
        print("TODOS OS TESTES CONCLUIDOS COM SUCESSO!")
        print("=" * 70)
        print(f"\n[OK] {len(destinatarios) * 2} emails enviados com sucesso!")
        print("\n[INFO] Verifique a caixa de entrada dos destinatarios:")
        for i, email in enumerate(destinatarios, 1):
            print(f"       {i}. {email}")

        print("\n[AVISO] Se os emails nao chegaram:")
        print("        1. Verifique a pasta de SPAM")
        print("        2. Confirme os emails em config_email.py")
        print("        3. Verifique os logs em: logs/")

        print("\n" + "=" * 70)

        logger.info("=" * 60)
        logger.info("=== TESTES DE EMAIL CONCLUIDOS COM SUCESSO ===")
        logger.info("=" * 60)

    except Exception as e:
        print("\n" + "=" * 70)
        print("ERRO DURANTE OS TESTES")
        print("=" * 70)
        print(f"\n[ERRO] {str(e)}")
        print("\n[INFO] Verifique os logs para mais detalhes")

        logger.error("=" * 60)
        logger.error("=== ERRO DURANTE TESTES DE EMAIL ===")
        logger.error(f"Erro: {str(e)}")
        logger.error("=" * 60)

        raise


if __name__ == '__main__':
    main()
