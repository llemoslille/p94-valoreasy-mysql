"""
Script de Teste - Sistema de Notificação por Email
===================================================

Este script testa o envio de emails de sucesso e erro.
"""

import time
from email_notifier import enviar_notificacao_sucesso, enviar_notificacao_erro
from config_email import EMAIL_DESTINATARIO

def testar_notificacao_sucesso():
    """Testa o envio de email de sucesso"""
    print("\n" + "="*80)
    print("TESTE 1: Notificação de Sucesso")
    print("="*80)
    
    # Simular dados de execução bem-sucedida
    tempo_execucao = "5 minutos e 32 segundos"
    
    diagnostico_geral = {
        "Scripts Executados": "25 scripts",
        "Registros Processados": "15.432 registros",
        "Arquivos Gerados": "8 arquivos parquet",
        "Camadas Processadas": "Bronze > Silver > Gold",
        "Tabelas BigQuery": "5 tabelas atualizadas",
        "Status Validacao": "Todas as validacoes passaram"
    }
    
    print(f"\nEnviando email de SUCESSO para: {EMAIL_DESTINATARIO}")
    print(f"Tempo de execucao: {tempo_execucao}")
    print(f"Diagnostico: {len(diagnostico_geral)} itens")
    
    try:
        # Verificar se é lista ou string
        if isinstance(EMAIL_DESTINATARIO, list):
            for email in EMAIL_DESTINATARIO:
                print(f"\n   > Enviando para: {email}")
                enviar_notificacao_sucesso(tempo_execucao, email, diagnostico_geral)
        else:
            enviar_notificacao_sucesso(tempo_execucao, EMAIL_DESTINATARIO, diagnostico_geral)
        
        print("\n[OK] Teste de notificacao de SUCESSO concluido!")
        return True
        
    except Exception as e:
        print(f"\n[ERRO] Erro no teste de sucesso: {e}")
        return False


def testar_notificacao_erro():
    """Testa o envio de email de erro"""
    print("\n" + "="*80)
    print("TESTE 2: Notificação de Erro")
    print("="*80)
    
    # Simular mensagem de erro
    erro_mensagem = """
Traceback (most recent call last):
  File "nbronze_produtos.py", line 185, in main
    df = bronze.coletar_todas_paginas(registros_por_pagina=50)
  File "nbronze_produtos.py", line 144, in coletar_todas_paginas
    resposta = self.coletar_produtos(pagina=pagina)
  File "nbronze_produtos.py", line 88, in coletar_produtos
    return self._make_request(data)
requests.exceptions.Timeout: HTTPSConnectionPool(host='app.omie.com.br', port=443): 
Read timed out. (read timeout=60)

ERRO: Falha ao coletar produtos da API Omie após 3 tentativas.
Página que falhou: 15
Total de registros coletados antes do erro: 700
    """
    
    print(f"\nEnviando email de ERRO para: {EMAIL_DESTINATARIO}")
    print(f"Erro simulado: Timeout na API Omie")
    
    try:
        # Verificar se é lista ou string
        if isinstance(EMAIL_DESTINATARIO, list):
            for email in EMAIL_DESTINATARIO:
                print(f"\n   > Enviando para: {email}")
                enviar_notificacao_erro(erro_mensagem, email)
        else:
            enviar_notificacao_erro(erro_mensagem, EMAIL_DESTINATARIO)
        
        print("\n[OK] Teste de notificacao de ERRO concluido!")
        return True
        
    except Exception as e:
        print(f"\n[ERRO] Erro no teste de erro: {e}")
        return False


def main():
    """Executa todos os testes"""
    print("\n" + "="*80)
    print("INICIANDO TESTES DO SISTEMA DE NOTIFICACAO POR EMAIL")
    print("="*80)
    
    # Mostrar configuração
    print(f"\nConfiguracao:")
    if isinstance(EMAIL_DESTINATARIO, list):
        print(f"   Destinatarios: {len(EMAIL_DESTINATARIO)} emails")
        for i, email in enumerate(EMAIL_DESTINATARIO, 1):
            print(f"   {i}. {email}")
    else:
        print(f"   Destinatario: {EMAIL_DESTINATARIO}")
    
    print(f"\nATENCAO: Este teste enviara 2 emails de teste!")
    print(f"   - 1 email de SUCESSO")
    print(f"   - 1 email de ERRO")
    
    resposta = input("\nDeseja continuar? (s/n): ")
    
    if resposta.lower() != 's':
        print("\n[X] Testes cancelados pelo usuario.")
        return
    
    # Executar testes
    resultados = []
    
    # Teste 1: Sucesso
    resultado_sucesso = testar_notificacao_sucesso()
    resultados.append(("Notificacao de Sucesso", resultado_sucesso))
    
    # Aguardar 3 segundos entre os testes
    print("\nAguardando 3 segundos antes do proximo teste...")
    time.sleep(3)
    
    # Teste 2: Erro
    resultado_erro = testar_notificacao_erro()
    resultados.append(("Notificacao de Erro", resultado_erro))
    
    # Resumo dos testes
    print("\n" + "="*80)
    print("RESUMO DOS TESTES")
    print("="*80)
    
    for nome_teste, resultado in resultados:
        status = "[OK] PASSOU" if resultado else "[X] FALHOU"
        print(f"{status} - {nome_teste}")
    
    total_passou = sum(1 for _, r in resultados if r)
    total_testes = len(resultados)
    
    print(f"\nResultado Final: {total_passou}/{total_testes} testes passaram")
    
    if total_passou == total_testes:
        print("\n[OK] Todos os testes foram bem-sucedidos!")
        print("[OK] Sistema de notificacao por email esta funcionando corretamente!")
    else:
        print("\n[AVISO] Alguns testes falharam. Verifique os erros acima.")
    
    print("\nVerifique sua caixa de entrada para confirmar o recebimento dos emails.")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()

