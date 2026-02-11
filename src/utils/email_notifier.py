import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import traceback


def enviar_email(config, assunto, corpo_html, corpo_texto=None, erro=None):
    """
    Envia e-mail de notificação sobre o status do pipeline ETL.

    Args:
        config: Dicionário com as configurações (incluindo email)
        assunto: Assunto do e-mail
        corpo_html: Corpo do e-mail em HTML
        corpo_texto: Corpo do e-mail em texto plano (opcional)
        erro: Objeto de exceção ou string com detalhes do erro (opcional)

    Returns:
        bool: True se o e-mail foi enviado com sucesso, False caso contrário
    """
    try:
        # Verificar se as configurações de e-mail existem
        if 'email' not in config:
            print(
                "[EMAIL] Configurações de e-mail não encontradas. E-mail não será enviado.")
            return False

        email_config = config['email']

        # Verificar se o envio de e-mail está habilitado
        if not email_config.get('enabled', False):
            print("[EMAIL] Envio de e-mail desabilitado nas configurações.")
            return False

        # Obter configurações SMTP
        smtp_server = email_config.get('smtp_server')
        smtp_port = email_config.get('smtp_port', 587)
        smtp_user = email_config.get('smtp_user')
        smtp_password = email_config.get('smtp_password')
        from_email = email_config.get('from_email', smtp_user)
        to_emails = email_config.get('to_emails', [])

        if not smtp_server or not smtp_user or not smtp_password:
            print("[EMAIL] Configurações de SMTP incompletas. E-mail não será enviado.")
            return False

        if not to_emails:
            print("[EMAIL] Nenhum destinatário configurado. E-mail não será enviado.")
            return False

        # Remover espaços da senha (senhas de aplicativo do Gmail não devem ter espaços)
        smtp_password = smtp_password.replace(' ', '')

        # Criar mensagem
        msg = MIMEMultipart('alternative')
        msg['Subject'] = assunto
        msg['From'] = from_email
        msg['To'] = ', '.join(to_emails) if isinstance(
            to_emails, list) else to_emails

        # Adicionar corpo em texto plano se fornecido
        if corpo_texto:
            part1 = MIMEText(corpo_texto, 'plain', 'utf-8')
            msg.attach(part1)

        # Adicionar corpo em HTML
        part2 = MIMEText(corpo_html, 'html', 'utf-8')
        msg.attach(part2)

        # Conectar e enviar
        # Tentar porta 587 (TLS) primeiro, depois 465 (SSL) se falhar
        try:
            if smtp_port == 465:
                # Usar SSL para porta 465
                with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
                    server.login(smtp_user, smtp_password)
                    server.send_message(msg)
            else:
                # Usar TLS para porta 587
                with smtplib.SMTP(smtp_server, smtp_port) as server:
                    server.starttls()
                    server.login(smtp_user, smtp_password)
                    server.send_message(msg)
        except smtplib.SMTPAuthenticationError as auth_error:
            error_code = auth_error.smtp_code if hasattr(
                auth_error, 'smtp_code') else None
            error_msg = str(auth_error)

            if error_code == 535 or '535' in error_msg or 'BadCredentials' in error_msg:
                print(
                    "[EMAIL] Erro de autenticação (535): Credenciais inválidas ou senha de aplicativo expirada.")
                print("[EMAIL] Verifique:")
                print("[EMAIL]   1. Se o email e senha estão corretos no config.yaml")
                print("[EMAIL]   2. Se a senha de aplicativo do Gmail está válida")
                print(
                    "[EMAIL]   3. Se a autenticação de dois fatores está habilitada")
                print(
                    "[EMAIL]   4. Se uma nova senha de aplicativo precisa ser gerada")
                print(f"[EMAIL] Detalhes do erro: {error_msg}")
            else:
                print(f"[EMAIL] Erro de autenticação SMTP: {error_msg}")
            return False

        print(
            f"[EMAIL] E-mail enviado com sucesso para: {', '.join(to_emails) if isinstance(to_emails, list) else to_emails}")
        return True

    except Exception as e:
        error_msg = str(e)
        if '535' in error_msg or 'BadCredentials' in error_msg:
            print("[EMAIL] Erro ao enviar e-mail: Credenciais inválidas (535)")
            print(
                "[EMAIL] A senha de aplicativo do Gmail pode ter expirado ou estar incorreta.")
            print(
                "[EMAIL] Acesse: https://myaccount.google.com/apppasswords para gerar uma nova senha.")
        else:
            print(f"[EMAIL] Erro ao enviar e-mail: {e}")
        return False


def criar_corpo_email_sucesso(tipo_pipeline, inicio, fim, duracao, resultados_raw=None,
                              resultados_silver=None, resultados_gold=None):
    """
    Cria o corpo do e-mail para notificação de sucesso no formato simplificado.

    Args:
        tipo_pipeline: Tipo de pipeline executado (RAW, SILVER_GOLD, COMPLETO)
        inicio: Data/hora de início
        fim: Data/hora de fim
        duracao: Duração total do processamento
        resultados_raw: Dicionário com resultados da camada RAW (opcional)
        resultados_silver: Dicionário com resultados da camada SILVER (opcional)
        resultados_gold: Dicionário com resultados da camada GOLD (opcional)

    Returns:
        tuple: (corpo_html, corpo_texto)
    """
    # Calcular estatísticas
    stats = {}
    scripts_raw = []
    scripts_silver = []
    scripts_gold = []

    if resultados_raw:
        total_raw = len(resultados_raw)
        sucesso_raw = sum(1 for v in resultados_raw.values()
                          if isinstance(v, bool) and v)
        erro_raw = total_raw - sucesso_raw
        stats['RAW'] = {'total': total_raw,
                        'sucesso': sucesso_raw, 'erro': erro_raw}
        scripts_raw = list(resultados_raw.keys())

    if resultados_silver:
        total_silver = len(resultados_silver)
        sucesso_silver = sum(
            1 for v in resultados_silver.values() if isinstance(v, bool) and v)
        erro_silver = total_silver - sucesso_silver
        stats['SILVER'] = {'total': total_silver,
                           'sucesso': sucesso_silver, 'erro': erro_silver}
        scripts_silver = list(resultados_silver.keys())

    if resultados_gold:
        total_gold = len(resultados_gold)
        sucesso_gold = sum(1 for v in resultados_gold.values()
                           if isinstance(v, bool) and v)
        erro_gold = total_gold - sucesso_gold
        stats['GOLD'] = {'total': total_gold,
                         'sucesso': sucesso_gold, 'erro': erro_gold}
        scripts_gold = list(resultados_gold.keys())

    # Calcular totais gerais
    total_geral = sum(s['total'] for s in stats.values())
    sucesso_geral = sum(s['sucesso'] for s in stats.values())
    erro_geral = sum(s['erro'] for s in stats.values())

    # Formatar duração
    if isinstance(duracao, str):
        duracao_str = duracao
    else:
        total_seconds = int(duracao.total_seconds())
        minutos = total_seconds // 60
        segundos = total_seconds % 60
        duracao_str = f"{minutos} minutos e {segundos} segundos"

    # Corpo HTML no formato simplificado
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333; background-color: #ffffff; }}
            .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
            .header {{ color: #4CAF50; padding: 20px 0; border-bottom: 2px solid #4CAF50; }}
            .header h1 {{ margin: 0; font-size: 24px; color: #4CAF50; }}
            .content {{ padding: 20px 0; }}
            .info-row {{ margin: 10px 0; }}
            .info-label {{ font-weight: bold; }}
            .checklist {{ margin: 15px 0; }}
            .checklist li {{ margin: 8px 0; color: #4CAF50; }}
            .summary {{ background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin: 20px 0; }}
            .summary-title {{ font-weight: bold; font-size: 16px; margin-bottom: 10px; }}
            .summary-item {{ margin: 5px 0; }}
            .summary-label {{ font-weight: bold; }}
            .camada-section {{ margin: 15px 0; }}
            .camada-title {{ font-weight: bold; margin-bottom: 5px; }}
            .next-steps {{ margin-top: 20px; padding-top: 20px; border-top: 1px solid #ddd; }}
            .next-steps h3 {{ margin-top: 0; }}
            .next-steps ul {{ margin: 10px 0; padding-left: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>✓ ETL Projeto Valoreasy - Finalizado com Sucesso!</h1>
            </div>
            <div class="content">
                <div class="info-row">
                    <span class="info-label">Data/Hora:</span> {fim.strftime('%d/%m/%Y %H:%M:%S')}
                </div>
                <div class="info-row">
                    <span class="info-label">Tempo de Execução:</span> {duracao_str}
                </div>
                
                <p>O processo ETL de Projeto Valoreasy foi executado com sucesso.</p>
                
                <ul class="checklist">
                    <li>✓ Dados extraídos, limpos e carregados no BigQuery</li>
                    <li>✓ Validações aplicadas (duplicatas removidas, datas corrigidas)</li>
                    <li>✓ Tabelas prontas para uso no Power BI</li>
                </ul>

                <div class="summary">
                    <div class="summary-title">📊 Resumo da Execução:</div>
                    <div class="summary-item">
                        <span class="summary-label">Status:</span> {'✓ Todos os scripts executados com sucesso' if erro_geral == 0 else f'⚠ {erro_geral} script(s) com erro'}
                    </div>
                    <div class="summary-item">
                        <span class="summary-label">Scripts Executados:</span> {sucesso_geral}/{total_geral}
                    </div>
                    {f'<div class="summary-item"><span class="summary-label">Camada RAW:</span> {len(scripts_raw)} scripts</div>' if scripts_raw else ''}
                    {f'<div class="summary-item"><span class="summary-label">Camada SILVER:</span> {len(scripts_silver)} scripts</div>' if scripts_silver else ''}
                    {f'<div class="summary-item"><span class="summary-label">Camada GOLD:</span> {len(scripts_gold)} scripts</div>' if scripts_gold else ''}
                    <div class="summary-item">
                        <span class="summary-label">Tempo Total:</span> {duracao_str}
                    </div>
                    <div class="summary-item">
                        <span class="summary-label">Data/Hora Conclusão:</span> {fim.strftime('%d/%m/%Y %H:%M:%S')}
                    </div>
                </div>

                <div class="next-steps">
                    <h3>Próximos passos:</h3>
                    <ul>
                        <li>Acesse o BigQuery Console</li>
                        <li>Verifique o projeto: lille</li>
                        <li>Verifique os datasets atualizados</li>
                        <li>Conecte o Power BI às tabelas atualizadas</li>
                    </ul>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    # Corpo em texto plano
    texto = f"""
ETL Projeto Valoreasy - Finalizado com Sucesso!
===============================================

Data/Hora: {fim.strftime('%d/%m/%Y %H:%M:%S')}
Tempo de Execução: {duracao_str}

O processo ETL de Projeto Valoreasy foi executado com sucesso.

✓ Dados extraídos, limpos e carregados no BigQuery
✓ Validações aplicadas (duplicatas removidas, datas corrigidas)
✓ Tabelas prontas para uso no Power BI

Resumo da Execução:
- Status: {'Todos os scripts executados com sucesso' if erro_geral == 0 else f'{erro_geral} script(s) com erro'}
- Scripts Executados: {sucesso_geral}/{total_geral}
{f'- Camada RAW: {len(scripts_raw)} scripts' if scripts_raw else ''}
{f'- Camada SILVER: {len(scripts_silver)} scripts' if scripts_silver else ''}
{f'- Camada GOLD: {len(scripts_gold)} scripts' if scripts_gold else ''}
- Tempo Total: {duracao_str}
- Data/Hora Conclusão: {fim.strftime('%d/%m/%Y %H:%M:%S')}

Próximos passos:
- Acesse o BigQuery Console
- Verifique o projeto: lille
- Verifique os datasets atualizados
- Conecte o Power BI às tabelas atualizadas
"""

    return html, texto


def criar_corpo_email_erro(tipo_pipeline, inicio, erro, traceback_str=None):
    """
    Cria o corpo do e-mail para notificação de erro.

    Args:
        tipo_pipeline: Tipo de pipeline executado
        inicio: Data/hora de início
        erro: Mensagem de erro ou objeto de exceção
        traceback_str: String com o traceback completo (opcional)

    Returns:
        tuple: (corpo_html, corpo_texto)
    """
    erro_msg = str(erro) if erro else "Erro desconhecido"
    fim = datetime.now()
    duracao = fim - inicio

    # Corpo HTML
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333; }}
            .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
            .header {{ background-color: #f44336; color: white; padding: 20px; border-radius: 5px 5px 0 0; }}
            .content {{ background-color: #f9f9f9; padding: 20px; border: 1px solid #ddd; }}
            .error-box {{ background-color: #ffebee; border-left: 4px solid #f44336; padding: 15px; margin: 20px 0; }}
            .error-message {{ color: #c62828; font-weight: bold; font-size: 14px; }}
            .traceback {{ background-color: #263238; color: #aed581; padding: 15px; border-radius: 5px; font-family: 'Courier New', monospace; font-size: 10px; white-space: pre-wrap; overflow-x: auto; }}
            .footer {{ background-color: #333; color: white; padding: 10px; text-align: center; border-radius: 0 0 5px 5px; font-size: 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>✗ Erro na Execução do Pipeline ETL</h2>
                <p>Tipo: {tipo_pipeline}</p>
            </div>
            <div class="content">
                <h3>Informações da Execução</h3>
                <p><strong>Início:</strong> {inicio.strftime('%d/%m/%Y %H:%M:%S')}</p>
                <p><strong>Fim:</strong> {fim.strftime('%d/%m/%Y %H:%M:%S')}</p>
                <p><strong>Duração:</strong> {duracao}</p>

                <div class="error-box">
                    <div class="error-message">Erro:</div>
                    <p>{erro_msg}</p>
                </div>
    """

    if traceback_str:
        html += f"""
                <h3>Detalhes do Erro (Traceback)</h3>
                <div class="traceback">{traceback_str}</div>
        """

    html += """
            </div>
            <div class="footer">
                <p>Este é um e-mail automático do sistema de ETL Valoreasy.</p>
                <p>Por favor, verifique os logs e corrija o problema.</p>
            </div>
        </div>
    </body>
    </html>
    """

    # Corpo em texto plano
    texto = f"""
Erro na Execução do Pipeline ETL
=================================

Tipo: {tipo_pipeline}
Início: {inicio.strftime('%d/%m/%Y %H:%M:%S')}
Fim: {fim.strftime('%d/%m/%Y %H:%M:%S')}
Duração: {duracao}

Erro:
{erro_msg}

"""

    if traceback_str:
        texto += f"\nTraceback:\n{traceback_str}\n"

    return html, texto


def criar_corpo_email_erro_processamento(nome_processamento, camada, inicio, erro_msg, traceback_str=None):
    """
    Cria o corpo do e-mail para notificação de erro em um processamento específico.

    Args:
        nome_processamento: Nome do processamento que falhou
        camada: Camada onde ocorreu o erro (RAW, SILVER, GOLD)
        inicio: Data/hora de início do processamento
        erro_msg: Mensagem de erro
        traceback_str: String com o traceback completo (opcional)

    Returns:
        tuple: (corpo_html, corpo_texto)
    """
    fim = datetime.now()
    duracao = fim - inicio

    # Corpo HTML
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333; background-color: #ffffff; }}
            .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
            .header {{ color: #f44336; padding: 20px 0; border-bottom: 2px solid #f44336; }}
            .header h1 {{ margin: 0; font-size: 24px; color: #f44336; }}
            .content {{ padding: 20px 0; }}
            .info-row {{ margin: 10px 0; }}
            .info-label {{ font-weight: bold; }}
            .error-box {{ background-color: #ffebee; border-left: 4px solid #f44336; padding: 15px; margin: 20px 0; }}
            .error-message {{ color: #c62828; font-weight: bold; font-size: 16px; margin-bottom: 10px; }}
            .error-details {{ color: #333; }}
            .traceback {{ background-color: #263238; color: #aed581; padding: 15px; border-radius: 5px; font-family: 'Courier New', monospace; font-size: 10px; white-space: pre-wrap; overflow-x: auto; margin-top: 15px; }}
            .actions {{ margin-top: 20px; padding-top: 20px; border-top: 1px solid #ddd; }}
            .actions h3 {{ margin-top: 0; }}
            .actions ul {{ margin: 10px 0; padding-left: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>✗ Erro no Processamento: {nome_processamento}</h1>
            </div>
            <div class="content">
                <div class="info-row">
                    <span class="info-label">Camada:</span> {camada}
                </div>
                <div class="info-row">
                    <span class="info-label">Processamento:</span> {nome_processamento}
                </div>
                <div class="info-row">
                    <span class="info-label">Data/Hora:</span> {fim.strftime('%d/%m/%Y %H:%M:%S')}
                </div>
                <div class="info-row">
                    <span class="info-label">Duração até o erro:</span> {duracao}
                </div>

                <div class="error-box">
                    <div class="error-message">Erro Detectado:</div>
                    <div class="error-details">{erro_msg}</div>
                </div>
    """

    if traceback_str:
        html += f"""
                <h3>Detalhes Técnicos do Erro (Traceback):</h3>
                <div class="traceback">{traceback_str}</div>
        """

    html += f"""
                <div class="actions">
                    <h3>Ações Recomendadas:</h3>
                    <ul>
                        <li>Verificar os logs do sistema para mais detalhes</li>
                        <li>Verificar a conectividade com o banco de dados (se aplicável)</li>
                        <li>Verificar se os dados de entrada estão corretos</li>
                        <li>Verificar as configurações do sistema</li>
                        <li>Contatar o suporte técnico se o problema persistir</li>
                    </ul>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    # Corpo em texto plano
    texto = f"""
Erro no Processamento: {nome_processamento}
==========================================

Camada: {camada}
Processamento: {nome_processamento}
Data/Hora: {fim.strftime('%d/%m/%Y %H:%M:%S')}
Duração até o erro: {duracao}

Erro Detectado:
{erro_msg}

"""

    if traceback_str:
        texto += f"\nDetalhes Técnicos do Erro (Traceback):\n{traceback_str}\n"

    texto += """
Ações Recomendadas:
- Verificar os logs do sistema para mais detalhes
- Verificar a conectividade com o banco de dados (se aplicável)
- Verificar se os dados de entrada estão corretos
- Verificar as configurações do sistema
- Contatar o suporte técnico se o problema persistir
"""

    return html, texto


def enviar_email_erro_processamento(config, nome_processamento, camada, inicio, erro_msg, traceback_str=None):
    """
    Envia e-mail de notificação quando há erro em um processamento específico.

    Args:
        config: Dicionário com as configurações (incluindo email)
        nome_processamento: Nome do processamento que falhou
        camada: Camada onde ocorreu o erro (RAW, SILVER, GOLD)
        inicio: Data/hora de início do processamento
        erro_msg: Mensagem de erro
        traceback_str: String com o traceback completo (opcional)

    Returns:
        bool: True se o e-mail foi enviado com sucesso, False caso contrário
    """
    assunto = f"✗ Valoreasy - Projeto Financeiro OMIE - Erro: {nome_processamento} - Camada {camada} em {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
    corpo_html, corpo_texto = criar_corpo_email_erro_processamento(
        nome_processamento, camada, inicio, erro_msg, traceback_str
    )
    return enviar_email(config, assunto, corpo_html, corpo_texto)
