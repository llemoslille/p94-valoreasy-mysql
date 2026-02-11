import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime


def enviar_notificacao_sucesso(tempo_execucao, email_destinatario, diagnostico_geral=None):
    """
    Envia email de notificação de sucesso do processo Valoreasy - Projeto Financeiro OMIE 

    Args:
        tempo_execucao (str): Tempo formatado de execução
        email_destinatario (str): Email para enviar notificação
        diagnostico_geral (dict): Dicionário com informações de diagnóstico do ETL
    """

    # Configurações do Gmail
    GMAIL_USER = "lilleschoolbr@gmail.com"
    GMAIL_PASSWORD = "fbqu zrkt bfbe kfal"

    try:
        # Criar mensagem
        msg = MIMEMultipart()
        msg['From'] = GMAIL_USER
        msg['To'] = email_destinatario
        msg['Subject'] = f"✓ Valoreasy - Projeto Financeiro OMIE - Sucesso em {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"

        # Construir seção de diagnóstico
        diagnostico_html = ""
        if diagnostico_geral:
            diagnostico_html = "<h3>📊 Resumo da Execução:</h3><ul>"
            for item, valor in diagnostico_geral.items():
                if valor is not None:
                    diagnostico_html += f"<li><strong>{item}:</strong> {valor}</li>"
            diagnostico_html += "</ul>"

        # Corpo do email
        corpo = f"""
        <html>
            <body style="font-family: Arial, sans-serif;">
                <h2 style="color: green;">✓ ETL Valoreasy - Projeto Financeiro OMIE - Finalizado com Sucesso!</h2>
                <p><strong>Data/Hora:</strong> {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
                <p><strong>Tempo de Execução:</strong> {tempo_execucao}</p>
                <hr>
                <p>O processo ETL de projeto financeiro foi executado com sucesso.</p>
                <p>✅ Dados extraídos, limpos e carregados no BigQuery</p>
                <p>✅ Validações aplicadas (duplicatas removidas, datas corrigidas)</p>
                <p>✅ Tabelas prontas para uso no Power BI</p>
                {diagnostico_html}
                <hr>
                <p><strong>Próximos passos:</strong></p>
                <ul>
                    <li>Acesse o BigQuery Console</li>
                    <li>Verifique o projeto: lille</li>
                    <li>Verifique o dataset: P54_Valoresay</li>
                    <li>Conecte o Power BI às tabelas atualizadas</li>
                </ul>
            </body>
        </html>
        """

        msg.attach(MIMEText(corpo, 'html'))

        # Enviar email
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(GMAIL_USER, GMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()

        print(f"[OK] Email de notificacao enviado para {email_destinatario}")

    except Exception as e:
        print(f"[ERRO] Erro ao enviar email: {str(e)}")


def enviar_notificacao_erro(erro_mensagem, email_destinatario):
    """
    Envia email de notificação de erro do processo Valoreasy - Projeto Financeiro OMIE 

    Args:
        erro_mensagem (str): Mensagem de erro
        email_destinatario (str): Email para enviar notificação
    """

    # Configurações do Gmail
    GMAIL_USER = "lilleschoolbr@gmail.com"
    GMAIL_PASSWORD = "fbqu zrkt bfbe kfal"

    try:
        # Criar mensagem
        msg = MIMEMultipart()
        msg['From'] = GMAIL_USER
        msg['To'] = email_destinatario
        msg['Subject'] = f"✗ Valoreasy - Projeto Financeiro OMIE - Erro em {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"

        # Corpo do email
        corpo = f"""
        <html>
            <body style="font-family: Arial, sans-serif;">
                <h2 style="color: red;">✗ Erro no ETL Valoreasy - Projeto Financeiro OMIE!</h2>
                <p><strong>Data/Hora:</strong> {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
                <p><strong>Erro:</strong></p>
                <pre style="background-color: #f0f0f0; padding: 10px; border-left: 4px solid red;">{erro_mensagem}</pre>
                <hr>
                <p><strong>Ações recomendadas:</strong></p>
                <ul>
                    <li>Verificar se houve erro na execução do ETL</li>
                    <li>Acessar a VM-CLIENTES</li>
                    <li>Diretório: C:\Repositorio\Python\p94-valoreasy\logs</li>
            </body>
        </html>
        """

        msg.attach(MIMEText(corpo, 'html'))

        # Enviar email
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(GMAIL_USER, GMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()

        print(f"[OK] Email de erro enviado para {email_destinatario}")

    except Exception as e:
        print(f"[ERRO] Erro ao enviar email: {str(e)}")
