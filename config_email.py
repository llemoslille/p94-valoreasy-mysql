"""
Configurações de Email para Notificações ETL
============================================

Este arquivo contém as configurações de email para o sistema de notificações
do ETL de Remanejamentos do Grupo Tensor.

Para usar as notificações por email:
1. Configure o EMAIL_DESTINATARIO abaixo
2. Execute o ETL com: python main.py --email "seu@email.com"
3. Ou use os arquivos .bat configurados

As credenciais SMTP já estão configuradas no email_notifier.py
"""

# ============================================================================
# CONFIGURAÇÕES DE EMAIL
# ============================================================================

# Emails que receberão as notificações de sucesso/erro
# Pode ser uma string (um email) ou uma lista (múltiplos emails)
EMAIL_DESTINATARIO = [
    "rubens@lilleschool.com.br",
    "llemos@lilleschool.com.br"  # ALTERE AQUI para o segundo email
]

# ============================================================================
# EXEMPLOS DE USO
# ============================================================================

# Comando para execução manual com notificação:
# python main.py --overwrite --email "seu@email.com"

# Para usar nos arquivos .bat, descomente as linhas com --email nos arquivos:
# - executar_etl.bat
# - executar_etl_auto.bat

# ============================================================================
# CONFIGURAÇÕES SMTP (JÁ CONFIGURADAS)
# ============================================================================

# As configurações SMTP estão em email_notifier.py:
# - Servidor: smtp.gmail.com
# - Porta: 465 (SSL)
# - Usuário: lilleschoolbr@gmail.com
# - Senha: fbqu zrkt bfbe kfal (App Password)

# ============================================================================
# TIPOS DE NOTIFICAÇÃO
# ============================================================================

# SUCESSO:
# - Assunto: "✓ Grupo Tensor - REMANEJAMENTO - Sucesso em DD/MM/YYYY HH:MM:SS"
# - Conteúdo: Resumo da execução, tempo, arquivos gerados, próximos passos

# ERRO:
# - Assunto: "✗ Grupo Tensor - REMANEJAMENTO - Erro em DD/MM/YYYY HH:MM:SS"
# - Conteúdo: Detalhes do erro, ações recomendadas, troubleshooting

# ============================================================================
# TROUBLESHOOTING
# ============================================================================

# Se as notificações não funcionarem:
# 1. Verificar conectividade com internet
# 2. Confirmar que o email destinatário está correto
# 3. Verificar logs para erros de SMTP
# 4. Testar manualmente: python main.py --email "teste@email.com"

print("Configurações de email carregadas.")
if isinstance(EMAIL_DESTINATARIO, list):
    print(
        f"Emails destinatários configurados: {', '.join(EMAIL_DESTINATARIO)}")
else:
    print(f"Email destinatário configurado: {EMAIL_DESTINATARIO}")
print("Para alterar, edite este arquivo: config_email.py")
