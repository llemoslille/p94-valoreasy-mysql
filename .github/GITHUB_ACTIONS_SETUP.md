# Configuração do GitHub Actions

Este documento explica como configurar o GitHub Actions para executar o pipeline ETL automaticamente.

## 📋 Pré-requisitos

1. Repositório no GitHub configurado
2. Acesso de administrador ao repositório (para configurar secrets)

## 🔐 Configuração de Secrets

Para que o pipeline funcione corretamente, você precisa configurar os seguintes secrets no GitHub:

### Como adicionar secrets:

1. Acesse o repositório no GitHub
2. Vá em **Settings** → **Secrets and variables** → **Actions**
3. Clique em **New repository secret**
4. Adicione cada um dos secrets abaixo:

### Secrets necessários:

#### 🔐 Secrets Obrigatórios (para config.yaml)

#### 1. GCP_CREDENTIALS (Obrigatório se usar GCP)
- **Nome**: `GCP_CREDENTIALS`
- **Valor**: Conteúdo completo do arquivo JSON de credenciais do GCP (cole todo o conteúdo do arquivo)
- **Descrição**: Credenciais para acesso ao Google Cloud Storage
- **Exemplo**: Cole o conteúdo completo de `lille-422512-a12a0a3c757b.json`

#### 2. SMTP_USER (Obrigatório para envio de emails)
- **Nome**: `SMTP_USER`
- **Valor**: `lilleschoolbr@gmail.com` (ou seu usuário SMTP)
- **Descrição**: Usuário do servidor SMTP

#### 3. SMTP_PASSWORD (Obrigatório para envio de emails)
- **Nome**: `SMTP_PASSWORD`
- **Valor**: `fbquzrktbfbekfal` (ou sua senha de aplicativo)
- **Descrição**: Senha de aplicativo do Gmail (ou senha SMTP)

#### 4. FROM_EMAIL (Obrigatório para envio de emails)
- **Nome**: `FROM_EMAIL`
- **Valor**: `lilleschoolbr@gmail.com` (ou seu email remetente)
- **Descrição**: Email remetente

#### 5. TO_EMAILS (Obrigatório para envio de emails)
- **Nome**: `TO_EMAILS`
- **Valor**: `rubens@lilleschool.com.br,llemos@lilleschool.com.br` (emails separados por vírgula)
- **Descrição**: Lista de emails destinatários separados por vírgula

#### ⚙️ Secrets Opcionais (têm valores padrão)

#### 6. PROJECT_ID
- **Nome**: `PROJECT_ID`
- **Valor padrão**: `lille-422512`
- **Descrição**: ID do projeto GCP

#### 7. PROJECT_NAME
- **Nome**: `PROJECT_NAME`
- **Valor padrão**: `lille`
- **Descrição**: Nome do projeto

#### 8. CLOUD
- **Nome**: `CLOUD`
- **Valor padrão**: `gcp`
- **Descrição**: Provedor de nuvem

#### 9. BUCKET_PROJETO
- **Nome**: `BUCKET_PROJETO`
- **Valor padrão**: `p94_valoreasy`
- **Descrição**: Nome do bucket principal

#### 10. BUCKET_RAW
- **Nome**: `BUCKET_RAW`
- **Valor padrão**: `bronze`
- **Descrição**: Nome da pasta/bucket para dados RAW

#### 11. BUCKET_SILVER
- **Nome**: `BUCKET_SILVER`
- **Valor padrão**: `silver`
- **Descrição**: Nome da pasta/bucket para dados SILVER

#### 12. BUCKET_GOLD
- **Nome**: `BUCKET_GOLD`
- **Valor padrão**: `gold`
- **Descrição**: Nome da pasta/bucket para dados GOLD

#### 13. BUCKET_PROCESSED
- **Nome**: `BUCKET_PROCESSED`
- **Valor padrão**: `processed-data`
- **Descrição**: Nome do bucket para dados processados

#### 14. BUCKET_DW
- **Nome**: `BUCKET_DW`
- **Valor padrão**: `dw-data`
- **Descrição**: Nome do bucket para data warehouse

#### 15. EMAIL_ENABLED
- **Nome**: `EMAIL_ENABLED`
- **Valor padrão**: `true`
- **Descrição**: Habilitar/desabilitar envio de emails

#### 16. SMTP_SERVER
- **Nome**: `SMTP_SERVER`
- **Valor padrão**: `smtp.gmail.com`
- **Descrição**: Servidor SMTP

#### 17. SMTP_PORT
- **Nome**: `SMTP_PORT`
- **Valor padrão**: `587`
- **Descrição**: Porta SMTP

> **Nota**: O workflow gera automaticamente o arquivo `config.yaml` usando esses secrets. Se algum secret opcional não for configurado, será usado o valor padrão.

## 🚀 Como executar o workflow

### Execução Manual

1. Acesse a aba **Actions** no GitHub
2. Selecione o workflow **Executar Pipeline ETL**
3. Clique em **Run workflow**
4. Escolha o modo de execução:
   - **full**: Executa o pipeline completo (RAW → SILVER → GOLD)
   - **raw**: Executa apenas a camada RAW
   - **silver_gold**: Executa apenas as camadas SILVER e GOLD
5. Clique em **Run workflow**

### Execução Automática

O workflow está configurado para executar automaticamente:

1. **Agendado**: Diariamente às 07:30 (horário de Brasília) = 10:30 UTC (pode ser ajustado no arquivo `.github/workflows/run_pipeline.yml`)
2. **Push**: Quando há alterações nos arquivos:
   - `main.py`
   - Arquivos em `src/`
   - Arquivos em `config/`
   - O próprio arquivo de workflow

## 📊 Monitoramento

- Acesse a aba **Actions** para ver o histórico de execuções
- Cada execução mostra logs detalhados de cada etapa
- Em caso de erro, os logs são salvos como artifacts por 7 dias

## ⚙️ Personalização

### Alterar horário de execução agendada

Edite o arquivo `.github/workflows/run_pipeline.yml` e modifique a linha:

```yaml
schedule:
  - cron: '30 10 * * *'  # Formato: minuto hora dia mês dia-da-semana
```

Exemplos:
- `'30 10 * * *'` - Diariamente às 07:30 (horário de Brasília) = 10:30 UTC
- `'0 0 * * 1'` - Toda segunda-feira à meia-noite UTC
- `'0 */6 * * *'` - A cada 6 horas

### Alterar timeout

O timeout padrão é de 60 minutos. Para alterar, modifique:

```yaml
timeout-minutes: 60
```

## 🔍 Troubleshooting

### Erro: "GCP_CREDENTIALS não configurado"
- Configure o secret `GCP_CREDENTIALS` se você usar Google Cloud Storage
- Se não usar GCP, o pipeline ainda pode funcionar para outras partes

### Erro de conexão MySQL
- Verifique se os secrets do MySQL estão configurados corretamente
- Verifique se o servidor MySQL está acessível do GitHub Actions
- Considere usar variáveis de ambiente no código em vez de valores hardcoded

### Timeout
- Aumente o `timeout-minutes` no workflow se o pipeline demorar mais de 60 minutos
- Considere dividir o pipeline em jobs separados

## 📝 Notas Importantes

1. **Credenciais**: O código atual tem credenciais hardcoded. Recomenda-se migrar para usar variáveis de ambiente ou secrets para maior segurança.

2. **Config.yaml**: O workflow gera automaticamente o arquivo `config.yaml` usando os secrets configurados. Não é necessário manter o arquivo no repositório.

3. **Dependências**: O workflow instala automaticamente todas as dependências do `requirements.txt`.

4. **Logs**: Em caso de falha, os logs são automaticamente salvos como artifacts para download.
