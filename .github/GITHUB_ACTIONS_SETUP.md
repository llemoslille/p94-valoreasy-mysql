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

#### 1. GCP_CREDENTIALS (Opcional, mas recomendado)
- **Nome**: `GCP_CREDENTIALS`
- **Valor**: Conteúdo completo do arquivo JSON de credenciais do GCP
- **Descrição**: Credenciais para acesso ao Google Cloud Storage

#### 2. MYSQL_HOST (Opcional - tem valor padrão)
- **Nome**: `MYSQL_HOST`
- **Valor**: `45.179.90.60` (ou seu host MySQL)
- **Descrição**: Host do servidor MySQL

#### 3. MYSQL_PORT (Opcional - tem valor padrão)
- **Nome**: `MYSQL_PORT`
- **Valor**: `7513` (ou sua porta MySQL)
- **Descrição**: Porta do servidor MySQL

#### 4. MYSQL_USER (Opcional - tem valor padrão)
- **Nome**: `MYSQL_USER`
- **Valor**: `lille` (ou seu usuário MySQL)
- **Descrição**: Usuário do banco de dados MySQL

#### 5. MYSQL_PASSWORD (Opcional - tem valor padrão)
- **Nome**: `MYSQL_PASSWORD`
- **Valor**: Sua senha MySQL
- **Descrição**: Senha do banco de dados MySQL

#### 6. MYSQL_DATABASE (Opcional - tem valor padrão)
- **Nome**: `MYSQL_DATABASE`
- **Valor**: `lille` (ou seu banco de dados)
- **Descrição**: Nome do banco de dados MySQL

> **Nota**: Os secrets do MySQL são opcionais porque o código atual usa valores hardcoded. Recomenda-se configurá-los para maior segurança.

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

1. **Agendado**: Diariamente às 02:00 UTC (pode ser ajustado no arquivo `.github/workflows/run_pipeline.yml`)
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
  - cron: '0 2 * * *'  # Formato: minuto hora dia mês dia-da-semana
```

Exemplos:
- `'0 2 * * *'` - Diariamente às 02:00 UTC
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

2. **Caminhos**: O workflow ajusta automaticamente os caminhos do Windows para Linux no arquivo `config.yaml`.

3. **Dependências**: O workflow instala automaticamente todas as dependências do `requirements.txt`.

4. **Logs**: Em caso de falha, os logs são automaticamente salvos como artifacts para download.
