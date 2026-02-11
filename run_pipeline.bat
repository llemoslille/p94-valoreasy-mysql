@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

:: ============================================
:: Script para executar o Pipeline Valoreasy
:: ============================================

:: Mudar para o diretório do script
cd /d "%~dp0"

title Pipeline Valoreasy - Executor

:: Verificar e instalar dependências
call :verificar_dependencias

:menu
cls
echo.
echo ================================================================
echo           PIPELINE VALOREASY - MENU DE EXECUÇÃO
echo ================================================================
echo.
echo   [1] Executar Pipeline Completo (RAW → SILVER → GOLD)
echo   [2] Executar apenas Camada RAW
echo   [3] Executar apenas Camadas SILVER e GOLD
echo   [4] Executar Pipeline Completo (Modo Automático - Sem Menu)
echo   [5] Sair
echo.
echo ================================================================
echo.
echo   Executando Pipeline Completo automaticamente em 10 segundos...
echo   Pressione qualquer tecla para ver o menu interativo
echo.

:: Contagem regressiva com possibilidade de interrupção
set contador=10
:contagem
if %contador% leq 0 goto completo_automatico
echo   Iniciando em %contador% segundos... (Pressione qualquer tecla para interromper)
:: Usar choice para detectar tecla pressionada (timeout de 1 segundo)
choice /C YN /N /T 1 /D N /M "" >nul 2>&1
if errorlevel 2 goto menu_interativo
if errorlevel 1 (
    set /a contador-=1
    goto contagem
)
goto menu_interativo

:menu_interativo
cls
echo.
echo ================================================================
echo           PIPELINE VALOREASY - MENU DE EXECUÇÃO
echo ================================================================
echo.
echo   [1] Executar Pipeline Completo (RAW → SILVER → GOLD)
echo   [2] Executar apenas Camada RAW
echo   [3] Executar apenas Camadas SILVER e GOLD
echo   [4] Executar Pipeline Completo (Modo Automático - Sem Menu)
echo   [5] Sair
echo.
echo ================================================================
echo.

set /p opcao="Escolha uma opção (1-5): "

if "%opcao%"=="1" goto completo
if "%opcao%"=="2" goto raw
if "%opcao%"=="3" goto silver_gold
if "%opcao%"=="4" goto completo_automatico
if "%opcao%"=="5" goto sair

echo.
echo [ERRO] Opção inválida! Por favor, escolha uma opção entre 1 e 5.
echo.
pause
goto menu_interativo

:completo_automatico
cls
echo.
echo ================================================================
echo   Executando Pipeline Completo (RAW → SILVER → GOLD)
echo   Modo automático - sem interação do usuário
echo ================================================================
echo.
set PYTHONPATH=%~dp0;%PYTHONPATH%
python main.py
echo.
echo ================================================================
echo   Execução finalizada!
echo ================================================================
echo.
timeout /t 3 >nul
exit

:completo
cls
echo.
echo ================================================================
echo   Executando Pipeline Completo (RAW → SILVER → GOLD)
echo ================================================================
echo.
set PYTHONPATH=%~dp0;%PYTHONPATH%
python main.py
goto fim

:raw
cls
echo.
echo ================================================================
echo   Executando apenas Camada RAW
echo ================================================================
echo.
set PYTHONPATH=%~dp0;%PYTHONPATH%
python main.py raw
goto fim

:silver_gold
cls
echo.
echo ================================================================
echo   Executando Camadas SILVER e GOLD
echo ================================================================
echo.
set PYTHONPATH=%~dp0;%PYTHONPATH%
python main.py silver_gold
goto fim

:fim
echo.
echo ================================================================
echo   Execução finalizada!
echo ================================================================
echo.
set /p continuar="Deseja executar novamente? (S/N): "
if /i "!continuar!"=="S" goto menu
goto sair

:sair
cls
echo.
echo ================================================================
echo   Encerrando...
echo ================================================================
echo.
timeout /t 2 >nul
exit

:: ============================================
:: Função para verificar e instalar dependências
:: ============================================
:verificar_dependencias
echo.
echo ================================================================
echo   Verificando dependências Python...
echo ================================================================
echo.

:: Verificar se Python está instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERRO] Python não encontrado! Por favor, instale o Python primeiro.
    echo.
    pause
    exit /b 1
)

:: Verificar se pip está disponível
python -m pip --version >nul 2>&1
if errorlevel 1 (
    echo [ERRO] pip não encontrado! Por favor, instale o pip primeiro.
    echo.
    pause
    exit /b 1
)

:: Verificar se requirements.txt existe
if not exist "requirements.txt" (
    echo [AVISO] Arquivo requirements.txt não encontrado!
    echo Criando arquivo requirements.txt...
    (
        echo mysql-connector-python^>=8.0.33
        echo PyYAML^>=6.0
        echo pandas^>=2.0.0
        echo google-cloud-storage^>=2.10.0
        echo pyarrow^>=12.0.0
    ) > requirements.txt
)

:: Verificar se as dependências estão instaladas
echo Verificando dependências instaladas...
python -c "import mysql.connector; import yaml; import pandas; from google.cloud import storage; import pyarrow" >nul 2>&1
if errorlevel 1 (
    echo.
    echo [AVISO] Algumas dependências não estão instaladas.
    echo Instalando dependências do arquivo requirements.txt...
    echo.
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt
    if errorlevel 1 (
        echo.
        echo [ERRO] Falha ao instalar dependências!
        echo Por favor, instale manualmente executando:
        echo   pip install -r requirements.txt
        echo.
        pause
        exit /b 1
    )
    echo.
    echo [OK] Dependências instaladas com sucesso!
) else (
    echo [OK] Todas as dependências estão instaladas.
)

echo.
timeout /t 2 >nul
exit /b 0
