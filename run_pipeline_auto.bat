@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

:: ============================================
:: Script para executar o Pipeline Valoreasy
:: Versão Automática - Sem Menu
:: ============================================

:: Mudar para o diretório do script
cd /d "%~dp0"

title Pipeline Valoreasy - Execução Automática

:: Verificar e instalar dependências
call :verificar_dependencias

cls
echo.
echo ================================================================
echo   EXECUTANDO PIPELINE COMPLETO (RAW → SILVER → GOLD)
echo   Modo automático - sem interação
echo ================================================================
echo.
echo   Iniciando em 3 segundos...
timeout /t 3 /nobreak >nul 2>&1

cls
echo.
echo ================================================================
echo   Executando Pipeline Completo (RAW → SILVER → GOLD)
echo ================================================================
echo.

set PYTHONPATH=%~dp0;%PYTHONPATH%
python main.py

echo.
echo ================================================================
echo   Execução finalizada!
echo ================================================================
echo.
pause
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
