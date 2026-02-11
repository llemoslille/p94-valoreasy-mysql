import pandas as pd
import yaml
from google.cloud import storage
from datetime import datetime, timedelta
import os
import numpy as np
from typing import Optional


def carregar_config():
    """
    Carrega as configurações do arquivo config.yaml

    Returns:
        dict: Dicionário com as configurações
    """
    config_path = os.path.join(os.path.dirname(
        __file__), '..', '..', '..', 'config', 'config.yaml')
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def ler_do_gcs(bucket_name, caminho_gcs, credentials_path):
    """
    Lê um arquivo parquet do Google Cloud Storage

    Args:
        bucket_name: Nome do bucket GCS
        caminho_gcs: Caminho completo do arquivo no bucket
        credentials_path: Caminho para o arquivo de credenciais

    Returns:
        pd.DataFrame: DataFrame com os dados lidos ou None em caso de erro
    """
    try:
        # Configurar credenciais
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        # Criar cliente do GCS
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Criar blob
        blob = bucket.blob(caminho_gcs)

        # Baixar para arquivo temporário
        import tempfile
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, 'temp_fato_extrato.parquet')

        blob.download_to_filename(temp_file)

        # Ler o arquivo parquet
        df = pd.read_parquet(temp_file, engine='pyarrow')

        print(
            f"[EXTRATO DIARIO CC] Arquivo lido do GCS: gs://{bucket_name}/{caminho_gcs}")
        print(f"[EXTRATO DIARIO CC] Total de registros lidos: {len(df)}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return df

    except Exception as e:
        print(f"[EXTRATO DIARIO CC] Erro ao ler do GCS: {e}")
        return None


def salvar_no_gcs(df, bucket_name, caminho_gcs, credentials_path, nome_arquivo):
    """
    Salva o DataFrame no Google Cloud Storage em formato parquet

    Args:
        df: DataFrame a ser salvo
        bucket_name: Nome do bucket GCS
        caminho_gcs: Caminho dentro do bucket
        credentials_path: Caminho para o arquivo de credenciais
        nome_arquivo: Nome do arquivo a ser salvo

    Returns:
        str: Caminho completo do arquivo salvo ou None em caso de erro
    """
    try:
        # Configurar credenciais
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        # Criar cliente do GCS
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Criar caminho completo
        caminho_completo = f"{caminho_gcs}/{nome_arquivo}"

        # Salvar DataFrame temporariamente como parquet
        import tempfile
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, nome_arquivo)

        df.to_parquet(temp_file, index=False, engine='pyarrow')

        # Upload para o GCS
        blob = bucket.blob(caminho_completo)
        blob.upload_from_filename(temp_file)

        print(
            f"[EXTRATO DIARIO CC] Arquivo salvo no GCS: gs://{bucket_name}/{caminho_completo}")
        print(f"[EXTRATO DIARIO CC] Total de registros salvos: {len(df)}")

        # Remover arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)

        return caminho_completo

    except Exception as e:
        print(f"[EXTRATO DIARIO CC] Erro ao salvar no GCS: {e}")
        return None


def parse_date_safe(date_str: str) -> Optional[datetime]:
    """
    Converte string de data no formato '%d/%m/%Y' para datetime.
    Retorna None se a conversão falhar.

    Args:
        date_str: String de data no formato '%d/%m/%Y'

    Returns:
        datetime ou None
    """
    if pd.isna(date_str) or date_str == '' or date_str == 'nan':
        return None
    try:
        return pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce')
    except:
        return None


def safe_cast_float(value) -> Optional[float]:
    """
    Converte valor para float de forma segura.

    Args:
        value: Valor a ser convertido

    Returns:
        float ou None
    """
    if pd.isna(value) or value == '' or value == 'NA' or value == 'nan':
        return None
    try:
        return float(value)
    except:
        return None


def safe_cast_int(value) -> Optional[int]:
    """
    Converte valor para int de forma segura.

    Args:
        value: Valor a ser convertido

    Returns:
        int ou None
    """
    if pd.isna(value) or value == '' or value == 'NA' or value == 'nan':
        return None
    try:
        return int(float(value))
    except:
        return None


def safe_cast_string(value) -> Optional[str]:
    """
    Converte valor para string de forma segura, aplicando UPPER.

    Args:
        value: Valor a ser convertido

    Returns:
        str ou None
    """
    if pd.isna(value) or value == '' or value == 'nan':
        return None
    try:
        return str(value).upper().strip()
    except:
        return None


def transformar_extrato_diario(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma o DataFrame de extrato seguindo a lógica da query SQL.
    Versão otimizada para melhor performance.

    Args:
        df: DataFrame com os dados do extrato

    Returns:
        pd.DataFrame: DataFrame transformado
    """
    print(f"[EXTRATO DIARIO CC] Iniciando transformação dos dados...")
    print(f"[EXTRATO DIARIO CC] Registros de entrada: {len(df)}")

    # ========== FILTROS DE TESTE (COMENTADO PARA PEGAR TODAS AS EMPRESAS) ==========
    # O bloco abaixo realizava filtros para uso em desenvolvimento/teste,
    # limitando os dados a uma empresa e descrição específica, em data fixa.
    # Agora está apenas comentado para produção, permitindo processar todas as empresas.
    #
    # print("[EXTRATO DIARIO CC] Aplicando filtros de teste...")
    # mask_empresa = df['empresa_id'].astype(str) == 'P940022'
    # mask_descricao = df['cdescricao'].astype(str).str.contains(
    #     'Bradesco - Mobilidade', case=False, na=False)
    # mask_periodo = df['dperiodoinicial'].astype(str) == '01/01/2026'
    #
    # df = df[mask_empresa & mask_descricao & mask_periodo].copy()
    # print(f"[EXTRATO DIARIO CC] Após filtros de teste: {len(df)} registros")
    # ============================================================

    # ========== FILTRO DE PERÍODO: COMENTADO PARA PROCESSAR TODOS OS REGISTROS ==========
    # O bloco abaixo filtra registros a partir de 01/01/2025 até hoje.
    # Está comentado para processar TODOS os registros disponíveis.
    #
    # print("[EXTRATO DIARIO CC] Aplicando filtro de período: a partir de 01/01/2025 até hoje...")
    #
    # # Converter dperiodoinicial para datetime para comparação
    # df['dt_periodo_inicial_temp'] = pd.to_datetime(
    #     df['dperiodoinicial'], format='%d/%m/%Y', errors='coerce')
    #
    # # Data mínima: 01/01/2025
    # data_minima = pd.to_datetime('01/01/2025', format='%d/%m/%Y')
    # # Data máxima: hoje
    # data_maxima = pd.to_datetime(datetime.now().date())
    #
    # # Filtrar apenas registros com período inicial >= 01/01/2025 e <= hoje
    # mask_periodo_valido = (
    #     (df['dt_periodo_inicial_temp'] >= data_minima) &
    #     (df['dt_periodo_inicial_temp'] <= data_maxima)
    # )
    #
    # df = df[mask_periodo_valido].copy()
    #
    # # Remover coluna temporária
    # df = df.drop(columns=['dt_periodo_inicial_temp'], errors='ignore')
    #
    # print(f"[EXTRATO DIARIO CC] Após filtro de período (01/01/2025 até hoje): {len(df)} registros")
    # ========================================================================================

    print(
        f"[EXTRATO DIARIO CC] Processando TODOS os registros disponíveis: {len(df)} registros")

    # Verificar se a coluna nsaldoprovisorio existe, caso contrário criar como None
    if 'nsaldoprovisorio' not in df.columns:
        print("[EXTRATO DIARIO CC] AVISO: Coluna 'nsaldoprovisorio' não encontrada. Criando como None.")
        df['nsaldoprovisorio'] = None

    # ETAPA 1: EXTRATO_PERIODO - Identificar TODOS os períodos únicos por empresa
    print("[EXTRATO DIARIO CC] Etapa 1: Identificando TODOS os períodos por empresa...")

    # Converter datas de forma vetorizada
    df['dt_periodo_inicial'] = pd.to_datetime(
        df['dperiodoinicial'], format='%d/%m/%Y', errors='coerce')
    df['dt_periodo_final'] = pd.to_datetime(
        df['dperiodofinal'], format='%d/%m/%Y', errors='coerce')

    # Identificar TODOS os períodos únicos por empresa
    # Para cada combinação empresa/período_inicial, pegar o período_final máximo
    # Isso é usado apenas para gerar os dias do período, não para filtrar registros
    extrato_periodo_max = df.groupby(['empresa_id', 'dt_periodo_inicial'], as_index=False).agg({
        'dt_periodo_final': 'max'
    })
    extrato_periodo_max['cod_empresa'] = extrato_periodo_max['empresa_id'].astype(
        str)

    print(
        f"[EXTRATO DIARIO CC] Total de períodos únicos encontrados: {len(extrato_periodo_max)}")
    print(f"[EXTRATO DIARIO CC] Períodos por empresa (primeiros 10):")
    for idx, row in extrato_periodo_max.head(10).iterrows():
        print(
            f"  - {row['cod_empresa']}: {row['dt_periodo_inicial']} a {row['dt_periodo_final']}")

    # NÃO fazer merge para filtrar - processar TODOS os registros diretamente
    # O extrato_periodo_max será usado apenas para gerar os dias do período
    print(
        f"[EXTRATO DIARIO CC] Processando TODOS os {len(df)} registros (sem filtro de período)")

    # ETAPA 3: EXTRATO_CC_STG1 - Transformar dados do extrato
    print("[EXTRATO DIARIO CC] Etapa 3: Transformando dados do extrato...")

    # Filtrar registros onde cdescliente != 'SALDO' (vetorizado)
    mask_nao_saldo = df['cdescliente'].astype(str).str.upper() != 'SALDO'
    df_filtrado = df[mask_nao_saldo].copy()

    # Preparar dados transformados de forma vetorizada
    extrato_cc_stg1 = pd.DataFrame(index=df_filtrado.index)

    # Converter datas de forma vetorizada
    dt_periodo_inicial = pd.to_datetime(
        df_filtrado['dperiodoinicial'], format='%d/%m/%Y', errors='coerce')
    dt_periodo_final = pd.to_datetime(
        df_filtrado['dperiodofinal'], format='%d/%m/%Y', errors='coerce')

    extrato_cc_stg1['cod_empresa'] = df_filtrado['empresa_id'].astype(str)
    extrato_cc_stg1['fk_cc'] = df_filtrado['ncodcc'].astype(str)
    extrato_cc_stg1['tipo_cc'] = df_filtrado['ccodtipo'].astype(str)
    extrato_cc_stg1['de_cc'] = df_filtrado['cdescricao'].astype(
        str).str.upper().str.strip()
    extrato_cc_stg1['de_cc'] = extrato_cc_stg1['de_cc'].replace(
        ['NAN', 'NONE', ''], None)
    extrato_cc_stg1['de_cliente'] = df_filtrado['cdescliente'].astype(
        str).str.upper().str.strip()
    extrato_cc_stg1['de_cliente'] = extrato_cc_stg1['de_cliente'].replace(
        ['NAN', 'NONE', ''], None)
    extrato_cc_stg1['dt_periodo_inicial'] = dt_periodo_inicial
    extrato_cc_stg1['dt_periodo_final'] = dt_periodo_final

    # Calcular dt_lancamento de forma vetorizada
    dt_lancamento = pd.to_datetime(
        df_filtrado['ddatalancamento'], format='%d/%m/%Y', errors='coerce')
    mask_saldo_anterior = extrato_cc_stg1['de_cliente'] == 'SALDO ANTERIOR'
    extrato_cc_stg1['dt_lancamento'] = dt_lancamento
    extrato_cc_stg1.loc[mask_saldo_anterior,
                        'dt_lancamento'] = extrato_cc_stg1.loc[mask_saldo_anterior, 'dt_periodo_inicial']

    # Criar new_cdescliente: SALDO ANTERIOR -> SALDO INICIO PERÍODO
    print("[EXTRATO DIARIO CC] Criando coluna new_cdescliente...")
    extrato_cc_stg1['new_cdescliente'] = extrato_cc_stg1['de_cliente']
    extrato_cc_stg1.loc[mask_saldo_anterior,
                        'new_cdescliente'] = 'SALDO INICIO PERÍODO'

    # Criar new_ddatalancamento: SALDO ANTERIOR -> dperiodoinicial, caso contrário -> ddatalancamento
    print("[EXTRATO DIARIO CC] Criando coluna new_ddatalancamento...")
    extrato_cc_stg1['new_ddatalancamento'] = extrato_cc_stg1['dt_lancamento']
    extrato_cc_stg1.loc[mask_saldo_anterior,
                        'new_ddatalancamento'] = extrato_cc_stg1.loc[mask_saldo_anterior, 'dt_periodo_inicial']

    # Converter valores numéricos de forma vetorizada
    extrato_cc_stg1['vlr_saldo'] = pd.to_numeric(
        df_filtrado['nsaldo'], errors='coerce')
    extrato_cc_stg1['vlr_saldo_anterior'] = pd.to_numeric(
        df_filtrado['nsaldoanterior'], errors='coerce')
    extrato_cc_stg1['vlr_documento'] = pd.to_numeric(
        df_filtrado['nvalordocumento'], errors='coerce')
    extrato_cc_stg1['de_situacao'] = df_filtrado['csituacao'].astype(
        str).str.upper().str.strip()
    extrato_cc_stg1['de_situacao'] = extrato_cc_stg1['de_situacao'].replace(
        ['NAN', 'NONE', ''], None)

    # Código lançamento relacionamento (vetorizado) - manter como string para evitar overflow
    cod_lanc_rel_str = df_filtrado['ncodlancrelac'].astype(str).str.strip()
    cod_lanc_rel_str = cod_lanc_rel_str.replace(
        ['', 'NA', 'nan', 'None', '<NA>', 'NaN'], None)
    # Converter para float primeiro para preservar valores grandes, depois para string
    cod_lanc_rel_float = pd.to_numeric(cod_lanc_rel_str, errors='coerce')
    # Converter para string preservando valores None - usar método vetorizado
    mask_notna = cod_lanc_rel_float.notna()
    extrato_cc_stg1['codigo_lancamento_relacionamento'] = None
    extrato_cc_stg1.loc[mask_notna, 'codigo_lancamento_relacionamento'] = (
        cod_lanc_rel_float[mask_notna].astype('Int64').astype(str)
    )
    extrato_cc_stg1['codigo_lancamento_relacionamento'] = extrato_cc_stg1['codigo_lancamento_relacionamento'].replace([
                                                                                                                      'nan', 'None', '<NA>', 'NaN', '<NA>'], None)

    # Código lançamento (vetorizado) - manter como string para evitar overflow
    cod_lanc_str = df_filtrado['ncodlancamento'].astype(str).str.strip()
    cod_lanc_str = cod_lanc_str.replace(
        ['', 'NA', 'nan', 'None', '<NA>', 'NaN'], None)
    # Converter para float primeiro para preservar valores grandes, depois para string
    cod_lanc_float = pd.to_numeric(cod_lanc_str, errors='coerce')
    # Converter para string preservando valores None - usar método vetorizado
    mask_notna = cod_lanc_float.notna()
    extrato_cc_stg1['codigo_lancamento'] = None
    extrato_cc_stg1.loc[mask_notna, 'codigo_lancamento'] = (
        cod_lanc_float[mask_notna].astype('Int64').astype(str)
    )
    extrato_cc_stg1['codigo_lancamento'] = extrato_cc_stg1['codigo_lancamento'].replace(
        ['nan', 'None', '<NA>', 'NaN', '<NA>'], None)

    extrato_cc_stg1['de_observacao'] = df_filtrado['cobservacoes'].astype(
        str).str.upper().str.strip()
    extrato_cc_stg1['de_observacao'] = extrato_cc_stg1['de_observacao'].replace([
                                                                                'NAN', 'NONE', ''], None)

    # Adicionar coluna nsaldoprovisorio
    extrato_cc_stg1['nsaldoprovisorio'] = pd.to_numeric(
        df_filtrado['nsaldoprovisorio'], errors='coerce')

    # Adicionar campos cnatureza e corigem
    if 'cnatureza' in df_filtrado.columns:
        extrato_cc_stg1['fl_natureza'] = df_filtrado['cnatureza'].astype(
            str).str.upper().str.strip()
        extrato_cc_stg1['fl_natureza'] = extrato_cc_stg1['fl_natureza'].replace(
            ['NAN', 'NONE', ''], None)
    else:
        extrato_cc_stg1['fl_natureza'] = None

    if 'corigem' in df_filtrado.columns:
        extrato_cc_stg1['de_origem'] = df_filtrado['corigem'].astype(
            str).str.upper().str.strip()
        extrato_cc_stg1['de_origem'] = extrato_cc_stg1['de_origem'].replace(
            ['NAN', 'NONE', ''], None)
    else:
        extrato_cc_stg1['de_origem'] = None

    # Adicionar campo cdatainclusao
    if 'cdatainclusao' in df_filtrado.columns:
        # Tentar múltiplos formatos de data
        extrato_cc_stg1['dt_inclusao'] = pd.to_datetime(
            df_filtrado['cdatainclusao'], format='%d/%m/%Y', errors='coerce')
        # Se não funcionar com o formato específico, tentar inferir automaticamente
        mask_na = extrato_cc_stg1['dt_inclusao'].isna()
        if mask_na.any():
            extrato_cc_stg1.loc[mask_na, 'dt_inclusao'] = pd.to_datetime(
                df_filtrado.loc[mask_na, 'cdatainclusao'], errors='coerce', infer_datetime_format=True)
    else:
        extrato_cc_stg1['dt_inclusao'] = None

    # Adicionar campo ccodcategoria
    if 'ccodcategoria' in df_filtrado.columns:
        extrato_cc_stg1['cod_categoria'] = df_filtrado['ccodcategoria'].astype(
            str).str.upper().str.strip()
        extrato_cc_stg1['cod_categoria'] = extrato_cc_stg1['cod_categoria'].replace(
            ['NAN', 'NONE', ''], None)
    else:
        extrato_cc_stg1['cod_categoria'] = None

    # Calcular num_linha_dia
    print("[EXTRATO DIARIO CC] Etapa 3.1: Calculando num_linha_dia...")

    # SALDO ANTERIOR deve ter num_linha_dia = 0
    mask_saldo_anterior = extrato_cc_stg1['de_cliente'] == 'SALDO ANTERIOR'
    extrato_cc_stg1['num_linha_dia'] = 0
    extrato_cc_stg1.loc[mask_saldo_anterior, 'num_linha_dia'] = 0

    # Para registros que não são 'SALDO ANTERIOR', calcular ROW_NUMBER sequencial
    mask_nao_saldo_anterior = extrato_cc_stg1['de_cliente'] != 'SALDO ANTERIOR'

    if mask_nao_saldo_anterior.any():
        # Ordenar conforme especificado: empresa → conta corrente → período inicial → data lançamento → código lançamento
        df_ordenado = extrato_cc_stg1[mask_nao_saldo_anterior].copy()

        # Converter código lançamento para numérico (usar Int64 para suportar valores grandes)
        cod_lanc_numeric = pd.to_numeric(
            df_ordenado['codigo_lancamento'], errors='coerce')
        # Usar Int64 que suporta valores maiores que int32
        df_ordenado['cod_lanc_int'] = cod_lanc_numeric.fillna(
            0).astype('Int64')

        # Calcular num_linha_dia: sequência ordenando do menor codigo_lancamento até o maior
        # Agrupamento: fk_cc, dt_lancamento (reinicia contagem para cada combinação conta corrente + data)
        df_ordenado['dt_lancamento_date'] = df_ordenado['dt_lancamento'].dt.date

        # Ordenar: conta → data → codigo_lancamento (ASC: menor para maior)
        df_ordenado = df_ordenado.sort_values(
            by=['fk_cc', 'dt_lancamento_date', 'cod_lanc_int'],
            # codigo_lancamento em ordem crescente
            ascending=[True, True, True],
            na_position='last'  # Valores NULL vão para o final
        )

        # Criar sequência num_linha_dia: menor código = 1, maior código = maior número
        # Reinicia a numeração para cada combinação de fk_cc + dt_lancamento
        df_ordenado['num_linha_dia'] = df_ordenado.groupby(
            ['fk_cc', 'dt_lancamento_date']
        ).cumcount() + 1

        print("[EXTRATO DIARIO CC] num_linha_dia calculado: menor codigo_lancamento = 1, maior codigo_lancamento = maior num_linha_dia")
        print("[EXTRATO DIARIO CC] Agrupamento: fk_cc + dt_lancamento (reinicia numeração para cada combinação)")

        # Validação: verificar se a numeração está correta (não reiniciando incorretamente)
        print("[EXTRATO DIARIO CC] Validando numeração do num_linha_dia...")
        grupos_validacao = df_ordenado.groupby(
            ['fk_cc', 'dt_lancamento_date'])
        problemas_encontrados = []
        # Validar primeiros 10 grupos
        for (cc, data), grupo in list(grupos_validacao)[:10]:
            grupo_ordenado = grupo.sort_values(
                'cod_lanc_int', ascending=True, na_position='last')
            num_linhas = grupo_ordenado['num_linha_dia'].tolist()
            cod_lancamentos = grupo_ordenado['cod_lanc_int'].tolist()

            # Verificar se a sequência está correta (1, 2, 3, ...)
            sequencia_esperada = list(range(1, len(num_linhas) + 1))
            if num_linhas != sequencia_esperada:
                problemas_encontrados.append({
                    'grupo': f"{cc}|{data}",
                    'num_linhas': num_linhas,
                    'sequencia_esperada': sequencia_esperada,
                    'cod_lancamentos': cod_lancamentos
                })

        if problemas_encontrados:
            print(
                f"  ✗ ERRO: Encontrados {len(problemas_encontrados)} grupos com numeração incorreta:")
            # Mostrar apenas os 3 primeiros
            for prob in problemas_encontrados[:3]:
                print(f"    Grupo: {prob['grupo']}")
                print(f"    Esperado: {prob['sequencia_esperada']}")
                print(f"    Encontrado: {prob['num_linhas']}")
                print(f"    Códigos: {prob['cod_lancamentos']}")
        else:
            print("  ✓ OK: Numeração está correta em todos os grupos validados")

        # Atualizar valores no DataFrame original usando índice para garantir correspondência correta
        # df_ordenado mantém os índices originais de extrato_cc_stg1[mask_nao_saldo_anterior]
        # então podemos atualizar diretamente pelos índices
        extrato_cc_stg1.loc[df_ordenado.index,
                            'num_linha_dia'] = df_ordenado['num_linha_dia']

    # ETAPA 4: EXTRATO_CC_STG1_FILTRADO - Usar todos os registros (sem filtro de período máximo)
    print("[EXTRATO DIARIO CC] Etapa 4: Processando todos os registros (sem filtro de período máximo)...")
    # Não fazer merge - usar todos os registros diretamente
    extrato_cc_stg1_filtrado = extrato_cc_stg1.copy()

    print(
        f"[EXTRATO DIARIO CC] Total de registros a processar: {len(extrato_cc_stg1_filtrado)}")

    # ETAPA 5: ULTIMA_LINHA_POR_DIA - Identificar última linha por dia
    print("[EXTRATO DIARIO CC] Etapa 5: Identificando última linha por dia...")
    extrato_cc_stg1_filtrado['dt_lancamento_date'] = extrato_cc_stg1_filtrado['dt_lancamento'].dt.date

    # Calcular max_num_linha_dia por grupo (vetorizado) - agrupamento: conta e data
    extrato_cc_stg1_filtrado['max_num_linha_dia'] = extrato_cc_stg1_filtrado.groupby(
        ['fk_cc', 'dt_lancamento_date']
    )['num_linha_dia'].transform('max')

    # Calcular rn_ultima (ROW_NUMBER ordenado por num_linha_dia DESC)
    extrato_cc_stg1_filtrado = extrato_cc_stg1_filtrado.sort_values(
        by=['fk_cc', 'dt_lancamento_date', 'num_linha_dia'],
        ascending=[True, True, False]
    )
    extrato_cc_stg1_filtrado['rn_ultima'] = extrato_cc_stg1_filtrado.groupby(
        ['fk_cc', 'dt_lancamento_date']
    ).cumcount() + 1

    # ETAPA 6: ULTIMA_LINHA_POR_DIA_FINAL - Selecionar apenas última linha
    print("[EXTRATO DIARIO CC] Etapa 6: Selecionando última linha por dia...")
    ultima_linha_por_dia_final = extrato_cc_stg1_filtrado[
        (extrato_cc_stg1_filtrado['max_num_linha_dia'] > 0) &
        (extrato_cc_stg1_filtrado['rn_ultima'] == 1)
    ].copy()
    ultima_linha_por_dia_final['dt_lancamento'] = ultima_linha_por_dia_final['dt_lancamento'].dt.date

    # ETAPA 7: DIAS_DO_PERIODO - Gerar todos os dias do período por empresa E conta corrente
    print("[EXTRATO DIARIO CC] Etapa 7: Gerando todos os dias do período por conta corrente...")

    # Primeiro, identificar TODAS as contas correntes únicas (antes do filtro de período máximo)
    # para garantir que todas as contas apareçam, mesmo que não tenham transações no período máximo
    todas_contas = extrato_cc_stg1[[
        'cod_empresa', 'fk_cc', 'tipo_cc', 'de_cc'
    ]].drop_duplicates()

    print(
        f"[EXTRATO DIARIO CC] Total de contas correntes únicas encontradas: {len(todas_contas)}")
    if len(todas_contas) > 0:
        print(f"[EXTRATO DIARIO CC] Contas encontradas (primeiras 10):")
        for idx, row in todas_contas.head(10).iterrows():
            print(
                f"  - {row['cod_empresa']} | {row['fk_cc']} | {row['de_cc']}")

    # Fazer cross join com os períodos máximos para garantir que todas as contas tenham seus períodos
    contas_por_periodo = todas_contas.merge(
        extrato_periodo_max,
        on='cod_empresa',
        how='inner'
    )

    print(
        f"[EXTRATO DIARIO CC] Total de combinações conta/período: {len(contas_por_periodo)}")

    dias_do_periodo_list = []

    for _, row in contas_por_periodo.iterrows():
        dt_inicial = row['dt_periodo_inicial']
        dt_final = row['dt_periodo_final']

        if pd.notna(dt_inicial) and pd.notna(dt_final):
            # Gerar range de datas de forma mais eficiente
            num_dias = (dt_final - dt_inicial).days + 1
            datas = pd.date_range(start=dt_inicial, periods=num_dias, freq='D')

            dias_do_periodo_list.extend([{
                'cod_empresa': row['cod_empresa'],
                'dt_periodo_inicial': dt_inicial,
                'dt_periodo_final': dt_final,
                'fk_cc': row['fk_cc'],
                'tipo_cc': row['tipo_cc'],
                'de_cc': row['de_cc'],
                'dt_dia': dt.date()
            } for dt in datas])

    dias_do_periodo = pd.DataFrame(dias_do_periodo_list)
    if len(dias_do_periodo) > 0:
        dias_do_periodo['dt_dia'] = pd.to_datetime(
            dias_do_periodo['dt_dia']).dt.date

    # ETAPA 8: SALDO_DIA_BASE - Juntar dias com saldos
    print("[EXTRATO DIARIO CC] Etapa 8: Juntando dias com saldos...")
    if len(dias_do_periodo) > 0:
        # Fazer merge incluindo fk_cc para garantir que cada conta corrente tenha seus dias
        saldo_dia_base = dias_do_periodo.merge(
            ultima_linha_por_dia_final,
            left_on=['cod_empresa', 'dt_periodo_inicial', 'fk_cc', 'dt_dia'],
            right_on=['cod_empresa', 'dt_periodo_inicial',
                      'fk_cc', 'dt_lancamento'],
            how='left',
            suffixes=('', '_y')
        )
        saldo_dia_base['dt_lancamento'] = saldo_dia_base['dt_dia']

        # Remover colunas duplicadas do merge, mas preservar campos importantes
        colunas_remover = ['dt_dia']
        # Remover apenas colunas duplicadas que não são necessárias
        for col in saldo_dia_base.columns:
            if col.endswith('_y') and col.replace('_y', '') in saldo_dia_base.columns:
                # Se a coluna original existe, usar o valor do merge se o original estiver vazio
                col_original = col.replace('_y', '')
                mask_na = saldo_dia_base[col_original].isna()
                if mask_na.any():
                    saldo_dia_base.loc[mask_na,
                                       col_original] = saldo_dia_base.loc[mask_na, col]
                colunas_remover.append(col)

        saldo_dia_base = saldo_dia_base.drop(
            columns=colunas_remover, errors='ignore')
    else:
        saldo_dia_base = pd.DataFrame(columns=[
            'dt_lancamento', 'cod_empresa', 'dt_periodo_inicial', 'dt_periodo_final',
            'fk_cc', 'tipo_cc', 'de_cc', 'vlr_saldo',
            'vlr_saldo_anterior', 'vlr_documento', 'de_observacao',
            'fl_natureza', 'de_origem', 'dt_inclusao', 'cod_categoria'
        ])

    # ETAPA 9: SALDO_DIA_POR_DIA - Preencher valores faltantes com forward fill (otimizado)
    print("[EXTRATO DIARIO CC] Etapa 9: Preenchendo valores faltantes...")

    # Buscar saldo anterior para cada empresa/conta corrente/período (vetorizado)
    mask_saldo_anterior = extrato_cc_stg1_filtrado['de_cliente'] == 'SALDO ANTERIOR'
    saldo_anterior_df = extrato_cc_stg1_filtrado[mask_saldo_anterior][
        ['cod_empresa', 'fk_cc', 'dt_periodo_inicial',
            'vlr_saldo', 'vlr_saldo_anterior']
    ].copy()
    saldo_anterior_df['vlr_saldo'] = saldo_anterior_df['vlr_saldo'].fillna(0.0)
    saldo_anterior_df['vlr_saldo_anterior'] = saldo_anterior_df['vlr_saldo_anterior'].fillna(
        0.0)

    if len(saldo_dia_base) > 0:
        # Ordenar por empresa, conta corrente, período e data
        saldo_dia_base = saldo_dia_base.sort_values(
            by=['cod_empresa', 'fk_cc', 'dt_periodo_inicial', 'dt_lancamento']
        )

        # Aplicar forward fill por grupo (usando ffill que é mais eficiente)
        # Agrupar por empresa, conta corrente e período
        grupo_cols = ['cod_empresa', 'fk_cc', 'dt_periodo_inicial']

        # Campos que devem ser preenchidos com forward fill (copiados da última transação)
        campos_ffill = ['tipo_cc', 'de_cc', 'de_observacao', 'de_situacao',
                        'codigo_lancamento_relacionamento', 'codigo_lancamento', 'nsaldoprovisorio',
                        'new_cdescliente', 'new_ddatalancamento', 'fl_natureza', 'de_origem',
                        'dt_inclusao', 'cod_categoria']

        for col in campos_ffill:
            if col in saldo_dia_base.columns:
                saldo_dia_base[col] = saldo_dia_base.groupby(grupo_cols)[
                    col].ffill()

        # Para vlr_saldo e vlr_saldo_anterior, usar forward fill e depois preencher com saldo anterior
        for col in ['vlr_saldo', 'vlr_saldo_anterior']:
            saldo_dia_base[col] = saldo_dia_base.groupby(grupo_cols)[
                col].ffill()

            # Preencher valores ainda faltantes com saldo anterior (merge é mais eficiente que loop)
            # Usar saldo_anterior_df que já foi criado acima
            saldo_dia_base = saldo_dia_base.merge(
                saldo_anterior_df[['cod_empresa', 'fk_cc', 'dt_periodo_inicial', col]].rename(
                    columns={col: f'{col}_default'}),
                on=['cod_empresa', 'fk_cc', 'dt_periodo_inicial'],
                how='left'
            )
            mask_na = saldo_dia_base[col].isna()
            saldo_dia_base.loc[mask_na, col] = saldo_dia_base.loc[mask_na,
                                                                  f'{col}_default'].fillna(0.0)
            saldo_dia_base = saldo_dia_base.drop(
                columns=[f'{col}_default'], errors='ignore')

        # Preencher vlr_documento com 0.0 se None (campo específico do título, não deve ser copiado)
        saldo_dia_base['vlr_documento'] = saldo_dia_base['vlr_documento'].fillna(
            0.0)

    # ETAPA 10: SALDO_DIA_FINAL - Formatar dados finais
    print("[EXTRATO DIARIO CC] Etapa 10: Formatando dados finais...")
    if len(saldo_dia_base) > 0:
        saldo_dia_final = saldo_dia_base.copy()

        # Garantir que cod_empresa, fk_cc, tipo_cc e de_cc estejam preenchidos
        # (devem vir do dias_do_periodo, mas vamos garantir)
        grupo_cols = ['cod_empresa', 'fk_cc', 'dt_periodo_inicial']
        for col in ['cod_empresa', 'fk_cc', 'tipo_cc', 'de_cc']:
            if col in saldo_dia_final.columns:
                saldo_dia_final[col] = saldo_dia_final.groupby(grupo_cols)[
                    col].ffill().bfill()

        # Garantir que new_cdescliente e new_ddatalancamento existam
        if 'new_cdescliente' not in saldo_dia_final.columns:
            saldo_dia_final['new_cdescliente'] = None
        if 'new_ddatalancamento' not in saldo_dia_final.columns:
            saldo_dia_final['new_ddatalancamento'] = None

        # Identificar dias com movimento (que têm dados na última linha do dia)
        # e dias sem movimento (que não têm dados)
        mask_com_movimento = saldo_dia_final['vlr_saldo'].notna()
        mask_sem_movimento = saldo_dia_final['vlr_saldo'].isna()

        print(
            f"[EXTRATO DIARIO CC] Dias com movimento: {mask_com_movimento.sum()}")
        print(
            f"[EXTRATO DIARIO CC] Dias sem movimento: {mask_sem_movimento.sum()}")

        # Para dias COM movimento: usar a última linha do dia e marcar como SALDO FINAL DIA
        saldo_dia_final.loc[mask_com_movimento,
                            'new_cdescliente'] = 'SALDO FINAL DIA'
        saldo_dia_final.loc[mask_com_movimento,
                            'new_ddatalancamento'] = saldo_dia_final.loc[mask_com_movimento, 'dt_lancamento']

        # Para dias SEM movimento: copiar linha de SALDO ANTERIOR mudando a data
        if mask_sem_movimento.any():
            # Buscar linha de SALDO ANTERIOR (SALDO INICIO PERÍODO) por conta corrente
            saldo_inicio_df = extrato_cc_stg1_filtrado[
                extrato_cc_stg1_filtrado['new_cdescliente'] == 'SALDO INICIO PERÍODO'
            ].copy()

            if len(saldo_inicio_df) > 0:
                # Para cada dia sem movimento, buscar o saldo inicial correspondente à conta corrente
                for idx in saldo_dia_final[mask_sem_movimento].index:
                    fk_cc_dia = saldo_dia_final.loc[idx, 'fk_cc']
                    saldo_inicio = saldo_inicio_df[saldo_inicio_df['fk_cc'] == fk_cc_dia]

                    if len(saldo_inicio) > 0:
                        saldo_inicio = saldo_inicio.iloc[0]
                        # Copiar dados do SALDO INICIO PERÍODO para dias sem movimento
                        saldo_dia_final.loc[idx,
                                            'new_cdescliente'] = 'SALDO FINAL DIA'
                        saldo_dia_final.loc[idx,
                                            'new_ddatalancamento'] = saldo_dia_final.loc[idx, 'dt_lancamento']
                        # Copiar TODOS os campos importantes do saldo anterior
                        saldo_dia_final.loc[idx, 'cod_empresa'] = saldo_inicio.get(
                            'cod_empresa')
                        saldo_dia_final.loc[idx,
                                            'fk_cc'] = saldo_inicio.get('fk_cc')
                        saldo_dia_final.loc[idx, 'tipo_cc'] = saldo_inicio.get(
                            'tipo_cc')
                        saldo_dia_final.loc[idx,
                                            'de_cc'] = saldo_inicio.get('de_cc')
                        saldo_dia_final.loc[idx, 'dt_periodo_inicial'] = saldo_inicio.get(
                            'dt_periodo_inicial')
                        saldo_dia_final.loc[idx, 'dt_periodo_final'] = saldo_inicio.get(
                            'dt_periodo_final')
                        saldo_dia_final.loc[idx, 'vlr_saldo'] = saldo_inicio.get(
                            'vlr_saldo', 0.0)
                        saldo_dia_final.loc[idx, 'vlr_saldo_anterior'] = saldo_inicio.get(
                            'vlr_saldo_anterior', 0.0)
                        saldo_dia_final.loc[idx, 'nsaldoprovisorio'] = saldo_inicio.get(
                            'nsaldoprovisorio')
                        saldo_dia_final.loc[idx, 'fl_natureza'] = saldo_inicio.get(
                            'fl_natureza')
                        saldo_dia_final.loc[idx, 'de_origem'] = saldo_inicio.get(
                            'de_origem')
                        saldo_dia_final.loc[idx, 'dt_inclusao'] = saldo_inicio.get(
                            'dt_inclusao')
                        saldo_dia_final.loc[idx, 'cod_categoria'] = saldo_inicio.get(
                            'cod_categoria')

        # Garantir que TODOS os registros sejam marcados como SALDO FINAL DIA
        # (caso algum não tenha sido marcado anteriormente)
        saldo_dia_final['new_cdescliente'] = saldo_dia_final['new_cdescliente'].fillna(
            'SALDO FINAL DIA')
        saldo_dia_final['new_ddatalancamento'] = saldo_dia_final['new_ddatalancamento'].fillna(
            saldo_dia_final['dt_lancamento'])

        # SALDO DIA deve ter num_linha_dia = 999999
        saldo_dia_final['num_linha_dia'] = 999999

        # Campos específicos do título devem ser None/0 para SALDO DIA
        saldo_dia_final['vlr_documento'] = 0.0

        # Para SALDO FINAL DIA, definir campos como null
        mask_saldo_final_dia = saldo_dia_final['new_cdescliente'] == 'SALDO FINAL DIA'
        saldo_dia_final.loc[mask_saldo_final_dia, 'de_situacao'] = None
        saldo_dia_final.loc[mask_saldo_final_dia,
                            'codigo_lancamento_relacionamento'] = None
        saldo_dia_final.loc[mask_saldo_final_dia, 'codigo_lancamento'] = None
        saldo_dia_final.loc[mask_saldo_final_dia, 'de_observacao'] = None

        print(
            f"[EXTRATO DIARIO CC] Total de registros SALDO FINAL DIA criados: {len(saldo_dia_final)}")
        if len(saldo_dia_final) > 0:
            print(
                f"[EXTRATO DIARIO CC] Amostra de saldo_dia_final (primeiras 3 linhas):")
            amostra_cols = ['cod_empresa', 'de_cc',
                            'new_cdescliente', 'dt_lancamento', 'vlr_saldo']
            amostra_cols = [
                col for col in amostra_cols if col in saldo_dia_final.columns]
            print(saldo_dia_final[amostra_cols].head(3).to_string())
    else:
        saldo_dia_final = pd.DataFrame(columns=[
            'cod_empresa', 'fk_cc', 'tipo_cc', 'de_cc', 'de_cliente',
            'dt_periodo_inicial', 'dt_periodo_final', 'dt_lancamento',
            'vlr_saldo', 'vlr_saldo_anterior', 'vlr_documento', 'de_situacao',
            'codigo_lancamento_relacionamento', 'codigo_lancamento', 'de_observacao',
            'num_linha_dia', 'nsaldoprovisorio', 'new_cdescliente', 'new_ddatalancamento',
            'fl_natureza', 'de_origem', 'dt_inclusao', 'cod_categoria'
        ])

    # ETAPA 11: UNION ALL - Combinar EXTRATO_CC_STG1_FILTRADO e SALDO_DIA_FINAL
    print("[EXTRATO DIARIO CC] Etapa 11: Combinando dados finais...")

    print(
        f"[EXTRATO DIARIO CC] Registros em extrato_cc_stg1_filtrado: {len(extrato_cc_stg1_filtrado)}")
    print(
        f"[EXTRATO DIARIO CC] Registros em saldo_dia_final: {len(saldo_dia_final)}")

    # Preparar extrato_cc_stg1_filtrado para união (incluindo novas colunas)
    colunas_extrato = [
        'cod_empresa', 'fk_cc', 'tipo_cc', 'de_cc', 'de_cliente',
        'dt_periodo_inicial', 'dt_periodo_final', 'dt_lancamento',
        'vlr_saldo', 'vlr_saldo_anterior', 'vlr_documento', 'de_situacao',
        'codigo_lancamento_relacionamento', 'codigo_lancamento', 'de_observacao',
        'num_linha_dia', 'nsaldoprovisorio', 'new_cdescliente', 'new_ddatalancamento',
        'fl_natureza', 'de_origem', 'dt_inclusao', 'cod_categoria'
    ]
    # Filtrar apenas colunas que existem
    colunas_extrato = [
        col for col in colunas_extrato if col in extrato_cc_stg1_filtrado.columns]
    extrato_final = extrato_cc_stg1_filtrado[colunas_extrato].copy()

    # Substituir de_cliente por new_cdescliente (usar a lógica de new_cdescliente)
    if 'new_cdescliente' in extrato_final.columns:
        extrato_final['de_cliente'] = extrato_final['new_cdescliente']
    else:
        print(
            "[EXTRATO DIARIO CC] AVISO: new_cdescliente não encontrado em extrato_final")

    # Converter dt_lancamento para date se necessário
    if pd.api.types.is_datetime64_any_dtype(extrato_final['dt_lancamento']):
        extrato_final['dt_lancamento'] = extrato_final['dt_lancamento'].dt.date

    # Converter new_ddatalancamento para date se necessário
    if 'new_ddatalancamento' in extrato_final.columns:
        if pd.api.types.is_datetime64_any_dtype(extrato_final['new_ddatalancamento']):
            extrato_final['new_ddatalancamento'] = extrato_final['new_ddatalancamento'].dt.date
        # Garantir que todos os valores sejam date (pode ter tipos mistos)
        extrato_final['new_ddatalancamento'] = pd.to_datetime(
            extrato_final['new_ddatalancamento'], errors='coerce').dt.date

    # Converter saldo_dia_final dt_lancamento e new_ddatalancamento para date
    if len(saldo_dia_final) > 0:
        if pd.api.types.is_datetime64_any_dtype(saldo_dia_final['dt_lancamento']):
            saldo_dia_final['dt_lancamento'] = saldo_dia_final['dt_lancamento'].dt.date
        if 'new_ddatalancamento' in saldo_dia_final.columns:
            if pd.api.types.is_datetime64_any_dtype(saldo_dia_final['new_ddatalancamento']):
                saldo_dia_final['new_ddatalancamento'] = saldo_dia_final['new_ddatalancamento'].dt.date
            # Garantir que todos os valores sejam date (pode ter tipos mistos)
            saldo_dia_final['new_ddatalancamento'] = pd.to_datetime(
                saldo_dia_final['new_ddatalancamento'], errors='coerce').dt.date

        # Substituir de_cliente por new_cdescliente em saldo_dia_final também
        if 'new_cdescliente' in saldo_dia_final.columns:
            saldo_dia_final['de_cliente'] = saldo_dia_final['new_cdescliente']
        else:
            print(
                "[EXTRATO DIARIO CC] AVISO: new_cdescliente não encontrado em saldo_dia_final")
            # Criar new_cdescliente se não existir
            saldo_dia_final['new_cdescliente'] = 'SALDO FINAL DIA'
            saldo_dia_final['de_cliente'] = 'SALDO FINAL DIA'

        # Garantir que todas as colunas necessárias existam em saldo_dia_final
        colunas_necessarias_saldo = ['cod_empresa', 'fk_cc', 'tipo_cc', 'de_cc', 'de_cliente',
                                     'dt_periodo_inicial', 'dt_periodo_final', 'dt_lancamento',
                                     'vlr_saldo', 'vlr_saldo_anterior', 'vlr_documento',
                                     'de_situacao', 'codigo_lancamento_relacionamento',
                                     'codigo_lancamento', 'de_observacao', 'num_linha_dia',
                                     'nsaldoprovisorio', 'new_ddatalancamento', 'fl_natureza', 'de_origem',
                                     'dt_inclusao', 'cod_categoria']
        for col in colunas_necessarias_saldo:
            if col not in saldo_dia_final.columns:
                print(
                    f"[EXTRATO DIARIO CC] AVISO: Coluna {col} não encontrada em saldo_dia_final, criando...")
                saldo_dia_final[col] = None

    # Garantir que ambas as tabelas tenham as mesmas colunas antes de unir
    if len(saldo_dia_final) > 0:
        # Obter todas as colunas de ambos os DataFrames
        todas_colunas = list(set(extrato_final.columns) |
                             set(saldo_dia_final.columns))

        # Adicionar colunas faltantes em cada DataFrame
        for col in todas_colunas:
            if col not in extrato_final.columns:
                extrato_final[col] = None
            if col not in saldo_dia_final.columns:
                saldo_dia_final[col] = None

        # Ordenar colunas para garantir consistência
        todas_colunas_sorted = sorted(todas_colunas)
        extrato_final = extrato_final[todas_colunas_sorted].copy()
        saldo_dia_final = saldo_dia_final[todas_colunas_sorted].copy()

        print(
            f"[EXTRATO DIARIO CC] Total de colunas para união: {len(todas_colunas_sorted)}")
        print(f"[EXTRATO DIARIO CC] Colunas: {todas_colunas_sorted}")

    # Unir os DataFrames
    if len(saldo_dia_final) > 0:
        resultado_final = pd.concat(
            [extrato_final, saldo_dia_final], ignore_index=True)
    else:
        resultado_final = extrato_final.copy()
        print(
            "[EXTRATO DIARIO CC] AVISO: saldo_dia_final está vazio, usando apenas extrato_final")

    # Remover coluna new_cdescliente (já foi copiada para de_cliente)
    if 'new_cdescliente' in resultado_final.columns:
        resultado_final = resultado_final.drop(columns=['new_cdescliente'])

    # Garantir que a coluna de ordenação tenha todos os valores no mesmo formato
    col_ordenacao = 'new_ddatalancamento' if 'new_ddatalancamento' in resultado_final.columns else 'dt_lancamento'

    # Converter para date se ainda houver tipos mistos
    if col_ordenacao in resultado_final.columns:
        # Converter todos os valores para date usando pd.to_datetime primeiro
        resultado_final[col_ordenacao] = pd.to_datetime(
            resultado_final[col_ordenacao], errors='coerce').dt.date

    # Preparar código_lancamento para ordenação numérica
    if 'codigo_lancamento' in resultado_final.columns:
        cod_lanc_ord = pd.to_numeric(
            resultado_final['codigo_lancamento'], errors='coerce')
        resultado_final['cod_lanc_ord'] = cod_lanc_ord.fillna(
            0).astype('Int64')
    else:
        resultado_final['cod_lanc_ord'] = 0

    # Preparar dt_inclusao para ordenação
    if 'dt_inclusao' in resultado_final.columns:
        resultado_final['dt_inclusao_ord'] = pd.to_datetime(
            resultado_final['dt_inclusao'], errors='coerce')
    else:
        resultado_final['dt_inclusao_ord'] = None

    # Ordenar conforme especificado: empresa → conta corrente → período inicial → data lançamento → dt_inclusao → código lançamento
    resultado_final = resultado_final.sort_values(
        by=['cod_empresa', 'fk_cc', 'dt_periodo_inicial',
            col_ordenacao, 'dt_inclusao_ord', 'cod_lanc_ord'],
        ascending=[True, True, True, True, True, True],
        na_position='last'
    ).reset_index(drop=True)

    # Remover colunas temporárias de ordenação
    if 'cod_lanc_ord' in resultado_final.columns:
        resultado_final = resultado_final.drop(columns=['cod_lanc_ord'])
    if 'dt_inclusao_ord' in resultado_final.columns:
        resultado_final = resultado_final.drop(columns=['dt_inclusao_ord'])

    print(f"[EXTRATO DIARIO CC] Transformação concluída!")
    print(f"[EXTRATO DIARIO CC] Registros de saída: {len(resultado_final)}")
    print(
        f"[EXTRATO DIARIO CC] Colunas finais: {list(resultado_final.columns)}")

    return resultado_final


def processar_fato_extrato_diario_conta_corrente(config_yaml):
    """
    Processa os dados da camada gold para criar o fato extrato diário de conta corrente.

    Args:
        config_yaml: Dicionário com as configurações

    Returns:
        bool: True se o processo foi bem-sucedido, False caso contrário
    """
    try:
        # Configurações do GCS - lendo da camada gold
        bucket_name = config_yaml['gcs']['gold']['bucket']
        caminho_gold = f"{config_yaml['gcs']['gold']['folder']}/mysql_omie/fato_extrato.parquet"
        credentials_path = config_yaml['credentials-path']

        # Ler dados da camada gold
        print(f"[EXTRATO DIARIO CC] Lendo dados da camada gold...")
        df = ler_do_gcs(bucket_name, caminho_gold, credentials_path)

        if df is None or len(df) == 0:
            print(f"[EXTRATO DIARIO CC] Nenhum dado encontrado na camada gold.")
            return False

        print(
            f"[EXTRATO DIARIO CC] Total de registros para processar: {len(df)}")
        print(f"[EXTRATO DIARIO CC] Colunas: {list(df.columns)}")

        # Transformar os dados
        df_gold = transformar_extrato_diario(df)

        if df_gold is None or len(df_gold) == 0:
            print(f"[EXTRATO DIARIO CC] Erro na transformação dos dados.")
            return False

        # Salvar na camada gold
        bucket_name_gold = config_yaml['gcs']['gold']['bucket']
        caminho_gold_folder = f"{config_yaml['gcs']['gold']['folder']}/mysql_omie"

        resultado = salvar_no_gcs(
            df_gold,
            bucket_name_gold,
            caminho_gold_folder,
            credentials_path,
            "fato_extrato_diario_conta_corrente_v2.parquet"
        )

        return resultado is not None

    except Exception as e:
        print(f"[EXTRATO DIARIO CC] Erro no processamento: {e}")
        import traceback
        print(
            f"[EXTRATO DIARIO CC] Detalhes do erro:\n{traceback.format_exc()}")
        return False


if __name__ == "__main__":
    # Teste individual do módulo
    print("="*80)
    print("PROCESSAMENTO GOLD - FATO EXTRATO DIÁRIO CONTA CORRENTE")
    print("="*80)

    config_yaml = carregar_config()

    try:
        sucesso = processar_fato_extrato_diario_conta_corrente(config_yaml)
        print(f"\n{'='*80}")
        print(f"Processo {'concluído com sucesso' if sucesso else 'falhou'}!")
        print(f"{'='*80}")

    except Exception as e:
        print(f"Erro durante o processamento: {e}")
        import traceback
        print(traceback.format_exc())
