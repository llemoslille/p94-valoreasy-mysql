import pandas as pd
import yaml
import os
from dim_categorias import carregar_config, ler_do_gcs


def validar_categoria_p940022():
    """
    Valida se a categoria 'ADM - Almoço / Jantar' na empresa P940022
    foi ajustada para incluir o sufixo '(INATIVA)'.
    """
    print("="*80)
    print("VALIDAÇÃO - CATEGORIA P940022")
    print("="*80)

    try:
        # Carregar configurações
        config_yaml = carregar_config()

        # Configurações do GCS Gold
        bucket_name_gold = config_yaml['gcs']['gold']['bucket']
        caminho_gold = f"{config_yaml['gcs']['gold']['folder']}/mysql_omie/dim_categorias.parquet"
        credentials_path = config_yaml['credentials-path']

        # Ler dados da camada gold
        print(f"\n[VALIDAÇÃO] Lendo dados da camada gold...")
        df_gold = ler_do_gcs(bucket_name_gold, caminho_gold, credentials_path)

        if df_gold is None or len(df_gold) == 0:
            print("[VALIDAÇÃO] ERRO: Nenhum dado encontrado na camada gold.")
            return False

        # Filtrar pela empresa P940022
        empresa_id = 'P940022'
        df_empresa = df_gold[df_gold['cod_empresa'] == empresa_id].copy()

        if len(df_empresa) == 0:
            print(
                f"[VALIDAÇÃO] AVISO: Nenhum registro encontrado para a empresa {empresa_id}")
            print(
                f"[VALIDAÇÃO] Empresas disponíveis: {sorted(df_gold['cod_empresa'].unique())}")
            return False

        print(
            f"\n[VALIDAÇÃO] Total de registros para empresa {empresa_id}: {len(df_empresa)}")

        # Buscar categoria "ADM - Almoço / Jantar"
        categoria_procurada = "ADM - Almoço / Jantar"
        categoria_procurada_inativa = "ADM - Almoço / Jantar (INATIVA)"

        # Buscar na coluna descricao
        mask_descricao = df_empresa['descricao'].str.contains(
            categoria_procurada, case=False, na=False
        )

        # Buscar na coluna descricao_padrao
        mask_descricao_padrao = df_empresa['descricao_padrao'].str.contains(
            categoria_procurada, case=False, na=False
        )

        # Combinar as máscaras
        mask_total = mask_descricao | mask_descricao_padrao

        if not mask_total.any():
            print(
                f"\n[VALIDAÇÃO] AVISO: Categoria '{categoria_procurada}' não encontrada para empresa {empresa_id}")
            print(f"\n[VALIDAÇÃO] Categorias disponíveis (primeiras 20):")
            print(df_empresa[['cod_empresa', 'descricao',
                  'descricao_padrao']].head(20).to_string())
            return False

        # Filtrar registros encontrados
        df_encontrados = df_empresa[mask_total].copy()

        print(
            f"\n[VALIDAÇÃO] Registros encontrados com '{categoria_procurada}': {len(df_encontrados)}")
        print("\n[VALIDAÇÃO] Detalhes dos registros encontrados:")
        print(df_encontrados[['cod_empresa', 'codigo',
              'descricao', 'descricao_padrao']].to_string())

        # Verificar se tem o sufixo "(INATIVA)"
        tem_inativa_descricao = df_encontrados['descricao'].str.contains(
            r'\(INATIVA\)', case=False, na=False
        ).any()

        tem_inativa_descricao_padrao = df_encontrados['descricao_padrao'].str.contains(
            r'\(INATIVA\)', case=False, na=False
        ).any()

        print("\n" + "="*80)
        print("RESULTADO DA VALIDAÇÃO:")
        print("="*80)

        if tem_inativa_descricao or tem_inativa_descricao_padrao:
            print("[OK] SUCESSO: A categoria foi ajustada corretamente!")
            print(
                f"  - Campo 'descricao' contem '(INATIVA)': {tem_inativa_descricao}")
            print(
                f"  - Campo 'descricao_padrao' contem '(INATIVA)': {tem_inativa_descricao_padrao}")

            # Mostrar valores exatos
            if tem_inativa_descricao:
                valores_descricao = df_encontrados[df_encontrados['descricao'].str.contains(
                    r'\(INATIVA\)', case=False, na=False
                )]['descricao'].unique()
                print(f"\n  Valores em 'descricao' com '(INATIVA)':")
                for val in valores_descricao:
                    print(f"    - {val}")

            if tem_inativa_descricao_padrao:
                valores_descricao_padrao = df_encontrados[df_encontrados['descricao_padrao'].str.contains(
                    r'\(INATIVA\)', case=False, na=False
                )]['descricao_padrao'].unique()
                print(f"\n  Valores em 'descricao_padrao' com '(INATIVA)':")
                for val in valores_descricao_padrao:
                    print(f"    - {val}")

            return True
        else:
            print("[ERRO] FALHA: A categoria NAO foi ajustada com o sufixo '(INATIVA)'")
            print("\n  Valores encontrados:")
            print(f"    - descricao: {df_encontrados['descricao'].unique()}")
            print(
                f"    - descricao_padrao: {df_encontrados['descricao_padrao'].unique()}")

            # Verificar dados brutos da bronze para entender o problema
            print("\n  Verificando dados brutos da camada bronze...")
            try:
                bucket_name_bronze = config_yaml['gcs']['bronze']['bucket']
                caminho_bronze = f"{config_yaml['gcs']['bronze']['folder']}/mysql_omie/categorias.parquet"
                df_bronze = ler_do_gcs(
                    bucket_name_bronze, caminho_bronze, credentials_path)

                if df_bronze is not None:
                    df_bronze_empresa = df_bronze[df_bronze['empresa_id'] == empresa_id].copy(
                    )
                    mask_bronze = df_bronze_empresa['descricao'].str.contains(
                        categoria_procurada, case=False, na=False
                    )
                    if mask_bronze.any():
                        df_bronze_encontrado = df_bronze_empresa[mask_bronze]
                        print(f"\n  Dados brutos encontrados:")
                        print(
                            f"    - conta_inativa: {df_bronze_encontrado['conta_inativa'].values if 'conta_inativa' in df_bronze_encontrado.columns else 'COLUNA NAO ENCONTRADA'}")
                        print(
                            f"    - descricao: {df_bronze_encontrado['descricao'].values}")
                        print(
                            f"    - descricao_padrao: {df_bronze_encontrado['descricao_padrao'].values if 'descricao_padrao' in df_bronze_encontrado.columns else 'COLUNA NAO ENCONTRADA'}")
            except Exception as e:
                print(f"    Erro ao verificar dados bronze: {e}")

            return False

    except Exception as e:
        print(f"\n[VALIDAÇÃO] ERRO durante a validação: {e}")
        import traceback
        print(f"[VALIDAÇÃO] Detalhes do erro:\n{traceback.format_exc()}")
        return False


if __name__ == "__main__":
    sucesso = validar_categoria_p940022()
    print(f"\n{'='*80}")
    print(f"Validação {'concluída com sucesso' if sucesso else 'falhou'}!")
    print(f"{'='*80}")
