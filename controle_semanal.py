import pandas as pd
import sys
import numpy as np
import logging
import os

# --- Configuração de Logging ---
LOG_FILE_NAME = 'controle_semanal.log'
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    encoding='utf-8')
logger = logging.getLogger(__name__)

# --- Configurações dos Arquivos e Abas (Variáveis RENOMEADAS) ---
PLANILHA_ANTERIOR_PATH = "anterior.xlsx"
ABA_ANTERIOR = "Sheet1"

PLANILHA_SEMANAL_PATH = "semana 18 a 25.xlsx"
ABA_SEMANAL = "Export"

PLANILHA_FUTURA_PATH = "adicional2207.xlsx"
ABA_FUTURA = "Planilha1"

# --- Nomes das Colunas (DEFINIDOS COM EXATIDÃO PARA CADA PLANILHA) ---
COL_ANTERIOR_CNPJ = "CNPJ/CPF do EC \n(sem / ou -)"
COL_ANTERIOR_NOME = "Razão Social do EC"
COL_ANTERIOR_VALOR_LIQUIDADO_PASSADO = "Valor já liquidado ao EC até a data base"
COL_ANTERIOR_VALOR_LIQUIDAR_FUTURO = "Valor a liquidar ao EC a partir da data base \n(agenda futura)"

COL_SEMANAL_CNPJ = "CPF/CNPJ"
COL_SEMANAL_NOME = "Razão Social"
COL_SEMANAL_PAGAMENTOS = "Pagamentos ECs Relatorio"

COL_FUTURA_CNPJ = "Cnpj"
COL_FUTURA_NOME = "Nome"
COL_FUTURA_AGENDA_FUTURA = "Valor a Antecipar"

# --- Mensagens de Início e Log ---
print("=" * 80)
print("             INICIANDO PROCESSAMENTO DE RELATÓRIO SEMANAL (controle_semanal.py)             ")
print("=" * 80)
logger.info("Iniciando script de atualização de relatório semanal.")


# --- Funções Auxiliares Comuns ---
def padronizar_cnpj(cnpj_series):
    """
    Remove caracteres não numéricos de uma série de CNPJs,
    garantindo que o tipo seja string antes da operação, e removendo '.0' se for float.
    """
    if cnpj_series.empty:
        return pd.Series(dtype=str)

    # Primeiro, converte para string para lidar com floats que viraram '123.0'
    cnpj_str_series = cnpj_series.astype(str)

    # Remove '.0' de números que foram lidos como float
    cnpj_str_series = cnpj_str_series.apply(lambda x: x.replace('.0', '') if x.endswith('.0') else x)

    # Remove caracteres não numéricos e espaços extras
    return cnpj_str_series.str.replace(r'[^\d]', '', regex=True).str.strip()


def padronizar_nome(nome_series):
    """
    Converte uma série de nomes para minúsculas e remove espaços extras,
    garantindo que o tipo seja string antes da operação.
    """
    if nome_series.empty:
        return pd.Series(dtype=str)
    return nome_series.astype(str).str.lower().str.strip()


def validar_colunas(df, df_path_str, df_name_str, required_cols):
    """
    Verifica se todas as colunas essenciais estão presentes no DataFrame.
    Em caso de falha, imprime um erro crítico e encerra o script.
    """
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        error_msg = (
            f"\n❌ ERRO CRÍTICO: As seguintes colunas esperadas NÃO foram encontradas no DataFrame de '{df_name_str}'.\n"
            f"   Arquivo: '{df_path_str}'\n"
            f"   Colunas faltando: {missing_cols}\n"
            f"   Por favor, verifique a ortografia exata e a existência das colunas no seu arquivo Excel.\n"
            f"   Colunas disponíveis em '{df_name_str}': {list(df.columns)}"
        )
        print(error_msg)
        logger.critical(error_msg)
        sys.exit(1)
    logger.info(f"Todas as colunas essenciais verificadas em {df_name_str}.")


def carregar_planilha_robusto(file_path, sheet_name, display_name):
    """
    Carrega uma planilha Excel de forma robusta, com tratamento de erros para
    arquivo não encontrado, aba inexistente, arquivo vazio ou outros erros.
    """
    if not os.path.exists(file_path):
        error_msg = f"\n❌ ERRO FATAL: Arquivo '{file_path}' NÃO encontrado.\n   Verifique o caminho e o nome do arquivo."
        print(error_msg)
        logger.critical(error_msg)
        sys.exit(1)

    try:
        print(f"\n🔄 Carregando {display_name}: '{file_path}' (aba '{sheet_name}')...")
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        print(f"   ✅ {display_name} carregada. Total de linhas: {len(df)}")
        logger.info(f"{display_name} carregada: {file_path} ({sheet_name}) com {len(df)} linhas.")
        return df
    except ValueError as e:
        error_msg = (
            f"\n❌ ERRO FATAL: A aba '{sheet_name}' NÃO foi encontrada no arquivo '{file_path}'.\n"
            f"   Por favor, verifique o nome exato da aba (case-sensitive).\n"
            f"   Detalhes técnicos: {e}"
        )
        print(error_msg)
        logger.critical(error_msg)
        sys.exit(1)
    except pd.errors.EmptyDataError:
        error_msg = (
            f"\n❌ ERRO FATAL: O arquivo '{file_path}' está vazio ou não possui dados válidos.\n"
            f"   Verifique o conteúdo do arquivo."
        )
        print(error_msg)
        logger.critical(error_msg)
        sys.exit(1)
    except Exception as e:
        error_msg = (
            f"\n❌ ERRO FATAL: Ocorreu um erro inesperado ao carregar '{file_path}'.\n"
            f"   Verifique se o arquivo não está aberto em outro programa e se está no formato correto (.xlsx).\n"
            f"   Detalhes técnicos: {e}"
        )
        print(error_msg)
        logger.critical(error_msg)
        sys.exit(1)


# --- INÍCIO DO FLUXO PRINCIPAL ---

# --- Etapa 1/7: Carregamento de Planilhas ---
print("\n--- Etapa 1/7: Carregamento de Planilhas ---")
df_anterior = carregar_planilha_robusto(PLANILHA_ANTERIOR_PATH, ABA_ANTERIOR, "planilha anterior")
df_semanal = carregar_planilha_robusto(PLANILHA_SEMANAL_PATH, ABA_SEMANAL, "planilha semanal")
df_futura = carregar_planilha_robusto(PLANILHA_FUTURA_PATH, ABA_FUTURA, "planilha futura (agenda futura)")

# --- Etapa 2/7: Validação de Colunas Essenciais ---
print("\n--- Etapa 2/7: Validação de Colunas Essenciais ---")
required_cols_anterior = [COL_ANTERIOR_CNPJ, COL_ANTERIOR_NOME, COL_ANTERIOR_VALOR_LIQUIDADO_PASSADO,
                          COL_ANTERIOR_VALOR_LIQUIDAR_FUTURO]
required_cols_semanal = [COL_SEMANAL_CNPJ, COL_SEMANAL_NOME, COL_SEMANAL_PAGAMENTOS]
required_cols_futura = [COL_FUTURA_CNPJ, COL_FUTURA_NOME, COL_FUTURA_AGENDA_FUTURA]

validar_colunas(df_anterior, PLANILHA_ANTERIOR_PATH, "planilha anterior", required_cols_anterior)
validar_colunas(df_semanal, PLANILHA_SEMANAL_PATH, "planilha semanal", required_cols_semanal)
validar_colunas(df_futura, PLANILHA_FUTURA_PATH, "planilha futura (agenda futura)", required_cols_futura)
print("✅ Todas as colunas essenciais foram encontradas em todas as planilhas.")

# --- Etapa 3/7: Padronização de Dados e Preparação de Valores Numéricos ---
print("\n--- Etapa 3/7: Padronizando CNPJs e Nomes, e preparando valores numéricos ---")
try:
    # Planilha Anterior
    df_anterior[COL_ANTERIOR_CNPJ] = df_anterior[COL_ANTERIOR_CNPJ].astype(
        str)  # Garante que a coluna original é string
    df_anterior['CNPJ_LIMPO'] = padronizar_cnpj(df_anterior[COL_ANTERIOR_CNPJ])
    df_anterior['NOME_LIMPO'] = padronizar_nome(df_anterior[COL_ANTERIOR_NOME])

    for col_val in [COL_ANTERIOR_VALOR_LIQUIDADO_PASSADO, COL_ANTERIOR_VALOR_LIQUIDAR_FUTURO]:
        initial_nan_count = df_anterior[col_val].isnull().sum()
        df_anterior[col_val] = pd.to_numeric(df_anterior[col_val], errors='coerce').fillna(0)
        if initial_nan_count > 0:
            print(
                f"   ⚠️ ATENÇÃO: Coluna '{col_val}' (anterior) continha {initial_nan_count} valores não numéricos/vazios, convertidos para 0.")
            logger.warning(
                f"Coluna {col_val} na anterior tinha {initial_nan_count} NaN/não numéricos, convertidos para 0.")

    # Planilha Semanal
    df_semanal[COL_SEMANAL_CNPJ] = df_semanal[COL_SEMANAL_CNPJ].astype(
        str)  # Garante que a coluna original é string
    df_semanal['CNPJ_LIMPO'] = padronizar_cnpj(df_semanal[COL_SEMANAL_CNPJ])
    df_semanal['NOME_LIMPO'] = padronizar_nome(df_semanal[COL_SEMANAL_NOME])
    initial_nan_semanal_pagamentos = df_semanal[COL_SEMANAL_PAGAMENTOS].isnull().sum()
    df_semanal[COL_SEMANAL_PAGAMENTOS] = pd.to_numeric(df_semanal[COL_SEMANAL_PAGAMENTOS], errors='coerce').fillna(0)
    if initial_nan_semanal_pagamentos > 0:
        print(
            f"   ⚠️ ATENÇÃO: Coluna '{COL_SEMANAL_PAGAMENTOS}' (semanal) continha {initial_nan_semanal_pagamentos} valores não numéricos/vazios, convertidos para 0.")
        logger.warning(
            f"Coluna {COL_SEMANAL_PAGAMENTOS} na semanal tinha {initial_nan_semanal_pagamentos} NaN/não numéricos, convertidos para 0.")

    # Planilha Futura
    df_futura[COL_FUTURA_CNPJ] = df_futura[COL_FUTURA_CNPJ].astype(str)  # Garante que a coluna original é string
    df_futura['CNPJ_LIMPO'] = padronizar_cnpj(df_futura[COL_FUTURA_CNPJ])
    df_futura['NOME_LIMPO'] = padronizar_nome(df_futura[COL_FUTURA_NOME])
    initial_nan_futura_agenda = df_futura[COL_FUTURA_AGENDA_FUTURA].isnull().sum()
    df_futura[COL_FUTURA_AGENDA_FUTURA] = pd.to_numeric(df_futura[COL_FUTURA_AGENDA_FUTURA],
                                                          errors='coerce').fillna(0)
    if initial_nan_futura_agenda > 0:
        print(
            f"   ⚠️ ATENÇÃO: Coluna '{COL_FUTURA_AGENDA_FUTURA}' (futura) continha {initial_nan_futura_agenda} valores não numéricos/vazios, convertidos para 0.")
        logger.warning(
            f"Coluna {COL_FUTURA_AGENDA_FUTURA} na futura tinha {initial_nan_futura_agenda} NaN/não numéricos, convertidos para 0.")

    empty_cnpj_anterior = df_anterior[df_anterior['CNPJ_LIMPO'] == ''].shape[0]
    empty_cnpj_semanal = df_semanal[df_semanal['CNPJ_LIMPO'] == ''].shape[0]
    empty_cnpj_futura = df_futura[df_futura['CNPJ_LIMPO'] == ''].shape[0]

    if empty_cnpj_anterior > 0:
        print(
            f"   ⚠️ ATENÇÃO: {empty_cnpj_anterior} linhas na planilha anterior possuem CNPJ vazio após padronização. Isso pode afetar o matching.")
        logger.warning(f"{empty_cnpj_anterior} CNPJs vazios na planilha anterior.")
    if empty_cnpj_semanal > 0:
        print(
            f"   ⚠️ ATENÇÃO: {empty_cnpj_semanal} linhas na planilha semanal possuem CNPJ vazio após padronização. Isso pode afetar o matching.")
        logger.warning(f"{empty_cnpj_semanal} CNPJs vazios na planilha semanal.")
    if empty_cnpj_futura > 0:
        print(
            f"   ⚠️ ATENÇÃO: {empty_cnpj_futura} linhas na planilha futura possuem CNPJ vazio após padronização. Isso pode afetar o matching.")
        logger.warning(f"{empty_cnpj_futura} CNPJs vazios na planilha futura.")

    print("✅ CNPJs e Nomes padronizados e valores numéricos preparados em todas as planilhas.")
    logger.info("Padronização de dados e preparação numérica concluídas.")

except Exception as e:
    error_msg = f"\n❌ ERRO FATAL: Falha durante a padronização de dados ou conversão de tipo.\n   Detalhes técnicos: {e}"
    print(error_msg)
    logger.critical(error_msg)
    sys.exit(1)

# --- Etapa 4/7: Agrupando dados das planilhas de origem ---
print("\n--- Etapa 4/7: Agrupando dados das planilhas de origem (Semanal e Futura) ---")
try:
    df_semanal_agrupado = df_semanal.groupby(['CNPJ_LIMPO', 'NOME_LIMPO'])[COL_SEMANAL_PAGAMENTOS].sum().reset_index()
    df_semanal_agrupado.rename(columns={COL_SEMANAL_PAGAMENTOS: 'Soma_Pagamentos_Semanal'}, inplace=True)
    print(f"   ✅ Dados da Semanal agrupados por CNPJ e Nome. Total de entradas únicas na semanal: {len(df_semanal_agrupado)}")
    logger.info(f"Dados Semanais agrupados. {len(df_semanal_agrupado)} entradas únicas.")

    # Para a planilha futura, se houver múltiplos valores para o mesmo CNPJ/Nome,
    # estamos pegando o PRIMEIRO.
    df_futura_agrupado = df_futura.groupby(['CNPJ_LIMPO', 'NOME_LIMPO'])[
        COL_FUTURA_AGENDA_FUTURA].first().reset_index()
    df_futura_agrupado.rename(columns={COL_FUTURA_AGENDA_FUTURA: 'Valor_Agenda_Futura_Futura'}, inplace=True)
    print(
        f"   ✅ Dados da planilha futura agrupados por CNPJ e Nome. Total de entradas únicas: {len(df_futura_agrupado)}")
    logger.info(f"Dados da futura agrupados. {len(df_futura_agrupado)} entradas únicas.")

except Exception as e:
    error_msg = f"\n❌ ERRO CRÍTICO: Falha ao agrupar dados das planilhas de origem.\n   Detalhes técnicos: {e}"
    print(error_msg)
    logger.critical(error_msg)
    sys.exit(1)

# --- Etapa 5/7: Identificando e adicionando novas lojas da Semanal ao relatório ---
print("\n--- Etapa 5/7: Identificando e adicionando novas lojas da Semanal ao relatório ---")

novas_lojas_encontradas = []
df_novas_lojas_para_adicionar_list = [] # Usar uma lista de dicionários para construir o DF

# Cria um set de chaves (CNPJ_LIMPO, NOME_LIMPO) do df_anterior ORIGINAL
# para identificar o que já existe no relatório.
chaves_anterior_existente = set(zip(df_anterior['CNPJ_LIMPO'], df_anterior['NOME_LIMPO']))

for _, row_semanal in df_semanal_agrupado.iterrows():
    semanal_cnpj_limpo = row_semanal['CNPJ_LIMPO']
    semanal_nome_limpo = row_semanal['NOME_LIMPO']
    semanal_pagamento = row_semanal['Soma_Pagamentos_Semanal']

    # VERIFICA SE A LOJA JÁ EXISTE NO RELATÓRIO ANTERIOR
    if (semanal_cnpj_limpo, semanal_nome_limpo) not in chaves_anterior_existente:
        nova_linha_dict = {}

        # Tenta pegar os valores originais da semanal antes da padronização para a nova linha
        original_semanal_row = df_semanal[(df_semanal['CNPJ_LIMPO'] == semanal_cnpj_limpo) & (df_semanal['NOME_LIMPO'] == semanal_nome_limpo)]

        if not original_semanal_row.empty:
            # Pega o primeiro valor original encontrado para CNPJ e Nome
            original_cnpj = str(original_semanal_row[COL_SEMANAL_CNPJ].iloc[0])
            if original_cnpj.endswith('.0'):
                original_cnpj = original_cnpj.replace('.0', '')
            nova_linha_dict[COL_ANTERIOR_CNPJ] = original_cnpj

            nova_linha_dict[COL_ANTERIOR_NOME] = original_semanal_row[COL_SEMANAL_NOME].iloc[0]
        else:
            # Fallback para os valores padronizados se o original não for encontrado (improvável)
            nova_linha_dict[COL_ANTERIOR_CNPJ] = semanal_cnpj_limpo
            nova_linha_dict[COL_ANTERIOR_NOME] = semanal_nome_limpo

        nova_linha_dict[COL_ANTERIOR_VALOR_LIQUIDADO_PASSADO] = semanal_pagamento
        nova_linha_dict[COL_ANTERIOR_VALOR_LIQUIDAR_FUTURO] = 0.0 # Nova loja começa com 0 para agenda futura

        # Preenche outras colunas com NaN (ou 0 se for numérico, dependendo da necessidade) para as novas lojas
        for col in df_anterior.columns:
            if col not in nova_linha_dict:
                if col in [COL_ANTERIOR_VALOR_LIQUIDADO_PASSADO, COL_ANTERIOR_VALOR_LIQUIDAR_FUTURO]:
                     nova_linha_dict[col] = 0.0
                else:
                    nova_linha_dict[col] = np.nan

        # Adiciona as colunas padronizadas para o futuro re-cálculo da chave
        nova_linha_dict['CNPJ_LIMPO'] = semanal_cnpj_limpo
        nova_linha_dict['NOME_LIMPO'] = semanal_nome_limpo

        df_novas_lojas_para_adicionar_list.append(nova_linha_dict)
        novas_lojas_encontradas.append(
            f"CNPJ: {nova_linha_dict[COL_ANTERIOR_CNPJ]}, Loja: {nova_linha_dict[COL_ANTERIOR_NOME]}, Valor Pagamento Semanal: {semanal_pagamento:.2f}")

# Concatena as novas lojas APENAS SE HOUVEREM
if df_novas_lojas_para_adicionar_list:
    df_novas_lojas_para_adicionar_df = pd.DataFrame(df_novas_lojas_para_adicionar_list)
    print("\n   --- Novas lojas encontradas e adicionadas ao relatório: ---")
    for loja_info in novas_lojas_encontradas:
        print(f"   ➕ {loja_info}")
    print("   ---------------------------------------------------------")
    df_anterior_atualizado = pd.concat([df_anterior, df_novas_lojas_para_adicionar_df], ignore_index=True)
    print(f"   ✅ Total de lojas na planilha anterior após adicionar novas: {len(df_anterior_atualizado)}")
    logger.info(f"Novas lojas adicionadas: {len(novas_lojas_encontradas)}")
else:
    print("   ✅ Nenhuma nova loja encontrada na planilha semanal para adicionar.")
    df_anterior_atualizado = df_anterior.copy()

# Garante que as colunas _LIMPO estão atualizadas para o df_anterior_atualizado
df_anterior_atualizado['CNPJ_LIMPO'] = padronizar_cnpj(df_anterior_atualizado[COL_ANTERIOR_CNPJ])
df_anterior_atualizado['NOME_LIMPO'] = padronizar_nome(df_anterior_atualizado[COL_ANTERIOR_NOME])

logger.info("Processo de identificação de novas lojas concluído.")

# --- INÍCIO DA NOVA ETAPA: Remoção e Consolidação de Duplicatas ---
print("\n--- Etapa 5.5/7: Verificando e Consolidando Duplicatas ---")
duplicatas = df_anterior_atualizado.duplicated(subset=['CNPJ_LIMPO', 'NOME_LIMPO'], keep=False)
num_duplicatas_detectadas = duplicatas.sum()
if num_duplicatas_detectadas > 0:
    print(f"   ⚠️ ATENÇÃO: {num_duplicatas_detectadas} linhas com CNPJ/Nome duplicados detectadas no relatório.")
    logger.warning(f"{num_duplicatas_detectadas} linhas duplicadas detectadas antes da consolidação.")
    agg_funcs = {
        COL_ANTERIOR_VALOR_LIQUIDADO_PASSADO: 'sum',
        COL_ANTERIOR_VALOR_LIQUIDAR_FUTURO: 'first'
    }
    for col in df_anterior_atualizado.columns:
        if col not in [COL_ANTERIOR_CNPJ, COL_ANTERIOR_NOME, 'CNPJ_LIMPO', 'NOME_LIMPO',
                       COL_ANTERIOR_VALOR_LIQUIDADO_PASSADO, COL_ANTERIOR_VALOR_LIQUIDAR_FUTURO]:
            agg_funcs[col] = 'first'
    agg_funcs[COL_ANTERIOR_CNPJ] = 'first'
    agg_funcs[COL_ANTERIOR_NOME] = 'first'
    df_anterior_atualizado = df_anterior_atualizado.groupby(['CNPJ_LIMPO', 'NOME_LIMPO'], as_index=False).agg(agg_funcs)
    num_linhas_apos_consolidacao = len(df_anterior_atualizado)
    print(f"   ✅ Duplicatas consolidadas. Total de linhas após consolidação: {num_linhas_apos_consolidacao}")
    logger.info(f"Duplicatas consolidadas. {num_linhas_apos_consolidacao} linhas após consolidação.")
else:
    print("   ✅ Nenhuma duplicata encontrada para consolidação.")
    logger.info("Nenhuma duplicata encontrada para consolidação.")

# --- FIM DA NOVA ETAPA ---

# --- Etapa 6/7: Realizar Match e Atualizações de Valores (Agora com Fuzzy Match apenas no CNPJ para a Futura) ---
print("\n--- Etapa 6/7: Realizando match e atualizando valores nas colunas do relatório ---")

semanal_combined_map = {(row['CNPJ_LIMPO'], row['NOME_LIMPO']): row['Soma_Pagamentos_Semanal']
                       for _, row in df_semanal_agrupado.iterrows()}
# Não usaremos o mapa da futura, faremos o merge
# adicional_combined_map = {(row['CNPJ_LIMPO'], row['NOME_LIMPO']): row['Valor_Agenda_Futura_Futura']
#                           for _, row in df_futura_agrupado.iterrows()}

lojas_somadas_semanal = 0
lojas_substituidas_futura = 0

# Itera sobre cada linha do DataFrame de entrega atualizado
for idx, row in df_anterior_atualizado.iterrows():
    cnpj_anterior = row['CNPJ_LIMPO']
    nome_anterior = row['NOME_LIMPO']
    chave_combinada = (cnpj_anterior, nome_anterior)

    # === Atualização da coluna de SOMA (Valor já liquidado ao EC até a data base) ===
    # Esta parte permanece inalterada, pois a regra de somar do BI (semanal) se mantém.
    if chave_combinada in semanal_combined_map:
        pagamento_semanal_para_somar = semanal_combined_map[chave_combinada]
        # Atualiza a linha usando .loc para melhor performance e segurança
        df_anterior_atualizado.loc[idx, COL_ANTERIOR_VALOR_LIQUIDADO_PASSADO] += pagamento_semanal_para_somar
        lojas_somadas_semanal += 1

# === Implementando o Fuzzy Match para a planilha futura APENAS PELO CNPJ ===
print("\n   🔄 Realizando o 'fuzzy match' (merge) da planilha futura apenas pelo CNPJ...")

# Prepara a planilha futura para o merge, renomeando as colunas necessárias
df_futura_para_merge = df_futura_agrupado.rename(columns={'CNPJ_LIMPO': 'CNPJ_LIMPO_FUTURA',
                                                          'Valor_Agenda_Futura_Futura': 'Valor_Agenda_Futura_Futura'})
# Mantém apenas o CNPJ limpo e a coluna de valor
df_futura_para_merge = df_futura_para_merge[['CNPJ_LIMPO_FUTURA', 'Valor_Agenda_Futura_Futura']]

# Realiza o merge. O 'how="left"' garante que todas as linhas da planilha anterior sejam mantidas.
# O 'on' é a chave de junção, que é apenas o CNPJ_LIMPO.
df_merged = pd.merge(df_anterior_atualizado,
                     df_futura_para_merge,
                     left_on='CNPJ_LIMPO',
                     right_on='CNPJ_LIMPO_FUTURA',
                     how='left')

# Agora, substitui os valores na coluna de agenda futura da planilha anterior,
# usando os valores que vieram do merge.
# Se o valor do merge for NaN (não encontrou correspondência), o valor original é mantido.
df_merged[COL_ANTERIOR_VALOR_LIQUIDAR_FUTURO] = df_merged['Valor_Agenda_Futura_Futura'].fillna(
    df_merged[COL_ANTERIOR_VALOR_LIQUIDAR_FUTURO])

# Contagem de quantas linhas foram substituídas
# Pega o número de linhas onde a coluna do merge não é nula
lojas_substituidas_futura = df_merged['Valor_Agenda_Futura_Futura'].notna().sum()

# Remove as colunas temporárias criadas pelo merge
df_anterior_atualizado = df_merged.drop(columns=['CNPJ_LIMPO_FUTURA', 'Valor_Agenda_Futura_Futura'])


print(f"\n   ✅ Atualização de valores concluída.")
print(f"      - Lojas com valores 'já liquidado' SOMADOS (da semanal): {lojas_somadas_semanal}")
print(
    f"      - Lojas com valores 'a liquidar (agenda futura)' SUBSTITUÍDOS (da futura): {lojas_substituidas_futura}")

if lojas_substituidas_futura == 0 and len(df_futura_agrupado) > 0:
    print("\n   ⚠️ ATENÇÃO: Nenhuma atualização de 'agenda futura' foi realizada pela planilha futura.")
    logger.warning("Nenhuma atualização de agenda futura da planilha futura foi realizada.")

logger.info(
    f"Atualização de valores concluída. Somadas: {lojas_somadas_semanal}, Substituídas: {lojas_substituidas_futura}")

# --- Etapa 7/7: Finalização e Salvamento da Planilha Atualizada ---
print("\n--- Etapa 7/7: Finalização e Salvamento da Planilha Atualizada ---")

# Remove as colunas temporárias de CNPJ/Nome padronizados
df_final = df_anterior_atualizado.drop(columns=['CNPJ_LIMPO', 'NOME_LIMPO'])

# Garante que as colunas na planilha final mantenham a ordem original da planilha anterior.
colunas_originais_df_anterior_inicial = df_anterior.columns.tolist()
colunas_finais_ordenadas = [col for col in colunas_originais_df_anterior_inicial if col in df_final.columns]
for col in df_final.columns:
    if col not in colunas_finais_ordenadas:
        colunas_finais_ordenadas.append(col)

df_final = df_final[colunas_finais_ordenadas]

# Salva o DataFrame final no mesmo arquivo da planilha anterior, substituindo a aba.
try:
    print(f"\n💾 Salvando planilha atualizada em: '{PLANILHA_ANTERIOR_PATH}' (aba '{ABA_ANTERIOR}')...")
    with pd.ExcelWriter(PLANILHA_ANTERIOR_PATH, engine='openpyxl', mode='a',
                        if_sheet_exists='replace') as writer:
        df_final.to_excel(writer, sheet_name=ABA_ANTERIOR, index=False)

    print(f"\n🎉 SUCESSO! A planilha '{PLANILHA_ANTERIOR_PATH}' foi atualizada com sucesso.")
    print("   Verifique o arquivo e o log para os resultados finais.")
    logger.info("Script finalizado com sucesso. Planilha salva.")

except Exception as e:
    error_msg = (
        f"\n❌ ERRO FATAL: Ocorreu um erro ao salvar a planilha atualizada.\n"
        f"   Por favor, feche o arquivo '{PLANILHA_ANTERIOR_PATH}' se estiver aberto\n"
        f"   e tente novamente. Certifique-se de ter permissão de escrita na pasta.\n"
        f"   Detalhes técnicos: {e}"
    )
    print(error_msg)
    logger.critical(error_msg)
    sys.exit(1)

print("\n" + "=" * 80)
print("               PROCESSAMENTO CONCLUÍDO COM SUCESSO!               ")
print("=" + "=" * 80)
logger.info("Fim da execução do script.")
