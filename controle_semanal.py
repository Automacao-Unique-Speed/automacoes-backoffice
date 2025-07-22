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

# --- Configurações dos Arquivos e Abas ---
PLANILHA_SEMANAL_ENTREGA_PATH = "semanal1407.xlsx"
ABA_SEMANAL_ENTREGA = "Sheet1"

PLANILHA_BI_SEMANAL_PATH = "semanal2207.xlsx"
ABA_BI_SEMANAL = "Export"

PLANILHA_ADICIONAL_PATH = "adicional2207.xlsx"
ABA_ADICIONAL = "Sheet1"

# --- Nomes das Colunas (DEFINIDOS COM EXATIDÃO PARA CADA PLANILHA) ---
COL_ENTREGA_CNPJ = "CNPJ/CPF do EC \n(sem / ou -)"
COL_ENTREGA_NOME = "Razão Social do EC"
COL_ENTREGA_VALOR_LIQUIDADO_PASSADO = "Valor já liquidado ao EC até a data base"
COL_ENTREGA_VALOR_LIQUIDAR_FUTURO = "Valor a liquidar ao EC a partir da data base \n(agenda futura)"

COL_BI_CNPJ = "CPF/CNPJ"
COL_BI_NOME = "Razão Social"
COL_BI_PAGAMENTOS = "Pagamentos ECs Relatorio"

COL_ADICIONAL_CNPJ = "Cnpj"
COL_ADICIONAL_NOME = "Nome "
COL_ADICIONAL_AGENDA_FUTURA = "Valor a Antecipar"

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
df_entrega = carregar_planilha_robusto(PLANILHA_SEMANAL_ENTREGA_PATH, ABA_SEMANAL_ENTREGA, "planilha de entrega")
df_bi = carregar_planilha_robusto(PLANILHA_BI_SEMANAL_PATH, ABA_BI_SEMANAL, "planilha BI")
df_adicional = carregar_planilha_robusto(PLANILHA_ADICIONAL_PATH, ABA_ADICIONAL, "planilha adicional (agenda futura)")

# --- Etapa 2/7: Validação de Colunas Essenciais ---
print("\n--- Etapa 2/7: Validação de Colunas Essenciais ---")
required_cols_entrega = [COL_ENTREGA_CNPJ, COL_ENTREGA_NOME, COL_ENTREGA_VALOR_LIQUIDADO_PASSADO,
                         COL_ENTREGA_VALOR_LIQUIDAR_FUTURO]
required_cols_bi = [COL_BI_CNPJ, COL_BI_NOME, COL_BI_PAGAMENTOS]
required_cols_adicional = [COL_ADICIONAL_CNPJ, COL_ADICIONAL_NOME, COL_ADICIONAL_AGENDA_FUTURA]

validar_colunas(df_entrega, PLANILHA_SEMANAL_ENTREGA_PATH, "planilha de entrega", required_cols_entrega)
validar_colunas(df_bi, PLANILHA_BI_SEMANAL_PATH, "planilha BI", required_cols_bi)
validar_colunas(df_adicional, PLANILHA_ADICIONAL_PATH, "planilha adicional (agenda futura)", required_cols_adicional)
print("✅ Todas as colunas essenciais foram encontradas em todas as planilhas.")

# --- Etapa 3/7: Padronização de Dados e Preparação de Valores Numéricos ---
print("\n--- Etapa 3/7: Padronizando CNPJs e Nomes, e preparando valores numéricos ---")
try:
    # Planilha de Entrega
    df_entrega[COL_ENTREGA_CNPJ] = df_entrega[COL_ENTREGA_CNPJ].astype(str)  # Garante que a coluna original é string
    df_entrega['CNPJ_LIMPO'] = padronizar_cnpj(df_entrega[COL_ENTREGA_CNPJ])
    df_entrega['NOME_LIMPO'] = padronizar_nome(df_entrega[COL_ENTREGA_NOME])

    for col_val in [COL_ENTREGA_VALOR_LIQUIDADO_PASSADO, COL_ENTREGA_VALOR_LIQUIDAR_FUTURO]:
        initial_nan_count = df_entrega[col_val].isnull().sum()
        df_entrega[col_val] = pd.to_numeric(df_entrega[col_val], errors='coerce').fillna(0)
        if initial_nan_count > 0:
            print(
                f"   ⚠️ ATENÇÃO: Coluna '{col_val}' (entrega) continha {initial_nan_count} valores não numéricos/vazios, convertidos para 0.")
            logger.warning(
                f"Coluna {col_val} na entrega tinha {initial_nan_count} NaN/não numéricos, convertidos para 0.")

    # Planilha BI
    df_bi[COL_BI_CNPJ] = df_bi[COL_BI_CNPJ].astype(str)  # Garante que a coluna original é string
    df_bi['CNPJ_LIMPO'] = padronizar_cnpj(df_bi[COL_BI_CNPJ])
    df_bi['NOME_LIMPO'] = padronizar_nome(df_bi[COL_BI_NOME])
    initial_nan_bi_pagamentos = df_bi[COL_BI_PAGAMENTOS].isnull().sum()
    df_bi[COL_BI_PAGAMENTOS] = pd.to_numeric(df_bi[COL_BI_PAGAMENTOS], errors='coerce').fillna(0)
    if initial_nan_bi_pagamentos > 0:
        print(
            f"   ⚠️ ATENÇÃO: Coluna '{COL_BI_PAGAMENTOS}' (BI) continha {initial_nan_bi_pagamentos} valores não numéricos/vazios, convertidos para 0.")
        logger.warning(
            f"Coluna {COL_BI_PAGAMENTOS} no BI tinha {initial_nan_bi_pagamentos} NaN/não numéricos, convertidos para 0.")

    # Planilha Adicional
    df_adicional[COL_ADICIONAL_CNPJ] = df_adicional[COL_ADICIONAL_CNPJ].astype(
        str)  # Garante que a coluna original é string
    df_adicional['CNPJ_LIMPO'] = padronizar_cnpj(df_adicional[COL_ADICIONAL_CNPJ])
    df_adicional['NOME_LIMPO'] = padronizar_nome(df_adicional[COL_ADICIONAL_NOME])
    initial_nan_adicional_agenda = df_adicional[COL_ADICIONAL_AGENDA_FUTURA].isnull().sum()
    df_adicional[COL_ADICIONAL_AGENDA_FUTURA] = pd.to_numeric(df_adicional[COL_ADICIONAL_AGENDA_FUTURA],
                                                              errors='coerce').fillna(0)
    if initial_nan_adicional_agenda > 0:
        print(
            f"   ⚠️ ATENÇÃO: Coluna '{COL_ADICIONAL_AGENDA_FUTURA}' (adicional) continha {initial_nan_adicional_agenda} valores não numéricos/vazios, convertidos para 0.")
        logger.warning(
            f"Coluna {COL_ADICIONAL_AGENDA_FUTURA} na adicional tinha {initial_nan_adicional_agenda} NaN/não numéricos, convertidos para 0.")

    empty_cnpj_entrega = df_entrega[df_entrega['CNPJ_LIMPO'] == ''].shape[0]
    empty_cnpj_bi = df_bi[df_bi['CNPJ_LIMPO'] == ''].shape[0]
    empty_cnpj_adicional = df_adicional[df_adicional['CNPJ_LIMPO'] == ''].shape[0]

    if empty_cnpj_entrega > 0:
        print(
            f"   ⚠️ ATENÇÃO: {empty_cnpj_entrega} linhas na planilha de entrega possuem CNPJ vazio após padronização. Isso pode afetar o matching.")
        logger.warning(f"{empty_cnpj_entrega} CNPJs vazios na planilha de entrega.")
    if empty_cnpj_bi > 0:
        print(
            f"   ⚠️ ATENÇÃO: {empty_cnpj_bi} linhas na planilha BI possuem CNPJ vazio após padronização. Isso pode afetar o matching.")
        logger.warning(f"{empty_cnpj_bi} CNPJs vazios na planilha BI.")
    if empty_cnpj_adicional > 0:
        print(
            f"   ⚠️ ATENÇÃO: {empty_cnpj_adicional} linhas na planilha adicional possuem CNPJ vazio após padronização. Isso pode afetar o matching.")
        logger.warning(f"{empty_cnpj_adicional} CNPJs vazios na planilha adicional.")

    print("✅ CNPJs e Nomes padronizados e valores numéricos preparados em todas as planilhas.")
    logger.info("Padronização de dados e preparação numérica concluídas.")

except Exception as e:
    error_msg = f"\n❌ ERRO FATAL: Falha durante a padronização de dados ou conversão de tipo.\n   Detalhes técnicos: {e}"
    print(error_msg)
    logger.critical(error_msg)
    sys.exit(1)

# --- Etapa 4/7: Agrupando dados das planilhas de origem ---
print("\n--- Etapa 4/7: Agrupando dados das planilhas de origem (BI e Adicional) ---")
try:
    df_bi_agrupado = df_bi.groupby(['CNPJ_LIMPO', 'NOME_LIMPO'])[COL_BI_PAGAMENTOS].sum().reset_index()
    df_bi_agrupado.rename(columns={COL_BI_PAGAMENTOS: 'Soma_Pagamentos_BI'}, inplace=True)
    print(f"   ✅ Dados do BI agrupados por CNPJ e Nome. Total de entradas únicas no BI: {len(df_bi_agrupado)}")
    logger.info(f"Dados BI agrupados. {len(df_bi_agrupado)} entradas únicas.")

    # Para a planilha adicional, se houver múltiplos valores para o mesmo CNPJ/Nome,
    # estamos pegando o PRIMEIRO. Se a regra for diferente (ex: somar, pegar o último), ajustar aqui.
    df_adicional_agrupado = df_adicional.groupby(['CNPJ_LIMPO', 'NOME_LIMPO'])[
        COL_ADICIONAL_AGENDA_FUTURA].first().reset_index()
    df_adicional_agrupado.rename(columns={COL_ADICIONAL_AGENDA_FUTURA: 'Valor_Agenda_Futura_Adicional'}, inplace=True)
    print(
        f"   ✅ Dados da planilha adicional agrupados por CNPJ e Nome. Total de entradas únicas: {len(df_adicional_agrupado)}")
    logger.info(f"Dados adicionais agrupados. {len(df_adicional_agrupado)} entradas únicas.")

except Exception as e:
    error_msg = f"\n❌ ERRO CRÍTICO: Falha ao agrupar dados das planilhas de origem.\n   Detalhes técnicos: {e}"
    print(error_msg)
    logger.critical(error_msg)
    sys.exit(1)

# --- Etapa 5/7: Identificando e adicionando novas lojas do BI ao relatório ---
print("\n--- Etapa 5/7: Identificando e adicionando novas lojas do BI ao relatório ---")

novas_lojas_encontradas = []
df_novas_lojas_para_adicionar_list = [] # Usar uma lista de dicionários para construir o DF

# Cria um set de chaves (CNPJ_LIMPO, NOME_LIMPO) do df_entrega ORIGINAL
# para identificar o que já existe no relatório.
chaves_entrega_existente = set(zip(df_entrega['CNPJ_LIMPO'], df_entrega['NOME_LIMPO']))

for _, row_bi in df_bi_agrupado.iterrows():
    bi_cnpj_limpo = row_bi['CNPJ_LIMPO']
    bi_nome_limpo = row_bi['NOME_LIMPO']
    bi_pagamento = row_bi['Soma_Pagamentos_BI']

    # VERIFICA SE A LOJA JÁ EXISTE NO RELATÓRIO DE ENTREGA
    if (bi_cnpj_limpo, bi_nome_limpo) not in chaves_entrega_existente:
        nova_linha_dict = {}

        # Tenta pegar os valores originais do BI antes da padronização para a nova linha
        original_bi_row = df_bi[(df_bi['CNPJ_LIMPO'] == bi_cnpj_limpo) & (df_bi['NOME_LIMPO'] == bi_nome_limpo)]

        if not original_bi_row.empty:
            # Pega o primeiro valor original encontrado para CNPJ e Nome
            # Garante que o CNPJ original seja uma string ANTES de ser adicionado, e remove '.0'
            original_cnpj = str(original_bi_row[COL_BI_CNPJ].iloc[0])
            if original_cnpj.endswith('.0'):
                original_cnpj = original_cnpj.replace('.0', '')
            nova_linha_dict[COL_ENTREGA_CNPJ] = original_cnpj

            nova_linha_dict[COL_ENTREGA_NOME] = original_bi_row[COL_BI_NOME].iloc[0]
        else:
            # Fallback para os valores padronizados se o original não for encontrado (improvável)
            nova_linha_dict[COL_ENTREGA_CNPJ] = bi_cnpj_limpo
            nova_linha_dict[COL_ENTREGA_NOME] = bi_nome_limpo

        nova_linha_dict[COL_ENTREGA_VALOR_LIQUIDADO_PASSADO] = bi_pagamento
        nova_linha_dict[COL_ENTREGA_VALOR_LIQUIDAR_FUTURO] = 0.0 # Nova loja começa com 0 para agenda futura

        # Preenche outras colunas com NaN (ou 0 se for numérico, dependendo da necessidade) para as novas lojas
        # É importante que as novas linhas tenham as mesmas colunas que o df_entrega original
        for col in df_entrega.columns:
            if col not in nova_linha_dict:
                # Se for uma coluna de valor, pode ser melhor preencher com 0.0, senão NaN.
                if col in [COL_ENTREGA_VALOR_LIQUIDADO_PASSADO, COL_ENTREGA_VALOR_LIQUIDAR_FUTURO]:
                     nova_linha_dict[col] = 0.0
                else:
                    nova_linha_dict[col] = np.nan

        # Adiciona as colunas padronizadas para o futuro re-cálculo da chave
        nova_linha_dict['CNPJ_LIMPO'] = bi_cnpj_limpo
        nova_linha_dict['NOME_LIMPO'] = bi_nome_limpo

        df_novas_lojas_para_adicionar_list.append(nova_linha_dict)
        novas_lojas_encontradas.append(
            f"CNPJ: {nova_linha_dict[COL_ENTREGA_CNPJ]}, Loja: {nova_linha_dict[COL_ENTREGA_NOME]}, Valor Pagamento BI: {bi_pagamento:.2f}")

# Concatena as novas lojas APENAS SE HOUVEREM
if df_novas_lojas_para_adicionar_list:
    df_novas_lojas_para_adicionar_df = pd.DataFrame(df_novas_lojas_para_adicionar_list)

    print("\n   --- Novas lojas encontradas e adicionadas ao relatório: ---")
    for loja_info in novas_lojas_encontradas:
        print(f"   ➕ {loja_info}")
    print("   ---------------------------------------------------------")

    # Concatenar o df_entrega ORIGINAL com as NOVAS lojas.
    # O df_entrega_atualizado é o que será trabalhado nas etapas seguintes.
    df_entrega_atualizado = pd.concat([df_entrega, df_novas_lojas_para_adicionar_df], ignore_index=True)

    print(f"   ✅ Total de lojas na planilha de entrega após adicionar novas: {len(df_entrega_atualizado)}")
    logger.info(f"Novas lojas adicionadas: {len(novas_lojas_encontradas)}")
else:
    print("   ✅ Nenhuma nova loja encontrada na planilha BI para adicionar.")
    # Se não houver novas lojas, df_entrega_atualizado é apenas uma cópia do df_entrega original
    df_entrega_atualizado = df_entrega.copy()

# Garante que as colunas _LIMPO estão atualizadas para o df_entrega_atualizado,
# especialmente se novas linhas foram adicionadas.
df_entrega_atualizado['CNPJ_LIMPO'] = padronizar_cnpj(df_entrega_atualizado[COL_ENTREGA_CNPJ])
df_entrega_atualizado['NOME_LIMPO'] = padronizar_nome(df_entrega_atualizado[COL_ENTREGA_NOME])

logger.info("Processo de identificação de novas lojas concluído.")

# --- INÍCIO DA NOVA ETAPA: Remoção e Consolidação de Duplicatas ---
print("\n--- Etapa 5.5/7: Verificando e Consolidando Duplicatas ---")

# Identificar duplicatas baseadas em CNPJ_LIMPO e NOME_LIMPO
# O `keep=False` marca todas as ocorrências de duplicatas como True
duplicatas = df_entrega_atualizado.duplicated(subset=['CNPJ_LIMPO', 'NOME_LIMPO'], keep=False)
num_duplicatas_detectadas = duplicatas.sum()

if num_duplicatas_detectadas > 0:
    print(f"   ⚠️ ATENÇÃO: {num_duplicatas_detectadas} linhas com CNPJ/Nome duplicados detectadas no relatório.")
    logger.warning(f"{num_duplicatas_detectadas} linhas duplicadas detectadas antes da consolidação.")

    # Definir como as colunas devem ser agregadas para as duplicatas
    # COL_ENTREGA_VALOR_LIQUIDADO_PASSADO (Valor já liquidado) deve ser somado
    # COL_ENTREGA_VALOR_LIQUIDAR_FUTURO (Agenda Futura) deve pegar o último valor (ou o primeiro, dependendo da regra)
    # Para outras colunas, pegamos o primeiro valor não nulo.

    # Mapeamento de funções de agregação para cada coluna
    agg_funcs = {
        COL_ENTREGA_VALOR_LIQUIDADO_PASSADO: 'sum', # Somar valores já liquidados
        COL_ENTREGA_VALOR_LIQUIDAR_FUTURO: 'first' # Pegar o primeiro para agenda futura (ou 'last' se for o caso)
    }

    # Para todas as outras colunas, queremos pegar o primeiro valor não nulo
    for col in df_entrega_atualizado.columns:
        if col not in [COL_ENTREGA_CNPJ, COL_ENTREGA_NOME, 'CNPJ_LIMPO', 'NOME_LIMPO',
                       COL_ENTREGA_VALOR_LIQUIDADO_PASSADO, COL_ENTREGA_VALOR_LIQUIDAR_FUTURO]:
            agg_funcs[col] = 'first' # Ou lambda x: x.dropna().iloc[0] se quiser o primeiro não-nulo

    # Garante que as colunas originais de CNPJ e Nome sejam mantidas (pegando o primeiro valor)
    agg_funcs[COL_ENTREGA_CNPJ] = 'first'
    agg_funcs[COL_ENTREGA_NOME] = 'first'

    # Realiza o agrupamento e a agregação para consolidar as duplicatas
    df_entrega_atualizado = df_entrega_atualizado.groupby(['CNPJ_LIMPO', 'NOME_LIMPO'], as_index=False).agg(agg_funcs)

    num_linhas_apos_consolidacao = len(df_entrega_atualizado)
    print(f"   ✅ Duplicatas consolidadas. Total de linhas após consolidação: {num_linhas_apos_consolidacao}")
    logger.info(f"Duplicatas consolidadas. {num_linhas_apos_consolidacao} linhas após consolidação.")
else:
    print("   ✅ Nenhuma duplicata encontrada para consolidação.")
    logger.info("Nenhuma duplicata encontrada para consolidação.")

# --- FIM DA NOVA ETAPA ---

# --- Etapa 6/7: Realizar Match e Atualizações de Valores ---
print("\n--- Etapa 6/7: Realizando match e atualizando valores nas colunas do relatório ---")

bi_combined_map = {(row['CNPJ_LIMPO'], row['NOME_LIMPO']): row['Soma_Pagamentos_BI']
                   for _, row in df_bi_agrupado.iterrows()}

adicional_combined_map = {(row['CNPJ_LIMPO'], row['NOME_LIMPO']): row['Valor_Agenda_Futura_Adicional']
                          for _, row in df_adicional_agrupado.iterrows()}

lojas_somadas_bi = 0
lojas_substituidas_adicional = 0

# Itera sobre cada linha do DataFrame de entrega atualizado
# Usamos .loc para uma atualização mais eficiente e segura.
for idx, row in df_entrega_atualizado.iterrows():
    cnpj_entrega = row['CNPJ_LIMPO']
    nome_entrega = row['NOME_LIMPO']
    chave_combinada = (cnpj_entrega, nome_entrega)

    # === Atualização da coluna de SOMA (Valor já liquidado ao EC até a data base) ===
    if chave_combinada in bi_combined_map:
        pagamento_bi_para_somar = bi_combined_map[chave_combinada]
        # Atualiza a linha usando .loc para melhor performance e segurança
        df_entrega_atualizado.loc[idx, COL_ENTREGA_VALOR_LIQUIDADO_PASSADO] += pagamento_bi_para_somar
        lojas_somadas_bi += 1

    # === Atualização da coluna de SUBSTITUIÇÃO (Valor a liquidar ao EC a partir da data base (agenda futura)) ===
    if chave_combinada in adicional_combined_map:
        valor_agenda_futura_adicional = adicional_combined_map[chave_combinada]
        # Atualiza a linha usando .loc para melhor performance e segurança
        df_entrega_atualizado.loc[idx, COL_ENTREGA_VALOR_LIQUIDAR_FUTURO] = valor_agenda_futura_adicional
        lojas_substituidas_adicional += 1

print(f"\n   ✅ Atualização de valores concluída.")
print(f"      - Lojas com valores 'já liquidado' SOMADOS (do BI): {lojas_somadas_bi}")
print(
    f"      - Lojas com valores 'a liquidar (agenda futura)' SUBSTITUÍDOS (da adicional): {lojas_substituidas_adicional}")

# Se houverem lojas que deveriam ter sido atualizadas pela adicional, mas não foram
if lojas_substituidas_adicional == 0 and len(df_adicional_agrupado) > 0:
    print("\n   ⚠️ ATENÇÃO: Nenhuma atualização de 'agenda futura' foi realizada pela planilha adicional.")
    print("      Verificando possíveis motivos (comparando as chaves):")
    # Pegue uma amostra dos dados padronizados da planilha adicional
    print("      Algumas chaves (CNPJ_LIMPO, NOME_LIMPO) da planilha ADICIONAL:")
    for i, (cnpj_a, nome_a) in enumerate(adicional_combined_map.keys()):
        print(f"        - Adicional: ('{cnpj_a}', '{nome_a}')")
        if i >= 4:  # Imprime apenas 5 exemplos para não poluir
            break

    print("\n      Algumas chaves (CNPJ_LIMPO, NOME_LIMPO) da planilha de ENTREGA (com novas lojas):")
    # Pegue uma amostra dos dados padronizados da planilha de entrega
    sample_entrega_keys = set(zip(df_entrega_atualizado['CNPJ_LIMPO'], df_entrega_atualizado['NOME_LIMPO']))
    for i, (cnpj_e, nome_e) in enumerate(sample_entrega_keys):
        print(f"        - Entrega: ('{cnpj_e}', '{nome_e}')")
        if i >= 4:
            break
    print(
        "\n      Compare essas chaves para identificar inconsistências (espaços, maiúsculas/minúsculas, caracteres especiais).")
    logger.warning("Nenhuma atualização de agenda futura da planilha adicional foi realizada.")

logger.info(
    f"Atualização de valores concluída. Somadas: {lojas_somadas_bi}, Substituídas: {lojas_substituidas_adicional}")

# --- Etapa 7/7: Finalização e Salvamento da Planilha Atualizada ---
print("\n--- Etapa 7/7: Finalização e Salvamento da Planilha Atualizada ---")

# Remove as colunas temporárias de CNPJ/Nome padronizados
df_final = df_entrega_atualizado.drop(columns=['CNPJ_LIMPO', 'NOME_LIMPO'])

# Garante que as colunas na planilha final mantenham a ordem original da planilha de entrega.
# Novas colunas (se houverem) serão adicionadas no final.
colunas_originais_df_entrega_inicial = df_entrega.columns.tolist()  # Pega as colunas da planilha de entrega original
colunas_finais_ordenadas = [col for col in colunas_originais_df_entrega_inicial if col in df_final.columns]
for col in df_final.columns:
    if col not in colunas_finais_ordenadas:
        colunas_finais_ordenadas.append(col)

df_final = df_final[colunas_finais_ordenadas]

# Salva o DataFrame final no mesmo arquivo da planilha de entrega, substituindo a aba.
try:
    print(f"\n💾 Salvando planilha atualizada em: '{PLANILHA_SEMANAL_ENTREGA_PATH}' (aba '{ABA_SEMANAL_ENTREGA}')...")
    with pd.ExcelWriter(PLANILHA_SEMANAL_ENTREGA_PATH, engine='openpyxl', mode='a',
                        if_sheet_exists='replace') as writer:
        df_final.to_excel(writer, sheet_name=ABA_SEMANAL_ENTREGA, index=False)

    print(f"\n🎉 SUCESSO! A planilha '{PLANILHA_SEMANAL_ENTREGA_PATH}' foi atualizada com sucesso.")
    print("   Verifique o arquivo e o log para os resultados finais.")
    logger.info("Script finalizado com sucesso. Planilha salva.")

except Exception as e:
    error_msg = (
        f"\n❌ ERRO FATAL: Ocorreu um erro ao salvar a planilha atualizada.\n"
        f"   Por favor, feche o arquivo '{PLANILHA_SEMANAL_ENTREGA_PATH}' se estiver aberto\n"
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