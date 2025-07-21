import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import sys
import openpyxl
import numpy as np  # Importa numpy para usar np.nan

# --- Configurações dos Arquivos e Colunas ---

# Planilha Principal (que será lida e de onde a aba será extraída e atualizada)
PLANILHA_PRINCIPAL_PATH = "principal.xlsx"
ABA_DEVOLUCAO = "Devolução de Maquininhas - Inat"  # Nome da aba a ser processada e salva

# Colunas da aba 'Devolução de Maquininhas - Inat' da principal.xlsx
COL_DEVOLUCAO_DESCRICAO = "Descrição"  # Ainda usada para leitura, mas não para matching
COL_DEVOLUCAO_CNPJ_CPF = "CPF/CNPJ"
COL_DEVOLUCAO_POS_PLANILHA = "POS Planilha"  # Coluna a ser preenchida/sobrescrita

# Planilha de Quantidade de Máquinas
PLANILHA_QTD_MAQUINAS_PATH = "quantidade_maquinas_por_empresa.xlsx"

# Colunas da planilha quantidade_maquinas_por_empresa.xlsx
COL_QTD_RAZAO = "RAZÃO EMPRESARIAL"  # Ainda usada para leitura, mas não para matching
COL_QTD_CNPJ = "CNPJ"
COL_QTD_QUANTIDADE = "Quantidade de Máquinas"

# --- Limiar para Fuzzy Matching de CNPJ ---
# ATENÇÃO: Um valor muito baixo pode levar a falsos positivos.
# CNPJs são identificadores únicos, e pequenas diferenças podem significar empresas diferentes.
# Recomenda-se um limiar MUITO ALTO (ex: 90-95) para CNPJs.
FUZZY_CNPJ_THRESHOLD = 90  # Porcentagem de similaridade (0-100). Ajuste com cautela.

# --- Caminho do Novo Arquivo de Saída (apenas com a aba de devolução) ---
NOVA_PLANILHA_SAIDA_PATH = "devolucao_maquininhas_atualizada_por_cnpj_fuzzy.xlsx"

# --- Carregar Planilhas ---
try:
    print(f"🔄 Carregando '{PLANILHA_PRINCIPAL_PATH}' para extrair a aba '{ABA_DEVOLUCAO}'...")
    df_devolucao = pd.read_excel(PLANILHA_PRINCIPAL_PATH, sheet_name=ABA_DEVOLUCAO)

    print(f"🔄 Carregando '{PLANILHA_QTD_MAQUINAS_PATH}'...")
    df_quantidade = pd.read_excel(PLANILHA_QTD_MAQUINAS_PATH)

    print("✅ Planilhas carregadas com sucesso.")

except FileNotFoundError as e:
    print(
        f"\n❌ ERRO: Arquivo não encontrado. Verifique os caminhos dos arquivos e certifique-se de que estão na mesma pasta do script.")
    print(f"Detalhes: {e}")
    sys.exit(1)
except ValueError as e:  # Captura o erro específico se a aba não for encontrada
    if f"Worksheet named '{ABA_DEVOLUCAO}' not found" in str(e):
        print(f"\n❌ ERRO: A aba '{ABA_DEVOLUCAO}' não foi encontrada em '{PLANILHA_PRINCIPAL_PATH}'.")
        print("Por favor, verifique o nome exato da aba na sua planilha e corrija na variável 'ABA_DEVOLUCAO'.")
    else:
        print(f"\n❌ Ocorreu um erro ao carregar os arquivos Excel. Detalhes: {e}")
    sys.exit(1)
except Exception as e:
    print(f"\n❌ Ocorreu um erro inesperado ao carregar os arquivos Excel. Detalhes: {e}")
    print("Verifique se os arquivos não estão abertos em outro programa e se estão no formato correto.")
    sys.exit(1)


# --- Validação de Colunas Essenciais ---
def validar_colunas(df, df_nome_str, colunas_requeridas):
    missing_cols = [col for col in colunas_requeridas if col not in df.columns]
    if missing_cols:
        print(f"\n❌ ERRO: As seguintes colunas esperadas não foram encontradas no DataFrame de '{df_nome_str}':")
        for col in missing_cols:
            print(f"- '{col}'")
        print(f"Colunas disponíveis: {list(df.columns)}")
        sys.exit(1)


validar_colunas(df_devolucao, ABA_DEVOLUCAO,
                [COL_DEVOLUCAO_DESCRICAO, COL_DEVOLUCAO_CNPJ_CPF, COL_DEVOLUCAO_POS_PLANILHA])
validar_colunas(df_quantidade, PLANILHA_QTD_MAQUINAS_PATH, [COL_QTD_RAZAO, COL_QTD_CNPJ, COL_QTD_QUANTIDADE])
print("✅ Colunas essenciais verificadas.")

# --- Preparação dos Dados para Matching ---
print("🔄 Padronizando dados de CNPJ/CPF para o matching fuzzy...")

# Limpar CNPJs/CPFs em ambas as planilhas (manter apenas dígitos)
df_devolucao['CNPJ_LIMPO'] = df_devolucao[COL_DEVOLUCAO_CNPJ_CPF].astype(str).str.replace(r'[^\d]', '',
                                                                                          regex=True).str.strip()
df_quantidade['CNPJ_LIMPO'] = df_quantidade[COL_QTD_CNPJ].astype(str).str.replace(r'[^\d]', '', regex=True).str.strip()

print("✅ Dados padronizados.")

# --- Preparar Dicionário de Quantidade de Máquinas por CNPJ (Limpo e Agrupado) ---
# Agrupa df_quantidade por CNPJ_LIMPO e soma as Quantidade de Máquinas
# Isso é feito para ter um valor único de máquinas por CNPJ limpo como referência.
print("🔄 Agrupando 'Quantidade de Máquinas' por CNPJ na planilha de referência...")
df_quantidade_agrupado = df_quantidade.groupby('CNPJ_LIMPO')[COL_QTD_QUANTIDADE].sum().reset_index()
# Converte para um dicionário para busca eficiente por CNPJ limpo
cnpj_para_quantidade_total = df_quantidade_agrupado.set_index('CNPJ_LIMPO')[COL_QTD_QUANTIDADE].to_dict()

# Lista de CNPJs limpos da planilha de quantidade para o fuzzy matching
lista_cnpjs_ref = df_quantidade_agrupado['CNPJ_LIMPO'].tolist()
print("✅ Dicionário e lista de CNPJs de referência criados.")

# --- Executar Fuzzy Matching de CNPJ e Preencher Coluna ---
print(f"🔄 Iniciando o processo de fuzzy matching de CNPJ e preenchimento da coluna '{COL_DEVOLUCAO_POS_PLANILHA}'...")
linhas_atualizadas = 0

# Pré-preenche a coluna 'POS Planilha' com NaN para todas as linhas.
# Isso garante que as linhas sem match serão NaN, como solicitado.
df_devolucao[COL_DEVOLUCAO_POS_PLANILHA] = np.nan

# Iterar sobre as linhas da planilha de Devolução de Maquininhas - Inat
for idx_dev, row_dev in df_devolucao.iterrows():
    cnpj_dev_limpo = row_dev['CNPJ_LIMPO']

    # Busca o melhor match fuzzy para o CNPJ atual na lista de CNPJs de referência
    if lista_cnpjs_ref:  # Garante que a lista de referência não está vazia
        melhor_match = process.extractOne(
            query=cnpj_dev_limpo,
            choices=lista_cnpjs_ref,
            scorer=fuzz.ratio  # Ou fuzz.partial_ratio, fuzz.token_set_ratio dependendo da sua necessidade
        )

        if melhor_match:
            cnpj_match_ref, score_cnpj = melhor_match[0], melhor_match[1]

            # Se o score for maior ou igual ao limiar definido
            if score_cnpj >= FUZZY_CNPJ_THRESHOLD:
                # Pega a quantidade total de máquinas para o CNPJ que deu match na referência.
                # O valor pode ser 0 se a soma das máquinas for 0 para aquele CNPJ.
                valor_encontrado_na_ref = cnpj_para_quantidade_total.get(cnpj_match_ref)

                # Atribui o valor encontrado (que pode ser 0) à coluna.
                # Se valor_encontrado_na_ref for None por algum motivo (não deveria acontecer aqui),
                # o valor original de np.nan para essa linha permanecerá.
                df_devolucao.at[idx_dev, COL_DEVOLUCAO_POS_PLANILHA] = valor_encontrado_na_ref

                # Contabiliza a linha como atualizada SOMENTE se um valor válido (não NaN) foi preenchido.
                if not pd.isna(valor_encontrado_na_ref):
                    linhas_atualizadas += 1

print(f"✅ Fuzzy matching de CNPJ concluído. {linhas_atualizadas} linhas atualizadas na aba '{ABA_DEVOLUCAO}'.")
if linhas_atualizadas == 0:
    print("\n⚠️ Nenhuma linha foi atualizada. Isso pode indicar:")
    print("  - CNPJs muito diferentes entre as planilhas, mesmo com fuzzy matching.")
    print(f"  - O limiar de similaridade de CNPJ ({FUZZY_CNPJ_THRESHOLD}%) pode ser muito alto.")
    print("  - Considere diminuir 'FUZZY_CNPJ_THRESHOLD' com CAUTELA, ou inspecione os dados manualmente.")

# --- Remover colunas temporárias ---
df_devolucao = df_devolucao.drop(columns=['CNPJ_LIMPO'])

# --- Salvar Apenas a Aba Atualizada em uma Nova Planilha Excel ---
print(f"🔄 Salvando a aba '{ABA_DEVOLUCAO}' atualizada em '{NOVA_PLANILHA_SAIDA_PATH}'...")
try:
    # Salva apenas o DataFrame 'df_devolucao' no novo arquivo.
    df_devolucao.to_excel(NOVA_PLANILHA_SAIDA_PATH, sheet_name=ABA_DEVOLUCAO, index=False)

    print(
        f"\n🎉 Sucesso! A nova planilha com a aba '{ABA_DEVOLUCAO}' atualizada foi criada em: '{NOVA_PLANILHA_SAIDA_PATH}'")

except Exception as e:
    print(f"\n❌ ERRO ao salvar a nova planilha '{NOVA_PLANILHA_SAIDA_PATH}'.")
    print(f"Detalhes: {e}")
    sys.exit(1)

print("\n✨ Processamento finalizado. ✨")