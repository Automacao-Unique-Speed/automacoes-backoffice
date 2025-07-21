import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import sys
import numpy as np  # Import numpy for np.nan

# --- Configurações dos Arquivos e Colunas ---

# Planilha de Origem (pos_bi)
PLANILHA_POS_BI_PATH = "pos_bi.xlsx"  # Assumindo o nome do arquivo
ABA_POS_BI = "Export"  # Nome da aba da planilha pos_bi

# Colunas da planilha pos_bi (CORRIGIDO conforme o erro)
COL_POS_BI_NOME_EMPRESA = "Razão Social"  # <--- CORRIGIDO AQUI!
COL_POS_BI_TOTAL_POS_ALOCADAS = "Total POS Alocadas"
COL_POS_BI_TOTAL_POS_NAO_UTILIZADAS = "Total POS Não Utilizadas"

# Planilha de Destino (a que criamos e atualizamos anteriormente)
PLANILHA_DESTINO_PATH = "devolucao_maquininhas_atualizada_por_cnpj_fuzzy.xlsx"  # Planilha da última execução
ABA_DESTINO = "Devolução de Maquininhas - Inat"  # Aba que contém a coluna "Descrição"

# Colunas da Planilha de Destino
COL_DESTINO_NOME_DESCRICAO = "Descrição"  # Coluna para o fuzzy match na planilha de destino
COL_DESTINO_POS_ADIQ = "POS Adiq"  # Nova coluna a ser criada/preenchida
COL_DESTINO_POS_NAO_UTILIZADA = "POS NÃO UTILIZADA"  # Nova coluna a ser criada/preenchida

# --- Limiar para Fuzzy Matching de Nomes ---
# Ajuste conforme a similaridade esperada dos nomes das empresas.
# Para nomes, 75-85 geralmente é um bom ponto de partida.
FUZZY_NAME_THRESHOLD = 80

# --- Carregar Planilhas ---
try:
    print(f"🔄 Carregando '{PLANILHA_POS_BI_PATH}' (aba '{ABA_POS_BI}')...")
    df_pos_bi = pd.read_excel(PLANILHA_POS_BI_PATH, sheet_name=ABA_POS_BI)

    print(f"🔄 Carregando '{PLANILHA_DESTINO_PATH}' (aba '{ABA_DESTINO}')...")
    df_destino = pd.read_excel(PLANILHA_DESTINO_PATH, sheet_name=ABA_DESTINO)

    print("✅ Planilhas carregadas com sucesso.")

except FileNotFoundError as e:
    print(
        f"\n❌ ERRO: Arquivo não encontrado. Verifique os caminhos dos arquivos e certifique-se de que estão na mesma pasta do script.")
    print(f"Detalhes: {e}")
    sys.exit(1)
except ValueError as e:  # Captura o erro específico se a aba não for encontrada
    print(f"\n❌ ERRO: A aba especificada não foi encontrada. Detalhes: {e}")
    print(f"Verifique o nome da aba em '{PLANILHA_POS_BI_PATH}' ou '{PLANILHA_DESTINO_PATH}'.")
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


validar_colunas(df_pos_bi, PLANILHA_POS_BI_PATH,
                [COL_POS_BI_NOME_EMPRESA, COL_POS_BI_TOTAL_POS_ALOCADAS, COL_POS_BI_TOTAL_POS_NAO_UTILIZADAS])
validar_colunas(df_destino, PLANILHA_DESTINO_PATH, [COL_DESTINO_NOME_DESCRICAO])
print("✅ Colunas essenciais verificadas.")

# --- Preparação dos Dados para Fuzzy Matching ---
print("🔄 Padronizando nomes para fuzzy matching...")

# Criar uma versão padronizada dos nomes para o matching (minúsculas, sem espaços extras)
df_pos_bi['NOME_EMPRESA_LIMPO'] = df_pos_bi[COL_POS_BI_NOME_EMPRESA].astype(str).str.lower().str.strip()
df_destino['NOME_DESCRICAO_LIMPO'] = df_destino[COL_DESTINO_NOME_DESCRICAO].astype(str).str.lower().str.strip()

# Criar uma lista de nomes limpos da pos_bi para o fuzzy matching (choices)
lista_nomes_pos_bi = df_pos_bi['NOME_EMPRESA_LIMPO'].tolist()

# Criar um dicionário para mapear o nome limpo da pos_bi de volta para os dados originais
# Pode haver nomes repetidos em pos_bi, então vamos agrupar para ter um total único por nome limpo
# Se um nome limpo tiver múltiplas entradas com diferentes totais, vamos somá-los.
df_pos_bi_agrupado = df_pos_bi.groupby('NOME_EMPRESA_LIMPO').agg(
    {
        COL_POS_BI_TOTAL_POS_ALOCADAS: 'sum',
        COL_POS_BI_TOTAL_POS_NAO_UTILIZADAS: 'sum'
    }
).reset_index()

# Dicionário para busca rápida (Nome Limpo -> {Total POS Alocadas, Total POS Não Utilizadas})
mapa_dados_pos_bi = df_pos_bi_agrupado.set_index('NOME_EMPRESA_LIMPO').to_dict('index')

print("✅ Nomes padronizados e dados de referência preparados.")

# --- Inicializar Novas Colunas no DataFrame de Destino ---
# Pre-encher as novas colunas com NaN. Elas serão preenchidas se um match for encontrado.
df_destino[COL_DESTINO_POS_ADIQ] = np.nan
df_destino[COL_DESTINO_POS_NAO_UTILIZADA] = np.nan
print(f"✅ Novas colunas '{COL_DESTINO_POS_ADIQ}' e '{COL_DESTINO_POS_NAO_UTILIZADA}' inicializadas com NaN.")

# --- Realizar Fuzzy Matching e Preencher Colunas ---
print(
    f"🔄 Iniciando o fuzzy matching de nomes e preenchimento das colunas '{COL_DESTINO_POS_ADIQ}' e '{COL_DESTINO_POS_NAO_UTILIZADA}'...")
linhas_atualizadas = 0

for idx_dest, row_dest in df_destino.iterrows():
    nome_destino_limpo = row_dest['NOME_DESCRICAO_LIMPO']

    # Se a lista de nomes de referência não estiver vazia
    if lista_nomes_pos_bi:
        # Encontra o melhor match fuzzy para o nome na planilha de destino
        # usando token_set_ratio para lidar melhor com ordem e palavras extras/faltando
        best_match_tuple = process.extractOne(
            query=nome_destino_limpo,
            choices=lista_nomes_pos_bi,
            scorer=fuzz.token_set_ratio
        )

        if best_match_tuple:
            matched_name_pos_bi, score = best_match_tuple[0], best_match_tuple[1]

            if score >= FUZZY_NAME_THRESHOLD:
                # Se um match satisfatório for encontrado, pegue os dados do mapa
                dados_do_match = mapa_dados_pos_bi.get(matched_name_pos_bi)

                if dados_do_match:  # Garante que os dados foram encontrados no mapa
                    df_destino.at[idx_dest, COL_DESTINO_POS_ADIQ] = dados_do_match[COL_POS_BI_TOTAL_POS_ALOCADAS]
                    df_destino.at[idx_dest, COL_DESTINO_POS_NAO_UTILIZADA] = dados_do_match[
                        COL_POS_BI_TOTAL_POS_NAO_UTILIZADAS]
                    linhas_atualizadas += 1

print(f"✅ Fuzzy matching de nomes concluído. {linhas_atualizadas} linhas atualizadas.")
if linhas_atualizadas == 0:
    print("\n⚠️ Nenhuma linha foi atualizada. Isso pode indicar:")
    print("  - Nomes de empresas muito diferentes entre as planilhas.")
    print(f"  - O limiar de similaridade de nomes ({FUZZY_NAME_THRESHOLD}%) pode ser muito alto.")
    print(
        "  - Considere diminuir 'FUZZY_NAME_THRESHOLD' ou inspecione os dados manualmente para entender as diferenças.")

# --- Limpeza (Remover colunas temporárias) ---
df_destino = df_destino.drop(columns=['NOME_DESCRICAO_LIMPO'])  # Remove a coluna temporária de nomes limpos

# --- Salvar a Planilha de Destino Atualizada ---
# Salvaremos a planilha de destino sobrescrevendo APENAS a aba 'Devolução de Maquininhas - Inat'.
# Isso é feito usando ExcelWriter com mode='a' (append) e if_sheet_exists='replace'.
try:
    with pd.ExcelWriter(PLANILHA_DESTINO_PATH, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df_destino.to_excel(writer, sheet_name=ABA_DESTINO, index=False)

    print(
        f"\n🎉 Sucesso! A planilha '{PLANILHA_DESTINO_PATH}' foi atualizada na aba '{ABA_DESTINO}' com os dados da pos_bi.")

except Exception as e:
    print(f"\n❌ ERRO ao salvar a planilha '{PLANILHA_DESTINO_PATH}'.")
    print(f"Detalhes: {e}")
    sys.exit(1)

print("\n✨ Processamento finalizado. ✨")