import pandas as pd
import openpyxl
import sys

# --- Configurações do Arquivo ---
# Nome do arquivo de trabalho. Garanta que este arquivo esteja na mesma pasta do script,
# ou forneça o caminho completo (ex: "C:/Users/SeuUsuario/Documentos/PAMELA MESCLAR.xlsx").
ARQUIVO_EXCEL = "PAMELA MESCLAR.xlsx"
NOME_ABA_PLANILHA1 = "Planilha1"
NOME_ABA_PLANILHA2 = "Planilha2"

# --- ALTERAÇÃO AQUI: Nome do novo arquivo Excel que será salvo ---
NOVO_ARQUIVO = "PLANILHA FINAL.xlsx"

# --- Nomes das Colunas Esperadas ---
COL_CONTA = "Conta"
COL_EXCLUIR = "Excluir"
COL_DETALHAR_MOTIVO = "DETALHAR MOTIVO"
COL_ENCERRAR_SIM_OU_NAO = "ENCERRAR? (Sim ou Não)"
COL_INFORMAR_MOTIVO_NAO_ENCERRAR = "Informar na planilha, na linha da conta o motivo de não encerrar:"

# --- Carregar Planilhas ---
try:
    print(f"🔄 Carregando o arquivo: '{ARQUIVO_EXCEL}'...")
    planilha1 = pd.read_excel(ARQUIVO_EXCEL, sheet_name=NOME_ABA_PLANILHA1, dtype=str)
    planilha2 = pd.read_excel(ARQUIVO_EXCEL, sheet_name=NOME_ABA_PLANILHA2, dtype=str)
    print("✅ Planilhas carregadas com sucesso.")
except FileNotFoundError:
    print(f"\n❌ ERRO: O arquivo '{ARQUIVO_EXCEL}' não foi encontrado.")
    print("Por favor, verifique se o nome do arquivo está correto e se ele está na mesma pasta do script.")
    sys.exit(1)
except Exception as e:
    print(f"\n❌ ERRO ao carregar o arquivo Excel '{ARQUIVO_EXCEL}'. Detalhes: {e}")
    sys.exit(1)

# --- Limpar Nomes das Colunas ---
planilha1.columns = planilha1.columns.str.strip()
planilha2.columns = planilha2.columns.str.strip()
print("✅ Nomes das colunas limpos (espaços iniciais/finais removidos).")

# --- Verificação de Colunas Essenciais ---
required_cols_planilha1 = [COL_CONTA, COL_EXCLUIR, COL_DETALHAR_MOTIVO]
required_cols_planilha2 = [COL_CONTA, COL_ENCERRAR_SIM_OU_NAO, COL_INFORMAR_MOTIVO_NAO_ENCERRAR]

for df_name, df, required_cols in [
    (NOME_ABA_PLANILHA1, planilha1, required_cols_planilha1),
    (NOME_ABA_PLANILHA2, planilha2, required_cols_planilha2)
]:
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"\n❌ ERRO: A aba '{df_name}' está faltando colunas essenciais.")
        print(f"As seguintes colunas não foram encontradas (após a limpeza de nomes):\n{missing_cols}")
        print(f"Colunas disponíveis na aba '{df_name}': {df.columns.tolist()}")
        print("Por favor, verifique os nomes das colunas no seu arquivo Excel e no código.")
        sys.exit(1)

# --- Limpar Espaços Extras nas Colunas de 'Conta' (conteúdo das células) ---
planilha1[COL_CONTA] = planilha1[COL_CONTA].astype(str).str.strip()
planilha2[COL_CONTA] = planilha2[COL_CONTA].astype(str).str.strip()
print(f"✅ Conteúdo da coluna '{COL_CONTA}' limpo (espaços iniciais/finais removidos).")

# --- Criar um Mapeamento Rápido de Contas da Planilha1 para Otimizar a Busca ---
contas_planilha1_map = dict(zip(planilha1[COL_CONTA], planilha1.index))

# --- Lista para Armazenar Erros de Contas Não Encontradas ---
erros_contas_nao_encontradas = []

print(f"\n🔄 Iniciando o processamento das contas da '{NOME_ABA_PLANILHA2}'...")
# --- Processar e Atualizar Planilha2 Linha a Linha ---
for i, row2 in planilha2.iterrows():
    conta2 = str(row2[COL_CONTA]).strip()

    if conta2 in contas_planilha1_map:
        idx1 = contas_planilha1_map[conta2]

        excluir_valor = str(planilha1.loc[idx1, COL_EXCLUIR]).lower().strip()
        motivo_detalhado_from_p1 = str(planilha1.loc[idx1, COL_DETALHAR_MOTIVO]).strip()

        # --- Etapa 1: Atualizar 'ENCERRAR? (Sim ou Não)' na Planilha2 ---
        current_encerrar_val_p2 = planilha2.at[i, COL_ENCERRAR_SIM_OU_NAO]
        if pd.isna(current_encerrar_val_p2) or str(current_encerrar_val_p2).strip() == "":
            if "encerrar" in excluir_valor:
                planilha2.at[i, COL_ENCERRAR_SIM_OU_NAO] = "ENCERRAR"
            elif "manter" in excluir_valor:
                planilha2.at[i, COL_ENCERRAR_SIM_OU_NAO] = "NAO ENCERRAR"

        # --- Etapa 2: Preencher 'Informar na planilha, na linha da conta o motivo de não encerrar:' CONDICIONALMENTE ---
        status_encerrar_p2 = str(planilha2.at[i, COL_ENCERRAR_SIM_OU_NAO]).upper().strip()

        current_motivo_val_p2 = planilha2.at[i, COL_INFORMAR_MOTIVO_NAO_ENCERRAR]
        if pd.isna(current_motivo_val_p2) or str(current_motivo_val_p2).strip() == "":
            if status_encerrar_p2 == "NÃO" or status_encerrar_p2 == "NAO ENCERRAR":
                planilha2.at[i, COL_INFORMAR_MOTIVO_NAO_ENCERRAR] = motivo_detalhado_from_p1

    else:
        erros_contas_nao_encontradas.append(
            f"⚠️ Conta '{conta2}' (linha {i + 2} da '{NOME_ABA_PLANILHA2}') não encontrada na '{NOME_ABA_PLANILHA1}'.")

print("✅ Processamento das contas concluído.")

# --- Etapa 3: Padronização Final dos Termos na Coluna 'ENCERRAR? (Sim ou Não)' ---
print("\n🔄 Realizando a padronização final dos termos na coluna 'ENCERRAR? (Sim ou Não)'...")
planilha2[COL_ENCERRAR_SIM_OU_NAO] = planilha2[COL_ENCERRAR_SIM_OU_NAO].astype(str)
planilha2[COL_ENCERRAR_SIM_OU_NAO] = planilha2[COL_ENCERRAR_SIM_OU_NAO].replace({
    'NAO ENCERRAR': 'Não',
    'ENCERRAR': 'Sim'
})
print("✅ Padronização concluída.")

# --- Salvar Nova Planilha Atualizada ---
try:
    print(f"\n🔄 Salvando o arquivo atualizado como: '{NOVO_ARQUIVO}'...")
    with pd.ExcelWriter(NOVO_ARQUIVO, engine='openpyxl') as writer:
        planilha1.to_excel(writer, sheet_name=NOME_ABA_PLANILHA1, index=False)
        planilha2.to_excel(writer, sheet_name=NOME_ABA_PLANILHA2, index=False)

    print(f"✅ Arquivo salvo com sucesso: '{NOVO_ARQUIVO}'")

except Exception as e:
    print(f"\n❌ ERRO ao salvar o arquivo '{NOVO_ARQUIVO}'. Detalhes: {e}")
    sys.exit(1)

# --- Exibir Log de Processamento ---
print("\n--- RESUMO FINAL DO PROCESSAMENTO ---")
if erros_contas_nao_encontradas:
    print("\n📋 As seguintes contas da Planilha2 não foram encontradas na Planilha1 e NÃO foram atualizadas:")
    for erro in erros_contas_nao_encontradas:
        print(erro)
    print(f"\nPor favor, verifique essas contas manualmente no arquivo original '{ARQUIVO_EXCEL}'.")
else:
    print("✅ Todas as contas da Planilha2 foram associadas e processadas com sucesso na Planilha1!")

print("\n✨ Processamento finalizado. ✨")