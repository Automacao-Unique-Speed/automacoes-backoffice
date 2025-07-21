import pandas as pd
import sys  # Importa sys para poder encerrar o script em caso de erro

# --- Configurações do Arquivo ---
import pandas as pd
import sys  # Importa sys para poder encerrar o script em caso de erro

# --- Configurações do Arquivo ---
arquivo_xlsx = 'principal.xlsx'
NOME_ABA = 'Inventário Analítico SPD'  # Nome exato da aba onde estão os dados

# --- Nomes das Colunas Originais no Excel e Seus Novos Nomes ---
# ATENÇÃO: Este mapeamento foi CORRIGIDO com base nos nomes das colunas
# REALMENTE encontradas na sua planilha, que foram listadas no seu último output de erro.
COLUNAS_PARA_PROCESSAR = {
    'RAZÃO EMPRESARIAL': 'RAZÃO EMPRESARIAL',
    # Cabeçalho original: 'RAZÃO EMPRESARIAL' -> Novo nome: 'RAZÃO EMPRESARIAL'
    'CNPJ': 'CNPJ',  # Cabeçalho original: 'CNPJ' -> Novo nome: 'CNPJ'
    'NÚMERO DE SÉRIE DA POS': 'MÁQUINA'  # Cabeçalho original: 'NÚMERO DE SÉRIE DA POS' -> Novo nome: 'MÁQUINA'
}

# --- Caminho do arquivo de saída ---
arquivo_saida = 'quantidade_maquinas_por_empresa.xlsx'

# --- Início do Script ---
try:
    print(f"🔄 Lendo o arquivo '{arquivo_xlsx}' na aba '{NOME_ABA}'...")

    # Lê o arquivo Excel da aba específica.
    # Por padrão, pd.read_excel() usa a primeira linha como cabeçalho (header=0).
    df = pd.read_excel(arquivo_xlsx, sheet_name=NOME_ABA)

    print(f"✅ Arquivo '{arquivo_xlsx}' lido com sucesso da aba '{NOME_ABA}'.")
    print("\n--- Primeiras linhas do arquivo lido (com cabeçalhos originais) ---")
    print(df.head())
    print("------------------------------------------------------------------")
    print(f"Colunas originais encontradas: {list(df.columns)}")

    # --- Validação das Colunas Essenciais ---
    # Verifica se todas as colunas originais esperadas existem no DataFrame
    missing_cols = [col for col in COLUNAS_PARA_PROCESSAR.keys() if col not in df.columns]

    if missing_cols:
        print(f"\n❌ ERRO: A aba '{NOME_ABA}' está faltando as seguintes colunas essenciais:")
        for col in missing_cols:
            print(f"- '{col}'")
        print(
            "Por favor, verifique se os nomes das colunas no seu Excel correspondem EXATAMENTE ao mapeamento no script.")
        print(f"As colunas DISPONÍVEIS na planilha são: {list(df.columns)}")  # Adicionado para clareza
        sys.exit(1)  # Encerra o script com erro

    # --- Seleciona as colunas relevantes e as renomeia ---
    # Cria um novo DataFrame apenas com as colunas que você precisa e já as renomeia
    df_processar = df[list(COLUNAS_PARA_PROCESSAR.keys())].rename(columns=COLUNAS_PARA_PROCESSAR)

    print(f"\n✅ Colunas relevantes selecionadas e renomeadas para: {list(df_processar.columns)}")

    # --- Limpeza e Padronização dos Dados das Colunas ---
    # Assegura que os valores são strings e remove espaços em branco extras
    df_processar['RAZÃO EMPRESARIAL'] = df_processar['RAZÃO EMPRESARIAL'].astype(str).str.strip()

    # Para CNPJ: Converte para string, remove quaisquer caracteres não numéricos e remove espaços
    df_processar['CNPJ'] = df_processar['CNPJ'].astype(str).str.replace(r'[^\d]', '', regex=True).str.strip()

    df_processar['MÁQUINA'] = df_processar['MÁQUINA'].astype(str).str.strip()
    print("✅ Dados das colunas 'RAZÃO EMPRESARIAL', 'CNPJ' e 'MÁQUINA' limpos e padronizados.")

    # --- Agrupa e Conta as Máquinas ---
    print("\n🔄 Agrupando por 'RAZÃO EMPRESARIAL' e 'CNPJ' e contando as máquinas...")
    resultado = df_processar.groupby(['RAZÃO EMPRESARIAL', 'CNPJ']).size().reset_index(name='Quantidade de Máquinas')
    print("✅ Agrupamento e contagem de máquinas por empresa/CNPJ concluídos.")

    # --- Salva o Resultado ---
    print(f"\n🔄 Salvando o resultado em: '{arquivo_saida}'...")
    resultado.to_excel(arquivo_saida, index=False)  # index=False para não incluir a coluna de índice do DataFrame

    print(f'\n🎉 Sucesso! O resultado foi salvo em: {arquivo_saida}')

except FileNotFoundError:
    print(f"\n❌ ERRO: O arquivo '{arquivo_xlsx}' não foi encontrado.")
    print("Por favor, verifique se o nome do arquivo está correto e se ele está na mesma pasta do script.")
    sys.exit(1)
except Exception as e:
    print(f"\n❌ Ocorreu um erro inesperado durante a execução do script: {e}")
    print("Verifique os detalhes do erro acima e a estrutura da sua planilha Excel.")
    sys.exit(1)