import csv
import os

# Caminho completo do seu arquivo
caminho_csv = r'c:\Users\enryk\Documents\Estudos\kamusari\novo_bd1971_robusto\output_csvs\partidas.csv'
caminho_temp = caminho_csv + '.tmp'

print(f"🔄 Corrigindo alinhamento de colunas em: {caminho_csv}")

# Definimos exatamente como o cabeçalho deve ser agora
campos_corretos = [
    'id','edicao_id','campeonato_id','data','hora','fase','rodada',
    'estadio_id','mandante_id','visitante_id','mandante_placar',
    'visitante_placar','mandante_penalti','visitante_penalti',
    'prorrogacao', 'publico'
]

try:
    with open(caminho_csv, 'r', encoding='utf-8') as f_in, \
         open(caminho_temp, 'w', encoding='utf-8', newline='') as f_out:

        # O reader comum lê linha por linha como listas
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)

        # Pula a linha do cabeçalho antigo que você editou
        next(reader)

        # Escreve o novo cabeçalho garantindo que está perfeito
        writer.writerow(campos_corretos)

        linhas_corrigidas = 0
        for row in reader:
            # Se a linha tiver 15 colunas (o antigo), adicionamos a 16ª vazia
            if len(row) == 15:
                row.append('') # Adiciona o campo 'publico' vazio
                linhas_corrigidas += 1
            writer.writerow(row)

    # Substitui o arquivo original pelo corrigido
    os.replace(caminho_temp, caminho_csv)
    print(f"✅ Sucesso! {linhas_corrigidas} linhas antigas foram formatadas corretamente.")

except Exception as e:
    print(f"❌ Erro ao processar arquivo: {e}")
    if os.path.exists(caminho_temp):
        os.remove(caminho_temp)
