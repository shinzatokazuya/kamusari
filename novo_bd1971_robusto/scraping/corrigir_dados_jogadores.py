import csv
import os

caminho_csv = r'c:\Users\enryk\Documents\Estudos\kamusari\novo_bd1971_robusto\output_csvs\jogadores.csv'
caminho_temp = caminho_csv + '.tmp'

print(f"🛠️ Corrigindo Altura e Peso no arquivo: {caminho_csv}")

try:
    with open(caminho_csv, 'r', encoding='utf-8') as f_in, \
         open(caminho_temp, 'w', encoding='utf-8', newline='') as f_out:

        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
        writer.writeheader()

        corrigidos = 0
        for row in reader:
            altura = row.get('altura', '')
            peso = row.get('peso', '')

            # Se a altura tem 5 dígitos (ex: 18575) e o peso está vazio
            if len(altura) == 5 and (not peso or peso == ''):
                row['altura'] = altura[:3] # Pega os 3 primeiros (185)
                row['peso'] = altura[3:]   # Pega o restante (75)
                corrigidos += 1

            writer.writerow(row)

    os.replace(caminho_temp, caminho_csv)
    print(f"✅ Sucesso! {corrigidos} jogadores foram corrigidos.")

except Exception as e:
    print(f"❌ Erro: {e}")
    if os.path.exists(caminho_temp):
        os.remove(caminho_temp)
