# for laço
#for item in sequencia:
    # codigo a ser executado para cada item na sequencia

numeros = [1, 2, 3, 4, 5]
for num in numeros:
    print(num)

# loops com range()
for i in range(5):
    print(i)

# loops com else
for num in numeros:
    if num % 2 == 0:
        print(f"{num} é um número par")
else:
    print("Todos os números foram processados.")


# Laço while
# while condição:
    # codigo a ser executado while a condicao bater

i = 1
while i <= 5:
    print(i)
    i += 1

# Instruções de controle
# break
minerio = ["Prata", "Cobre", "Ouro", "Pedra"]
for joia in minerio:
    if joia == "Ouro":
        print("Ouro encontrado! Parando o loop...")
        break
    print(joia)

# continue
nome_completo = "Bob Odenkirk"
iniciais = ""
for char in nome_completo:
    if not char.isupper():
        continue
    iniciais += char
print(iniciais)

# Loops aninhados
for i in range(1, 11):
    for j in range(1, 11):
        print(i * j, end='\t')
    print()


