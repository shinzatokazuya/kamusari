# Fazendo dicionario com chaves {}
my_dict = {}

# Add elementos
my_dict['Maça'] = 0.5
my_dict['Banana'] = 0.25
my_dict['Laranja'] = 0.75


# usando o construtor dic()
my_dict = dict(maça=0.5, Banana=0.25, Laranja=0.75)
                 # ↑
my_dict = {fruta: preco for fruta, preco in [('Maça', 0.5), ('Banana', 0.25), ('Laranja', 0.75)]}

# acessando elementos do dicionario
print(my_dict['Maça']) # 0.5

print(my_dict.get('Maça')) # 0.5
print(my_dict.get('Morango')) # vazio

# Verificar se existe a palavra dentro do dicionario
if 'Maça' in my_dict:
    print(my_dict['Maça'])
else:
    print('Chave não encontrada')

# Acessando todas as chaves e valores do dicionario
print(my_dict.keys()) # ['Maça', 'Banana', 'Laranja']
print(my_dict.values()) # [0.5, 0.25, 0.75]

# Obter uma lista de pares chave-valor
print(my_dict.items()) # ['Maça', 0.5, 'Banana', 0.25, 'Laranja', 0.75]

# Metodos Integrados
# Remove todos os itens do dicionario
my_dict.clear()

# Cópia superficial do dicionario
new_dict = my_dict.copy()

# Cria um novo dicionario 
new_dict = dict.fromkeys(['Maça', 'Banana', 'Laranja'], 0)

# Retorna o valor da chave, se ela existir no dicionario
print(my_dict.get('Maça'))

# Remove e retorna um elemento com a chave especificada
print(my_dict.pop('Maça'))

# Removee retorna um par arbitrário (chave, valor) do dicionário
print(my_dict.popitem())

# Retorna o valor da chave, se existir no dicionário; caso contrário, adiciona uma chave com um valor padrão ao dicionário
print(my_dict.setdefault(['Maça'], 0.5))

# Atualiza o dicionário com pares chave-valor de outros, sobrescrevendo as chaves existentes
my_dict.update({'Morango': 1.0, 'Kiwi': 0.5})

