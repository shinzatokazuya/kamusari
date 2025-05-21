# listas
nums = [1, 2, 3, 4, 5]

nomes = ["Sara", "Bob", "Carlos"]

data = [1, "dois", True,]

print(type(data[0])) #Output: <class 'int'>
print(type(data[1])) #Output: <class 'str'>
print(type(data[2])) #Output: <class 'bool'>

print(nomes[1]) #Output: Bob

# Usando len() = encontrar o numero de elementos qua a lista contém
print(len(nomes)) #Output: 3

# Iterando por uma lista
for num in nums:
    print(num)

nums = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
cubo_pares = [x**3 for x in nums if x % 2 == 0]
print(cubo_pares) #Output: [8, 64, 216, 512, 1000]

# Usando o construtor list()
lista_compras = "bananas, maças, laranjas, pão, leite"
itens = list(lista_compras.split(", "))
print(itens) #Output: ['bananas', 'apples', 'oranges', 'bread', 'milk']

# Add elementos em uma lista
nomes.append("David")
print(nomes) #Output: ["Sara", "Bob", "Charlie", "David"]

# Removendo elementos de uma lista
# pop remove sempre o ultimo
nomes.pop()
print(nomes) #Output: ["Sara", "Bob", "Charlie"]

# remove() ou del() para especificar qual elemento remover
nomes.remove("Bob")
print(nomes) #Output: ["Sara", "Charlie"]

del nomes[1]
print(nomes) #Output: ["Sara"]

# limpar a lista e deixa-la vazia
nomes.clear()
print(nomes) #Output: []

# Fatiamento de lista
nums = [1, 2, 3, 4, 5]
print(nums[:3]) #Output: [1, 2, 3]

print(nums[-3:]) #Output: [3, 4, 5]

# Classificando elementos em uma lista
# sort() classifica a lista em ordem crescente
nomes = ["Sara", "Charlie", "Bob"]
nomes.sort()
print(nomes) #Output: ["Bob", "Charlie", "Sara"]

nums = [5, 2, 1, 4, 3]
nums.sort()
print(nums) #Output: [1, 2, 3, 4, 5]

# Fazer em ordem decrescente
nums = [5, 2, 1, 4, 3]
nums.sort(reverse=True)
print(nums) #Output: [5, 4, 3, 2, 1]