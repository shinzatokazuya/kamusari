# função lambda ou função anônimas
lambda arguemntos: expressao

add = lambda x, y: x + y
print(add(3, 4)) #Output: 7


multiplicacao = lambda x, y, z: x * y * z
print(multiplicacao(2, 3, 4)) #Output: 24


# Combinando funções regulares e lambda
def criando_funcao(n):
    return lambda x: x + n 

add_5 = criando_funcao(5)
add_10 = criando_funcao(10)

print(add_5(3)) #Output: 8
print(add_10(3)) #Output: 13

# Funções lambda em objetos iteráveis
# map()
numeros = [1, 2, 3, 4, 5]
quadrado_numeros = list(map(lambda x: x**2, numeros))
print(quadrado_numeros) #Output: [1, 4, 9, 16, 25]

# filter()
numeros = [1, 2, 3, 4, 5]
numeros_pares = list(filter(lambda x: x %2 == 0, numeros))
print(numeros_pares) #Output: [2, 4]

# reduce()
from functools import reduce
numeros = [1, 2, 3, 4, 5]
produto = reduce(lambda x, y: x * y, numeros)
print(produto) #Output: 120