# criando um set unico de cidades
cities = {'New York', 'London', 'Paris', 'Berlin'}

# adicionando um elemento
cities.add('Tokyo')
print(cities)

# adicionar uma duplicata no set (nao vai funcionar)
cities.add('London')
print(cities)

# tentando acessar os elementos usando indexing (vai resultar em erro)
city = cities[0]
print(city)