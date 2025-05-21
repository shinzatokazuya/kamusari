# usando a função print()
print('Python')


# Para criar um função em python utiliza-se def palavra-chave
def saudar():
    print('Olá Python')

# Chamando a função
saudar() # Olá Python

# Criando uma função definida pelo usuario
def nome_funcao(parametros):
    # corpo da declaração
    return expressao
    # retorno da funcao

# Chamando a funcao definida pelo usuario
chamando_funcao(argumentos)

# funcao que recebe um numero como argumento retorna resultado de uma operação de divisao
def dividido_por_2(num):
    resultado = num / 2
    return resultado

# Chamando a funcao com um argumento
divisao = dividido_por_2(10)
# Escrevendo o valor da divisao
print(divisao) # 5.0

# Expandindo
def saudacao(nome):
    return 'Obrigado' + nome

# Chamando a funcao saudacao com argumento
sauda = saudacao('João')
# Escrevendo o valor da variavel
print(sauda)

# argumentos padrão recebem automaticamente um valor padrão se não fornecermos um valor quando a função for chamada
def descricao_carro(make='Tesla'):
    print('O carro é um ' + make)

descricao_carro('Ford') # O carro é um Ford
descricao_carro() # O carro é um Tesla 