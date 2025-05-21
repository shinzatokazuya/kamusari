import sys
print(sys.path)

# modulo aleatorio
import random

print(random.random()) # gera um float aleatorio entre 0 e 1

print(random.randint(1, 10)) # gera um inteiro aleatorio entre 1 e 10

print(random.choice(["Python", "JavaScript", "Java"])) # seleciona um item aleatorio da lista

# modulo datetime
import datetime

now = datetime.datetime.now()

print(now) # escreve a data atual e o tempo

print(now.year) # escreve o ano atual

print(now.strftime("%Y-%m-%d")) # escreve a data no formato YYYY-MM-DD

# Explorando o conteúdo do módulo
import random

print(dir(random))

# Recorrendo a módulos de terceiros
# Abra o Terminal e digite:
# pip install modulo_name

import requests

response = requests.get("https://jsonplaceholder.typicode.com/todos/1")
data = response.json()

print(f"Task title: {data['title']}")
print(f"Completed: {'Yes' if data['title'] else 'No'}")

# Criando Módulos Personalizados
# Para criar um módulo personalizado, basta escrever seu código Python em um arquivo separado com a .py extensão
# meu_modulo.py
def fatorial(n):
    if n == 0 or n == 1:
        return 1
    return n * fatorial(n - 1)

import meu_modulo

resultado = meu_modulo.fatorial(5)

print(f"Fatorial de 5: {resultado}")
