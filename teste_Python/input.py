nome = input("Qual o seu nome? ")
print(f"Prazer em te conhecer {nome}!")

# convertendo valor do input
idade = int(input("Quantos anos você tem? "))
print(f"Uau! você já tem {str(idade)} anos.")

altura = float(input("Qual sua altura? (em Metros) "))
print(f"Sua altura é {str(altura)}m.")

# Jogo de adivinhar o número
import random

print("Bem-Vindo ao Jogo de adivinhar o número!")
print("Eu estou pensando em um número entre 1 a 100. Você consegue adivinhar?")

numero = random.randint(1, 100)
chute = 0
tentativas = 0

while chute != numero:
    tentativas += 1
    chute = int(input("Entre com seu palpite: "))
    if chute < numero:
        print("Muito baixo, tente novamente!")
    elif chute > numero:
        print("Muito alto, tente novamente!")

print(f"Parabéns! Você adivinhou o número em {tentativas} tentativas.")
