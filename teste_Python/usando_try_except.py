#try:
    # código que pode gerar uma exceção
#except ExceptionType:
    # código para lidar com essa exceção
#finally:
    # segmentos específicos de código sejam executados, independentemente de uma exceção ser gerada


try:
    num1 = int(input("Entre com um número: "))
    num2 = int(input("Entre com outro número: "))

    resultado = num1 / num2

    print(resultado)

except ZeroDivisionError:
    print("Você não pode dividir por zero!")
except ValueError:
    print("número inválido. Por favor, entre com um número inteiro.")


# bloco finally
try:
    file = open("data.txt", "r")
    data = file.read()
    print(data)
except FileNotFoundError:
    print("Arquivo não encontrado!")
#finally:
    #file.close()

# Gerando exceções
# raise ExceptionType("Error message")

def saudar(nome):
    if not nome:
        raise ValueError("Nome não pode estar vazio.")
    return f"Olá, {nome}!"

try:
    mensagem = saudar("")
except ValueError as error:
    print(error)

# Exceções personalizadas
class CustomException(Exception):
    pass 

class InvalidInputError(Exception):
    def __init__(self, mensagem):
        self.mensagem = mensagem

try:
    idade = int(input("Qual a sua idade? "))
    if idade < 0:
        raise InvalidInputError("Idade não pode ser negativa.")
except InvalidInputError as error:
    print(error.mensagem)