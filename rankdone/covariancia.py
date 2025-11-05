import math

correlacao = 0.45
variancia_x = 2
variancia_y = 6

desvio_x = math.sqrt(variancia_x)
desvio_y = math.sqrt(variancia_y)

covariancia = correlacao * desvio_x * desvio_y

print(f"A covariância entre X e Y é aproximadamente {covariancia}.")
