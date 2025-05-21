level = int(input("Digite seu level: "))

if level >= 10:
    if level >= 20:
        print("Você é um Mago Grande.")
    else:
        print("Você é um Mago Sênior.")
else:
    if level >= 5:
        print("Você é um Mago Júnior.")
    else:
        print("Você é um Mago Aprendiz.")