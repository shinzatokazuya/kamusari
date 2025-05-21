# GRRAFICOS
# Matplotlib
import matplotlib.pyplot as plt

x = [1, 2, 3, 4]
y = [10, 20, 25, 30]

plt.plot(x, y)
plt.title('Gráfico simples')
plt.xlabel('Eixo X')
plt.ylabel('Eixo Y')
plt.show()

# Pygame
import pygame

pygame.init()
tela = pygame.display.set_mode((400, 300))
pygame.display.set_caption("Janela com Pygame")

rodando = True
while rodando:
    for evento in pygame.event.get():
        if evento.type == pygame.QUIT:
            rodando = False
    tela.fill((0, 0, 0))  # preto
    pygame.draw.circle(tela, (255, 0, 0), (200, 150), 50)  # círculo vermelho
    pygame.display.flip()
pygame.quit()

# thinker
import tkinter as tk

janela = tk.Tk()
janela.title("Janela com Tkinter")
canvas = tk.Canvas(janela, width=400, height=300)
canvas.pack()
canvas.create_oval(150, 100, 250, 200, fill="blue")
janela.mainloop()

# EFEITOS SONOROS
# Pygame
import pygame

pygame.init()
pygame.mixer.init()
pygame.mixer.music.load("som.mp3")  # substitua pelo caminho do seu arquivo
pygame.mixer.music.play()

# Espera enquanto o som toca
while pygame.mixer.music.get_busy():
    pygame.time.Clock().tick(10)

# Playsound
from playsound import playsound

playsound("som.mp3")  # funciona com .mp3 e .wav

# Sounddevice + Numpy
import sounddevice as sd
import numpy as np

fs = 44100  # frequência de amostragem
duracao = 1  # segundos
frequencia = 440  # Hz (Lá)

t = np.linspace(0, duracao, int(fs * duracao), False)
onda = 0.5 * np.sin(2 * np.pi * frequencia * t)

sd.play(onda, fs)
sd.wait()