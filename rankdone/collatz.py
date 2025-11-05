def collatz_length(x):
    count = 1
    while x > 1:
        if x % 2 == 0:
            x /= 2
        else:
            x = 3 * x + 1
        count += 1
    return count


max_length = 0
max_number = 0

for i in range(1, 100001):
    length = collatz_length(i)
    if length > max_length:
        max_length = length
        max_number = i

print(f"O maior tamanho de sequência de Collatz entre 1 e 100000 é {max_length}, para o número {max_number}.")
