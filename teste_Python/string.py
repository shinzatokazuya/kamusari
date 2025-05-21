my_string = "Hello, World!"
print(my_string[0])  # Output: H

my_string = "Python is awesome!"
for char in my_string:
    print(char)

multi_line_string = '''This is a string
that spans
multiple lines.'''
print(multi_line_string)

# Output:
#This is a string
#that spans
#multiple lines.

# Using the escape sequence for a single quote
my_string = "She said, \"Hello!\""
print(my_string)  # Output: She said, "Hello!"

# Using the escape sequence for a newline and a tab
my_string = 'First line.\nSecond line.\n\tIndented line.'
print(my_string)  

# Output:
#First line.
#Second line.
#    Indented line.
#\n PULA LINHA \t PARAGRAFO

# len() function
my_string = "Python is awesome!"
print(len(my_string))  # Output: 18

# Além disso, podemos verificar e pesquisar uma substring específica em uma string maior usando as palavras-chave ine not in.
sentence = "The quick brown fox jumps over the lazy dog"
print("fox" in sentence)  # Output: True

if "cat" not in sentence:
    print("No cats present here")

# Output: No cats present here

# usando upper() e lower() function
my_string = "Python is awesome!"
print(my_string.upper())  # Output: PYTHON IS AWESOME!
print(my_string.lower())  # Output: python is awesome!

# substituir uma parte da string original e inserir uma nova substring com o replace()
my_string = 'Python is awesome!'
new_string = my_string.replace('awesome', 'incredible')
print(new_string)  # Output: 'Python is incredible!'

# podemos dividi-lo em uma lista de substrings com o split()
my_string = 'This is a long string with several words'
words = my_string.split(' ')
print(words)  # Output: ['This', 'is', 'a', 'long', 'string', 'with', 'several', 'words']

# corte de strings
my_string = "This is a long string with several words"
substring = my_string[8:18]
print(substring)  # Output: a long str

my_string = "This is a long string with several words"
substring = my_string[-6:-1]
print(substring)  # Output: word

# Formatação de strings
name = "Bob"
age = 30
message = "My name is %s and I am %d years old." % (name, age)
print(message)  # Output: My name is Bob and I am 30 years old.

name = "Bob"
age = 30
message = f"My name is {name} and I am {age} years old."
print(message)  # Output: My name is Bob and I am 30 years old.

name = "Bob"
age = 30
message = "My name is {} and I am {} years old.".format(name, age)
print(message)  # Output: My name is Bob and I am 30 years old.

