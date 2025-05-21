# Criando uma tupla
tuple_example = (1, 2, 3)

tuple_example = (1, "hello", 3.14, True)

print(type(tuple_example[0])) # Output: <class 'int'>
print(type(tuple_example[1])) # Output: <class 'str'>
print(type(tuple_example[2])) # Output: <class 'float'>
print(type(tuple_example[3])) # Output: <class 'bool'>

# Usando tuple()
new_tuple = tuple(("Python", "JavaScript", "C++"))

print(new_tuple)

# Output: ('Python', 'JavaScript', 'C++')

# tupla de elemento unico, mas precisa colocar uma virgula para identificar q é uma tupla
single_element_tuple = (1,)

print(single_element_tuple)

# Output: (1,)

# Descompactação de Tuplas
unpack_tuple = ("Python", 1991, "Guido van Rossum")

(language, year, creator) = unpack_tuple

print(language) # Output: Python
print(year) # Output: 1991
print(creator) # Output: Guido van Rossum
print(unpack_tuple) # Output ('Python', 1991, 'Guido van Rossum')

# Alterando tuplas
tuple_example = (1, 2, 3)

my_list = list(tuple_example)
my_list.append(4)

tuple_example = tuple(my_list)

print(tuple_example) # Output: (1, 2, 3, 4)

# combinando tuplas
tuple1 = (1, 2, 3)
tuple2 = (4, 5, 6)
tuple3 = tuple1 + tuple2
print(tuple3) # Output: (1, 2, 3, 4, 5, 6)

# metodos tupla
# count()
breakfast = ("waffles", "syrup", "bacon", "eggs", "syrup")
print(breakfast.count("syrup")) # Output: 2

# index()
pets = ("dog", "cat", "bird", "dog", "fish")
print(pets.index("dog")) # Output: 0

