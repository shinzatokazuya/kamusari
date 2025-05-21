nums = [1, 2, 2, 3, 4] # lista com numero duplicado
valores_unicos = set(nums) # construtor set
print(valores_unicos) # Output: [1, 2, 3, 4]

novo_set = {"Python", "JavaScript", "Java"}
print(novo_set) # Output: {"Python", "JavaScript", "Java"}
print(type(novo_set)) # Output: <class 'set'>

# acessando elementos 
languages = {"Python", "JavaScript", "Java"}

for elements in languages:
    print(elements)

languages = {"Python", "JavaScript", "Java"}

print("Python" in languages) # Output: True

# Adicionar e remover elementos
example_set = {"pizza", "burger", "tacos"}
example_set.add("ramen")
print(example_set)

example_set = {"pizza", "burger", "tacos", "ramen"}
example_set.remove("burger")
print(example_set)

# Operações Matemáticas
first_set = {1, 2, 3, 4, 5}
second_set = {5, 7, 9}

# Union using the pipe operator (|)
print(first_set | second_set) # {1, 2, 3, 4, 5, 7, 9}

# Intersection using the ampersand operator (&)
print(first_set & second_set) # {5}

# Difference using the minus operator (-)
print(first_set - second_set) # {1, 2, 3, 4}

# Symmetric Difference using the caret operator (^)
print(first_set ^ second_set) # {1, 2, 3, 4, 7, 9}

# Union using the union method
print(first_set.union(second_set)) # {1, 2, 3, 4, 5, 7, 9}

# Intersection using the intersection method
print(first_set.intersection(second_set)) # {5}

# Difference using the difference method
print(first_set.difference(second_set)) # {1, 2, 3, 4}