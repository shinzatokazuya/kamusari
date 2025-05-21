# criando um dicionario
dict_example = {
    "name": "Guido van Rossum",
    "age": 67,
    "city": "Amsterdam"
}

employees = {
    "John Doe": {
        "age": 30,
        "city": "New York"
    },
    "Jane Doe": {
        "age": 25,
        "city": "Los Angeles"
    }
}

john_doe = employees["John Doe"]
print(john_doe["age"]) 
print(john_doe["city"])

print(employees["John Doe"]["age"])

print(dict_example["name"])