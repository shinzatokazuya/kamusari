# Ler arquivos em JSON
import json

# Abrir o arquivo
with open("exemplo.json", "r") as file:
    # Carregue o JSON data do arquivo
    data = json.load(file)

# Escreve a data
print(data)

# Convertendo JSON em Python
import json

# JSON string
json_string = '{"Empregado": "Bob", "idade": 28, "Trabalho": "designer"}'

# Desserializar o JSON string
data = json.loads(json_string)

# Escreve a data
print(data["age"]) #Output: 28

# Transformando Python em JSON
import json

# Objeto Python
dict_example = {
    "nome": "Little João",
    "idade": 29,
    "desenvolvedor": True
}

# Serializar o objeto Python
json_string = json.dumps(dict_example, indent=4)

# Escrever o JSON string
print(json_string)
# Output: {"name": "Guido van Rossum", "age": 67, "developer": true}
# indent argumento para especificar o número de espaços a serem usados para recuo
# com o indent a Saída é assim:
# Output:
# {
#    "name": "Guido van Rossum",
#    "age": 67,
#    "developer": true
# }

