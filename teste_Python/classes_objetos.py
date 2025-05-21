class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def Introduce(self):
        print(f"Olá meu nome é {self.name} e eu tenho {self.age} anos.")

person = Person("Kazuya", 20)

person.Introduce()