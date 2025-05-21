// Quando uma propriedade se torna um método
const carro = {
    marca: "Mercedes-Benz", // propriedade
    modelo: "GLS", // propriedade
    descricao: function() { // metodo
        return this.marca + ' ' + this.modelo;
    }
};

console.log(carro.descricao()) // Output: Mercedes-Benz GLS

// Métodos de Objetos Integrados
// O Object.create() Método
const saudacaoPrototipo = {
    saudacao: function() {
        return 'Bem-vindo ' + this.name;
    }
};

const addInfo = Object.create(saudacaoPrototipo, {
    name: {
        value: 'web Reference!'
    }
});

console.log(addInfo.saudacao()); // Output: Bem-vindo Web Reference!

// O Object.assign() Método
const infoBasico = {
    nome: "Sara Vasquez",
    idade: 20
};

const infoEndereco = {
    rua: "Rio Preto",
    cidade: "Santo André",
    estado: "SP"
};

const infoTotal = Object.assign({}, infoBasico, infoEndereco);

console.log(infoTotal); // Output: { nome: 'Sara Vasquez', idade: 21, rua: 'Rio Preto', cidade: 'Santo André', estado: 'SP' }

// O Object.keys() Método
const filme = {
    filme: "Matrix",
    diretor: "Lana Wachowski",
    anoLancamento: 1999
};

const chavesFilme = Object.keys(filme);

console.log(chavesFilme); // Output: [ "filme", "diretor", "anoLancamento" ]

// O Object.entries() Método
// retorna as chaves e os valores do objeto na forma de matrizes.
const entradasFilme = Object.entries(filme);

console.log(entradasFilme); // Output: [ [ "filme", "Matrix" ], [ "diretor", "Lana Wachowski" ], [ "anoLancamento", 1999 ] ]

// O Object.values() Método
let estudante = {
    nome: "Sara Vasquez",
    idade: 20,
    nota: "A",
    materias: ["Russo", "Português", "Pedagogia"]
};

let valorEstudante = Object.values(estudante);

console.log(valorEstudante); // Output: [ 'Sara Vasquez', 20, 'A', [ 'Russo', 'Português', 'Pedagogia' ] ]

