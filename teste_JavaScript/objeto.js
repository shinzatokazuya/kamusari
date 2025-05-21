// Criando objeto do mais mais comum
let meuObjeto1 = {
    chave1: "valor1",
    chave2: "valor2",
    chave3: "valor3"
};

// Usando o object() construtor
let meuObjeto2 = new Object();
meuObjeto.chave1 = "valor1";
meuObjeto.chave2 = "valor2";
meuObjeto.chave3 = "valor3";

// método de desestruturação de objetos
const myObject = {key1, key2, key3};

// Propriedades são os valores armazenados em um objeto e podem ser acessadas usando a notação de ponto ou a notação de colchetes.
console,log(meuObjeto.chave1);
console.log(meuObjeto["chave1"]);

// Metodos
// Métodos são funções armazenadas como propriedades dentro de um objeto.
let meuObjeto3 = {
    chave1: "valor1",
    chave2: "valor2",
    chave3: "valor3",

    meuMetodo: function() {
        console.log("Esse é o meu método.")
    }
}

meuObjeto3.meuMetodo();

// Herança
let meuObjeto4 = {
    chave1: "valor1",
    chave2: "valor2",
    chave3: "valor3",
};

let meuSegundoObjeto = Object.create(meuObjeto4);
console.log(meuSegundoObjeto.chave1) // Output: 'valor1'

// Exemplos comuns de Objetos
// Simples objeto que armazena os dados do usuario
let usuario = {
    nome: "Sara Vasquez",
    email: "saravasquez@email.com",
    idade: 20
};

// Obejeto mais complexo que represena o carrinho de compras
let carrinho = {
    itens: [
      { nome: "item1", preco: 12.99},
      { nome: "item2", preco: 24.99},
      { nome: "item3", preco: 9.99}
    ],
    total: function() {
        let soma = 0;
        for (let i = 0; i < this.itens.length; i++) {
            soma += this.itens[i].price;
        }
        return soma;
    }
};

console.log(carrinho.total());