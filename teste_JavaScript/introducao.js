// usando o objeto construtor
var usuario = new Object();
usuario.nome = "André Marques";
usuario.email = "andremarques@example.com";
usuario.idade = 35;
usuario.login = function() {
    // codigo de entrada do login
}
usuario.logout = function() {
    // codigo de saida do login
}

// usando class palavra-chave
class Usuario {
    constructor() {
        this.nome = "André Marques";
        this.email = "andremarques@example.com";
        this.idade = 35;
    }

    login() {
        // codigo de entrada do login
    }

    logout() {
        // codigo de saida do login
    }
}

var usuario = new Usuario();
console.log(usuario.nome); // "André Marques"
usuario.login();

// HERANÇA
class Admin extends Usuario {
    constructor() {
        super(); // chama o metodos do Usuario
        this.isAdmin = true; // add novo propriedade
    }

    deleteUsuario(usuario) {
        // codigo de deletar
    }
}

// Programação Assíncrona
function doSomething(callback) {
    // perform some task
    callback(); // call the callback function when the task is done
}

doSomething(function() {
    // code to be executed when doSomething() is done
});

// Promise
function doSomethingAsync() {
    return new Promise(function(resolve, reject) {
        // perform some asynchronous task
        if (// task was successful ) {
            resolve(// result ); // resolve the promise with the result
        } else {
            reject(// error ); // reject the promise with an error
        }
    });
}

doSomethingAsync()
    .then(function(result) {
        // code to be executed when the promise is resolved
    })
    .catch(function(error) {
        // code to be executed when the promise is rejected
    });

// Modulos JS
// util.js
export function add(x, y) {
    return x + y;
}

export function subtract(x, y) {
    return x - y;
}

// main.js
import { add, subtract } from "./util";

console.log(add(10, 20)); // 30
console.log(subtract(10, 20)); // -10
