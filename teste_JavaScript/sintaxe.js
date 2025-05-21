// White Space
let x =    5;   // x is 5

// Semicolons
let x = 5 
let y = 6; // Semicolons are optional but recommended

// Case Sensitivity
let myString = "hello";
let mystring = "world"; // These are different variables
console.log(myString); // Outputs: "hello"
console.log(mystring); // Outputs: "world"

// declarações de variaveis
var x; // declares a variable named x
let y = 5; // declares a variable named y and assigns it the value of 5
const z = "hello"; // declares a constant variable named z and assigns it the value of "hello"

// Estruturas de controle
/* 
if instruções: usadas para executar uma ação específica com base em uma condição dada.

for loops: usados ​​para iterar sobre uma coleção de itens.

while loops: usados ​​para repetir um bloco de código enquanto uma determinada condição for verdadeira.

switch instruções: usadas para executar diferentes ações com base em um valor específico.
*/

// Funções
function add(x, y) {
    return x + y;
  }
  
  let result = add(2, 3); // result = 5

// JavaScript também suporta funções de seta, que fornecem uma sintaxe abreviada para definir funções.
function add(x, y) => { return x + y; }

let result = add(2, 3); // result = 5

let numbers = [1, 2, 3, 4, 5];
numbers.forEach((number) => {
  console.log(number);
});

// Tipos de Dados
// Primitivos => numeros, strings e booleanos
let x = 5;
let y = 3.14;

let name = "John Doe";
let greeting = "Hello, " + name + "!";

let isTrue = true;
let isFalse = false;

// Tipos de Objetos => Matrizes, obejtos, simbolo, BigInt
let colors = ["red", "green", "blue"];
let numbers = [1, 2, 3, 4, 5];

let person = {
    name: "John Doe",
    age: 30,
    city: "New York"
};

let sym = Symbol();

// BigInt is used to represent integers larger than 2^53-1.
