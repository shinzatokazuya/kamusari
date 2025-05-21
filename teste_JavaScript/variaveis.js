// Criando variavel
let message = "Hello!";
message = "World!"; 
console.log(message); // "World!"

// Alterando a variavel
message = null;
console.log(message); // null

// Copiando e movendo para outra variavel
let hello = "Hello world!";
message = hello;
console.log(hello); // "Hello world!"
console.log(message); // "Hello world!"

// var
var x = "I am a global variable";

function myFunction() {
  var y = "I am a local variable";
  console.log(x);  // "I am a global variable"
  console.log(y);  // "I am a local variable"
}

// let
let x = "I am a global variable";

function myFunction() {
  let y = "I am a local variable";
  console.log(x);  // "I am a global variable"
  console.log(y);  // "I am a local variable"
}

// variavel global
let globalVar = "I am a global variable";

function myFunction() {
  console.log(globalVar);  // "I am a global variable"
}

// variavel local
function myFunction() {
    let localVar = "I am a local variable";
    console.log(localVar);  // "I am a local variable"
  }
  
  console.log(localVar);  // ReferenceError: localVar is not defined

