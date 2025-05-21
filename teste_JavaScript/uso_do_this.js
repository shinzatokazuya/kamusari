let obj = {
    name: "My Object",
    logName: function() {
      console.log(this.name);
    }
  };
  
  obj.logName(); // Output: "My Object"

// Contexto do Método
  let greeting = {
    message: "Hello",
    showMessage: function() {
      console.log(this.message);
    }
  };
  
  greeting.showMessage(); // Output: "Hello"


// Contexto de construtor e classe
class Greeting {
    constructor(message) {
        this.message = message;
    }

    showMessage() {
        console.log(this.message);
    }
}

let gretting = new Gretting("Hello");
gretting.showMessage(); // Output: "Hello"

// Contexto global
var message = "Global";

function showMessage() {
  console.log(this.message);
}

showMessage(); // Output: "Global"

// Manipuladores de eventos com this
let button = document.getElementById('myButton');
button.addEventListener("click", function() {
    console.log(this); // Output: <button id="myButton">
})

// Usando this no Modo Estrito
// this dentro de uma função autônoma resulta undefined em vez do objeto global
"use strict"
function logThis(){
  console.log(this)
}
logThis(); // Output: undefined

// this em uma função de seta não pode ser modificado usando call, apply, ou bind.
"use strict"
var obj = {
  message: "Hello",
  showMessage: () => {
    console.log(this.message);
  }
};
var message = "Global";
obj.showMessage.call({message: "Hello"}); // Output: "Global"

// this dentro de um construtor de classe e método de classe refere-se à instância da classe.
class MyClass {
    constructor(name) {
      this.name = name;
    }
  
    logName() {
      console.log(this.name);
    }
  }
  let myInstance = new MyClass("My Instance");
  myInstance.logName(); // Output: "My Instance"