// conversoes automaticas
let num = 42;
let str = "The answer is " + num;  // "The answer is 42"

let num = 42;
let str = "42";
console.log(num == str);  // true

// operador de igualdade estrita
let num = 42;
let str = "42";
console.log(num === str);  // false

// Conversão implícita vs. explícita
// implicita
let num = 42 + true; 
console.log(num);  // 43

// explicita
let num = parseInt("42");  // 42

// Métodos de conversão comumente usados ​​em JavaScript
// string
let num = 12;
let str = String(num);  // "12"

// .toString()
let num = 12;
let str = num.toString();  // "12"

// Conversão numerica
// Number()
let str = "42";
let num = Number(str);  // 42

// parseInt()
let str = "3";
let num = parseInt(str);  // 3

// parseFloat()
let str = "3.14";
let num = parseFloat(str);  // 3.14

// Conversão booleana
let bool = Boolean(0);  // false


