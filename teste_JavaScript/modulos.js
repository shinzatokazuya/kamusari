// CommonJS
// nome do arquivo myModule.js
const myFunction = () => {
    console.log("Olá para meu módulo");
};

module.exports = myFunction;

// para importar modulo de outro arquivo usa-se require
// nome do arquivo = index.js
const myModule = require("./myModule");
myModule(); // Output: "Olá para meu módulo"

// ECMAScript (ESM)
// nome do arquivo = myModule2.js
export const myFunction2 = () => {
    console.log("Olá para meu módulo");
}

// importar modulo para outro arquivo
// nome do arquivo = index2.js
import { myFunction2 } from './myModule2';
myFunction2(); // Output: "Olá para meu módulo"

// AMD ou Assíncrono
// nome do arquivo = module-a.js
define('moduleA', function(){
    return { message: 'Olá para modulo A'};
});
// nome do arquivo = module-b.js
require(['moduleA'], function(moduleA) {
    console.log(moduleA.message); // Output: "Olá para modulo A"
})

// Declarações export e import
// moduleA.js
export const message = 'Olá de module A';

// moduleB.js
import { message } from './moduleA';
console.log(message);

// module.exports e require()
// moduleA2.js
module.exports = {
    message: 'Olá de modulo A',
    greeting: function () {
        console.log('Greetings!');
    },
};

// moduleB2.js
const moduleA = require('./moduleA2');
console.log(moduleA2.message) // Output: 'Olá de modulo A'
moduleA.greeting(); // Output: 'Greetings!'

// Exportações nomeadas e padrão
// math.js
export const sum = (a, b) => a + b;
export const multiply = (a, b) => a * b;

// em outro arquivo
import { sum, multiply } from './math';
console.log(sum(1, 2)); // Output: 3
console.log(multiply(1, 2)); // Output: 2

// greeting.js
export default () => 'Olá mundo!';

// em outro arquivo
import greeting from './greeting';
console.log(greeting()); // 'Olá mundo!'

// Renomeando Módulos
// myModule.js
export const myFunction3 = () => {
    console.log('Olá de meu modulo!');
};

// index3.js
import { myFunction3 as greeting } from './myModule';
greeting(); // Output: 'Olá de meu modulo!'