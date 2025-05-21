// aparando strings
let myString = '   Web Reference is live!   ';
console.log(myString.trim());  // Output: 'Web Reference is live!'

let myString = '   Learn JavaScript!   ';
console.log(myString.trimStart());  // Output: 'Learn JavaScript!   '
console.log(myString.trimEnd());  // Output: '   Learn JavaScript!'

// substituindo e extraindo strings
let myString = 'Oh, coding is hard.';
console.log(myString.replace('hard', 'challenging')); 
// Output: 'Oh, coding is challenging.'
let myString = 'Separate wheat from chaff';
console.log(myString.slice(0, 14));  // Output: 'Separate wheat'

// Pesquisando, Indexando e Localizando
let myString = 'String cheese';
console.log(myString.indexOf('cheese'));  // Output: 7
console.log(myString.lastIndexOf('e'));  // Output: 12
console.log(myString.includes('String')); // Output: 0

let myString = 'We won!';
console.log(myString.charAt(0));  // Output: 'W'

// Mudança de maiúsculas e minúsculas
let myString = 'Shapeshifting Shenanigans';
console.log(myString.toLowerCase());  // Output: 'shapeshifting shenanigans'
console.log(myString.toUpperCase());  // Output: 'SHAPESHIFTING SHENANIGANS'