// console.log()
let nome = "João da Silva";
console.log('Olá, ' + nome);

// document.write()
document.write("<ul>");
document.write("<li>Apples</li>");
document.write("<li>Oranges</li>");
document.write("<li>Bananas</li>");
document.write("</ul>");

// innerHTML
<ul id="fruits">
  <li>Apples</li>
  <li>Oranges</li>
</ul>
let newFruit = "Bananas";
document.getElementById("fruits").innerHTML += "<li>" + newFruit + "</li>";

// window.alert()
let message = 'Olá.';
window.alert(message); // pode ser tbm apenas alert()
