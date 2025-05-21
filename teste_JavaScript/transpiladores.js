const items = [{name: 'item1', price: 10}, {name: 'item2', price: 20}];
const displayItems = items.map(({ name, price }) => `${name}: $${price}`);
console.log(displayItems); // Output: ['item1: $10', 'item2: $20']

// Transpilado com Babel
"use strict";

var items = [{name: 'item1', price: 10}, {name: 'item2', price: 20}];
var displayItems = items.map(function(item) {
    var name = item.name,
        price = item.price;

    return name + ': $' + price;
});
console.log(displayItems); // ['item1: $10', 'item2: $20']