const promise = new Promise((resolve, reject) => {
    // processo assincrono
    if (success) {
        resolve(result);
    } else {
        reject(error);
    }
});

// Objeto Prometido
promise.then((result) => {
    console.log(result);
});

promise.catch((error) => {
    console.log(error);
});

// Encadeamento de Promessas
promise
    .then((result) => {
        // faça algo com o resultado
        return new Promise((resolve) => {
            resolve(result);
        });
    })
    .then((result) => {
        // faça outro coisa com o resultado
        console.log(result);
    });
    .catch((error) => {
        console.log(error);
    });

// Promisificação
function callbackFunction() {
    // operação assincrona
    callback(result);
}

const promisifiedFunction = (arg) => {
    return new Promise((resolve, reject) => {
        callbackFunction(arg, (result) => {
            resolve(result);
        });
    });
};

// Exmeplos de Uso Moderno
fetch('https://jsonplaceholder.typicode.com/todos/1')
    .then((response) => {
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.json();
    })
    .then((data) => {
        console.log(`Fetched todo: ${data.title}`);
    })
    .catch((error) => {
        console.log(`Error: ${error}`);
    });

