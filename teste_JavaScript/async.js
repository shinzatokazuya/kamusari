myAsyncFunction() 
    .then((result) => {
        console.log(result);
    })
    .catch((error) => {
        console.log(error);
    });


async function delayedLog() {
    await new Promise((resolve) => setTimeout(resolve, 1000));
    console.log('1 second has passed');
}
delayedLog();

async function getData() {
    try {
        const response = await fetch("https://jsonplaceholder.typicode.com/posts");
        const data = await response.json();
        console.log(data);
    } catch (error) {
        console.log(error)
    }
}
getData();

// manipular a entrada do usuario
async function handleSubmit(event) {
    event.preventDefault();
    const input = document.querySelector("input");
    const value = await new Promise((resolve) => {
      setTimeout(() => {
        resolve(input.value);
      }, 1000);
    });
    console.log(`You typed: ${value}`);
}
document.querySelector("form").addEventListener("submit", handleSubmit);

// lidar com registro e login do usuario
// Common function for making POST requests
const makeRequest = async (url, body) => {
    try {
      const response = await fetch(url, {
        method: 'POST',
        body: JSON.stringify(body),
        headers: { 'Content-Type': 'application/json' }
      });
      const data = await response.json();
      if (data.error) {
        throw new Error(data.error);
      }
      return data;
    } catch (error) {
      console.error(error);
    }
  }
  
  // Function for user registration
  const registerUser = async (email, password) => {
    // Make the POST request to register the user
    const data = await makeRequest('/api/register', { email, password });
    
    // Log success message
    console.log('User registered successfully');
    
    // Return the token
    return data.token;
  }
  
  // Function for user login
  const login = async (email, password) => {
    // Make the POST request to login the user
    const data = await makeRequest('/api/login', { email, password });
    
    // Log welcome message
    console.log(`Welcome ${data.name}!`);
    
    // Return the token
    return data.token;
}