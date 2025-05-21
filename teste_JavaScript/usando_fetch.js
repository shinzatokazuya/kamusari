fetch('/data')
    .then(response => response.json())
    .then(data => {
        console.log(data);
    });
    