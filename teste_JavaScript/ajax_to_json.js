var xhr = new XMLHttpRequest();
xhr.open('GET', '/data', true);
xhr.onreadystatechange = function() {
    if (xhr.readyState === 4 && xhr.status === 200) {
        var data = JSON.parse(xhr.responseText);
        document.getElementById('name').innerHTML = data.name;
        document.getElementById('age').innerHTML = data.age;
        document.getElementById('email').innerHTML = data.email;
    }
};
xhr.send();