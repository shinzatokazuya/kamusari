import requests
from bs4 import BeautifulSoup
import time

urls = ['https://www.https://www.google.com/search?q=ogol.com.br/edicao/campeonato-brasileiro-serie-a-2004/457', 'URL_DA_PROXIMA_PAGINA', ...]
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

for url in urls:
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')

        # Seu código para extrair dados da página 'soup' aqui
        tables = soup.find_all('table', class_='zztable stats zz-datatable dataTable no-footer')
        print(f"Dados da URL: {url}")
        for table in tables:
            print(f"- {table.text[:100]}...")

        time.sleep(5) # Espera 5 segundos antes de ir para a próxima URL

    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a URL {url}: {e}")
        time.sleep(10) # Espera um pouco mais em caso de erro
    except AttributeError as e:
        print(f"Erro ao encontrar elemento na URL {url}: {e}")
        time.sleep(5)
