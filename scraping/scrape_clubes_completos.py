import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import unicodedata
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configura sessão com retries
def create_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    return session

# Normaliza nome para URL
def normalize_name(nome):
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('utf-8')
    nome = re.sub(r'\s+', '-', nome.lower()).strip('-')
    return nome

# Extrai dados do clube
def get_clube_data(clube_id, nome, cidade, estado, regiao):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    session = create_session()
    normalized_nome = normalize_name(nome)
    clube_url = f"https://www.ogol.com.br/equipe/{normalized_nome}/"
    try:
        response = session.get(clube_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extrai ID_Ogol
        jogadores_link = soup.find('a', href=re.compile(r'/jogadores'))
        id_ogol = 'N/A'
        if jogadores_link:
            match = re.search(r'equipe/[^/]+/(\d+)/jogadores', jogadores_link['href'])
            id_ogol = match.group(1) if match else 'N/A'

        # Nome completo
        nome_completo_elem = soup.find('h1')
        nome_completo = nome_completo_elem.text.strip() if nome_completo_elem else nome

        # Ano de fundação
        fundacao = 'N/A'
        fundacao_elem = soup.find('span', string=re.compile(r'Fundação', re.I))
        if fundacao_elem:
            fundacao_text = fundacao_elem.find_parent('div').text
            match = re.search(r'\d{4}', fundacao_text)
            fundacao = match.group(0) if match else 'N/A'

        # Cores
        cores = 'N/A'
        cores_elem = soup.find('span', string=re.compile(r'Cores', re.I))
        if cores_elem:
            cores_text = cores_elem.find_parent('div').text
            cores = cores_text.replace('Cores:', '').strip() if cores_text else 'N/A'

        return {
            'ID': clube_id,
            'Nome': nome,
            'Nome_Completo': nome_completo,
            'Cidade': cidade,
            'Estado': estado,
            'Região': regiao,
            'Ano_Fundação': fundacao,
            'Cores': cores,
            'ID_Ogol': id_ogol
        }
    except Exception as e:
        print(f"Erro ao acessar {clube_url}: {e}")
        with open('failed_urls.txt', 'a', encoding='utf-8') as f:
            f.write(f"Erro no clube: {clube_url} - {str(e)}\n")
        return {
            'ID': clube_id,
            'Nome': nome,
            'Nome_Completo': nome,
            'Cidade': cidade,
            'Estado': estado,
            'Região': regiao,
            'Ano_Fundação': 'N/A',
            'Cores': 'N/A',
            'ID_Ogol': 'N/A'
        }

# Função principal
def main():
    # Tabela de clubes fornecida (adapte com os 557 clubes)
    clubes_data = [
        {'ID': 1, 'Nome': 'Grêmio', 'Cidade': 'Porto Alegre', 'Estado': 'RS', 'Região': 'Sul'},
        {'ID': 2, 'Nome': 'Santos', 'Cidade': 'Santos', 'Estado': 'SP', 'Região': 'Sudeste'},
        {'ID': 3, 'Nome': 'Atlético Mineiro', 'Cidade': 'Belo Horizonte', 'Estado': 'MG', 'Região': 'Sudeste'},
        # ... Adicione os outros clubes (ou leia de um CSV)
        4|Palmeiras|São Paulo|SP|Sudeste
        5|Cruzeiro|Belo Horizonte|MG|Sudeste
        6|Botafogo|Rio de Janeiro|RJ|Sudeste
        7|Fluminense|Rio de Janeiro|RJ|Sudeste
        8|Flamengo|Rio de Janeiro|RJ|Sudeste
        9|Internacional|Porto Alegre|RS|Sul
        10|São Paulo|São Paulo|SP|Sudeste
        11|Corinthians|São Paulo|SP|Sudeste
        12|Vasco da Gama|Rio de Janeiro|RJ|Sudeste
        13|Bahia|Salvador|BA|Nordeste
        14|Athletico Paranaense|Curitiba|PR|Sul
        15|Sport|Recife|PE|Nordeste
        16|Goiás|Goiânia|GO|Centro-Oeste
        17|Coritiba|Curitiba|PR|Sul
        18|Vitória|Salvador|BA|Nordeste
        19|Portuguesa|São Paulo|SP|Sudeste
        20|Náutico|Recife|PE|Nordeste
        21|Guarani|Campinas|SP|Sudeste
        22|Ceará|Fortaleza|CE|Nordeste
        23|Paysandu|Belém|PA|Norte
        24|Fortaleza|Fortaleza|CE|Nordeste
        25|Ponte Preta|Campinas|SP|Sudeste
        26|Santa Cruz|Recife|PE|Nordeste
        27|Juventude|Caxias do Sul|RS|Sul
        28|América Mineiro|Belo Horizonte|MG|Sudeste
        29|CSA|Maceió|AL|Nordeste
        30|America-RJ|Rio de Janeiro|RJ|Sudeste
        31|Figueirense|Florianópolis|SC|Sul
        32|Remo|Belém|PA|Norte
        33|Paraná|Curitiba|PR|Sul
        34|Nacional-AM|Manaus|AM|Norte
        35|América-RN|Natal|RN|Nordeste
        36|Red Bull Bragantino [RBB]|Bragança Paulista|SP|Sudeste
        37|Desportiva Capixaba|Cariacica|ES|Sudeste
        38|Criciúma|Criciúma|SC|Sul
        39|ABC|Natal|RN|Nordeste
        40|Atlético Goianiense|Goiânia|GO|Centro-Oeste
        41|Rio Branco-ES|Vitória|ES|Sudeste
        42|Sampaio Corrêa|São Luís|MA|Nordeste
        43|Joinville|Joinville|SC|Sul
        44|Sergipe|Aracaju|SE|Nordeste
        45|CRB|Maceió|AL|Nordeste
        46|Avaí|Florianópolis|SC|Sul
        47|Moto Club|São Luís|MA|Nordeste
        48|Bangu|Rio de Janeiro|RJ|Sudeste
        49|Campinense [CAM]|Campina Grande|PB|Nordeste
        50|Operário-MS|Campo Grande|MS|Centro-Oeste
        51|Vila Nova|Goiânia|GO|Centro-Oeste
        52|Treze [TRE]|Campina Grande|PB|Nordeste
        53|Chapecoense|Chapecó|SC|Sul
        54|Americano|Campos dos Goytacazes|RJ|Sudeste
        55|Confiança|Aracaju|SE|Nordeste
        56|River-PI|Teresina|PI|Nordeste
        57|Mixto|Cuiabá|MT|Centro-Oeste
        58|Londrina|Londrina|PR|Sul
        59|São Caetano|São Caetano do Sul|SP|Sudeste
        60|Rio Negro-AM|Manaus|AM|Norte
        61|Botafogo-PB|João Pessoa|PB|Nordeste
        62|Inter de Limeira|Limeira|SP|Sudeste
        63|Flamengo-PI|Teresina|PI|Nordeste
        64|Brasília|Brasília|DF|Centro-Oeste
        65|Botafogo-SP|Ribeirão Preto|SP|Sudeste
        66|Gama|Gama|DF|Centro-Oeste
        67|Ferroviário|Fortaleza|CE|Nordeste
        68|Grêmio Maringá|Maringá|PR|Sul
        69|Uberaba|Uberaba|MG|Sudeste
        70|Goytacaz|Campos dos Goytacazes|RJ|Sudeste
        71|Comercial-MS|Campo Grande|MS|Centro-Oeste
        72|Itabaiana|Itabaiana|SE|Nordeste
        73|Tiradentes-PI|Teresina|PI|Nordeste
        74|Colorado †|Curitiba|PR|Sul
        75|Metropol †|Criciúma|SC|Sul
        76|Tuna Luso|Belém|PA|Norte
        77|Anapolina|Anápolis|GO|Centro-Oeste
        78|Caxias|Caxias do Sul|RS|Sul
        79|GE Brasil|Pelotas|RS|Sul
        80|União São João|Araras|SP|Sudeste
        81|Uberlândia|Uberlândia|MG|Sudeste
        82|Fluminense de Feira|Feira de Santana|BA|Nordeste
        83|Operário-MT|Várzea Grande|MT|Centro-Oeste
        84|Goiânia|Goiânia|GO|Centro-Oeste
        85|Cuiabá|Cuiabá|MT|Centro-Oeste
        86|Piauí|Teresina|PI|Nordeste
        87|XV de Piracicaba|Piracicaba|SP|Sudeste
        88|Volta Redonda|Volta Redonda|RJ|Sudeste
        89|Maranhão|São Luís|MA|Nordeste
        90|Leônico [LEO]|Simões Filho|BA|Nordeste
        91|Villa Nova-MG|Nova Lima|MG|Sudeste
        92|Fast Clube|Manaus|AM|Norte
        93|Alecrim|Natal|RN|Nordeste
        94|São Paulo-RS|Rio Grande|RS|Sul
        95|Dom Bosco|Cuiabá|MT|Centro-Oeste
        96|CEUB †|Brasília|DF|Centro-Oeste
        97|Ferroviário-PR †|Curitiba|PR|Sul
        98|Fonseca †|Niterói|RJ|Sudeste
        99|Rabello †|Brasília|DF|Centro-Oeste
        100|Central|Caruaru|PE|Nordeste
        101|Santo André|Santo André|SP|Sudeste
        102|América-SP|São José do Rio Preto|SP|Sudeste
        103|Campo Grande-RJ|Rio de Janeiro|RJ|Sudeste
        104|Grêmio Barueri [GBA]|Barueri|SP|Sudeste
        105|Pinheiros-PR|Curitiba|PR|Sul
        106|São José-SP|São José dos Campos|SP|Sudeste
        107|Itabuna|Itabuna|BA|Nordeste
        108|Comercial|Ribeirão Preto|SP|Sudeste
        109|Galícia|Salvador|BA|Nordeste
        110|Capelense|Capela|AL|Nordeste
        111|Olaria|Rio de Janeiro|RJ|Sudeste
        112|XV de Jaú|Jaú|SP|Sudeste
        113|Santa Cruz-SE †|Estância|SE|Nordeste
        114|Santo Antônio †|Vitória|ES|Sudeste
        115|Juventus-SP|São Paulo|SP|Sudeste
        116|Operário Ferroviário|Ponta Grossa|PR|Sul
        117|Catuense|Catu|BA|Nordeste
        118|Novo Hamburgo|Novo Hamburgo|RS|Sul
        119|ASA|Arapiraca|AL|Nordeste
        120|Brasiliense|Taguatinga|DF|Centro-Oeste
        121|Itumbiara|Itumbiara|GO|Centro-Oeste
        122|Bandeirante-DF|Taguatinga|DF|Centro-Oeste
        123|Ipatinga [IPA]|Ipatinga|MG|Sudeste
        124|Noroeste|Bauru|SP|Sudeste
        125|Ferroviária|Araraquara|SP|Sudeste
        126|São Bento|Sorocaba|SP|Sudeste
        127|Anápolis|Anápolis|GO|Centro-Oeste
        128|Vitória-ES|Vitória|ES|Sudeste
        129|AA Colatina †|Colatina|ES|Sudeste
        130|CR Guará|Guará|DF|Centro-Oeste
        131|Sobradinho|Sobradinho|DF|Centro-Oeste
        132|Mirassol|Mirassol|SP|Sudeste
        133|Inter SM|Santa Maria|RS|Sul
        134|Auto Esporte-PB|João Pessoa|PB|Nordeste
        135|Malutrom|Curitiba|PR|Sul
        136|Caldense|Poços de Caldas|MG|Sudeste
        137|Corumbaense|Corumbá|MS|Centro-Oeste
        138|Auto Esporte-PI †|Teresina|PI|Nordeste
        139|Ferroviário-MA †|São Luís|MA|Nordeste
        140|Potiguar de Mossoró|Mossoró|RN|Nordeste
        141|Francana|Franca|SP|Sudeste
        142|América de Propriá|Propriá|SE|Nordeste
        143|Rio Branco-RJ †|Campos dos Goytacazes|RJ|Sudeste
        144|Inter de Lages|Lages|SC|Sul
        145|Hercílio Luz|Tubarão|SC|Sul
        146|Água Verde †|Curitiba|PR|Sul
        147|América-CE|Fortaleza|CE|Nordeste
        148|Comercial-PR †|Cornélio Procópio|PR|Sul
        149|Cruzeiro do Sul †|Cruzeiro|DF|Centro-Oeste
        150|Defelê †|Brasília|DF|Centro-Oeste
        151|Estrela do Mar †|João Pessoa|PB|Nordeste
        152|Guanabara-DF †|Brasília|DF|Centro-Oeste
        153|Manufatora †|Niterói|RJ|Sudeste
        154|Olímpico-AM †|Manaus|AM|Norte
        155|Olímpico-SC †|Blumenau|SC|Sul
        156|Paula Ramos †|Florianópolis|SC|Sul
        157|Perdigão †|Videira|SC|Sul
        158|Siderúrgica|Sabará|MG|Sudeste
        159|Aliança †|Campos dos Goytacazes|RJ|Sudeste
        160|Liga da Marinha †|Rio de Janeiro|RJ|Sudeste
        161|Mogi Mirim|Mogi Mirim|SP|Sudeste
        162|Oeste [OES]|Barueri|SP|Sudeste
        163|Ituano|Itu|SP|Sudeste
        164|Boa Esporte [BOA]|Varginha|MG|Sudeste
        165|Marília|Marília|SP|Sudeste
        166|São Raimundo-AM|Manaus|AM|Norte
        167|Paulista|Jundiaí|SP|Sudeste
        168|Guarany de Sobral|Sobral|CE|Nordeste
        169|União Rondonópolis|Rondonópolis|MT|Centro-Oeste
        170|Goiatuba|Goiatuba|GO|Centro-Oeste
        171|Luverdense|Lucas do Rio Verde|MT|Centro-Oeste
        172|Icasa|Juazeiro do Norte|CE|Nordeste
        173|Guaratinguetá [GTA] †|Guaratinguetá|SP|Sudeste
        174|Brusque|Brusque|SC|Sul
        175|Itaperuna [ITA]|Itaperuna|RJ|Sudeste
        176|América-PE|Recife|PE|Nordeste
        177|Novorizontino|Novo Horizonte|SP|Sudeste
        178|Tupi|Juiz de Fora|MG|Sudeste
        179|Rio Branco-AC|Rio Branco|AC|Norte
        180|Marcílio Dias|Itajaí|SC|Sul
        181|Baraúnas|Mossoró|RN|Nordeste
        182|Blumenau|Blumenau|SC|Sul
        183|Duque de Caxias|Duque de Caxias|RJ|Sudeste
        184|Esportivo|Bento Gonçalves|RS|Sul
        185|Ubiratan-MS|Dourados|MS|Centro-Oeste
        186|Cascavel EC †|Cascavel|PR|Sul
        187|GE Novorizontino †|Novo Horizonte|SP|Sudeste
        188|Tombense|Tombos|MG|Sudeste
        189|Atlético de Alagoinhas|Alagoinhas|BA|Nordeste
        190|Pelotas|Pelotas|RS|Sul
        191|Serra|Serra|ES|Sudeste
        192|Esportivo de Passos †|Passos|MG|Sudeste
        193|Rio Branco-MG †|Andradas|MG|Sudeste
        194|Tiradentes-DF †|Ceilândia|DF|Centro-Oeste
        195|Valeriodoce|Itabira|MG|Sudeste
        196|Amazonas|Manaus|AM|Norte
        197|Democrata GV|Governador Valadares|MG|Sudeste
        198|Barra do Garças †|Barra do Garças|MT|Centro-Oeste
        199|Bonsucesso|Rio de Janeiro|RJ|Sudeste
        200|Ferroviário do Recife †|Recife|PE|Nordeste
        201|Santa Cruz-RS|Santa Cruz do Sul|RS|Sul
        202|Serrano-RJ|Petrópolis|RJ|Sudeste
        203|Sport Belém|Belém|PA|Norte
        204|União Bandeirante †|Bandeirantes|PR|Sul
        205|Salgueiro|Salgueiro|PE|Nordeste
        206|Macaé|Macaé|RJ|Sudeste
        207|Imperatriz [IMP]|Imperatriz|MA|Nordeste
        208|Estrela do Norte|Cachoeiro de Itapemirim|ES|Sudeste
        209|Picos|Picos|PI|Nordeste
        210|Ceilândia|Ceilândia|DF|Centro-Oeste
        211|União Barbarense|Santa Bárbara d'Oeste|SP|Sudeste
        212|Icasa EC †|Juazeiro do Norte|CE|Nordeste
        213|Parnahyba|Parnaíba|PI|Nordeste
        214|Nacional de Patos|Patos|PB|Nordeste
        215|Independência|Rio Branco|AC|Norte
        216|Lagarto EC †|Lagarto|SE|Nordeste
        217|4 de Julho|Piripiri|PI|Nordeste
        218|Athletic|São João del-Rei|MG|Sudeste
        219|Democrata-SL|Sete Lagoas|MG|Sudeste
        220|AA Cabofriense †|Cabo Frio|RJ|Sudeste
        221|Douradense †|Dourados|MS|Centro-Oeste
        222|Estudantes †|Timbaúba|PE|Nordeste
        223|Glória|Vacaria|RS|Sul
        224|Princesa do Solimões|Manacapuru|AM|Norte
        225|Serrano-BA|Porto Seguro|BA|Nordeste
        226|Tiradentes-CE|Fortaleza|CE|Nordeste
        227|América de Joinville †|Joinville|SC|Sul
        228|Atlético Taguatinga [CBA] †|Taguatinga|DF|Centro-Oeste
        229|Botafogo-BA [BSC]|Senhor do Bonfim|BA|Nordeste
        230|Calouros do Ar|Fortaleza|CE|Nordeste
        231|GE Catanduvense †|Catanduva|SP|Sudeste
        232|Central-RJ †|Barra do Piraí|RJ|Sudeste
        233|Guarapari|Guarapari|ES|Sudeste
        234|Maguari †|Fortaleza|CE|Nordeste
        235|Foz do Iguaçu EC †|Foz do Iguaçu|PR|Sul
        236|Nacional de Itumbiara †|Itumbiara|GO|Centro-Oeste
        237|Rodoviária †|Manaus|AM|Norte
        238|São Domingos|Marechal Deodoro|AL|Nordeste
        239|Madureira|Rio de Janeiro|RJ|Sudeste
        240|Rio Branco-SP|Americana|SP|Sudeste
        241|Ypiranga de Erechim|Erechim|RS|Sul
        242|São José-RS|Porto Alegre|RS|Sul
        243|Águia de Marabá|Marabá|PA|Norte
        244|Atlético Sorocaba †|Sorocaba|SP|Sudeste
        245|Porto-PE|Caruaru|PE|Nordeste
        246|Ji-Paraná|Ji-Paraná|RO|Norte
        247|Tocantinópolis|Tocantinópolis|TO|Norte
        248|CENE †|Campo Grande|MS|Centro-Oeste
        249|Friburguense|Nova Friburgo|RJ|Sudeste
        250|Juazeiro|Juazeiro|BA|Nordeste
        251|Palmas †|Palmas|TO|Norte
        252|Ypiranga-AP|Macapá|AP|Norte
        253|Atlético Roraima|Boa Vista|RR|Norte
        254|Baré|Boa Vista|RR|Norte
        255|Floresta|Fortaleza|CE|Nordeste
        256|Iraty †|Irati|PR|Sul
        257|Rio Branco-PR|Paranaguá|PR|Sul
        258|Vitória-PE [VIT]|Vitória de Santo Antão|PE|Nordeste
        259|Canoas [CAN] †|Canoas|RS|Sul
        260|Corinthians-AL †|Maceió|AL|Nordeste
        261|Matsubara †|Cambará|PR|Sul
        262|Portuguesa Santista|Santos|SP|Sudeste
        263|Tubarão FC †|Tubarão|SC|Sul
        264|Atlético Acreano|Rio Branco|AC|Norte
        265|Coruripe|Coruripe|AL|Nordeste
        266|CRAC|Catalão|GO|Centro-Oeste
        267|Manaus|Manaus|AM|Norte
        268|Aparecidense|Aparecida de Goiânia|GO|Centro-Oeste
        269|São Raimundo-RR|Boa Vista|RR|Norte
        270|Sousa|Sousa|PB|Nordeste
        271|Altos|Altos|PI|Nordeste
        272|Araguaína|Araguaína|TO|Norte
        273|Cabofriense|Cabo Frio|RJ|Sudeste
        274|Gurupi|Gurupi|TO|Norte
        275|São Bernardo|São Bernardo do Campo|SP|Sudeste
        276|Atlético Cajazeirense|Cajazeiras|PB|Nordeste
        277|Colo Colo|Ilhéus|BA|Nordeste
        278|Dom Pedro|Núcleo Bandeirante|DF|Centro-Oeste
        279|Andirá|Rio Branco|AC|Norte
        280|CFZ-DF|Brasília|DF|Centro-Oeste
        281|Corisabbá|Floriano|PI|Nordeste
        282|Nacional-SP|São Paulo|SP|Sudeste
        283|Paranavaí|Paranavaí|PR|Sul
        284|Cianorte|Cianorte|PR|Sul
        285|Globo|Ceará-Mirim|RN|Nordeste
        286|Portuguesa-RJ|Rio de Janeiro|RJ|Sudeste
        287|Jacuipense|Riachão do Jacuípe|BA|Nordeste
        288|São Raimundo-PA|Santarém|PA|Norte
        289|Genus [GEN]|Porto Velho|RO|Norte
        290|Águia Negra|Rio Brilhante|MS|Centro-Oeste
        291|Juventus de Jaraguá|Jaraguá do Sul|SC|Sul
        292|Ypiranga-PE|Santa Cruz do Capibaribe|PE|Nordeste
        293|Castanhal|Castanhal|PA|Norte
        294|Grêmio Anápolis [GRA]|Anápolis|GO|Centro-Oeste
        295|União Araguainense|Araguaína|TO|Norte
        296|15 de Novembro †|Campo Bom|RS|Sul
        297|ADAP/Galo Maringá †|Maringá|PR|Sul
        298|Amapá|Macapá|AP|Norte
        299|Ananindeua|Ananindeua|PA|Norte
        300|Atlético de Ibirama|Ibirama|SC|Sul
        301|Bacabal|Bacabal|MA|Nordeste
        302|Barras †|Barras|PI|Nordeste
        303|Batel|Guarapuava|PR|Sul
        304|Caxiense †|Caxias|MA|Nordeste
        305|Ceilandense|Ceilândia|DF|Centro-Oeste
        306|Corintians de Caicó|Caicó|RN|Nordeste
        307|Grêmio Coariense|Coari|AM|Norte
        308|Grêmio Jaciara|Jaciara|MT|Centro-Oeste
        309|Ipitanga|Lauro de Freitas|BA|Nordeste
        310|Itapipoca|Itapipoca|CE|Nordeste
        311|Izabelense|Santa Izabel do Pará|PA|Norte
        312|Jataiense|Jataí|GO|Centro-Oeste
        313|Kaburé|Colinas do Tocantins|TO|Norte
        314|Linhares|Linhares|ES|Sudeste
        315|Maruinense|Maruim|SE|Nordeste
        316|Matonense|Matão|SP|Sudeste
        317|Palmeiras do Nordeste †|Feira de Santana|BA|Nordeste
        318|Planaltina|Planaltina|DF|Centro-Oeste
        319|Ponta Grossa †|Ponta Grossa|PR|Sul
        320|Progresso|Mucajaí|RR|Norte
        321|Santa Inês †|Santa Inês|MA|Nordeste
        322|São Gonçalo-RN †|São Gonçalo do Amarante|RN|Nordeste
        323|Sertãozinho|Sertãozinho|SP|Sudeste
        324|Taveirópolis|Campo Grande|MS|Centro-Oeste
        325|Vasco-AC|Rio Branco|AC|Norte
        326|Viana|Viana|MA|Nordeste
        327|Juazeirense|Juazeiro|BA|Nordeste
        328|Metropolitano|Blumenau|SC|Sul
        329|Santos-AP|Macapá|AP|Norte
        330|Trem|Macapá|AP|Norte
        331|Boavista-RJ|Saquarema|RJ|Sudeste
        332|Maringá|Maringá|PR|Sul
        333|Vitória da Conquista|Vitória da Conquista|BA|Nordeste
        334|Atlético Cearense|Fortaleza|CE|Nordeste
        335|URT|Patos de Minas|MG|Sudeste
        336|Retrô|Camaragibe|PE|Nordeste
        337|Luziânia|Luziânia|DF|Centro-Oeste
        338|Pouso Alegre|Pouso Alegre|MG|Sudeste
        339|São Luiz|Ijuí|RS|Sul
        340|Cristal|Macapá|AP|Norte
        341|GAS [GAS]|Boa Vista|RR|Norte
        342|Horizonte|Horizonte|CE|Nordeste
        343|Linense|Lins|SP|Sudeste
        344|Petrolina|Petrolina|PE|Nordeste
        345|São Francisco-PA|Santarém|PA|Norte
        346|Vila Aurora|Rondonópolis|MT|Centro-Oeste
        347|América-AM †|Manaus|AM|Norte
        348|Botafogo-DF [BOT]|Guará|DF|Centro-Oeste
        349|Camaçari|Camaçari|BA|Nordeste
        350|Flamengo de Arcoverde|Arcoverde|PE|Nordeste
        351|Gaúcho|Passo Fundo|RS|Sul
        352|Guarani-MG|Divinópolis|MG|Sudeste
        353|Macapá|Macapá|AP|Norte
        354|Nacional-PR|Rolândia|PR|Sul
        355|Santa Cruz-RN|Santa Cruz|RN|Nordeste
        356|São Mateus|São Mateus|ES|Sudeste
        357|Toledo|Toledo|PR|Sul
        358|Abaeté †|Abaetetuba|PA|Norte
        359|ADAP †|Campo Mourão|PR|Sul
        360|ADESG|Senador Guiomard|AC|Norte
        361|Alvorada|Alvorada|TO|Norte
        362|América de Esperança †|Esperança|PB|Nordeste
        363|Araçatuba|Araçatuba|SP|Sudeste
        364|SE Ariquemes †|Ariquemes|RO|Norte
        365|Atlético Três Corações|Três Corações|MG|Sudeste
        366|Barra de Teresópolis †|Teresópolis|RJ|Sudeste
        367|Barra Mansa|Barra Mansa|RJ|Sudeste
        368|Batalhense †|Belém|AL|Nordeste
        369|Bayer †|Belford Roxo|RJ|Sudeste
        370|Caçadorense †|Caçador|SC|Sul
        371|Cacerense|Cáceres|MT|Centro-Oeste
        372|Cachoeiro|Cachoeiro de Itapemirim|ES|Sudeste
        373|Caldas|Caldas Novas|GO|Centro-Oeste
        374|Caxias-SC|Joinville|SC|Sul
        375|Centro Limoeirense|Limoeiro|PE|Nordeste
        376|CFA †|Porto Velho|RO|Norte
        377|CFZ-RJ †|Rio de Janeiro|RJ|Sudeste
        378|Chapadão|Chapadão do Sul|MS|Centro-Oeste
        379|Chapadinha|Chapadinha|MA|Nordeste
        380|Corinthians-PP †|Presidente Prudente|SP|Sudeste
        381|Coroatá †|Coroatá|MA|Nordeste
        382|Coxim|Coxim|MS|Centro-Oeste
        383|Duque de Caxias-MA †|Caxias|MA|Nordeste
        384|Engenheiro Beltrão †|Engenheiro Beltrão|PR|Sul
        385|EC Limoeiro|Limoeiro do Norte|CE|Nordeste
        386|Fabril|Lavras|MG|Sudeste
        387|Guarabira|Guarabira|PB|Nordeste
        388|Guarany de Cruz Alta †|Cruz Alta|RS|Sul
        389|Holanda|Rio Preto da Eva|AM|Norte
        390|Iguaçu|União da Vitória|PR|Sul
        391|Itacuruba †|Itacuruba|PE|Nordeste
        392|Jaguaré|Jaguaré|ES|Sudeste
        393|Jaruense †|Jaru|RO|Norte
        394|Joaçaba †|Joaçaba|SC|Sul
        395|Juventude-MT †|Primavera do Leste|MT|Centro-Oeste
        396|Lagartense|Lagarto|SE|Nordeste
        397|Lages †|Lages|SC|Sul
        398|Legião|Brasília|DF|Centro-Oeste
        399|AD Limoeiro|Limoeiro do Norte|CE|Nordeste
        400|Linhares EC †|Linhares|ES|Sudeste
        401|Mamoré|Patos de Minas|MG|Sudeste
        402|Manchete †[MAN]|Recife|PE|Nordeste
        403|Mineiros|Mineiros|GO|Centro-Oeste
        404|Montes Claros|Montes Claros|MG|Sudeste
        405|Novo Horizonte|Ipameri|GO|Centro-Oeste
        406|Olímpia|Olímpia|SP|Sudeste
        407|Operário FC|Várzea Grande|MT|Centro-Oeste
        408|EC Operário †|Várzea Grande|MT|Centro-Oeste
        409|Paraíso [PAR]|Paraíso do Tocantins|TO|Norte
        410|Paranoá|Paranoá|DF|Centro-Oeste
        411|Passo Fundo|Passo Fundo|RS|Sul
        412|Pauferrense †|Pau dos Ferros|RN|Nordeste
        413|Paulistano †|Paulista|PE|Nordeste
        414|Pedrabranca [PED]|Alvorada|RS|Sul
        415|Pirambu|Pirambu|SE|Nordeste
        416|Poções|Poções|BA|Nordeste
        417|Quixadá|Quixadá|CE|Nordeste
        418|Real Clube †|Itumbiara|GO|Centro-Oeste
        419|Rio Claro|Rio Claro|SP|Sudeste
        420|Rio Negro-RR|Boa Vista|RR|Norte
        421|Rio Pardo †|Iúna|ES|Sudeste
        422|Rio Verde|Rio Verde|GO|Centro-Oeste
        423|Rioverdense|Rio Verde|GO|Centro-Oeste
        424|Roma Apucarana †|Apucarana|PR|Sul
        425|Santa Cruz|Santa Rita|PB|Nordeste
        426|Santa Rosa|Belém|PA|Norte
        427|São Borja [SBO]|São Borja|RS|Sul
        428|Sãocarlense|São Carlos|SP|Sudeste
        429|São Cristóvão|Rio de Janeiro|RJ|Sudeste
        430|São Gabriel FC †|São Gabriel|RS|Sul
        431|São José-AP|Macapá|AP|Norte
        432|Serrano-PE|Serra Talhada|PE|Nordeste
        433|Sete de Setembro-AL|Maceió|AL|Nordeste
        434|Social|Coronel Fabriciano|MG|Sudeste
        435|Tiradentes-PA|Belém|PA|Norte
        436|Tocantins-MA †|Imperatriz|MA|Nordeste
        437|Ulbra-RO †|Ji-Paraná|RO|Norte
        438|Unibol †|Paulista|PE|Nordeste
        439|Vênus|Abaetetuba|PA|Norte
        440|Vera Cruz|Vitória de Santo Antão|PE|Nordeste
        441|Goianésia|Goianésia|GO|Centro-Oeste
        442|Bahia de Feira|Feira de Santana|BA|Nordeste
        443|FC Cascavel|Cascavel|PR|Sul
        444|Nova Iguaçu|Nova Iguaçu|RJ|Sudeste
        445|Guarani de Juazeiro|Juazeiro do Norte|CE|Nordeste
        446|Iporá|Iporá|GO|Centro-Oeste
        447|Náutico-RR [NAU]|Boa Vista|RR|Norte
        448|Sinop|Sinop|MT|Centro-Oeste
        449|Real Ariquemes|Ariquemes|RO|Norte
        450|Humaitá|Porto Acre|AC|Norte
        451|Interporto|Porto Nacional|TO|Norte
        452|Murici|Murici|AL|Nordeste
        453|Patrocinense|Patrocínio|MG|Sudeste
        454|Porto Velho|Porto Velho|RO|Norte
        455|Real Noroeste|Águia Branca|ES|Sudeste
        456|Aimoré|São Leopoldo|RS|Sul
        457|Concórdia|Concórdia|SC|Sul
        458|Cordino|Barra do Corda|MA|Nordeste
        459|Espírito Santo|Vitória|ES|Sudeste
        460|Fluminense-PI|Teresina|PI|Nordeste
        461|Foz do Iguaçu|Foz do Iguaçu|PR|Sul
        462|Galvez|Rio Branco|AC|Norte
        463|Iguatu|Iguatu|CE|Nordeste
        464|Independente-PA|Tucuruí|PA|Norte
        465|Juventude Samas|São Mateus do Maranhão|MA|Nordeste
        466|Penarol|Itacoatiara|AM|Norte
        467|Plácido de Castro|Plácido de Castro|AC|Norte
        468|Resende|Resende|RJ|Sudeste
        469|Santa Cruz de Natal|Natal|RN|Nordeste
        470|Atlético Tubarão|Tubarão|SC|Sul
        471|Vilhena|Vilhena|RO|Norte
        472|Afogados|Afogados da Ingazeira|PE|Nordeste
        473|Água Santa|Diadema|SP|Sudeste
        474|Aquidauanense|Aquidauana|MS|Centro-Oeste
        475|Aracruz|Aracruz|ES|Sudeste
        476|Audax|Osasco|SP|Sudeste
        477|Audax Rio|São João de Meriti|RJ|Sudeste
        478|Avenida|Santa Cruz do Sul|RS|Sul
        479|Azuriz|Pato Branco|PR|Sul
        480|Barcelona-RO|Vilhena|RO|Norte
        481|Barra-SC|Balneário Camboriú|SC|Sul
        482|Bragantino-PA|Bragança|PA|Norte
        483|Cametá|Cametá|PA|Norte
        484|Caucaia|Caucaia|CE|Nordeste
        485|Cerâmica|Gravataí|RS|Sul
        486|Comercial-PI|Campo Maior|PI|Nordeste
        487|Costa Rica|Costa Rica|MS|Centro-Oeste
        488|CSE|Palmeira dos Índios|AL|Nordeste
        489|Lagarto|Lagarto|SE|Nordeste
        490|Lajeadense|Lajeado|RS|Sul
        491|Manauara|Manaus|AM|Norte
        492|Maracanã-CE|Maracanaú|CE|Nordeste
        493|Pacajus|Pacajus|CE|Nordeste
        494|Paragominas|Paragominas|PA|Norte
        495|Penapolense|Penápolis|SP|Sudeste
        496|PSTC|Cornélio Procópio|PR|Sul
        497|Red Bull Brasil|Campinas|SP|Sudeste
        498|River Plate-SE†|Carmópolis|SE|Nordeste
        499|Serra Talhada|Serra Talhada|PE|Nordeste
        500|Sete de Dourados|Dourados|MS|Centro-Oeste
        501|Ação|Santo Antônio de Leverger|MT|Centro-Oeste
        502|AA Araguaia|Barra do Garças|MT|Centro-Oeste
        503|Araguaia AC|Alto Araguaia|MT|Centro-Oeste
        504|Arapongas|Arapongas|PR|Sul
        505|Araxá|Araxá|MG|Sudeste
        506|ASSU|Assú|RN|Nordeste
        507|Atlético Itapemirim|Itapemirim|ES|Sudeste
        508|Atlético Pernambucano|Carpina|PE|Nordeste
        509|Barcelona de Ilhéus|Ilhéus|BA|Nordeste
        510|Belo Jardim|Belo Jardim|PE|Nordeste
        511|Camboriú|Camboriú|SC|Sul
        512|Capital-DF|Paranoá|DF|Centro-Oeste
        513|Capital-TO|Palmas|TO|Norte
        514|Crato|Crato|CE|Nordeste
        515|Cruzeiro de Arapiraca|Arapiraca|AL|Nordeste
        516|Cruzeiro-RS|Cachoeirinha|RS|Sul
        517|Estanciano|Estância|SE|Nordeste
        518|Falcon|Barra dos Coqueiros|SE|Nordeste
        519|Feirense|Feira de Santana|BA|Nordeste
        520|Formosa|Formosa|DF|Centro-Oeste
        521|Freipaulistano|Frei Paulo|SE|Nordeste
        522|Guarani de Palhoça|Palhoça|SC|Sul
        523|Guarany de Bagé|Bagé|RS|Sul
        524|Itabirito|Itabirito|MG|Sudeste
        525|Itaboraí|Itaboraí|RJ|Sudeste
        526|Itaporã|Itaporã|MS|Centro-Oeste
        527|Jaraguá|Jaraguá|GO|Centro-Oeste
        528|Jacyobá|Pão de Açúcar|AL|Nordeste
        529|Jacobina|Jacobina|BA|Nordeste
        530|Jequié|Jequié|BA|Nordeste
        531|JV Lideral|Imperatriz|MA|Nordeste
        532|Maricá|Maricá|RJ|Sudeste
        533|Monte Azul|Monte Azul Paulista|SP|Sudeste
        534|Nacional-MG [NAC]|Esmeraldas|MG|Sudeste
        535|Náuas|Cruzeiro do Sul|AC|Norte
        536|Naviraiense|Naviraí|MS|Centro-Oeste
        537|Nova Mutum|Nova Mutum|MT|Centro-Oeste
        538|Nova Venécia|Nova Venécia|ES|Sudeste
        539|Novo|Campo Grande|MS|Centro-Oeste
        540|Penedense|Penedo|AL|Nordeste
        541|Pérolas Negras|Resende|RJ|Sudeste
        542|Porto Vitória|Vitória|ES|Sudeste
        543|Próspera|Criciúma|SC|Sul
        544|Prudentópolis|Prudentópolis|PR|Sul
        545|Rio Branco VN|Venda Nova do Imigrante|ES|Sudeste
        546|Rondoniense|Porto Velho|RO|Norte
        547|Santa Rita|Boca da Mata|AL|Nordeste
        548|Santana|Santana|AP|Norte
        549|São Francisco-AC|Rio Branco|AC|Norte
        550|São Joseense|São José dos Pinhais|PR|Sul
        551|São Paulo Crystal|Cruz do Espírito Santo|PB|Nordeste
        552|Serrano-PB|Campina Grande|PB|Nordeste
        553|Sparta|Araguaína|TO|Norte
        554|Tocantins de Miracema|Miracema do Tocantins|TO|Norte
        555|Tocantins de Palmas †|Palmas|TO|Norte
        556|Tupynambás|Juiz de Fora|MG|Sudeste
        557|Vilhenense|Vilhena|RO|Norte

    ]
    clubes_df = pd.DataFrame(clubes_data)

    clubes_completos = []
    for i, row in enumerate(clubes_df.iterrows(), 1):
        clube_data = get_clube_data(row[1]['ID'], row[1]['Nome'], row[1]['Cidade'], row[1]['Estado'], row[1]['Região'])
        clubes_completos.append(clube_data)
        print(f"Processado clube {i}/{len(clubes_df)}: {row[1]['Nome']}")
        time.sleep(5)

    # Salva em CSV
    df = pd.DataFrame(clubes_completos)
    df.to_csv('clubes_completos.csv', index=False, encoding='utf-8')
    print(f"Dados salvos em 'clubes_completos.csv' com {len(df)} clubes")
    print(df.head())

if __name__ == "__main__":
    main()
