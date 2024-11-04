import csv
import random
import time
from urllib import request
from bs4 import BeautifulSoup
import concurrent
import concurrent.futures

import requests

import os

print("Diretório atual:", os.getcwd())

# global headers to bs used requests
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'}

MAX_THREADS = 10


def extract_movie_details(movie_link):
    # The sleep function of the time library pauses program execution for the specified number of seconds (explicando a funão time.sleep())
    # generates a random floating point number between 0 and 0.2
    time.sleep(random.uniform(0, 0.2))
    # request of movie_link, params headers used to send additional information along with the HTTP request
    response = request.get(movie_link, headers=headers)
    # response.content request response
    # BeautifulSoup used to facilited the analysis oh HTML and XML documents
    # the code creats object that struture HTML page
    movie_soup = BeautifulSoup(response.content, 'html.parser')

    if movie_soup is not None:
        title = None
        date = None

        # uses BeautifulSoup to find specific part of the HTML documents
        page_section = movie_soup.find(
            'section', attrs={'class': 'ipc-page-section'})

        if page_section is not None:
            # Encontrando todas as divs dentro da seção
            # Busca todas as ocorrências de <div> dentro de page_section, incluindo as que estão em níveis mais profundos da hierarquia (ou seja, dentro de outras tags)
            divs = page_section.find_all('div', recursive=False)

            if len(divs) > 1:
                target_div = divs[1]

                # Finding the movie title
                title_tag = target_div.find('h1')
                if title_tag:
                    title = title_tag.find('span').get_text()

                # Finding the date Movie
                date_tag = target_div.find(
                    'a', href=lambda href: href and 'releaseinfo' in href)
                if date_tag:
                    date = date_tag.get_text().strip()

                # Encontrando a classificação do filme
                rating_tag = movie_soup.find(
                    'div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
                rating = rating_tag.get_text() if rating_tag else None

                # Encontrando a sinopse do filme
                plot_tag = movie_soup.find(
                    'span', attrs={'data-testid': 'plot-xs_to_m'})
                plot_text = plot_tag.get_text().strip() if plot_tag else None

                print("Tentando gravar no CSV...")
                print("Título:", title)
                print("Data:", date)
                print("Classificação:", rating)
                print("Sinopse:", plot_text)
                # escreve todas essa informações dentro de um csv
                with open('movies.csv', mode='a', newline='', encoding='utf-8') as file:
                    # create object to write lines in CSV, delimiter that colums will be separed by commas (virgula)
                    movie_writer = csv.writer(
                        file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    if all([title, date, rating, plot_text]):
                        print(title, date, rating, plot_text)
                        # writes a new lines to the CSV file with the given values
                        movie_writer.writerow([title, date, rating, plot_text])


def extract_movies(soup):
    movies_table = soup.find(
        'div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
    movies_table_rows = movies_table.find_all('li')
    movie_links = ['https://imdb.com' +
                   movie.find('a')['href'] for movie in movies_table_rows]

    # get min value between
    threads = min(MAX_THREADS, len(movie_links))
    # ThreadPoolExecutor é uma classe da biblioteca concurrent.futures que permite executar chamadas de função em paralelo usando threads.
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        # O método map do executor é utilizado para aplicar a função extract_movie_details a cada item da lista movie_links
        # Isso significa que a função será chamada em paralelo para cada link, permitindo que a extração de detalhes dos filmes ocorra simultaneamente em várias threads.
        executor.map(extract_movie_details, movie_links)


def main():
    start_time = time.time()

    # IMDB Most Popular Movies - 100 movies
    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Main function to extract the 100 movies from IMDB Most Popular Movies
    extract_movies(soup)

    end_time = time.time()
    print('Total time taken: ', end_time - start_time)


if __name__ == '__main__':
    main()
