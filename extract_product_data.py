from bs4 import BeautifulSoup
import requests
import asyncio
import aiohttp
import time
import csv
import re

filename = 'happy_horse.csv'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.5249.103 Safari/537.36'
}

def get_links():
    sitemap = 'https://happy-horse.dk/feeds/google_sitemap/'
    r = requests.get(url=sitemap, headers=headers)

    base_url = 'https://happy-horse.dk/shop/'
    raw_urls = re.findall(f'{base_url}(?:[a-zA-Z]|[0-9]|[$-_@.&+])+<', r.text)
    urls = list(map(lambda x: x[:-1], raw_urls))

    print(f'{len(urls)} was scraped!')
    return urls

def create_csv():
    with open(filename, 'w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            'ProductTitle', 'ProductDescription', 'ProductPrice', 'ProductPriceBeforeDiscount', 'ProductLink'
        ])

def extract_data(html, url):
    soup = BeautifulSoup(html, 'lxml')

    try:
        name = soup.find('h1', class_='m-product-title product-title').text.strip()
    except:
        name = ''
    try:
        description = soup.find('div', attrs={'data-controller': 'description'}).text.strip()
    except:
        description = ''
    try:
        price = soup.find('span', class_='h4 m-product-price').text.strip()
    except:
        price = ''
    try:
        price_before = soup.find('s', class_='m-product-price-before-discount').text.strip()
    except:
        price_before = ''

    with open(filename, 'a', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            name, description, price, price_before, url
        ])

def get_tasks(session):
    tasks = []
    links = get_links()
    for link in links:
        tasks.append(asyncio.create_task(session.get(url=link, headers=headers, ssl=False)))
    return tasks, links

async def runner():
    async with aiohttp.ClientSession() as session:
        tasks, links = get_tasks(session)
        length = len(links)

        responses = await asyncio.gather(*tasks)
        for i, response in enumerate(responses):
            html = await response.text()

            extract_data(html, links[i])

            print(f'{i + 1}/{length}')

def main():
    t0 = time.time()
    create_csv()
    asyncio.run(runner())
    print(time.time() - t0)

if __name__ == '__main__':
    main()