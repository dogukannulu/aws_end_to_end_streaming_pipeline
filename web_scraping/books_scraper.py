import requests
from bs4 import BeautifulSoup
import json
import random
from urllib.parse import urljoin
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class BookScraper:
    def __init__(self, base_url):
        self.base_url = base_url
    
    def extract_details(self, detail_url):
        try:
            response = requests.get(detail_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            availability = "In stock"
            availability_element = soup.find("p", class_="instock availability")
            if availability_element:
                availability_text = availability_element.text.strip()
                if "In stock" not in availability_text:
                    availability = "Out of stock"

            return availability
        except requests.RequestException as e:
            logger.error(f"Error extracting details from {detail_url}: {e}")
            return "Unknown"
    
    def scrape_books(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            books = []

            for article in soup.find_all("article", class_="product_pod"):
                title = article.h3.a["title"]
                price = article.find("p", class_="price_color").text.strip()

                rating_element = article.find("p", class_="star-rating")
                num_reviews = rating_element.attrs["class"][1]

                product_url = urljoin(self.base_url, article.h3.a["href"])
                availability = self.extract_details(product_url)

                upc = "f" + hex(hash(title))[2:] + hex(hash(price))[2:]

                books.append({
                    "title": title,
                    "price": price,
                    "num_reviews": num_reviews,
                    "upc": upc,
                    "availability": availability
                })

            return books
        except requests.RequestException as e:
            logger.error(f"Error scraping books from {url}: {e}")
            return []

def main():
    base_url = "http://books.toscrape.com"
    total_pages = 10
    data = []

    scraper = BookScraper(base_url)

    for page_num in range(1, total_pages + 1):
        page_url = f"{base_url}/catalogue/page-{page_num}.html"
        page_data = scraper.scrape_books(page_url)
        data.extend(page_data)

    for book in data:
        book["availability"] = random.choice(["In stock", "Out of stock"])

    try:
        with open("books_data.json", "w") as json_file:
            json.dump(data, json_file, indent=4)
        logger.info(f"Scraped and saved {len(data)} book entries.")
    except Exception as e:
        logger.error(f"Error saving data to JSON: {e}")

if __name__ == "__main__":
    main()
