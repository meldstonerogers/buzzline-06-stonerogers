import os
from dotenv import load_dotenv
from newsapi import NewsApiClient

# Load environment variables from .env file
load_dotenv()

# Fetch NewsAPI Key from environment
API_KEY = os.getenv("NEWSAPI_KEY")

# Initialize NewsApiClient
newsapi = NewsApiClient(api_key=API_KEY)

def fetch_top_headlines():
    """Fetch top headlines from NewsAPI."""
    try:
        print("Fetching top headlines...")
        top_headlines = newsapi.get_top_headlines(country='us')

        # Log the status of the response
        print(f"API response status: {top_headlines['status']}")  # Log the status

        articles = top_headlines.get("articles", [])
        print(f"Number of articles retrieved: {len(articles)}")  # Log number of articles

        if not articles:
            print("No articles found in response.")
            return None

        # Print the articles
        for article in articles:
            print(f"Title: {article['title']}")
            print(f"Source: {article['source']['name']}")
            print(f"URL: {article['url']}")
            print(f"Published At: {article['publishedAt']}")
            print("-" * 50)

    except Exception as e:
        print(f"Error fetching news data: {e}")

if __name__ == "__main__":
    fetch_top_headlines()
