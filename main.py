# main.py
from fastapi import FastAPI, Depends, HTTPException, Header
from typing import Optional, List
import httpx
import asyncio
from bs4 import BeautifulSoup
import json
from abc import ABC, abstractmethod
import redis
import os
from datetime import datetime
from pydantic import BaseModel, HttpUrl
from functools import wraps
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Configuration
API_TOKEN = "my-test-token-123"  
REDIS_HOST = "localhost"
REDIS_PORT = 6379
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Models
class ScrapingSettings(BaseModel):
    page_limit: Optional[int] = None
    proxy: Optional[str] = None
    target_url: HttpUrl

class Product(BaseModel):
    product_title: str
    product_price: float
    path_to_image: str

# Abstract base classes for storage and notification
class StorageStrategy(ABC):
    @abstractmethod
    async def save_products(self, products: List[Product]) -> None:
        pass

    @abstractmethod
    async def get_products(self) -> List[Product]:
        pass

class NotificationStrategy(ABC):
    @abstractmethod
    async def notify(self, message: str) -> None:
        pass

# Concrete implementations
class JSONFileStorage(StorageStrategy):
    def __init__(self, filename: str = "products.json"):
        self.filename = filename

    async def save_products(self, products: List[Product]) -> None:
        with open(self.filename, 'w') as f:
            json.dump([product.dict() for product in products], f, indent=2)

    async def get_products(self) -> List[Product]:
        try:
            with open(self.filename, 'r') as f:
                data = json.load(f)
                return [Product(**item) for item in data]
        except FileNotFoundError:
            return []

class ConsoleNotification(NotificationStrategy):
    async def notify(self, message: str) -> None:
        logger.info(f"Scraping notification: {message}")

# Cache manager
class CacheManager:
    def __init__(self):
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

    def get_cached_price(self, product_title: str) -> Optional[float]:
        cached_price = self.redis_client.get(product_title)
        return float(cached_price) if cached_price else None

    def set_cached_price(self, product_title: str, price: float) -> None:
        self.redis_client.set(product_title, str(price))

# Scraper class
class WebScraper:
    def __init__(
        self,
        storage: StorageStrategy,
        notification: NotificationStrategy,
        cache_manager: CacheManager
    ):
        self.storage = storage
        self.notification = notification
        self.cache_manager = cache_manager

    async def scrape_with_retry(self, url: str, proxy: Optional[str] = None) -> str:
        for attempt in range(MAX_RETRIES):
            try:
                async with httpx.AsyncClient(proxies=proxy) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                    return response.text
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    raise
                logger.warning(f"Retry {attempt + 1}/{MAX_RETRIES} after error: {str(e)}")
                await asyncio.sleep(RETRY_DELAY)

    async def parse_product(self, html: str) -> List[Product]:
        soup = BeautifulSoup(html, 'html.parser')
        products = []
        
        # Note: This is a placeholder implementation. 
        # You need to adjust the selectors based on the target website's structure
        for product_element in soup.select('.product-item'):
            title = product_element.select_one('.product-title').text.strip()
            price = float(product_element.select_one('.product-price').text.strip().replace('$', ''))
            image_url = product_element.select_one('img')['src']
            
            # Download and save image
            image_path = f"images/{title.lower().replace(' ', '_')}.jpg"
            os.makedirs('images', exist_ok=True)
            
            # Only process if price has changed
            cached_price = self.cache_manager.get_cached_price(title)
            if cached_price != price:
                products.append(Product(
                    product_title=title,
                    product_price=price,
                    path_to_image=image_path
                ))
                self.cache_manager.set_cached_price(title, price)
                
                # Download image
                async with httpx.AsyncClient() as client:
                    response = await client.get(image_url)
                    with open(image_path, 'wb') as f:
                        f.write(response.content)
                        
        return products

    async def scrape_catalog(self, settings: ScrapingSettings) -> List[Product]:
        all_products = []
        page = 1
        
        while True:
            if settings.page_limit and page > settings.page_limit:
                break
                
            url = f"{settings.target_url}?page={page}"
            html = await self.scrape_with_retry(url, settings.proxy)
            products = await self.parse_product(html)
            
            if not products:  # No more products found
                break
                
            all_products.extend(products)
            page += 1
            
        return all_products

# Authentication dependency
async def verify_token(authorization: str = Header(...)):
    if authorization != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=401, detail="Invalid token")
    return authorization

# FastAPI endpoints
@app.post("/scrape")
async def scrape_products(
    settings: ScrapingSettings,
    _: str = Depends(verify_token)
):
    try:
        # Initialize components
        storage = JSONFileStorage()
        notification = ConsoleNotification()
        cache_manager = CacheManager()
        scraper = WebScraper(storage, notification, cache_manager)
        
        # Perform scraping
        products = await scraper.scrape_catalog(settings)
        
        # Save products
        await storage.save_products(products)
        
        # Send notification
        message = f"Scraping completed. {len(products)} products were processed."
        await notification.notify(message)
        
        return {"status": "success", "products_processed": len(products)}
        
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))