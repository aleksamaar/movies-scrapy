import re
import json
import scrapy


class ImdbRatingSpider(scrapy.Spider):
    name = "imdbratingspider"
    allowed_domains = ["ru.wikipedia.org", "www.imdb.com", "imdb.com"]

    start_urls = [
        "https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"
    ]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,

        # очень медленно и последовательно
        "DOWNLOAD_DELAY": 15.0,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,

        # авто-троттлинг
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 10.0,
        "AUTOTHROTTLE_MAX_DELAY": 30.0,

        # чтобы IMDb реже капризничал
        "COOKIES_ENABLED": True,
        "DEFAULT_REQUEST_HEADERS": {
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
        },

        "RETRY_ENABLED": True,
        "RETRY_TIMES": 2,
        "REDIRECT_ENABLED": True,

        # выводим только нужные поля
        "FEED_EXPORT_FIELDS": ["imdb_id", "title", "imdb_rating"],
    }

    max_movies = 3

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._movie_count = 0

    def parse(self, response):
        subcat_links = response.css("#mw-subcategories a::attr(href)").getall()
        for href in subcat_links:
            yield response.follow(href, callback=self.parse_category)

        yield from self.parse_category(response)

    def parse_category(self, response):
        if self._movie_count >= self.max_movies:
            return

        page_links = response.xpath(
            '//*[@id="mw-pages"]//a[starts-with(@href, "/wiki/")]/@href'
        ).getall()

        for href in page_links:
            if self._movie_count >= self.max_movies:
                break

            if ":" in href or href.startswith("/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:"):
                continue

            self._movie_count += 1
            yield response.follow(href, callback=self.parse_wikipedia_movie)

        if self._movie_count < self.max_movies:
            next_page = response.xpath(
                '//*[@id="mw-pages"]//a[contains(., "Следующая страница")]/@href'
            ).get()
            if next_page:
                yield response.follow(next_page, callback=self.parse_category)

    def parse_wikipedia_movie(self, response):
        # FIX: достаём title надёжно (с fallback)
        title = self.extract_wiki_title(response)

        imdb_id = self.extract_imdb_id_from_wiki_html(response)
        if imdb_id:
            yield from self.request_imdb(title, imdb_id)
            return

        raw_url = response.url + ("&" if "?" in response.url else "?") + "action=raw"
        yield response.follow(
            raw_url,
            callback=self.parse_wikipedia_movie_raw,
            meta={"title": title},
        )

    def parse_wikipedia_movie_raw(self, response):
        title = response.meta.get("title")

        imdb_id = self.extract_imdb_id_from_text(response.text or "")
        if imdb_id:
            yield from self.request_imdb(title, imdb_id)
        else:
            yield {
                "imdb_id": None,
                "title": title,
                "imdb_rating": None,
            }

    def request_imdb(self, title: str, imdb_id: str):
        imdb_url = f"https://www.imdb.com/title/{imdb_id}/"
        yield scrapy.Request(
            imdb_url,
            callback=self.parse_imdb,
            meta={"title": title, "imdb_id": imdb_id},
            dont_filter=True,
        )

    def parse_imdb(self, response):
        title = response.meta.get("title")
        imdb_id = response.meta.get("imdb_id")

        rating = self.extract_rating_from_imdb_jsonld(response)

        yield {
            "imdb_id": imdb_id,
            "title": title,
            "imdb_rating": rating,
        }

    # ---------------- helpers ----------------

    def extract_wiki_title(self, response) -> str | None:
        # новая разметка: <span class="mw-page-title-main">...</span>
        t = response.css("#firstHeading .mw-page-title-main::text").get()
        t = self.clean_text(t)
        if t:
            return t

        # иногда текст прямо в #firstHeading
        t = response.css("#firstHeading::text").get()
        t = self.clean_text(t)
        if t:
            return t

        # запасной вариант
        t = response.css("h1#firstHeading::text").get()
        return self.clean_text(t)

    @staticmethod
    def extract_imdb_id_from_wiki_html(response) -> str | None:
        hrefs = response.xpath('//a[contains(@href,"imdb.com/title/")]/@href').getall()
        for h in hrefs:
            m = re.search(r"(tt\d{7,8})", h)
            if m:
                return m.group(1)
        return None

    @staticmethod
    def extract_imdb_id_from_text(text: str) -> str | None:
        m = re.search(r"(tt\d{7,8})", text or "")
        return m.group(1) if m else None

    @staticmethod
    def extract_rating_from_imdb_jsonld(response) -> str | None:
        scripts = response.css('script[type="application/ld+json"]::text').getall()
        for s in scripts:
            s = (s or "").strip()
            if not s:
                continue
            try:
                data = json.loads(s)
            except Exception:
                continue

            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if not isinstance(obj, dict):
                    continue
                agg = obj.get("aggregateRating")
                if isinstance(agg, dict):
                    rating = agg.get("ratingValue")
                    if rating is not None:
                        return str(rating)

        return None

    @staticmethod
    def clean_text(text):
        if not text:
            return None
        text = re.sub(r"\[\d+\]", "", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text or None
