import re
import scrapy
from movies_scrapy.items import MovieItem


class MovieSpider(scrapy.Spider):
    name = "moviespider"
    allowed_domains = ["ru.wikipedia.org"]

    start_urls = [
        "https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"
    ]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 3,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,

        # фиксируем порядок и набор колонок в CSV (url не попадёт вообще)
        "FEED_EXPORT_FIELDS": ["title", "genre", "director", "country", "year", "imdb_id"],
    }

    def parse(self, response):
        subcat_links = response.css("#mw-subcategories a::attr(href)").getall()
        for href in subcat_links:
            yield response.follow(href, callback=self.parse_category)

        yield from self.parse_category(response)

    def parse_category(self, response):
        page_links = response.xpath(
            '//*[@id="mw-pages"]//a[starts-with(@href, "/wiki/")]/@href'
        ).getall()

        movie_links = []
        for href in page_links:
            if ":" in href or href.startswith("/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:"):
                continue
            movie_links.append(href)
            yield response.follow(href, callback=self.parse_movie)

        self.logger.warning(
            "Category page: %s | movie links found: %d",
            response.url,
            len(movie_links),
        )

        # пагинация
        next_page = response.xpath(
            '//*[@id="mw-pages"]//a[contains(., "Следующая страница")]/@href'
        ).get()
        if next_page:
            yield response.follow(next_page, callback=self.parse_category)

    # ---------------- main movie parsing ----------------

    def parse_movie(self, response):
        item = MovieItem()

        title = response.css("#firstHeading span::text").get()
        item["title"] = self.clean_text(title)

        infobox = response.css("table.infobox")
        if infobox:

            def get_infobox_value(labels):
                for label in labels:
                    row = infobox.xpath(
                        './/tr[th[contains(normalize-space(.), $label)]]',
                        label=label
                    )
                    if row:
                        td = row.xpath("./td[1]")
                        if td:
                            text = self.td_text(td)
                            if text:
                                return text
                return None

            item["genre"] = get_infobox_value(["Жанр", "Жанры"])
            item["director"] = get_infobox_value(["Режиссёр", "Режиссер", "Режиссёры", "Режиссеры"])
            item["country"] = get_infobox_value(["Страна", "Страны"])

            year_raw = get_infobox_value(["Год", "Годы", "Дата выхода", "Премьера"])
            item["year"] = self.extract_year(year_raw)

        # IMDb ID — как в ImdbRatingSpider: сначала HTML, если нет — action=raw
        imdb_id = self.extract_imdb_id_from_wiki_html(response)
        if imdb_id:
            item["imdb_id"] = imdb_id
            yield item
            return

        raw_url = response.url + ("&" if "?" in response.url else "?") + "action=raw"
        yield response.follow(raw_url, callback=self.parse_movie_raw, meta={"item": item})

    def parse_movie_raw(self, response):
        item = response.meta["item"]
        item["imdb_id"] = self.extract_imdb_id_from_text(response.text or "")
        yield item

    # ---------------- helpers ----------------

    def td_text(self, td) -> str | None:
        parts = td.xpath('.//text()[not(ancestor::style) and not(ancestor::script)]').getall()
        text = " ".join(p.strip() for p in parts if p and p.strip())
        text = self.clean_text(text)
        return text or None

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
        m = re.search(r"(tt\d{7,8})", text)
        return m.group(1) if m else None

    @staticmethod
    def clean_text(text):
        if not text:
            return None
        # remove footnote refs like [1], [2], ...
        text = re.sub(r"\[\d+\]", "", text)
        # normalize spaces
        text = re.sub(r"\s+", " ", text).strip()
        return text or None

    @staticmethod
    def extract_year(text):
        if not text:
            return None
        m = re.search(r"(18|19|20)\d{2}", text)
        return m.group(0) if m else None
