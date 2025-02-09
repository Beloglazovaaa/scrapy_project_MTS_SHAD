import scrapy
import re

class FilmDataSpider(scrapy.Spider):
    name = 'film_data'
    allowed_domains = ['ru.wikipedia.org', 'www.imdb.com']
    start_urls = ['https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту']

    def exclude_special_notations(self, data):
        """Фильтрует список, удаляя служебные обозначения."""
        pattern = r'(\[|\()\w+.?(\]|\))|\xa0|\n|\d|рус\.|англ\.|\[.*?\]|/|\*|\(|\)|,'
        cleaned_data = [text.strip() for text in data if not re.search(pattern, text)]
        return [s for s in ','.join(cleaned_data).replace(',,,', ',').replace(',,', ',').split(',') if s]

    def parse(self, response):
        """Парсит список фильмов на странице категории и переходит по ссылкам."""
        links = response.xpath('//div[@id="mw-pages"]//div[@class="mw-category-group"]//a/@href').getall()
        for link in links:
            yield response.follow(link, callback=self.parse_film_data)
        next_page = response.xpath('//a[contains(text(), "Следующая страница")]/@href').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_film_data(self, response):
        """Извлекает данные о фильме со страницы фильма."""
        title = response.xpath('//*[@class="infobox-above"]//text()').get()
        if not title:
            title = response.xpath('//h1[@id="firstHeading"]/text()').get()

        genre = response.xpath('//*[@data-wikidata-property-id="P136"]//text()').getall()
        director = response.xpath('//*[@data-wikidata-property-id="P57"]//text()').getall()
        country_name = response.xpath('//*[@data-wikidata-property-id="P495"]//text()').getall()
        year = response.xpath(
            '//*[@data-wikidata-property-id="P577"]//a[@title]//text() | '
            '//*[@class="dtstart"]//text()'
        ).getall()

        data = {
            'Название': title.strip() if title else "Не найдено",
            'Жанр': self.exclude_special_notations(genre),
            'Режиссер': self.exclude_special_notations(director),
            'Страна': self.exclude_special_notations(country_name),
            'Год': year[-1].strip() if year else "Не найден",
            'Рейтинг IMDB': None,
        }

        imdb_url = response.xpath('//a[contains(@href, "imdb.com/title/")]/@href').get()
        if imdb_url:
            yield scrapy.Request(imdb_url, callback=self.parse_imdb_rating, cb_kwargs={'data': data})
        else:
            yield data

    def parse_imdb_rating(self, response, data):
        """Извлекает рейтинг фильма со страницы IMDb."""
        rating = response.xpath(
            '//div[@data-testid="hero-rating-bar__aggregate-rating__score"]//text() | '
            '//span[@itemprop="ratingValue"]/text()'
        ).get()
        data['Рейтинг IMDB'] = rating.strip() if rating else "Нет рейтинга"
        yield data
