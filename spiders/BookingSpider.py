import scrapy
import re

class BookingSpider(scrapy.Spider):
    name = "booking"
    start_urls = [
        'https://www.booking.com/hotel/ch/caschu-alp-boutique-design.html',
        'https://www.booking.com/hotel/ch/sporthotelstoos.html',
    ]

    custom_settings = {
        'LOG_FILE': 'log/booking.log',
        'DOWNLOAD_DELAY' : 0.40422,
    }


    def store_result(self, url, dict):
        id = self.url_to_id[url]
        self.results[id] = dict
        print(str(len(self.results.keys())) + "/" + str(len(self.url_to_id)) + ": Collected " + dict['bk_name'] + " from Booking.com")

    def parse(self, response):
        url = response.url
        attributes = {}
        data_questions = ["hotel_clean", "hotel_comfort", "hotel_location", "hotel_services", "hotel_staff",
                          "hotel_value", "hotel_wifi"]
        data_queries = ['(//*[@data-question="' + q + '"])[1]/p/text()' for q in data_questions]

        # Retrieve the name
        name = response.xpath('//*[@id="hp_hotel_name"]/text()').extract()[0].strip('\n').encode('utf-8')
        attributes['bk_name'] = name
        # Retrieve the rating
        rating = response.xpath('(//*[@class="review-score-badge"])[1]/text()').extract()
        if len(rating) > 0:
            attributes['bk_ratingvalue'] = rating[0].strip().replace(",",".").encode('utf-8')
        # Retrieve the review count
        reviews = response.xpath('(//*[@class="review-score-widget__subtext"])[1]/text()').extract()
        if len(reviews) > 0:
            reviews = [elem.encode('utf-8').strip() for elem in reviews]
            reviews = [re.sub("[^0-9]", "", elem) for elem in reviews]
            for review in reviews:
                if len(review) > 0:
                    attributes['bk_reviewcount'] = review
        # Retrieve the ratings for specific parts of the hotel
        for i in range(len(data_queries)):
            result = response.xpath(data_queries[i]).extract()
            if len(result) > 0:
                attributes['bk_'+data_questions[i]] = result[1].replace(",",".").encode('utf-8')
        # Store the result
        self.store_result(url, attributes)