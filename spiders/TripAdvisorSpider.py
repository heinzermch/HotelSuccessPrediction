import scrapy
import re

class TripAdvisorSpider(scrapy.Spider):
    name = "tripadvisor"
    start_urls = [
        'https://www.tripadvisor.ch/Hotel_Review-g1096125-d654887-Reviews-Seminar_und_Wellnesshotel_Stoos-Stoos.html',
        'https://www.tripadvisor.ch/Hotel_Review-g1096125-d1204244-Reviews-Minotel_Alpstubli-Stoos.html',
    ]

    custom_settings = {
        'LOG_FILE': 'log/tripadvisor.log',
        'DOWNLOAD_DELAY': 0.631,
    }

    allowed_values = ['name', 'pricerange', 'ratingvalue', 'reviewcount', 'streetaddress', 'addresslocality', 'postalcode']

    def store_result(self, url, dict):
        if 'ta_addresslocality' in dict.keys():
            dict['ta_city'] = dict['ta_addresslocality']
            del dict['ta_addresslocality']
        if self.use_url_as_id:
            self.results[url] = dict
            print(str(len(self.results.keys())) + "/" + str(len(self.start_urls)) + ": Collected " + dict[
                'ta_name'] + " from TripAdvisor, storing on ID " + str(url))
        else:
            id = self.url_to_id[url]
            self.results[id] = dict
            print(str(len(self.results.keys())) + "/" + str(len(self.url_to_id)) + ": Collected " + dict['ta_name'] + " from TripAdvisor, storing on ID " + str(id))

    def parse(self, response):
        url = response.url
        # Lets try to extract the info directly from the script tag
        script_content = response.xpath('//*[@type="application/ld+json"]/text()').extract()[0]
        # Change encoding from unicode to ascii
        script_content = script_content.encode('utf-8')
        # Clean the data
        script_content = script_content.replace('"','').replace('@', '').replace('{', '').replace('}','')
        attributes = script_content.split(',')
        attributes = [attribute for attribute in attributes if ':' in attribute]
        storable_attributes = {}
        for attribute in attributes:
            # Find the first occurence of ':' and split take whats before as id, whats after as content
            attribute = attribute.split(':')
            if len(attribute) != 2:
                next
            else:
                id = attribute[0].lower().strip()
                value = attribute[1].strip()
                if id in self.allowed_values:
                    # Avoid overwriting the name of the hotel with the country
                    if 'ta_' + id not in storable_attributes.keys():
                        # Remove the non-breaking space in UTF-8 character
                        storable_attributes['ta_' + id] = value.replace("\xc2\xa0", "")

        # Treat the rare case where letters are in the code (example: CH-3843)
        if 'ta_postalcode' in storable_attributes.keys():
            storable_attributes['ta_postalcode'] = re.sub("[^0-9]", "", storable_attributes['ta_postalcode'])

        self.store_result(url, storable_attributes)
