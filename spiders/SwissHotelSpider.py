import scrapy
import re

class SwissHotelSpider(scrapy.Spider):
    name = "swisshotel"
    start_urls = [
        'https://hotels.swisshoteldata.ch/?module=hotel&submodule=detail&id=10580',
        'https://hotels.swisshoteldata.ch/?module=hotel&submodule=detail&id=12351',
    ]

    custom_settings = {
        'LOG_FILE': 'log/swisshotel.log',
        'DOWNLOAD_DELAY' : 0.2238,
    }

    def store_result(self, url, dict):
        id = self.url_to_id[url]
        self.results[id] = dict
        print(str(len(self.results.keys())) + "/" + str(len(self.url_to_id)) + ": Collected " + dict['sh_name'] + " from SwissHotel")

    def clean_list(self, list):
        list = [elem.encode('utf-8') for elem in list]
        return [elem.replace(' ','_').replace('/','_').lower() for elem in list]

    def starts_with_four_digits(self, str):
        return str.strip()[0:4].isdigit()

    def parse(self, response):
        url = response.url
        attributes = {}
        # Collect the name
        attributes['sh_name'] = response.xpath('//*[@class="page-title"]/text()').extract()[0].encode('utf-8').strip()
        # Collect infos such as check-in time, check-out time, Room | Beds, banquet room, meeting room
        infos = response.xpath('//*[@class="hotel-info-block"]/div/ul/li').extract()
        infos = [info.encode('utf-8').strip("<li>").strip("</li>").strip("</span>") for info in infos]
        infos = [info.split("<span>") for info in infos]
        for id, value in infos:
            if '|' in id:
                ids = id.split('|')
                values = value.split('|')
                attributes['sh_rooms'] = values[0].strip(' ')
                attributes['sh_beds'] = values[1].strip(' ')
            else:
                attributes['sh_'+id.lower().replace(' ', '_')] = value
        trust_you_link = response.xpath('//*[@class="button hotel-review"]/@href').extract()
        if len(trust_you_link) > 0:
            attributes['trust_you'] = trust_you_link[0]
        # Add all the infrastructure attributes
        infrastructure = response.xpath('//*[@class="content-column"]/ul/li/text()').extract()
        if len(infrastructure) > 0:
            for element in self.clean_list(infrastructure):
                attributes['sh_infrastructure_'+element] = True

        # Local infrastructure attributes
        local_infrastructure = response.xpath('//*[@class="infrastructur-ort clearfix"]/li/img/@alt').extract()
        if len(local_infrastructure) > 0:
            for element in self.clean_list(local_infrastructure):
                attributes['sh_local_'+element] = True
        # Extract possible payment methods
        payment_methods = response.xpath('//*[@class="credit-cards clearfix"]/img/@alt').extract()
        if len(payment_methods) > 0:
            for element in self.clean_list(payment_methods):
                attributes['sh_payment_method_'+element] = True

        # Collect the address and the managers
        address = response.xpath('//p[@class="address"]/text()').extract()
        address = [elem.encode('utf-8') for elem in address]
        address = [elem.strip() for elem in address]
        attributes['sh_street'] = address[0]
        # Which line starts with the zip code?
        start_pos = 1
        while not self.starts_with_four_digits(address[start_pos]):
            start_pos += 1
            if start_pos == len(address) :
                break
        if start_pos < len(address):
            attributes['sh_code'] = re.sub("[^0-9]", "", address[start_pos])
            attributes['sh_city'] = re.sub("[,.!?0-9]", "", address[start_pos]).strip()
            attributes['sh_telephone'] = address[start_pos+1]
            managers = ""
            for manager in address[start_pos+3:]:
                managers += manager + "; "
            attributes['sh_managers'] = managers.strip()
        else:
            print("Could not retrieve address from " + attributes['sh_hotel'])

        # Retrieve the star classification of the hotel and others if necessary
        classification = response.xpath('//*[@class="block block-classification"]/div/div/img/@alt').extract()
        if len(classification) > 0:
            classification_0 = classification[0].encode('utf-8')
            start = 0
            # Special treatment for stars
            if 'stars' in classification_0:
                attributes['sh_stars'] = classification_0
                start = 1
            classification = self.clean_list(classification)
            # All other classifications are added normally
            for i in range(start, len(classification)):
                attributes['sh_classification_' + classification[i]] = True

        # Hotel chains
        chains = response.xpath('//*[@class="content-section"]/p/a/text()').extract()
        if len(chains) > 0:
            for element in self.clean_list(chains):
                attributes['sh_chain_' + element] = True

        # Specialization such as golf or seminar hotel
        specialization = response.xpath('//*[@class="block block-specializations"]/div/ul/li/img/@alt').extract()
        if len(specialization) > 0:
            for element in self.clean_list(specialization):
                attributes['sh_specialization_'+element] = True

        self.store_result(url, attributes)