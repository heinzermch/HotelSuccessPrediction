"""
    Main entry point for the program which handeles all the data. Can also be collect data directly
    using the crawlers. Some methods are dependent on each other, especially the basic customer
    data has to be initialzed for almost all actions.

    Author: Michael Heinzer
    Date: 01.05.2018

"""


from DatabasePandas import Database
from scrapy.crawler import CrawlerProcess
from TripAdvisorSpider import TripAdvisorSpider
from BookingSpider import BookingSpider
from SwissHotelSpider import SwissHotelSpider
from DatabasePandas import Matching
import pandas as pd

# Set the google API key for geolocation queries, key needs to be set before the import of geocoder!
import os

import geocoder
# Necessary to read and write non-standard characters
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


# data sources for hotel data, the original data, the results of the booking crawl, the results of the tripadvisor crawl
INPUT_HOTELS_DATA = 'fullRun/input.csv'
INPUT_HOTELS_COORDINATES = 'coordinates/hotels_coordinates.csv'
INPUT_SWISSHOTELS = "swisshotels/swisshotels_full.csv" # Complete data from swisshotel crawl
INPUT_HOTELS_GOOGLE_CRAWL = "fullRun/autogoogle.csv" # Data from the google rating crawl
INPUT_SWISSHOTELS_COORDINATES = "coordinates/swisshotel_coordinates.csv" # Coordinates for the swisshotels
INPUT_SWISSHOTELS_GOOGLE_CRAWL = "swisshotels/sh_google.csv" # Complete data from google crawl for swisshotels
INPUT_SWISSHOTELS_ATTRIBUTES_MERGE = 'swisshotels/swisshotels_attributes_replace.csv'
INPUT_SWISSHOTELS_FULL = 'swisshotels/swisshotels_full_cleaned.csv'
INPUT_GOOGLE_TRENDS_PLACES = 'trends/places.csv'
INPUT_MATCHING_TEST_SAMPLE = 'matching/random_sample_corrected.csv'
INPUT_MATCHING_FULL_CORRECTED = 'matching/matching_full_corrected.csv'
INPUT_TRIPADVISOR_DATA = 'fullRun/output_ta_full.csv'
INPUT_TRIPADVISOR_REVIEWS = 'tripadvisor/reviews_scraped.csv'
INPUT_TRIPADVISOR_REVIEWS_YEARLY = 'tripadvisor/reviews_yearly.csv'
INPUT_TRIPADVISOR_MISSING_REVIEWS = 'tripadvisor/missing_scraped.csv'
INPUT_TRIPADVISOR_ALL_HOTELS = 'tripadvisor/all_tripadvisor_hotels_crawled.csv'
INPUT_TRIPADVISOR_HOTELS_COORDINATES = 'coordinates/tripadvisor_hotels_raw.csv'
INPUT_TRIPADVISOR_HOTELS_WEBSCRAPER = 'tripadvisor/all_tripadvisor_hotels_information.csv'
INPUT_BOOKING_DATA = 'fullRun/output_bk_full.csv'
INPUT_PREDICTION = 'prediction/pred_vals.csv'
INPUT_REVENUE_DETAILS = 'prediction/PredValDet.csv'
INPUT_ECONOMIC_DATA = 'economic/economic_data.csv'
INPUT_ECONOMIC_DATA_COORDINATES = 'coordinates/economic_data_coordinates.csv'
INPUT_MATCHING_HOTEL_ECONOMIC = 'matching/hotels_economic.csv'
INPUT_HOTELS = [INPUT_HOTELS_DATA, INPUT_BOOKING_DATA, INPUT_TRIPADVISOR_DATA, INPUT_TRIPADVISOR_REVIEWS_YEARLY, INPUT_HOTELS_COORDINATES]
# Output files
OUTPUT_HOTELS = "fullRun/hotels_full.csv" # All the hotel data in one file
OUTPUT_MATCHING = "matching/matched_hotels.csv" # Matching hotels with swisshotels
OUTPUT_HOTELS_GOOGLE_PRECRAWL = "H:/homepage/google.html" # A page containing all the entries for which we want a google crawl
OUTPUT_SWISSHOTELS_GOOGLE_PRECRAWL = "H:/homepage/swisshotels/google.html"
OUTPUT_SWISSHOTELS_PRECRAWL = 'H:/homepage/swisshotels/index.html'
OUTPUT_SWISSHOTELS_CLEANED = 'swisshotels/swisshotels_full_cleaned.csv'
OUTPUT_SWISSHOTELS_COORDINATES = 'coordinates/swisshotel_coordinates.csv'
OUTPUT_SWISSHOTELS_ATTRIBUTE_NAMES = 'swisshotels/attribute_names.csv'
OUTPUT_GOOGLE_TRENDS_PRECRAWL = 'H:/homepage/trends/places.html'
OUTPUT_TRIPADVISOR_PRECRAWL = 'H:/homepage/tripadvisor/places.html'
OUTPUT_TRIPADVISOR_HOTELS_PRECRAWL = 'H:/homepage/tripadvisor/hotels.html'
OUTPUT_HOTELS_SCRAPING_ERRORS = 'fullRun/errors.csv'
OUTPUT_HOTELS_DISCREPANCIES = 'fullRun/discrepancies.csv'
OUTPUT_MATCHING_FUZZY_TEST = 'matching/matching_fuzzy_test.csv'
OUTPUT_MATCHING_FUZZY_FULL = 'matching/matching_fuzzy_full.csv'
OUTPUT_MATCHING_UID = 'uids/uid_matching.csv'
OUTPUT_TRIPADVISOR_DATA = 'fullRun/output_ta_full.csv'
OUTPUT_TRIPADVISOR_REVIEWS = 'tripadvisor/reviews.csv'
OUTPUT_TRIPADVISOR_REVIEWS_YEARLY = 'tripadvisor/reviews_yearly.csv'
OUTPUT_TRIPADVISOR_HOTELS = 'tripadvisor/tripadvisor_hotels.csv'
OUTPUT_TRIPADVISOR_HOTELS_CRAWL = 'tripadvisor/tripavsior_hotels_crawled.csv'
OUTPUT_TRIPADVISOR_HOTELS_COORDINATES = 'coordinates/tripadvisor_hotels_raw.csv'
OUTPUT_TRIPADVISOR_HOTELS_COORDINATES_CLEAN = 'coordinates/tripadvisor_hotels.csv'
OUTPUT_TRIPADVISOR_HOTELS_PRICE_PREDICTION = 'prediction/tripadvisor_price.csv'
OUTPUT_TRIPADVISOR_HOTELS_ROOM_PREDICTION = 'prediction/tripadvisor_rooms.csv'
OUTPUT_PREDICTION_REVENUE_SMALL = 'prediction/revenue_prediction_small.csv'
OUTPUT_PREDICTION_REVENUE_ALL = 'prediction/revenue_prediction_all.csv'
OUTPUT_REVENUE_DETAILS = 'prediction/yearly_revenue.csv'
OUTPUT_REVENUE_CLASSIFICATION = 'prediction/yearly_revenue_classification.csv'
OUTPUT_PREDICTION_REVENUE_CLASSIFICATION = 'prediction/revenue_classification.csv'
OUTPUT_HOTEL_SWISSHOTEL_REVIEWS_MERGE = 'prediction/hotel_swisshotel_merged.csv'
OUTPUT_HOTELS_COORDINATES = 'coordinates/hotels_coordinates.csv'
OUTPUT_ECONOMIC_DATA_COORDINATES = 'coordinates/economic_data_coordinates.csv'
OUTPUT_MATCHING_HOTEL_ECONOMIC = 'matching/hotels_economic.csv'
OUTPUT_MATCHING_TRIPADVISOR_SWISSHOTELS = 'matching/tripadvisor_swisshotels.csv'
OUTPUT_MATCHING_TRIPADVISOR_ECONOMIC_DATA = 'matching/tripadvisor_economic.csv'
# HTML Code
PREFIX_GOOGLE_CRAWL = "<!DOCTYPE html><html><body><h2>A list of Google querys to scrape</h2><ul>"
PREFIX_SWISSHOTELS_CRAWL = "<!DOCTYPE html><html><body><h2>A list of Swisshotel overviews to scrape</h2><ul>"
PREFIX_GOOGLE_TRENDS_CRAWL = "<!DOCTYPE html><html><body><h2>A list of Google Trends Places to scrape</h2><ul>"
PREFIX_TRIPADIVSOR_CRAWL = "<!DOCTYPE html><html><body><h2>A list of Tripadvisor sites to scrape</h2><ul>"
POSTFIX = "</ul></body></html>"
# Other parameters
TEST_MODE = False # When activated only a limited number of websites will get crawled
TEST_LIMIT = 100 # How many websites will be crawled in test mode
RANDOMIZE_TEST = True # Should the test sample be randomized?
MINIMUM_AVAILABLE_VALUES = 3


def kwargs_dict_from_urls(urls):
    """
    Take a list of (id,url) pairs and create a list of urls and a dictionary from url to id.
    :param urls: list of (id, url) pairs
    :return: dictionary which will be set as kwargs parameter, the name of the dictionary is the name of the
                attribute, the value will be the value of the attribute
    """
    start_urls = []
    url_to_id = {}
    for id, url in urls:
        start_urls.append(url)
        url_to_id[url] = id
    return {"url_to_id": url_to_id, "start_urls" : start_urls}


def write_html_file(filename, prefix, content, postfix):
    """
    Used to write a list of links to a file, or any file which is structures in the manner of prefix, content, postfix
    :param filename: path/filename to which the html content will be written
    :param prefix: header and pre content data
    :param content: list of elements
    :param postfix: closing the html list
    :return: None
    """
    file = open(filename, "wb")
    file.write(prefix)
    file.write(content)
    file.write(postfix)
    file.close()
    print("Wrote HTML-file to " + filename)


def prepare_google_data_collection(database, output_file=OUTPUT_HOTELS_GOOGLE_PRECRAWL):
    """
    Collect ratings from google, first we write an html page which contains all the google searches to be
    done by the scraper.
    Will need to use the booking or tripadvisor names in order to find the hotels.
    The crawl itself is then done externally by a chrome extension which uses this page as starting point
    :param database: a database object which is capable of returning a list of ids and urls
    :param output_file: where the html output will be stored (by default a public accessible webspace)
    :return: None
    """
    # Get names and place from the data we collected from tripadvisor and booking.com data
    names = database.get_tripadvisor_booking_names(TEST_MODE, TEST_LIMIT, RANDOMIZE_TEST)
    # Create html file content
    content = ""
    for name in names:
        content += '<li><a href="https://www.google.ch/search?q=' + name[1].replace(" ", "+").replace("&","") + '">'
        content +=  str(name[0]) + '</a><p>' + name[1] + '</p></li>\n'
    # Get the entries where we only have a website but not tripadivsor and booking data
    websites = database.get_websites_only_names(TEST_MODE, TEST_LIMIT, RANDOMIZE_TEST)
    for name in websites:
        content += '<li><a href="https://www.google.ch/search?q=' + name[1].replace(" ", "+").replace("&","") + '">'
        content +=  str(name[0]) + '</a><p>' + name[1] + '</p></li>\n'
    # Write the html file to disk
    write_html_file(output_file, PREFIX_GOOGLE_CRAWL, content, POSTFIX)

def prepare_tripadvisor_collection(database, output_file=OUTPUT_TRIPADVISOR_PRECRAWL):
    """
    Collect each rating from a list of tripadvisor hotels
    :param database: a database object which is capable of returning a list of ids and urls
    :param output_file: where the html output will be stored (by default a public accessible webspace)
    :return: None
    """
    # Get names and place from the data we collected from tripadvisor and booking.com data
    urls = database.get_tripadvisor_urls(TEST_MODE, TEST_LIMIT, RANDOMIZE_TEST)
    # Create html file content
    content = ""
    for url in urls:
        content += '<li><a href="'+ url[1] + '">'
        content +=  str(url[0]) + '</a><p>' + url[1] + '</p></li>\n'
    # Write the html file to disk
    write_html_file(output_file, PREFIX_TRIPADIVSOR_CRAWL, content, POSTFIX)


def prepare_tripadvisor_hotel_collection(database, input_file=OUTPUT_TRIPADVISOR_HOTELS, output_file=OUTPUT_TRIPADVISOR_HOTELS_PRECRAWL):
    """
        Collecting data which was downloaded with the internal crawler, exports a list in html
        which allows the webscraper inside Chrome to go through the websits (should be a neat list)
    :param database: initialized database object
    :param input_file: A tripadvisor CSV file containing urls
    :param output_file: A HTML file containing a list with hotel URLs to be crawled
    :return: None
    """
    urls = database.get_tripadvisor_hotels_urls(input_file)
    content = ""
    for url in urls:
        content += '<li><a href="' + url + '">'
        content += str(url) + '</a></li>\n'
    # Write the html file to disk
    write_html_file(output_file, PREFIX_TRIPADIVSOR_CRAWL, content, POSTFIX)

def prepare_google_data_collection_swisshotel(database, output_file=OUTPUT_SWISSHOTELS_GOOGLE_PRECRAWL):
    """
    Retrieves a list of names and ids from swisshotel data which will be used to crawl google using a
    webscraper in Chrome Browser
    :param database: a database object which is capable of returning a list of ids and urls
    :param output_file: where the html output will be stored (by default a public accessible webspace)
    :return: None
    """
    names = database.get_swisshotels_names()
    # Create html file content
    content = ""
    for name in names:
        content += '<li><a href="https://www.google.ch/search?q=' + name[1].replace(" ", "+").replace("&", "") + '">'
        content += str(name[0]) + '</a><p>' + name[1] + '</p></li>\n'
    # Write the html file to disk
    write_html_file(output_file, PREFIX_GOOGLE_CRAWL, content, POSTFIX)


def prepare_swisshotel_collection(output_file=OUTPUT_SWISSHOTELS_PRECRAWL):
    """
    Write a list of links to crawl for the chrome extension in order to collect all entries on the swisshotel
    homepage (currently around 4'000)
    :param output_file: where the html output will be stored (by default a public accessible webspace)
    :return: None
    """
    content = ""
    # The hardcoded number of 250 is ugly, however there are currently pages to crawl which are numbered from
    # 1 to 249, check https://hotels.swisshoteldata.ch and do a search without any restrictions to have an updated
    # number of the available data
    for i in range(1,250):
        content += '<li><a href="https://hotels.swisshoteldata.ch/?module=hotel&submodule=searchlist&page=' + str(i) +'&sorttype=classification&sortdirection=ASC">'
        content += 'Page ' + str(i) + '</a></li>\n'
    # Write the html file to disk
    write_html_file(output_file, PREFIX_SWISSHOTELS_CRAWL, content, POSTFIX)


def prepare_google_trends_collection(input_file=INPUT_GOOGLE_TRENDS_PLACES, output_file=OUTPUT_GOOGLE_TRENDS_PRECRAWL):
    """
    We create a list of places for which we want the google trends data to be scraped, the query will always
    be "<place> hotel". The resulting crawl will then automatically download the file without bothering if the
    trend data even exists.
    :param output_file: List of URLs to google trend searches
    :return: None
    """
    content = ""
    # Read the places
    places = pd.read_csv(input_file)
    for place in list(places.places):
        content += '<li><a href="https://trends.google.de/trends/explore?date=all&q=hotel%20'
        content += place.replace(' ', '%20') + '">'
        content += 'Page ' + place + '</a></li>\n'
    # Write the html file to disk
    write_html_file(output_file, PREFIX_GOOGLE_TRENDS_CRAWL, content, POSTFIX)


def collect_google_data(database, hotel_path, swisshotel_path):
    """
    Gives the order to retrieve data from the Google crawls and incorporate the data into the two
    existing databases of hotels and swisshotels
    :param database: The database in which the data has to be read
    :param hotel_path: Path to the CSV file which contains data from the Google crawl of hotel entries
    :param swisshotel_path: Path to the CSV file which contains data from the Google crawl of swisshotel entries
    :return: None
    """
    database.collect_google_data_from_csv(hotel_path, swisshotel_path)


def add_booking_spider(process, urls):
    """
    Retrieves the data for the Booking spider from the database and adds it to the spider. A dictionary
    containing the results is created and shared with the spider. It will be used to store the data retrieved
    from the crawl
    :param process: A scrapy process which will handle the spider
    :param database: Database which contains the URLs to be scraped from the Booking.com website
    :return: A dictionary in which the data from the crawl will be stored, indexed by ID
    """
    kwargs = kwargs_dict_from_urls(urls)
    results = {}
    kwargs["results"] = results
    process.crawl(BookingSpider(), **kwargs)
    return results


def add_tripadvisor_spider(process, urls):
    """
    Retrieves the data for the Tripadvisor spider from the database and adds it to the spider. A dictionary
    containing the results is created and shared with the spider. It will be used to store the data retrieved
    from the crawl
    :param process: A scrapy process which will handle the spider
    :param database: Database which contains the URLs to be scraped from the Tripadvisor website
    :return: A dictionary in which the data from the crawl will be stored, indexed by ID
    """
    kwargs = kwargs_dict_from_urls(urls)
    results = {}
    kwargs["results"] = results
    kwargs["use_url_as_id"] = False
    process.crawl(TripAdvisorSpider(), **kwargs)
    return results


def add_swisshotel_spider(process, urls):
    """
    Retrieves the swisshotel URLs to be scraped from the database and creates a spider which crawls the swisshotel
    website. A dictionary containing the results is created and shared with the spider.
    It will be used to store the data retrieved
    from the crawl
    :param process: A scrapy process which will handle the spider
    :param database: Database which contains the URLs to be scraped from the swisshotels website
    :return: A dictionary in which the data from the crawl will be stored, indexed by ID
    """
    kwargs = kwargs_dict_from_urls(urls)
    results = {}
    kwargs["results"] = results
    process.crawl(SwissHotelSpider(), **kwargs)
    return results


def collect_tripadvisor_data(database, output_tripadvisor='fullRun/tripadvisor_crawl.csv'):
    """
    Creates and starts a process which will handle the crawl, once the crawl is finished the results will be stored
    in the database
    :param database: database which is able to retrieve the tripadivsor urls and store the results of the crawl
    :return: None
    """
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })
    results = add_tripadvisor_spider(process, database.get_tripadvisor_urls(TEST_MODE, TEST_LIMIT, RANDOMIZE_TEST))
    print("Starting the crawl for tripadvisor")
    process.start()  # the script will block here until the crawling is finished
    print("Finished the crawl for tripadvisor")
    database.store_scraping_results(results, True)


def collect_tripadvisor_all_hotel_data(database, tripadvisor_urls_input=INPUT_TRIPADVISOR_ALL_HOTELS):
    """

    :param database:
    :return:
    """
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })
    start_urls = database.get_all_tripadvisor_urls(tripadvisor_urls_input, TEST_MODE, TEST_LIMIT, RANDOMIZE_TEST)
    kwargs = {"start_urls" : start_urls}
    results = {}
    kwargs["results"] = results
    kwargs["use_url_as_id"] = True
    process.crawl(TripAdvisorSpider(), **kwargs)
    print("Starting the crawl for tripadvisor")
    process.start()  # the script will block here until the crawling is finished
    print("Finished the crawl for tripadvisor")
    database.store_scraping_results(results, True, True)


def collect_booking_data(database):
    """
    Creates and starts a process which will handle the crawl, once the crawl is finished the results will be stored
    in the database
    :param database: database which is able to retrieve the booking.com urls and store the results of the crawl
    :return: None
    """
    process = CrawlerProcess({})
    results = add_booking_spider(process, database.get_booking_urls(TEST_MODE, TEST_LIMIT, RANDOMIZE_TEST))
    print("Starting the crawl for booking")
    process.start()  # the script will block here until the crawling is finished
    print("Finished the crawl for booking")
    database.store_scraping_results(results, True)


def collect_swisshotel_database(database):
    """
    Creates and starts a process which will handle the crawl, once the crawl is finished the results will be stored
    in the database
    :param database: database which is able to retrieve the swisshotels urls and store the results of the crawl
    :return: None
    """
    process = CrawlerProcess({})
    results = add_swisshotel_spider(process, database.get_swisshotel_urls(TEST_MODE, TEST_LIMIT, RANDOMIZE_TEST))
    print("Starting the crawl for swisshotel")
    process.start()  # the script will block here until the crawling is finished
    print("Finished the crawl for swisshotel")
    database.store_scraping_results(results, False)




def collect_tripadvisor_booking_data(database):
    """
    Creates and starts a process which will handle the crawl, once the crawl is finished the results will be stored
    in the database. The two crawls of tripadvisor and booking will run in parallel, both will store their results
    in different dictionaries in order not to overwrite each others results. The crawl has to be this way as it
    is not possible to run multiple processes easily in one session of the program.
    :param database: database which is able to retrieve the tripadivsor/booking urls and store the results of the crawl
    :return: None
    """
    process = CrawlerProcess({})
    results_t = add_tripadvisor_spider(process, database.get_tripadvisor_urls(TEST_MODE, TEST_LIMIT, RANDOMIZE_TEST))
    results_b = add_booking_spider(process, database.get_booking_urls(TEST_MODE, TEST_LIMIT, RANDOMIZE_TEST))
    print("Starting the crawl for tripadvisor and booking")
    process.start()  # the script will block here until the crawling is finished
    print("Finished the crawl for tripadvisor and booking")
    database.store_scraping_results(results_t, True)
    database.store_scraping_results(results_b, True)


def construct_database(hotels_csv=INPUT_HOTELS, swisshotels_csv=INPUT_SWISSHOTELS_FULL, economic_data=INPUT_ECONOMIC_DATA):
    """
    Create the database object which will be vital to process and store all the information we retrieve online.
    Usually the results of the crawls from booking and tripadvisor are stored in separate CSVs in order to handle
    them easier. All of that data will be merged inside the database.
    :param hotels_csv: List containing all the CSVs which belong into the same hotel database
    :param swisshotels_csv: The path to the swisshotel CSV file, if its none we wont load swisshotel data
    :return: the database object to access and store data related to hotels
    """
    database = Database()
    print("Loading the hotel database from CSV")
    # Check if we want to load a single csv or a many
    if 'str' in str(type(hotels_csv)):
        database.retrieve_hotels_from_csv(hotels_csv)
    else:
        database.retrieve_hotels_from_csvs(hotels_csv)
    print("Loading the swisshotel database from CSV")
    database.retrieve_swisshotels_from_csv(swisshotels_csv)
    print("Loading the economic data from CSV")
    database.retrieve_economic_data_from_csv(economic_data)
    return database


def collect_geolocation_data(database, output_file=OUTPUT_HOTELS_COORDINATES):
    """
    Order the database to start downloading the coordinates from the map
    :param database: an initialized database object
    :return: None
    """
    print("Collecting geolocation data")
    database.collect_hotel_geolocation_data()
    database.store_hotel_geolocation_data(output_file)
    print("Done collection geolocation data")


def create_location_map(database):
    """
    Retrieve geolocation data from the database and draw a point for every hotel in the database
    on the map of Switzerland
    :param database: initialized database objects which contains geolocation data
    :return: None
    """
    from mpl_toolkits.basemap import Basemap
    import matplotlib.pyplot as plt

    map = Basemap(llcrnrlon=5.4, llcrnrlat=45.6, urcrnrlon=10.8, urcrnrlat=47.8,
                  area_thresh=0.1, resolution='i', projection='tmerc', lat_0=46., lon_0=8.)

    map.drawmapboundary(fill_color='black')
    map.fillcontinents(color='#f7f7a5', lake_color='#99bbff')
    map.drawcoastlines()
    map.drawcountries()

    geolocation_data = database.get_geolocation_data()
    for id, lat, lon in geolocation_data:
        x, y = map(lon, lat)
        map.plot(x, y, 'yo', markersize=4)
    plt.show()


def create_statistics(database):
    """
    OUTDATED
    Was used to create files to use in R and display some information about the entries in the database
    :param database: Initialized database object
    :return: None
    """
    print("Tripadvisor ratings: " + database.get_number_of_tripadvisor_ratings())
    print("Google ratings: " +database.get_number_of_google_ratings())
    print("Booking ratings: " + database.get_number_of_booking_ratings())
    print("Total entries: " + database.get_number_of_entries())


def find_scraping_errors(database, output_file=OUTPUT_HOTELS_SCRAPING_ERRORS):
    """
    Creates a collection of entries from tripadvisor and booking.com which should have been collected
    but are missing in our database. The missing entries are written in a CSV file
    :param database: Initialized database object
    :return: None
    """
    print("Exporting the scraping errors to " + output_file)
    database.export_scraping_errors(output_file)

def pre_matching_diagnostics(database, output_file=OUTPUT_HOTELS_DISCREPANCIES):
    """
    Used for diagnostics, gives a list of discrepancies between the original city code and the one found online
    :param database: initialized database with hotels loaded
    :return: None
    """
    database.compare_data_sources(output_file)


def match_hotels_swisshotels(database, TEST_MATCHING = False):
    """
    Match all the entries in the hotel dataset which have address information with the swisshotel dataset. The
    algorithm can be chosen from the matching class, however the ALL_P_NAME_P_STREET has has the best results
    so far during testing.
    :param database: Initialized database which contains both hotel and swisshotel data
    :param TEST_MATCHING: Should only a subset be tested and validated?
    :return: None
    """
    if TEST_MATCHING:
        # Matching only a subset
        database.create_matching_by_fuzzy(INPUT_MATCHING_TEST_SAMPLE, algorithm=Matching.ALL_P_NAME_P_STREET)
        database.validate_matching()
        database.store_matched_hotels_to_csv(OUTPUT_MATCHING_FUZZY_TEST)
    else:
        # Full matching
        database.create_matching_by_fuzzy(algorithm=Matching.ALL_P_NAME_P_STREET)
        database.store_matched_hotels_to_csv(OUTPUT_MATCHING_FUZZY_FULL)

def match_tripadvisor_swisshotels(database, input_tripadvisor=OUTPUT_TRIPADVISOR_HOTELS, output_matching=OUTPUT_MATCHING_TRIPADVISOR_SWISSHOTELS):
    """
    Try to match the hotels from the tripadvisor crawl to the hotels from the swisshotel crawl
    :param database: database with initialized swisshotel data
    :param input_tripadvisor:
    :param input_swisshotels:
    :param output_matching:
    :return:
    """
    database.create_matching_tripadvisor_hotels(input_tripadvisor, output_matching, algorithm=Matching.ALL_P_DYNAMIC)

def match_tripadvisor_economic_data(databasae, input_tripadvisor=OUTPUT_TRIPADVISOR_HOTELS, output_matching=OUTPUT_MATCHING_TRIPADVISOR_ECONOMIC_DATA, economic_data_coordinates=INPUT_ECONOMIC_DATA_COORDINATES):
    """

    :param databasae:
    :param input_tripadvisor:
    :param output_matching:
    :return:
    """
    databasae.create_matching_tripadvisor_economic_data(input_tripadvisor, economic_data_coordinates, output_matching)


def create_database_for_uid_request(database, output_file=OUTPUT_MATCHING_UID):
    """
    Create a CSV file with data from the hotels and swisshotels database containing the names and addresses
    of all the hotels, no checks for duplicates is done.
    :param database: Initialized database with hotels and swisshotels data
    :param output_file: storage path for the CSV output
    :return:
    """
    database.create_csv_for_uid_request(output_file)


def create_features_swisshotels(database, output_file=OUTPUT_SWISSHOTELS_CLEANED, merge_features_file=INPUT_SWISSHOTELS_ATTRIBUTES_MERGE, attribute_name_file=OUTPUT_SWISSHOTELS_ATTRIBUTE_NAMES):
    """
    Will merge features which are the same but in different languages, only features in in english will be kept, unless
    there was no equivalent in it. From existing fields we also create new features such as the number of managers and
    the maximum size of banquet/meeting rooms.
    :param database: Initialized database with swisshotel data
    :param output_file: A path to the output file for the treated swisshotel data
    :param merge_features_file: A path to a file which contains the features to be merged
    :return: None
    """
    database.create_features_swisshotels(output_file, merge_features_file, attribute_name_file)


def collect_swisshotel_coordinates(database, output_file=OUTPUT_SWISSHOTELS_COORDINATES):
    """
    Will collect the coordinates for all 4000 swisshotel entries from their address and store the result of the
    coordinate retrieval in an external file
    :param database: Initialized database with swisshotel data
    :param output_file: CSV file where the retrieved coordinates will be stored
    :return: None
    """
    database.retrieve_swisshotel_coordinates(output_file)


def load_swisshotel_coordinates(database, swisshotels_coordinates=INPUT_SWISSHOTELS_COORDINATES):
    """
    Updates the swisshotel part of the database with coordinates from a CSV file
    :param database: Initialized database with loaded swisshotel data
    :param swisshotels_coordinates: A CSV file containing the numeric coordinates and id for all swisshotels
    :return: None
    """
    database.load_swisshotel_coordinates(swisshotels_coordinates)


def treat_tripadivsor_database(input_file=INPUT_TRIPADVISOR_DATA, output_file=OUTPUT_TRIPADVISOR_DATA):
    """
    Reads only the tripadvisor data from a CSV and cleans the data, the treated data will be stored in a different
    output file.
    :param input_file: The CSV from where the data has to be read
    :param output_file: The CSV where the data has to be stored after treatment
    :return: None
    """
    database = construct_database(input_file, INPUT_SWISSHOTELS)
    database.treat_ta_data()
    database.store_hotels_to_csv(output_file)


def clean_tripadvisor_reviews(database, input_file=INPUT_TRIPADVISOR_REVIEWS, missing=INPUT_TRIPADVISOR_MISSING_REVIEWS, output_file=OUTPUT_TRIPADVISOR_REVIEWS, yearly_file=OUTPUT_TRIPADVISOR_REVIEWS_YEARLY):
    """
    Reads the tripadivsor reviews file and cleans up the individual fields, only has to be done once. The raw file is
    quite big (around 20 Mb) compared to the others.
    The processed file is ready for storage in an SQL database. At the same time we generate a file which contains the
    ratings calculated on a yearly basis when possible for each hotel (if possible).
    :param database:
    :param input_file: The raw data collected from the crawl
    :param output_file: The cleaned up data which is ready to be used in R
    :param yearly_file: Contains the rating on a yearly basis for each hotel (use only past ratings for predictions)
    :return: None
    """
    database.read_and_clean_tripadvisor_reviews(input_file, missing)
    database.create_tripadivsor_yearly_ratings()
    database.store_tripadvisor_reviews(output_file, yearly_file)

def clean_tripadvisor_reviews_resti(database, input_file="C:/temp/TripAdvisor_history.csv", output_file="C:/temp/TripAdvisor_history_clean.csv", yearly_file="C:/temp/TripAdvisor_yearly.csv"):
    """
    Version of cleaning for the restaurants from Dominiks file
    :param database:
    :param input_file:
    :param output_file:
    :param yearly_file:
    :return:
    """
    database.read_and_clean_tripadvisor_reviews_resti(input_file)
    database.create_tripadivsor_yearly_ratings()
    database.store_tripadvisor_reviews(output_file, yearly_file)


def merge_hotel_swisshotel_economic_data(database, output_file=OUTPUT_HOTEL_SWISSHOTEL_REVIEWS_MERGE, economic_match=INPUT_MATCHING_HOTEL_ECONOMIC, input_tripadvisor=OUTPUT_TRIPADVISOR_HOTELS, min_available_values=MINIMUM_AVAILABLE_VALUES):
    """
    Take all available data from Google, Tripadvisor, Booking.com, Swisshotels and Single Tripadvisor Reviews and
    merge them into one single file. The data will be added to the original hotels data.
    :param database: Initialized database where hotels, swisshotels and tripadvisor reviews are loaded
    :param output_file: Path where the combined data will be stored (CSV)
    :param min_available_values: Drop every attribute which has less than 'n' values set, getting rid of
    :return: None
    """
    database.merge_all_data_from_hotels(INPUT_MATCHING_FULL_CORRECTED, economic_match, input_tripadvisor, min_available_values)
    database.store_merged_data(output_file)

def collect_economic_data_geolocation(database, output_geolocation=OUTPUT_ECONOMIC_DATA_COORDINATES):
    """

    :param database: Database which need not be initialized with other datasets
    :param economic_data:
    :param output_geolocation:
    :return:
    """
    database.collect_economic_geolocation_data(output_geolocation)

def clean_tripadvisor_hotels_and_coordinates(database, input_hotels=INPUT_TRIPADVISOR_ALL_HOTELS, input_hotels_webscraper=INPUT_TRIPADVISOR_HOTELS_WEBSCRAPER, input_coordinates=INPUT_TRIPADVISOR_HOTELS_COORDINATES, output_hotels=OUTPUT_TRIPADVISOR_HOTELS, output_coordinates=OUTPUT_TRIPADVISOR_HOTELS_COORDINATES_CLEAN):
    """

    :param database:
    :param input_hotels:
    :param input_hotels_webscraper:
    :param input_coordinates:
    :param output_hotels:
    :param output_coordinates:
    :return:
    """
    database.clean_tripadvisor_hotels_and_coordinates(input_hotels, input_coordinates, input_hotels_webscraper)
    database.store_tripadvisor_hotels_and_coordinates(output_hotels, output_coordinates)

def collect_tripadvisor_geolocation(database, tripadvisor_hotels=INPUT_TRIPADVISOR_ALL_HOTELS, output_geolocation=OUTPUT_TRIPADVISOR_HOTELS_COORDINATES):
    """

    :param database:
    :param tripadvisor_hotels:
    :param output_geolocation:
    :return:
    """
    database.collect_tripadvisor_geolocation(tripadvisor_hotels, output_geolocation)

def match_hotels_economic_data(database, economic_data_coordinates=INPUT_ECONOMIC_DATA_COORDINATES, matching_file=OUTPUT_MATCHING_HOTEL_ECONOMIC):
    """
    Create a matching between hotels and the economic data for regions
    :param database: Initialized database with hotel data and their coordinates
    :param economic_data: Path to a file containing the economic data
    :return: None
    """
    database.match_hotels_economic_data(economic_data_coordinates)
    database.store_hotel_econmic_data_matching(matching_file)

def create_predicitive_files(database, output_revenue_small=OUTPUT_PREDICTION_REVENUE_SMALL, input_revenue=INPUT_PREDICTION, input_tripadvisor=OUTPUT_TRIPADVISOR_HOTELS, economic_matching=OUTPUT_MATCHING_TRIPADVISOR_ECONOMIC_DATA,swisshotel_matching=OUTPUT_MATCHING_TRIPADVISOR_SWISSHOTELS, output_price=OUTPUT_TRIPADVISOR_HOTELS_PRICE_PREDICTION,  output_rooms=OUTPUT_TRIPADVISOR_HOTELS_ROOM_PREDICTION, output_revenue_all=OUTPUT_PREDICTION_REVENUE_ALL, output_classification=OUTPUT_PREDICTION_REVENUE_CLASSIFICATION, input_revenue_classification=OUTPUT_REVENUE_CLASSIFICATION, input_yearly_ratings=OUTPUT_TRIPADVISOR_REVIEWS_YEARLY):
    """

    :param database:
    :param output_file:
    :param input_file:
    :return:
    """
    database.create_prediction_revenue_small(output_revenue_small, input_revenue)
    database.create_prediction_revenue_all(output_revenue_all, input_revenue)
    #database.create_prediction_tripadvisor_price(output_price, input_tripadvisor)
    #database.create_prediction_tripadvisor_rooms(input_tripadvisor, economic_matching, swisshotel_matching, output_rooms)
    database.create_prediction_revenue_classification(output_classification, input_revenue_classification, input_yearly_ratings)

def clean_revenue_data(database, input_revenue=INPUT_REVENUE_DETAILS, output_revenue=OUTPUT_REVENUE_DETAILS, output_classification=OUTPUT_REVENUE_CLASSIFICATION, interpolation=None, beginning_year=2008, ending_year=2016):
    """
    Take the messy revenue data with multiple datapoint per year and create one with a revenue for each year
    :param database:
    :param input_revenue:
    :param output_revenue:
    :param interpolation:
    :return:
    """
    database.clean_revenue_data(input_revenue, output_revenue, output_classification, interpolation, beginning_year, ending_year)



def main():
    """
    Choose what we want to do during this run
    TODO Better structure of the options, very confusing for now with only comments
    :return:
    """
    # Read the database from csv
    database = construct_database()
    # Crawl the booking.com website
    #collect_booking_data(database)

    # Crawl the tripadvisor website
    #collect_tripadvisor_data(database)
    #collect_tripadvisor_all_hotel_data(database)
    #Create swisshotel list of hotels
    #collect_swisshotel_database(database)
    # Crawl the two sites
    #collect_tripadvisor_booking_data(database)

    # Output the necessary file for crawling google
    #prepare_google_data_collection(database)
    #prepare_google_data_collection_swisshotel(database)
    #prepare_tripadvisor_collection(database)
    #prepare_tripadvisor_hotel_collection(database)
    #clean_tripadvisor_reviews_resti(database)

    # Read and store the data from the google crawl
    #print("Please prepare the file from google now and continue when it is in place")
    #raw_input("Press Enter to continue...")
    collect_google_data(database, INPUT_HOTELS_GOOGLE_CRAWL, INPUT_SWISSHOTELS_GOOGLE_CRAWL)

    # Some cleaning functions
    #clean_tripadvisor_reviews(database)
    #clean_tripadvisor_hotels_and_coordinates(database)
    #clean_revenue_data(database)  # SLOW

    # Collect geolocation data
    #collect_geolocation_data(database)
    #collect_swisshotel_coordinates(database)
    #collect_economic_data_geolocation(database)
    #collect_tripadvisor_geolocation(database)
    #load_swisshotel_coordinates(database)

    #create_location_map(database)
    #create_statistics(database)

    # Check for errors in scraping data
    #find_scraping_errors(database)
    #database.export_only_website_entries()
    #pre_matching_diagnostics(database)

    # Match the data
    #match_hotels_swisshotels(database)
    #match_hotels_economic_data(database)
    #match_tripadvisor_swisshotels(database)
    #match_tripadvisor_economic_data(database)

    # Merge Hotels and Swisshotel data
    merge_hotel_swisshotel_economic_data(database)

    # Create features and clean up data from swisshotels
    #create_features_swisshotels(database)

    # Store all the data back to a csv file
    #database.store_matched_hotels_to_csv(MATCHED_OUTPUT_CSV)
    #database.store_hotels_to_csv(OUTPUT_HOTELS)
    #database.store_tripadvisor_hotels(OUTPUT_TRIPADVISOR_HOTELS_CRAWL)

    # Prediction
    create_predicitive_files(database)


if __name__ == "__main__":
    main()
    #prepare_google_trends_collection()
    #treat_ta_database()











