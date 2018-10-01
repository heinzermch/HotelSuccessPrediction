# This Python file uses the following encoding: utf-8
import re
import pandas as pd
import numpy as np
import geocoder
import random
import Levenshtein
import os
import time
from dateutil.relativedelta import *
from datetime import datetime, date
from requests.exceptions import ReadTimeout
from difflib import SequenceMatcher as SM
from pandas import DataFrame
from time import mktime
from numpy import sqrt


class Matching:
    ALL, ALL_T_NAME, ALL_T_NAME_T_STREET, ALL_P_NAME, ALL_P_NAME_P_STREET, ALL_P_DYNAMIC = range(6)


class Database():
    """
        This version of Database uses pandas internally, which should make scaling up easier. Also the code is much more
        brief as many functions regarding csv files are already precoded. The communication outside of this class is still
        done via dictionaries or lists
    """
    ID = 'tempid'
    SWISS_ID = 'swissid'
    BOOKING_LINK = 'booking'
    BOOKING_NAME = 'bk_name'
    TRIPADVISOR_LINK = 'tripadvisor'
    TRIPADVISOR_NAME = 'ta_name'
    SWISSHOTEL_LINK = 'swisshotel'
    SWISSHOTEL_NAME = 'sh_name'
    geolocation_cache = {}
    # The pandas databases will be stored here
    hotels = None
    swisshotels = None
    reviews = None
    yearly_ratings = None
    matching = None
    hotel_economic_matching = None
    subset_file = None
    merged = None
    tripadvisor_hotels = None
    economic_data = None


    def store_scraping_results(self, results, hotels_database, tripadvisor_hotels = False):
        """
        The results of the scraping will be stored in a dictionary and need to be filled into the database
        If the attribute does not yet exist, it will be filled with NaN for other entries.
        :param results: A dictionary which contains a dictionary, the keys are the ids from the database hotels
        :param hotels_database: If the attribute and its key belong to the hotels or swisshotels database
        :return: None
        """
        if tripadvisor_hotels:
            for key in results.keys():
                attributes = results[key]
                # TODO not very elegant, might be easier to create a Series object and store all at once
                for attribute_name in attributes:
                    self.tripadvisor_hotels.loc[self.tripadvisor_hotels['link-href'] == key, attribute_name] = attributes[attribute_name]
        else:
            for key in results.keys():
                attributes = results[key]
                for attribute_name in attributes:
                    if hotels_database:
                        self.hotels.loc[self.hotels[self.ID] == key, attribute_name] = attributes[attribute_name]
                    else:
                        self.swisshotels.loc[self.swisshotels[self.SWISS_ID] == key, attribute_name] = attributes[attribute_name]


    def clean_url(self, url):
        """
        Shortens the booking.com urls, deletes everything after the "html" part.
        :param url: a String which contains an URL
        :return: String
        """
        return url.split("html", 1)[0] + "html"


    def clean_reviews(self, text):
        """
        Removes all characters which are not numbers from a string
        Example: "29 Reviews" becomes "29"
        :param text: String
        :return: a String which contains only numbers
        """

        return re.sub("[^0-9]", "", text)

    def clean_reviews_test(self, text):
        """
        Removes all characters which are not numbers from a string
        Example: "29 Reviews" becomes "29"
        :param text: String
        :return: a String which contains only numbers
        """
        return re.sub("[^0-9]", "", text)


    def retrieve_hotels_from_csv(self, filename):
        """
        Read the hotels from the CSV file and store them in an internal data structure
        :param filename: Path to the csv file
        :return: None - results are stored internally
        """
        print('Loading CSV file: ' + filename)
        hotels = pd.read_csv(filename)
        # Clean up the urls by removing everything after 'html' if the entry is not NaN, then do nothing
        hotels[self.BOOKING_LINK] = hotels[self.BOOKING_LINK].apply(lambda x: self.clean_url(x) if pd.notnull(x) else x)
        # Make all the keys lowercase
        hotels = hotels.rename(columns=dict(zip(hotels.keys(), [key.lower() for key in hotels.keys()])))
        # Transform the key to int
        hotels[self.ID] = hotels[self.ID].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
        self.hotels = hotels


    def retrieve_hotels_from_csvs(self, filenames):
        """
         We suppose the arguments are a list of filenames. The first will be the main database and the rest will
        be merged with it. If a column from the main file already exists in the new file, it will be dropped.
        :param filenames: A list of paths to csv files
        :return: None - The results are stored in the object
        """
        # Load the first file normally
        self.retrieve_hotels_from_csv(filenames[0])
        # Merge the subsequent files
        for i in range(1,len(filenames)):
            filename = filenames[i]
            print("Loading additional CSV file: " + filename)
            right = pd.read_csv(filename, encoding='utf-8')
            # Make all columns names lower letters
            right = right.rename(columns=dict(zip(right.keys(), [key.lower() for key in right.keys()])))
            # Transform the key to int and set it as index
            right[self.ID] = right[self.ID].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
            # Drop duplicate columns but for the id
            hotel_keys = list(self.hotels.keys()[1:])
            keys = [key for key in list(right.keys()) if key in hotel_keys]
            right = right.drop(keys, 1)
            # Merge on the id
            self.hotels = self.hotels.merge(right, on=self.ID, how='left')


    def retrieve_swisshotels_from_csv(self, filename):
        """
        Retrieve the swisshotels database from a csv, drop some minor unnecessary columns
        :param filename: path to the csv file
        :return: None
        """
        swisshotels = pd.read_csv(filename, encoding='utf-8')
        # In this case its the raw url file
        if not 'swissid' in swisshotels.keys():
            # Create index
            swisshotels['swissid'] = swisshotels.index
            swisshotels = swisshotels.drop('links', 1)
            swisshotels = swisshotels.rename(columns={'links-href' : 'swisshotel'})
            swisshotels = swisshotels[['swissid', 'swisshotel']]
        self.swisshotels = swisshotels

    def retrieve_economic_data_from_csv(self, input_economic_data):
        """

        :param input_economic_data:
        :return:
        """
        self.economic_data = pd.read_csv(input_economic_data, encoding='utf-8-sig')

    def extract_date(self, text, reference_date):
        """
        Take a text date and transform it into a date object
        :param text: Raw text of the form 'Bewertet am 7. Oktober 2016' or similarly in English
        :return: a date object with corresponding value
        """
        text = text.encode('utf-8')
        if 'Bewertet' in text:
            #German
            # Unfortunately we have to 'translate' the months manually as strptime seems unable to handle the 'ä' in
            # 'März' if we want to use German words
            if '.' in text:
                # Standard German date
                text = text.replace('Januar', 'January').replace('Februar', 'February').replace('März', 'March').replace('Mai', 'May').replace('Juni', 'June').replace('Juli', 'July').replace('Oktober', 'October').replace('Dezember', 'December')
                date = text.replace('Bewertet am ', '')
                date = date.split('.')
                # Do zero padding if necessary, else it just removes the '.'
                if len(date[0]) < 2:
                    date = '0' + date[0] + date[1]
                else:
                    date = date[0] + date[1]
                datetime_object = datetime.strptime(date, '%d %B %Y')
                return datetime_object.date()
            else:
                # Have to check which special case it is (yesterday, days, weeks)
                if 'gestern' in text:
                    offset = relativedelta(days=1)
                elif 'heute' in text:
                    return reference_date.date()
                else:
                    nb = self.clean_reviews(text)
                    if 'Tagen' in text:
                        offset = relativedelta(days=int(nb))
                    elif 'Woche' in text:
                        if len(nb) == 0:
                            offset = relativedelta(weeks=1)
                        else:
                            offset = relativedelta(weeks=int(nb))
                return (reference_date - offset).date()
        elif 'Reviewed' in text:
            # English
            if ',' in text:
                # Normal case
                text = text.replace('Reviewed ', '')
                text = text.split(',')
                day = self.clean_reviews(text[0])
                month = text[0].split(' ')[0]
                year = self.clean_reviews(text[1])
                if int(day) < 10:
                    day = '0' + day
                datetime_object = datetime.strptime(day + " " + month + " " + year, '%d %B %Y')
                return datetime_object.date()
            else:
                # Non standard date
                # Have to check which special case it is (yesterday, days, weeks)
                nb = self.clean_reviews(text)
                if 'yesterday' in text:
                    offset = relativedelta(days=1)
                    return (reference_date - offset).date()
                elif 'week' in text:
                    if len(nb) == 0:
                        offset = relativedelta(weeks=1)
                    else:
                        offset = relativedelta(weeks=int(nb))
                    return (reference_date - offset).date()
                elif 'day' in text:
                    offset = relativedelta(days=int(nb))
                    return (reference_date - offset).date()
                elif 'today' in text:
                    return reference_date.date()
                else:
                    # Degenerated date without a point
                    text = text.replace('Reviewed ', '')
                    if int(nb) < 10:
                        text = '0' + text
                    datetime_object = datetime.strptime(text, '%d %B %Y')
                    return datetime_object.date()

    def extract_local_ranking(self, text):
        if 'null' in text:
            return (np.NaN, np.NaN)
        if ' of ' in text:
            #English
            text = text.split('of')
        elif ' von ' in text:
            # German
            text = text.split('von')
        else:
            raise ValueError('Unknown language in local ranking field')
        # Make sure to return integers
        return (int(self.clean_reviews(text[0])), int(self.clean_reviews(text[1])))

    def extract_local_percentile(self, text):
        a, b = self.extract_local_ranking(text)
        if pd.notnull(a):
            return float(a)/b
        return np.NaN


    def read_and_clean_tripadvisor_reviews(self, filename, missing):
        """
        Take the raw crawl data and process it into objects which are easier to use for other methods. Some attributes
        are extracted from text.
        :param filename: path to the crawl data in a CSV
        :return: None, data is stored internally
        """
        reviews = pd.read_csv(filename, encoding='utf-8')
        missing = pd.read_csv(missing, encoding='utf-8')
        to_remove = missing['TripAdvisorLink'].unique()
        # Remove entries which would be duplicated
        for tempid in to_remove:
            reviews = reviews[reviews['TripAdvisorLink'] != tempid]
        # Append the missing reviews
        reviews = reviews.append(missing)
        # Calculate the reference date
        float_time = os.path.getmtime(filename)
        struct = time.localtime(float_time)
        reference_date = datetime.fromtimestamp(mktime(struct))
        reviews.loc[:, "overallRating"] = reviews['overallRating'].apply(lambda x: np.NaN if 'null' in str(x) or pd.isnull(x) else x)
        reviews.loc[:, "overallRating"] = reviews['overallRating'].apply(lambda x: np.NaN if 'null' in str(x) or pd.isnull(x) else x)
        reviews.loc[:, "localRanking1"] = reviews['localRanking1'].apply(lambda x: np.NaN if 'null' in str(x) or pd.isnull(x) else x)
        # Predefined concentions (externalise?)
        remove = ['web-scraper-order', 'web-scraper-start-url', 'localRanking2', 'TripAdvisorLink-href', 'Name']
        rename = {'TripAdvisorLink': 'tempid', 'NrReviews': 'ta_reviews_reviewcount',
                  'overallRating' : 'ta_reviews_ratingvalue', 'localRanking1' : 'ta_local_ranking',
                  'datumH': 'ta_review_date', 'ratingH' : 'ta_review_score', 'titleH' : 'ta_review_title',
                  'textH' : 'ta_review_text', 'rooms': 'ta_rooms'}
        # Drop unnecessary columns
        reviews = reviews.drop(remove, 1)
        # Rename useful columns according to naming convention
        reviews = reviews.rename(columns=rename)
        # Treat columns
        reviews.loc[:, "ta_review_score"] = reviews['ta_review_score'].apply(
            lambda x: float(self.clean_reviews(x)) / 10 if pd.notnull(x) else x)
        reviews['ta_local_ranking_value'], reviews['ta_local_ranking_max'] = zip(*reviews['ta_local_ranking'].apply(
            lambda x: self.extract_local_ranking(x) if pd.notnull(x) else (x, x)))
        reviews.loc[:, "ta_review_date"] = reviews['ta_review_date'].apply(
            lambda x: self.extract_date(x, reference_date) if pd.notnull(x) else x)
        reviews.loc[:, "ta_reviews_reviewcount"] = reviews['ta_reviews_reviewcount'].apply(
            lambda x: self.clean_reviews(x) if pd.notnull(x) else x)
        reviews.loc[:, 'ta_reviews_ratingvalue'] = reviews['ta_reviews_ratingvalue'].apply(
            lambda x: str(x).replace(',','.') if pd.notnull(x) else x)
        reviews.loc[:, "ta_rooms"] = reviews['ta_rooms'].apply(
            lambda x: self.clean_reviews(x) if pd.notnull(x) else x)
        self.reviews = reviews

    def read_and_clean_tripadvisor_reviews_resti(self, filename):
        """
        Take the raw crawl data and process it into objects which are easier to use for other methods. Some attributes
        are extracted from text.
        :param filename: path to the crawl data in a CSV
        :return: None, data is stored internally
        """
        reviews = pd.read_csv(filename, encoding='utf-8')
        float_time = os.path.getmtime(filename)
        reference_date = datetime.strptime("31-12-2017", "%d-%m-%Y")
        reviews.loc[:, "overallRating"] = reviews['overallRating'].apply(
            lambda x: np.NaN if 'null' in str(x) or pd.isnull(x) else x)
        reviews.loc[:, "overallRating"] = reviews['overallRating'].apply(
            lambda x: np.NaN if 'null' in str(x) or pd.isnull(x) else x)
        reviews.loc[:, "localRanking1"] = reviews['localRanking1'].apply(
            lambda x: np.NaN if 'null' in str(x) or pd.isnull(x) else x)
        reviews.loc[:, "localRanking2"] = reviews['localRanking2'].apply(
            lambda x: np.NaN if 'null' in str(x) or pd.isnull(x) else x)
        reviews.loc[:, "NrReviews"] = reviews['NrReviews'].apply(
            lambda x: np.NaN if 'null' in str(x) or pd.isnull(x) else x)
        # Predefined concentions (externalise?)
        remove = ['web.scraper.start.url', 'TripAdvisorLink.href', 'TripAdvisorLink', 'X.U.FEFF.web.scraper.order']
        rename = {'ID': 'tempid', 'NrReviews': 'ta_reviews_reviewcount',
                  'overallRating': 'ta_reviews_ratingvalue', 'localRanking1': 'ta_local_ranking',
                  'datumH': 'ta_review_date', 'ratingH': 'ta_review_score', 'titleH': 'ta_review_title',
                  'textH': 'ta_review_text'}
        # Drop unnecessary columns
        reviews = reviews.drop(remove, 1)
        # Rename useful columns according to naming convention
        reviews = reviews.rename(columns=rename)
        # Treat columns
        reviews.loc[:, "ta_review_score"] = reviews['ta_review_score'].apply(
            lambda x: float(self.clean_reviews(x)) / 10 if pd.notnull(x) else x)
        reviews['ta_local_ranking_value'], reviews['ta_local_ranking_max'] = zip(*reviews['ta_local_ranking'].apply(
            lambda x: self.extract_local_ranking(x) if pd.notnull(x) else (x, x)))
        reviews.loc[:, "ta_review_date"] = reviews['ta_review_date'].apply(
            lambda x: self.extract_date(x, reference_date) if pd.notnull(x) else x)
        #reviews.loc[:, "ta_reviews_reviewcount"] = reviews['ta_reviews_reviewcount'].apply(
        #    lambda x: self.clean_reviews_test(x) if pd.notnull(x) else x)
        reviews.loc[:, 'ta_reviews_ratingvalue'] = reviews['ta_reviews_ratingvalue'].apply(
            lambda x: str(x).replace(',', '.') if pd.notnull(x) else x)
        self.reviews = reviews

    def create_tripadvisor_yearly_ratingvalue_entries(self, row, years, reviews):
        """
        Retrieve all reviews from a specific ID among all the reviews. Then generate a rating
        from the reviews which are older than a specific date. This is done by taking the mean. Values which are the
        same among all the reviews such as the total rating and the local ranking are also extracted and stored.
        It is expected that the column 'ta_review_date' consists of date objects
        :param row: A data row which contains the 'tempid' attribute
        :param years: A list of dates for which the ratings have to be generated
        :param reviews: A dataframe which contains all collected reviews
        :return: A panda Series object containing the yearly ratings and other attributes
        """
        d = {}
        d['tempid'] = row['tempid']
        # Extract all reviews which belong to the specified hotel
        reviews = reviews.loc[reviews['tempid'] == row['tempid'], :]
        # Transform text to date objects for comparison
        #reviews.loc[:, 'ta_review_date'] = reviews['ta_review_date'].apply(lambda text: datetime.strptime(text, '%m/%d/%Y').date())
        # Go through all dates and create a rating up to that specific point in time
        for year in years:
            current_reviews = reviews.loc[reviews['ta_review_date'] < year, :]
            mean = current_reviews['ta_review_score'].mean()
            variance = current_reviews['ta_review_score'].var()
            # Check if we have available reviews
            if pd.notnull(mean):
                d["ta_ratingvalue_at_" + str(year).replace('-','_').replace('/','_')] = np.round(mean,2)
                d["ta_variance_at_" + str(year).replace('-', '_').replace('/', '_')] = np.round(variance, 2)
                d['ta_reviewcount_at_' + str(year).replace('-', '_').replace('/', '_')] = current_reviews[
                    'tempid'].notnull().count()
            else:
                d["ta_ratingvalue_at_" + str(year).replace('-', '_').replace('/', '_')] = np.NaN
                d['ta_reviewcount_at_' + str(year).replace('-', '_').replace('/', '_')] = np.NaN
                d["ta_variance_at_" + str(year).replace('-', '_').replace('/', '_')] = np.NaN
                # Extract values which are unique to each hotel
        d['ta_local_ranking_value'] = reviews['ta_local_ranking_value'].head(1).values[0]
        d['ta_local_ranking_max'] = reviews['ta_local_ranking_max'].head(1).values[0]
        # Only do this if data is available
        if pd.notnull(d['ta_local_ranking_value']):
            d['ta_local_ranking_percentile'] = np.round((d['ta_local_ranking_value']+0.0) / d['ta_local_ranking_max'],2)
        else:
            d['ta_local_ranking_percentile'] = np.NaN
        d['ta_reviews_reviewcount'] = reviews['ta_reviews_reviewcount'].head(1).values[0]
        d['ta_reviews_ratingvalue'] = reviews['ta_reviews_ratingvalue'].head(1).values[0]
        d['ta_rooms'] = reviews['ta_rooms'].head(1).values[0]
        return pd.Series(d)

    def create_tripadivsor_yearly_ratings(self):
        """
        Create ratings scores for specific points in time for each hotel (for now at the beginning of each year)
        :return: None, data is stored internally
        """
        print("Creating yearly ratings ...")
        reviews = self.reviews
        unique_ids = reviews['tempid'].unique()
        unique_ids = np.sort(unique_ids)
        yearly_data = DataFrame({'tempid': unique_ids})
        # Create dates, there are not many ratings from before 2010 (only 1000 out of 30000)
        years = range(2011, 2019)
        years = [date(year, 1, 1) for year in years]
        # Calculate the ratings for each year
        yearly_ratings = yearly_data.apply(lambda row: self.create_tripadvisor_yearly_ratingvalue_entries(row, years, reviews), axis=1)
        # Reorder the columns to have 'tempid' as first element (more visually pleasing)
        cols = yearly_ratings.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        self.yearly_ratings = yearly_ratings[cols]


    def store_tripadvisor_reviews(self, filename, yearly_filename):
        """
        Store the cleaned reviews as UTF-8 encoded CSV file for further processing
        :param filename: path to the storage file
        :return: None
        """
        self.reviews.to_csv(filename, index=False, encoding='utf-8-sig')
        self.yearly_ratings.to_csv(yearly_filename, index=False, encoding='utf-8-sig')
        print("Stored the cleaned tripadvisor reviews under " + filename)
        print("Stored the yearly ratings under: " + yearly_filename)

    def store_tripadvisor_hotels(self, filename):
        """

        :param filename:
        :return:
        """
        if self.tripadvisor_hotels is None:
            print("No tripadvisor hotels were available to store")
        else:
            self.tripadvisor_hotels.to_csv(filename, index=False, encoding='utf-8-sig')
            print("Stored the tripadvisor hotels under: " + filename)

    def merge_all_data_from_hotels(self, matching_hotels_swisshotels_file, matching_economic_hotel_file, input_tripadvisor, min_available_values):
        """
        Merge all available data starting with the hotel database, adding the swissids from the matching which was
        conducted automatically but corrected manually afterwards and finally add all swisshotel attributes for each
        available entry.
        :param matching_file: Path to a CSV file which contains the corrected matches between hotels and swisshotels
        :return: None
        """
        # Load dataframes from files
        matching_hotels_swisshotels = pd.read_csv(matching_hotels_swisshotels_file, encoding='utf-8-sig')
        matching_economic_hotel = pd.read_csv(matching_economic_hotel_file, encoding='utf-8-sig')
        economic_data = self.economic_data
        tripadvisor = pd.read_csv(input_tripadvisor, encoding='utf-8-sig')
        # Prepare for merging by keeping relevant columns and renaming ids
        tripadvisor = tripadvisor[['link-href', 'ta_stars',  'ta_fives',  'ta_fours',  'ta_threes',  'ta_twos',  'ta_ones',  'ta_ratingvalue_exact',  'ta_reviewcount_exact', 'ta_type']]
        tripadvisor = tripadvisor.rename(columns={'link-href': 'tripadvisor'})
        matching_hotels_swisshotels = matching_hotels_swisshotels.drop('Failures', axis=1)
        # Change type from object to integer, otherwise merging will not work
        self.hotels.loc[:,'tempid'] = self.hotels['tempid'].apply(lambda x: int(x))
        # Add the swissid and edid to the hotel database
        merged = self.hotels.merge(matching_hotels_swisshotels, how='left', on='tempid')
        merged = merged.merge(matching_economic_hotel, how='left', on='tempid')
        # Add the swisshotel data to matched hotels
        merged = merged.merge(self.swisshotels, how='left', on='swissid')
        # Add the economic data to hotels
        merged = merged.merge(economic_data, how='left', on='edid')
        # Add the additional tripadvisor data by merging via tripadvisor url
        merged = merged.merge(tripadvisor, how='left', on='tripadvisor')
        # Remove duplicates 496,503,515,525,540,568,573,615,643
        merged.is_copy = False
        merged = merged.query('tempid not in [496,503,515,525,540,568,573,615,643]')
        # TODO merge some attributes such as coordinates and google ratings
        # Remove columns where none or too few of the attributes are available
        # TODO use dropna
        counted = merged.isnull().sum()
        max = merged['tempid'].notnull().sum()
        to_drop = counted.apply(lambda x: x >= max-min_available_values)
        to_drop = zip(to_drop.keys(), to_drop.tolist())
        to_remove = []
        for name, do_drop in to_drop:
            if do_drop:
                to_remove.append(name)
        #  merge conflicting data such as number of rooms and stars
        merged.loc[:, 'rooms'] = merged['sh_rooms']
        before = str(merged['rooms'].notnull().sum())
        merged.loc[merged['rooms'].isnull(), 'rooms'] = merged['ta_rooms']
        after = str(merged['rooms'].notnull().sum())
        print("After swisshotel we had room size data for  " + before + " rooms, now we have " + after)
        merged.loc[:, 'stars'] = merged['sh_nb_stars']
        before = str(merged['stars'].notnull().sum())
        merged.loc[merged['stars'].isnull(), 'stars'] = merged['ta_stars']
        after = str(merged['stars'].notnull().sum())
        print("After swisshotel we had stars for  " + before + " rooms, now we have " + after)
        self.merged = merged.drop(to_remove, axis=1)

    def store_merged_data(self, filename):
        """
        Save the merged data to the disk
        :param output_file: Path where the merged database will be stored (CSV)
        :return:
        """
        self.merged.to_csv(filename, index=False, encoding='utf-8-sig')
        print("Stored the merged dataset of hotels and swisshotels under: " + filename)

    def merge_fields(self, list, new_name):
        """
        Merges the fields according to the order in the list, only NaN values will be replaced.
        Input is a list of strings, ordered according to priority
        First row will always be the id to ensure we take the full list
        :param list: attributes to be merged, ordered by priority
        :param new_name: the name of the field where the merged values will be stored
        :return: pandas Dataframe object with the new column
        """
        merged_field = self.hotels[[self.ID, list[0]]]
        merged_field = merged_field.rename(columns={list[0] : new_name})
        mask = merged_field[new_name].isnull()
        for i in range(1, len(list)):
            merged_field.loc[mask, new_name] = self.hotels[list[i]]
            mask = merged_field[new_name].isnull()
        return merged_field

    def get_swisshotels_names(self):
        """
        Retrieve a list of all hotels in the swisshotels database in order to process them to queries for Google
        :return: tuple consisting of (swissid, name + city)
        """
        google_data = self.swisshotels.loc[:, ['swissid', 'sh_name', 'sh_city']]
        google_data.loc[:, 'query'] = self.swisshotels.apply(lambda row: row['sh_name'] + ' ' + row['sh_city'], axis = 1)
        google_data = google_data.drop('sh_name', 1)
        google_data = google_data.drop('sh_city', 1)
        return [tuple(x) for x in google_data.values]


    def get_tripadvisor_booking_names(self, test_mode, test_limit, test_randomize):
        """
        Returns tuples of all names which have been collected either from booking.com or tripadvisor with their
        internal id
        :param test_mode: limit the number of entries returned to the test limit?
        :param test_limit: Number of entries which will be returned if in test mode
        :param test_randomize: Randomize the order of the entries?
        :return: tuples consisting of (tempid, hotel name and city)
        """
        names = []
        tripadvisor_names = None
        booking_names = None
        # Find all the available names from tripadvisor, those come with the city name automatically
        if self.TRIPADVISOR_NAME in self.hotels.keys():
            tripadvisor_names = self.hotels.loc[pd.notnull(self.hotels[self.TRIPADVISOR_NAME]), [self.ID, self.TRIPADVISOR_NAME, 'city']]
            tripadvisor_names[self.TRIPADVISOR_NAME] = tripadvisor_names[self.TRIPADVISOR_NAME] + " " + tripadvisor_names['city']
            tripadvisor_names = tripadvisor_names.drop('city', 1)
        # Check if the booking names are available, add city names to them to increase likelihood of finding them on google
        if self.BOOKING_NAME in self.hotels.keys():
            booking_names = self.hotels.loc[pd.notnull(self.hotels[self.BOOKING_NAME]), [self.ID, self.BOOKING_NAME, 'city']]
            booking_names[self.BOOKING_NAME] = booking_names[self.BOOKING_NAME] + " " + booking_names['city']
            booking_names = booking_names.drop('city', 1)
        # If we have both, we need to merge the dataframes to take advantage of additional data
        if booking_names is not None and tripadvisor_names is not None:
            # Outer join fills the non-existent entries on either side with NaN
            both = booking_names.merge(tripadvisor_names, on=self.ID, how='outer')
            # Replace NaN values in tripadvisor names with the ones from booking
            both.loc[pd.isnull(both[self.TRIPADVISOR_NAME]), self.TRIPADVISOR_NAME] = both[self.BOOKING_NAME]
            # Get rid of the additional colum
            both = both.drop(self.BOOKING_NAME, 1)
            names = [tuple(x) for x in both.values]
        elif booking_names is not None:
            names = [tuple(x) for x in booking_names.values]
        elif tripadvisor_names is not None:
            names = [tuple(x) for x in tripadvisor_names.values]
        return self.test_cropper(names, test_mode, test_limit, test_randomize)


    def extract_code(self, google_address):
        """
        The code is the most easily extracted data point as it always consists of a number between 1000 and 9999
        Street numbers in Switzerland are very unlikely to be that high
        Most of the addresses are of the form "street, code city". However some are of the
        form "Hotel name, street, code city"
        :param google_address: address field collected from a google crawl
        :return: The city code/NPA or "null" value for pandas if not found
        """
        splits = google_address.split(",")
        if len(splits) == 2:
            return re.sub("[^0-9]", "", splits[1])
        splits = [re.sub("[^0-9]", "", split) for split in splits]
        for split in splits:
            if len(split) == 4:
                return split
        return np.NaN


    def extract_street(self, google_address):
        """
        Street is usually in the field before the city, hence we look again for the city code and use the field before
        it
        :param google_address: address field collected from a google crawl
        :return: Street address or "null" value for pandas if not found
        """
        splits = google_address.split(",")
        if len(splits) == 2:
            return google_address.split(",")[0]
        splits = [re.sub("[^0-9]", "", split) for split in splits]
        for i in range(len(splits)):
            if len(splits[i]) == 4:
                return google_address.split(",")[i-1]
        return np.NaN

    """
        The city is in the same field as the city code, so the simples way is to look for the code and
        then extract the letters from the field.
    """
    def extract_city(self, google_address):
        splits = google_address.split(",")
        if len(splits) == 2:
            return re.sub("[,.!?0-9]", "", google_address.split(",")[1]).replace(" ","")
        splits = [re.sub("[^0-9]", "", split) for split in splits]
        for i in range(len(splits)):
            if len(splits[i]) == 4:
                return re.sub("[,.!?0-9]", "", google_address.split(",")[i]).replace(" ","")
        return np.NaN

    """
        Address to latitude and longitude, caching for multiple requests
    """
    def get_coordinates(self, address):
        address = str(address)
        if self.geolocation_cache.has_key(address):
            return self.geolocation_cache[address]
        print("Collecting geolocation for: " + address)
        try:
            geolocation = geocoder.bing(address, key="AoiuIaSUNtkn5bpSwZNlH8kHWRtGrhM7M0VdemJ4icBvfqibkzPV-IolVtnzgLiv").latlng
            self.geolocation_cache[address] = geolocation
            return geolocation
        except ReadTimeout:
            print('Read Timeout for ' + address)
            return ['','']

    def collect_economic_geolocation_data(self, output_file):
        """

        :param economic_data_file:
        :param output_file:
        :return:
        """
        economic_data = self.economic_data
        economic_data['address'] = economic_data['ed_city'].apply(lambda city: city + ", Switzerland")
        economic_data['coord'] = economic_data['address'].apply(lambda elem: self.get_coordinates(elem))
        # Split them up in a x and y part
        economic_data['x'] = economic_data.coord.apply(lambda x: x[0])
        economic_data['y'] = economic_data.coord.apply(lambda x: x[1])
        economic_data[['edid', 'x', 'y']].to_csv(output_file, index=False, encoding='utf-8-sig')

    def collect_tripadvisor_geolocation(self, input_file, output_file):
        """
        Retrieve geolocation information from Bing via the address of the hotels
        :param input_file:
        :param output_file:
        :return:
        """
        tripadvisor = pd.read_csv(input_file, encoding='utf-8')
        tripadvisor.loc[:, 'ta_postalcode'] = tripadvisor['ta_postalcode'].apply(lambda x: str(int(x)) if pd.notnull(x) else '')
        tripadvisor.loc[:, 'ta_streetaddress'] = tripadvisor['ta_streetaddress'].apply(
            lambda x: x if pd.notnull(x) else '')
        tripadvisor.loc[:, 'ta_city'] = tripadvisor['ta_city'].apply(
            lambda x: x if pd.notnull(x) else '')
        tripadvisor['address'] = tripadvisor['ta_streetaddress'] + ", " + tripadvisor['ta_postalcode'] + " " + tripadvisor['ta_city'] + ", Switzerland"
        tripadvisor['coord'] = tripadvisor['address'].apply(lambda elem: self.get_coordinates(elem))
        # Split them up in a x and y part
        #tripadvisor['x'] = tripadvisor.coord.apply(lambda x: x[0])
        #tripadvisor['y'] = tripadvisor.coord.apply(lambda x: x[1])
        tripadvisor[['link-href', 'coord']].to_csv(output_file, index=False, encoding='utf-8-sig')


    def collect_hotel_geolocation_data(self):
        """
        Collects the location data from all entries where an address is available, with preference
        from google, then tripadvisor, then the original datasource
        :return: None, data is stored in database
        """
        # Update the category of the columns to object to enable string concatenation
        self.hotels['go_postalcode'] = self.hotels['go_postalcode'].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
        self.hotels['ta_postalcode'] = self.hotels['ta_postalcode'].apply(
            lambda x: str(int(x)) if pd.notnull(x) else x)
        self.hotels['plz'] = self.hotels['plz'].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
        # First priority are addresses from google
        self.hotels['address'] = self.hotels['go_street'] + ", " + self.hotels['go_postalcode'] + " " + self.hotels["go_city"] + ", Switzerland"
        # Update from tripadvisor where google has no address
        self.hotels.loc[self.hotels['address'].isnull(), 'address'] = self.hotels['ta_streetaddress'] + ", "\
                                                                      + self.hotels['ta_postalcode'] + " " \
                                                                      + self.hotels['ta_city'] + ", Switzerland"
        # Update the address with the given data (not very precise sometimes)
        self.hotels.loc[self.hotels['address'].isnull(), 'address'] = self.hotels.street + ", " + self.hotels.plz \
                                                                      + " " + self.hotels.city + ", Switzerland"
        # Retrieve coordinates from bing
        self.hotels['coord'] = self.hotels['address'].apply(lambda elem: self.get_coordinates(elem))
        # Split them up in a x and y part
        self.hotels['x'] = self.hotels.coord.apply(lambda x: x[0])
        self.hotels['y'] = self.hotels.coord.apply(lambda x: x[1])


    def store_hotel_geolocation_data(self, filename):
        """
        Stores the geolocation data for hotel database
        :param output_file:
        :return: None
        """
        hotel_geolocation = self.hotels[['tempid', 'x', 'y']]
        hotel_geolocation.to_csv(filename, index=False, encoding='utf-8-sig')
        print('Wrote Geolocation data to: ' + filename)

    def get_geolocation_data(self):
        geolocation_data = self.hotels.loc[self.hotels['coord'].notnull(), [self.ID, 'x' , 'y']]
        return [tuple(x) for x in geolocation_data.values]

    """
        Read the scraped data from the csv, drop unnecessary data, treat special cases and merge with the existing database
    """
    def collect_google_data_from_csv(self, hotel_path, swisshotel_path):
        google_hotel = pd.read_csv(hotel_path)
        google_swisshotel = pd.read_csv(swisshotel_path)
        # Rename the links column to TempID and transform to integer
        google_hotel = google_hotel.rename(columns={'links': self.ID, 'google_score': 'go_ratingvalue', 'google_name' : 'go_name'})
        google_hotel[self.ID] = google_hotel[self.ID].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
        google_swisshotel = google_swisshotel.rename(
            columns={'query': 'swissid', 'go_rating': 'sh_google_ratingvalue', 'go_reviews': 'sh_google_reviewcount'})
        google_swisshotel.loc[:, 'swissid'] = google_swisshotel['swissid'].apply(lambda x: int(x) if pd.notnull(x) else x)
        # Replace all the 'null' strings with NaN to ensure consistency over the database
        google_hotel = google_hotel.replace('null',np.NaN)
        google_swisshotel = google_swisshotel.replace('null', np.NaN)
        # Clean up and split the data retrieved from google
        google_hotel['go_reviewcount'] = google_hotel['google_reviews'].apply(
            lambda x: self.clean_reviews(x) if pd.notnull(x) else x)
        google_hotel['go_street'] = google_hotel['google_address'].apply(
            lambda x: self.extract_street(x) if pd.notnull(x) else x)
        google_hotel['go_postalcode'] = google_hotel['google_address'].apply(
            lambda x: self.extract_code(x) if pd.notnull(x) else x)
        google_hotel['go_city'] = google_hotel['google_address'].apply(
            lambda x: self.extract_city(x) if pd.notnull(x) else x)
        google_swisshotel.loc[:, 'sh_google_reviewcount'] = google_swisshotel['sh_google_reviewcount'].apply(
            lambda x: self.clean_reviews(x) if pd.notnull(x) else x)
        # Cleaning up some errors
        google_swisshotel.loc[:, 'sh_google_ratingvalue'] = google_swisshotel['sh_google_ratingvalue'].str.replace(',','.')
        # If we don't have a name, the rating is wrong and should not be set
        google_swisshotel.loc[google_swisshotel['sh_google_name'].isnull(), 'sh_google_ratingvalue'] = np.NaN
        # Change the code type to object for later use:
        google_hotel['go_postalcode'] = google_hotel['go_postalcode'].replace('', np.NaN)
        google_hotel['go_postalcode'] = google_hotel['go_postalcode'].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
        # Drop unnecessary columns before merging
        google_hotel = google_hotel.drop(['google_reviews', 'web-scraper-start-url', 'links-href','google_address'], 1)
        google_swisshotel = google_swisshotel.drop(['query-href', 'web-scraper-start-url'], 1)
        # Merge the google reviews with the database, filling up non-existing entries with NaN in the original database
        self.hotels = self.hotels.merge(google_hotel, on=self.ID, how='left')
        self.swisshotels = self.swisshotels.merge(google_swisshotel, on='swissid', how='left')

    def test_cropper(self, urls, test_mode, test_limit, test_randomize):
        if test_randomize:
            random.shuffle(urls)
        if test_mode:
            return urls[0:test_limit]
        return urls

    def extract_room_prices(self, text):
        """
        TODO test on real world data
        :param text:
        :return:
        """
        multiplicator = 1.0
        if 'CHF' in text:
            text = text.split('CHF')
        elif '$' in text:
            text = text.split('$')
        elif '€' in text:
            text = text.split('€')
            multiplicator = 1.2
        else:
            return (np.NaN, np.NaN, np.NaN)
        numbers = [re.sub("[^0-9]", "", str) for str in text]
        if '' in numbers:
            numbers.remove('')
        if len(numbers) == 2:
            numbers = [int(entry) for entry in numbers]
            return numbers[0]*multiplicator, numbers[1]*multiplicator, (numbers[0]+0.0+numbers[1])*multiplicator/2
        if len(numbers) == 1 and len(numbers[0]) > 0:
            return np.NaN, np.NaN, int(numbers[0])*multiplicator
        return np.NaN, np.NaN, np.NaN


    def treat_ta_data(self):
        """
        Some of the tripadvisor data needs to be treated additionally, street might contain the city and code as well
        We need to split the upper and lower room prices, extract the numbers into different fields
        :return:
        """
        # Remove the city and postalcode from the street address, we do this by splitting the address
        # among any four consecutive numbers, and leave only characters which appeared before
        self.hotels['ta_streetaddress'] = self.hotels['ta_streetaddress'].apply(
            lambda x: re.split("[0-9]{4}", x)[0].strip() if pd.notnull(x) else x)
        # Create two new columns with only prices
        self.hotels['ta_lower_price'], self.hotels['ta_higher_price'], self.hotels['ta_higher_price'] = zip(*self.hotels['ta_pricerange'].apply(
            lambda x: self.extract_room_prices(x) if pd.notnull(x) else (x,x,x)))

    def extract_zipcode_from_street(self, street):
        if len(re.split("[0-9]{4}", street)) != 2:
            return np.NaN
        pos = len(re.split("[0-9]{4}", street)[0])
        return street[pos:pos+4]

    def extract_five_ratings_from_string(self, text):
        if 'null' in text:
            return (np.NaN, np.NaN, np.NaN, np.NaN, np.NaN)
        text = text.replace('\'', '')
        text = text.split('Ausgezeichnet')[1]
        five, text = text.split('Sehr gut')
        four, text = text.split('Befriedigend ')
        three, text = text.split('Mangelhaft')
        two, one = text.split('Ungenügend')
        return (float(five), float(four), float(three), float(two), float(one))

    def get_exact_tripadvisor_ratingvalue(self, five, four, three, two, one):
        if pd.isnull(five):
            return np.NaN
        return (five*5+four*4+three*3+two*2+one)/(five+four+three+two+one)

    def extract_hotel_type(self, local_ranking, name):
        if pd.isnull(local_ranking) or 'null' in local_ranking:
            # Not so clear if it has hotel in the name if it really is a hotel or a pension, classification
            # seems a bit random anyway
            name = name.lower()
            if 'pension' in name or 'b&b' in name:
                return 'pension'
            return np.NaN
        if 'Pensionen' in local_ranking:
            return 'pension'
        if 'Hotels' in local_ranking:
            return 'hotel'
        if 'Sonstigen Unterkünften' in local_ranking:
            return 'other'
        raise ValueError('Trying to extract a type which does not exist')

    def clean_tripadvisor_hotels_and_coordinates(self, input_hotels, input_coordinates, input_webscraper):
        """
        Clean up the data from the full tripadvisor crawl and add the coordinates to it
        :param input_hotels: path to the CSV with the hotel data
        :param input_coordinates: path to the CSV with the coordinates for each hotel
        :return: None
        """
        hotels = pd.read_csv(input_hotels, encoding='utf-8-sig')
        coordinates = pd.read_csv(input_coordinates, encoding='utf-8-sig')
        webscraper = pd.read_csv(input_webscraper, encoding='utf-8')
        webscraper = webscraper.drop(['web-scraper-order', 'web-scraper-start-url', 'link'], axis=1)
        #webscraper = webscraper.rename(columns={'rooms': 'ta_rooms', })
        hotels = hotels.merge(webscraper, on='link-href', how='left')
        hotels = hotels.merge(coordinates, on='link-href', how='left')
        # Create an index
        hotels.insert(0, 'taid', range(1, len(hotels)+1))
        # Handle coordinates
        # Coordinates have to be between (45, 5) (48, 12)
        hotels['x'] = hotels['coord'].apply(lambda c: c.split(',')[0].replace('[','').strip())
        hotels['y'] = hotels['coord'].apply(lambda c: c.split(',')[1].replace(']','').strip())
        hotels['xn'] = hotels['x'].apply(lambda c: float(c) - 45)
        hotels['yn'] = hotels['y'].apply(lambda c: float(c) - 5)
        # Clean streets
        hotels['ta_streetaddress'] = hotels['ta_streetaddress'].apply(
            lambda x: re.split("[0-9]{4}", x)[0].strip() if pd.notnull(x) else x)
        # Retrieve zip code from streetaddress if not available
        hotels.loc[hotels['ta_postalcode'].isnull(), 'ta_postalcode'] =\
            hotels.loc[hotels['ta_postalcode'].isnull(),'ta_streetaddress'].apply(
            lambda x: self.extract_zipcode_from_street(x) if pd.notnull(x) else x)
        # Retrieve the price(s)
        hotels['ta_lower_price'], hotels['ta_higher_price'], hotels['ta_price'] = zip(*hotels['ta_pricerange'].apply(
            lambda x: self.extract_room_prices(x) if pd.notnull(x) else (x, x, x)))
        # Extract the number of rooms
        hotels['ta_rooms'] = hotels['rooms'].apply(
            lambda x: self.clean_reviews_test(x) if pd.notnull(x) else x)
        # Extract the exact number of ratings per hotel
        hotels['ta_fives'], hotels['ta_fours'], hotels['ta_threes'], hotels['ta_twos'], hotels['ta_ones']  = zip(
            *hotels['ratings'].apply(
                lambda x: self.extract_five_ratings_from_string(x) if pd.notnull(x) else (x, x, x, x, x)))
        hotels['ta_ratingvalue_exact'] = hotels.apply(lambda line: self.get_exact_tripadvisor_ratingvalue(line['ta_fives'], line['ta_fours'], line['ta_threes'], line['ta_twos'], line['ta_ones']), axis=1)
        hotels['ta_reviewcount_exact'] = hotels.apply(lambda line: line['ta_fives'] + line['ta_fours'] + line['ta_threes'] + line['ta_twos'] + line['ta_ones'] if pd.notnull(line['ta_fives']) else np.NaN, axis=1)
        # Extract the star ratings
        hotels['ta_stars'] = hotels['ta_stars'].apply(
            lambda x: float(x.split('ui_star_rating star_')[1][0]) if pd.notnull(x) and 'null' not in x else np.NaN)
        # Update the stars
        hotels['ta_stars'] = hotels['ta_stars'].apply(lambda x: x if pd.notnull(x) else 0)
        # Get the local ranking percentile
        hotels['ta_local_ranking_percentile'] = hotels['ta_local_ranking'].apply(lambda x: self.extract_local_percentile(x) if pd.notnull(x) else x)
        # Get the hotel type
        hotels['ta_type'] = hotels.apply(lambda line: self.extract_hotel_type(line['ta_local_ranking'], line['ta_name']), axis=1)

        # Remove the useless 'link' field
        hotels = hotels.drop(['link','rooms','ratings', 'ta_pricerange', 'coord'], axis=1)
        # Prepare for storage
        self.tripadvisor_hotels = hotels

    def store_tripadvisor_hotels_and_coordinates(self, output_hotels, output_coordinates):
        """

        :param output_hotels:
        :param output_coordinates:
        :return:
        """

        self.tripadvisor_hotels.to_csv(output_hotels, index=False, encoding='utf-8-sig')
        print("Stored the clean tripadvisor hotels to " + output_hotels)
        self.tripadvisor_hotels[['taid', 'x', 'y']].to_csv(output_coordinates, index=False, encoding='utf-8-sig')
        print("Stored the clean tripadvisor coordinates to " + output_coordinates)


    def clean_revenue_data(self, input_revenue, output_revenue, output_classification, interpolation, beginning_year, ending_year, vote=False):
        """
        Read, process and store the revenue data for the hotels
        TODO This way of looping over the data seems very slow compared to other actions in pandas
        :param input_revenue:
        :param output_revenue:
        :param interpolation:
        :param beginning_year:
        :param ending_year:
        :return:
        """
        print("Cleaning and interpolating revenue data...")
        revenue = pd.read_csv(input_revenue, encoding='utf-8')
        revenue = revenue.drop('month', axis=1)
        min_value = revenue['year'].min()
        revenue_table = revenue.values

        # Extract revenues per year for all entries
        tempids = {}
        for i in range(len(revenue_table)):
            tempid = revenue_table[i][0]
            year = revenue_table[i][1]
            revenue = revenue_table[i][2]
            if tempids.has_key(tempid):
                years = tempids[tempid]
                if years.has_key(year):
                    years[year].append(revenue)
                else:
                    years[year] = [revenue]
            else:
                years = {}
                years[year] = [revenue]
                tempids[tempid] = years

        # Majority vote for the revenue, in case we have more than one per year
        print("Extracting one revenue per year...")
        for tempid in tempids.keys():
            years = tempids[tempid]
            for year in years.keys():
                if vote:
                    years[year] = np.bincount(years[year]).argmax()
                else:
                    years[year] = np.average(years[year])



        # Sometimes we have multiple revenue estimates per year, we take the average to be the data we want
        years_wanted = range(beginning_year+1, ending_year+1)
        yearly_revenue = DataFrame({'tempid': tempids.keys()})
        # Look for the first value if we do not have one for the current minimum year

        print("Filling in the beginning year")
        for tempid in tempids.keys():
            years = tempids[tempid]
            # Did we get a revenue
            if years.has_key(beginning_year):
                yearly_revenue.loc[yearly_revenue['tempid'] == tempid, beginning_year] = years[beginning_year]
            else:
                yearly_revenue.loc[yearly_revenue['tempid'] == tempid, beginning_year] = np.NaN
                # No we did not, going back the last years
                previous_years = range(min_value, beginning_year)
                previous_years.reverse()
                for year in previous_years:
                    # Check if we found one, if yes we set the value and stop
                    if years.has_key(year):
                        yearly_revenue.loc[yearly_revenue['tempid'] == tempid, beginning_year] = years[year]
                        years[beginning_year] = years[year]
                        break
        # Store the values of revenue data if available in the new table
        print("Filling in the later years")
        for tempid in tempids.keys():
            years = tempids[tempid]
            for year in years_wanted:
                if years.has_key(year):
                    yearly_revenue.loc[yearly_revenue['tempid'] == tempid, year] = years[year]
                else:
                    yearly_revenue.loc[yearly_revenue['tempid'] == tempid, year] = np.NaN

        # Fill the first year if there are no values yet with revenues from the next years
        print("Filling up the first year with later years data if no previous data available")
        for tempid in tempids.keys():
            years = tempids[tempid]
            for year in years_wanted:
                if years.has_key(year):
                    # Fill up the values in pandas and our hashmap
                    yearly_revenue.loc[yearly_revenue['tempid'] == tempid, beginning_year] = years[year]
                    years[beginning_year] = years[year]
                    break

        # Interpolate for the next ones
        print("Interpolating for values in the middle which are not available")
        years_wanted = range(beginning_year+1, ending_year)
        for tempid in tempids.keys():
            years = tempids[tempid]
            for year in years_wanted:
                if years.has_key(year):
                    continue
                # Get the next year which has a value and the value
                # diff/(nextyear-prevyear) is new value
                next_value = -1
                last_year = year - 1
                last_value = years[last_year]
                next_years = range(year, ending_year+1)
                for next_year in next_years:
                    if years.has_key(next_year):
                        next_value = years[next_year]
                        break
                # Did we find a next value?
                if next_value == -1:
                    # No, so take the value from last year
                    yearly_revenue.loc[yearly_revenue['tempid'] == tempid, year] = last_value
                    years[year] = last_value
                else:
                    # Yes, so we set an interpolated one
                    this_value = float(next_value-last_value)/(next_year-last_year)+last_value
                    yearly_revenue.loc[yearly_revenue['tempid'] == tempid, year] = this_value
                    years[year] = this_value

        # For the last year we just take the previous value
        for tempid in tempids.keys():
            years = tempids[tempid]
            if not years.has_key(ending_year):
                yearly_revenue.loc[yearly_revenue['tempid'] == tempid, ending_year] = years[ending_year-1]
        # Store the differences
        years_wanted = range(beginning_year + 1, ending_year + 1)
        for year in years_wanted:
            yearly_revenue[str(year-1)+"_"+str(year)] = yearly_revenue[year] - yearly_revenue[year-1]
        for year in years_wanted:
            yearly_revenue["cl_"+str(year-1)+"_"+str(year)] = yearly_revenue[str(year-1)+"_"+str(year)].apply(lambda x: np.sign(int(x)))

        yearly_revenue.to_csv(output_revenue, index=False, encoding='utf-8-sig')
        print("Stored cleaned revenue data in " + output_revenue)
        # Making the columns into lines  TODO not very elegant, and could be automated
        newdf1 = DataFrame(
            {'tempid': yearly_revenue['tempid'].values, 'class': yearly_revenue['cl_2015_2016'].values, 'year': [2016] * len(yearly_revenue)})
        newdf2 = DataFrame(
            {'tempid': yearly_revenue['tempid'].values, 'class': yearly_revenue['cl_2014_2015'].values,
             'year': [2015] * len(yearly_revenue)})
        newdf3 = DataFrame(
            {'tempid': yearly_revenue['tempid'].values, 'class': yearly_revenue['cl_2013_2014'].values,
             'year': [2014] * len(yearly_revenue)})
        all = newdf1.append(newdf2)
        all = all.append(newdf3)
        all.to_csv(output_classification, index=False, encoding='utf-8-sig')
        print("Stored revenue classification in " + output_revenue)



    def get_tripadvisor_urls(self, test_mode, test_limit, test_randomize):
        """
        Returns a list of tuples of ids and the url to the tripadvisor entry
        :param test_mode:
        :param test_limit:
        :param test_randomize:
        :return:
        """
        # Extract id and url for non empty entries
        tripadvisors = self.hotels.loc[self.hotels[self.TRIPADVISOR_LINK].str.contains("tripadvisor", na=False), [self.ID, self.TRIPADVISOR_LINK]]
        tripadvisor_urls = [tuple(x) for x in tripadvisors.values]
        return self.test_cropper(tripadvisor_urls, test_mode, test_limit, test_randomize)

    def get_tripadvisor_hotels_urls(self, input_file):
        """

        :param input_file:
        :return:
        """
        tripadvisor = pd.read_csv(input_file, encoding='utf-8-sig')
        return tripadvisor['link-href'].values.tolist()

    def get_all_tripadvisor_urls(self, input_file, test_mode, test_limit, test_randomize):
        """
        Reads a file with URLs and returns the links according to the test parameters
        :param test_mode:
        :param test_limit:
        :param test_randomize:
        :return:
        """
        if self.tripadvisor_hotels is None:
            self.tripadvisor_hotels = pd.read_csv(input_file, encoding='utf-8-sig')
        tripadvisor_urls = self.tripadvisor_hotels['link-href'].values
        print("Have " + str(len(tripadvisor_urls)) + " tripadvisor urls to handle")
        return self.test_cropper(tripadvisor_urls, test_mode, test_limit, test_randomize)


    """
        Returns a list of tuples of ids and the url to the booking.com entry
    """
    def get_booking_urls(self, test_mode, test_limit, test_randomize):
        # Extract id and url for non empty entries
        bookings = self.hotels.loc[self.hotels[self.BOOKING_LINK].str.contains("booking", na=False), [self.ID, self.BOOKING_LINK]]
        booking_urls = [tuple(x) for x in bookings.values]
        return self.test_cropper(booking_urls, test_mode, test_limit, test_randomize)

    def get_swisshotel_urls(self, test_mode, test_limit, test_randomize):
        swisshotel_urls = self.swisshotels.loc[self.swisshotels[self.SWISSHOTEL_LINK].str.contains("swisshotel", na=False), [self.SWISS_ID, self.SWISSHOTEL_LINK]]
        swisshotel_urls = [tuple(x) for x in swisshotel_urls.values]
        return self.test_cropper(swisshotel_urls, test_mode, test_limit, test_randomize)

    """
        Returns a list of all the websites
    """
    def get_website_urls(self, test_mode):
        website_urls = self.hotels.loc[self.hotels['website'].str.contains("http", na=False), 'website'].tolist()
        if test_mode:
            return website_urls[0:3]
        return website_urls

    """
        Stores all the available data in a csv file
        If an attribute is not available, just an empty string will be stored
    """
    def store_hotels_to_csv(self, filename="testRun/output_pandas.csv"):
        # We need to add the BOM in order for excel to recognize the accents
        self.hotels.to_csv(filename, index=False, encoding='utf-8-sig')
        print("Wrote hotel data to " + filename)

    def store_swisshotel_to_csv(self, filename):
        self.swisshotels.to_csv(filename, index=False, encoding='utf-8-sig')
        print("Wrote swisshotel data to " + filename)

    def get_number_of_tripadvisor_ratings(self):
        return str(self.hotels.ta_ratingvalue.notnull().sum())

    def get_number_of_google_ratings(self):
        return str(self.hotels.google_score.notnull().sum())

    def get_number_of_booking_ratings(self):
        return str(self.hotels.bk_ratingvalue.notnull().sum())

    def get_number_of_entries(self):
        return str(self.hotels.tempid.notnull().sum())

    def get_entries_with_google_rating(self):
        return self.hotels.loc[self.hotels['google_score'].notnull(), ['tempid', 'google_score']]

    # Find all the entries where we have a tripadvisor link but no data from scraping
    def find_tripadvisor_errors(self):
        print("We had " + str(self.hotels.tripadvisor.notnull().sum()) + " tripadvisor URLs to crawl and have results for " + str(self.hotels.ta_ratingvalue.notnull().sum()))
        tripadvisor_errors = self.hotels.loc[self.hotels['tripadvisor'].notnull() & self.hotels['ta_ratingvalue'].isnull(), [self.ID, 'tripadvisor','ta_ratingvalue']]
        print("Found " + str(tripadvisor_errors['ta_ratingvalue'].isnull().sum()) + " empty tripadvisor fields")
        return tripadvisor_errors

    def find_booking_errors(self):
        print("We had " + str(self.hotels.booking.notnull().sum()) + " booking URLs to crawl and have results for " + str(
            self.hotels.bk_ratingvalue.notnull().sum()))
        booking_errors = self.hotels.loc[
            self.hotels['booking'].notnull() & self.hotels['bk_ratingvalue'].isnull(), [self.ID, 'booking','bk_ratingvalue']]
        print("Found " + str(booking_errors['bk_ratingvalue'].isnull().sum()) + " empty booking fields")
        return booking_errors

    def export_scraping_errors(self, error_file='fullRun/errors.csv'):
        tripadvisor_errors = self.find_tripadvisor_errors()
        booking_errors = self.find_booking_errors()
        errors = tripadvisor_errors.merge(booking_errors, on='tempid', how='outer')
        errors.to_csv(error_file, index=False, encoding='utf-8-sig')

    """
        Change an url to a keywords separated by an empty space
    """
    def extract_keywords_from_url(self, url):
        # Remove http://
        url = url.split('//')[1].lower()
        keywords = url.replace(".", " ").replace("-"," ").replace("/", " ").replace("?","")
        return keywords.replace("www", "").replace("home", "").replace("html","").replace("php", "").replace("home","").replace("index","").replace("de", "").replace("htm", "")

    def get_only_website_entries_database(self):
        only_websites = self.hotels.loc[
            self.hotels['website'].str.contains('http', na=False) & self.hotels['tripadvisor'].isnull() & self.hotels[
                'booking'].isnull(), ['tempid', 'website']]
        only_websites['website_keywords'] = only_websites['website'].apply(
            lambda x: self.extract_keywords_from_url(x) if pd.notnull(x) else x)
        return only_websites

    def get_websites_only_names(self, test_mode, test_limit, test_randomize):
        only_websites = self.get_only_website_entries_database()
        only_websites = only_websites[['tempid', 'website_keywords']]
        names = [tuple(x) for x in only_websites.values]
        return self.test_cropper(names, test_mode, test_limit, test_randomize)

    """
        Write a file containing all entries with a website but not a tripadvisor or booking.com entry
    """
    def export_only_website_entries(self, websites_file='fullRun/websites.csv'):
        only_websites = self.get_only_website_entries_database()
        print("Found " + str(only_websites['tempid'].notnull().sum()) + " entries with only a website")
        only_websites.to_csv(websites_file, index=False, encoding='utf-8-sig')

    """
        We consider only postal codes different if they are more than 10 apart
        TODO Is 10 really the right number?
    """
    def compare_data_sources(self, discrepancies_file):
        # Prepare the dataframe for the comparisons
        discrepancies = self.hotels.loc[:,['tempid', 'plz', 'ta_postalcode', 'go_postalcode', 'ta_name', 'go_name']]

        discrepancies['plz'] = discrepancies['plz'].apply(lambda x: int(x) if pd.notnull(x) else x)
        discrepancies['ta_postalcode'] = discrepancies['ta_postalcode'].apply(lambda x: int(x) if pd.notnull(x) else x)
        discrepancies['go_postalcode'] = discrepancies['go_postalcode'].apply(lambda x: int(x) if pd.notnull(x) else x)
        # Compare Tripadvisor and google addresses
        discrepancies.loc[:, 'ta_go_code'] = abs(discrepancies['ta_postalcode'] - discrepancies['go_postalcode']) > 10
        print("Found " + str(discrepancies.ta_go_code.sum()) + " discrepancies between tripadvisor and google")
        # Compare Tripadvisor and original addresses (less reliable)
        discrepancies.loc[:, 'ta_or_code'] = abs(discrepancies['ta_postalcode'] - discrepancies['plz']) > 10
        print("Found " + str(discrepancies.ta_or_code.sum()) + " discrepancies between tripadvisor and original data")
        # Compare Google and original addresses (less reliable)
        discrepancies.loc[:, 'go_or_code'] = abs(discrepancies['go_postalcode'] - discrepancies['plz']) > 10
        print(
        "Found " + str(discrepancies.go_or_code.sum()) + " discrepancies between google and original data")
        # Export the problem cases
        discrepancies = discrepancies.loc[discrepancies['ta_go_code'] | discrepancies['ta_or_code'] | discrepancies['go_or_code'], :]
        discrepancies.to_csv(discrepancies_file, index=False, encoding='utf-8-sig')
        print("Printed " + str(discrepancies.tempid.notnull().sum())+ " discrepancies to " + discrepancies_file)

    def create_matching_string(self, str):
        str = re.sub("[,.\-!?']", "", str).replace(" ", "")
        return str.lower()

    """
        Match the two datasets
    """
    def create_matching_by_address(self):
        self.hotels['matching_street'] = self.hotels['go_street'] + self.hotels['go_city']
        self.swisshotels['matching_street'] = self.swisshotels['sh_street'] + self.swisshotels['sh_city']
        self.hotels['matching_street'] = self.hotels['matching_street'].apply(lambda x: self.create_matching_string(x) if pd.notnull(x) else x)
        self.swisshotels['matching_street'] = self.swisshotels['matching_street'].apply(lambda x: self.create_matching_string(x) if pd.notnull(x) else x)
        # Merge only on nonzero keys
        self.matching = self.hotels.loc[self.hotels['matching_street'].notnull()].merge(self.swisshotels[['sh_name', 'swissid', 'sh_street', 'matching_street']], on='matching_street', how='inner')

    def unique_list(self, str):
        strl = str.split()
        ulist = []
        [ulist.append(x) for x in strl if x not in ulist]
        return ' '.join(ulist)

    def normalize_hotel_name(self, str):
        str = str.lower().replace(u'ô','o').replace(u'é','e').replace(u'è','e').replace(u'ü','u').replace(u'ö','o').replace(u'ä', 'a')
        str = re.sub("[,.\-!/?']", "", str)
        return str


    def create_fuzzy_strings(self, verbose=False, swisshotels=True, hotels=True, tripadvisor=False):
        if self.swisshotels is None and swisshotels:
            raise ValueError('Read the Swisshotel data first')
        if self.tripadvisor_hotels is None and tripadvisor:
            raise ValueError('Read the Tripadvisor data first')
        # Create the fuzzy string for swisshotels if necessary
        if 'sh_fuzzy' not in self.swisshotels.keys() and swisshotels:
            # Create and prepare a fuzzy field for swisshotels. We have to take care of NaN values as they might erase
            # the whole line if not taken into account
            self.swisshotels['sh_fuzzy'] = self.swisshotels['sh_name'] + " " + \
                                           self.swisshotels['sh_city'].apply(
                                               lambda x: x if pd.notnull(x) else "") + " " + \
                                           self.swisshotels['sh_street'].apply(
                                               lambda x: x if pd.notnull(x) else "") + " " + \
                                           self.swisshotels['sh_code'].apply(lambda x: str(x) if pd.notnull(x) else "")
            # Normalize the fuzzy text
            self.swisshotels['sh_fuzzy'] = self.swisshotels['sh_fuzzy'].apply(
                lambda x: self.normalize_hotel_name(x) if pd.notnull(x) else x)
            self.swisshotels['sh_fuzzy'] = self.swisshotels['sh_fuzzy'].apply(
                lambda x: self.unique_list(x) if pd.notnull(x) else x)
            # Add fuzzy name
            self.swisshotels['sh_fuzzy_name'] = self.swisshotels['sh_name'].apply(
                lambda x: self.normalize_hotel_name(x).replace('hotel', '').replace('restaurant', '') if pd.notnull(x) else x)
            self.swisshotels['sh_fuzzy_name'] = self.swisshotels['sh_fuzzy_name'].apply(
                lambda x: self.unique_list(x) if pd.notnull(x) else x)
            # Add fuzzy address
            self.swisshotels['sh_fuzzy_street'] = self.swisshotels['sh_street'].apply(
                lambda x: self.normalize_hotel_name(x) if pd.notnull(x) else x)
            self.swisshotels['sh_fuzzy_street'] = self.swisshotels['sh_fuzzy_street'].apply(
                lambda x: self.unique_list(x) if pd.notnull(x) else x)
            # Add fuzzy city
            self.swisshotels['sh_fuzzy_city'] = self.swisshotels['sh_city'].apply(
                lambda x: self.normalize_hotel_name(x) if pd.notnull(x) else x)

        # Create all the fuzzy strings for tripadvisor hotels
        if 'fuzzy' not in self.tripadvisor_hotels.keys() and tripadvisor:
            # Easier to write with a temporary reference
            tripadvisor = self.tripadvisor_hotels
            # Create and prepare a fuzzy field for swisshotels. We have to take care of NaN values as they might erase
            # the whole line if not taken into account
            tripadvisor['fuzzy'] = tripadvisor['ta_name'] + " " + \
                                           tripadvisor['ta_city'].apply(
                                               lambda x: x if pd.notnull(x) else "") + " " + \
                                           tripadvisor['ta_streetaddress'].apply(
                                               lambda x: x if pd.notnull(x) else "") + " " + \
                                           tripadvisor['ta_postalcode'].apply(lambda x: str(int(x)) if pd.notnull(x) else "")

            # Normalize the fuzzy text
            tripadvisor['fuzzy'] = tripadvisor['fuzzy'].apply(
                lambda x: self.normalize_hotel_name(x) if pd.notnull(x) else x)
            tripadvisor['fuzzy'] = tripadvisor['fuzzy'].apply(
                lambda x: self.unique_list(x) if pd.notnull(x) else x)
            # Add fuzzy name
            tripadvisor['fuzzy_name'] = tripadvisor['ta_name'].apply(
                lambda x: self.normalize_hotel_name(x).replace('hotel', '').replace('restaurant', '') if pd.notnull(
                    x) else x)
            tripadvisor['fuzzy_name'] = tripadvisor['fuzzy_name'].apply(
                lambda x: self.unique_list(x) if pd.notnull(x) else x)
            # Add fuzzy address
            tripadvisor['fuzzy_street'] = tripadvisor['ta_streetaddress'].apply(
                lambda x: self.normalize_hotel_name(x) if pd.notnull(x) else x)
            tripadvisor['fuzzy_street'] = tripadvisor['fuzzy_street'].apply(
                lambda x: self.unique_list(x) if pd.notnull(x) else x)
            # Add fuzzy city
            tripadvisor['fuzzy_city'] = tripadvisor['ta_city'].apply(
                lambda x: self.normalize_hotel_name(x) if pd.notnull(x) else x)
            self.tripadvisor_hotels = tripadvisor


        # Create the fuzzy strings for the hotels if necessary
        if 'fuzzy' not in self.hotels.keys() and hotels:
            # Get wherever possible the data from booking or tripadvisor, then from google for address and name
            all_names = self.merge_fields(['bk_name', 'ta_name', 'go_name'], 'all_name')
            all_cities = self.merge_fields(['ta_city', 'go_city'], 'all_city')
            all_streets = self.merge_fields(['ta_streetaddress', 'go_street'], 'all_street')
            all_postalcode = self.merge_fields(['ta_postalcode', 'go_postalcode'], 'all_postalcode')
            all_postalcode['all_postalcode'] = all_postalcode['all_postalcode'].apply(lambda x : int(x) if pd.notnull(x) else x)
            if verbose:
                print("#names " + str(all_names['all_name'].count()))
                print("#cities " + str(all_cities['all_city'].count()))
                print("#street " + str(all_streets['all_street'].count()))
                print("#postalcode " + str(all_postalcode['all_postalcode'].count()))

            all_names['fuzzy'] = all_names['all_name'] + " " + all_cities['all_city'].apply(
                lambda x: x if pd.notnull(x) else "") + " " + all_streets['all_street'].apply(
                lambda x: x if pd.notnull(x) else "") + " " + \
                                 all_postalcode['all_postalcode'].apply(lambda x: str(int(x)) if pd.notnull(x) else "")
            # Prepare the fuzzy field for matching by removing special characters and removing duplicates
            all_names['fuzzy'] = all_names['fuzzy'].apply(
                lambda x: self.normalize_hotel_name(x) if pd.notnull(x) else x)
            all_names['fuzzy'] = all_names['fuzzy'].apply(
                lambda x: self.unique_list(x) if pd.notnull(x) else x)
            # Add the fuzzy name
            all_names['fuzzy_name'] = all_names['all_name'].apply(
                lambda x: self.normalize_hotel_name(x).replace('hotel', '').replace('restaurant', '') if pd.notnull(x) else x)
            all_names['fuzzy_name'] = all_names['fuzzy_name'].apply(
                lambda x: self.unique_list(x) if pd.notnull(x) else x)
            # Add fuzzy_street
            all_names['fuzzy_street'] = all_streets['all_street'].apply(
                lambda x: self.normalize_hotel_name(x) if pd.notnull(x) else x)
            all_names['fuzzy_street'] = all_names['fuzzy_street'].apply(
                lambda x: self.unique_list(x) if pd.notnull(x) else x)
            # Add the postal code and street names
            all_names = all_names.merge(all_postalcode, on='tempid', how='left')
            all_names = all_names.merge(all_streets, on='tempid', how='left')
            self.hotels = self.hotels.merge(all_names, on='tempid', how='left')

    def compare_two_strings(self, a, b, levenshtein=False, verbose=False):
        if verbose:
            print([a,b])
        if levenshtein:
            return Levenshtein.ratio(a, b)
        return SM(None, a, b).ratio()

    def test(self, a, b, c, d):
        return (a+b+c)/3

    def find_best_fuzzy_match(self, row, algo, tripadvisor=False):
        if tripadvisor:
            id_name = 'taid'
            postalcode_name = 'ta_postalcode'
        else:
            id_name = 'tempid'
            postalcode_name = 'all_postalcode'
        if pd.isnull(row['fuzzy']) or pd.isnull(row[postalcode_name]):
            return pd.Series({id_name : row[id_name], 'swissid': np.NaN, 'score':np.NaN})
        temp_swisshotels = self.swisshotels[abs(self.swisshotels['sh_code'] - int(row[postalcode_name])) < 50]
        # Match according to the predefined matching algorithm, the final score should however be stored in the field
        # 'fuzzy score'.
        if algo == Matching.ALL:
            temp_swisshotels.loc[:, 'fuzzy_score'] = temp_swisshotels['sh_fuzzy'].apply(lambda x: self.compare_two_strings(row['fuzzy'], x) if pd.notnull(x) else 0.0)
        elif algo == Matching.ALL_T_NAME:
            temp_swisshotels.loc[:, 'fuzzy_score_all'] = temp_swisshotels['sh_fuzzy'].apply(
                lambda x: self.compare_two_strings(row['fuzzy'], x) if pd.notnull(x) else 0.0)
            temp_swisshotels.loc[:, 'fuzzy_score_name'] = temp_swisshotels['sh_fuzzy_name'].apply(
                lambda x: self.compare_two_strings(row['fuzzy_name'], x) if pd.notnull(x) else 0.0)
            temp_swisshotels.loc[:, 'fuzzy_score'] = temp_swisshotels.apply(
                lambda r: pow(r['fuzzy_score_all'] * r['fuzzy_score_name'], 0.5), axis=1)
        elif algo == Matching.ALL_T_NAME_T_STREET:
            temp_swisshotels.loc[:, 'fuzzy_score_all'] = temp_swisshotels['sh_fuzzy'].apply(
                lambda x: self.compare_two_strings(row['fuzzy'], x) if pd.notnull(x) else 0.0)
            temp_swisshotels.loc[:, 'fuzzy_score_name'] = temp_swisshotels['sh_fuzzy_name'].apply(
                lambda x: self.compare_two_strings(row['fuzzy_name'], x) if pd.notnull(x) else 0.0)
            temp_swisshotels.loc[:, 'fuzzy_score_street'] = temp_swisshotels['sh_fuzzy_street'].apply(
                lambda x: self.compare_two_strings(row['fuzzy_street'], x) if pd.notnull(x) else 0.0)
            temp_swisshotels.loc[:, 'fuzzy_score'] = temp_swisshotels.apply(
                lambda r: pow(r['fuzzy_score_all'] * r['fuzzy_score_name'] * r['fuzzy_score_street'],1.0/3), axis=1)
        elif algo == Matching.ALL_P_NAME:
            temp_swisshotels.loc[:, 'fuzzy_score_all'] = temp_swisshotels['sh_fuzzy'].apply(
                lambda x: self.compare_two_strings(row['fuzzy'], x) if pd.notnull(x) else 0.0)
            temp_swisshotels.loc[:, 'fuzzy_score_name'] = temp_swisshotels['sh_fuzzy_name'].apply(
                lambda x: self.compare_two_strings(row['fuzzy_name'], x) if pd.notnull(x) else 0.0)
            temp_swisshotels.loc[:, 'fuzzy_score'] = temp_swisshotels.apply(
                lambda r: (r['fuzzy_score_all'] + r['fuzzy_score_name'])/2, axis=1)
        elif algo == Matching.ALL_P_NAME_P_STREET:
            temp_swisshotels.loc[:, 'fuzzy_score_all'] = temp_swisshotels['sh_fuzzy'].apply(
                lambda x: self.compare_two_strings(row['fuzzy'], x) if pd.notnull(x) else 0.0)
            temp_swisshotels.loc[:, 'fuzzy_score_name'] = temp_swisshotels['sh_fuzzy_name'].apply(
                lambda x: self.compare_two_strings(row['fuzzy_name'], x) if pd.notnull(x) else 0.0)
            temp_swisshotels.loc[:, 'fuzzy_score_street'] = temp_swisshotels['sh_fuzzy_street'].apply(
                lambda x: self.compare_two_strings(row['fuzzy_street'], x) if pd.notnull(x) and pd.notnull(row['fuzzy_street']) else 0.0)
            temp_swisshotels.loc[:, 'fuzzy_score'] = temp_swisshotels.apply(
                lambda r: (r['fuzzy_score_all'] + r['fuzzy_score_name'] + r['fuzzy_score_street']) / 3, axis=1)
        elif algo == Matching.ALL_P_DYNAMIC:
            temp_swisshotels.loc[:, 'fuzzy_score_all'] = temp_swisshotels['sh_fuzzy'].apply(
                lambda x: self.compare_two_strings(row['fuzzy'], x) if pd.notnull(x) else 0.0)
            temp_swisshotels.loc[:, 'fuzzy_score_name'] = temp_swisshotels['sh_fuzzy_name'].apply(
                lambda x: self.compare_two_strings(row['fuzzy_name'], x) if pd.notnull(x) else 0.0)
            counter = 2
            # Calculate the street score only if we have a street
            if pd.notnull(row['fuzzy_street']):
                temp_swisshotels.loc[:, 'fuzzy_score_street'] = temp_swisshotels['sh_fuzzy_street'].apply(
                    lambda x: self.compare_two_strings(row['fuzzy_street'], x) if pd.notnull(x) and pd.notnull(
                        row['fuzzy_street']) else 0.0)
                counter += 1
            else:
                temp_swisshotels.loc[:, 'fuzzy_score_street'] = 0.0
            # Calculate the city score only if we have a city
            if pd.notnull(row['fuzzy_city']):
                temp_swisshotels.loc[:, 'fuzzy_score_city'] = temp_swisshotels['sh_fuzzy_city'].apply(
                    lambda x: self.compare_two_strings(row['fuzzy_city'], x) if pd.notnull(x) and pd.notnull(
                        row['fuzzy_city']) else 0.0)
                counter += 1
            else:
                temp_swisshotels.loc[:, 'fuzzy_score_city'] = 0.0
            temp_swisshotels.loc[:, 'fuzzy_score'] = temp_swisshotels.apply(
                lambda r: (r['fuzzy_score_all'] + r['fuzzy_score_name'] + r['fuzzy_score_street'] + r['fuzzy_score_city']) / counter, axis=1)
        else:
            raise ValueError('Unrecognized matching algorithm code')

        maxid = temp_swisshotels.swissid[temp_swisshotels.fuzzy_score.argmax()]
        maxscore = temp_swisshotels.fuzzy_score.max()
        return pd.Series({id_name : row[id_name], 'swissid': maxid, 'score': maxscore})


    def create_matching_by_fuzzy(self, filename = None, hotel_fields=['tempid', 'fuzzy', 'fuzzy_name' ,'fuzzy_street', 'all_name', 'all_street', 'all_postalcode'], swisshotel_fields = ['sh_name', 'sh_street', 'sh_code', 'sh_city', 'sh_fuzzy', 'sh_fuzzy_name', 'sh_fuzzy_street', 'swissid'], algorithm = Matching.ALL):
        # Create the fuzzy strings for swisshotel and hotel data
        self.create_fuzzy_strings()
        # Take the hotel data where it makes sense, only the entries with fuzzy fields
        hotel_data = self.hotels.loc[self.hotels['fuzzy'].notnull(),hotel_fields]
        # Check if we only need to match a special subset (indicated by a file)
        if filename is not None:
            # Check if we have to update the subset by reading it from a file, case were both are None is excluded
            if self.subset_file is not filename:
                self.subset = pd.read_csv(filename)
                self.subset_filename = filename
                # convert to object type so they match properly with hotel file
                self.subset['tempid'] = self.subset['tempid'].apply(lambda x: str(x))
                print("#samples: " + str(self.subset['tempid'].count()))
            # Only keep the data in the subset
            hotel_data = self.subset.merge(hotel_data, on='tempid', how='left')

        # Create the matching
        all_names_scores = hotel_data.apply(lambda row: self.find_best_fuzzy_match(row, algorithm), axis=1)
        all_names = hotel_data.merge(all_names_scores, on='tempid', how='left')
        self.matching = all_names.loc[all_names['swissid'].notnull(),:].merge(
            self.swisshotels[swisshotel_fields], on='swissid', how='left')

    def create_matching_tripadvisor_hotels(self, input_tripadvisor, output_matching, algorithm=Matching.ALL, swisshotel_fields = ['sh_name', 'sh_street', 'sh_code', 'sh_city', 'sh_fuzzy', 'sh_fuzzy_name', 'sh_fuzzy_street', 'swissid']):
        """

        :param input_tripadvisor: Path to the files with tripadvisor data, especially name and address
        :param output_matching: Path to a CSV file where the matched IDs will be stored
        :return:
        """
        if self.tripadvisor_hotels is None:
            self.tripadvisor_hotels = pd.read_csv(input_tripadvisor, encoding='utf-8-sig')
        # Remove hotels which will lead to trouble due to unusal zip codes
        self.tripadvisor_hotels = self.tripadvisor_hotels[self.tripadvisor_hotels['ta_postalcode'] < 10000]
        self.tripadvisor_hotels = self.tripadvisor_hotels[self.tripadvisor_hotels['ta_postalcode'] > 999]
        self.tripadvisor_hotels = self.tripadvisor_hotels.head(200)
        # Prepare fuzzy strings in both datasets
        self.create_fuzzy_strings(tripadvisor=True, hotels=False, swisshotels=True)
        all_names_scores = self.tripadvisor_hotels.apply(lambda row: self.find_best_fuzzy_match(row, algorithm, tripadvisor=True), axis=1)
        all_names = self.tripadvisor_hotels.merge(all_names_scores, on='taid', how='left')
        tripadvisor = all_names.loc[all_names['swissid'].notnull(), :].merge(
            self.swisshotels[swisshotel_fields], on='swissid', how='left')
        tripadvisor.to_csv(output_matching, index=False, encoding='utf-8-sig')
        print("Stored the matching between tripadvisor and swisshotel at " + output_matching)

    def find_closest_match(self, target, database, id):
        """
        Find the point which is closest to the target
        :param target: The point for which we need to find the closest match (x,y)
        :param database: A dataframe containing the 'id' field plus 'x' and 'y' coordinates corresponding to it
        :param id: The name of the id field
        :return: returns the content of the 'id' field for the closest match
        """
        x,y = target
        database.loc[:, 'distance'] = database.apply(
            lambda line: sqrt(pow(x-float(line['x']), 2)+pow(y-float(line['y']), 2)), axis=1)
        pos = database['distance'].argmin()
        return database[id][pos]

    def create_code_city_dictionaries_for_economic_data(self, key_code, key_city):
        """

        :param key_code:
        :param key_city:
        :return:
        """
        code_to_id = {}
        city_to_id = {}
        for elem in key_code:
            dest = elem[0]
            list = elem[1]
            for nb in list:
                code_to_id[nb.encode('utf-8')] = dest
        for elem in key_city:
            dest = elem[0]
            city = elem[1].encode('utf-8').lower()
            city_to_id[city] = dest
        return code_to_id, city_to_id


    def match_hotels_economic_data(self, input_economic_data_coordinates):
        """
        Match the hotels with their economic region
        :param economic_data: path to the econmic data
        :return:
        """
        economic_data = self.economic_data
        economic_data_coordinates = pd.read_csv(input_economic_data_coordinates, encoding='utf-8')
        economic_data.loc[:, 'ed_city_codes'] = economic_data['ed_city_codes'].apply(lambda x: x.split(','))
        # Create dictionaries for matching where the city or zip code match
        key_code = economic_data[['edid', 'ed_city_codes']].values
        key_city = economic_data[['edid', 'ed_city']].values
        code_to_id, city_to_id = self.create_code_city_dictionaries_for_economic_data(key_code, key_city)
        # Create city and zip code for matching according to priority: Google, Tripadvisor, own data
        self.hotels.loc[:, 'match_code'] = self.hotels['go_postalcode']
        self.hotels.loc[:, 'match_city'] = self.hotels['go_city'].str.lower()
        self.hotels.loc[self.hotels['match_code'].isnull(), 'match_code'] = self.hotels['ta_postalcode']
        self.hotels.loc[self.hotels['match_city'].isnull(), 'match_city'] = self.hotels['ta_city'].str.lower()
        self.hotels.loc[self.hotels['match_code'].isnull(), 'match_code'] = self.hotels['plz']
        self.hotels.loc[self.hotels['match_city'].isnull(), 'match_city'] = self.hotels['city'].str.lower()
        # Try matching according to the city name
        self.hotels.loc[:, 'edid'] = self.hotels['match_city'].apply(
            lambda city: city_to_id[city] if city_to_id.has_key(city) else np.NaN)
        print("Match with city name: " + str(self.hotels['edid'].notnull().sum()))
        # Try matching according to the zip code
        self.hotels.loc[self.hotels['edid'].isnull(), 'edid'] = self.hotels['match_code'].apply(
            lambda code: code_to_id[code] if code_to_id.has_key(code) else np.NaN)
        print("Match with code: " + str(self.hotels['edid'].notnull().sum()))
        # Get the closest location if no match has been found
        self.hotels.loc[self.hotels['edid'].isnull(), 'edid'] = self.hotels.apply(
            lambda line: self.find_closest_match((float(line['x']), float(line['y'])), economic_data_coordinates, 'edid'), axis=1)
        print("Match with NN: " + str(self.hotels['edid'].notnull().sum()))
        # Store the matching
        self.hotel_economic_matching = self.hotels[['tempid', 'edid']]

    def create_matching_tripadvisor_economic_data(self, input_tripadvisor, input_economic_data_coordinates, output_matching):
        """
        Match the tripadvisor hotels with their economic data
        :param input_tripadvisor:
        :param input_economic_data_coordinates:
        :param output_matching:
        :return:
        """
        hotels = pd.read_csv(input_tripadvisor, encoding='utf-8-sig')
        economic_data = self.economic_data
        economic_data_coordinates = pd.read_csv(input_economic_data_coordinates, encoding='utf-8')
        economic_data.loc[:, 'ed_city_codes'] = economic_data['ed_city_codes'].apply(lambda x: x.split(','))
        # Create dictionaries for matching where the city or zip code match
        key_code = economic_data[['edid', 'ed_city_codes']].values
        key_city = economic_data[['edid', 'ed_city']].values
        code_to_id, city_to_id = self.create_code_city_dictionaries_for_economic_data(key_code, key_city)
        # Prepare the relevant fields
        hotels.loc[:, 'match_code'] = hotels['ta_postalcode'].apply(lambda code: str(int(code)) if pd.notnull(code) else code)
        hotels.loc[:, 'match_city'] = hotels['ta_city'].str.lower()
        # Try matching according to the zip code
        hotels.loc[:, 'edid'] = hotels['match_code'].apply(
            lambda code: code_to_id[code] if pd.notnull(code) and code_to_id.has_key(code) else np.NaN)
        print("Match with code: " + str(hotels['edid'].notnull().sum()))
        # Try matching according to the city name
        hotels.loc[hotels['edid'].isnull(), 'edid'] = hotels['match_city'].apply(
            lambda city: city_to_id[city] if pd.notnull(city) and city_to_id.has_key(city) else np.NaN)
        print("Match with city name: " + str(hotels['edid'].notnull().sum()))
        # Get the closest location if no match has been found
        hotels.loc[hotels['edid'].isnull(), 'edid'] = hotels[hotels['edid'].isnull()].apply(
            lambda line: self.find_closest_match((float(line['x']), float(line['y'])), economic_data_coordinates,
                                                 'edid'), axis=1)
        print("Match with NN: " + str(hotels['edid'].notnull().sum()))
        # Store the matching
        hotels[['taid', 'edid']].to_csv(output_matching, index=False, encoding='utf-8-sig')



    def store_hotel_econmic_data_matching(self, filename):
        """

        :param filename:
        :return:
        """
        self.hotel_economic_matching.to_csv(filename, index=False, encoding='utf-8-sig')
        print("Stored the matching between hotels and economic data to: " + filename)

    def validate_matching(self):
        if self.matching is None:
            raise ValueError('The matching has to be created first before it can be validated...')
        total = self.matching['tempid'].count()
        ts = []
        for cutoff in np.arange(0.5,0.9,0.001):
            self.matching['temp'] = self.matching.apply(lambda row: row['swissid'] if row['score'] > cutoff else -1, axis = 1)
            correct = self.matching.loc[
                self.matching['temp'] == self.matching['swissid_corrected'], 'tempid'].count()
            percentage = (correct+0.0)/total
            ts += [(cutoff, percentage)]

        for a, b in ts:
            print(str(a) + ", " + str(b))

    def merge_row(self, row):
        if 't' in row['swissid']:
            # Have a mobiliar data row
            if pd.notnull(row['all_street']):
                street_split = re.split('(\d.*)', row['all_street'])
                if len(street_split) > 1:
                    street = street_split[0]
                    street_nb = street_split[1]
                else:
                    street = row['all_street']
                    street_nb = ''
            else:
                street = np.NaN
                street_nb = np.NaN
            return pd.Series(
                {'swissid': row['swissid'], 'NAME_KUNDE': row['all_name'], 'STRASSE_KUNDE': street,
                 'HAUSNUMMER_KUNDE' : street_nb, 'PLZ_KUNDE': row['all_postalcode'], 'ORT_KUNDE': row['all_city']})
        else:
            # Row from swisshotels
            if pd.notnull(row['sh_street']):
                street_split = re.split('(\d.*)', row['sh_street'])
                if len(street_split) > 1:
                    street = street_split[0]
                    street_nb = street_split[1]
                else:
                    street = row['sh_street']
                    street_nb = ''
            else:
                street = np.NaN
                street_nb = np.NaN
            return pd.Series(
                {'swissid': row['swissid'], 'NAME_KUNDE': row['sh_name'], 'STRASSE_KUNDE': street,
                 'HAUSNUMMER_KUNDE' : street_nb, 'PLZ_KUNDE': row['sh_code'], 'ORT_KUNDE': row['sh_city']})

    def merge_swisshotel_columns(self, keep, remove):
        self.swisshotels.loc[self.swisshotels[remove].notnull(), keep] = 'TRUE'
        self.swisshotels = self.swisshotels.drop(remove, 1)

    def extract_max_persons(self, str):
        if '-' in str:
            str = str.split('-')[1]
        return self.clean_reviews(str)

    def clean_column_name(self, name):
        """
        Remove any special character which would impede the column name from being used as a sql field name
        :param name: original name of the column/feature
        :return: name without special characters
        """
        name = name.replace(u'ô', 'o').replace(u'é', 'e').replace(u'è', 'e').replace(u'î', 'i')
        name = name.replace(u'â','a')
        name = name.replace(u'ü', 'u').replace(u'ö', 'o').replace(u'ä', 'a').replace('-', '_')
        name = re.sub("[(),.\*\-!/&?']", "", name)
        name = name.replace('__', '_') # more of an aesthetic thing
        return name

    def shorten_key(self, key):
        """
        Removes or shortens special keywords in a column name in order to not violate the thirty character
        limit for a column name in SQL
        :param key: the column name of a swisshotel feature
        :return: shortened key
        """
        key = key.replace('infrastructure', 'in')
        key = key.replace('specialization', 'sp')
        key = key.replace('local', 'lo')
        key = key.replace('chain', 'ch')
        key = key.replace('classification', 'cl')
        key = key.replace('payment_method', 'pm')
        key = key.replace('wheelchair_accessible', 'wa')
        # For special cases where the previous changes were not enough
        if len(key) > 30:
            key = key.replace('restaurant', 're')
            key = key.replace('hotel', 'ho')
            key = key.replace('accessible', 'ac')
            key = key.replace('offer_for_', '')
            key = key.replace('adjustable', 'adj')
            key = key.replace('bathroom', 'bath')
            key = key.replace('conditioning', 'cond')
        return key[0:30]

    def create_features_swisshotels(self, cleaned_output, merge_features_file, attribute_names):
        """
        Method which merges some rare fields in swisshotels data and writes a new CSV file
        We will also extract other features from the data such as number of managers, number of stars, size of of
        banquet rooms, check-in times, check-out times
        :return: None
        """
        # Reduce duplicate features
        merge_features = pd.read_csv(merge_features_file, encoding='utf-8')
        # Take only the fields where we have a field to merge with
        merge_features = merge_features.loc[merge_features['merge_with'].notnull(), :]
        print("Swisshotel currently has " + str(len(self.swisshotels.keys())) + " attributes")
        merge_features.apply(lambda row: self.merge_swisshotel_columns(row['merge_with'], row['attribute_name']), axis = 1)
        print("Swisshotel now has " + str(len(self.swisshotels.keys())) + " attributes after merging")

        # Remove 'not specified' by empty fields
        self.swisshotels.loc[:, 'sh_check-in'] = self.swisshotels['sh_check-in'].apply(
            lambda x: np.NaN if 'Not specified' in x else x)
        self.swisshotels.loc[:, 'sh_check-out'] = self.swisshotels['sh_check-out'].apply(
            lambda x: np.NaN if 'Not specified' in x else x)
        self.swisshotels.loc[:, 'sh_meeting_room'] = self.swisshotels['sh_meeting_room'].apply(
            lambda x: np.NaN if pd.isnull(x) or 'Not specified' in x else x)
        self.swisshotels.loc[:, 'sh_banquet_room'] = self.swisshotels['sh_banquet_room'].apply(
            lambda x: np.NaN if pd.isnull(x) or 'Not specified' in x else x)

        # Extract features from existing fields
        self.swisshotels.loc[self.swisshotels['sh_check-in'].notnull(), 'sh_check-in_specified'] = 'TRUE'
        self.swisshotels.loc[:, 'sh_24_hours_check-in'] = self.swisshotels['sh_check-in'].apply(
            lambda x: 'TRUE' if pd.notnull(x) and '24-hr' in x else np.NaN)
        self.swisshotels.loc[:, 'sh_max_meeting_room_size'] = self.swisshotels['sh_meeting_room'].apply(
            lambda x: self.extract_max_persons(x) if pd.notnull(x) else x)
        self.swisshotels.loc[:, 'sh_max_banquet_room_size'] = self.swisshotels['sh_banquet_room'].apply(
            lambda x: self.extract_max_persons(x) if pd.notnull(x) else x)
        self.swisshotels.loc[:, 'sh_nb_stars'] = self.swisshotels['sh_stars'].apply(
            lambda x: self.clean_reviews(x) if pd.notnull(x) else x)
        self.swisshotels.loc[self.swisshotels['sh_managers'].notnull(), 'sh_managers_available'] = 'TRUE'
        self.swisshotels.loc[:, 'sh_nb_managers'] = self.swisshotels['sh_managers'].apply(
            lambda x: x.count(';') + x.count('+') + x.count('&') if pd.notnull(x) else x)
        self.swisshotels.loc[:, 'sh_manager_couple'] = self.swisshotels['sh_managers'].apply(
            lambda x: 'TRUE' if pd.notnull(x) and ('+' in x or '&' in x) else np.NaN)

        # Clean up the column names in order to make them usable for SQL
        raw_keys = self.swisshotels.keys()
        clean_keys = [self.clean_column_name(key.lower()) for key in raw_keys]
        shortened_keys = [self.shorten_key(key) for key in clean_keys]
        self.swisshotels = self.swisshotels.rename(columns=dict(zip(raw_keys, shortened_keys)))
        keys = DataFrame({'shortened_attribute': shortened_keys, 'original_attribute' : clean_keys})

        # Export the clean version
        self.swisshotels.to_csv(cleaned_output, index=False, encoding='utf-8-sig')
        keys.to_csv(attribute_names, index=False, encoding='utf-8-sig')


    def create_csv_for_uid_request(self, merged_output_csv):
        """
        Takes all the available data from swisshotels and hotels and combines them in one big database, we add
        a 't' in front of the tempids to have unique identifiers. The process yields a CSV which is suitable
        for UID requests from the swiss government:
        https://www.bfs.admin.ch/bfs/de/home/register/unternehmensregister/unternehmens-identifikationsnummer/allgemeines-uid/uid-system.html

        :param merged_output_csv: where we write the data in CSV form
        :return: None
        """
        # Swisshotel part of the data
        swisshotels = self.swisshotels.loc[:,['swissid', 'sh_name', 'sh_city', 'sh_code', 'sh_street']]
        swisshotels['swissid'] = swisshotels['swissid'].apply(lambda x: str(x))
        # Mobiliar part of the data
        all_names = self.merge_fields(['bk_name', 'ta_name', 'go_name'], 'all_name')
        all_cities = self.merge_fields(['ta_city', 'go_city'], 'all_city')
        all_streets = self.merge_fields(['ta_streetaddress', 'go_street'], 'all_street')
        all_postalcode = self.merge_fields(['ta_postalcode', 'go_postalcode'], 'all_postalcode')
        all_names = all_names.merge(all_postalcode, on='tempid', how='left')
        all_names = all_names.merge(all_streets, on='tempid', how='left')
        hotels = all_names.merge(all_cities, on='tempid', how='left')
        hotels = hotels.loc[hotels['all_name'].notnull(), :]
        hotels.loc[:, 'tempid'] = hotels['tempid'].apply(lambda x: 't' + str(x))
        hotels = hotels.rename(columns={'tempid': 'swissid'})

        merged = swisshotels.merge(hotels, on='swissid', how='outer')
        merged = merged.apply(lambda row: self.merge_row(row), axis=1)
        merged.to_csv(merged_output_csv, index=False, encoding='utf-8-sig')

    def retrieve_swisshotel_coordinates(self, output_file):
        """
        Retrieve the GPS coordinates from the Bing maps service and store them to two CSV files,
        one contains the raw data retrieved from the service,
        the other tries to extract the 'x' and 'y' coordinates from the data retrieved (usually a list)
        :return: None
        """
        self.swisshotels.loc[:, 'sh_full_address'] = self.swisshotels.apply(
            lambda row: row['sh_street'] + ", " + str(row['sh_code']) + " " + row['sh_city'] + ", Switzerland"
            if pd.notnull(row['sh_street']) else str(row['sh_code']) + " " + row['sh_city'] + ", Switzerland", axis=1)
        self.swisshotels.loc[:, 'sh_coordinates'] = self.swisshotels['sh_full_address'].apply(lambda address: self.get_coordinates(address))
        self.swisshotels[['swissid', 'sh_coordinates']].to_csv(output_file + 'raw.csv', index=False,
                                                                               encoding='utf-8-sig')
        # Split them up in a x and y part
        self.swisshotels.loc[:, 'sh_x'] = self.swisshotels['sh_coordinates'].apply(lambda x: x[0])
        self.swisshotels.loc[:, 'sh_y'] = self.swisshotels['sh_coordinates'].apply(lambda x: x[1])
        self.swisshotels[['swissid', 'sh_coordinates', 'sh_x', 'sh_y']].to_csv(output_file, index=False, encoding='utf-8-sig')

    def load_swisshotel_coordinates(self, coordinates_file):
        coordinates = pd.read_csv(coordinates_file, encoding='utf-8')
        self.swisshotels = self.swisshotels.merge(coordinates, on='swissid', how='left')

    def store_matched_hotels_to_csv(self, matched_output_csv):
        """

        :param matched_output_csv: String containing the path/filename where the matched records should be stored
        :return: None
        """
        self.matching.to_csv(matched_output_csv, index=False, encoding='utf-8-sig')
        print("Wrote matched hotel data to " + matched_output_csv)

    def combine_ratings(self, go_ratingvalue, bk_ratingvalue, ta_ratingvalue, go_reviewcount, bk_reviewcount, ta_reviewcount, reviewcount):
        if pd.isnull(reviewcount):
            return np.NaN
        ratingvalue = 0.0
        if pd.notnull(go_reviewcount):
            ratingvalue += int(go_reviewcount)*float(go_ratingvalue)
        if pd.notnull(bk_reviewcount):
            ratingvalue += int(bk_reviewcount)*float(bk_ratingvalue)
        if pd.notnull(ta_reviewcount):
            ratingvalue += int(ta_reviewcount)*float(ta_ratingvalue)
        return ratingvalue/reviewcount

    def combine_reviewcount(self, go_reviewcount, bk_reviewcount, ta_reviewcount):
        count = 0
        if pd.notnull(go_reviewcount):
            count += int(go_reviewcount)
        if pd.notnull(bk_reviewcount):
            count += int(bk_reviewcount)
        if pd.notnull(ta_reviewcount):
            count+= int(ta_reviewcount)
        if count == 0:
            return np.NaN
        return count

    def create_prediction_tripadvisor_price(self, output_tripadvisor, input_tripadvisor):
        """
        Creating a database for hotel price prediction
        :param output_tripadvisor:
        :param input_tripadvisor:
        :return:
        """
        hotels = pd.read_csv(input_tripadvisor, encoding='utf-8-sig')
        self.tripadvisor_hotels = hotels
        print("Starting with " + str(len(hotels)) + " hotels for price prediction. Cleaning up...")
        to_keep = ['xn', 'yn', 'ta_ratingvalue_exact', 'ta_stars', 'ta_price', 'ta_rooms' ,'ta_reviewcount']
        # Remove outliers from data
        # Remove hotels without a price or a price lower than 10
        hotels = hotels[hotels['ta_price'] > 10]
        # Remove hotels with irregular coordinates
        hotels = hotels[hotels['xn'] > 0]
        hotels = hotels[hotels['yn'] > 0]
        # Remove hotels with none or very few ratings
        hotels = hotels[hotels['ta_reviewcount'] > 10]
        # Only relevant data for KNN prediction
        hotels = hotels[to_keep]
        hotels['ta_stars'] = hotels['ta_stars'].apply(lambda x: x if pd.notnull(x) else 0)
        # Rename the columns for convenience
        hotels = hotels.rename(columns={'ta_price': 'price', 'ta_ratingvalue_exact' : 'ratingvalue', 'ta_stars' : 'stars', 'ta_rooms' : 'rooms', 'ta_reviewcount' : 'reviewcount'})
        # Store to csv for use in R
        hotels.to_csv(output_tripadvisor, index=False, encoding='utf-8')
        print("We have " + str(len(hotels)) + " entries remaining, storing to " + output_tripadvisor)

    def create_prediction_tripadvisor_rooms(self, input_tripadvisor, economic_matching, swisshotel_matching, output_rooms, min_prop=0.05):
        """

        :param input_tripadvisor:
        :param output_rooms:
        :return:
        """
        hotels = pd.read_csv(input_tripadvisor, encoding='utf-8-sig')
        matching_econ = pd.read_csv(economic_matching, encoding='utf-8-sig')
        matching_swisshotel = pd.read_csv(swisshotel_matching, encoding='utf-8-sig')
        print("Starting with " + str(len(hotels)) + " hotels for price prediction. Cleaning up...")
        to_keep = ['taid', 'ta_rooms', 'x', 'y', 'ta_stars','ta_ratingvalue_exact', 'ta_reviewcount_exact', 'ta_type']
        hotels = hotels[to_keep]
        # Merge with swisshotel data
        hotels = hotels.merge(matching_swisshotel, on='taid', how='left')
        hotels = hotels.merge(self.swisshotels, on='swissid', how='left')
        hotels = self.drop_and_transform_swisshotel_columns(hotels, min_prop)
        # Help fill in attributes if necessary
        # TODO sh_rooms in rooms, only keep entries with google ratings, put sh_stars in stars
        hotels.loc[hotels['sh_rooms'].notnull(), 'ta_rooms'] = hotels['sh_rooms']
        hotels.loc[hotels['sh_nb_stars'].notnull(), 'ta_stars'] = hotels['sh_nb_stars']
        hotels = hotels.rename(columns={'sh_google_ratingvalue_x': 'go_ratingvalue', 'sh_google_reviewcount_x': 'go_reviewcount'})
        hotels = hotels[hotels['ta_rooms'].notnull()]
        hotels = hotels[hotels['go_reviewcount'].notnull()]
        hotels = hotels[hotels['go_ratingvalue'].notnull()]
        hotels = hotels[hotels['ta_ratingvalue_exact'].notnull()]
        print("We have " + str(len(hotels)) + " entries with room size data")
        # Merge with economic data
        hotels = hotels.merge(matching_econ, on='taid', how='left')
        hotels = hotels.merge(self.economic_data[['edid', 'ed_hotels_2016', 'ed_rooms_2016']])
        hotels['average_room'] = hotels['ed_rooms_2016']/hotels['ed_hotels_2016']
        to_remove = ['swissid', 'swisshotel','sh_nb_managers', 'sh_rooms', 'sh_beds', 'sh_nb_stars', 'sh_name', 'sh_stars', 'sh_check_out', 'sh_meeting_room', 'sh_code', 'sh_city', 'sh_street', 'trust_you', 'sh_managers', 'sh_banquet_room', 'sh_check_in', 'sh_telephone', 'sh_google_name_x', 'sh_coordinates','sh_x', 'sh_y', 'sh_google_name_y']
        hotels = hotels.drop(to_remove, axis=1)
        # TODO: Only keep entries with sisshotel data? Empirical test
        # Put the type into dummy variables
        hotels.loc[hotels['ta_type'].isnull(), 'ta_type'] = 'unknown'
        type_dummies = pd.get_dummies(hotels['ta_type'])
        hotels = hotels.join(type_dummies)
        hotels = hotels.drop(['ta_type', 'taid','edid', 'ed_hotels_2016' ,'ed_rooms_2016', 'sh_google_ratingvalue_y', 'sh_google_reviewcount_y', 'unknown'], axis=1)
        hotels = hotels.rename(columns={'ta_rooms': 'rooms', 'ta_ratingvalue_exact': 'ta_ratingvalue', 'ta_reviewcount_exact' : 'ta_reviewcount', 'ta_stars' : 'stars', 'average_room': 'ed_average_rooms'})
        hotels.to_csv(output_rooms, index=False, encoding='utf-8')

        print("Stored the prediction file for rooms under " + output_rooms)


    def prepare_attributes_for_prediction(self, df):
        """

        :param df:
        :return:
        """
        # Remove unnecessary lines with no data
        df = df[df['go_name'].notnull() | df['ta_name'].notnull() | df['bk_name'].notnull()]
        # Normalization
        # Remove the annoying warnings
        df.is_copy = False
        df['xn'] = df['x'].apply(lambda x: x - 45)
        df['yn'] = df['y'].apply(lambda x: x - 5)
        df['bk_ratingvalue'] = df['bk_ratingvalue'].apply(lambda x: x - 5 if pd.notnull(x) else x)
        # Merge all the reviews from google, tripadvisor and booking into one field
        df.loc[:, 'reviewcount'] = df.apply(lambda line: self.combine_reviewcount(
            line['go_reviewcount'], line['bk_reviewcount'], line['ta_reviewcount']), axis=1)
        df.loc[:, 'ratingvalue'] = df.apply(lambda line: self.combine_ratings(
            line['go_ratingvalue'], line['bk_ratingvalue'], line['ta_ratingvalue'],
            line['go_reviewcount'], line['bk_reviewcount'], line['ta_reviewcount'], line['reviewcount']), axis=1)
        # Create a single price for each room
        df.loc[df['ta_lower_price'].notnull(), 'price'] = df.loc[df['ta_lower_price'].notnull(),
                                                                      :].apply(
            lambda line: (float(line['ta_lower_price']) + float(line['ta_higher_price'])) / 2, axis=1)
        df.loc[df['stars'].isnull(), 'stars'] = 0
        # Remove lines where we do not have any rating, even if we might have found a website for the hotel
        df = df[df['ratingvalue'].notnull()]
        return df

    def create_prediction_revenue_small(self, output_revenue, input_revenue):
        """

        :param output_revenue:
        :param input_revenue:
        :return:
        """
        print('Creating prediction file')
        to_keep = ['tempid','ta_name', 'go_name', 'bk_name', 'rooms','ta_lower_price','ta_higher_price','ta_local_ranking_percentile',
                   'sh_max_meeting_room_size','go_ratingvalue','go_reviewcount',
                   'bk_ratingvalue', 'bk_reviewcount', 'ta_ratingvalue', 'ta_reviewcount','x','y', 'stars']
        x_values = self.merged[to_keep]
        x_values = self.prepare_attributes_for_prediction(x_values)
        y_values = pd.read_csv(input_revenue)
        y_values.loc[:,'tempid'] = y_values['tempid'].apply(lambda x: int(x) if pd.notnull(x) else x)
        all_values = x_values.merge(y_values[['tempid','rev_newest']], on='tempid', how='left')
        # Remove columns we do not want exported
        all_values = all_values.drop(['tempid', 'ta_name', 'go_name', 'bk_name', 'ta_lower_price', 'ta_higher_price'], axis=1)
        all_values = all_values.rename(columns={'rev_newest': 'revenue'})
        print("After droping lines with only id and location the dataset has now size of " + str(len(all_values)) + "\n Writing file to " + output_revenue)
        all_values.to_csv(output_revenue, index=False, encoding='utf-8')

    def extract_economic_data_for_year(self, line):
        """
        Returns the matching economic data for a given year (or its closest match)
        :param line: Line of the dataframe used for prediction
        :return: 5-let of economic data for the specific year line['year_newest']
        """
        year = line['year_newest']
        if year < 2013:
            year = 2013
        elif year > 2016:
            year = 2016
        year = str(year)
        ed_hotels = line['ed_hotels_' + year]
        ed_rooms = line['ed_rooms_' + year]
        ed_arrivals = line['ed_arrivals_' + year]
        ed_room_stays = line['ed_room_stays_' + year]
        ed_room_occupancy = line['ed_room_occupancy_' + year]
        return (ed_hotels, ed_rooms, ed_arrivals, ed_room_stays, ed_room_occupancy)

    def extract_variance_data_for_year(self, line):
        year = line['year_newest']
        end = '_01_01'
        if year < 2011:
            return np.NaN, np.NaN, np.NaN
        if year == 2011:
            year = str(year)
            variance = line['ta_variance_at_' + year + end]
            return variance, np.NaN, np.NaN
        if year == 2012:
            year = str(year)
            variance = line['ta_variance_at_' + year + end]
            variance_1y = variance - line['ta_variance_at_2011' + end]
            return variance, variance_1y, np.NaN
        year_1y = str(year-1)
        year_2y = str(year-2)
        year = str(year)
        variance = line['ta_variance_at_' + year + end]
        variance_1y = variance - line['ta_variance_at_'+ year_1y + end]
        variance_2y = variance - line['ta_variance_at_'+ year_2y + end]
        return variance, variance_1y, variance_2y

    def type_for_name(self, name):
        name = self.normalize_hotel_name(name)
        if 'restaurant' in name or 'pension' in name or 'gasthof' in name:
            return 'pension'
        if 'hotel' in name:
            return 'hotel'
        return 'other'

    def drop_and_transform_swisshotel_columns(self, df, min_prop):
        # Drop swisshotel columns with not enough data
        min_length = int(len(df) * min_prop)
        keys = [key for key in df.keys() if 'sh_' in key]
        remaining = []
        for key in keys:
            if df[key].notnull().sum() < min_length:
                df = df.drop(key, axis=1)
            else:
                remaining.append(key)
        keys = remaining
        if 'sh_max_meeting_room_size' in keys:
            df.loc[df['sh_max_meeting_room_size'].isnull(), 'sh_max_meeting_room_size'] = 0
        if 'sh_max_banquet_room_size' in keys:
            df.loc[df['sh_max_banquet_room_size'].isnull(), 'sh_max_banquet_room_size'] = 0
        # Transform the remaining columns with binary data to 0, 1
        for key in keys:
            if len(df[key].unique()) == 2:
                df.loc[df[key].notnull(), key] = 1
                df.loc[df[key].isnull(), key] = 0
        return df

    def get_change(self, line, change_attribute,t):
        now = change_attribute % (int(line['year']) - t)
        if now in line.keys():
            return line[now]
        return np.NaN

    def create_prediction_revenue_classification(self, output_classification, input_classification, yearly_ratings):
        """
        Create a file which should try to predict if a hotel will grow in a specified year
        :param output_classification:
        :param input_classification:
        :return:
        """

        change_attributes = ['ta_ratingvalue_at_%s_01_01', 'ta_reviewcount_at_%s_01_01', 'ta_variance_at_%s_01_01', 'ed_hotels_%s', 'ed_rooms_%s', 'ed_arrivals_%s', 'ed_stays_%s', 'ed_room_stays_%s', 'ed_room_occupancy_%s', 'ed_bed_occupandcy_%s']
        growth = pd.read_csv(input_classification, encoding='utf-8-sig')
        growth = growth.merge(self.x_values, on='tempid', how='left')

        print("Revenue data points raw " + str(len(growth)))
        growth = growth[growth['sh_in_close_to_public_transpor'].notnull()]
        growth = growth[growth['ta_ratingvalue_at_2018_01_01'].notnull()]
        growth.to_csv("elgichter2.csv", encoding='utf-8', index=False)
        for change_attribute in change_attributes:
            for t in np.arange(0,3+1):
                name = change_attribute.split("%s")[0] + "t" + str(t)
                growth[name] = growth.apply(lambda line: self.get_change(line, change_attribute, t), axis=1)


        """print("Revenue data points with data " + str(len(growth)))
        growth['change_ratingvalue_yoy'] = growth.apply(
            lambda line: line['ta_ratingvalue_at_'+str(line['year']) +'_01_01'] - line[
                'ta_ratingvalue_at_'+str(line['year']-1)+'_01_01'], axis=1)
        growth['change_ratingvalue_2yoy'] = growth.apply(
            lambda line: line['ta_ratingvalue_at_' + str(line['year']) + '_01_01'] - line[
                'ta_ratingvalue_at_' + str(line['year'] - 2) + '_01_01'], axis=1)
        growth['change_reviewcount_yoy'] = growth.apply(
            lambda line: line['ta_reviewcount_at_'+str(line['year']) +'_01_01'] - line[
                'ta_reviewcount_at_'+str(line['year']-1)+'_01_01'], axis=1)
        growth['change_reviewcount_2yoy'] = growth.apply(
            lambda line: line['ta_reviewcount_at_' + str(line['year']) + '_01_01'] - line[
                'ta_reviewcount_at_' + str(line['year'] - 2) + '_01_01'], axis=1)
        growth['change_rating_variance_yoy'] = growth.apply(
            lambda line: line['ta_variance_at_' + str(line['year']) + '_01_01'] - line[
                'ta_variance_at_' + str(line['year'] - 1) + '_01_01'], axis=1)
        growth['change_rating_variance_2yoy'] = growth.apply(
            lambda line: line['ta_variance_at_' + str(line['year']) + '_01_01'] - line[
                'ta_variance_at_' + str(line['year'] - 2) + '_01_01'], axis=1)

        growth['change_rooms'] = growth.apply(lambda line: line['ed_rooms_'+str(line['year'])] - line['ed_rooms_'+str(line['year']-1)], axis=1)
        growth['change_hotels'] = growth.apply(lambda line: line['ed_hotels_'+str(line['year'])] - line['ed_hotels_'+str(line['year']-1)], axis=1)
        growth['change_arrivals'] = growth.apply(
            lambda line: line['ed_arrivals_' + str(line['year'])] - line['ed_arrivals_' + str(line['year'] - 1)], axis=1)
        growth['change_stays'] = growth.apply(
            lambda line: line['ed_stays_' + str(line['year'])] - line['ed_stays_' + str(line['year'] - 1)], axis=1)
        growth['change_room_stays'] = growth.apply(
            lambda line: line['ed_room_stays_' + str(line['year'])] - line['ed_room_stays_' + str(line['year'] - 1)], axis=1)
        growth['change_room_occupancy'] = growth.apply(
            lambda line: line['ed_room_occupancy_' + str(line['year'])] - line[
                'ed_room_occupancy_' + str(line['year'] - 1)], axis=1)
        growth['change_bed_occupandcy_'] = growth.apply(
            lambda line: line['ed_bed_occupandcy_' + str(line['year'])] - line[
                'ed_bed_occupandcy_' + str(line['year'] - 1)], axis=1)"""
        # Also drop 'year', 'tempid',
        growth = growth.drop(
            [ 'ta_name', 'go_name', 'bk_name', 'ed_hotels_2013', 'ed_rooms_2013', 'ed_beds_2013',
             'ed_arrivals_2013', 'ed_stays_2013', 'ed_room_stays_2013', 'ed_room_occupancy_2013',
             'ed_bed_occupandcy_2013', 'ed_hotels_2014', 'ed_rooms_2014', 'ed_beds_2014', 'ed_arrivals_2014',
             'ed_stays_2014', 'ed_room_stays_2014', 'ed_room_occupancy_2014', 'ed_bed_occupandcy_2014', '_2014',
             'ed_hotels_2015', 'ed_rooms_2015', 'ed_beds_2015', 'ed_arrivals_2015', 'ed_stays_2015',
             'ed_room_stays_2015', 'ed_room_occupancy_2015', 'ed_bed_occupandcy_2015', '_2015', 'ed_hotels_2016',
             'ed_rooms_2016', 'ed_beds_2016', 'ed_arrivals_2016', 'ed_stays_2016', 'ed_room_stays_2016',
             'ed_room_occupancy_2016', 'ed_bed_occupandcy_2016', '_2016',  'ta_local_ranking_max',
             'ta_local_ranking_value', 'ta_ratingvalue_at_2011_01_01', 'ta_ratingvalue_at_2012_01_01',
             'ta_ratingvalue_at_2013_01_01', 'ta_ratingvalue_at_2014_01_01', 'ta_ratingvalue_at_2015_01_01',
             'ta_ratingvalue_at_2016_01_01', 'ta_ratingvalue_at_2017_01_01', 'ta_ratingvalue_at_2018_01_01',
             'ta_reviewcount_at_2011_01_01', 'ta_reviewcount_at_2012_01_01', 'ta_reviewcount_at_2013_01_01',
             'ta_reviewcount_at_2014_01_01', 'ta_reviewcount_at_2015_01_01', 'ta_reviewcount_at_2016_01_01',
             'ta_reviewcount_at_2017_01_01', 'ta_reviewcount_at_2018_01_01', 'ta_reviews_ratingvalue',
             'ta_reviews_reviewcount', 'ta_rooms', 'sh_rooms', 'sh_beds', 'sh_google_ratingvalue_x',
             'sh_google_reviewcount_x', 'sh_nb_stars', 'ta_stars', 'ta_reviewcount_exact', 'xn', 'yn',
             'ta_variance_at_2011_01_01', 'ta_variance_at_2012_01_01', 'ta_variance_at_2013_01_01',
             'ta_variance_at_2014_01_01', 'ta_variance_at_2015_01_01', 'ta_variance_at_2016_01_01',
             'ta_variance_at_2017_01_01', 'ta_variance_at_2018_01_01'],
            axis=1)
        growth.to_csv(output_classification, encoding='utf-8', index=False)


    def create_prediction_revenue_all(self, output_revenue, input_revenue, min_prop=0.05):
        """

        :param ouput_revenue:
        :param input_revenue:
        :return:
        """
        to_remove = ['street', 'nb', 'city', 'website', 'tripadvisor', 'booking', 'ta_streetaddress', 'ta_postalcode', 'ta_pricerange', 'ta_image', 'ta_addressregion', 'ta_city', 'go_street', 'go_postalcode', 'go_city', 'swissid', 'edid', 'swisshotel', 'sh_name', 'sh_stars', 'sh_check_out', 'sh_meeting_room', 'sh_code', 'sh_city', 'sh_street', 'trust_you', 'sh_managers', 'sh_banquet_room', 'sh_check_in', 'sh_telephone', 'sh_google_name_x', 'sh_coordinates', 'sh_x', 'sh_y', 'sh_google_name_y', 'sh_google_ratingvalue_y', 'sh_google_reviewcount_y', 'ed_city', 'ed_city_codes']
        x_values = self.merged.drop(to_remove, axis=1)
        x_values = self.prepare_attributes_for_prediction(x_values)
        # Drop swisshotel columns with not enough data
        x_values = self.drop_and_transform_swisshotel_columns(x_values, min_prop)
        print('We now have ' + str(len(x_values.keys()))+ ' columns remaining')
        # Do some simple imputation for google reviews (few fields)
        x_values.loc[x_values['go_reviewcount'].isnull(), 'go_reviewcount'] = 0
        x_values.loc[x_values['go_reviewcount'] == 0, 'go_ratingvalue'] = x_values['go_ratingvalue'].apply(lambda x: float(x) if pd.notnull(x) else x).mean()
        y_values = pd.read_csv(input_revenue)
        y_values.loc[:, 'tempid'] = y_values['tempid'].apply(lambda x: int(x) if pd.notnull(x) else x)

        # Create dummy variables for type after guessing them as much as possible
        x_values.loc[x_values['ta_type'].isnull(), 'ta_type'] = x_values.loc[x_values['ta_type'].isnull(), 'go_name'].apply(lambda x: self.type_for_name(x) if pd.notnull(x) else 'other')
        type_dummies = pd.get_dummies(x_values['ta_type'])
        x_values = x_values.join(type_dummies)
        x_values = x_values.drop('ta_type', axis=1)
        # Store the x_values for reuse in classification
        self.x_values = x_values

        all_values = x_values.merge(y_values[['tempid', 'rev_newest', 'year_newest']], on='tempid', how='left')
        # Use only the economic data corresponding to the year from the revenue
        all_values['ed_hotels'], all_values['ed_rooms'], all_values['ed_arrivals'], all_values['ed_room_stays'], \
        all_values['ed_room_occupancy'] = zip(*all_values.apply(
            lambda line: self.extract_economic_data_for_year(line), axis=1))
        # Use only the variance and variance change from the relevant year
        all_values['ta_variance'], all_values['ta_variance_change_1y'], all_values['ta_variance_change_2y'] = zip(*all_values.apply(
            lambda line: self.extract_variance_data_for_year(line), axis=1))

        # Remove unnecessary or duplicate data
        all_values = all_values.drop(['tempid', 'ta_name', 'go_name', 'bk_name','ed_hotels_2013', 'ed_rooms_2013', 'ed_beds_2013', 'ed_arrivals_2013', 'ed_stays_2013', 'ed_room_stays_2013', 'ed_room_occupancy_2013', 'ed_bed_occupandcy_2013', 'ed_hotels_2014', 'ed_rooms_2014', 'ed_beds_2014', 'ed_arrivals_2014', 'ed_stays_2014', 'ed_room_stays_2014', 'ed_room_occupancy_2014', 'ed_bed_occupandcy_2014', '_2014', 'ed_hotels_2015', 'ed_rooms_2015', 'ed_beds_2015', 'ed_arrivals_2015', 'ed_stays_2015', 'ed_room_stays_2015', 'ed_room_occupancy_2015', 'ed_bed_occupandcy_2015', '_2015', 'ed_hotels_2016', 'ed_rooms_2016', 'ed_beds_2016', 'ed_arrivals_2016', 'ed_stays_2016', 'ed_room_stays_2016', 'ed_room_occupancy_2016', 'ed_bed_occupandcy_2016', '_2016', 'year_newest', 'ta_local_ranking_max', 'ta_local_ranking_value', 'ta_ratingvalue_at_2011_01_01', 'ta_ratingvalue_at_2012_01_01', 'ta_ratingvalue_at_2013_01_01', 'ta_ratingvalue_at_2014_01_01', 'ta_ratingvalue_at_2015_01_01', 'ta_ratingvalue_at_2016_01_01', 'ta_ratingvalue_at_2017_01_01', 'ta_ratingvalue_at_2018_01_01', 'ta_reviewcount_at_2011_01_01', 'ta_reviewcount_at_2012_01_01', 'ta_reviewcount_at_2013_01_01', 'ta_reviewcount_at_2014_01_01', 'ta_reviewcount_at_2015_01_01', 'ta_reviewcount_at_2016_01_01', 'ta_reviewcount_at_2017_01_01', 'ta_reviewcount_at_2018_01_01', 'ta_reviews_ratingvalue', 'ta_reviews_reviewcount', 'ta_rooms', 'sh_rooms', 'sh_beds', 'sh_google_ratingvalue_x', 'sh_google_reviewcount_x', 'sh_nb_stars', 'ta_stars', 'ta_reviewcount_exact', 'xn', 'yn', 'ta_variance_at_2011_01_01', 'ta_variance_at_2012_01_01', 'ta_variance_at_2013_01_01', 'ta_variance_at_2014_01_01', 'ta_variance_at_2015_01_01', 'ta_variance_at_2016_01_01', 'ta_variance_at_2017_01_01', 'ta_variance_at_2018_01_01'],
                                     axis=1)
        all_values['ed_average_rooms'] = all_values['ed_rooms']/all_values['ed_hotels']
        all_values = all_values.rename(columns={'rev_newest' : 'revenue'})
        all_values.to_csv(output_revenue, index=False, encoding='utf-8')
        print("Printed the bigger file to " + output_revenue)






















