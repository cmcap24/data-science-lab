import gzip
import itertools
import logging
from io import BytesIO
from operator import itemgetter

import pandas as pd
from urllib.request import urlopen

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from utils.datetime_utils.time_wrapper import timefunc

from utils.database_utils.mongodb_handler import MongoDBHandler

logging.basicConfig(level=logging.INFO)


class AirBnBScraper(object):
    BASE_URL = 'http://insideairbnb.com/get-the-data.html'

    def __init__(self):
        self.listing_links = None
        self.calendar_links = None
        self.review_links = None

    @timefunc
    def execute(self):
        logging.info('Executing Scraper Pipeline...')
        self._get_file_links()
        listings_list = self._get_listings_data()
        review_dict = self._get_review_data()
        calendar_dict = self._get_calendar_data()

        result_list = []

        logging.info('Combining Results...')
        for listing in listings_list:
            _id = listing['listing_id']
            temp_dict = listing.copy()
            temp_dict['review_list'] = review_dict[_id]
            temp_dict['calendar_list'] = calendar_dict[_id]
            result_list.append(temp_dict)

        self._push_data_to_mongodb(result_list)

    @staticmethod
    @timefunc
    def _push_data_to_mongodb(data):
        logging.info('Pushing Data to MongoDB...')
        mongo_handler = MongoDBHandler(db_name='airbnb')
        mongo_handler.insert_values(collection_name='listings', data=data)
        logging.info('Push Complete')

    def _get_file_links(self, city='new-york'):
        logging.info(f'Collecting Links {city.upper()} for...')
        response = urlopen(self.BASE_URL)
        soup = BeautifulSoup(response, from_encoding=response.info().get_param('charset'))

        link_list = [link['href'] for link in soup.find_all('a', href=True)]

        filtered_link_list = list(filter(lambda link: link.endswith('.csv.gz') and city in link, link_list))
        self.listing_links = [link for link in filtered_link_list if 'listings' in link]
        self.calendar_links = [link for link in filtered_link_list if 'calendar' in link]
        self.review_links = [link for link in filtered_link_list if 'review' in link]
        logging.info(f'{len(self.listing_links)} file links collected')

    @timefunc
    def _get_listings_data(self):
        logging.info('Getting Listings Data...')
        df_listings = pd.DataFrame()

        if self.listing_links:
            logging.info('pulling listings data')
            for link in tqdm(self.listing_links):
                df_listings = df_listings.append(
                    self._download_zipfile_to_dataframe(link=link)
                )

        else:
            logging.info('no links to pull')
        df_listings.rename(columns={'id': 'listing_id'}, inplace=True)
        return df_listings.drop_duplicates().to_dict(orient='records')

    @timefunc
    def _get_review_data(self):
        logging.info('Getting Review Data...')
        df_reviews = pd.DataFrame()

        if self.listing_links:
            logging.info('pulling review data')
            for link in tqdm(self.review_links):
                df_reviews = df_reviews.append(
                    self._download_zipfile_to_dataframe(link=link)
                )

        else:
            logging.info('no links to pull')
        return self._prepare_data(df_reviews)

    @timefunc
    def _get_calendar_data(self):
        logging.info('Getting Calendar Data...')
        df_calendars = pd.DataFrame()

        if self.listing_links:
            logging.info('pulling calendar data')
            for link in tqdm(self.calendar_links):
                df_calendars = df_calendars.append(
                    self._download_zipfile_to_dataframe(link=link)
                )

        else:
            logging.info('no links to pull')
        return self._prepare_data(df_calendars)

    @staticmethod
    def _download_zipfile_to_dataframe(link):
        response = requests.get(link)
        gzip_file = gzip.GzipFile(fileobj=BytesIO(response.content))
        df = pd.read_csv(gzip_file)
        return df

    @staticmethod
    def _prepare_data(df):
        _list = df.drop_duplicates().to_dict(orient='records')
        _list = sorted(_list, key=itemgetter('listing_id'))
        _list_grouped = {
            k: list(v) for k, v in itertools.groupby(_list, key=itemgetter('listing_id'))
        }
        return _list_grouped
