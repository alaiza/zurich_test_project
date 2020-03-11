import logging
import requests
import json
from retry import retry

_logger = logging.getLogger(__name__)

class APIService:

    def __init__(self, key):
        self._fetch_url = "https://www.worldcoinindex.com/apiservice/json?key={key}".format(key=key)
        self.raw_data = None
        self.data = None
        self.update_data()

    def data_read(self, raw_data):
        data_dict = dict()
        for crypto in raw_data['Markets']:
            data_dict[crypto['Label'].split('/')[0]] = crypto
        return data_dict

    @retry(tries=3)
    def update_data(self):
        self.raw_data = json.loads(requests.get(self._fetch_url).content)
        self.data = self.data_read(self.raw_data)

    def get_all_data(self):
        return self.data

    def get_value_of(self, crypto, fiat):
        return self.data[crypto.upper()]['Price_{}'.format(fiat.lower())]