import requests
import singledb.settings as settings

from django.core.urlresolvers import reverse


APP_NAME = 'singledb_urls'

class Client(object):
    def get(self, view_name, payload=None):
        self.url = settings.MASTER_URL + reverse('{}:{}'.format(APP_NAME, view_name))
        self.response = requests.get(self.url, payload)
        return self.response.json()
