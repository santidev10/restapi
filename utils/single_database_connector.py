"""
Single database API connector module
"""
import requests
from django.conf import settings


class SingleDatabaseApiConnectorException(Exception):
    """
    Exception class for single database api connector
    """
    pass


class SingleDatabaseApiConnector(object):
    """
    Connector class for IQ api
    """
    single_database_api_url = settings.SINGLE_DATABASE_API_URL

    def execute_get_call(self, endpoint, query_params):
        """
        Make GET call to api
        """
        # prepare header
        headers = {"Content-Type": "application/json"}
        # prepare query params
        params = "?{}".format(
            "&".join(
                ["{}={}".format(key, value)
                 for key, value in query_params.items()]
            )
        )
        # build url
        url = "{}{}{}".format(self.single_database_api_url, endpoint, params)
        # execute call
        try:
            self.response = requests.get(url, headers=headers)
            response_data = self.response.json()
        except Exception as e:
            raise SingleDatabaseApiConnectorException(
                "Unable to reach API. Original exception: {}".format(e))
        else:
            if self.response.status_code > 300:
                raise SingleDatabaseApiConnectorException(
                    "Error during iq api call: {}".format(response_data))
        return response_data

    def get_channel_list(self, query_params):
        """
        Obtain channel list
        :param query_params: dict
        """
        endpoint = "channels/"
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data
