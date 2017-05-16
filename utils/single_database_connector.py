"""
Single database API connector module
"""
import json

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
    iq_api_url = settings.IQ_API_URL

    def execute_post_call(self, url, body):
        """
        Make POST call to api
        """
        headers = {"Content-Type": "application/json"}
        try:
            response = requests.post(url, body, headers=headers).json()
        except Exception as e:
            raise IQApiConnectorException(
                "Unable to reach API. Original exception: {}".format(e))
        if response.status_code > 300:
            # TODO check for error this part
            # TODO save last response
            raise IQApiConnectorException(
                "Error during iq api call: {}".format(response.json()))
        return response

    def authenticate_channel(self, token):
        """
        Make channel authentication
        """
        endpoint = "site/channels/authentication/"
        url = "{}{}".format(self.iq_api_url, endpoint)
        post_body = json.dumps({"youtube_channel_token": token})
        response = self.execute_post_call(url, post_body)
        return response.json()

    def authenticate_page(self, token, page_id):
        """
        Make facebook page authentication
        """
        endpoint = "site/fb_pages/authentication/"
        url = "{}{}".format(self.iq_api_url, endpoint)
        post_body = json.dumps(
            {"facebook_page_token": token,
             "facebook_page_id": page_id})
        response = self.execute_post_call(url, post_body)
        return response.json()
