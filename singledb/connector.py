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
    single_database_api_url = settings.SINGLE_DATABASE_API_URL

    
    def execute_get_call(self, *args, **kwargs):
        return self.execute_call(requests.get, *args, **kwargs)

    def execute_put_call(self, *args, **kwargs):
        return self.execute_call(requests.put, *args, **kwargs)

    def execute_post_call(self, *args, **kwargs):
        return self.execute_call(requests.post, *args, **kwargs)

    def execute_delete_call(self, *args, **kwargs):
        return self.execute_call(requests.delete, *args, **kwargs)

    def execute_call(self, method, endpoint, query_params, data=None):
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
            if data is None:
                self.response = method(url, headers=headers, verify=False)
            else:
                self.response = method(url, headers=headers, verify=False, data=json.dumps(data))
            response_data = self.response.json()
        except Exception as e:
            raise SingleDatabaseApiConnectorException(
                "Unable to reach API. Original exception: {}".format(e))
        else:
            if self.response.status_code > 300:
                raise SingleDatabaseApiConnectorException(
                    "Error during iq api call: {}".format(response_data))
        return response_data

    def get_country_list(self, query_params):
        """
        Obtain coutry list
        :param query_params: dict
        """
        endpoint = "countries/"
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def get_channel(self, query_params, pk):
        """
        Obtain channel
        :param query_params: dict
        """
        endpoint = "channels/" + pk
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def put_channel(self, query_params, pk, data):
        """
        Update channel
        :param query_params: dict
        """
        endpoint = "channels/" + pk + "/"
        response_data = self.execute_put_call(endpoint, query_params, data)
        return response_data

    def get_channel_list(self, query_params):
        """
        Obtain channel list
        :param query_params: dict
        """
        endpoint = "channels/"
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def get_channel_filters_list(self, query_params):
        """
        Obtain channel filters list
        :param query_params: dict
        """
        endpoint = "channels/filters/"
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def delete_channels(self, query_params, data):
        """
        Delete channels
        :param query_params: dict
        """
        endpoint = "channel_set/"
        response_data = self.execute_delete_call(endpoint, query_params, data)
        return response_data

    def get_video(self, query_params, pk):
        """
        Obtain video
        :param query_params: dict
        """
        endpoint = "videos/" + pk
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def put_video(self, query_params, pk, data):
        """
        Update video
        :param query_params: dict
        """
        endpoint = "videos/" + pk + "/"
        response_data = self.execute_put_call(endpoint, query_params, data)
        return response_data

    def get_video_list(self, query_params):
        """
        Obtain video list
        :param query_params: dict
        """
        endpoint = "videos/"
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def get_video_filters_list(self, query_params):
        """
        Obtain video filters list
        :param query_params: dict
        """
        endpoint = "videos/filters/"
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def delete_videos(self, query_params, data):
        """
        Delete videos
        :param query_params: dict
        """
        endpoint = "video_set/"
        response_data = self.execute_delete_call(endpoint, query_params, data)
        return response_data
