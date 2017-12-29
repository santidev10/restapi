"""
Single database API connector module
"""
import json
from urllib.parse import urlencode

import requests
from django.conf import settings

from singledb.settings import DEFAULT_VIDEO_DETAILS_FIELDS, \
    DEFAULT_VIDEO_LIST_FIELDS, DEFAULT_CHANNEL_LIST_FIELDS, \
    DEFAULT_CHANNEL_DETAILS_FIELDS, DEFAULT_KEYWORD_DETAILS_FIELDS, \
    DEFAULT_KEYWORD_LIST_FIELDS


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
        params = "?{}".format(urlencode(query_params, doseq=True))
        # build url
        url = "{}{}{}".format(self.single_database_api_url, endpoint, params)
        # execute call
        try:
            if data is None:
                self.response = method(url, headers=headers, verify=False)
            else:
                self.response = method(url, headers=headers, verify=False,
                                       data=json.dumps(data))
        except Exception as e:
            raise SingleDatabaseApiConnectorException(
                "Unable to reach API. Original exception: {}".format(e))
        else:
            if self.response.status_code > 300:
                raise SingleDatabaseApiConnectorException(
                    "Error during iq api call: {}".format(self.response.text))
        try:
            response_data = self.response.json()
        except Exception as e:
            raise SingleDatabaseApiConnectorException(
                "Unable to parse api response: {}\n{}" \
                    .format(e, self.response.text))
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
        self.set_fields_query_param(
            query_params, DEFAULT_CHANNEL_DETAILS_FIELDS)
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def put_channel(self, query_params, pk, data):
        """
        Update channel
        :param query_params: dict
        """
        endpoint = "channels/" + pk + "/"
        self.set_fields_query_param(query_params,
                                    DEFAULT_CHANNEL_DETAILS_FIELDS)
        response_data = self.execute_put_call(endpoint, query_params, data)
        return response_data

    def get_channel_list(self, query_params):
        """
        Obtain channel list
        :param query_params: dict
        """
        endpoint = "channels/"
        self.set_fields_query_param(query_params, DEFAULT_CHANNEL_LIST_FIELDS)
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

    def get_top_channel_keywords(self, query_params):
        """
        Get top keywords for popular channels
        :param query_params: dict
        """
        endpoint = "channels/top_keywords/"
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

    def delete_channel_test(self, pk: str):
        """
        Delete channel
        :param pk: str Channel ID
        """
        endpoint = "channel_test/" + pk
        response_data = self.execute_delete_call(endpoint, query_params=dict())
        return response_data

    def get_video(self, query_params, pk):
        """
        Obtain video
        :param query_params: dict
        """
        endpoint = "videos/" + pk
        self.set_fields_query_param(
            query_params, DEFAULT_VIDEO_DETAILS_FIELDS)
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
        self.set_fields_query_param(
            query_params, DEFAULT_VIDEO_LIST_FIELDS)
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

    def get_channels_statistics(self, **params):
        endpoint = "channels/statistics/"
        response_data = self.execute_post_call(endpoint, {}, data=params)
        return response_data

    def get_videos_statistics(self, **params):
        endpoint = "videos/statistics/"
        response_data = self.execute_post_call(endpoint, {}, data=params)
        return response_data

    def store_ids(self, ids):
        """
        Wrap requested ids into hash
        """
        endpoint = "cached_objects/"
        response_data = self.execute_post_call(endpoint, {}, data=ids)
        return response_data['hash']

    @staticmethod
    def set_fields_query_param(query_params, default_fields):
        """
        Add fields query param to query params if absent
        """
        if "fields" not in query_params:
            query_params._mutable = True
            query_params["fields"] = ",".join(default_fields)
        return query_params

    def get_highlights_channels(self, query_params):
        endpoint = "channels/"
        self.set_fields_query_param(query_params, DEFAULT_CHANNEL_LIST_FIELDS)
        response_data = self.execute_get_call(endpoint, query_params)
        max_page = response_data.get('max_page', None)
        if max_page:
            response_data['max_page'] = 5 if max_page > 5 else max_page
        return response_data

    def get_highlights_videos(self, query_params):
        endpoint = "videos/"
        self.set_fields_query_param(query_params, DEFAULT_VIDEO_LIST_FIELDS)
        response_data = self.execute_get_call(endpoint, query_params)
        max_page = response_data.get('max_page', None)
        if max_page:
            response_data['max_page'] = 5 if max_page > 5 else max_page
        return response_data

    def get_channels_base_info(self, ids):
        fields = ("channel_id", "title", "thumbnail_image_url")
        ids_hash = self.store_ids(ids)
        query_params = dict(fields=",".join(fields), size=len(ids),
                            ids_hash=ids_hash)
        response_data = self.get_channel_list(query_params)
        items = response_data["items"]
        for i in items:
            i["id"] = i["channel_id"]
            del i["channel_id"]
        return items

    def get_videos_base_info(self, ids):
        fields = ("video_id", "title", "thumbnail_image_url", "duration")
        ids_hash = self.store_ids(ids)
        query_params = dict(fields=",".join(fields), size=len(ids),
                            ids_hash=ids_hash)
        response_data = self.get_video_list(query_params)
        items = response_data["items"]
        for i in items:
            i["id"] = i["video_id"]
            del i["video_id"]
        return items

    def auth_channel(self, data):
        """
        Authenticate channel
        :param query_params: dict
        """
        endpoint = "channels/authentication/"
        response_data = self.execute_post_call(endpoint, {}, data)
        return response_data

    def post_channels(self, channels_ids):
        """
        Create channels
        :param channels_ids: list of ids
        """
        endpoint = "channels/"
        response_data = self.execute_post_call(
            endpoint, {}, {"channels_ids": channels_ids})
        return response_data

    def get_keyword(self, query_params, pk):
        """
        Obtain keywords
        :param query_params: dict
        :param pk: str
        """
        endpoint = "keywords/" + pk + "/"
        self.set_fields_query_param(
            query_params, DEFAULT_KEYWORD_DETAILS_FIELDS)
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def get_keyword_list(self, query_params):
        """
        Obtain keywords list
        :param query_params: dict
        """
        endpoint = "keywords/"
        self.set_fields_query_param(
            query_params, DEFAULT_KEYWORD_LIST_FIELDS)
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data


class IQApiConnector(object):
    single_database_api_url = settings.IQ_API_URL

    def execute_post_call(self, *args, **kwargs):
        return self.execute_call(requests.post, *args, **kwargs)

    def execute_call(self, method, endpoint, query_params, data=None):
        """
        Make GET call to api
        """
        # prepare header
        headers = {"Content-Type": "application/json"}
        # prepare query params
        params = "?{}".format(urlencode(query_params, doseq=True))
        # build url
        url = "{}{}{}".format(self.single_database_api_url, endpoint, params)
        self.response = None
        # execute call
        try:
            if data is None:
                self.response = method(url, headers=headers, verify=False)
            else:
                self.response = method(url, headers=headers, verify=False,
                                       data=json.dumps(data))
        except Exception as e:
            raise SingleDatabaseApiConnectorException(
                "Unable to reach API. Original exception: {}".format(e))

        try:
            response_data = self.response.json()
        except Exception as e:
            return None

        return response_data

    def auth_channel(self, params):
        endpoint = "channels/remoteauthentication/"
        response_data = self.execute_post_call(endpoint, {}, data=params)
        return response_data
