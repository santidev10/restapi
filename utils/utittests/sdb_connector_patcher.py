import json

from django.http import Http404

from utils.singleton import singleton


# @singleton
class SingleDatabaseApiConnectorPatcher:
    """
    We can use the class to patch SingleDatabaseApiConnector in tests
    """

    @staticmethod
    def get_channel_list(*args, **kwargs):
        with open('saas/fixtures/tests/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        for i in data["items"]:
            i["channel_id"] = i["id"]
        return data

    @staticmethod
    def get_video_list(*args, **kwargs):
        query_params = kwargs.get("query_params", dict())
        if query_params.get("video_id__range", ",") != ",":
            return dict(items=[])
        with open('saas/fixtures/tests/singledb_video_list.json') as data_file:
            data = json.load(data_file)
        for i in data["items"]:
            i["video_id"] = i["id"]
        size = query_params.get("size", 1)
        if size == 0:
            data["max_page"] = None
        aggregations = query_params.get("aggregations", None)
        if aggregations is None:
            del data["aggregations"]
        return data

    def get_keyword_list(*args, **kwargs):
        with open('saas/fixtures/tests/singledb_keyword_list.json') as data_file:
            data = json.load(data_file)
        return data

    def get_channels_base_info(self, *args, **kwargs):
        data = self.get_channel_list()
        return data["items"]

    def get_videos_base_info(self, *args, **kwargs):
        data = self.get_video_list()
        return data["items"]

    def auth_channel(self, *args):
        return dict(channel_id="Chanel Id", access_token="Access Token")

    def put_channel(self, query_params, pk, data):
        channel = self.get_channel(query_params, pk=pk)
        channel.update(data)
        return channel

    def get_channel(self, query_params, pk):
        with open('saas/fixtures/tests/singledb_channel_list.json') as data_file:
            channels = json.load(data_file)
        channel = next(filter(lambda c: c["id"] == pk, channels["items"]))
        return channel

    def get_video(self, query_params, pk):
        with open('saas/fixtures/tests/singledb_video_list.json') as data_file:
            videos = json.load(data_file)
        try:
            video = next(filter(lambda c: c["id"] == pk, videos["items"]))
        except StopIteration:
            raise Http404("No video")
        return video

    def get_bad_words_list(self, *args):
        with open("saas/fixtures/tests/singledb_bad_words_list.json") as data_file:
            bad_words = json.load(data_file)
        return bad_words

    def get_highlights_keywords(self, *args):
        return self.get_keyword_list(*args)

    def delete_channels(self, *args):
        pass

    def get_keyword(self, *args, **kwargs):
        return dict(keyword="123")

    def post_bad_word(self, *args):
        pass

    def get_bad_word(self, *args):
        pass

    def put_bad_word(self, *args):
        pass

    def delete_bad_word(self, *args):
        pass

    def get_bad_words_history_list(self, *args):
        pass

    def get_bad_words_categories_list(self, *args):
        pass

    def put_video(self, *args):
        pass

    def delete_videos(self, *args):
        pass

    def store_ids(self, ids):
        pass

    def delete_channel_test(self, *args, **kwargs):
        pass

    def get_channel_list_full(self, *args, **kwargs):
        pass

    def get_video_list_full(self, *args, **kwargs):
        pass

    def unauthorize_channel(self, *args):
        pass


def monkey_patch():
    import singledb.connector
    singledb.connector.SingleDatabaseApiConnector_origin = singledb.connector.SingleDatabaseApiConnector
    singledb.connector.SingleDatabaseApiConnector = SingleDatabaseApiConnectorPatcher
