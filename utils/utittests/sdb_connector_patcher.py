import json


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
        with open('saas/fixtures/tests/singledb_video_list.json') as data_file:
            data = json.load(data_file)
        for i in data["items"]:
            i["video_id"] = i["id"]
        query_params = kwargs.get("query_params", dict())
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
        video = next(filter(lambda c: c["id"] == pk, videos["items"]))
        return video

    def store_ids(self, query_params):
        pass
