from singledb.connector import SingleDatabaseApiConnector as Connector
import re

class AuditMixin(object):
    def get_all_bad_words(self):
        if not self.connector:
            self.connector = Connector()
        bad_words = self.connector.get_bad_words_list({})
        bad_words_names = [item["name"] for item in bad_words]
        bad_words_names = list(set(bad_words_names))

        return bad_words_names

    def compile_audit_regexp(self, keywords: list):
        regexp = re.compile(
            "({})".format("|".join([r"\b{}\b".format(re.escape(word)) for word in keywords]))
        )
        return regexp

    def get_videos_batch(self, channel_ids: list, fields: str) -> list:
        """
        Retrieves all videos associated with channel_ids
        :param channel_ids: (list) -> Channel id strings
        :return: (list) -> video objects from singledb
        """
        params = dict(
            fields=fields,
            sort="video_id",
            size=10000,
            channel_id__terms=",".join(channel_ids),
        )
        response = self.connector.execute_get_call("videos/", params)

        return response.get('items')