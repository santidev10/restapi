from brand_safety import constants
from brand_safety.audit_services.base import AuditService
from brand_safety.audit_models.base import Audit
from brand_safety.audit_models.brand_safety_channel_audit import BrandSafetyChannelAudit
from brand_safety.audit_models.brand_safety_video_audit import BrandSafetyVideoAudit
from singledb.connector import SingleDatabaseApiConnector
from utils.data_providers.sdb_data_provider import SDBDataProvider
from utils.data_providers.youtube_data_provider import YoutubeDataProvider
from utils.elasticsearch import ElasticSearchConnector


class StandardBrandSafetyService(AuditService):
    """
    Interface for consuming source data from providers and driving brand safety logic
    """
    sdb_batch_limit = 10000
    video_audits_sorted = False
    video_fields = "video_id,title,channel_id,channel__title,channel__subscribers,description," \
                   "tags,category,likes,dislikes,views,language,transcript,country,thumbnail_image_url"
    channel_fields = "channel_id,title,description,category,subscribers,likes,dislikes,views,language,url,country,thumbnail_image_url"

    def __init__(self, *_, **kwargs):
        audit_types = kwargs["audit_types"]
        super().__init__(audit_types)
        self.es_connector = ElasticSearchConnector()
        self.sdb_connector = SingleDatabaseApiConnector()
        self.yt_connector = YoutubeDataProvider()
        self.sdb_data_provider = SDBDataProvider()
        self.score_mapping = kwargs["score_mapping"]
        self.brand_safety_score_multiplier = kwargs["score_multiplier"]
        self.default_video_category_scores = kwargs["default_video_category_scores"]
        self.default_channel_category_scores = kwargs["default_channel_category_scores"]

    def audit_videos(self, video_data=None, channel_ids=None):
        """
        Audits SingleDB video data
        :param video_data: list -> Video data from SDB
        :param channel_ids: list -> Channel ids
        :return: Video audit objects
        """
        if video_data and channel_ids:
            raise ValueError("You must either provide video data to audit or channel ids to retrieve video data for.")
        if channel_ids:
            video_data = self.get_channel_video_data(channel_ids)
        video_audits = []
        for video in video_data:
            if not video.get("channel_id"):
                continue
            # Create a copy of default scores for each audit
            default_category_score_copy = {}
            default_category_score_copy.update(self.default_video_category_scores)
            audit = BrandSafetyVideoAudit(
                video,
                self.audit_types,
                source=constants.SDB,
                score_mapping=self.score_mapping,
                brand_safety_score_multiplier=self.brand_safety_score_multiplier,
                default_category_scores=default_category_score_copy,
            )
            audit.run_audit()
            video_audits.append(audit)
        self.video_audits_sorted = True
        return video_audits

    def audit_channels(self, sorted_video_audits: dict) -> list:
        """
        Audits Channels by retrieving channel data and using sorted Video audit objects by channel id
        :param sorted_video_audits: list -> Video Audit objects
        :return: list -> Channel Audit objects
        """
        channel_ids = list(sorted_video_audits.keys())
        sorted_channel_data = self.get_sorted_channel_data(channel_ids)
        channel_audits = []
        for channel_id, video_audits in sorted_video_audits.items():
            channel_data = sorted_channel_data.get(channel_id, None)
            if not channel_data:
                continue
            # Create a copy of default scores for each audit
            default_category_score_copy = {}
            default_category_score_copy.update(self.default_channel_category_scores)
            channel_audit = BrandSafetyChannelAudit(
                video_audits,
                self.audit_types,
                channel_data,
                source=constants.SDB,
                score_mapping=self.score_mapping,
                brand_safety_score_multiplier=self.brand_safety_score_multiplier,
                default_category_scores=default_category_score_copy,
            )
            channel_audit.run_audit()
            channel_audits.append(channel_audit)
        return channel_audits

    @staticmethod
    def gather_brand_safety_results(audits):
        """
        Maps audits to their brand safety scores
        :param audits: Video Audit objects
        :return: list -> brand safety score dictionaries
        """
        results = []
        for audit in audits:
            es_repr = audit.es_repr()
            results.append(es_repr)
        return results

    def get_sorted_channel_data(self, channel_ids: list) -> dict:
        """
        Retrieves singledb data with given channel ids
        :param channel_ids: list -> Youtube channel ids
        :return: dict -> channel_id: channel_data
        """
        params = dict(
            fields=self.channel_fields,
            sort="channel_id",
            size=self.sdb_batch_limit,
            channel_id__terms=",".join(channel_ids),
        )
        response = self.sdb_connector.get_channel_list(params)
        data = response.get("items")
        retrieved_channel_ids = [channel["channel_id"] for channel in data]
        remaining_channel_ids = set(channel_ids) - set(retrieved_channel_ids)
        remaining_channel_data = self.yt_connector.get_channel_data(list(remaining_channel_ids))
        mapped_to_sdb = [self.map_youtube_channel_data(channel) for channel in remaining_channel_data]
        data.extend(mapped_to_sdb)

        channel_data_by_id = {
            channel["channel_id"]: channel
            for channel in data
            if channel.get("channel_id") is not None
        }
        return channel_data_by_id

    def get_channel_video_data(self, channel_ids: list, fields=None) -> list:
        """
        Retrieve channel metadata from SDB
        :param channel_ids:
        :return:
        """
        params = dict(
            fields=self.video_fields if fields is None else fields,
            sort="video_id",
            size=self.sdb_batch_limit,
            channel_id__terms=",".join(channel_ids),
        )
        response = self.sdb_connector.get_video_list(params)
        video_data = response.get("items", [])
        retrieved_channel_ids = [video["channel_id"] for video in video_data if video.get("channel_id")]
        remaining_channel_ids = set(channel_ids) - set(retrieved_channel_ids)
        remaining_video_data = self.yt_connector.get_channel_video_data(list(remaining_channel_ids))
        mapped_to_sdb = [self.map_youtube_video_data(video) for video in remaining_video_data]

        all_data = video_data + mapped_to_sdb
        return all_data

    def get_video_data(self, video_ids: iter) -> list:
        """
        Retrieve video metadata
            First get from sdb, then default to Youtube Data API
        :param video_ids: list | tuple -> Youtube video ids
        :return: list
        """
        params = {
            "fields": self.video_fields,
            "sort": "video_id",
            "size": self.sdb_batch_limit,
            "video_id__terms": ",".join(video_ids)
        }
        sdb_response = SingleDatabaseApiConnector().get_video_list(params, ignore_sources=True)
        remaining = list(set(video_ids) - set([item["video_id"] for item in sdb_response.get("items", [])]))

        yt_response = self.yt_connector.get_video_data(remaining)
        yt_mapped = [self.map_youtube_video_data(item) for item in yt_response]
        all_data = sdb_response.get("items", []) + yt_mapped
        return all_data

    @staticmethod
    def map_youtube_video_data(data: dict) -> dict:
        """
        Map Youtube Data API Video list response to SDB video response
        :param data:
        :return:
        """
        text = data["snippet"].get("title", "") + data["snippet"].get("description", "")
        sdb_mapped = {
            "channel_id": data["snippet"].get("channelId", ""),
            "channel_url": "https://www.youtube.com/channel/" + data["snippet"].get("channelId", ""),
            "channel__title": data["snippet"].get("channelTitle", ""),
            "channel__subscribers": data.get("statistics", {}).get("channelSubscriberCount"),
            "video_id": data["id"],
            "title": data["snippet"]["title"],
            "video_url": "https://www.youtube.com/video/" + data["id"],
            "views": data["statistics"].get("viewCount", 0),
            "description": data["snippet"].get("description", ""),
            "category": constants.VIDEO_CATEGORIES.get(data["snippet"].get("categoryId"), constants.DISABLED),
            "language": Audit.get_language(text),
            "country": data["snippet"].get("country", constants.UNKNOWN),
            "likes": data["statistics"].get("likeCount", 0),
            "dislikes": data["statistics"].get("dislikeCount", 0),
            "tags": ",".join(data["snippet"].get("tags", [])),
            "transcript": data["snippet"].get("transcript", ""),
            "thumbnail_image_url": data["snippet"].get("thumbnails", {}).get("default", {}).get("url"),
        }
        return sdb_mapped

    @staticmethod
    def map_youtube_channel_data(data: dict) -> dict:
        """
        Map Youtube Data API Channel list response to SDB video response
        :param data:
        :return:
        """
        sdb_mapped = {
            "channel_id": data["id"],
            "title": data["snippet"]["title"],
            "url": "https://www.youtube.com/channel/" + data["snippet"].get("channelId", ""),
            "category": constants.VIDEO_CATEGORIES.get(data["snippet"].get("categoryId"), None),
            "description": data["snippet"].get("description", ""),
            "videos": data["statistics"].get("videoCount"),
            "views": data["statistics"].get("viewCount", 0),
            "country": data["snippet"].get("country", constants.UNKNOWN),
            "thumbnail_image_url": data["snippet"].get("thumbnails", {}).get("default", {}).get("url"),
            "tags": ",".join(data["snippet"].get("tags", [])),
            "subscribers": data.get("statistics", {}).get("subscriberCount"),
        }
        return sdb_mapped


class StandardAuditException(Exception):
    pass
