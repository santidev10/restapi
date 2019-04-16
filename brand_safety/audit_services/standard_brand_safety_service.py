from brand_safety import constants
from brand_safety.audit_services.base import AuditService
from brand_safety.audit_models.brand_safety_channel_audit import BrandSafetyChannelAudit
from brand_safety.audit_models.brand_safety_video_audit import BrandSafetyVideoAudit
from singledb.connector import SingleDatabaseApiConnector
from utils.data_providers.sdb_data_provider import SDBDataProvider


class StandardBrandSafetyService(AuditService):
    """
    Interface for consuming source data from providers and driving brand safety logic
    """
    sdb_batch_limit = 10000
    video_audits_sorted = False
    video_fields = "video_id,title,channel_id,channel__title,channel__subscribers,description," \
                   "tags,category,likes,dislikes,views,language,transcript,country,thumbnail_image_url"
    channel_fields = "channel_id,title,description,category,subscribers,likes,dislikes,views,language,url,country,thumbnail_image_url"

    def __init__(self, audit_types, score_mapping, brand_safety_score_multiplier, logger=None):
        super().__init__(audit_types)
        self.sdb_connector = SingleDatabaseApiConnector()
        self.sdb_data_provider = SDBDataProvider()
        self.score_mapping = score_mapping
        self.brand_safety_score_multiplier = brand_safety_score_multiplier
        self.logger = logger

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
            audit = BrandSafetyVideoAudit(
                video,
                self.audit_types,
                source=constants.SDB,
                score_mapping=self.score_mapping,
                brand_safety_score_multiplier=self.brand_safety_score_multiplier,
            )
            audit.run_audit()
            video_audits.append(audit)
        self.video_audits_sorted = True
        return video_audits

    def audit_channels(self, sorted_video_audits):
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
            channel_audit = BrandSafetyChannelAudit(
                video_audits,
                self.audit_types,
                channel_data,
                source=constants.SDB,
                score_mapping=self.score_mapping,
                brand_safety_score_multiplier=self.brand_safety_score_multiplier
            )
            channel_audit.run_audit()
            channel_audits.append(channel_audit)
        return channel_audits

    @staticmethod
    def gather_brand_safety_results(video_audits):
        """
        Maps audits to their brand safety scores
        :param video_audits: Video Audit objects
        :return: list -> brand safety score dictionaries
        """
        results = []
        for audit in video_audits:
            brand_safety_score = audit.es_repr()
            results.append(brand_safety_score)
        return results

    @staticmethod
    def gather_video_brand_safety_results(video_audits):
        """
        Maps audits to their brand safety scores
        :param video_audits: Video Audit objects
        :return: list -> brand safety score dictionaries
        """
        results = []
        for audit in video_audits:
            # brand_safety_score = audit.brand_safety_score
            # # Map categories field from collections.Counter instance to dictionary
            # brand_safety_score["categories"] = dict(brand_safety_score["categories"])
            brand_safety_score = audit.es_repr()
            results.append(brand_safety_score)
        return results

    @staticmethod
    def gather_channel_brand_safety_results(channel_audits):
        """
        Maps audits to their brand safety scores
        :param channel_audits: Channel Audit objects
        :return: list -> brand safety score dictionaries
        """
        results = []
        for audit in channel_audits:
            brand_safety_score = audit.brand_safety_score
            # Map categories field from collections.Counter instance to dictionary
            brand_safety_score["categories"] = dict(brand_safety_score["categories"])
            results.append(brand_safety_score)
        return results

    def get_sorted_channel_data(self, channel_ids):
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
        response = self.sdb_connector.get_video_list(params)
        sorted_channel_data = {
            channel["channel_id"]: channel
            for channel in response["items"]
        }
        return sorted_channel_data

    def get_channel_video_data(self, channel_ids):
        params = dict(
            fields=self.video_fields,
            sort="video_id",
            size=self.sdb_batch_limit,
            channel_id__terms=",".join(channel_ids),
        )
        response = self.sdb_connector.get_video_list(params)
        video_data = response.get("items", [])
        return video_data


class StandardAuditException(Exception):
    pass
