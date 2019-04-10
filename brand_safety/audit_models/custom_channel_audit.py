from collections import defaultdict
from collections import Counter

from brand_safety.audit_models.base import Audit
from brand_safety import constants


class ChannelAudit(Audit):
    video_fail_limit = 3
    subscribers_threshold = 1000
    brand_safety_hits_threshold = 1

    def __init__(self, video_audits, audit_types, channel_data, source=constants.YOUTUBE):
        self.results = {}
        self.source = source
        self.video_audits = video_audits
        self.audit_types = audit_types
        # self.metadata = self.get_metadata(channel_data, source)
        self.metadata = channel_data
        self.update_aggregate_video_audit_data()

    @property
    def pk(self):
        pk = self.metadata["channel_id"]
        return pk

    def get_channel_videos_failed(self):
        videos_failed = 0
        for video in self.video_audits:
            if video["results"].get(constants.BRAND_SAFETY):
                videos_failed += 1
            if videos_failed >= self.video_fail_limit:
                return True
        return False

    def get_metadata(self, channel_data):
        text = channel_data["snippet"].get("title", "") + channel_data["snippet"].get("description", "")
        metadata = {
            "channel_title": channel_data["snippet"].get("title", ""),
            "channel_url": "https://www.youtube.com/channel/" + channel_data["id"],
            "language": self.get_language(channel_data),
            "category": channel_data["snippet"].get("category", constants.UNKNOWN),
            "description": channel_data["snippet"].get("description", ""),
            "videos": channel_data["statistics"].get("videoCount", constants.DISABLED),
            "subscribers": channel_data["statistics"].get("subscriberCount", constants.DISABLED),
            "views": channel_data["statistics"].get("viewCount", constants.DISABLED),
            "audited_videos": len(self.video_audits),
            "has_emoji": self.detect_emoji(text),
            "likes": channel_data["statistics"].get("likeCount", constants.DISABLED),
            "dislikes": channel_data["statistics"].get("dislikeCount", constants.DISABLED),
            "country": channel_data["snippet"].get("country", constants.UNKNOWN),
        }
        return metadata

    def get_text(self):
        metadata = self.metadata
        text = ""
        text += metadata["title"]
        text += metadata["description"]
        return text

    def run_audit(self):
        for video in self.video_audits:
            for key, audit in self.audit_types.items():
                self.results[key] = self.results.get(key, [])
                self.results[key].extend(video.results[key])

    def update_aggregate_video_audit_data(self):
        video_audits = self.video_audits
        # First get audit data
        aggregated_data = {
            "likes": 0,
            "dislikes": 0,
            "category": [],
        }
        for video in video_audits:
            # Preserve "Disabled" value for videos if not initially given in api response
            try:
                aggregated_data["likes"] += int(video.metadata["likes"])
            except ValueError:
                pass

            try:
                aggregated_data["dislikes"] += int(video.metadata["dislikes"])
            except ValueError:
                pass

            aggregated_data["category"].append(video.metadata["category"])
            # Try to update the video"s country with current channel"s country
            if video.metadata["country"] == "Unknown":
                video.metadata["country"] = self.metadata["country"]

            for audit, regexp in self.audit_types.items():
                aggregated_data[audit] = aggregated_data.get(audit, [])
                aggregated_data[audit].extend(video.results[audit])
        try:
            aggregated_data["category"] = Counter(aggregated_data["category"]).most_common()[0][0]

        except IndexError:
            aggregated_data["category"] = "Unknown"

        self.metadata.update(aggregated_data)

    # def run_standard_audit(self):
    #     brand_safety_failed = self.results.get(constants.BRAND_SAFETY) \
    #                           and len(self.results[constants.BRAND_SAFETY]) > self.brand_safety_hits_threshold
    #     channel_videos_failed = self.get_channel_videos_failed()
    #     subscribers = self.metadata["subscribers"] if self.metadata["subscribers"] is not constants.DISABLED else 0
    #     failed_standard_audit = brand_safety_failed \
    #                             and channel_videos_failed \
    #                             and subscribers > self.subscribers_threshold
    #     return failed_standard_audit

    def get_export_row(self, audit_type=constants.BRAND_SAFETY):
        """
        Formats exportable csv row using object metadata
        :param audit_type:
        :return:
        """
        row = [
            self.metadata["channel_title"],
            self.metadata["channel_url"],
            self.metadata["language"],
            self.metadata["category"],
            self.metadata["videos"],
            self.metadata["subscribers"],
            self.metadata["views"],
            self.metadata["audited_videos"],
            self.metadata["likes"],
            self.metadata["dislikes"],
            self.metadata["country"],
            self.get_keyword_count(self.results[audit_type])
        ]
        return row

