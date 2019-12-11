import logging
import re
from typing import Type

from audit_tool.models import ChannelAuditIgnore, AuditIgnoreModel
from audit_tool.models import VideoAuditIgnore
from brand_safety.models import BadWord
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent import PersistentSegmentRelatedVideo
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentTitles
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager
from es_components.query_builder import QueryBuilder
from es_components.constants import SortDirections

logger = logging.getLogger(__name__)


class SegmentedAudit:
    BATCH_SIZE = 10000
    CHANNELS_BATCH_LIMIT = 100

    BAD_WORDS_DATA_KEY = "__found_bad_words"
    AUDITED_VIDEOS_DATA_KEY = "__audited_videos"

    def __init__(self):
        self.bad_words_regexp = re.compile(
            "({})".format(
                "|".join(
                    [r"\b{}\b".format(re.escape(w)) for w in self.get_all_bad_words()]
                )
            )
        )

    def run(self):
        last_channel = PersistentSegmentRelatedChannel.objects.order_by("-updated_at").first()
        last_channel_id = last_channel.related_id if last_channel else None
        channels = [
            channel.to_dict()
            for channel in self.get_next_channels_batch(last_id=last_channel_id, limit=self.CHANNELS_BATCH_LIMIT)
        ]

        videos = [
            video.to_dict()
            for video in self.get_all_videos(channel_ids=[c.get("main").get("id") for c in channels])
        ]

        # parse videos
        found = [self._parse_video(video) for video in videos]
        for idx, video in enumerate(videos):
            video[self.BAD_WORDS_DATA_KEY] = found[idx]

        # group results from videos to channel
        channel_bad_words = {}
        channel_audited_videos = {}
        for video in videos:

            channel_id = video.get("channel").get("id")
            if channel_id not in channel_bad_words.keys():
                channel_bad_words[channel_id] = []
            channel_bad_words[channel_id] += video[self.BAD_WORDS_DATA_KEY]

            if channel_id not in channel_audited_videos.keys():
                channel_audited_videos[channel_id] = 0
            channel_audited_videos[channel_id] += 1

        # apply results to channels
        for channel in channels:
            channel[self.BAD_WORDS_DATA_KEY] = channel_bad_words.get(channel.get("main").get("id"), [])
            channel[self.AUDITED_VIDEOS_DATA_KEY] = channel_audited_videos.get(channel.get("main").get("id"), 0)

        # storing results
        self.store_channels(channels)
        self.store_videos(videos)

        return len(channels), len(videos)

    def get_next_channels_batch(self, last_id=None, limit=100):
        manager = ChannelManager()
        channels = manager.search(
            filters=QueryBuilder().build().must().range().field("main.id").lt(last_id or ""),
            sort=[{"main.id": {"order": SortDirections.ASCENDING}}],
            limit=limit
        ).\
            sources(includes=["main.id", "general_data.title", "general_data.description",
                              "general_data.thumbnail_image_url", "general_data.top_category",
                              "general_data.top_language", "stats.subscribers", "stats.likes", "stats.dislikes",
                              "stats.views"])

        for channel in channels:
            if not channel.general_data.top_category:
                channel.general_data.top_category = "Unclassified"
            if not channel.general_data.top_language:
                channel.general_data.top_language = "Unknown"

        if not channels:
            channels = self.get_next_channels_batch(limit=limit)

        return channels

    def get_all_videos(self, channel_ids):
        manager = VideoManager()

        fields_to_load = ["main.id", "channel.id", "general_data.title", "general_data.description",
                          "general_data.thumbnail_image_url", "general_data.category", "general_data.tags",
                          "general_data.language", "stats.subscribers", "stats.likes", "stats.dislikes",
                          "stats.views", "captions"]
        sort = [{"main.id": {"order": SortDirections.ASCENDING}}]
        filters = manager.by_channel_ids_query(channel_ids)
        offset = 0

        while True:
            videos = manager.search(filters=filters, sort=sort, limit=self.BATCH_SIZE, offset=offset)\
                .sources(includes=fields_to_load)

            if not videos:
                break

            for video in videos:
                if not video.general_data.category:
                    video.general_data.category = "Unknown"
                if not video.general_data.language:
                    video.general_data.language = "Unknown"
                yield video
            offset += self.BATCH_SIZE

    def get_all_bad_words(self):
        bad_words_names = BadWord.objects.values_list("name", flat=True)
        bad_words_names = list(set(bad_words_names))
        return bad_words_names

    def _parse_video(self, video):
        items = [
            video.get("general_data").get("title") or "",
            video.get("general_data").get("description") or "",
            video.get("general_data").get("tags") or "",
            video.get("captions")[0].text if video.get("captions") else "",
        ]
        text = " ".join(items)
        found = re.findall(self.bad_words_regexp, text)
        return found

    def _segment_category(self, item):
        category = PersistentSegmentCategory.WHITELIST
        language = item.get("general_data").get("language") or item.get("general_data").get("top_language")
        if item[self.BAD_WORDS_DATA_KEY] or language != "English":
            category = PersistentSegmentCategory.BLACKLIST
        return category

    def _video_details(self, video):
        details = dict(
            likes=video.get("stats").get("likes"),
            dislikes=video.get("stats").get("dislikes"),
            views=video.get("stats").get("views"),
            tags=video.get("general_data").get("tags"),
            description=video.get("general_data").get("description"),
            language=video.get("general_data").get("language"),
            bad_words=video[self.BAD_WORDS_DATA_KEY],
        )
        return details

    def _channel_details(self, channel):
        details = dict(
            subscribers=channel.get("stats").get("subscribers"),
            likes=channel.get("stats").get("likes"),
            dislikes=channel.get("stats").get("dislikes"),
            views=channel.get("stats").get("views"),
            tags=channel.get("general_data").get("tags"),
            description=channel.get("general_data").get("description"),
            language=channel.get("general_data").get("top_language"),
            bad_words=channel[self.BAD_WORDS_DATA_KEY],
            audited_videos=channel[self.AUDITED_VIDEOS_DATA_KEY],
        )
        return details

    def _store(self, items, segments_model, items_model, get_details):
        segments_manager = segments_model.objects
        items_manager = items_model.objects

        # group items by segments
        grouped_by_segment = {}
        for item in items:
            segment_category = self._segment_category(item)
            segment_type = segments_model.segment_type
            categorized_segment_title = "{}s {} {}".format(
                segment_type.capitalize(),
                item.get("general_data").get("category") or item.get("general_data").get("top_category"),
                segment_category.capitalize(),
            )
            master_segment_title = dict(dict(PersistentSegmentTitles.TITLES_MAP)[segment_type])[segment_category]
            for segment_title in [categorized_segment_title, master_segment_title]:
                if segment_title not in grouped_by_segment:
                    segment, _ = segments_manager.get_or_create(title=segment_title, category=segment_category)
                    grouped_by_segment[segment_title] = (segment, [])  # segment, items
                grouped_by_segment[segment_title][1].append(item)

        master_segments = [
            segment
            for segment, _ in grouped_by_segment.values()
            if segment.title in PersistentSegmentTitles.ALL_MASTER_SEGMENT_TITLES
        ]

        no_audit_segment_ids = segments_manager.filter(title__in=PersistentSegmentTitles.NO_AUDIT_SEGMENTS) \
            .values_list("id", flat=True)

        # store to segments
        for segment, items in grouped_by_segment.values():
            all_ids = [item.get("main").get("id") for item in items]
            old_ids = items_manager.filter(segment=segment, related_id__in=all_ids) \
                .values_list("related_id", flat=True)
            new_ids = set(all_ids) - set(old_ids)
            # save new items to relevant segment
            new_items = [
                items_manager.model(
                    segment=segment,
                    related_id=item.get("main").get("id"),
                    category=item.get("general_data").get("category") or item.get("general_data").get("top_category"),
                    title=item.get("general_data").get("title"),
                    thumbnail_image_url=item.get("general_data").get("thumbnail_image_url"),
                    details=get_details(item),
                )
                for item in items if item.get("main").get("id") in new_ids
            ]
            items_manager.bulk_create(new_items)

            # Remove new items from irrelevant segments
            if segment in master_segments:
                # remove from master segments
                items_manager.filter(segment__in=master_segments) \
                    .exclude(segment=segment) \
                    .exclude(segment_id__in=no_audit_segment_ids) \
                    .filter(related_id__in=new_ids) \
                    .delete()
            else:
                # remove from categorized segments
                items_manager.exclude(segment__in=master_segments + [segment]) \
                    .exclude(segment_id__in=no_audit_segment_ids) \
                    .filter(related_id__in=new_ids) \
                    .delete()

    def _filter_manual_items(self, model: Type[AuditIgnoreModel], items):
        ids_to_ignore = model.objects.all() \
            .values_list("id", flat=True)
        return list(filter(lambda item: item.get("main").get("id") not in ids_to_ignore, items))

    def store_videos(self, videos):
        logger.info("store videos")
        videos = self._filter_manual_items(VideoAuditIgnore, videos)
        self._store(
            items=videos,
            segments_model=PersistentSegmentVideo,
            items_model=PersistentSegmentRelatedVideo,
            get_details=self._video_details,
        )

    def store_channels(self, channels):
        logger.info("store channels")
        channels = self._filter_manual_items(ChannelAuditIgnore, channels)
        self._store(
            items=channels,
            segments_model=PersistentSegmentChannel,
            items_model=PersistentSegmentRelatedChannel,
            get_details=self._channel_details,
        )
