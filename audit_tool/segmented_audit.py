import re
from typing import Type

from audit_tool.models import ChannelAuditIgnore, AuditIgnoreModel
from audit_tool.models import VideoAuditIgnore
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent import PersistentSegmentRelatedVideo
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentTitles
from singledb.connector import SingleDatabaseApiConnector as Connector


class SegmentedAudit:
    BATCH_SIZE = 10000
    CHANNELS_BATCH_LIMIT = 100

    BAD_WORDS_DATA_KEY = "__found_bad_words"
    AUDITED_VIDEOS_DATA_KEY = "__audited_videos"

    def __init__(self):
        self.connector = Connector()

        bad_words = self.get_all_bad_words()
        self.bad_words_regexp = re.compile(
            "({})".format("|".join([r"\b{}\b".format(re.escape(w)) for w in bad_words]))
        )

    def run(self):
        last_channel = PersistentSegmentRelatedChannel.objects.order_by("-updated_at").first()
        last_channel_id = last_channel.related_id if last_channel else None
        channels = self.get_next_channels_batch(last_id=last_channel_id, limit=self.CHANNELS_BATCH_LIMIT)
        channel_ids = [c["channel_id"] for c in channels]
        videos = list(self.get_all_videos(channel_ids=channel_ids))

        # parse videos
        found = [self._parse_video(video) for video in videos]
        for idx, video in enumerate(videos):
            video[self.BAD_WORDS_DATA_KEY] = found[idx]

        # group results from videos to channel
        channel_bad_words = {}
        channel_audited_videos = {}
        for video in videos:

            channel_id = video["channel_id"]
            if channel_id not in channel_bad_words.keys():
                channel_bad_words[channel_id] = []
            channel_bad_words[channel_id] += video[self.BAD_WORDS_DATA_KEY]

            if channel_id not in channel_audited_videos.keys():
                channel_audited_videos[channel_id] = 0
            channel_audited_videos[channel_id] += 1

        # apply results to channels
        for channel in channels:
            channel[self.BAD_WORDS_DATA_KEY] = channel_bad_words.get(channel["channel_id"], [])
            channel[self.AUDITED_VIDEOS_DATA_KEY] = channel_audited_videos.get(channel["channel_id"], 0)

        # storing results
        self.store_channels(channels)
        self.store_videos(videos)

        return len(channels), len(videos)

    def get_next_channels_batch(self, last_id=None, limit=100):
        size = limit + 1 if last_id else limit
        params = dict(
            fields="channel_id,title,description,thumbnail_image_url,category,subscribers,likes,dislikes,views,language",
            sort="channel_id",
            size=size,
            channel_id__range="{},".format(last_id or ""),
        )

        response = self.connector.get_channel_list(params, True)
        channels = [item for item in response.get("items", []) if item["channel_id"] != last_id]

        for channel in channels:
            if not channel.get("category"):
                channel["category"] = "Unclassified"
            if not channel.get("language"):
                channel["language"] = "Unknown"

        if not channels:
            channels = self.get_next_channels_batch(limit=limit)

        return channels

    def get_all_videos(self, channel_ids):
        last_id = None
        params = dict(
            fields="video_id,channel_id,title,description,tags,thumbnail_image_url,category,likes,dislikes,views,"
                   "language,transcript",
            sort="video_id",
            size=self.BATCH_SIZE,
            channel_id__terms=",".join(channel_ids),
        )
        while True:
            params["video_id__range"] = "{},".format(last_id or "")
            response = self.connector.get_video_list(query_params=params)
            videos = [item for item in response.get("items", []) if item["video_id"] != last_id]
            if not videos:
                break
            for video in videos:
                if video["video_id"] == last_id:
                    continue
                if not video.get("category"):
                    video["category"] = "Unknown"
                if not video.get("language"):
                    video["language"] = "Unknown"
                yield video
            last_id = videos[-1]["video_id"]

    def get_all_bad_words(self):
        bad_words = self.connector.get_bad_words_list({})
        bad_words_names = [item["name"] for item in bad_words]
        bad_words_names = list(set(bad_words_names))
        return bad_words_names

    def _parse_video(self, video):
        items = [
            video.get("title") or "",
            video.get("description") or "",
            video.get("tags") or "",
            video.get("transcript") or "",
        ]
        text = " ".join(items)
        found = re.findall(self.bad_words_regexp, text)
        return found

    def _segment_category(self, item):
        category = PersistentSegmentCategory.WHITELIST
        if item[self.BAD_WORDS_DATA_KEY] or item["language"] != "English":
            category = PersistentSegmentCategory.BLACKLIST
        return category

    def _video_details(self, video):
        details = dict(
            likes=video["likes"],
            dislikes=video["dislikes"],
            views=video["views"],
            tags=video["tags"],
            description=video["description"],
            language=video["language"],
            bad_words=video[self.BAD_WORDS_DATA_KEY],
        )
        return details

    def _channel_details(self, channel):
        details = dict(
            subscribers=channel["subscribers"],
            likes=channel["likes"],
            dislikes=channel["dislikes"],
            views=channel["views"],
            language=channel["language"],
            bad_words=channel[self.BAD_WORDS_DATA_KEY],
            audited_videos=channel[self.AUDITED_VIDEOS_DATA_KEY],
        )
        return details

    def _store(self, items, segments_model, items_model, id_field_name, get_details):
        segments_manager = segments_model.objects
        items_manager = items_model.objects

        # group items by segments
        grouped_by_segment = {}
        for item in items:
            segment_category = self._segment_category(item)
            segment_type = segments_model.segment_type
            categorized_segment_title = "{}s {} {}".format(
                segment_type.capitalize(),
                item["category"],
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

        no_audit_segment_ids = segments_manager.filter(title__in=PersistentSegmentTitles.NO_AUDIT_SEGMENTS)\
                                               .values_list("id", flat=True)

        # store to segments
        for segment, items in grouped_by_segment.values():
            all_ids = [item[id_field_name] for item in items]
            old_ids = items_manager.filter(segment=segment, related_id__in=all_ids) \
                .values_list("related_id", flat=True)
            new_ids = set(all_ids) - set(old_ids)
            # save new items to relevant segment
            new_items = [
                items_manager.model(
                    segment=segment,
                    related_id=item[id_field_name],
                    category=item["category"],
                    title=item["title"],
                    thumbnail_image_url=item["thumbnail_image_url"],
                    details=get_details(item),
                )
                for item in items if item[id_field_name] in new_ids
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
        return list(filter(lambda item: item["id"] not in ids_to_ignore, items))

    def store_videos(self, videos):
        videos = self._filter_manual_items(VideoAuditIgnore, videos)
        self._store(
            items=videos,
            segments_model=PersistentSegmentVideo,
            items_model=PersistentSegmentRelatedVideo,
            id_field_name="video_id",
            get_details=self._video_details,
        )

    def store_channels(self, channels):
        channels = self._filter_manual_items(ChannelAuditIgnore, channels)
        self._store(
            items=channels,
            segments_model=PersistentSegmentChannel,
            items_model=PersistentSegmentRelatedChannel,
            id_field_name="channel_id",
            get_details=self._channel_details,
        )
