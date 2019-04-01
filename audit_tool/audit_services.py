import re
from collections import Counter

import langid

from . import audit_constants as constants


class AuditService(object):
    def __init__(self, audit_types):
        self.audit_keyword_hit_mapping = constants.AUDIT_KEYWORD_MAPPING
        self.set_audits(audit_types)
        self.audit_types = audit_types

    def set_audits(self, audit_types):
        for audit in audit_types:
            if audit:
                setattr(self, audit['type'], audit['regexp'])


class StandardAuditService(AuditService):
    def __init__(self, audit_types, video_audit, channel_audit):
        super().__init__(audit_types)
        self.VideoAudit = video_audit
        self.ChannelAudit = video_audit

    def audit_videos(self, video_objs=None, channel_objs=None):
        video_audits = []
        for video in video_objs:
            video_audit = self.V


class YoutubeAuditService(AuditService):
    def __init__(self, audit_types, youtube, sdb):
        super().__init__(audit_types)
        self.youtube_data_provider = youtube()
        self.sdb_connector = sdb()

    def connector_get_channel_videos(self, channel_ids: list, fields: str) -> list:
        """
        Retrieves all videos associated with channel_ids from Singledb
        :param channel_ids: Channel id strings
        :param fields: Video fields to retrieve
        :return: video objects from Singledb
        """
        params = dict(
            fields=fields,
            sort="video_id",
            size=10000,
            channel_id__terms=",".join(channel_ids),
        )
        response = self.sdb_connector.execute_get_call("videos/", params)
        return response.get('items')

    def audit_videos(self, video_ids=None, channel_ids=None):
        video_youtube_data = self.youtube_data_provider.get_video_data(video_ids) if video_ids else \
            self.youtube_data_provider.get_channel_video_data(channel_ids)
        all_video_audits = []
        for video in video_youtube_data:
            video_audit = VideoAudit(video, self.audit_types)
            video_audit.run_audit()
            all_video_audits.append(video_audit)
        return all_video_audits

    def audit_channels(self, video_audits):
        all_channel_audits = []
        sorted_channel_data = self.sort_video_audits(video_audits)
        channel_ids = list(sorted_channel_data.keys())

        # sorted_channel_data is dict of channel ids with their video audits
        channel_youtube_data = self.youtube_data_provider.get_channel_data(channel_ids)
        for channel in channel_youtube_data:
            channel_video_audits = sorted_channel_data[channel['id']]
            channel_audit = ChannelAudit(channel_video_audits, self.audits, channel)
            channel_audit.run_audit()
            all_channel_audits.append(channel_audit)
        return all_channel_audits

    @staticmethod
    def sort_video_audits(video_audits):
        channel_videos = {}
        for video in video_audits:
            channel_id = video.metadata['channel_id']
            channel_videos[channel_id] = channel_videos.get(channel_id, [])
            channel_videos[channel_id].append(video)
        return channel_videos

    @staticmethod
    def parse_video(youtube_data, regexp):
        text = ''
        text += youtube_data['snippet'].get('title', '')
        text += youtube_data['snippet'].get('description', '')
        text += youtube_data['snippet'].get('channelTitle', '')
        found = re.search(regexp, text)
        return found


class Audit(object):
    emoji_regexp = re.compile(
        u"["
            u"\U0001F600-\U0001F64F"  # emoticons
            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
            u"\U0001F680-\U0001F6FF"  # transport & map symbols
            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "]", flags=re.UNICODE
    )
    
    def audit(self, regexp):
        """
        Finds all matches of regexp in Youtube data object
        :param regexp: Compiled regular expression to match
        :return:
        """
        metadata = self.metadata
        text = ''
        text += metadata.get('title', '')
        text += metadata.get('description', '')
        text += metadata.get('channelTitle', '')
        text += metadata.get('channel_title', '')
        text += metadata.get('transcript', '')

        if metadata.get('tags'):
            text += ' '.join(metadata['tags'])
        hits = re.findall(regexp, text)
        return hits

    def set_keyword_terms(self, keywords, attribute):
        setattr(self, attribute, keywords)

    def get_metadata(self, data, source):
        if source == constants.YOUTUBE:
            metadata = self.get_youtube_metadata(data)
        elif source == constants.SDB:
            metadata = self.get_sdb_metadata(data)
        else:
            raise ValueError('Source type {} unsupported.'.format(source))
        return metadata

    @staticmethod
    def get_language(data):
        language = data['snippet'].get('defaultLanguage', None)
        if language is None:
            text = data['snippet'].get('title', '') + data['snippet'].get('description', '')
            language = langid.classify(text)[0].lower()
        return language

    def get_export_row(self, audit_type=constants.BRAND_SAFETY):
        # Remove previously used metadata from export
        row = dict(**self.metadata)
        row.pop('channel_id', None)
        row.pop('video_id', None)
        row.pop('id', None)
        row.pop('tags', None)
        row.pop('transcript', None)
        row = list(row.values())
        audit_hits = self.get_keyword_count(self.results[audit_type])
        row.append(audit_hits)
        return row

    def detect_emoji(self, text):
        has_emoji = bool(re.search(self.emoji_regexp, text))
        return has_emoji

    @staticmethod
    def get_keyword_count(items):
        counted = Counter(items)
        return ', '.join(['{}: {}'.format(key, value) for key, value in counted.items()])


class VideoAudit(Audit):
    dislike_ratio_audit_threshold = 0.2
    views_audit_threshold = 1000
    brand_safety_unique_threshold = 2
    brand_safety_hits_threshold = 3

    def __init__(self, data, audit_types, source=constants.YOUTUBE):
        self.audit_types = audit_types
        self.metadata = self.get_metadata(data, source)
        self.results = {}

    def get_youtube_metadata(self, data):
        text = data['snippet'].get('title', '') + data['snippet'].get('description', '')
        metadata = {
            'channel_title': data['snippet'].get('channelTitle', ''),
            'channel_url': 'https://www.youtube.com/channel/' + data['snippet'].get('channelId', ''),
            'channelSubscribers': data.get('statistics', {}).get('channelSubscriberCount'),
            'video_title': data['snippet']['title'],
            'video_url': 'https://www.youtube.com/video/' + data['id'],
            'has_emoji': self.detect_emoji(text),
            'views': data['statistics'].get('viewCount', constants.DISABLED),
            'description': data['snippet'].get('description', ''),
            'category': constants.VIDEO_CATEGORIES.get(data['snippet'].get('categoryId'), constants.DISABLED),
            'language': self.get_language(data),
            'country': data['snippet'].get('country', constants.UNKNOWN),
            'likes': data['statistics'].get('likeCount', constants.DISABLED),
            'dislikes': data['statistics'].get('dislikeCount', constants.DISABLED),
            'channel_id': data['snippet'].get('channelId', ''),
            'tags': data['snippet'].get('tags', []),
            'video_id': data['id'],
            'transcript': data['snippet'].get('transcript', ''),
        }
        return metadata

    def get_sdb_metadata(self, data):
        text = (data['title'] or '') + (data['description'] or '')
        metadata = {
            'channel_title': data['snippet'].get('channelTitle', ''),
            'channel_url': 'https://www.youtube.com/channel/' + data['channel']['id'],
            'channelSubscribers': data.get('statistics', {}).get('channelSubscriberCount'),
            'video_title': data['title'],
            'video_url': 'https://www.youtube.com/video/' + data['id'],
            'has_emoji': self.detect_emoji(text),
            'views': data['views'],
            'description': data['description'],
            'category': data['category'],
            'language': data['language'],
            'country': data['snippet'].get('country', constants.UNKNOWN),
            'likes': data['likes'],
            'dislikes': data['dislikes'],
            'channel_id': data['snippet'].get('channelId', ''),
            'tags': data['snippet'].get('tags', []),
            'video_id': data['id'],
            'transcript': data['transcript'],
        }
        return metadata

    def run_audit(self):
        for audit in self.audit_types:
            hits = self.audit(audit['regexp'])
            self.results[audit['type']] = hits

    def get_language(self, data):
        language = data['snippet'].get('defaultLanguage', None)
        if language is None:
            text = data['snippet'].get('title', '') + data['snippet'].get('description', '')
            language = langid.classify(text)[0].lower()
        return language

    def get_dislike_ratio(self):
        likes = self.metadata['likes'] if self.metadata['likes'] is not constants.DISABLED else 0
        dislikes = self.metadata['dislikes'] if self.metadata['dislikes'] is not constants.DISABLED else 0
        try:
            ratio = dislikes / (likes + dislikes)
        except ZeroDivisionError:
            ratio = 0
        return ratio

    def run_standard_audit(self):
        brand_safety_counts = self.results.get(constants.BRAND_SAFETY)
        brand_safety_failed = brand_safety_counts \
            and (
                    len(brand_safety_counts.keys() >= self.brand_safety_hits_threshold)
                    or any(brand_safety_counts[keyword] > self.brand_safety_hits_threshold for keyword in brand_safety_counts)
                )
        dislike_ratio = self.get_dislike_ratio()
        views = self.metadata['views'] if self.metadata['views'] is not constants.DISABLED else 0
        failed_standard_audit = dislike_ratio > self.dislike_ratio_audit_threshold \
            and views > self.views_audit_threshold \
            and brand_safety_failed
        return failed_standard_audit


class ChannelAudit(Audit):
    video_fail_limit = 3
    subscribers_threshold = 1000
    brand_safety_hits_threshold = 1

    def __init__(self, video_audits, audit_types, channel_data):
        self.results = {}
        self.video_audits = video_audits
        self.audit_types = audit_types
        self.metadata = self.get_metadata(channel_data)
        self.update_aggregate_video_audit_data()

    def get_channel_videos_failed(self):
        videos_failed = 0
        for video in self.video_audits:
            if video['results'].get(constants.BRAND_SAFETY):
                videos_failed += 1
            if videos_failed >= self.video_fail_limit:
                return True
        return False

    def get_youtube_metadata(self, channel_data):
        text = channel_data['snippet'].get('title', '') + channel_data['snippet'].get('description', '')
        metadata = {
            'channel_title': channel_data['snippet'].get('title', ''),
            'channel_url': 'https://www.youtube.com/channel/' + channel_data['id'],
            'language': self.get_language(channel_data),
            'category': channel_data['snippet'].get('category', constants.UNKNOWN),
            'description': channel_data['snippet'].get('description', ''),
            'videos': channel_data['statistics'].get('videoCount', constants.DISABLED),
            'subscribers': channel_data['statistics'].get('subscriberCount', constants.DISABLED),
            'views': channel_data['statistics'].get('viewCount', constants.DISABLED),
            'audited_videos': len(self.video_audits),
            'has_emoji': self.detect_emoji(text),
            'likes': channel_data['statistics'].get('likeCount', constants.DISABLED),
            'dislikes': channel_data['statistics'].get('dislikeCount', constants.DISABLED),
            'country': channel_data['snippet'].get('country', constants.UNKNOWN),
        }
        return metadata

    def get_sdb_metadata(self, channel_data):
        text = (channel_data['title'] or '') + (channel_data['description'] or '')
        metadata = {
            'channel_title': channel_data['title'],
            'channel_url': channel_data['url'],
            'language': channel_data['language'],
            'category': channel_data['category'],
            'description': channel_data['description'],
            'videos': channel_data['statistics'].get('videoCount', constants.DISABLED),
            'subscribers': channel_data['statistics'].get('subscriberCount', constants.DISABLED),
            'views': channel_data['views'],
            'audited_videos': len(self.video_audits),
            'has_emoji': self.detect_emoji(text),
            'likes': channel_data['likes'],
            'dislikes': channel_data['dislikes'],
            'country': channel_data['country'],
        }
        return metadata

    def run_audit(self):
        for video in self.video_audits:
            for audit in self.audit_types:
                audit_type = audit['type']
                self.results[audit_type] = self.results.get(audit_type, [])
                self.results[audit_type].extend(video.results[audit_type])

    def update_aggregate_video_audit_data(self):
        video_audits = self.video_audits
        # First get audit data
        aggregated_data = {
            'likes': 0,
            'dislikes': 0,
            'category': [],
        }
        for video in video_audits:
            # Preserve "Disabled" value for videos if not initially given in api response
            try:
                aggregated_data['likes'] += int(video.metadata['likes'])
            except ValueError:
                pass

            try:
                aggregated_data['dislikes'] += int(video.metadata['dislikes'])
            except ValueError:
                pass

            aggregated_data['category'].append(video.metadata['category'])
            # Try to update the video's country with current channel's country
            if video.metadata['country'] == 'Unknown':
                video.metadata['country'] = self.metadata['country']

            for audit in self.audit_types:
                audit_type = audit['type']
                aggregated_data[audit_type] = aggregated_data.get(audit_type, [])
                aggregated_data[audit_type].extend(video.results[audit_type])
        try:
            aggregated_data['category'] = Counter(aggregated_data['category']).most_common()[0][0]

        except IndexError:
            aggregated_data['category'] = 'Unknown'

        self.metadata.update(aggregated_data)

    def run_standard_audit(self):
        brand_safety_failed = self.results.get(constants.BRAND_SAFETY) \
                              and len(self.results[constants.BRAND_SAFETY]) > self.brand_safety_hits_threshold
        channel_videos_failed = self.get_channel_videos_failed()
        subscribers = self.metadata['subscribers'] if self.metadata['subscribers'] is not constants.DISABLED else 0
        failed_standard_audit = brand_safety_failed \
            and channel_videos_failed \
            and subscribers > self.subscribers_threshold
        return failed_standard_audit
