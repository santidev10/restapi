import re
from collections import Counter

import langid

from singledb.connector import SingleDatabaseApiConnector as Connector
from .youtube_data_provider import YoutubeDataProvider
from . import audit_constants as constants


class AuditService(object):
    audit_keyword_hit_mapping = {
        constants.BRAND_SAFETY_FAIL: constants.BRAND_SAFETY_HITS,
        constants.WHITELIST: constants.WHITELIST_HITS,
        constants.BLACKLIST: constants.BLACKLIST_HITS
    }

    def __init__(self):
        self.youtube_data_provider = YoutubeDataProvider()
        self.sdb_connector = Connector()

    def set_audits(self, audits):
        for audit in audits:
            if audit:
                setattr(self, audit['type'], audit['regexp'])

        self.audits = audits

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
            video_audit = VideoAudit(video, self.audits)

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
    emoji_regexp = re.compile(u"["
                              u"\U0001F600-\U0001F64F"  # emoticons
                              u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                              u"\U0001F680-\U0001F6FF"  # transport & map symbols
                              u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                              "]", flags=re.UNICODE)
    
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

        row = list(row.values())
        audit_hits = self.get_keyword_count(self.results[audit_type])
        row.append(audit_hits)

        return row

    def detect_emoji(self, youtube_data):
        metadata = youtube_data['snippet']
        text = metadata.get('title', '') + metadata.get('description', '')

        has_emoji = bool(re.search(self.emoji_regexp, text))
        return has_emoji

    @staticmethod
    def get_keyword_count(items):
        counted = Counter(items)
        return ', '.join(['{}: {}'.format(key, value) for key, value in counted.items()])


class VideoAudit(Audit):
    def __init__(self, data, audits):
        self.audits = audits
        self.metadata = self.get_metadata(data)
        self.results = {}

    def get_metadata(self, data):
        metadata = {
            'channel_title': data['snippet'].get('channelTitle', ''),
            'channel_url': 'https://www.youtube.com/channel/' + data['snippet'].get('channelId', ''),
            'channelSubscribers': data.get('statistics', {}).get('channelSubscriberCount'),
            'title': data['snippet']['title'],
            'video_url': 'https://www.youtube.com/video/' + data['id'],
            'has_emoji': self.detect_emoji(data),
            'views': data['statistics'].get('viewCount', 'Disabled'),
            'description': data['snippet'].get('description', ''),
            'category': constants.VIDEO_CATEGORIES.get(data['snippet'].get('categoryId'), 'Unknown'),
            'language': self.get_language(data),
            'country': data['snippet'].get('country', 'Unknown'),
            'likes': data['statistics'].get('likeCount', 'Disabled'),
            'dislikes': data['statistics'].get('dislikeCount', 'Disabled'),
            'channel_id': data['snippet'].get('channelId', ''),
            'tags': data['snippet'].get('tags', []),
            'video_id': data['id'],
        }

        return metadata

    def run_audit(self):
        for audit in self.audits:
            hits = self.audit(audit['regexp'])
            self.results[audit['type']] = hits

    def get_language(self, data):
        language = data['snippet'].get('defaultLanguage', None)

        if language is None:
            text = data['snippet'].get('title', '') + data['snippet'].get('description', '')
            language = langid.classify(text)[0].lower()

        return language


class ChannelAudit(Audit):
    def __init__(self, video_audits, audits, channel_data):
        self.results = {}
        self.video_audits = video_audits
        self.audits = audits
        self.metadata = self.get_metadata(channel_data)
        self.update_aggregate_video_audit_data()

    def get_metadata(self, channel_data):
        metadata = {
            'channel_title': channel_data['snippet'].get('title', ''),
            'channel_url': 'https://www.youtube.com/channel/' + channel_data['id'],
            'language': self.get_language(channel_data),
            'category': channel_data['snippet'].get('category', 'Unknown'),
            'description': channel_data['snippet'].get('description', ''),
            'videos': channel_data['statistics'].get('videoCount', 'Disabled'),
            'subscribers': channel_data['statistics'].get('subscriberCount', 'Disabled'),
            'views': channel_data['statistics'].get('viewCount', 'Disabled'),
            'audited_videos': len(self.video_audits),
            'has_emoji': self.detect_emoji(channel_data),
            'likes': channel_data['statistics'].get('likeCount', 'Disabled'),
            'dislikes': channel_data['statistics'].get('dislikeCount', 'Disabled'),
            'country': channel_data['snippet'].get('country', 'Unknown'),
        }
        return metadata

    def run_audit(self):
        for video in self.video_audits:
            for audit in self.audits:
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

            for audit in self.audits:
                audit_type = audit['type']
                aggregated_data[audit_type] = aggregated_data.get(audit_type, [])
                aggregated_data[audit_type].extend(video.results[audit_type])

        try:
            aggregated_data['category'] = Counter(aggregated_data['category']).most_common()[0][0]

        except IndexError:
            aggregated_data['category'] = 'Unknown'

        self.metadata.update(aggregated_data)
