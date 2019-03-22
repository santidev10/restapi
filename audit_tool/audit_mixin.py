from singledb.connector import SingleDatabaseApiConnector as Connector
from brand_safety.models import BadWord
from collections import Counter
import csv
import re
import langid
from . import audit_constants as constants

class AuditMixin(object):
    youtube_max_channel_list_limit = 50
    video_id_regexp = re.compile('(?<=video/).*')
    channel_id_regexp = re.compile('(?<=channel/).*')
    username_regexp = re.compile('(?<=user/).*')
    emoji_regexp = re.compile(u"["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               "]", flags=re.UNICODE)
    categories = {
        '1': 'Film & Animation',
        '2': 'Autos & Vehicles',
        '10': 'Music',
        '15': 'Pets & Animals',
        '17': 'Sports',
        '18': 'Short Movies',
        '19': 'Travel & Events',
        '20': 'Gaming',
        '21': 'Videoblogging',
        '22': 'People & Blogs',
        '23': 'Comedy',
        '24': 'Entertainment',
        '25': 'News & Politics',
        '26': 'Howto & Style',
        '27': 'Education',
        '28': 'Science & Technology',
        '29': 'Nonprofits & Activism',
        '30': 'Movies',
        '31': 'Anime / Animation',
        '32': 'Action / Adventure',
        '33': 'Classics',
        '34': 'Comedy',
        '37': 'Family',
        '42': 'Shorts',
        '43': 'Shows',
        '44': 'Trailers'
    }
    audit_keyword_hit_mapping = {
        constants.BRAND_SAFETY_FAIL: constants.BRAND_SAFETY_HITS,
        constants.WHITELIST: constants.WHITELIST_HITS,
        constants.BLACKLIST: constants.BLACKLIST_HITS
    }

    @staticmethod
    def read_and_create_keyword_regexp(csv_path):
        with open(csv_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)
            keywords = list(csv_reader)

            keyword_regexp = re.compile(
                '|'.join([word[0] for word in keywords]),
                re.IGNORECASE
            )

        return keyword_regexp

    @staticmethod
    def get_all_bad_words():
        bad_words_names = BadWord.objects.values_list("name", flat=True)
        bad_words_names = list(set(bad_words_names))

        return bad_words_names

    @staticmethod
    def compile_audit_regexp(keywords: list):
        """
        Compiles regular expression with given keywords
        :param keywords: List of keyword strings
        :return: Compiled Regular expression
        """
        regexp = re.compile(
            "({})".format("|".join([r"\b{}\b".format(re.escape(word)) for word in keywords]))
        )
        return regexp

    @staticmethod
    def update_video_channel_subscribers(videos, channels):
        """
        Uses channels data to update videos with their respective channel statistics
        :param videos: Youtube Data API objects
        :param channels: Youtube Data API objects with statistics data
        :return:
        """
        channel_subscribers = {
            channel['channelId']: channel['statistics']['subscriberCount']
            for channel in channels
        }

        for video in videos:
            channel_id = video['snippet']['channelId']
            video['statistics']['channelSubscriberCount'] = channel_subscribers[channel_id]

    @staticmethod
    def connector_get_channel_videos(connector: Connector, channel_ids: list, fields: str) -> list:
        """
        Retrieves all videos associated with channel_ids from Singledb
        :param connector: SingledbConnecctor instance
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
        response = connector.execute_get_call("videos/", params)

        return response.get('items')

    def audit_videos(self, videos: list, blacklist_regexp: re = None, whitelist_regexp: re = None) -> dict:
        """
        Audits videos and separates them depending on their audit result (brand safety, blacklist (optional), whitelist (optional)
            Sets keyword hits on each video object
        :param videos: (list) Youtubve video data
        :param blacklist_regexp: Compiled regular expression of blacklist keywords
        :param whitelist_regexp: Compiled regular expression of whitelist keywords
        :return: (dict) Video audit results
        """
        if not videos:
            return []

        results = {
            constants.WHITELIST_VIDEOS: [],
            constants.BLACKLIST_VIDEOS: [],
            constants.BRAND_SAFETY_FAIL_VIDEOS: [],
            constants.BRAND_SAFETY_PASS_VIDEOS: [],
        }

        for video in videos:
            self.set_language(video)
            self.set_has_emoji(video)

            # Only add English videos
            if video['snippet']['defaultLanguage'] != 'en':
                continue

            brand_safety_hits = self._parse_youtube_snippet(video, self.brand_safety_tags_regexp)
            if brand_safety_hits:
                self.set_keyword_hits(video, brand_safety_hits, constants.BRAND_SAFETY_HITS)
                results[constants.BRAND_SAFETY_FAIL_VIDEOS].append(video)

            else:
                results[constants.BRAND_SAFETY_PASS_VIDEOS].append(video)

            # If provided, more bad keywords to filter against
            blacklist_hits = []
            if blacklist_regexp:
                blacklist_hits = self._parse_youtube_snippet(video, blacklist_regexp)
                if blacklist_hits:
                    self.set_keyword_hits(video, blacklist_hits, constants.BLACKLIST_HITS)
                    results[constants.BLACKLIST_VIDEOS].append(video)

            # If whitelist keywords provided, keywords to filter for
            if not brand_safety_hits and not blacklist_hits and whitelist_regexp:
                whitelist_hits = set(self._parse_youtube_snippet(video, whitelist_regexp))

                if whitelist_hits:
                    self.set_keyword_hits(video, whitelist_hits, constants.WHITELIST_HITS)
                    results[constants.constants.WHITELIST_VIDEOS].append(video)

        return results

    def audit_channels(self, videos, youtube_connector):
        """
        Uses audited video data to extrapolate channel audit results
            Aggregates all video data and sets aggregated results on each channel object that exist in the videos
        :param videos: (list) Audited Youtube Videos
        :param youtube_connector: YoutubeAPIConnector instance
        :return: (list) Channel Youtube data with aggregated video audit results
        """
        if not videos:
            return []

        channel_data = {}

        for video in videos:
            channel_id = video['snippet']['channelId']
            channel_data[channel_id] = channel_data.get(channel_id, {})
            channel_data[channel_id]['aggregatedVideoData'] = channel_data[channel_id].get('aggregatedVideoData', {
                'totalLikes': 0,
                'totalDislikes': 0,
                constants.WHITELIST_HITS: [],
                constants.BLACKLIST_HITS: [],
                constants.BRAND_SAFETY_HITS: []
            })
            channel_data[channel_id]['aggregatedVideoData']['totalAuditedVideos'] = channel_data[channel_id][
                                                                                        'aggregatedVideoData'].get(
                'totalAuditedVideos', 0) + 1
            channel_data[channel_id]['aggregatedVideoData']['totalLikes'] += int(
                video['statistics'].get('likeCount', 0))
            channel_data[channel_id]['aggregatedVideoData']['totalDislikes'] += int(
                video['statistics'].get('dislikeCount', 0))

            channel_data[channel_id]['aggregatedVideoData'][constants.BRAND_SAFETY_HITS] += video.get(constants.BRAND_SAFETY_HITS, [])
            channel_data[channel_id]['aggregatedVideoData'][constants.BLACKLIST_HITS] += video.get(constants.BLACKLIST_HITS, [])
            channel_data[channel_id]['aggregatedVideoData'][constants.WHITELIST_HITS] += video.get(constants.WHITELIST_HITS, [])

            channel_data[channel_id]['categoryCount'] = channel_data[channel_id].get('categoryCount', [])
            channel_data[channel_id]['categoryCount'].append(video['snippet'].get('categoryId'))

        channel_ids = list(channel_data.keys())

        while channel_ids:
            batch = ','.join(channel_ids[:50])
            response = youtube_connector.obtain_channels(batch, part='snippet,statistics').get('items')

            for item in response:
                channel_id = item['id']
                self.set_language(item)
                channel_data[channel_id]['type'] = 'channel'
                channel_data[channel_id]['channelId'] = channel_id
                channel_data[channel_id]['statistics'] = item['statistics']
                channel_data[channel_id]['snippet'] = item['snippet']

            channel_ids = channel_ids[50:]

        return list(channel_data.values())

    def get_channel_statistics_with_video_data(self, channel_ids, connector):
        """
        Gets channel statistics for videos
        :param channel_ids: List of Youtube channel ids
        :return: (dict) Mapping of channels and their statistics
        """
        channel_data = []
        cursor = 0

        while True:
            if cursor >= len(channel_ids):
                break

            batch = channel_ids[cursor:self.youtube_max_channel_list_limit]
            response = connector.obtain_channels(','.join(batch), part='statistics')
            channel_data += response['items']
            cursor += len(batch)

        return channel_data

    @staticmethod
    def sort_channels_by_keyword_hits(channels: list):
        """
        Separate audited channels into audit categories for easier processing
        :param channels: (list) Audited Channel Youtube data
        :return: (dict) Sorted Channels based on on audit results
        """
        sorted_channels = {
            constants.BRAND_SAFETY_PASS_CHANNELS: [],
            constants.BRAND_SAFETY_FAIL_CHANNELS: [],
            constants.BLACKLIST_CHANNELS: [],
            constants.WHITELIST_CHANNELS: [],
        }

        for channel in channels:
            if not channel['aggregatedVideoData'].get(constants.BRAND_SAFETY_HITS):
                sorted_channels[constants.BRAND_SAFETY_PASS_CHANNELS].append(channel)

            if channel['aggregatedVideoData'].get(constants.BRAND_SAFETY_HITS):
                sorted_channels[constants.BRAND_SAFETY_FAIL_CHANNELS].append(channel)

            if channel['aggregatedVideoData'].get(constants.BLACKLIST_HITS):
                sorted_channels[constants.BLACKLIST_CHANNELS].append(channel)

            if not channel['aggregatedVideoData'].get(constants.BRAND_SAFETY_HITS) \
                    and not channel['aggregatedVideoData'].get(constants.BLACKLIST_HITS) \
                    and channel['aggregatedVideoData'].get(constants.WHITELIST_HITS):
                sorted_channels['whitelist_channels'].append(channel)

        return sorted_channels

    def set_has_emoji(self, item):
        """
        Sets boolean field for emoji existence
        :param item: Youtube Data API object
        :return: None
        """
        item['has_emoji'] = bool(self._parse_youtube_snippet(item, self.emoji_regexp))

    @staticmethod
    def set_language(item):
        """
        Sets defaultLanguage on Youtube object if none exists
        :param item: Youtube Data API object
        :return: None
        """
        lang = item['snippet'].get('defaultLanguage')

        if lang is None:
            text = item['snippet']['title'] + ' ' + item['snippet']['description']
            item['snippet']['defaultLanguage'] = langid.classify(text)[0].lower()

    @staticmethod
    def chunks(iterable, length):
        """
        Generator that yields equal sized lists
        """
        for i in range(0, len(iterable), length):
            yield iterable[i:i + length]

    @staticmethod
    def _parse_youtube_snippet(item, regexp):
        """
        Finds all matches of regexp in Youtube data object
        :param item: Youtube data
        :param regexp: Compiled regular expression to match
        :return:
        """
        item = item['snippet']

        text = ''
        text += item.get('title', '')
        text += item.get('description', '')
        text += item.get('channelTitle', '')
        text += item.get('transcript', '')

        if item.get('tags'):
            text += ' '.join(item['tags'])

        return re.findall(regexp, text)

    @staticmethod
    def get_keyword_count(keywords):
        """
        Counts occurrences of items in list
        :param keywords: List of keyword occurences
        :return: String formatted count e.g. "bad: 1, word: 2"
        """
        counted = Counter(keywords)
        return ', '.join(['{}: {}'.format(key, value) for key, value in counted.items()])

    @staticmethod
    def set_keyword_hits(item, hits, keyword_type):
        """
        Sets keyword hits on item with keyword_type key
        :param item: Dictionary instance
        :param hits: List of keyword hits
        :param keyword_type: Key for value hits
        :return: None
        """
        item[keyword_type] = hits

    @staticmethod
    def get_channel_id_for_username(username, connector):
        """
        Retrieves channel id for the given youtube username
        :param username: (str) youtube username
        :param connector: YoutubeAPIConnector instance
        :return: (str) channel id
        """
        response = connector.obtain_user_channels(username)

        try:
            channel_id = response.get('items')[0].get('id')

        except IndexError:
            raise ValueError('Could not get channel id for: {}'.format(username))

        return channel_id

    def get_all_channel_video_data(self, channel_ids: list, youtube_connector):
        """
        Gets all video metadata for each channel
        :param channel_ids: Youtube channel id strings
        :param youtube_connector: YoutubeAPIConnector instance
        :return: Youtube Video data
        """
        all_results = []

        for id in channel_ids:
            results = self.get_channel_videos(id, youtube_connector)
            all_results += results

        return all_results

    @staticmethod
    def get_channel_videos(channel_id, connector):
        """
        Retrieves all videos for given channel id from Youtube Data API
        :param channel_id: (str)
        :param connector: YoutubeAPIConnector instance
        :return: (list) Channel videos
        """
        channel_videos_full_data = []
        channel_videos = []
        response = connector.obtain_channel_videos(channel_id, part='snippet', order='viewCount', safe_search='strict')

        channel_videos += [video['id']['videoId'] for video in response.get('items')]
        next_page_token = response.get('nextPageToken')

        while next_page_token and response.get('items'):
            response = connector\
                .obtain_channel_videos(channel_id, part='snippet', page_token=next_page_token, order='viewCount', safe_search='strict')

            channel_videos += [video['id']['videoId'] for video in response.get('items')]
            next_page_token = response.get('nextPageToken')

        while channel_videos:
            video_full_data_batch = channel_videos[:50]

            response = connector.obtain_videos(','.join(video_full_data_batch), part='snippet,statistics')

            channel_videos_full_data += response.get('items')
            channel_videos = channel_videos[50:]

        return channel_videos_full_data

