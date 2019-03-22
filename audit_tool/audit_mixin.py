from singledb.connector import SingleDatabaseApiConnector as Connector
from brand_safety.models import BadWord
import csv
import re


class AuditMixin(object):
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

    def create_keyword_regexp(self, csv_path):
        with open(csv_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)
            keywords = list(csv_reader)

            keyword_regexp = re.compile(
                '|'.join([word[0] for word in keywords]),
                re.IGNORECASE
            )

        return keyword_regexp

    def get_all_bad_words(self):
        bad_words_names = BadWord.objects.values_list("name", flat=True)
        bad_words_names = list(set(bad_words_names))

        return bad_words_names

    def compile_audit_regexp(self, keywords: list):
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
        :param videos: (list) Youtubve video data
        :param blacklist_regexp: Compiled regular expression of blacklist keywords
        :param whitelist_regexp: Compiled regular expression of whitelist keywords
        :return: (dict) Video audit results
        """
        results = {
            'whitelist_videos': [],
            'blacklist_videos': [],
            'not_brand_safety_videos': [],
            'is_brand_safety_videos': [],
            'ignore_brand_safety_videos': []
        }

        for video in videos:
            self.set_language(video)
            self.set_has_emoji(video)

            # Only add English videos and add if category matches mapping
            if video['snippet']['defaultLanguage'] != 'en' and not self.video_categories.get(
                    video['snippet']['categoryId']):
                continue

            brand_safety_hits = self._parse_item(video, self.bad_words_regexp)
            if brand_safety_hits:
                self.set_keyword_hits(video, brand_safety_hits, 'brand_safety_hits')
                results['not_brand_safety_videos'].append(video)

            else:
                results['is_brand_safety_videos'].append(video)

            # If provided, more bad keywords to filter against
            blacklist_hits = []
            if blacklist_regexp:
                blacklist_hits = self._parse_item(video, blacklist_regexp)
                if blacklist_hits:
                    self.set_keyword_hits(video, blacklist_hits, 'blacklist_hits')
                    results['blacklist_videos'].append(video)

            # If whitelist keywords provided, keywords to filter for
            if not brand_safety_hits and not blacklist_hits and whitelist_regexp:
                whitelist_hits = set(self._parse_item(video, whitelist_regexp))

                if whitelist_hits:
                    self.set_keyword_hits(video, whitelist_hits, 'whitelist_hits')
                    results['whitelist_videos'].append(video)

        return results

    def audit_channels(self, videos, youtube_connector):
        """
        Uses audited video data to extrapolate channel audit results
        :param videos: (list) Audited Youtube Videos
        :param connector: YoutubeAPIConnector instance
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
                'whitelist_hits': [],
                'blacklist_hits': [],
                'brand_safety_hits': []
            })
            channel_data[channel_id]['aggregatedVideoData']['totalAuditedVideos'] = channel_data[channel_id][
                                                                                        'aggregatedVideoData'].get(
                'totalAuditedVideos', 0) + 1
            channel_data[channel_id]['aggregatedVideoData']['totalLikes'] += int(
                video['statistics'].get('likeCount', 0))
            channel_data[channel_id]['aggregatedVideoData']['totalDislikes'] += int(
                video['statistics'].get('dislikeCount', 0))

            channel_data[channel_id]['aggregatedVideoData']['brand_safety_hits'] += video.get('brand_safety_hits', [])
            channel_data[channel_id]['aggregatedVideoData']['blacklist_hits'] += video.get('blacklist_hits', [])
            channel_data[channel_id]['aggregatedVideoData']['whitelist_hits'] += video.get('whitelist_hits', [])

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

    def set_has_emoji(self, item):
        item['has_emoji'] = bool(self._parse_item(item, self.emoji_regexp))

    @staticmethod
    def set_language(item):
        lang = item['snippet'].get('defaultLanguage')

        if lang is None:
            text = item['snippet']['title'] + ' ' + item['snippet']['description']
            item['snippet']['defaultLanguage'] = langid.classify(text)[0].lower()

    @staticmethod
    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    @staticmethod
    def _parse_item(item, regexp):
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
    def set_keyword_hits(item, hits, keyword_type):
        item[keyword_type] = hits
