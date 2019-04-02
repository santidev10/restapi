import re
from collections import Counter

import langid

import audit_tool.audit_constants as constants


class Audit(object):
    emoji_regexp = re.compile(
        u"["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "]", flags=re.UNICODE
    )

    def execute(self):
        """
        Executes the required audit function defined by the original source data type
        :return:
        """
        audit_sources = {
            constants.SDB: self.run_standard_audit,
            constants.YOUTUBE: self.run_custom_audit,
        }
        try:
            audit_executor = audit_sources[self.source]
            audit_executor()
        except KeyError:
            raise ValueError('Unsupported data {} type.'.format(self.source))

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
        """
        Analyzes metadata for language using langid module
        :param data: Youtube data
        :return: Language code
        """
        language = data['snippet'].get('defaultLanguage', None)
        if language is None:
            text = data['snippet'].get('title', '') + data['snippet'].get('description', '')
            language = langid.classify(text)[0].lower()
        return language

    def get_export_row(self, audit_type=constants.BRAND_SAFETY):
        """
        Formats exportable csv row using object metadata
            Removes unused metadata before export
        :param audit_type:
        :return:
        """
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
        self.source = source
        self.metadata = self.get_metadata(data, source)
        self.results = {}

    def get_youtube_metadata(self, data):
        """
        Extract Youtube video metadata
        :param data: Youtube video data
        :return: Dictionary of formatted metadata
        """
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
        """
        Extarct Single DB (SDB) Video object metadata
        :param data: SDB video object
        :return: Dictionary of formatted metadata
        """
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

    def run_custom_audit(self):
        """
        Executes audit method on all existing audit types
        :return:
        """
        for audit in self.audit_types:
            hits = self.audit(audit['regexp'])
            self.results[audit['type']] = hits

    def get_language(self, data):
        """
        Parses metadata to detect language using langid
        :param data:
        :return:
        """
        language = data['snippet'].get('defaultLanguage', None)
        if language is None:
            text = data['snippet'].get('title', '') + data['snippet'].get('description', '')
            language = langid.classify(text)[0].lower()
        return language

    def get_dislike_ratio(self):
        """
        Calculate Youtube dislike to like ratio
        :return:
        """
        likes = self.metadata['likes'] if self.metadata['likes'] is not constants.DISABLED else 0
        dislikes = self.metadata['dislikes'] if self.metadata['dislikes'] is not constants.DISABLED else 0
        try:
            ratio = dislikes / (likes + dislikes)
        except ZeroDivisionError:
            ratio = 0
        return ratio

    def run_standard_audit(self):
        pass

    def calculate_brand_safety_score(self):
        """
        Calculate brand safety score total and across categories
        :return: tuple -> (int) total score, (dict) scores by category
        """
        brand_safety_hits = self.results[constants.BRAND_SAFETY]
        category_scores = {}
        overall_score = 0
        for keyword in brand_safety_hits:
            keyword_category = self.brand_safety_category_mapping[keyword]['category']
            keyword_score = self.brand_safety_category_mapping[keyword]['score']
            category_scores[keyword_category] = category_scores.get(keyword_category, {})
            category_scores[keyword_category][keyword] = category_scores[keyword_category].get(keyword, {})
            category_scores[keyword_category][keyword]['hits'] += 1
            category_scores[keyword_category][keyword]['score'] += keyword_score
            overall_score += keyword_score
        return overall_score, category_scores


    # def run_standard_audit(self):
    #     brand_safety_counts = self.results.get(constants.BRAND_SAFETY)
    #     brand_safety_failed = brand_safety_counts \
    #                           and (
    #                                   len(brand_safety_counts.keys() >= self.brand_safety_hits_threshold)
    #                                   or any(
    #                               brand_safety_counts[keyword] > self.brand_safety_hits_threshold for keyword in
    #                               brand_safety_counts)
    #                           )
    #     dislike_ratio = self.get_dislike_ratio()
    #     views = self.metadata['views'] if self.metadata['views'] is not constants.DISABLED else 0
    #     failed_standard_audit = dislike_ratio > self.dislike_ratio_audit_threshold \
    #                             and views > self.views_audit_threshold \
    #                             and brand_safety_failed
    #     return failed_standard_audit


class ChannelAudit(Audit):
    video_fail_limit = 3
    subscribers_threshold = 1000
    brand_safety_hits_threshold = 1

    def __init__(self, video_audits, audit_types, channel_data, source=constants.YOUTUBE):
        self.results = {}
        self.source = source
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

    def run_custom_audit(self):
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

