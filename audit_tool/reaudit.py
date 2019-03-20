from audit_tool.segmented_audit import SegmentedAudit
from utils.youtube_api import YoutubeAPIConnector
from utils.youtube_api import YoutubeAPIConnectorException
import csv
import re
from multiprocessing import Pool
import langid
import datetime
from collections import Counter

class Reaudit(SegmentedAudit):
    max_process_count = 5

    video_chunk_size = 10000
    video_batch_size = 30000
    channel_batch_size = 1000
    channel_chunk_size = 1
    channel_row_data = {}

    video_csv_headers = ['Title', 'Category', 'Video URL', 'Language', 'View Count',
                         'Like Count', 'Dislike Count', 'Comment Count', 'Channel Title', 'Channel URL', 'Channel Subscribers', 'Keyword Hits']

    channel_csv_headers = ['Title', 'Channel URL', 'Language', 'Category', 'Subscribers',
                           'Total Video Views', 'Total Audited Videos', 'Total Likes', 'Total Dislikes', 'AO Check 3/15. To keep? (Y/N)' 'AO Assigned']

    def __init__(self, *args, **kwargs):
        super().__init__()

        # self.youtube_connector = YoutubeAPIConnector()
        self.audit_type = kwargs.get('type')
        self.csv_file_path = kwargs.get('file')
        self.csv_export_dir = kwargs.get('export')
        self.csv_export_title = kwargs.get('title')
        self.csv_keyword_path = kwargs.get('keywords')
        self.categories = {
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
        self.video_categories = {
            '1': 'Film & Animation',
            '10': 'Music',
            '15': 'Pets & Animals',
            '23': 'Comedy',
            '24': 'Entertainment',
            '26': 'Howto & Style',
            '27': 'Education',
            '34': 'Comedy',
            '37': 'Family',
        }

        self.video_id_regexp = re.compile('(?<=video/).*')
        self.channel_id_regexp = re.compile('(?<=channel/).*')
        self.whitelist_regexp = self.create_keyword_regexp(self.csv_keyword_path) if self.csv_keyword_path else None
        self.more_bad_words = self.create_keyword_regexp(kwargs['badwords']) if kwargs.get('badwords') else None
        self.custom_csv = kwargs.get('custom_csv')
        self.brand_safety = kwargs.get('brand_safety')

        # # Write csv headers
        # with open(self.video_export_path, mode='w') as csv_file:
        #     writer = csv.writer(csv_file)
        #     writer.writerow(self.video_csv_headers)
        #
        # if self.audit_type == 'channel':
        #     # Write csv headers
        #     with open(self.channel_export_path, mode='w') as csv_file:
        #         writer = csv.writer(csv_file)
        #         writer.writerow(self.channel_csv_headers)


    def create_keyword_regexp(self, csv_path):
        with open(csv_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)
            keywords = list(csv_reader)

            keyword_regexp = re.compile(
                '|'.join([word[0] for word in keywords]),
                re.IGNORECASE
            )

        return keyword_regexp

    def run(self):
        print('starting...')
        if self.custom_csv:
            self.get_channel_row_data_mapping()

        target, data_generator, result_processor, chunk_size = self.get_executor()

        self.run_processor(target, data_generator, result_processor, chunk_size=chunk_size)

    def get_executor(self):
        if self.audit_type == 'video':
            return self.process_videos, self.video_csv_data_generator, self.process_video_results, self.video_chunk_size

        elif self.audit_type == 'channel':
            return self.process_channels, self.channel_csv_generator, self.process_channel_results, self.channel_chunk_size

        else:
            print('Audit type not supported: {}'.format(self.audit_type))

    def run_processor(self, target, generator, result_processor, chunk_size=5000):
        pool = Pool(processes=self.max_process_count)
        items_seen = 0

        # Channel batches = 1k, so every 1k channels data will write
        # Video batches = 10k, so every 10k videos will write
        for batch in generator():
            chunks = self.chunks(batch, chunk_size)
            all_results = pool.map(target, chunks)

            result_processor(all_results)

            items_seen += len(batch)
            print('Seen {} {}s'.format(items_seen, self.audit_type))

        print('Complete!')

    def process_video_results(self, results):
        self.write_data(results, 'video', audit_type='whitelist')

    def process_channel_results(self, results: list):
        """
        Results is list of dictionary results from all processes
            result.keys = ['whitelist_videos', 'blacklist_videos', 'whitelist_channels', 'blacklist_channels',
                'not_brand_safety_videos', 'not_brand_safety_channels']
        :param results:
        :return:
        """
        all_not_brand_safety_videos = []
        all_blacklist_videos = []
        all_whitelist_videos = []
        all_not_brand_safety_channels = []
        all_blacklist_channels = []
        all_whitelist_channels = []

        for result in results:
            all_not_brand_safety_videos += result['not_brand_safety_videos']
            all_blacklist_videos += result['blacklist_videos']
            all_whitelist_videos += result['whitelist_videos']

            all_not_brand_safety_channels += result['not_brand_safety_channels']
            all_whitelist_channels += result['whitelist_channels']
            all_blacklist_channels += result['blacklist_channels']

        self.write_data(all_not_brand_safety_videos, data_type='video', audit_type='brand_safety')
        self.write_data(all_blacklist_videos, data_type='video', audit_type='blacklist')
        self.write_data(all_whitelist_videos, data_type='video', audit_type='whitelist')
        self.write_data(all_not_brand_safety_channels, data_type='channel', audit_type='brand_safety')
        self.write_data(all_blacklist_channels, data_type='channel', audit_type='blacklist')
        self.write_data(all_whitelist_channels, data_type='channel', audit_type='whitelist')

    def process_videos(self, csv_videos):
        all_videos = []
        connector = YoutubeAPIConnector()

        while csv_videos:
            batch = csv_videos[:50]
            video_channel_subscriber_ref = {
                video.get('video_id'): video.get('channel_subscribers')
                for video in batch
            }
            batch_ids = ','.join([video.get('video_id') for video in batch])
            response = connector.obtain_videos(batch_ids, part='snippet,statistics').get('items')

            for video in response:
                video['statistics']['channelSubscriberCount'] = video_channel_subscriber_ref[video['id']]

            all_videos += response
            csv_videos = csv_videos[50:]

        audit_results = self.audit_videos(all_videos)

        return audit_results

    def process_channels(self, csv_channels):
        """
        Processes videos and aggregates video data to audit channels
            final_results stores the results of both channel audit and video audit
        :param csv_channels:
        :return:
        """
        final_results = {}
        all_videos = []
        connector = YoutubeAPIConnector()

        for row in csv_channels:
            channel_id = self.channel_id_regexp.search(row.get('channel_url')).group()
            channel_videos = self.get_channel_videos(channel_id, connector)
            all_videos += channel_videos

        # audit_videos func sets keyword hits key on each video and returns sorted videos
        video_audit_results = self.audit_videos(all_videos)

        all_video_audit_results = sum(video_audit_results.values(), [])

        # audit_channels aggregates all the videos for each channels
        channel_audit_results = self.audit_channels(all_video_audit_results, connector)

        self.update_video_channel_subscribers(all_video_audit_results, channel_audit_results)

        # sort channels based on their video keyword hits
        sorted_channels = self.sort_channels_by_keyword_hits(channel_audit_results)

        final_results.update(sorted_channels)
        final_results.update(video_audit_results)

        return final_results

    @staticmethod
    def update_video_channel_subscribers(videos, channels):
        # Create dictionary of channel to subscriber count
        channel_subscribers = {
            channel['channelId']: channel['statistics']['subscriberCount']
            for channel in channels
        }

        for video in videos:
            channel_id = video['snippet']['channelId']
            video['statistics']['channelSubscriberCount'] = channel_subscribers[channel_id]

    def get_all_channel_video_data(self, channel_ids):
        all_results = []
        connector = YoutubeAPIConnector()

        for id in channel_ids:
            results = self.get_channel_videos(id, connector)
            all_results += results

        return all_results

    def write_data(self, data, data_type='video', audit_type='whitelist'):
        export_path = '{dir}{title}{data_type}{audit_type}{time}.csv'.format(
            dir=self.csv_export_dir,
            title=self.csv_export_title,
            data_type=data_type,
            audit_type=audit_type,
            time=str(datetime.datetime.now())
        )

        with open(export_path, mode='a') as export_file:
            writer = csv.writer(export_file, delimiter=',')

            for item in data:
                row = self.get_video_export_row(item, audit_type) if data_type == 'video' else self.get_channel_export_row(item, audit_type)
                writer.writerow(row)

    def get_video_export_row(self, video, audit_type, csv_data={}):
        metadata = video['snippet']
        statistics = video['statistics']

        audit_types = {
            'brand_safety': 'brand_safety_hits',
            'whitelist': 'whitelist_hits',
            'blacklist': 'blacklist_hits',
        }

        export_row = [
            metadata['title'],
            self.categories.get(metadata['categoryId']),
            'http://www.youtube.com/video/' + video['id'],
            metadata.get('defaultLanguage'),
            statistics.get('viewCount', 0),
            statistics.get('likeCount', 0),
            statistics.get('dislikeCount', 0),
            statistics.get('commentCount', 0),
            metadata.get('channelTitle'),
            'http://www.youtube.com/channel/' + metadata['channelId'],
            csv_data.get('channel_subscribers') or statistics['channelSubscriberCount'],
            self.get_count(video.get(audit_types.get(audit_type), []))
        ]

        return export_row

    def sort_channels_by_keyword_hits(self, channels):
        sorted_channels = {
            'not_brand_safety_channels': [],
            'blacklist_channels': [],
            'whitelist_channels': [],
        }

        for channel in channels:
            if channel['aggregatedVideoData'].get('brand_safety_hits'):
                sorted_channels['not_brand_safety_channels'].append(channel)

            if channel['aggregatedVideoData'].get('blacklist_hits'):
                sorted_channels['blacklist_channels'].append(channel)

            if not channel['aggregatedVideoData'].get('brand_safety_hits') \
                    and not channel['aggregatedVideoData'].get('blacklist_hits') \
                    and channel['aggregatedVideoData'].get('whitelist_hits'):
                sorted_channels['whitelist_channels'].append(channel)

        return sorted_channels

    def get_channel_export_row(self, channel, audit_type='whitetlistt'):
        channel_id = channel['channelId']
        metadata = channel['snippet']
        statistics = channel['statistics']
        video_data = channel['aggregatedVideoData']
        channel_category_id = max(channel['categoryCount'], key=channel['categoryCount'].count)

        export_row = [
            metadata['title'],
            'http://www.youtube.com/channel/' + channel_id,
            metadata['defaultLanguage'],
            self.categories[channel_category_id],
            statistics['videoCount'],
            statistics['subscriberCount'],
            statistics['viewCount'],
            video_data['totalAuditedVideos'],
            video_data['totalLikes'],
            video_data['totalDislikes'],
        ]

        if audit_type == 'brand_safety':
            export_row.append(self.get_count(video_data.get('brand_safety_hits', [])))

        elif audit_type == 'whitelist':
            export_row.append(self.get_count(video_data.get('whitelist_hits', [])))

        elif audit_type == 'blacklist':
            export_row.append(self.get_count(video_data.get('blacklist_hits', [])))

        if self.custom_csv and self.channel_row_data.get(channel_id):
            aocheck, aoassigned = self.channel_row_data[channel_id]
            export_row.extend([aocheck, aoassigned])

        return export_row

    def video_csv_data_generator(self):
        with open(self.csv_file_path, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            # Pass over headers
            next(csv_reader, None)

            rows_batch = []

            for row in csv_reader:
                video_id = self.video_id_regexp.search(row.pop('url')).group()
                # rows_batch[video_id] = row
                row['video_id'] = video_id
                rows_batch.append(row)

                if len(rows_batch) >= self.video_batch_size:
                    yield rows_batch
                    rows_batch.clear()

            yield rows_batch

    def channel_csv_generator(self):
        with open(self.csv_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            batch = []
            csv_reader = csv.DictReader(csv_file)

            # Pass over headers
            next(csv_reader, None)

            for row in csv_reader:
                batch.append(row)

                if len(batch) >= self.channel_batch_size:
                    yield batch
                    batch.clear()

            yield batch

    def get_channel_row_data_mapping(self,):
        with open(self.csv_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row in csv_reader:
                if row.get('aocheck'):
                    channel_id = self.channel_id_regexp.search(row.get('channel_url')).group()
                    self.channel_row_data[channel_id] = (row.get('aocheck'), row.get('aoassigned'))

    def get_channel_videos(self, channel_id, connector):
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

    @staticmethod
    def get_count(items):
        counted = Counter(items)
        return ', '.join(['{}: {}'.format(key, value) for key, value in counted.items()])

    def audit_videos(self, videos):
        results = {
            'whitelist_videos': [],
            'blacklist_videos': [],
            'not_brand_safety_videos': [],
        }

        for video in videos:
            self.set_language(video)
            # Only add English videos and add if category matches mapping
            if video['snippet']['defaultLanguage'] != 'en' and not self.video_categories.get(
                    video['snippet']['categoryId']):
                continue

            brand_safety_hits = self._parse_video(video, self.bad_words_regexp)
            if brand_safety_hits:
                self.set_keyword_hits(video, brand_safety_hits, 'brand_safety_hits')
                results['not_brand_safety_videos'].append(video)

            # If provided, more bad keywords to filter against
            if self.more_bad_words:
                blacklist_hits = self._parse_video(video, self.more_bad_words)
                if blacklist_hits:
                    self.set_keyword_hits(video, blacklist_hits, 'blacklist_hits')
                    results['blacklist_videos'].append(video)

            # If whitelist keywords provided, keywords to filter for
            if not brand_safety_hits and not blacklist_hits and self.whitelist_regexp:
                whitelist_hits = set(self._parse_video(video, self.whitelist_regexp))

                if whitelist_hits:
                    self.set_keyword_hits(video, whitelist_hits, 'whitelist_hits')
                    results['whitelist_videos'].append(video)

        return results

    def audit_channels(self, videos, connector):
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
            channel_data[channel_id]['aggregatedVideoData']['totalAuditedVideos'] = channel_data[channel_id]['aggregatedVideoData'].get('totalAuditedVideos', 0) + 1
            channel_data[channel_id]['aggregatedVideoData']['totalLikes'] += int(video['statistics'].get('likeCount', 0))
            channel_data[channel_id]['aggregatedVideoData']['totalDislikes'] += int(video['statistics'].get('dislikeCount', 0))

            channel_data[channel_id]['aggregatedVideoData']['brand_safety_hits'] += video.get('brand_safety_hits', [])
            channel_data[channel_id]['aggregatedVideoData']['blacklist_hits'] += video.get('blacklist_hits', [])
            channel_data[channel_id]['aggregatedVideoData']['whitelist_hits'] += video.get('whitelist_hits', [])

            channel_data[channel_id]['categoryCount'] = channel_data[channel_id].get('categoryCount', [])
            channel_data[channel_id]['categoryCount'].append(video['snippet'].get('categoryId'))


        channel_ids = list(channel_data.keys())

        while channel_ids:
            batch = ','.join(channel_ids[:50])
            response = connector.obtain_channels(batch, part='snippet,statistics').get('items')

            for item in response:
                channel_id = item['id']
                self.set_language(item)
                channel_data[channel_id]['type'] = 'channel'
                channel_data[channel_id]['channelId'] = channel_id
                channel_data[channel_id]['statistics'] = item['statistics']
                channel_data[channel_id]['snippet'] = item['snippet']

            channel_ids = channel_ids[50:]

        return list(channel_data.values())

    def get_channel_data(self, channel_ids):
        pass

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
    def set_keyword_hits(video, hits, keyword_type):
        video[keyword_type] = hits

    @staticmethod
    def _parse_video(video, regexp):
        video = video.get('snippet')
        tags = video.get('tags')

        text = ''
        text += video.get("title ", '')
        text += video.get("description ", '')
        text += video.get("transcript ", '')

        if tags:
            text += ' '.join(tags)

        return re.findall(regexp, text)
