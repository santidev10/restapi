from audit_tool.topic_audit import TopicAudit
from audit_tool.segmented_audit import SegmentedAudit
from utils.youtube_api import YoutubeAPIConnector
from utils.youtube_api import YoutubeAPIConnectorException
import csv
import json
import re
import time
from multiprocessing import Pool
from multiprocessing import Lock
from threading import Lock as ThreadLock
import langid

# python manage.py reaudit --file /Users/kennethoh/Desktop/custom_audit/VIQ-1326_videos_positive_phase1.csv

class Reaudit(SegmentedAudit):
    # Limits the number of videos retrieved from youtube api; can not exceed
    video_batch_limit = 50
    channel_batch_limit = 50
    export_batch_limit = 5000
    export_channel_limit = 50
    max_process_count = 3
    lock = Lock()
    thread_lock = ThreadLock()
    max_pool_size = 20
    channel_video_retrieve_batch_size = 500
    get_channel_videos_chunk_size = 50
    audit_batch_size = 3000

    video_chunk_size = 10000
    channel_chunk_size = 1000

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
        self.keyword_regexp = self.create_keyword_regexp(self.csv_keyword_path)
        self.reverse_csv = None

        # with open(csv_export_path, mode='w') as export_file:
        #     writer = csv.writer(export_file, delimiter=',')
        #     writer.writerow(self.export_fields)

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
        target, data_generator, chunk_size = self.get_executor(self.audit_type)

        self.run_processor(target, data_generator, audit_type=self.audit_type, chunk_size=chunk_size)

    def get_executor(self):
        if self.audit_type == 'video':
            return self.process_videos, self.video_csv_data_generator, self.video_chunk_size

        elif self.audit_type == 'channel':
            return self.process_channels, self.channel_csv_generator, self.channel_chunk_size

        else:
            print('Audit type not supported: {}'.format(self.audit_type))

    def process_videos(self):
        # video batch is 200k videos
        pool = Pool(processes=self.max_process_count)
        for video_batch in self.video_csv_data_generator():

            chunks = self.chunks(video_batch, 10000)

            results = pool.map(self.video_process, chunks)
            all_results = [item for sublist in results for item in sublist]

            print('Writing {} videos'.format(len(all_results)))

            self.write_data(all_results, data_type='video')

    def run_processor(self, target, generator, audit_type='video', chunk_size=5000):
        pool = Pool(processes=self.max_process_count)

        # Channel batches = 1k, so every 1k channels data will write
        # Video batches = 10k, so every 10k videos will write
        for batch in generator():
            chunks = self.chunks(batch, chunk_size)
            results = pool.map(target, chunks)
            all_results = [item for sublist in results for item in sublist]

            self.write_data(all_results, audit_type)

    def process_channels(self, channel_ids):
        print('Starting audit...')

        all_videos = []
        connector = YoutubeAPIConnector()

        for channel_id in channel_ids:
            channel_videos = self.get_channel_videos(channel_id, connector)
            all_videos += channel_videos

        # audit videos
        audit_results = self.audit_videos(all_videos)

        return audit_results

    def get_all_channel_video_data(self, channel_ids):
        all_results = []
        connector = YoutubeAPIConnector()

        for id in channel_ids:
            results = self.get_channel_videos(id, connector)
            all_results += results

        return all_results

    def video_process(self, videos):
        connector = YoutubeAPIConnector()
        video_data = self.get_video_data(videos, connector)
        audit_results = self.audit_videos(video_data)

        return audit_results

    def get_video_data(self, videos, connector):
        all_results = []

        while videos:
            batch = ','.join([video.get('video_id') for video in videos[:50]])
            response = connector.obtain_videos(batch, part='snippet,statistics').get('items')
            all_results += response

            videos = videos[50:]

        print('videos retrieved', len(all_results))

        return all_results

    def write_data(self, data, audit_type='video'):
        export_path = self.csv_export_dir + self.csv_export_title + '.csv'
        with open(export_path, mode='a') as export_file:
            writer = csv.writer(export_file, delimiter=',')

            for item in data:
                if audit_type == 'video':
                    row = self.get_video_export_row(item)
                else:
                    row = self.get_channel_export_row(item)

                writer.writerow(row)

    def get_video_export_row(self, video, csv_data={}):
        metadata = video['snippet']
        statistics = video['statistics']

        export_row = [
            metadata['title'],
            self.categories.get(metadata['categoryId']),
            'http://www.youtube.com/video/' + video['id'],
            metadata.get('defaultLanguage'),
            metadata.get('thumbnails', {}).get('standard', {}).get('url', ''),
            metadata.get('channelTitle'),
            'http://www.youtube.com/channel/' + metadata['channelId'],
            statistics.get('viewCount', ''),
            statistics.get('likeCount', ''),
            statistics.get('dislikeCount', ''),
            statistics.get('commentCount', ''),
            csv_data.get('channel_subscribers'),
            ','.join(video['snippet']['keyword_hits'])
        ]

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

                if len(rows_batch) >= 50000:
                    yield rows_batch
                    rows_batch.clear()

            yield rows_batch

    def channel_csv_generator(self):
        with open(self.csv_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            batch = []
            csv_reader = csv.DictReader(csv_file)

            # Pass over headers
            next(csv_reader)

            for row in csv_reader:
                batch.append(row)

                if len(batch) >= 20000:
                    yield batch
                    batch.clear()

            yield batch

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
        # with self.lock:
        #     shared_results += channel_videos_full_data

    def audit_videos(self, videos):
        start = time.time()
        results = []

        for video in videos:
            # Continue over bad words
            if self._parse_video(video, self.bad_words_regexp):
                continue

            hits = set(self._parse_video(video, self.keyword_regexp))

            self.set_video_language(video)

            if hits:
                self.set_video_language(video)
                self.set_keyword_hits(video, hits)

                # Only add English videos and add if category matches mapping
                # print(video['snippet']['defaultLanguage'], self.video_categories.get(video['snippet']['categoryId']))
                if video['snippet']['defaultLanguage'] == 'en' and self.video_categories.get(video['snippet']['categoryId']):
                    results.append(video)

        end = time.time()

        print('Time auditing {} videos: {}'.format(len(videos), end - start))

        return results

    def audit_channels(self, videos, connector):
        channels = {}

        for video in videos:
            channel_id = video['snippet']['channelId']
            channels[channel_id] = channels.get(channel_id, {})
            channels[channel_id]['keyword_hits'] = channels[channel_id].get('keyword_hits', set()).update(video['snippet']['keyword_hits'])

        channel_ids = [channel_id for channel_id in channels.keys()]

        while channel_ids:
            batch = ','.join(channel_ids[:50])
            response = connector.obtain_channels(batch, part='snippet,statistics').get('items')

            for item in response:
                channel_id = item['id']
                channels[channel_id]



    def get_channel_data(self, channel_ids):
        pass
    @staticmethod
    def set_video_language(video):
        lang = video['snippet'].get('defaultLanguage')

        if lang is None:
            text = video['snippet']['title'] + ' ' + video['snippet']['description']
            video['snippet']['defaultLanguage'] = langid.classify(text)[0].lower()

    @staticmethod
    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    @staticmethod
    def set_keyword_hits(video, hits):
        video['snippet']['keyword_hits'] = hits

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
