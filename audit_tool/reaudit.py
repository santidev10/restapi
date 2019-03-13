from audit_tool.segmented_audit import SegmentedAudit
from utils.youtube_api import YoutubeAPIConnector
from utils.youtube_api import YoutubeAPIConnectorException
import csv
import json
import re
import time
from multiprocessing import Pool
import langid

class Reaudit(SegmentedAudit):
    max_process_count = 4

    # video_chunk_size = 10000
    # channel_chunk_size = 1000

    # TEST
    video_chunk_size = 10000
    video_batch_size = 50000
    channel_batch_size = 10000
    channel_chunk_size = 10

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
        target, data_generator, chunk_size = self.get_executor()

        self.run_processor(target, data_generator, audit_type=self.audit_type, chunk_size=chunk_size)

    def get_executor(self):
        if self.audit_type == 'video':
            return self.process_videos, self.video_csv_data_generator, self.video_chunk_size

        elif self.audit_type == 'channel':
            return self.process_channels, self.channel_csv_generator, self.channel_chunk_size

        else:
            print('Audit type not supported: {}'.format(self.audit_type))

    def run_processor(self, target, generator, audit_type='video', chunk_size=5000):
        pool = Pool(processes=self.max_process_count)
        items_seen = 0

        # Channel batches = 1k, so every 1k channels data will write
        # Video batches = 10k, so every 10k videos will write
        for batch in generator():
            chunks = self.chunks(batch, chunk_size)
            results = pool.map(target, chunks)
            all_results = [item for sublist in results for item in sublist]

            items_seen += len(batch)

            if audit_type == 'video':
                self.write_data(all_results, audit_type)

            else:
                self.write_data(all_results['videos'], audit_type)
                self.write_data(all_results['channels']. audit_type)

            print('Seen {} {}s'.format(items_seen, self.audit_type))

    def process_videos(self):
        # video batch is 200k videos
        pool = Pool(processes=self.max_process_count)
        for video_batch in self.video_csv_data_generator():

            chunks = self.chunks(video_batch, 10000)

            results = pool.map(self.video_process, chunks)
            all_results = [item for sublist in results for item in sublist]

            print('Writing {} videos'.format(len(all_results)))

            self.write_data(all_results, data_type='video')

    def process_channels(self, csv_channels):
        print('Starting audit...')

        all_videos = []
        connector = YoutubeAPIConnector()

        for row in csv_channels:
            channel_id = self.channel_id_regexp.search(row[1]).group()
            channel_videos = self.get_channel_videos(channel_id, connector)
            all_videos += channel_videos

        video_audit_results = self.audit_videos(all_videos)
        channel_audit_results = self.audit_channels(video_audit_results, connector)

        final_results = {
            'videos': video_audit_results,
            'channels': channel_audit_results
        }

        return final_results

    def get_all_channel_video_data(self, channel_ids):
        all_results = []
        connector = YoutubeAPIConnector()

        for id in channel_ids:
            results = self.get_channel_videos(id, connector)
            all_results += results

        return all_results

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
        if audit_type == 'video':
            export_path = self.csv_export_dir + self.csv_export_title + 'Video.csv'
        else:
            export_path = self.csv_export_dir + self.csv_export_title + 'Channel.csv'

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

    def get_channel_export_row(self, channel):
        print(channel)
        metadata = channel['snippet']
        statistics = channel['statistics']

        export_row = [
            metadata['title'],
            channel['channel_id'],
            'http://www.youtube.com/channel/' + channel['channel_id'],
            statistics['viewCount'],
            statistics['subscriberCount'],
            statistics['videoCount'],
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

                if len(rows_batch) >= self.video_batch_size:
                    yield rows_batch
                    rows_batch.clear()

            yield rows_batch

    def channel_csv_generator(self):
        with open(self.csv_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            batch = []
            csv_reader = csv.reader(csv_file)

            for row in csv_reader:
                batch.append(row)

                if len(batch) >= self.channel_batch_size:
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
            if hits:
                self.set_video_language(video)
                self.set_keyword_hits(video, hits)

                # Only add English videos and add if category matches mapping
                if video['snippet']['defaultLanguage'] == 'en' and self.video_categories.get(video['snippet']['categoryId']):
                    results.append(video)

        end = time.time()

        print('Time auditing {} videos: {}'.format(len(videos), end - start))

        return results

    def audit_channels(self, videos, connector):
        channel_data = {}

        for video in videos:
            channel_id = video['snippet']['channelId']
            channel_data[channel_id] = channel_data.get(channel_id, {})
            channel_data[channel_id]['keyword_hits'] = channel_data[channel_id].get('keyword_hits', set()).update(video['snippet']['keyword_hits'])

        channel_ids = [channel_id for channel_id in channel_data.keys()]

        while channel_ids:
            batch = ','.join(channel_ids[:50])
            response = connector.obtain_channels(batch, part='snippet,statistics').get('items')

            for item in response:
                channel_id = item['id']
                channel_data[channel_id]['channel_id'] = channel_id
                channel_data[channel_id]['statistics'] = item['statistics']
                channel_data[channel_id]['metadata'] = item['snippet']

        return channel_data.values()

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
