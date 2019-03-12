from audit_tool.topic_audit import TopicAudit
from audit_tool.segmented_audit import SegmentedAudit
from utils.youtube_api import YoutubeAPIConnector
from utils.youtube_api import YoutubeAPIConnectorException
import csv
import json
import re
import time
from multiprocessing import Queue
from multiprocessing import Process
from multiprocessing import Manager
from multiprocessing import Lock

# python manage.py reaudit --file /Users/kennethoh/Desktop/custom_audit/VIQ-1326_videos_positive_phase1.csv

class Reaudit(SegmentedAudit):
    # Limits the number of videos retrieved from youtube api; can not exceed
    video_batch_limit = 50
    channel_batch_limit = 50
    export_batch_limit = 5000
    export_channel_limit = 50
    channel_video_retrieve_batch_size = 400
    video_audit_max_process_count = 6
    lock = Lock()

    def __init__(self, csv_file_path, csv_export_path, csv_keyword_path, reverse=False):
        super().__init__()

        self.youtube_connector = YoutubeAPIConnector()
        self.csv_file_path = csv_file_path
        self.csv_export_path = csv_export_path
        self.export_fields = ['Title', 'Category', 'URL', 'Language', 'Video Thumbnail URL', 'Channel ID', 'Views', 'Likes', 'Dislikes', 'Comments', 'Keyword Hits']
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

        self.video_id_regexp = re.compile('(?<=video/).*')
        self.channel_id_regexp = re.compile('(?<=channel/).*')
        self.keyword_regexp = self.create_keyword_regexp(csv_keyword_path)
        self.reverse_csv = reverse

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
        self.audit_videos_youtube()

    def channel_run(self):
        # self.audit_channels()
        self.process_channels()

    def audit_videos_youtube(self):
        print('Starting audit...')

        total_parsed = 0
        found_videos = []

        for videos_batch in self.video_csv_data_generator():
            video_ids = ','.join(list(videos_batch.keys()))
            response = self.youtube_connector.obtain_videos(video_ids, part='snippet,statistics')

            for video in response['items']:
                category_id = video['snippet']['categoryId']

                # If category does not match or if a bad word hits, continue
                if not self.categories.get(category_id) or self._parse_video(video, self.bad_words_regexp):
                    continue

                video_id = video['id']

                # now check if custom keywords match
                hits = set(self._parse_video(video, self.keyword_regexp))

                row_data = videos_batch[video_id]

                if hits:
                    found_videos.append(
                        self.get_export_row(video, hits, row_data)
                    )

            # Batch write results
            if total_parsed >= self.export_batch_limit:
                self.write_data(found_videos)
                found_videos.clear()

            total_parsed += self.video_batch_limit

            print('Parsed videos: {}'.format(total_parsed))

        self.write_data(found_videos)

        print('Done!')

    def write_data(self, data):
        with open(self.csv_export_path, mode='a') as export_file:
            writer = csv.writer(export_file, delimiter=',')

            for item in data:
                writer.writerow(item.values())

    def get_export_row(self, video, keyword_hits, csv_data={}):
        metadata = video['snippet']
        statistics = video['statistics']

        export_row = {
            'Title': metadata['title'],
            'Category': self.categories.get(metadata['categoryId']),
            'URL': 'http://www.youtube.com/video/' + video['id'],
            'Language': metadata.get('defaultLanguage'),
            'Video Thumbnail URL': metadata.get('thumbnails', {}).get('standard', {}).get('url', ''),
            'Channel ID': metadata['channelId'],
            'Views': statistics.get('viewCount', ''),
            'Likes': statistics.get('likeCount', ''),
            'Dislikes': statistics.get('dislikeCount', ''),
            'Comments': statistics.get('commentCount', ''),
            'Subscribers': csv_data.get('channel_subscribers'),
            'Keyword Hits': ','.join(keyword_hits)
        }
        return export_row

    def _parse_video(self, video, regexp):
        video = video.get('snippet')

        tags = video.get('tags')

        text = ''
        text += video.get("title ", '')
        text += video.get("description ", '')
        text += video.get("transcript ", '')

        if tags:
            text += ' '.join(tags)

        return re.findall(regexp, text)

    def video_csv_data_generator(self):
        with open(self.csv_file_path, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            # Pass over headers
            next(csv_reader, None)

            rows_batch = {}

            for index, row in enumerate(csv_reader):
                video_id = self.video_id_regexp.search(row.pop('url')).group()
                rows_batch[video_id] = row

                if index % self.video_batch_limit == 0:
                    yield rows_batch

                    rows_batch.clear()

            yield rows_batch

    def channel_csv_generator(self, reverse=False):
        with open(self.csv_file_path, mode='r', encoding='utf-8-sig') as csv_file:

            if reverse:
                csv_reader = csv.DictReader(csv_file)
                rows = reversed(list(csv_reader))

                for row in rows:
                    yield row

            else:
                csv_reader = csv.DictReader(csv_file)

                # Pass over headers
                next(csv_reader)

                for row in csv_reader:
                    yield row

    def audit_channels(self):
        print('Starting audit...')

        channels_seen = 0
        all_results = []

        for channel in self.channel_csv_generator(reverse=self.reverse_csv):
            channel_id = self.channel_id_regexp.search(channel['channel_url']).group()

            channel_videos = self.get_channel_videos(channel_id)
            result = self.audit_channel_videos(channel, channel_videos)

            if result:
                all_results.append(result)

            channels_seen += 1
            print('Channels seen: {}'.format(channels_seen))

            if channels_seen % self.export_channel_limit == 0:
                self.write_channel_results(all_results)

                all_results.clear()
                print('Total audited channels: {}'.format(channels_seen))

        self.write_channel_results(all_results)

    def process_channels(self):
        print('Starting audit...')

        videos_seen = 0
        channels_seen = 0
        all_videos = []

        for channel in self.channel_csv_generator(reverse=self.reverse_csv):
            channel_id = self.channel_id_regexp.search(channel['channel_url']).group()

            channel_videos = self.get_channel_videos(channel_id)
            all_videos += channel_videos

            videos_seen += len(channel_videos)
            channels_seen += 1

            if channels_seen % self.channel_video_retrieve_batch_size == 0:
                print('Auditing next batch of videos: {}'.format(len(all_videos)))
                audit_results = self.start_audit_process(all_videos)

                self.write_channel_results(audit_results)

                all_videos.clear()
                print('Channels processed: {}'.format(channels_seen))
                print('Videos processed: {}'.format(videos_seen))
                print('Last channel seen: {}'.format(repr(channel)))

        print('Audit complete/')

    def start_audit_process(self, videos):
        print('Starting mp audit with {} processes...'.format(self.video_audit_max_process_count))

        audit_processes = []
        video_process_batch_limit = len(videos) // self.video_audit_max_process_count
        shared_results = Manager().list()

        for _ in range(self.video_audit_max_process_count):
            video_batch = videos[:video_process_batch_limit]
            process = Process(
                target=self.audit_videos,
                args=(shared_results, video_batch)
            )
            audit_processes.append(process)
            process.start()
            videos = videos[self.video_audit_max_process_count:]

        for process in audit_processes:
            process.join()

        return shared_results

    def write_channel_results(self, results):
        if results:
            print('Writing {} results'.format(len(results)))
        else:
            print('No results to write.')

        channel_export = self.csv_export_path + 'barney_channel_results.csv'
        video_export = self.csv_export_path + 'barney_video_results.csv'

        # with open(channel_export, mode='a') as csv_file:
        #     writer = csv.writer(csv_file, delimiter=',')
        #
        #     for item in results:
        #         channel = item['channel']
        #         writer.writerow(channel.values())

        with open(video_export, mode='a') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')

            for video in results:
                writer.writerow(video.values())

    def get_channel_videos(self, channel_id):
        channel_videos_full_data = []
        channel_videos = []
        response = self.youtube_connector.obtain_channel_videos(channel_id, part='snippet', order='viewCount', safe_search='strict')

        channel_videos += [video['id']['videoId'] for video in response.get('items')]
        next_page_token = response.get('nextPageToken')

        while next_page_token and response.get('items'):
            response = self.youtube_connector\
                .obtain_channel_videos(channel_id, part='snippet', page_token=next_page_token, order='viewCount', safe_search='strict')

            channel_videos += [video['id']['videoId'] for video in response.get('items')]
            next_page_token = response.get('nextPageToken')

        while channel_videos:
            video_full_data_batch = channel_videos[:50]

            response = self.youtube_connector.obtain_videos(','.join(video_full_data_batch), part='snippet,statistics')

            channel_videos_full_data += response.get('items')
            channel_videos = channel_videos[50:]

        return channel_videos_full_data

    def audit_videos(self, shared_results, videos):
        start = time.time()
        results = []

        for video in videos:
            if self._parse_video(video, self.bad_words_regexp):
                continue

            hits = set(self._parse_video(video, self.keyword_regexp))

            if hits:
                results.append(
                    self.get_export_row(video, hits)
                )

        if results:
            with self.lock:
                shared_results += results

        end = time.time()

        print('Time auditing {} videos: {}'.format(len(videos), end-start))

