from audit_tool.segmented_audit import SegmentedAudit
from utils.youtube_api import YoutubeAPIConnector
from utils.youtube_api import YoutubeAPIConnectorException
import csv
import re
from multiprocessing import Pool
import langid
import datetime

class Reaudit(SegmentedAudit):
    max_process_count = 4

    video_chunk_size = 10000
    video_batch_size = 30000
    channel_batch_size = 1000
    channel_chunk_size = 100
    channel_row_data = {}

    video_csv_headers = ['Title', 'Category', 'Video URL', 'Language', 'View Count',
                         'Like Count', 'Dislike Count', 'Comment Count', 'Keyword Hits', 'Channel Title', 'Channel URL', 'Channel Subscribers']

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
        self.video_export_path = '{dir}{title}{type}{time}.csv'.format(dir=self.csv_export_dir,
                                                                       title=self.csv_export_title,
                                                                       type='Video',
                                                                       time=str(datetime.datetime.now())
                                                                       )

        self.channel_export_path = '{dir}{title}{type}{time}.csv'.format(dir=self.csv_export_dir,
                                                                         title=self.csv_export_title,
                                                                         type='Channel',
                                                                         time=str(datetime.datetime.now())
                                                                         )
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
        self.keyword_regexp = self.create_keyword_regexp(self.csv_keyword_path) if self.csv_keyword_path else None
        self.more_bad_words = self.create_keyword_regexp(kwargs['badwords']) if kwargs.get('badwords') else None
        self.custom_csv = kwargs.get('custom_csv')

        # Write csv headers
        with open(self.video_export_path, mode='w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(self.video_csv_headers)

        if self.audit_type == 'channel':
            # Write csv headers
            with open(self.channel_export_path, mode='w') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(self.channel_csv_headers)


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

            self.write_data(all_results, audit_type)

            print('Seen {} {}s'.format(items_seen, self.audit_type))

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
        all_videos = []
        connector = YoutubeAPIConnector()

        for row in csv_channels:
            channel_id = self.channel_id_regexp.search(row.get('channel_url')).group()
            channel_videos = self.get_channel_videos(channel_id, connector)
            all_videos += channel_videos

        video_audit_results = self.audit_videos(all_videos)
        channel_audit_results = self.audit_channels(video_audit_results, connector)

        self.update_video_channel_subscribers(video_audit_results, channel_audit_results)

        return video_audit_results + channel_audit_results

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

    def write_data(self, data, audit_type='video'):
        videos = []
        channels = []

        if audit_type == 'video':
            videos = data
        else:
            for item in data:
                if item.get('type') == 'channel':
                    channels.append(item)
                else:
                    videos.append(item)

        with open(self.video_export_path, mode='a') as export_file:
            writer = csv.writer(export_file, delimiter=',')

            for item in videos:
                row = self.get_video_export_row(item)
                writer.writerow(row)

        if audit_type == 'channel':
            with open(self.channel_export_path, mode='a') as export_file:
                writer = csv.writer(export_file, delimiter=',')

                for item in channels:
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
            statistics.get('viewCount', 0),
            statistics.get('likeCount', 0),
            statistics.get('dislikeCount', 0),
            statistics.get('commentCount', 0),
            metadata.get('channelTitle'),
            'http://www.youtube.com/channel/' + metadata['channelId'],
            csv_data.get('channel_subscribers') or statistics['channelSubscriberCount'],
        ]

        if self.keyword_regexp:
            export_row.append(','.join(video['snippet']['keyword_hits']),)

        return export_row

    def get_channel_export_row(self, channel):
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
            statistics['subscriberCount'],
            statistics['viewCount'],
            video_data['totalAuditedVideos'],
            video_data['totalLikes'],
            video_data['totalDislikes'],
        ]

        if self.keyword_regexp:
            export_row.append(', '.join(channel['keyword_hits']))

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

    def audit_videos(self, videos):
        results = []

        for video in videos:
            # Continue over bad words
            if self._parse_video(video, self.bad_words_regexp):
                continue

            # If provided, more bad keywords to filter against
            if self.more_bad_words and self._parse_video(video, self.more_bad_words):
                continue

            # If whitelist keywords provided, keywords to filter for
            if self.keyword_regexp:
                hits = set(self._parse_video(video, self.keyword_regexp))

                if hits:
                    self.set_language(video)
                    self.set_keyword_hits(video, hits)

                    # Only add English videos and add if category matches mapping
                    if video['snippet']['defaultLanguage'] == 'en' and self.video_categories.get(video['snippet']['categoryId']):
                        results.append(video)
            else:
                self.set_language(video)
                results.append(video)

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
            })
            channel_data[channel_id]['aggregatedVideoData']['totalLikes'] += int(video['statistics'].get('likeCount', 0))
            channel_data[channel_id]['aggregatedVideoData']['totalDislikes'] += int(video['statistics'].get('dislikeCount', 0))
            channel_data[channel_id]['aggregatedVideoData']['totalAuditedVideos'] = channel_data[channel_id]['aggregatedVideoData'].get('totalAuditedVideos', 0) + 1
            channel_data[channel_id]['categoryCount'] = channel_data[channel_id].get('categoryCount', [])
            channel_data[channel_id]['categoryCount'].append(video['snippet'].get('categoryId'))

            if self.keyword_regexp:
                channel_data[channel_id]['keyword_hits'] = channel_data[channel_id].get('keyword_hits') or set()
                channel_data[channel_id]['keyword_hits'].update(video['snippet']['keyword_hits'])

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
