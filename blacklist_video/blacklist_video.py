from utils.youtube_api import YoutubeAPIConnector
from blacklist_video.models import BlacklistVideo
import csv
import re
import json
from multiprocessing import Pool
from django.db.utils import IntegrityError as DjangoIntegrityError
from psycopg2 import IntegrityError as PostgresIntegrityError

class BlacklistVideos(object):
    youtube_video_limit = 50
    video_batch_size = 100
    max_process_count = 12
    max_batch_size = 1200
    video_processing_batch_size = 100
    max_db_size = 750000
    csv_export_limit = 150000
    page_number = 1
    export_count = 0
    headers = ['Video Title', 'Video URL', 'Video ID', 'Video Description', 'Channel Title', 'Channel URL', 'Channel ID']

    def __init__(self, *args, **kwargs):
        self.yt_connector = YoutubeAPIConnector()
        self.seed_type = kwargs.get('seed_type')
        self.seed_file_path = kwargs.get('file')
        self.export_path = kwargs.get('export')
        self.export_title = kwargs.get('title')
        self.ignore_seed = kwargs.get('ignore_seed')
        self.export_force = kwargs.get('export_force')
        self.return_results = kwargs.get('return')

    def run(self):
        print('Starting video blacklist...')

        if self.export_force:
            self.export()
            return

        if not self.ignore_seed:
            if self.seed_type == 'channel':
                self.extract_channel_videos()
            elif self.seed_type == 'video':
                self.extract_video_id_seeds()
            else:
                raise ValueError('Unsupported seed_type: {}'.format(self.seed_type))

        self.run_process()

        if self.export_path:
            self.export_csv()

        if self.return_results:
            return BlacklistVideo.objects.all().values_list('video_id', flat=True)

        print('Audit complete!')
        total_videos = BlacklistVideo.objects.all().count()
        print('Total videos stored: {}'.format(total_videos))

    def export_existing(self):
        all_video_data = BlacklistVideo \
            .objects.all() \
            .distinct('video_id') \
            .values_list('video_id', 'title', 'description', 'channel_id', 'channel_title')

        self.export_csv(data=all_video_data,
                        headers=['Video ID', 'Video Title', 'Video Description', 'Channel ID', 'Channel Title'],
                        csv_export_path='/Users/kennethoh/Desktop/blacklist/blacklist_result.csv'
                        )

    def get_save_video_data(self, video_ids):
        while video_ids:
            video_batch = video_ids[:self.youtube_video_limit]
            video_ids_term = ','.join(video_batch)
            # Get metadata for the videos
            response = self.yt_connector.obtain_video_metadata(video_ids_term)

            video_data = response['items']
            print('Creating {} videos.'.format(len(video_data)))

            self._bulk_create_seeds(video_data)

            video_ids = video_ids[self.video_batch_size:]

        print('Total seed ids added', BlacklistVideo.objects.all().count())

    def _safe_bulk_create(self, video_objs):
        to_create = []
        duplicates = {}

        try:
            BlacklistVideo.objects.bulk_create(video_objs)

        except DjangoIntegrityError or PostgresIntegrityError:
            print('Duplicates')
            for video in video_objs:
                try:
                    BlacklistVideo.objects.get(video_id=video.video_id)

                except BlacklistVideo.DoesNotExist:
                    if not duplicates.get(video.video_id):
                        to_create.append(video)

                    duplicates[video.video_id] = True

        BlacklistVideo.objects.bulk_create(to_create)

    def _bulk_create_seeds(self, video_data):
        to_create = []

        for video in video_data:
            video_id = video['id'] if type(video['id']) is str else video['id']['videoId']

            try:
                BlacklistVideo.objects.get(video_id=video_id)

            except BlacklistVideo.DoesNotExist:

                to_create.append(
                    BlacklistVideo(
                        video_id=video_id,
                        channel_id=video['snippet']['channelId'],
                        channel_title=video['snippet']['channelTitle'],
                        title=video['snippet']['title'],
                        description=video['snippet']['description'],
                        scanned=False,
                        source=None
                    )
                )

        BlacklistVideo.objects.bulk_create(to_create)

    def extract_video_id_seeds(self):
        """
        Reads seeds csv and saves to db
        :return:
        """
        video_id_seeds = []
        with open(self.seed_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)

            for row in csv_reader:
                video_id = row[0]
                video_id_seeds.append(video_id)

        print('Video seeds extracted: {}'.format(len(video_id_seeds)))
        self.get_save_video_data(video_id_seeds)

    def run_process(self):
        # Need to pass video objects instead of ids to processes to set as foreign keys for found related items
        pool = Pool(processes=self.max_process_count)
        videos_to_scan = BlacklistVideo \
            .objects \
            .filter(scanned=False) \
            .distinct('video_id')[:self.max_batch_size]

        while videos_to_scan:
            print('Getting related videos for {} videos'.format(len(videos_to_scan)))

            batches = list(self.chunks(videos_to_scan, self.video_processing_batch_size))
            videos = pool.map(self.get_related_videos, batches)

            to_create = self.get_unique_items([item for batch_result in videos for item in batch_result], key='video_id')

            for batch in batches:
                # Update batch videos since they have been scanned for related items
                video_ids = [scanned.video_id for scanned in batch]
                BlacklistVideo.objects.filter(video_id__in=video_ids).update(scanned=True)

            print('Saving {} videos'.format(len(to_create)))
            self._safe_bulk_create(to_create)

            total_items = BlacklistVideo.objects.all().count()

            print('Total items saved: {}'.format(total_items))

            if total_items >= self.max_db_size:
                break

            videos_to_scan = BlacklistVideo \
                .objects \
                .filter(scanned=False) \
                .distinct('video_id')[:self.max_batch_size]

        print('Complete')

    @staticmethod
    def get_unique_items(items, key='video_id'):
        unique = {}

        for item in items:
            item_id = getattr(item, key)
            if not unique.get(item_id):
                unique[item_id] = item

        unique_items = unique.values()

        return unique_items

    def get_related_videos(self, videos):
        """
        Retrieves all related videos for videos list argument
        :param videos: (list) BlacklistVideo objects
        :return: (list) BlacklistVideo objects
        """
        all_related_videos = []
        yt_connector = YoutubeAPIConnector()

        for video in videos:
            response = yt_connector.get_related_videos(video.video_id)
            items = response['items']

            page_token = response.get('nextPageToken')

            related_videos = self.set_obj_fields(video, items)
            all_related_videos += related_videos

            while page_token and items:
                response = yt_connector.get_related_videos(video.video_id, page_token=page_token)
                items = response['items']

                page_token = response.get('nextPageToken')
                related_videos = self.set_obj_fields(video, items)

                all_related_videos += related_videos

        return all_related_videos

    def set_obj_fields(self, source, items):
        related_videos = [
            BlacklistVideo(
                video_id=item['id']['videoId'],
                channel_id=item['snippet']['channelId'],
                channel_title=item['snippet']['channelTitle'],
                title=item['snippet']['title'],
                description=item['snippet']['description'],
                scanned=False,
                source=source
            ) for item in items
        ]

        return related_videos

    def extract_channel_videos(self):
        """
        Gets all video ids for each of the channels in csv
        :return:
        """
        all_channel_ids = self.extract_channel_ids()

        for id in all_channel_ids:
            video_ids = self.get_channel_videos(id)
            self.get_save_video_data(video_ids)

        print('Got videos for {} channels'.format(len(all_channel_ids)))

    def extract_channel_ids(self):
        """
        Extracts all channel ids from csv
        :return:
        """
        all_channel_ids = []

        with open(self.seed_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row in csv_reader:
                # Extract channel id and add to queue to get all videos for channel
                channel_id = re.search(r'(?<=channel/).*', row['channel_url']).group()

                all_channel_ids.append(channel_id)

        return all_channel_ids

    def get_channel_videos(self, channel_id):
        """
        Gets all video ids for channel id
        :param channel_id:
        :return:
        """
        video_ids = []
        result = self.yt_connector.obtain_channel_videos(channel_id)

        while True:
            next_page_token = result.get('nextPageToken')

            for video in result.get('items'):
                video_ids.append(video['id']['videoId'])

            if not next_page_token:
                break

            result = self.yt_connector.obtain_channel_videos(channel_id, page_token=next_page_token)

        return video_ids

    def export_csv(self):
        all_video_data = BlacklistVideo \
            .objects.all() \
            .distinct('video_id') \
            .values('video_id', 'title', 'description', 'channel_id', 'channel_title')

        while True:
            next_page = False
            export_path = '{}Page{}{}.csv'.format(self.export_path, self.page_number, self.export_title)
            print('Exporting CSV to: {}'.format(export_path))

            with open(export_path, mode='w') as csv_export:
                writer = csv.writer(csv_export)
                writer.writerow(self.headers)

                for video in all_video_data:
                    row = self.get_csv_export_row(video)
                    writer.writerow(row)
                    self.export_count += 1

                    if self.export_count >= self.csv_export_limit:
                        next_page = True
                        self.page_number += 1
                        self.export_count = 0
                        all_video_data = all_video_data[self.csv_export_limit:]
                        break

            if next_page is False:
                break

        print('CSV export complete.')

    def get_csv_export_row(self, video):
        row = [
            video['title'][:255],
            'http://youtube.com/video/' + video['video_id'],
            video['video_id'],
            video['description'],
            video['channel_title'][:255],
            'http://youtube.com/channel/' + video['channel_id'],
            video['channel_id'],
        ]

        return row

    @staticmethod
    def chunks(iterable, length):
        for i in range(0, len(iterable), length):
            yield iterable[i:i + length]
