from utils.youtube_api import YoutubeAPIConnector
from audit_tool.auditor import Auditor
from .models import RelatedVideo
import csv
import re
from multiprocessing import Pool
from django.db.utils import IntegrityError as DjangoIntegrityError
from psycopg2 import IntegrityError as PostgresIntegrityError

class Related(object):
    youtube_video_limit = 50
    video_batch_size = 100
    max_process_count = 12
    max_batch_size = 1200
    video_processing_batch_size = 100
    max_database_size = 1000
    csv_export_limit = 100000
    page_number = 1
    export_count = 0
    headers = ['Video Title', 'Video URL', 'Video ID', 'Video Description', 'Channel Title', 'Channel URL',
               'Channel ID']
    channel_id_regexp = re.compile('(?<=channel/).*')
    video_id_regexp = re.compile('(?<=video/).*')

    def __init__(self, *args, **kwargs):
        self.yt_connector = YoutubeAPIConnector()
        self.export_force = kwargs.get('export_force')
        self.ignore_seed = kwargs.get('ignore_seed')
        self.seed_type = kwargs.get('seed_type')
        self.seed_file_path = kwargs.get('file')
        self.export_path = kwargs.get('export')
        self.export_title = kwargs.get('title')
        self.return_results = kwargs.get('return')

    def run(self):
        print('Starting related video retrieval process...')

        # If export_force option is set, export existing data
        if self.export_force:
            self.export_csv()
            return

        if not self.ignore_seed:
            if self.seed_type == 'channel':
                self.extract_channel_videos()
            elif self.seed_type == 'video':
                self.extract_video_id_seeds()
            else:
                raise ValueError('Unsupported seed_type: {}'.format(self.seed_type))

        self.export_csv()
        return

        self.run_process()

        if self.export_path:
            self.export_csv()

        if self.return_results:
            return RelatedVideo.objects.all().values_list('video_id', flat=True)

        print('Audit complete!')
        total_videos = RelatedVideo.objects.all().count()
        print('Total videos stored: {}'.format(total_videos))

    def get_save_video_data(self, video_ids):
        """
        Retrieves Youtube metadata and saves to database
        :param video_ids: (list) Youtube video ids
        :return: None
        """
        while video_ids:
            video_batch = video_ids[:self.youtube_video_limit]
            video_ids_term = ','.join(video_batch)
            # Get metadata for the videos
            response = self.yt_connector.obtain_video_metadata(video_ids_term)

            video_data = response['items']
            print('Creating {} videos.'.format(len(video_data)))

            self._bulk_create_seed_videos(video_data)

            video_ids = video_ids[self.video_batch_size:]

        print('Total seed videos added:', RelatedVideo.objects.all().count())

    def _safe_bulk_create(self, video_objs):
        to_create = []
        duplicates = {}

        try:
            RelatedVideo.objects.bulk_create(video_objs)

        except (DjangoIntegrityError, PostgresIntegrityError):
            for video in video_objs:
                try:
                    RelatedVideo.objects.get(video_id=video.video_id)

                except RelatedVideo.DoesNotExist:
                    if not duplicates.get(video.video_id):
                        to_create.append(video)

                    duplicates[video.video_id] = True

        RelatedVideo.objects.bulk_create(to_create)

    def _bulk_create_seed_videos(self, video_data):
        """
        Creates RelatedVideo objects using youtube video data
        :param video_data: (list) Youtube video data
        :return: None
        """
        to_create = []

        for video in video_data:
            video_id = video['id'] if type(video['id']) is str else video['id']['videoId']

            try:
                RelatedVideo.objects.get(video_id=video_id)

            except RelatedVideo.DoesNotExist:

                to_create.append(
                    RelatedVideo(
                        video_id=video_id,
                        channel_id=video['snippet']['channelId'],
                        channel_title=video['snippet']['channelTitle'][:225],
                        title=video['snippet']['title'][:225],
                        description=video['snippet']['description'],
                        scanned=False,
                        source=None
                    )
                )

        RelatedVideo.objects.bulk_create(to_create)

    def extract_video_id_seeds(self):
        """
        Reads video url seeds csv and saves to database
        :return:
        """
        video_id_seeds = []
        with open(self.seed_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row in csv_reader:
                video_id = re.search(self.video_id_regexp, row['video_url']).group()
                video_id_seeds.append(video_id)

        print('Video seeds extracted: {}'.format(len(video_id_seeds)))
        self.get_save_video_data(video_id_seeds)

    def run_process(self):
        # Need to pass video objects instead of ids to processes to set as foreign keys for found related items
        pool = Pool(processes=self.max_process_count)
        videos_to_scan = RelatedVideo \
                             .objects \
                             .filter(scanned=False) \
                             .distinct('video_id')[:self.max_batch_size]

        while videos_to_scan:
            print('Getting related videos for {} videos'.format(len(videos_to_scan)))

            batches = list(self.chunks(videos_to_scan, self.video_processing_batch_size))
            videos = pool.map(self.get_related_videos, batches)

            to_create = self.get_unique_items([item for batch_result in videos for item in batch_result],
                                              key='video_id')

            for batch in batches:
                # Update batch videos since they have been scanned for related items
                video_ids = [scanned.video_id for scanned in batch]
                RelatedVideo.objects.filter(video_id__in=video_ids).update(scanned=True)

            print('Saving {} videos'.format(len(to_create)))
            self._safe_bulk_create(to_create)

            total_items = RelatedVideo.objects.all().count()

            print('Total items saved: {}'.format(total_items))

            if total_items >= self.max_database_size:
                break

            videos_to_scan = RelatedVideo \
                                 .objects \
                                 .filter(scanned=False) \
                                 .distinct('video_id')[:self.max_batch_size]

    def get_related_videos(self, videos):
        """
        Retrieves all related videos for videos list argument
        :param videos: (list) RelatedVideo objects
        :return: (list) RelatedVideo objects
        """
        all_related_videos = []
        yt_connector = YoutubeAPIConnector()

        for video in videos:
            response = yt_connector.get_related_videos(video.video_id)
            items = response['items']

            page_token = response.get('nextPageToken')

            related_videos = self.prepare_related_items(video, items)
            all_related_videos += related_videos

            while page_token and items:
                response = yt_connector.get_related_videos(video.video_id, page_token=page_token)
                items = response['items']

                page_token = response.get('nextPageToken')
                related_videos = self.prepare_related_items(video, items)

                all_related_videos += related_videos

        return all_related_videos

    def prepare_related_items(self, source, items):
        """
        Prepares RelatedVideo instance fields
        :param source: RelatedVideo object that was used as source to retrieve related videos for
        :param items: Youtube video data
        :return: (list) RelatedVideo objects
        """
        related_videos = [
            RelatedVideo(
                video_id=item['id']['videoId'],
                channel_id=item['snippet']['channelId'],
                channel_title=item['snippet']['channelTitle'][:225],
                title=item['snippet']['title'][:225],
                description=item['snippet']['description'],
                scanned=False,
                source=source
            ) for item in items
        ]

        return related_videos

    def extract_channel_videos(self):
        """
        Retrieves all video ids for each of the channels in csv and saves to database
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
                channel_id = re.search(self.channel_id_regexp, row['channel_url']).group()

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
        """
        Export database data as csv
            Paginates export
        :return:
        """
        all_video_data = RelatedVideo \
            .objects.all() \
            .order_by('channel_title') \
            .values('video_id', 'title', 'description', 'channel_id', 'channel_title')

        while True:
            next_page = False
            export_path = '{}Page{}{}.csv'.format(self.export_path, self.page_number, self.export_title)
            print('Exporting CSV to: {}'.format(export_path))

            with open(export_path, mode='w', encoding='utf-8') as csv_export:
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

    @staticmethod
    def get_unique_items(items, key='video_id'):
        """
        Removes duplicate videos to be created
        :param items: (list) RelatedVideo objects
        :param key: Unique key to sort by
        :return: (list) RelatedVideo objects
        """
        unique = {}

        for item in items:
            item_id = getattr(item, key)
            if not unique.get(item_id):
                unique[item_id] = item

        unique_items = unique.values()

        return unique_items

    def get_csv_export_row(self, video):
        """
        Format csv export row
        :param video: Youtube video data
        :return: (list)
        """
        row = [
            video['title'],
            'http://youtube.com/video/' + video['video_id'],
            video['video_id'],
            video['description'],
            video['channel_title'],
            'http://youtube.com/channel/' + video['channel_id'],
            video['channel_id'],
        ]

        return row

    @staticmethod
    def chunks(iterable, length):
        for i in range(0, len(iterable), length):
            yield iterable[i:i + length]

