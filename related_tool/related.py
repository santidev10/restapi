# pylint: skip-file
# FIXME: Consider removing the file. Looks unused
from utils.youtube_api import YoutubeAPIConnector
from audit_tool.audit import AuditProvider
from audit_tool.auditor import AuditService
from audit_tool.auditor import Audit
from .models import RelatedVideo
import csv
import re
from multiprocessing import Pool
from django.db.utils import IntegrityError as DjangoIntegrityError
from psycopg2 import IntegrityError as PostgresIntegrityError
from utils.lang import fasttext_lang
from utils.lang import remove_mentions_hashes_urls

class Related(object):
    youtube_video_limit = 50
    max_batch_size = 200
    video_processing_batch_size = 20
    max_process_count = 10
    max_database_size = 750000
    csv_export_limit = 800000
    page_number = 1
    export_count = 0
    headers = ['Video Title', 'Video URL', 'Video ID', 'Video Description', 'Channel Title', 'Channel URL',
               'Channel ID']

    def __init__(self, *args, **kwargs):
        self.yt_connector = YoutubeAPIConnector()
        self.export_force = kwargs.get('export_force')
        self.ignore_seed = kwargs.get('ignore_seed')
        self.seed_type = kwargs.get('seed_type')
        self.seed_file_path = kwargs.get('file')
        self.export_path = kwargs.get('export')
        self.export_title = kwargs.get('title')
        self.return_results = kwargs.get('return')
        self.provider = AuditProvider()
        self.auditor = AuditService()
        self.audit = Audit()
        self.brand_safety_regexp = self.provider.get_brand_safety_regexp()

    def run(self):
        print('Starting related video retrieval process...')

        # If export_force option is set, export existing data
        if self.export_force:
            self.export_csv()
            return

        if not self.ignore_seed:
            if self.seed_type == 'channel':
                self.get_all_channel_videos()
            elif self.seed_type == 'video':
                self.extract_video_id_seeds()
            else:
                raise ValueError('Unsupported seed_type: {}'.format(self.seed_type))

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
            video_id_batch = ','.join(video_batch)

            # Get metadata for the videos
            response = self.yt_connector.obtain_videos(video_id_batch, part='snippet')
            video_data = response['items']

            audited_videos = [
                video for video in video_data
                if not self.auditor.parse_video(video, self.brand_safety_regexp)
            ]

            print('Creating {} videos.'.format(len(audited_videos)))

            self._bulk_create_seed_videos(audited_videos)

            video_ids = video_ids[self.youtube_video_limit:]

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

        audited_videos = [video for video in to_create if not self.audit_video(video)]

        RelatedVideo.objects.bulk_create(audited_videos)

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

        audited_videos = [
            video for video in all_related_videos
            if not self.audit_video(video)
            and 'en' in self.get_language(video)
        ]

        return audited_videos

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

    def extract_video_id_seeds(self):
        """
        Reads video url seeds csv and saves to database
        :return:
        """
        with open(self.seed_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            all_video_id_seeds = [
                re.search(self.auditor.video_id_regexp, row['video_url']).group()
                if row.get('video_url') else row['video_id']
                for row in csv_reader
            ]

        print('Video seeds extracted: {}'.format(len(all_video_id_seeds)))
        self.get_save_video_data(all_video_id_seeds)

    def extract_channel_id_seeds(self):
        """
        Extracts all channel ids from csv
        :return:
        """
        with open(self.seed_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            all_channel_ids = [
                re.search(self.auditor.channel_id_regexp, row['channel_url']).group()
                if row.get('channel_url') else row['channel_id']
                for row in csv_reader
            ]

        return all_channel_ids

    def get_all_channel_videos(self):
        """
        Retrieves all video ids for each of the channels in csv and saves to database
        :return:
        """
        all_channel_ids = self.extract_channel_id_seeds()

        for id in all_channel_ids:
            video_ids = self.get_channel_videos(id)
            self.get_save_video_data(video_ids)

        print('Got videos for {} channels'.format(len(all_channel_ids)))

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

    def audit_video(self, video_obj):
        text = ''
        text += video_obj.title
        text += video_obj.description
        text += video_obj.channel_title

        match = re.match(self.brand_safety_regexp, text)

        return match

    @staticmethod
    def get_language(obj):
        text = ''
        text += obj.title
        text += obj.description
        text += obj.channel_title
        text = remove_mentions_hashes_urls(text)

        language = fasttext_lang(text)

        return language
