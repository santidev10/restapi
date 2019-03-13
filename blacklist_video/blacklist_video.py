from utils.youtube_api import YoutubeAPIConnector
from blacklist_video.models import BlacklistVideo
import csv
import re
import json

class BlacklistVideos(object):
    video_batch_size = 10

    def __init__(self, audit_type):
        self.yt_connector = YoutubeAPIConnector()
        self.audit_type = audit_type

    def run(self):
        print('Starting video blacklist...')

        if self.audit_type == 'channel':
            self.extract_channel_videos()
        else:
            self.extract_video_id_seeds()

        self.get_related_videos()

        all_video_data = BlacklistVideo\
            .objects.all() \
            .distinct('video_id') \
            .values_list('video_id', 'title', 'description', 'channel_id', 'channel_title')

        self.export_csv(data=all_video_data,
                        headers=['Video ID', 'Video Title', 'Video Description', 'Channel ID', 'Channel Title', 'Channel URL'],
                        csv_export_path='/Users/kennethoh/Desktop/blacklist/blacklist_result.csv'
                        )

    def update_channel_seeds(self):
        channel_ids = self.extract_channel_ids()
        for channel_id in channel_ids:
            video_ids = self.get_channel_videos(channel_id)

            for video_id in video_ids:
                if video_id == 'Fxq5JowQA9U':
                    print('Last video met', video_id)
                    print('Channel id: {}'.format(channel_id))

                    break

                try:
                    BlacklistVideo.objects.filter(video_id=video_id).update(scanned=True)
                    print('video scanned updated: {}'.format(video_id))
                except BlacklistVideo.DoesNotExist:
                    pass


    def export_existing(self):
        all_video_data = BlacklistVideo \
            .objects.all() \
            .distinct('video_id') \
            .values_list('video_id', 'title', 'description', 'channel_id', 'channel_title')

        self.export_csv(data=all_video_data,
                        headers=['Video ID', 'Video Title', 'Video Description', 'Channel ID', 'Channel Title'],
                        csv_export_path='/Users/kennethoh/Desktop/blacklist/blacklist_result.csv'
                        )

    def get_video_data(self, video_ids):
        while video_ids:
            video_batch = video_ids[:self.video_batch_size]
            video_ids_term = ','.join(video_batch)
            # Get metadata for the videos
            response = self.yt_connector.obtain_video_metadata(video_ids_term)

            video_data = response['items']
            print('Creating {} videos.'.format(len(video_data)))
            self._bulk_create(video_data)

            video_ids = video_ids[self.video_batch_size:]

        print('Total seed ids added', BlacklistVideo.objects.all().count())

    def extract_video_id_seeds(self):
        """
        Reads seeds csv and saves to db
        :return:
        """
        video_id_seeds = []
        csv_file_path = '/Users/kennethoh/Desktop/blacklist/blacklist.csv'

        with open(csv_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)

            for row in csv_reader:
                video_id = row[0]
                video_id_seeds.append(video_id)

        print('Video seeds extracted: {}'.format(len(video_id_seeds)))
        self.get_video_data(video_id_seeds)

    def get_related_videos(self):
        print('Getting related videos')
        # Get all video ids to scan
        video_ids_in_db = BlacklistVideo\
            .objects\
            .filter(scanned=False)\
            .distinct('video_id')\
            .values_list('video_id', flat=True)

        while len(video_ids_in_db) > 0:
            print('Videos to scan: {}'.format(len(video_ids_in_db)))

            for id in video_ids_in_db:
                response = self.yt_connector.get_related_videos(id)
                video_data = response['items']

                self._bulk_create(video_data)

                print('Related videos retrieved {} for {}'.format(len(video_data), id))

                # While there is a next page and the response has data, get and save the results
                page_number = 1
                while response.get('nextPageToken') is not None and response.get('items'):
                    print('Getting page {} for id {}'.format(page_number, id))

                    page_token = response['nextPageToken']
                    response = self.yt_connector.get_related_videos(id, page_token=page_token)
                    video_data = response['items']

                    print('Related videos retrieved: {}'.format(len(video_data)))

                    if video_data:
                        self._bulk_create(video_data)

                    page_number += 1

            total_videos = BlacklistVideo.objects.all().count()
            print('Total videos stored: {}'.format(total_videos))

            self.update_scanned(video_ids_in_db)

            video_ids_in_db = BlacklistVideo \
                .objects \
                .filter(scanned=False) \
                .distinct('video_id') \
                .values_list('video_id', flat=True)

            print('Videos to scan: {}'.format(len(video_ids_in_db)))

        print('Audit complete!')
        total_videos = BlacklistVideo.objects.all().count()
        print('Total videos stored: {}'.format(total_videos))

    def get_related_videos_worker(self):
        while True:
            # Get video ids list from queue
            video_id = self.get_related_videos_queue.get()
            video_data = self.yt_connector.get_related_videos(video_id)['items']

            print('Related videos retrieved: {}'.format(len(video_data)))

            self._bulk_create(video_data)
            self.get_videos_from_db()

            self.get_related_videos_queue.task_done()

    def update_scanned(self, video_ids):
        print('Updated scanned for: {}'.format(video_ids))

        # BlacklistVideo.objects.filter(video_id__in=video_ids).update(scanned=True)
        for id in video_ids:
            video = BlacklistVideo.objects.get(video_id=id)
            video.scanned = True
            video.save()

    def _bulk_create(self, video_data):
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
                        scanned=False
                    )
                )

        BlacklistVideo.objects.bulk_create(to_create)

    def extract_channel_videos(self):
        """
        Gets all video ids for each of the channels in csv
        :return:
        """
        all_channel_ids = self.extract_channel_ids()

        for id in all_channel_ids:
            video_ids = self.get_channel_videos(id)
            self.get_video_data(video_ids)

        print('Get videos for {} channels'.format(len(all_channel_ids)))

    def extract_channel_ids(self):
        """
        Extracts all channel ids from csv
        :return:
        """
        csv_file_path = '/Users/kennethoh/Desktop/blacklist/blacklist_channels.csv'
        all_channel_ids = []

        with open(csv_file_path) as csv_file:
            csv_reader = csv.reader(csv_file)

            for row in csv_reader:
                # Extract channel id and add to queue to get all videos for channel
                channel_id = re.search(r'(?<=channel/).*', row[0]).group()
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

    def export_csv(self, data=None, headers=None, csv_export_path=None):
        if data is None:
            raise ValueError('You must provide a data source.')
        if headers is None:
            raise ValueError('You must provide a list of heaeders.')
        if csv_export_path is None:
            raise ValueError('You must provide a csv output path.')

        print('Exporting CSV to: {}'.format(csv_export_path))

        video_data = (video for video in data)

        with open(csv_export_path, mode='w') as csv_export:
            writer = csv.writer(csv_export, delimiter=',')
            writer.writerow(headers)

            for video in video_data:
                row = [item for item in video]
                row.append(
                    'http://youtube.com/channel/' + video[3]
                )
                writer.writerow(row)

        print('CSV export complete.')
