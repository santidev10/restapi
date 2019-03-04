import csv
import re
import time
import logging
from segment.models import SegmentChannel
from segment.models import SegmentRelatedChannel
from segment.models.persistent import PersistentSegmentRelatedChannel
from singledb.connector import SingleDatabaseApiConnector as Connector
from multiprocessing import Process
from multiprocessing import Manager

logger = logging.getLogger(__name__)


class CustomAudit(object):
    """
    Class to run custom audit against our existing channels and videos.
        Reads in a csv file to construct audit keyword regex
        Retrieves all existing channel ids from PersistentSegmentRelated table
        Slices group of channels, retrieves all videos for sliced channels, and audits videos
    """
    video_batch_size = 10000
    channel_batch_size = 40
    max_process_count = 8

    def __init__(self, *args, **kwargs):
        """
        :param args: None
        :param kwargs:
            (str) csv_file_path -> Absolute csv file path
            (str) segment_title -> Title for custom segment / Audit
        """
        self.csv_file_path = kwargs.get('csv_file_path', None)
        self.segment_title = kwargs.get('segment_title', None)

        if self.csv_file_path is None:
            raise ValueError('You must provide a csv path to read.')

        if self.segment_title is None:
            raise ValueError('You must provide a title for the custom segmented audit.')

        try:
            self.csv_file = open(self.csv_file_path, mode='r', encoding='utf-8-sig')
        except IOError:
            raise ValueError('The provided csv file was not found.')

        self.audit_regex = self.read_csv_create_regex()
        self.connector = Connector()

        print('Creating segment: {}'.format(self.segment_title))
        self.segment = SegmentChannel(
            title=self.segment_title,
            category=SegmentChannel.PRIVATE
        )
        self.segment.save()

    def run(self, *args, **kwargs):
        start = time.time()

        print('Getting all channel ids...')
        all_channel_ids = PersistentSegmentRelatedChannel.objects.order_by().values('related_id').distinct().values_list('related_id', flat=True)[:200]

        processes = []
        # batch_limit will split all_channel_ids evenly for each process
        batch_limit = len(all_channel_ids) // self.max_process_count

        # Shared dictionary across processes to keep track of audit found channels that have already been added
        shared_found_channels = Manager().dict()
        shared_found_channels['total_videos_audited'] = 0

        related_objects_to_create = Manager().list()

        print('Spawning {} processes...'.format(self.max_process_count))

        for _ in range(self.max_process_count):
            process_task = all_channel_ids[:batch_limit]
            process = Process(
                target=self.audit_channels,
                kwargs={'batch': process_task, 'found_channels': shared_found_channels, 'segment': self.segment, 'to_create': related_objects_to_create}
            )
            all_channel_ids = all_channel_ids[batch_limit - 1:]
            processes.append(process)
            process.start()

        for process in processes:
            process.join()

        end = time.time()

        print('All videos audited: {}'.format(shared_found_channels.pop('total_videos_audited')))
        print('Total execution time: {}'.format(end - start))

        # Each process will create their own found channels
        SegmentRelatedChannel.objects.bulk_create(related_objects_to_create)

        return shared_found_channels

    def audit_channels(self, batch: list, found_channels: Manager, segment: SegmentChannel, to_create: Manager):
        """
        Function that is executed by each process.
            Retrieves and audits videos for given batch channel ids.
        :param batch: (list) Channel ids to retrieve and audit videos
        :param found_channels: (dict) Shared dictionary for all processes to check if a channel has already been found
        :param segment: (SegmentChannel) Foreign key segment that will be used to create SegmentChannelRelated objects
        :return: None
        """
        channel_batch = batch

        while channel_batch:
            channel_batch_chunk = channel_batch[:self.channel_batch_size]
            videos = self.get_videos_batch(channel_ids=channel_batch_chunk)

            for video in videos:
                channel_id = video.get('channel__channel_id')

                if not channel_id:
                    continue

                # If the channel has not been flagged and fails audit, then add it
                # Checks shared found channels for all processes to read from
                found_words = self.audit_video(video, self.audit_regex)

                if not found_channels.get(channel_id) and found_words:
                    # List of all objects to create at end of audit
                    to_create.append(
                        SegmentRelatedChannel(
                            segment=segment,
                            related_id=channel_id)
                    )
                    found_channels[channel_id] = ', '.join(found_words)

            found_channels['total_videos_audited'] += len(videos)

            print('Total videos audited: {}'.format(found_channels['total_videos_audited'] ))
            channel_batch = channel_batch[self.channel_batch_size - 1:]

    def get_videos_batch(self, channel_ids: list = None) -> list:
        """
        Retrieves all videos associated with channel_ids
        :param channel_ids: (list) -> Channel id strings
        :return: (list) -> video objects from singledb
        """
        params = dict(
            fields="title,video_id,channel_id,title,description,tags,language,transcript,channel__channel_id",
            sort="video_id",
            size=self.video_batch_size,
            channel_id__terms=",".join(channel_ids),
        )
        response = self.connector.execute_get_call("videos/", params)

        return response.get('items')

    def audit_video(self, video, regex) -> bool:
        """
        Returns boolean if any match is found against audit regex
        :param video: (dict) video data
        :param regex: Compiled audit words regex
        :return: bool
        """
        metadata = [
            video.get("title") or "",
            video.get("description") or "",
            video.get("tags") or "",
            video.get("transcript") or ""
        ]
        metadata = ' '.join(metadata)

        found = re.findall(regex, metadata)

        return found

    def read_csv_create_regex(self) -> re:
        """
        Reads provided csv file of audit words and compiles regex for auditing

        :return: Regex of audit words
        """
        csv_reader = csv.reader(self.csv_file)
        escaped_audit_words = '|'.join([row[0] for row in csv_reader])

        return re.compile(escaped_audit_words)
