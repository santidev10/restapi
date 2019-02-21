import csv
import re
import time
import logging
from segment.models import SegmentChannel
from segment.models import SegmentRelatedChannel
from segment.models.persistent import PersistentSegmentRelatedChannel
from singledb.connector import SingleDatabaseApiConnector as Connector
from threading import Thread
from threading import Lock
from queue import Queue

logger = logging.getLogger(__name__)


class CustomAudit(object):
    """
    Class to run custom audit against our existing channels and videos.
        Reads in a csv file to construct audit keyword regex
        Retrieves all existing channel ids from PersistentSegmentRelated table
        Slices group of channels, retrieves all videos for sliced channels, and audits videos
    """
    queue_size = 1000
    video_batch_size = 10000
    channel_batch_size = 50
    # throttle is float or int seconds to rest in between retrieving videos as to not overwhelm singledb
    throttle = 1.5
    max_thread_count = 5
    audited_found_channels = {}
    channels_with_found_words = []
    lock = Lock()

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
        self.segment = SegmentChannel(
            title=self.segment_title,
            category=SegmentChannel.PRIVATE
        )
        self.segment.save()

    def run(self):
        """
        Creates queue and threads, enqueues channel id batches for threads to work
        :return:
        """
        start = time.time()
        all_channel_ids = PersistentSegmentRelatedChannel.objects.order_by().values('related_id').distinct().values_list('related_id', flat=True)

        queue = Queue(maxsize=self.queue_size)

        # Create threads and listen for queue
        self.start_threads(queue=queue)

        # Enqueue batches of channel ids for threads to work on
        while all_channel_ids:
            channel_batch = all_channel_ids[:self.channel_batch_size]
            queue.put(channel_batch)
            all_channel_ids = all_channel_ids[self.channel_batch_size - 1:]

        queue.join()

        SegmentRelatedChannel.objects.bulk_create(self.channels_with_found_words)

        end = time.time()
        logger.info('Custom audit for segment "{}" complete.'.format(self.segment_title))
        logger.info('Audit execution time: {}'.format(end - start))
        print('Audit execution time: {}'.format(end - start))

    def start_threads(self, queue: Queue) -> None:
        """
        Creates threads that will listen for queue tasks
        :param queue: Queue that threads will work on
        :return: None
        """
        logger.info('Starting {} threads.'.format(self.max_thread_count))
        for _ in range(self.max_thread_count):
            worker = Thread(target=self.execute_thread, args=(queue,))
            worker.setDaemon(True)
            worker.start()

    def execute_thread(self, queue: Queue) -> None:
        """
        Wrapper function to listen for Queue and execute audit_channels
        :param queue:
        :return:
        """
        while True:
            channel_ids = queue.get()
            self.audit_channels(channel_ids=channel_ids)
            queue.task_done()

    def audit_channels(self, channel_ids: list = None):
        """
        Executes custom audit process
        :return: None
        """
        channel_ids_batch = channel_ids
        videos = self.get_videos_batch(channel_ids=channel_ids_batch)

        for video in videos:
            channel_id = video.get('channel__channel_id')

            if not channel_id:
                continue

            # If the channel has not been flagged and fails audit, then add it
            # Checks global self.audited_found_channels for all threads to read from
            if not self.audited_found_channels.get(channel_id) and self.audit_video(video, self.audit_regex):

                # List of all objects to create at end of audit
                self.channels_with_found_words.append(
                    SegmentRelatedChannel(
                        segment=self.segment,
                        related_id=channel_id)
                )
                self.audited_found_channels[channel_id] = True

    def get_videos_batch(self, channel_ids: list = None) -> list:
        """
        Retrieves all videos associated with channel_ids
        :param channel_ids: (list) -> Channel id strings
        :return: (list) -> video dictrionaries from singledb
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

        return regex.search(metadata)

    def read_csv_create_regex(self) -> re:
        """
        Reads provided csv file of audit words and compiles regex for auditing

        :return: Regex of audit words
        """
        csv_reader = csv.reader(self.csv_file)
        escaped_audit_words = '|'.join([row[0] for row in csv_reader])

        return re.compile(escaped_audit_words)
