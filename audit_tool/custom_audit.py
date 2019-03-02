import csv
import re
import time
import logging

from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent import PersistentSegmentRelatedVideo
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentType
from singledb.connector import SingleDatabaseApiConnector as Connector
from multiprocessing import Process
from multiprocessing import Manager
from multiprocessing import Lock
from multiprocessing import Value


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
    master_process_batch_size = 1000
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

        print('Creating Persistent Channel and Video Segments: {}'.format(self.segment_title))

        self.persistent_channel_segment = PersistentSegmentChannel(
            title=self.create_segment_title(PersistentSegmentType.CHANNEL, self.segment_title),
            category=PersistentSegmentCategory.WHITELIST
        )
        self.persistent_video_segment = PersistentSegmentVideo(
            title=self.create_segment_title(PersistentSegmentType.VIDEO, self.segment_title),
            category=PersistentSegmentCategory.WHITELIST
        )
        self.persistent_channel_segment.save()
        self.persistent_video_segment.save()

    def create_segment_title(self, type, title):
        type = PersistentSegmentType.CHANNEL.capitalize() \
            if type == PersistentSegmentType.CHANNEL \
            else PersistentSegmentType.VIDEO.capitalize()

        return '{}s {} {}'.format(type, title, PersistentSegmentCategory.WHITELIST.capitalize())

    def run(self, *args, **kwargs):
        start = time.time()

        print('Getting all channel ids...')
        all_channels = PersistentSegmentRelatedChannel\
            .objects\
            .order_by()\
            .distinct('related_id')[:200]

        processes = []

        # Sanity check
        total_videos_audited = Value('i', 0)

        while all_channels:
            # Batch size controlled by main process to distribute to other processes
            master_batch = all_channels[:self.master_process_batch_size]

            # Items found by each of the processes. Will actually be created after joining the processes after each master batch
            found_items = Manager().dict()
            found_items['channels'] = []
            found_items['videos'] = []

            # batch_limit will split all_channel_ids evenly for each process
            batch_limit = len(master_batch) // self.max_process_count

            # Shared dictionary across processes to keep track of audit found channels that have already been added

            print('Spawning {} processes...'.format(self.max_process_count))

            for _ in range(self.max_process_count):
                process_task = master_batch[:batch_limit]
                process = Process(
                    target=self.audit_channels,
                    kwargs={'batch': process_task, 'counter': total_videos_audited, 'found_items': found_items}
                )
                processes.append(process)
                # Truncate master_batch for next process
                master_batch = master_batch[batch_limit:]
                process.start()

            for process in processes:
                process.join()


            PersistentSegmentRelatedChannel.objects.bulk_create(found_items['channels'])
            PersistentSegmentRelatedVideo.objects.bulk_create(found_items['videos'])

            all_channels = all_channels[self.master_process_batch_size:]

        end = time.time()

        # need to finalize results
        print('Finalizing segment details')
        self.finalize_segments()

        print('All videos audited: {}'.format(total_videos_audited.value))
        print('Total execution time: {}'.format(end - start))

    def audit_channels(self, batch: list, counter: Manager, found_items: Manager):
        """
        Function that is executed by each process.
            Retrieves and audits videos for given batch channel ids.
        :param batch: (list) Channel ids to retrieve and audit videos
        :param counter: Shared counter for processes
        :return: None
        """
        channel_batch = batch

        while channel_batch:
            channel_batch_chunk = channel_batch[:self.channel_batch_size]

            # map list of channel data to dictionary
            channel_batch_chunk = {
                channel.related_id: channel
                for channel in channel_batch_chunk
            }

            channel_batch_chunk_ids = channel_batch_chunk.keys()
            videos = self.get_videos_batch(channel_ids=channel_batch_chunk_ids)

            channel_found_words = {}
            found_videos = []

            for video in videos:
                channel_id = video.get('channel_id')

                if not channel_id:
                    continue

                found_words = self.audit_video(video, self.audit_regex)

                if found_words:
                    # Each video we find it should be created as related
                    found_videos.append(
                        PersistentSegmentRelatedVideo(
                            segment=self.persistent_video_segment,
                            related_id=video['video_id'],
                            category=video['category'],
                            title=video['title'],
                            thumbnail_image_url=video['thumbnail_image_url'],
                            details=self.video_details(video, found_words)
                        )
                    )

                    with self.lock:
                        if channel_found_words.get(channel_id) is None:
                            # List of all objects to create at end of batch since a channels videos could possibly be in different indicies
                            channel_found_words[channel_id] = channel_batch_chunk[channel_id].__dict__
                            channel_found_words[channel_id]['details']['bad_words'] = found_words

                        else:
                            channel_found_words[channel_id]['details']['bad_words'] += found_words

            found_channels = [
                PersistentSegmentRelatedChannel(
                    segment=self.persistent_channel_segment,
                    title=value['title'],
                    category=value['category'],
                    thumbnail_image_url=value['thumbnail_image_url'],
                    related_id=channel_id,
                    details=value['details']
                ) for channel_id, value in channel_found_words.items()
            ]

            with self.lock:
                found_items['channels'].extend(found_channels)
                found_items['videos'].extend(found_videos)

                counter.value += len(videos)
                print('Total videos audited: {}'.format(counter.value))

            channel_batch = channel_batch[self.channel_batch_size:]

    def get_videos_batch(self, channel_ids: list = None) -> list:
        """
        Retrieves all videos associated with channel_ids
        :param channel_ids: (list) -> Channel id strings
        :return: (list) -> video objects from singledb
        """

        params = dict(
            fields="video_id,channel_id,title,description,tags,thumbnail_image_url,category,likes,dislikes,views,"
                   "language,transcript",
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

    def video_details(self, video, found_words):
        details = dict(
            likes=video["likes"],
            dislikes=video["dislikes"],
            views=video["views"],
            tags=video["tags"],
            description=video["description"],
            language=video["language"],
            bad_words=found_words,
        )
        return details

    def finalize_segments(self):
        persistent_channel_segment = PersistentSegmentChannel.objects.get(title__contains=self.segment_title)
        persistent_channel_segment.details = persistent_channel_segment.calculate_details()

        persistent_video_segment = PersistentSegmentVideo.objects.get(title__contains=self.segment_title)
        persistent_video_segment.details = persistent_video_segment.calculate_details()
