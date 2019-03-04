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
    max_process_count = 3
    master_process_batch_size = 1000
    lock = Lock()

    def __init__(self, *args, **kwargs):
        """
        :param args: None
        :param kwargs:
            (str) topic -> Topic model
            (PersistentSegmentChannel) channel_segment_manager
            (PersistentSegmentVideo) channel_segment_manager
            (list) keywords -> list of keywords read from csv
        """
        keywords = kwargs.get('keywords')

        self.topic_manager = kwargs.get('topic')
        self.channel_segment_manager = kwargs.get('channel_segment')
        self.video_segment_manager = kwargs.get('video_segment')
        self.audit_regex = self.create_regex(keywords)
        self.connector = Connector()

    def run(self, *args, **kwargs):
        self.topic_manager.should_start = False
        self.topic_manager.is_running = True
        self.topic_manager.save()

        start = time.time()

        print('Getting all channel ids...')
        all_channels = PersistentSegmentRelatedChannel\
            .objects\
            .order_by()\
            .values()\
            .distinct('related_id')

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

        # Checks whether this audit should run again
        self.check_should_run()

    def check_should_run(self):
        """
        Checks the topic_should_stop flag to determine if the audit should run again
            This flag can be set through the shell or through another command
        :return:
        """
        should_run = not self.topic.should_stop

        if should_run:
            self.run()

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
                channel['related_id']: channel
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

                    if channel_found_words.get(channel_id) is None:
                        # List of all objects to create at end of batch since a channels videos could possibly be in different indicies
                        channel_found_words[channel_id] = channel_batch_chunk[channel_id]
                        channel_found_words[channel_id]['details']['bad_words'] = found_words
                        channel_found_words[channel_id]['details']['category'] = channel_batch_chunk[channel_id]['category']
                        channel_found_words[channel_id]['details']['title'] = channel_batch_chunk[channel_id]['title']
                        channel_found_words[channel_id]['details']['thumbnail_image_url'] = channel_batch_chunk[channel_id]['thumbnail_image_url']


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
                found_items['channels'] += found_channels
                found_items['videos'] += found_videos

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

    def create_regex(self, keywords) -> re:
        """
        Reads provided csv file of audit words and compiles regex for auditing

        :return: Regex of audit words
        """
        audit_keywords = '|'.join([keyword for keyword in keywords])

        return re.compile(audit_keywords)

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
        persistent_channel_segment.save()

        persistent_video_segment = PersistentSegmentVideo.objects.get(title__contains=self.segment_title)
        persistent_video_segment.details = persistent_video_segment.calculate_details()
        persistent_video_segment.save()
