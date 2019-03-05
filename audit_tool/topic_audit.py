from segment.models.persistent import PersistentSegmentRelatedVideo
from segment.models.persistent import PersistentSegmentRelatedChannel
from singledb.connector import SingleDatabaseApiConnector as Connector
from multiprocessing import Process
from multiprocessing import Manager
from multiprocessing import Lock
import re
import time
import logging
from django.utils import timezone
from audit_tool.models import APIScriptTracker

logger = logging.getLogger('topic_audit')

class TopicAudit(object):
    """
    Interface to run topic audits against our existing channels and videos.
        Retrieves all existing channel ids from PersistentSegmentRelated table
        Retrieves all videos for channels and audits videos
    """
    video_batch_size = 10000
    channel_batch_size = 40
    max_process_count = 10
    master_process_batch_size = 5500
    lock = Lock()
    running_topics = []

    def __init__(self, *args, **kwargs):
        self.connector = Connector()
        self.script_tracker = APIScriptTracker.objects.get_or_create(name='TopicAudit')
        self.cursor = self.script_tracker.cursor
        self.running_topics = self.get_topics_to_run(self.cursor)

    def run(self, *args, **kwargs):
        """
        Executes audit logic
            After each master process batch, retrieves pending topics to run
        """
        logger.info('Starting topic audit...')

        # Start from persistent cursor
        start = time.time()
        all_channels = PersistentSegmentRelatedChannel\
            .objects\
            .order_by()\
            .values()\
            .distinct('related_id')[self.cursor:]

        processes = []

        while all_channels:
            # Batch size controlled by main process to distribute to other processes
            master_batch = all_channels[:self.master_process_batch_size]

            # Items found by each of the processes. Will actually be created after joining the processes after each master batch
            found_items = Manager().dict()
            found_items['channels'] = []
            found_items['videos'] = []

            # batch_limit will split all_channels evenly for each process
            batch_limit = len(master_batch) // self.max_process_count
            logger.info('Spawning {} processes...'.format(self.max_process_count))

            for _ in range(self.max_process_count):
                process_task = master_batch[:batch_limit]
                process = Process(
                    target=self.audit_channels,
                    kwargs={'batch': process_task, 'found_items': found_items}
                )
                processes.append(process)
                # Truncate master_batch for next process
                master_batch = master_batch[batch_limit:]
                process.start()

            for process in processes:
                process.join()

            """
            Create objects once all processes have been joined
            Each process can not individually create objects as they share the same DB connection (Will get SSL error)
            psycopg2.OperationalError: SSL error: decryption failed or bad record mac
            """
            PersistentSegmentRelatedChannel.objects.bulk_create(found_items['channels'])
            PersistentSegmentRelatedVideo.objects.bulk_create(found_items['videos'])

            self.update_cursor(update_value=len(master_batch))

            all_channels = all_channels[self.master_process_batch_size:]

            # Pick up new topics for next master batch
            self.running_topics += self.get_topics_to_run(is_beginning=False)

        # Audit has completed, reset cursor to 0 for next audit
        self.update_cursor(reset=True)

        end = time.time()

        # Finalize topics
        self.finzalize_topics(self.running_topics)

        logger.info('Audit complete for: {} \n Total execution time: {}'.format(self.topic_manager.title, end - start))

    def update_cursor(self, update_value=0, reset=False):
        """
        Updates the script cursor for persistent progress
        :param update_value: Value to increment self.script_tracker.cursor with
        :param reset: Flag for if the script has completed
            If True, then cursor should be reset to 0 to run audit from beginning the next
            time the audit is run
        :return:
        """
        if reset:
            self.script_tracker.cursor = 0
            self.script_tracker.save()
        else:
            # Update script cursor
            self.script_tracker.cursor = self.cursor + update_value
            self.script_tracker.save()
            self.cursor = self.script_tracker.cursor

    def get_topics_to_run(self, cursor):
        """
        Retrieves topics that should be run
        :param cursor: (int) Current progress of audit
        :return: (list)  Topic
        """
        topic_audits = []
        topics = Topic.objects.all().exclude(is_running=False)

        """
        If cursor has value of 0, then the audit is running from the beginning
            Else, the topics retrieved have started after the beginning and will
            be run again to parse channels it has skipped
        """
        for topic in topics:
            topic.from_beginning = True if cursor == 0 else False
            topic.save()

            topic_audits.append(
                Topic(topic=topic)
            )

        return topic_audits

    def audit_channels(self, batch: list, found_items: Manager) -> None:
        """
        Function that is executed by each process.
            Retrieves and audits videos for given batch channel ids.
        :param batch: (list) Channel ids to retrieve and audit videos
        :return: None
        """
        channel_batch = batch

        while channel_batch:
            # Batch channels as to not exceed ElasticSearch 10k results limit
            channel_batch_chunk = channel_batch[:self.channel_batch_size]

            # map list of channel data to dictionary for easier reference
            channel_batch_chunk = {
                channel['related_id']: channel
                for channel in channel_batch_chunk
            }

            channel_batch_chunk_ids = channel_batch_chunk.keys()

            videos = self.get_videos_batch(channel_ids=channel_batch_chunk_ids)

            related_channels_to_create = []
            related_videos_to_create = []

            # Feed videos into each of the topics
            for topic in self.running_topics:
                results = topic.audit_videos(videos, channel_batch_chunk)
                related_channels_to_create += results['channels']
                related_videos_to_create += results['videos']

            with self.lock:
                # Add items to create to shared dictionary for all processes
                found_items['channels'] += related_channels_to_create
                found_items['videos'] += related_videos_to_create

            channel_batch = channel_batch[self.channel_batch_size:]

    def get_videos_batch(self, channel_ids: list) -> list:
        """
        Retrieves all videos associated with channel_ids
        :param channel_ids: (list) Channel id strings
        :return: (list) video objects from singledb
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

    @staticmethod
    def create_regex(keywords) -> re:
        """
        Reads provided csv file of keywordss and compiles regex for auditing
        :param keywords: (list) keyword string
        :return: Regex of keywordss
        """
        # Coerce into list if string
        if type(keywords) == 'str':
            keywords = keywords.split(',')

        audit_keywords = '|'.join([keyword for keyword in keywords])

        return re.compile(audit_keywords)

    @staticmethod
    def video_details(video: dict, found_words: list) -> dict:
        """
        Returns dictionary of video data and found keywords
        :param video: (dict)
        :param found_words: (list)
        :return:
        """
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

    @staticmethod
    def finalize_topics(topics: list) -> None:
        """
        Finalize topic details if completed
        :param topics: (list)
        :return: None
        """

        """
        If the topic is from beginning, then audit for topic is complete and save its details
        During next audit, these topics will be excluded
        Topics with from_beginning is False will be run again during next audit to parse skipped channels
        """
        for topic in topics:
            if topic.from_beginning:
                topic.is_running = False
                topic.completed_at = timezone.now()
                topic.channel_segment.details = topic.channel_segment.calculate_details()
                topic.channel_video.details = topic.channel_segment.calculate_details()

                topic.save()
                topic.channel_segment.save()
                topic.video_segment.save()


class Topic(object):
    """
    Class to encapsulate specific audit logic for each of the topics
    """
    def __init__(self, *args, **kwargs):
        """
        :param args:
        :param kwargs: topic -> Topic object
        """
        keywords = kwargs['keywords']

        self.topic_manager = kwargs['topic']
        self.channel_segment_manager = self.topic_manager.channel_segment
        self.video_segment_manager = self.topic_manager.video_segment
        self.audit_regex = self.create_regex(keywords)

    def audit_videos(self, videos: list, channel_data: dict) -> dict:
        """
        Audits videos for the given topic
        :param videos: (list)
        :param channel_data: channel_data to reference to create new PersistentSegmentRelatedChannel objects
        :return: (dict) channels: PersistentSegmentRelatedChannel objects to create
                        videos: PersistentSegmentRelatedVideo objects to create
        """
        for video in videos:
            channel_id = video.get('channel_id')

            if not channel_id:
                continue

            channel_found_words = {}
            found_videos = []

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

                # Aggregate the results of channels as they may have many videos to parse
                if channel_found_words.get(channel_id) is None:
                    channel_found_words[channel_id] = channel_data[channel_id]
                    channel_found_words[channel_id]['details']['bad_words'] = found_words
                    channel_found_words[channel_id]['details']['category'] = channel_data[channel_id]['category']
                    channel_found_words[channel_id]['details']['title'] = channel_data[channel_id]['title']
                    channel_found_words[channel_id]['details']['thumbnail_image_url'] = channel_data[channel_id][
                        'thumbnail_image_url']

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

        results = {
            'channels': found_channels,
            'videos': found_videos
        }

        return results

    def audit_video(self, video: dict) -> bool:
        """
        Returns list of all found words
        :param video: (dict) video data
        :param regex: Compiled keywords regex
        :return: (list)  Found words
        """
        metadata = [
            video.get("title") or "",
            video.get("description") or "",
            video.get("tags") or "",
            video.get("transcript") or ""
        ]
        metadata = ' '.join(metadata)

        found = re.findall(self.audit_regex, metadata)

        return found