from segment.models.persistent import PersistentSegmentRelatedVideo
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from singledb.connector import SingleDatabaseApiConnector as Connector
from multiprocessing import Process
from multiprocessing import Manager
from multiprocessing import Lock
import re
import time
import logging
from django.utils import timezone
from audit_tool.models import APIScriptTracker
from audit_tool.models import TopicAudit as TopicAuditModel
from django.db.utils import IntegrityError as DjangoIntegrityError
from psycopg2 import IntegrityError as PostgresIntegrityError

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
    master_process_batch_size = 5000
    lock = Lock()

    def __init__(self, *args, **kwargs):
        self.connector = Connector()
        tracker, _ = APIScriptTracker.objects.get_or_create(name='TopicAudit')
        self.script_tracker = tracker

        # Init the cursor with db value
        self.script_cursor = self.script_tracker.cursor
        self.running_topics = self.get_topics_to_run(self.script_cursor)

    def run(self, *args, **kwargs):
        """
        Executes audit logic
            After each master process batch, retrieves pending topics to run
        """

        if not self.running_topics:
            logger.info('No topics to run.')
            return

        logger.info('Starting topic audit with {} processes...'.format(self.max_process_count))

        # Start from persistent cursor
        start = time.time()
        all_channels = PersistentSegmentRelatedChannel\
            .objects\
            .order_by()\
            .values()\
            .distinct('related_id')[self.script_cursor:]

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

            # Spawn processes and distribute even work
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
            # If the event of existing objects (since topics may need to be run again), extract new objects
            if found_items['channels']:
                try:
                    PersistentSegmentRelatedChannel.objects.bulk_create(found_items['channels'])
                except DjangoIntegrityError or PostgresIntegrityError:
                    unique_items = self.get_unique_items(PersistentSegmentRelatedChannel, found_items['channels'])
                    PersistentSegmentRelatedChannel.objects.bulk_create(unique_items)

            if found_items['videos']:
                try:
                    PersistentSegmentRelatedVideo.objects.bulk_create(found_items['videos'])
                except DjangoIntegrityError or PostgresIntegrityError:
                    unique_items = self.get_unique_items(PersistentSegmentRelatedVideo, found_items['videos'])
                    PersistentSegmentRelatedVideo.objects.bulk_create(unique_items)

            self.update_script_cursor(update_value=self.master_process_batch_size)

            all_channels = all_channels[self.master_process_batch_size:]

            # check for topics that have passed their start cursors
            self.check_topic_cursors(self.script_cursor)

            # Pick up new topics for next master batch
            new_topics = self.get_topics_to_run(self.script_cursor)
            already_running = [topic.topic_manager for topic in self.running_topics]

            self.running_topics += [topic for topic in new_topics if topic.topic_manager.title not in already_running]

        # Audit has completed, reset cursor to 0 for next audit
        self.update_script_cursor(reset=True)

        end = time.time()

        # Finalize topics
        self.finalize_topics(self.running_topics)

        completed_topics = []
        topics_to_rerun = []

        for topic in self.running_topics:
            if topic.topic_manager.from_beginning:
                completed_topics.append(topic.topic_manager.title)
            else:
                topics_to_rerun.append(topic.topic_manager.title)

        logger.info('Audit complete. Total execution time: {}'.format(end - start))

        if completed_topics:
            logger.info('Completed topics: {}'.format(', '.join(completed_topics)))
        if topics_to_rerun:
            logger.info('Completed topics: {}'.format(', '.join(topics_to_rerun)))

    def get_unique_items(self, manager, items):
        new_items = []

        for item in items:
            try:
                manager.objects.get(related_id=item.related_id)
            except manager.DoesNotExist:
                new_items.append(item)

        return new_items

    def check_topic_cursors(self, script_cursor):
        """
        Checks if the script_cursor has passed a topic's start cursor.
            If True, then topic has seen all items in database and can complete the topic for efficiency
        :param script_cursor: (int)
        :return: None
        """
        incomplete_topics = []
        completed_topics = []
        for topic in self.running_topics:
            if topic.topic_manager.start_cursor >= script_cursor:
                topic.topic_manager.from_beginning = True

                completed_topics.append(topic)

            else:
                incomplete_topics.append(topic)

        self.finalize_topics(completed_topics)
        self.running_topics = incomplete_topics

    def update_script_cursor(self, update_value=0, reset=False):
        """
        Updates the script cursor for persistent progress
        :param update_value: Value to increment cursor with
        :param reset: Flag for if the script has completed
            If True, then cursor should be reset to 0 to run audit from beginning the next
            time the audit is run
        :return: None
        """
        if reset:
            self.script_tracker.cursor = 0
            self.script_tracker.save()
        else:
            self.script_tracker.cursor = self.script_cursor + update_value
            self.script_tracker.save()

        self.script_cursor = self.script_tracker.cursor

    def get_topics_to_run(self, cursor):
        """
        Retrieves topics that should be run
        :param cursor: (int) Current progress of audit
        :return: (list)  Topic
        """
        all_running_topic_audits = TopicAuditModel.objects.all().exclude(is_running=False)
        already_running = [topic.topic_manager.title for topic in self.running_topics]
        new_topic_audits = []

        """
        If cursor has value of 0, then the audit is running from the beginning
            Else, the topics retrieved have started after the beginning and will
            be run again to parse channels it has skipped
        """
        for topic_audit in all_running_topic_audits:
            if topic_audit.title not in already_running:
                topic_audit.from_beginning = True if cursor == 0 else False
                topic_audit.start_cursor = cursor
                topic_audit.save()

                keywords = topic_audit.keywords.all().values_list('keyword', flat=True)

                new_topic_audits.append(
                    Topic(topic=topic_audit, keywords=keywords)
                )

        return new_topic_audits

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
            if topic.topic_manager.from_beginning:
                topic.topic_manager.is_running = False
                topic.topic_manager.completed_at = timezone.now()
                topic.channel_segment_manager.details = topic.channel_segment_manager.calculate_details()
                topic.video_segment_manager.details = topic.video_segment_manager.calculate_details()

                topic.topic_manager.save()
                topic.channel_segment_manager.save()
                topic.video_segment_manager.save()


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
        channel_found_words = {}
        found_videos = []

        for video in videos:
            channel_id = video.get('channel_id')

            if not channel_id:
                continue

            found_words = self.audit_video(video)

            if found_words:
                # Each video we find it should be created as related
                found_videos.append(
                    PersistentSegmentRelatedVideo(
                        segment=self.video_segment_manager,
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
                segment=self.channel_segment_manager,
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

    def get_unique_items(self, manager, items):
        existing_ids = manager.related.all().values_list('related_id', flat=True)
        items_to_create = [item for item in items if item.related_id not in existing_ids]

        return items_to_create

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
