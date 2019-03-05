from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent import PersistentSegmentRelatedVideo
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent import PersistentSegmentChannel
from audit_tool.models import TopicAudit
from singledb.connector import SingleDatabaseApiConnector as Connector
from multiprocessing import Process
from multiprocessing import Manager
from multiprocessing import Lock
import re
import time
import logging
from django.utils import timezone

logger = logging.getLogger('topic_audit')

class TopicAudit(object):
    """
    Interface to run topic audits against our existing channels and videos.
        Retrieves all existing channel ids from PersistentSegmentRelated table
        Retrieves all videos for channels and audits videos
    """
    video_batch_size = 10000
    channel_batch_size = 40
    max_process_count = 8
    master_process_batch_size = 5000
    lock = Lock()
    running_topics = []

    def __init__(self, *args, **kwargs):
        self.connector = Connector()
        self.running_topics = self.get_topics_to_run()

    def run(self, *args, **kwargs):
        """
        Executes audit logic
            At the end of audit, checks whether to run audit again
        :param args:
        :param kwargs:
        :return:
        """
        logger.info('Starting topic audit...')

        start = time.time()
        all_channels = PersistentSegmentRelatedChannel\
            .objects\
            .order_by()\
            .values()\
            .distinct('related_id')

        processes = []

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

            PersistentSegmentRelatedChannel.objects.bulk_create(found_items['channels'])
            PersistentSegmentRelatedVideo.objects.bulk_create(found_items['videos'])

            # Add new topics that may have been added during last batch
            self.running_topics += self.get_topics_to_run(is_beginning=False)

            all_channels = all_channels[self.master_process_batch_size:]

        end = time.time()
        self.finalize_segments()

        logger.info('Audit complete for: {} \n Total execution time: {}'.format(self.topic_manager.title, end - start))

        # Finalize topics
        self.finzalize_topics(self.running_topics)

    def get_topics_to_run(self, is_beginning=False):
        """
        Checks the topic_should_stop flag to determine if the audit should run again
            This flag can be set through the shell or through another command
        :return:
        """
        topic_audits = []
        topics = Topic.objects.filter(from_beginning=False, is_running=False)

        # create new topic objects and set their values
        for topic in topics:
            topic.is_running = True
            topic.from_beginning = is_beginning
            topic.save()

            topic_audits.append(
                Topic(topic=topic)
            )

        return topic_audits

    def audit_channels(self, batch: list, found_items: Manager):
        """
        Function that is executed by each process.
            Retrieves and audits videos for given batch channel ids.
        :param batch: (list) Channel ids to retrieve and audit videos
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

            related_channels_to_create = []
            related_videos_to_create = []

            # Feed videos into each of the topics to audit
            for topic in self.running_topics:
                results = topic.audit_videos(videos, channel_batch_chunk)
                related_channels_to_create += results['channels']
                related_videos_to_create += results['videos']

            with self.lock:
                # Add items to create once all the processes have been joined
                found_items['channels'] += related_channels_to_create
                found_items['videos'] += related_videos_to_create

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

    @staticmethod
    def create_regex(keywords) -> re:
        """
        Reads provided csv file of audit words and compiles regex for auditing

        :return: Regex of audit words
        """
        # Coerce into list
        if type(keywords) == 'str':
            keywords = keywords.split(',')

        audit_keywords = '|'.join([keyword for keyword in keywords])

        return re.compile(audit_keywords)

    @staticmethod
    def video_details(video, found_words):
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

    def finalize_topics(self, topics):
        # If the topic is from beginning, save details
        # During next audit, these topics will not be filtered for
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
    def __init__(self, *args, **kwargs):
        keywords = kwargs['keywords']

        self.topic_manager = kwargs['topic']
        self.channel_segment_manager = self.topic_manager.channel_segment
        self.video_segment_manager = self.topic_manager.video_segment
        self.audit_regex = self.create_regex(keywords)

    def audit_videos(self, videos: list, channel_data: dict):
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

                if channel_found_words.get(channel_id) is None:
                    # List of all objects to create at end of batch since a channels videos could possibly be in different indicies
                    channel_found_words[channel_id] = channel_data[channel_id]
                    channel_found_words[channel_id]['details']['bad_words'] = found_words
                    channel_found_words[channel_id]['details']['category'] = channel_data[channel_id]['category']
                    channel_found_words[channel_id]['details']['title'] = channel_data[channel_id]['title']
                    channel_found_words[channel_id]['details']['thumbnail_image_url'] = channel_data[channel_id][
                        'thumbnail_image_url']

                else:
                    channel_found_words[channel_id]['details']['bad_words'] += found_words

        # Wait to create the found channels as we need to aggregate all the bad words for all videos in the batch
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

    def audit_video(self, video) -> bool:
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

        found = re.findall(self.audit_regex, metadata)

        return found