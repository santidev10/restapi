from .audit_mixin import AuditMixin
from utils.youtube_api import YoutubeAPIConnector
import csv
from multiprocessing import Pool
from . import audit_constants as constants

class Audit(AuditMixin):
    max_process_count = 5
    video_chunk_size = 10000
    video_batch_size = 30000
    channel_batch_size = 1000
    channel_chunk_size = 10
    channel_row_data = {}
    max_csv_export_count = 50000
    csv_pages = {
        constants.BRAND_SAFETY_PASS_VIDEOS: {'count': 0, 'page': 1},
        constants.BRAND_SAFETY_FAIL_VIDEOS: {'count': 0, 'page': 1},
        constants.BLACKLIST_VIDEOS: {'count': 0, 'page': 1},
        constants.WHITELIST_VIDEOS: {'count': 0, 'page': 1},

        constants.BRAND_SAFETY_FAIL_CHANNELS: {'count': 0, 'page': 1},
        constants.BRAND_SAFETY_PASS_CHANNELS: {'count': 0, 'page': 1},
        constants.WHITELIST_CHANNELS: {'count': 0, 'page': 1},
        constants.BLACKLIST_CHANNELS: {'count': 0, 'page': 1},
    }

    video_csv_headers = ['Channel Name', 'Channel URL', 'Video Name', 'Video URL', 'Emoji Y/N', 'Views', 'Description', 'Category', 'Language', 'Country', 'Likes', 'Dislikes', 'Country', 'Keyword Hits']
    channel_csv_headers = ['Channel Title', 'Channel URL', 'Language', 'Category', 'Videos', 'Channel Subscribers', 'Total Views', 'Audited Videos', 'Total Likes', 'Total Dislikes', 'Country', 'Keyword Hits']

    def __init__(self, *args, **kwargs):
        super().__init__()

        # self.youtube_connector = YoutubeAPIConnector()
        self.audit_type = kwargs.get('type')
        self.csv_source_file_path = kwargs.get('file')
        self.csv_export_dir = kwargs.get('export')
        self.csv_export_title = kwargs.get('title')
        self.whitelist_regexp = self.read_and_create_keyword_regexp(kwargs['whitelist']) if kwargs.get('whitelist') else None
        self.blacklist_regexp = self.read_and_create_keyword_regexp(kwargs['blacklist']) if kwargs.get('blacklist') else None
        self.brand_safety_tags_regexp = self.compile_audit_regexp(self.get_all_bad_words())

    def run(self):
        print('starting...')
        target, data_generator, result_processor, chunk_size = self.get_processor()

        self.run_processor(target, data_generator, result_processor, chunk_size=chunk_size)

    def get_processor(self):
        if self.audit_type == 'video':
            return self.process_videos, self.video_data_generator, self.process_results, self.video_chunk_size

        elif self.audit_type == 'channel':
            return self.process_channels, self.channel_data_generator, self.process_results, self.channel_chunk_size

        else:
            print('Audit type not supported: {}'.format(self.audit_type))

    def run_processor(self, target, generator, result_processor, chunk_size=5000):
        pool = Pool(processes=self.max_process_count)
        items_seen = 0

        for batch in generator():
            chunks = self.chunks(batch, chunk_size)
            all_results = pool.map(target, chunks)

            result_processor(all_results)

            items_seen += len(batch)
            print('Seen {} {}s'.format(items_seen, self.audit_type))

        print('Complete!')

    def process_results(self, results: list):
        """
        Extracts results from results (nested lists from processes) and writes data
        :param results:
        :return:
        """
        all_results = {
            constants.BRAND_SAFETY_PASS_VIDEOS: {
                'results': [],
                'options': {
                    'data_type': 'video',
                    'audit_type': constants.BRAND_SAFETY_PASS,
                }
            },
            constants.BRAND_SAFETY_FAIL_VIDEOS: {
                'results': [],
                'options': {
                    'data_type': 'video',
                    'audit_type': constants.BRAND_SAFETY_FAIL
                }
            },
            constants.BLACKLIST_VIDEOS: {
                'results': [],
                'options': {
                    'data_type': 'video',
                    'audit_type': constants.BLACKLIST
                }
            },
            constants.WHITELIST_VIDEOS: {
                'results': [],
                'options': {
                    'data_type': 'video',
                    'audit_type': constants.WHITELIST
                }
            },
            constants.BRAND_SAFETY_PASS_CHANNELS: {
                'results': [],
                'options': {
                    'data_type': 'channel',
                    'audit_type': constants.BRAND_SAFETY_PASS,
                }
            },
            constants.BRAND_SAFETY_FAIL_CHANNELS: {
                'results': [],
                'options': {
                    'data_type': 'channel',
                    'audit_type': constants.BRAND_SAFETY_FAIL
                }
            },
            constants.BLACKLIST_CHANNELS: {
                'results': [],
                'options': {
                    'data_type': 'channel',
                    'audit_type': constants.BLACKLIST
                }
            },
            constants.WHITELIST_CHANNELS: {
                'results': [],
                'options': {
                    'data_type': 'channel',
                    'audit_type': constants.WHITELIST
                }
            },
        }

        for result in results:
            all_results[constants.BRAND_SAFETY_PASS_VIDEOS]['results'] += result[constants.BRAND_SAFETY_PASS_VIDEOS]
            all_results[constants.BRAND_SAFETY_FAIL_VIDEOS]['results'] += result[constants.BRAND_SAFETY_FAIL_VIDEOS]
            all_results[constants.BLACKLIST_VIDEOS]['results'] += result[constants.BLACKLIST_VIDEOS]
            all_results[constants.WHITELIST_VIDEOS]['results'] += result[constants.WHITELIST_VIDEOS]

            all_results[constants.BRAND_SAFETY_PASS_CHANNELS]['results'] += result[constants.BRAND_SAFETY_PASS_CHANNELS]
            all_results[constants.BRAND_SAFETY_FAIL_CHANNELS]['results'] += result[constants.BRAND_SAFETY_FAIL_CHANNELS]
            all_results[constants.BLACKLIST_CHANNELS]['results'] += result[constants.BLACKLIST_CHANNELS]
            all_results[constants.WHITELIST_CHANNELS]['results'] += result[constants.WHITELIST_CHANNELS]

        for result in all_results.values():
            self.write_data(result['results'], **result['options'])

    def process_videos(self, csv_videos):
        """
        Manager to handle video audit process for video csv data
        :param csv_videos: (generator) Yields list of csv video data
        :return:
        """
        final_results = {}
        all_videos = []
        connector = YoutubeAPIConnector()

        while csv_videos:
            # Needs to be 50 for youtube limit
            batch = csv_videos[:50]
            batch_ids = ','.join([video.get('video_id') for video in batch])

            response = connector.obtain_videos(batch_ids, part='snippet,statistics').get('items')

            if not video.get('channel_subscribers'):
                channel_statistics_data = self.get_channel_statistics_with_video_data(response, connector)
                video_channel_subscriber_ref = {
                    channel['id']: channel['statistics']['subscribers']
                    for channel in channel_statistics_data
                }

            else:
                # Provided csv video data will have channel subscribers
                video_channel_subscriber_ref = {
                    video.get('video_id'): video.get('channel_subscribers')
                    for video in batch
                }

            for video in response:
                video['statistics']['channelSubscriberCount'] = video_channel_subscriber_ref[video['id']]

            all_videos += response
            csv_videos = csv_videos[50:]

        video_audit_results = self.audit_videos(all_videos, blacklist_regexp=self.blacklist_regexp, whitelist_regexp=self.whitelist_regexp)
        channel_audit_results = self.audit_channels(video_audit_results, connector)

        # sort channels based on their video keyword hits
        sorted_channels = self.sort_channels_by_keyword_hits(channel_audit_results)

        final_results.update(sorted_channels)
        final_results.update(video_audit_results)

        return final_results

    def process_channels(self, csv_channels):
        """
        Processes videos and aggregates video data to audit channels
            final_results stores the results of both channel audit and video audit
        :param csv_channels: (generator) Yields csv rows
        :return:
        """
        final_results = {}
        all_videos = []
        connector = YoutubeAPIConnector()

        for row in csv_channels:
            channel_id = self.channel_id_regexp.search(row.get('channel_url'))

            if channel_id:
                channel_id = channel_id.group()

            else:
                # If no channel id, then get user name to retrieve channel id
                username = self.username_regexp.search(row.get('channel_url')).group()
                channel_id = self.get_channel_id_for_username(username, connector)

                if not channel_id:
                    continue

            channel_videos = self.get_channel_videos(channel_id, connector)
            all_videos += channel_videos

        # audit_videos func sets keyword hits key on each video and returns sorted videos
        video_audit_results = self.audit_videos(all_videos)

        all_video_audit_results = sum(video_audit_results.values(), [])

        # audit_channels aggregates all the videos for each channels
        channel_audit_results = self.audit_channels(all_video_audit_results, connector)

        self.update_video_channel_subscribers(all_video_audit_results, channel_audit_results)

        # sort channels based on their video keyword hits
        sorted_channels = self.sort_channels_by_keyword_hits(channel_audit_results)

        final_results.update(sorted_channels)
        final_results.update(video_audit_results)

        return final_results

    def write_data(self, data, data_type='video', audit_type=constants.WHITELIST):
        """
        Writes data to csv
            Creates new page of csv if row count surpasses max csv export size
        :param data: (list) data to write
        :param data_type: video or channel (to get export row)
        :param audit_type: is_brand_safety, not_brand_safety, whitelist, blacklist
        :return: None
        """
        if not data:
            print('No data for: {} {}'.format(data_type, audit_type))
            return

        # Sort items by channel title
        sort_key = 'channelId' if data_type == 'video' else 'title'
        data.sort(key=lambda item: item['snippet'][sort_key])

        while True:
            next_page = False
            csv_pages_key = '{}_{}s'.format(audit_type, data_type)
            export_path = '{dir}Page{page}{title}{data_type}{audit_type}.csv'.format(
                page=self.csv_pages[csv_pages_key]['page'],
                dir=self.csv_export_dir,
                title=self.csv_export_title,
                data_type=data_type,
                audit_type=audit_type.capitalize(),
            )

            with open(export_path, mode='a') as export_file:
                writer = csv.writer(export_file, delimiter=',')

                if self.csv_pages[csv_pages_key]['count'] == 0:
                    if data_type == 'video':
                        writer.writerow(self.video_csv_headers)

                    else:
                        writer.writerow(self.channel_csv_headers)

                for index, item in enumerate(data):
                    row = self.get_video_export_row(item, audit_type) if data_type == 'video' else self.get_channel_export_row(item, audit_type)
                    writer.writerow(row)
                    self.csv_pages[csv_pages_key]['count'] += 1

                    # Create a new page (csv file) to write results to if count exceeds csv export limit
                    if self.csv_pages[csv_pages_key]['count'] >= self.max_csv_export_count:
                        self.csv_pages[csv_pages_key]['page'] += 1
                        data = data[index + 1:]
                        next_page = True
                        break

            if next_page is False:
                break

    def get_video_export_row(self, video, audit_type, csv_data={}):
        metadata = video['snippet']
        statistics = video['statistics']

        export_row = [
            metadata.get('channelTitle'),
            'http://www.youtube.com/channel/' + metadata['channelId'],
            metadata['title'],
            'http://www.youtube.com/video/' + video['id'],
            video['has_emoji'],
            statistics.get('viewCount', 0),
            metadata['description'],
            self.categories.get(metadata['categoryId']),
            metadata.get('defaultLanguage'),
            statistics.get('likeCount', 0),
            statistics.get('dislikeCount', 0),
            statistics.get('commentCount', 0),
            csv_data.get('channel_subscribers') or statistics['channelSubscriberCount'],
            self.get_keyword_count(video.get(self.audit_keyword_hit_mapping.get(audit_type), []))
        ]

        return export_row

    def get_channel_export_row(self, channel, audit_type=constants.WHITELIST):
        """
        Get channel csv export row
        :param channel: Audited Channel
        :param audit_type: is_brand_safety, not_brand_safety, whitelist, blacklist
        :return: (list) Export row
        """
        channel_id = channel['channelId']
        metadata = channel['snippet']
        statistics = channel['statistics']
        video_data = channel['aggregatedVideoData']
        channel_category_id = max(channel['categoryCount'], key=channel['categoryCount'].count)

        export_row = [
            metadata['title'],
            'http://www.youtube.com/channel/' + channel_id,
            metadata['defaultLanguage'],
            self.categories[channel_category_id],
            statistics['videoCount'],
            statistics['subscriberCount'],
            statistics['viewCount'],
            video_data['totalAuditedVideos'],
            video_data['totalLikes'],
            video_data['totalDislikes'],
            metadata.get('country', 'Unknown'),
            self.get_keyword_count(video_data.get(self.audit_keyword_hit_mapping.get(audit_type), []))
        ]

        return export_row

    def video_data_generator(self):
        """
        Yields each row of csv
        :return: (dict)
        """
        with open(self.csv_source_file_path, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            rows_batch = []

            for row in csv_reader:
                video_id = self.video_id_regexp.search(row.pop('url')).group()
                # rows_batch[video_id] = row
                row['video_id'] = video_id
                rows_batch.append(row)

                if len(rows_batch) >= self.video_batch_size:
                    yield rows_batch
                    rows_batch.clear()

            yield rows_batch

    def channel_data_generator(self):
        """
        Yields each row of csv
        :return: (dict)
        """
        with open(self.csv_source_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            batch = []
            csv_reader = csv.DictReader(csv_file)

            for row in csv_reader:
                batch.append(row)

                if len(batch) >= self.channel_batch_size:
                    yield batch
                    batch.clear()

            yield batch


