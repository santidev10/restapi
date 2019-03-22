from audit_tool.segmented_audit import SegmentedAudit
from utils.youtube_api import YoutubeAPIConnector
import csv
import re
from multiprocessing import Pool
import langid
from collections import Counter

class Audit(SegmentedAudit):
    max_process_count = 5
    video_chunk_size = 10000
    video_batch_size = 30000
    channel_batch_size = 1000
    channel_chunk_size = 10
    channel_row_data = {}
    max_csv_export_count = 50000
    youtube_max_channel_list_limit = 50
    csv_pages = {
        'is_brand_safety_videos': {'count': 0, 'page': 1},
        'not_brand_safety_videos': {'count': 0, 'page': 1},
        'blacklist_videos': {'count': 0, 'page': 1},
        'whitelist_videos': {'count': 0, 'page': 1},

        'is_brand_safety_channels': {'count': 0, 'page': 1},
        'not_brand_safety_channels': {'count': 0, 'page': 1},
        'blacklist_channels': {'count': 0, 'page': 1},
        'whitelist_channels': {'count': 0, 'page': 1},
    }

    video_csv_headers = ['Channel Name', 'Channel URL', 'Video Name', 'Video URL', 'Emoji Y/N', 'Views', 'Description', 'Category', 'Language', 'Country', 'Likes', 'Dislikes', 'Country', 'Keyword Hits']
    channel_csv_headers = ['Channel Title', 'Channel URL', 'Language', 'Category', 'Videos', 'Channel Subscribers', 'Total Views', 'Audited Videos', 'Total Likes', 'Total Dislikes', 'Country', 'Keyword Hits']

    def __init__(self, *args, **kwargs):
        super().__init__()

        # self.youtube_connector = YoutubeAPIConnector()
        self.audit_type = kwargs.get('type')
        self.csv_export_dir = kwargs.get('export')
        self.csv_export_title = kwargs.get('title')
        self.csv_keyword_path = kwargs.get('keywords')
        self.categories = {
            '1': 'Film & Animation',
            '2': 'Autos & Vehicles',
            '10': 'Music',
            '15': 'Pets & Animals',
            '17': 'Sports',
            '18': 'Short Movies',
            '19': 'Travel & Events',
            '20': 'Gaming',
            '21': 'Videoblogging',
            '22': 'People & Blogs',
            '23': 'Comedy',
            '24': 'Entertainment',
            '25': 'News & Politics',
            '26': 'Howto & Style',
            '27': 'Education',
            '28': 'Science & Technology',
            '29': 'Nonprofits & Activism',
            '30': 'Movies',
            '31': 'Anime / Animation',
            '32': 'Action / Adventure',
            '33': 'Classics',
            '34': 'Comedy',
            '37': 'Family',
            '42': 'Shorts',
            '43': 'Shows',
            '44': 'Trailers'
        }
        self.video_categories = {
            '1': 'Film & Animation',
            '10': 'Music',
            '15': 'Pets & Animals',
            '23': 'Comedy',
            '24': 'Entertainment',
            '26': 'Howto & Style',
            '27': 'Education',
            '34': 'Comedy',
            '37': 'Family',
        }

        self.video_id_regexp = re.compile('(?<=video/).*')
        self.channel_id_regexp = re.compile('(?<=channel/).*')
        self.username_regexp = re.compile('(?<=user/).*')
        self.whitelist_regexp = self.create_keyword_regexp(kwargs['whitelist']) if kwargs.get('whitelist') else None
        self.blacklist_regexp = self.create_keyword_regexp(kwargs['blacklist']) if kwargs.get('blacklist') else None
        self.brand_safety = kwargs.get('brand_safety')
        self.emoji_regexp = re.compile(u"["
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                   u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                   u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                   "]", flags=re.UNICODE)

    def create_keyword_regexp(self, csv_path):
        with open(csv_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)
            keywords = list(csv_reader)

            keyword_regexp = re.compile(
                '|'.join([word[0] for word in keywords]),
                re.IGNORECASE
            )

        return keyword_regexp

    def run(self):
        print('starting...')
        target, data_generator, result_processor, chunk_size = self.get_processor()

        self.run_processor(target, data_generator, result_processor, chunk_size=chunk_size)

    def related_video_audit(self, related_videos):

        pass

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

        # Channel batches = 1k, so every 1k channels data will write
        # Video batches = 10k, so every 10k videos will write
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
            'is_brand_safety_videos': {
                'results': [],
                'options': {
                    'data_type': 'video',
                    'audit_type': 'is_brand_safety',
                }
            },
            'not_brand_safety_videos': {
                'results': [],
                'options': {
                    'data_type': 'video',
                    'audit_type': 'not_brand_safety'
                }
            },
            'blacklist_videos': {
                'results': [],
                'options': {
                    'data_type': 'video',
                    'audit_type': 'blacklist'
                }
            },
            'whitelist_videos': {
                'results': [],
                'options': {
                    'data_type': 'video',
                    'audit_type': 'whitelist'
                }
            },

            'is_brand_safety_channels': {
                'results': [],
                'options': {
                    'data_type': 'channel',
                    'audit_type': 'is_brand_safety'
                }
            },
            'not_brand_safety_channels': {
                'results': [],
                'options': {
                    'data_type': 'channel',
                    'audit_type': 'not_brand_safety'
                }
            },
            'blacklist_channels': {
                'results': [],
                'options': {
                    'data_type': 'channel',
                    'audit_type': 'blacklist'
                }
            },
            'whitelist_channels': {
                'results': [],
                'options': {
                    'data_type': 'channel',
                    'audit_type': 'whitelist'
                }
            },
        }

        for result in results:
            all_results['is_brand_safety_videos']['results'] += result['is_brand_safety_videos']
            all_results['not_brand_safety_videos']['results'] += result['not_brand_safety_videos']
            all_results['blacklist_videos']['results'] += result['blacklist_videos']
            all_results['whitelist_videos']['results'] += result['whitelist_videos']

            all_results['is_brand_safety_channels']['results'] += result['is_brand_safety_channels']
            all_results['not_brand_safety_channels']['results'] += result['not_brand_safety_channels']
            all_results['blacklist_channels']['results'] += result['blacklist_channels']
            all_results['whitelist_channels']['results'] += result['whitelist_channels']

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
                channel_statistics_data = self.get_channel_statistics_with_video_id(response, connector)
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

        video_audit_results = self.audit_videos(all_videos)
        channel_audit_results = self.audit_channels(video_audit_results, connector)

        # sort channels based on their video keyword hits
        sorted_channels = self.sort_channels_by_keyword_hits(channel_audit_results)

        final_results.update(sorted_channels)
        final_results.update(video_audit_results)

        return final_results

    def get_channel_statistics_with_video_id(self, videos, connector):
        """
        Gets channel statistics for videos
        :param videos: (list) Youtube Video data
        :return: (dict) Mapping of channels and their statistics
        """
        channel_data = []
        cursor = 0

        # Can't mutuate videos as it's being used after this function call
        while True:
            if cursor >= len(videos):
                break

            batch = channel_data[cursor:self.youtube_max_channel_list_limit]
            cursor += len(batch)
            response = connector.obtain_channels(batch, part='statistics')
            channel_data += response['items']

        return channel_data

    @staticmethod
    def get_channel_id_for_username(username, connector):
        """
        Retrieves channel id for the given youtube username
        :param username: (str) youtube username
        :param connector: YoutubeAPIConnector instance
        :return: (str) channel id
        """
        response = connector.obtain_user_channels(username)

        try:
            channel_id = response.get('items')[0].get('id')

        except IndexError:
            print('Could not get channel id for: {}'.format(username))
            channel_id = None

        return channel_id

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

    @staticmethod
    def update_video_channel_subscribers(videos, channels):
        # Create dictionary of channel to subscriber count
        channel_subscribers = {
            channel['channelId']: channel['statistics']['subscriberCount']
            for channel in channels
        }

        for video in videos:
            channel_id = video['snippet']['channelId']
            video['statistics']['channelSubscriberCount'] = channel_subscribers[channel_id]

    def get_all_channel_video_data(self, channel_ids):
        all_results = []
        connector = YoutubeAPIConnector()

        for id in channel_ids:
            results = self.get_channel_videos(id, connector)
            all_results += results

        return all_results

    def write_data(self, data, data_type='video', audit_type='whitelist'):
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

        audit_types = {
            'not_brand_safety': 'brand_safety_hits',
            'whitelist': 'whitelist_hits',
            'blacklist': 'blacklist_hits',
        }

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
            self.get_keyword_count(video.get(audit_types.get(audit_type), []))
        ]

        return export_row

    def sort_channels_by_keyword_hits(self, channels):
        """
        Separte channels into audit categories for easier processing
        :param channels: (list) Audited Channel Youtube data
        :return: (dict) Sorted Channels based on on audit results
        """
        sorted_channels = {
            'is_brand_safety_channels': [],
            'not_brand_safety_channels': [],
            'blacklist_channels': [],
            'whitelist_channels': [],
        }

        for channel in channels:
            if not channel['aggregatedVideoData'].get('brand_safety_hits'):
                sorted_channels['is_brand_safety_channels'].append(channel)

            if channel['aggregatedVideoData'].get('brand_safety_hits'):
                sorted_channels['not_brand_safety_channels'].append(channel)

            if channel['aggregatedVideoData'].get('blacklist_hits'):
                sorted_channels['blacklist_channels'].append(channel)

            if not channel['aggregatedVideoData'].get('brand_safety_hits') \
                    and not channel['aggregatedVideoData'].get('blacklist_hits') \
                    and channel['aggregatedVideoData'].get('whitelist_hits'):
                sorted_channels['whitelist_channels'].append(channel)

        return sorted_channels

    def get_channel_export_row(self, channel, audit_type='whitelist'):
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
            metadata.get('country', 'Unknown')
        ]

        if audit_type == 'brand_safety':
            export_row.append(self.get_keyword_count(video_data.get('brand_safety_hits', [])))

        elif audit_type == 'whitelist':
            export_row.append(self.get_keyword_count(video_data.get('whitelist_hits', [])))

        elif audit_type == 'blacklist':
            export_row.append(self.get_keyword_count(video_data.get('blacklist_hits', [])))

        else:
            # Add more export types
            pass

        return export_row

    def video_data_generator(self):
        """
        Yields each row of csv
        :return: (dict)
        """
        with open(self.csv_file_path, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            # Pass over headers
            next(csv_reader, None)

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
        with open(self.csv_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            batch = []
            csv_reader = csv.DictReader(csv_file)

            # Pass over headers
            next(csv_reader, None)

            for row in csv_reader:
                batch.append(row)

                if len(batch) >= self.channel_batch_size:
                    yield batch
                    batch.clear()

            yield batch

    def get_channel_videos(self, channel_id, connector):
        """
        Retrieves all videos for given channel id from Youtube Data API
        :param channel_id: (str)
        :param connector: YoutubeAPIConnector instance
        :return: (list) Channel videos
        """
        channel_videos_full_data = []
        channel_videos = []
        response = connector.obtain_channel_videos(channel_id, part='snippet', order='viewCount', safe_search='strict')

        channel_videos += [video['id']['videoId'] for video in response.get('items')]
        next_page_token = response.get('nextPageToken')

        while next_page_token and response.get('items'):
            response = connector\
                .obtain_channel_videos(channel_id, part='snippet', page_token=next_page_token, order='viewCount', safe_search='strict')

            channel_videos += [video['id']['videoId'] for video in response.get('items')]
            next_page_token = response.get('nextPageToken')

        while channel_videos:
            video_full_data_batch = channel_videos[:50]

            response = connector.obtain_videos(','.join(video_full_data_batch), part='snippet,statistics')

            channel_videos_full_data += response.get('items')
            channel_videos = channel_videos[50:]

        return channel_videos_full_data

    @staticmethod
    def get_keyword_count(items):
        """
        Counts occurrences of items in list
        :param items: (list)
        :return: (str)
        """
        counted = Counter(items)
        return ', '.join(['{}: {}'.format(key, value) for key, value in counted.items()])

    def audit_videos(self, videos):
        """
        Audits videos and separates them depending on their audit result (brand safety, blacklist (optional), whitelist (optional)
        :param videos: (list) Youtubve video daata
        :return: (dict) Video audit results
        """
        results = {
            'whitelist_videos': [],
            'blacklist_videos': [],
            'not_brand_safety_videos': [],
            'is_brand_safety_videos': []
        }

        for video in videos:
            self.set_language(video)
            self.set_has_emoji(video)

            # Only add English videos and add if category matches mapping
            if video['snippet']['defaultLanguage'] != 'en' and not self.video_categories.get(
                    video['snippet']['categoryId']):
                continue

            brand_safety_hits = self._parse_item(video, self.bad_words_regexp)
            if brand_safety_hits:
                self.set_keyword_hits(video, brand_safety_hits, 'brand_safety_hits')
                results['not_brand_safety_videos'].append(video)

            else:
                results['is_brand_safety_videos'].append(video)

            # If provided, more bad keywords to filter against
            blacklist_hits = []
            if self.blacklist_regexp:
                blacklist_hits = self._parse_item(video, self.blacklist_regexp)
                if blacklist_hits:
                    self.set_keyword_hits(video, blacklist_hits, 'blacklist_hits')
                    results['blacklist_videos'].append(video)

            # If whitelist keywords provided, keywords to filter for
            if not brand_safety_hits and not self.blacklist_regexp and not blacklist_hits and self.whitelist_regexp:
                whitelist_hits = set(self._parse_item(video, self.whitelist_regexp))

                if whitelist_hits:
                    self.set_keyword_hits(video, whitelist_hits, 'whitelist_hits')
                    results['whitelist_videos'].append(video)

        return results

    def audit_channels(self, videos, connector):
        """
        Uses audited video data to extrapolate channel audit results
        :param videos: (list) Audited Youtube Videos
        :param connector: YoutubeAPIConnector instance
        :return: (list) Channel Youtube data with aggregated video audit results
        """
        if not videos:
            return []

        channel_data = {}

        for video in videos:
            channel_id = video['snippet']['channelId']
            channel_data[channel_id] = channel_data.get(channel_id, {})
            channel_data[channel_id]['aggregatedVideoData'] = channel_data[channel_id].get('aggregatedVideoData', {
                'totalLikes': 0,
                'totalDislikes': 0,
                'whitelist_hits': [],
                'blacklist_hits': [],
                'brand_safety_hits': []
            })
            channel_data[channel_id]['aggregatedVideoData']['totalAuditedVideos'] = channel_data[channel_id]['aggregatedVideoData'].get('totalAuditedVideos', 0) + 1
            channel_data[channel_id]['aggregatedVideoData']['totalLikes'] += int(video['statistics'].get('likeCount', 0))
            channel_data[channel_id]['aggregatedVideoData']['totalDislikes'] += int(video['statistics'].get('dislikeCount', 0))

            channel_data[channel_id]['aggregatedVideoData']['brand_safety_hits'] += video.get('brand_safety_hits', [])
            channel_data[channel_id]['aggregatedVideoData']['blacklist_hits'] += video.get('blacklist_hits', [])
            channel_data[channel_id]['aggregatedVideoData']['whitelist_hits'] += video.get('whitelist_hits', [])

            channel_data[channel_id]['categoryCount'] = channel_data[channel_id].get('categoryCount', [])
            channel_data[channel_id]['categoryCount'].append(video['snippet'].get('categoryId'))

            # if not channel_data[channel_id]['has_emoji']:
            #     channel_data[channel_id]['has_emoji'] = False
            #
            # else:
            #     if video['has_emoji']:
            #         channel_data[channel_id]['has_emoji'] = True

        channel_ids = list(channel_data.keys())

        while channel_ids:
            batch = ','.join(channel_ids[:50])
            response = connector.obtain_channels(batch, part='snippet,statistics').get('items')

            for item in response:
                channel_id = item['id']
                self.set_language(item)
                channel_data[channel_id]['type'] = 'channel'
                channel_data[channel_id]['channelId'] = channel_id
                channel_data[channel_id]['statistics'] = item['statistics']
                channel_data[channel_id]['snippet'] = item['snippet']

            channel_ids = channel_ids[50:]

        return list(channel_data.values())

    def set_has_emoji(self, item):
        item['has_emoji'] = bool(self._parse_item(item, self.emoji_regexp))

    @staticmethod
    def set_language(item):
        lang = item['snippet'].get('defaultLanguage')

        if lang is None:
            text = item['snippet']['title'] + ' ' + item['snippet']['description']
            item['snippet']['defaultLanguage'] = langid.classify(text)[0].lower()

    @staticmethod
    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    @staticmethod
    def _parse_item(item, regexp):
        item = item['snippet']

        text = ''
        text += item.get('title', '')
        text += item.get('description', '')
        text += item.get('channelTitle', '')
        text += item.get('transcript', '')

        if item.get('tags'):
            text += ' '.join(item['tags'])

        return re.findall(regexp, text)


    @staticmethod
    def set_keyword_hits(video, hits, keyword_type):
        video[keyword_type] = hits
