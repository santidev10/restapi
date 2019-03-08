import csv
import re
from segment.models.persistent import PersistentSegmentRelatedChannel
from singledb.connector import SingleDatabaseApiConnector as Connector
from utils.youtube_api import YoutubeAPIConnector
from utils.youtube_api import YoutubeAPIConnectorException
from audit_tool.audit_mixin import AuditMixin
from audit_tool.models import YoutubeUser
from audit_tool.models import Comment


class CommentAudit(AuditMixin):
    max_thread_count = 5
    channel_batch_limit = 40
    comment_batch_size = 50000
    video_fields = 'video_id,channel_id,title'
    csv_file_path = '/Users/kennethoh/Desktop/comments.csv'

    def __init__(self):
        self.connector = Connector()
        self.youtube_connector = YoutubeAPIConnector()

        bad_words = self.get_all_bad_words()

        self.bad_words_regexp = self.compile_audit_regexp(bad_words)
        self.timestamp_regexp = self.compile_timestamp_regexp()

    def get_videos_generator(self, channels):
        """
        Generates batches of video ids with given channels
        :param channels: (list) channel dicts
        :return: (list) video dicts
        """
        last_id = None
        channel_ids = ','.join(channels)
        params = dict(
            fields=self.video_fields,
            sort='video_id',
            size=5,
            channel_id__terms=channel_ids,
        )

        while True:
            params['video_id__range'] = '{},'.format(last_id or '')
            response = self.connector.execute_get_call('videos/', params)

            videos = [item for item in response.get('items', []) if item['video_id'] != last_id]

            if not videos:
                break

            yield videos
            last_id = videos[-1]['video_id']

    def run(self):
        """
        Handles main script logic
            Retrieves batches of videos to retrieve comments for and save
        :return:
        """
        comment_batch = []

        top_10k_channels = self.get_top_10k_channels()
        video_batch = next(self.get_videos_generator(top_10k_channels))

        while video_batch:
            video_comment_ref = {}

            for video in video_batch:
                comments = self.get_video_comments(video)
                comment_ids_with_replies = []

                for comment in comments:
                    is_top_level = comment['snippet'].get('topLevelComment')

                    # Get comment ids to retrieve if comment has replies
                    if is_top_level and comment['snippet']['topLevelComment'] > 0:
                        comment_ids_with_replies.append(comment['id'])

                    comment = comment['snippet'].get('topLevelComment') if is_top_level else comment['snippet']

                    if comment.get('videoId'):
                        video_comment_ref[comment['id']] = comment['videoId']

                    # Need to immediately get or create to provide for new comment creation
                    youtube_user, _ = YoutubeUser(
                                name=comment['authorDisplayName'],
                                channel_id=comment['authorChannelId']['value'],
                                thumbnail_image_url=comment['authorProfileImageUrl'],
                            )

                    found_words, found_time_stamps = self.parse_comment(comment)

                    found_items = {
                        'found_words': found_words,
                        'found_time_stamps': found_time_stamps
                    }

                    comment_batch.append(
                        Comment(
                            user=youtube_user,
                            id=comment['id'],
                            parent_id=comment.get('parentId'),
                            text=comment['snippet']['textOriginal'],
                            video_id=comment['snippet']['videoId'],
                            like_count=comment['snippet']['likeCount'],
                            reply_count=comment['snippet']['replyCount'],
                            published_at=comment['snippet']['publishedAt'],
                            is_top_level=is_top_level,
                            found_items=found_items
                        )
                    )

                # Get comment replies and append to current loop to reuse comment / youtube user creation logic
                # TODO: Comment replies do not have video id on them
                if comment_ids_with_replies:
                    for comment_id in comment_ids_with_replies:
                        replies = self.get_comment_replies(comment_id)

                        for reply in replies:
                            parent_id = reply['snippet']['parentId']
                            reply['videoId'] = video_comment_ref[parent_id]

                        comments += replies

            video_batch = next(self.get_videos_generator(top_10k_channels))

            if len(comment_batch) >= self.comment_batch_size:
                Comment.objects.bulk_create(comment_batch)

                comment_batch.clear()

        print('Complete')

    def get_comment_replies(self, parent_id):
        """
        Gets replies of comment with parent id
        :param parent_id: (str)
        :return: (list)
        """
        response = self.youtube_connector.get_video_comment_replies(parent_id)

        return response.get('items')

    def parse_comment(self, comment):
        """
        Finds and returns matches for bad words in comment text
        :param comment: (dict)
        :return: Found words, time stamp
        """
        text = comment.get('textOriginal')

        found_bad_words = re.findall(self.bad_words_regexp, text)
        found_time_stamps = re.findall(self.timestamp_regexp, text)

        return found_bad_words, found_time_stamps

    def get_top_10k_channels(self):
        """
        Get top 10k channels based on subscriber count
        :return: (list) channel dicts
        """
        all_channels = list(PersistentSegmentRelatedChannel.objects.all().distinct().exclude(details__subscribers__isnull=True).values('related_id', 'details'))
        top_10k = sorted(all_channels, key=lambda obj: obj['details'].get('subscribers'), reverse=True)
        top_10k_ids = [channel.get('related_id') for channel in top_10k]

        return top_10k_ids

    def get_video_comments(self, video, max_page_count=3):
        """
        Retrieves up to three pages of comments for a given video
        :param video:
        :return: (list) comment dicts
        """
        video_id = video['video_id']
        video_comments = []

        comment_reply_ids = []

        try:
            response = self.youtube_connector.get_video_comments(video_id=video_id)
            video_comments += response.get('items')

        except YoutubeAPIConnectorException:
            print('Unable to get comments for: ', video_id)

        next_page_token = response.get('nextPageToken')
        page = 1

        while next_page_token and page < max_page_count:
            try:
                response = self.youtube_connector.get_video_comments(video_id=video_id, page_token=next_page_token)

            except YoutubeAPIConnectorException:
                print('Unable to get comments for: ', video_id)

            video_comments += response.get('items')
            next_page_token = response.get('nextPageToken')

            page += 1

        return video_comments

    def compile_timestamp_regexp(self):
        regexp = re.compile(r'^([0-9]{0,2}):([0-9]{1,2})')

        return regexp

    def write_comments(self, comments):
        with open(self.csv_file_path, mode='a') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', )

            for comment in comments:
                comment = comment['snippet']['topLevelComment']
                row = [
                    comment['id'],
                    comment['snippet']['videoId'],
                    comment['snippet']['authorDisplayName'],
                    comment['snippet']['authorProfileImageUrl'],
                    comment['snippet']['textOriginal'],
                    comment['snippet']['publishedAt'],
                ]
                if comment['snippet'].get('authorChannelId'):
                    row.append(comment['snippet']['authorChannelId']['value'])
                writer.writerow(row)

    def audit_comments(self):
        all_comments = Comment.objects.all().order_by('video_id')
        comment_found_words = {}




'''
YT Response (video comment)
{
    'kind': 'youtube#commentThread',
    'etag': '\'XpPGQXPnxQJhLgs6enD_n8JR4Qk/WvDpdKVNFePdEZv8phCQQt-aBo4\'',
    'id': 'UgxRuZMZIJmdiuQEpHJ4AaABAg',
    'snippet': {
        'videoId': '-F1NwLNr0h0',
        'topLevelComment': {
            'kind': 'youtube#comment',
            'etag': '\'XpPGQXPnxQJhLgs6enD_n8JR4Qk/NpJP_hrJkDbxqNrGXdsSXOt9rE8\'',
            'id': 'UgxRuZMZIJmdiuQEpHJ4AaABAg',
            'snippet': {
                'authorDisplayName': 'Marshmello',
                'authorProfileImageUrl': 'https://yt3.ggpht.com/-2ecBJ0Rt1QM/AAAAAAAAAAI/AAAAAAAAAAA/7iC-PVcjk3Y/s28-c-k-no-mo-rj-c0xffffff/photo.jpg',
                'authorChannelUrl': 'http://www.youtube.com/channel/UCEdvpU2pFRCVqU6yIPyTpMQ',
                'authorChannelId': {
                    'value': 'UCEdvpU2pFRCVqU6yIPyTpMQ'
                },
                'videoId': '-F1NwLNr0h0',
                'textDisplay': '<b>FRIENDS - OFFICIAL MUSIC VIDEO | OUT NOW</b> <a href=\'https://youtu.be/jzD_yyEcp0M\'>https://youtu.be/jzD_yyEcp0M</a>',
                'textOriginal': '*FRIENDS - OFFICIAL MUSIC VIDEO | OUT NOW* https://youtu.be/jzD_yyEcp0M',
                'canRate': true,
                'viewerRating': 'none',
                'likeCount': 483,
                'publishedAt': '2018-02-16T12:56:49.000Z',
                'updatedAt': '2018-02-16T12:56:49.000Z'
            }
        },
        'canReply': true,
        'totalReplyCount': 45,
        'isPublic': true
    }
}



'''


# REPLY
# [
#    {
#       'kind':'youtube#comment',
#       'etag':'"XpPGQXPnxQJhLgs6enD_n8JR4Qk/ajV8gKBFNVGNDRa8jrsqXIVOTf8"',
#       'id':'UgwRdzPi9YZJ3uvQza14AaABAg.8pOio75OEEG8pOjKjdJ0mC',
#       'snippet':{
#          'authorDisplayName':'James Brown',
#          'authorProfileImageUrl':'https://yt3.ggpht.com/--wowMD5v_YE/AAAAAAAAAAI/AAAAAAAAAAA/hofkHvZ2BCs/s28-c-k-no-mo-rj-c0xffffff/photo.jpg',
#          'authorChannelUrl':'http://www.youtube.com/channel/UCKjpwfi52QGqidsT5N5BqIw',
#          'authorChannelId':{
#             'value':'UCKjpwfi52QGqidsT5N5BqIw'
#          },
#          'textDisplay':"Thanks for the report! And of course every such 'Zuckerberg world' is only survivable, when incorporated into a free society, taking care as the higher representation, and watching and monitoring closely.",
#          'textOriginal':"Thanks for the report! And of course every such 'Zuckerberg world' is only survivable, when incorporated into a free society, taking care as the higher representation, and watching and monitoring closely.",
#          'parentId':'UgwRdzPi9YZJ3uvQza14AaABAg',
#          'canRate':True,
#          'viewerRating':'none',
#          'likeCount':0,
#          'publishedAt':'2018-12-28T09:07:47.000Z',
#          'updatedAt':'2018-12-28T09:07:47.000Z'
#       }
#    }
]


