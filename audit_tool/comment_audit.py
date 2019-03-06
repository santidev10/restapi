import csv
import re
import time
import logging
from segment.models import SegmentChannel
from segment.models import SegmentRelatedChannel
from segment.models.persistent import PersistentSegmentRelatedChannel
from singledb.connector import SingleDatabaseApiConnector as Connector
from utils.youtube_api import YoutubeAPIConnector
from audit_tool.audit_mixin import AuditMixin
import json


class CommentAudit(AuditMixin):
    channel_batch_limit = 40
    comment_batch_size = 5000
    video_fields = 'video_id,channel_id,title'
    csv_file_path = '/Users/kennethoh/Desktop/comments.csv'

    def __init__(self):
        self.connector = Connector()
        self.youtube_connector = YoutubeAPIConnector()

        bad_words = self.get_all_bad_words()

        self.bad_words_regexp = self.compile_audit_regexp(bad_words)

    def run(self):
        results = {}
        print('Getting channels')
        top_10k_channels = self.get_top_10k_channels()[:5]
        comment_batch = []

        while top_10k_channels:
            channel_batch = [channel['related_id'] for channel in top_10k_channels][:self.channel_batch_limit]
            videos = self.get_videos_batch(channel_batch, self.video_fields)

            for video in videos:
                comment_batch += self.get_video_comments(video_id=video.get('video_id'))

            if len(comment_batch) >= self.comment_batch_size:
                self.write_comments(comment_batch)
                comment_batch.clear()

            top_10k_channels = top_10k_channels[self.channel_batch_limit:]

        self.write_comments(comment_batch)
        print('Complete')

    def parse_comment(self, comment):
        text = comment.get('textOriginal')

        found = re.findall(self.bad_words_regexp, text)

        return found

    def get_top_10k_channels(self):
        all_channels = list(PersistentSegmentRelatedChannel.objects.all().distinct().exclude(details__subscribers__isnull=True).values('related_id', 'details'))
        top_10k = sorted(all_channels, key=lambda obj: obj['details'].get('subscribers'), reverse=True)[:1]

        return top_10k

    def get_video_comments(self, video_id: str):
        all_comments = []

        response = self.youtube_connector.get_video_comments(video_id=video_id)
        all_comments += response.get('items')

        next_page_token = response.get('nextPageToken')
        page = 0

        while next_page_token and page < 2:
            response = self.youtube_connector.get_video_comments(video_id=video_id, page_token=next_page_token)
            all_comments += response.get('items')
            next_page_token = response.get('nextPageToken')

            page += 1

        return all_comments

    def write_comments(self, comments):
        with open(self.csv_file_path, mode='a') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')

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

"""
YT Response (video comment)
{
    "kind": "youtube#commentThread",
    "etag": "\"XpPGQXPnxQJhLgs6enD_n8JR4Qk/WvDpdKVNFePdEZv8phCQQt-aBo4\"",
    "id": "UgxRuZMZIJmdiuQEpHJ4AaABAg",
    "snippet": {
        "videoId": "-F1NwLNr0h0",
        "topLevelComment": {
            "kind": "youtube#comment",
            "etag": "\"XpPGQXPnxQJhLgs6enD_n8JR4Qk/NpJP_hrJkDbxqNrGXdsSXOt9rE8\"",
            "id": "UgxRuZMZIJmdiuQEpHJ4AaABAg",
            "snippet": {
                "authorDisplayName": "Marshmello",
                "authorProfileImageUrl": "https://yt3.ggpht.com/-2ecBJ0Rt1QM/AAAAAAAAAAI/AAAAAAAAAAA/7iC-PVcjk3Y/s28-c-k-no-mo-rj-c0xffffff/photo.jpg",
                "authorChannelUrl": "http://www.youtube.com/channel/UCEdvpU2pFRCVqU6yIPyTpMQ",
                "authorChannelId": {
                    "value": "UCEdvpU2pFRCVqU6yIPyTpMQ"
                },
                "videoId": "-F1NwLNr0h0",
                "textDisplay": "<b>FRIENDS - OFFICIAL MUSIC VIDEO | OUT NOW</b> <a href=\"https://youtu.be/jzD_yyEcp0M\">https://youtu.be/jzD_yyEcp0M</a>",
                "textOriginal": "*FRIENDS - OFFICIAL MUSIC VIDEO | OUT NOW* https://youtu.be/jzD_yyEcp0M",
                "canRate": true,
                "viewerRating": "none",
                "likeCount": 483,
                "publishedAt": "2018-02-16T12:56:49.000Z",
                "updatedAt": "2018-02-16T12:56:49.000Z"
            }
        },
        "canReply": true,
        "totalReplyCount": 45,
        "isPublic": true
    }
}



"""





