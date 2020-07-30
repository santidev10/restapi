import csv
import os
import statistics

from collections import Counter
from datetime import datetime
from io import StringIO

from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.mail import EmailMessage
from django.core.management.base import BaseCommand
from django.db import connections
from django.db.utils import OperationalError

from es_components.constants import Sections
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager
from es_components.query_builder import QueryBuilder

from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit


class Command(BaseCommand):

    MAX_SIZE = 10000
    BRAND_SAFETY_SCORE_FLAG_THRESHOLD = 89
    PERCENT_FLAGGED_VIDEOS_THRESHOLD_A = 2
    PERCENT_FLAGGED_VIDEOS_THRESHOLD_B = 5
    TRIES_AFTER_RECONNECT = 2

    def __init__(self, stdout=None, stderr=None, no_color=False, force_color=False):
        super().__init__(stdout, stderr, no_color, force_color)
        self.filename = None
        self.reader = None
        self.csv = None
        self.channel_ids = []
        self.channel_score_map = {}
        self.channel_average_scores_map = {}
        self.export_data = []
        self.serialized = []
        self.csv_header = ('channel id', 'current', 'algorithm only', 'flagged videos', 'total videos',
                           f'percentage flagged',
                           f'mean (>={self.PERCENT_FLAGGED_VIDEOS_THRESHOLD_A}% flagged)',
                           f'median >=({self.PERCENT_FLAGGED_VIDEOS_THRESHOLD_A}% flagged)',
                           f'modes (>={self.PERCENT_FLAGGED_VIDEOS_THRESHOLD_A}% flagged)',
                           f'mean (>={self.PERCENT_FLAGGED_VIDEOS_THRESHOLD_B}% flagged)',
                           f'median (>={self.PERCENT_FLAGGED_VIDEOS_THRESHOLD_B}% flagged)',
                           f'modes (>={self.PERCENT_FLAGGED_VIDEOS_THRESHOLD_B}% flagged)',
                           )
        self.channel_manager = ChannelManager(
            sections=(
                Sections.BRAND_SAFETY,
                Sections.GENERAL_DATA,
            ),
        )
        self.video_manager = VideoManager(
            sections=(
                Sections.BRAND_SAFETY,
                Sections.CHANNEL,
            ),
        )

    def add_arguments(self, parser):
        parser.add_argument(
            "--filename",
            help="Name of the csv channel source"
        )

    # pylint: disable=too-many-statements
    def handle(self, *args, **kwargs):
        start = datetime.now()

        self.set_filename(*args, **kwargs)
        self.read_channel_ids()
        # TODO remove
        # self.channel_ids = self.channel_ids[:20]
        self.get_current_scores()
        self.get_algorithmic_scores()
        self.get_video_scores_for_averaging()
        self.compute_average_scores()
        self.serialize()
        self.write_csv()
        self.email_csv()

        duration = datetime.now() - start
        days, seconds = duration.days, duration.seconds
        hours = days * 24 + seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        print(f'done! took {days}d, {hours}h, {minutes}m, {seconds}s')

    def read_channel_ids(self):
        with open(os.path.join(settings.BASE_DIR, self.filename), "r") as file:
            self.reader = csv.reader(file)
            for row in self.reader:
                channel_url = self.get_url(row)
                channel_id = self.get_channel_id(channel_url)
                self.channel_ids.append(channel_id)

    def email_csv(self):
        msg = EmailMessage(
            subject="medium score channel report",
            body="medium score channel report attached",
            from_email=settings.EXPORTS_EMAIL_ADDRESS,
            to=['andrew.wong@channelfactory.com'],
        )
        msg.attach("report.csv", self.csv, "text/csv")
        msg.send(fail_silently=False)

    def write_csv(self):
        csv_file = StringIO()
        writer = csv.writer(csv_file)
        writer.writerow(self.csv_header)
        writer.writerows(self.serialized)
        self.csv = csv_file.getvalue()

    def serialize(self):
        """
        prepare all computed scores for writing to csv
        """
        rows = []
        for channel_id, scores in self.channel_score_map.items():
            row = []
            row.append(channel_id)
            row.append(scores.get('current_score', None))
            row.append(scores.get('algorithmic_only_score', None))
            row.append(scores.get('flagged_videos_count', None))
            row.append(scores.get('total_videos_count', None))
            # row.append(scores.get('over_percent_flagged_threshold', None))
            row.append(scores.get('percentage_flagged', None))
            row.append(scores.get('mean_flagged_score_a', None))
            row.append(scores.get('median_flagged_score_a', None))
            row.append(scores.get('mode_flagged_score_a', None))
            row.append(scores.get('mean_flagged_score_b', None))
            row.append(scores.get('median_flagged_score_b', None))
            row.append(scores.get('mode_flagged_score_b', None))
            rows.append(row)
        self.serialized = rows

    def get_current_scores(self):
        print('getting current scores...')
        channels = self.get_channels()
        for channel in channels:
            self.add_score(channel.main.id, 'current_score', channel.brand_safety.overall_score)

    def reconnect(self):
        print("reconnecting...")
        default_connection = connections["default"]
        default_connection.connect()

    def get_algorithmic_scores(self):
        # get algorithmic score without id
        print('getting algorithmic scores...')
        auditor = BrandSafetyAudit(ignore_vetted_channels=False,
                                   ignore_vetted_videos=False,
                                   ignore_blacklist_data=True)
        slice_position = 0
        slice_size = 100
        while True:
            channel_ids = self.channel_ids[slice_position:slice_position + slice_size]
            print(f'processing: {slice_position}:{slice_position + slice_size} of {len(self.channel_ids)}')

            tries = 0
            while tries < self.TRIES_AFTER_RECONNECT:
                try:
                    videos_res, channels_res = auditor.process_channels(channel_ids, index=False)
                    break
                except OperationalError as e:
                    self.reconnect()
                finally:
                    tries += 1
                    print("tries:", tries)
            if tries >= self.TRIES_AFTER_RECONNECT:
                continue

            for channel in channels_res:
                overall_score = channel.brand_safety_score.overall_score
                self.add_score(channel.pk, 'algorithmic_only_score', overall_score)
            slice_position += slice_size
            if slice_position > len(self.channel_ids):
                break

    def compute_average_scores(self):
        print('computing averages...')
        for channel_id, data in self.channel_average_scores_map.items():
            scores = data.get('scores', {}).values()
            flagged_scores = data.get('flagged_scores', {}).values()
            total_videos_count = len(scores)
            flagged_videos_count = len(flagged_scores)
            self.add_score(channel_id, 'total_videos_count', total_videos_count)
            self.add_score(channel_id, 'flagged_videos_count', flagged_videos_count)
            if not flagged_videos_count:
                continue
            percentage_flagged = round((flagged_videos_count / total_videos_count) * 100, 2)
            self.add_score(channel_id, 'percentage_flagged', percentage_flagged)
            mean_score = int(round(statistics.mean(flagged_scores)))
            median_score = int(round(statistics.median(flagged_scores)))
            try:
                mode_score = int(round(statistics.mode(flagged_scores)))
            except statistics.StatisticsError as e:
                # if we have more than one mode, find them here
                most_common = Counter(flagged_scores).most_common()
                current_count = None
                modes = []
                for item in most_common:
                    if current_count and current_count > item[1]:
                        break
                    modes.append(item[0])
                    current_count = item[1]
                mode_score = ', '.join([str(mode) for mode in modes])
            if percentage_flagged >= self.PERCENT_FLAGGED_VIDEOS_THRESHOLD_A:
                self.add_score(channel_id, 'mean_flagged_score_a', mean_score)
                self.add_score(channel_id, 'median_flagged_score_a', median_score)
                self.add_score(channel_id, 'mode_flagged_score_a', mode_score)
            if percentage_flagged >= self.PERCENT_FLAGGED_VIDEOS_THRESHOLD_B:
                self.add_score(channel_id, 'mean_flagged_score_b', mean_score)
                self.add_score(channel_id, 'median_flagged_score_b', median_score)
                self.add_score(channel_id, 'mode_flagged_score_b', mode_score)

    def get_video_scores_for_averaging(self):
        print('getting video scores...')
        slice_position = 0
        slice_size = 100
        while True:
            channel_ids = self.channel_ids[slice_position:slice_position + slice_size]
            print(f'processing channels: {slice_position}:{slice_position + slice_size} of {len(self.channel_ids)}')
            tries = 0
            # try scanning through chunks of channels' videos
            while tries < self.TRIES_AFTER_RECONNECT:
                try:
                    by_channel_filter = self.video_manager.by_channel_ids_query(channel_ids)
                    videos_generator = self.video_manager.scan(filters=by_channel_filter)
                    videos_count = self.video_manager.search(filters=by_channel_filter).count()
                    print(f'{videos_count} videos to process in slice...')
                    count = 0
                    reported_progresses = []
                    for video in videos_generator:
                        count += 1
                        # progress indicator
                        percent_progress = round((count / videos_count) * 100)
                        if not percent_progress % 10 and percent_progress not in reported_progresses:
                            reported_progresses.append(percent_progress)
                            print(f'processed {percent_progress}% of video scores in slice...')

                        channel_id = video.channel.id
                        video_id = video.main.id
                        score = video.brand_safety.overall_score
                        if channel_id is None \
                            or video_id is None \
                            or score is None:
                            continue
                        channel_data = self.channel_average_scores_map.get(channel_id, {})
                        scores = channel_data.get('scores', {})
                        scores[video_id] = score
                        flagged_scores = channel_data.get('flagged_scores', {})
                        if score <= self.BRAND_SAFETY_SCORE_FLAG_THRESHOLD:
                            flagged_scores[video_id] = score
                        channel_data['scores'] = scores
                        channel_data['flagged_scores'] = flagged_scores
                        self.channel_average_scores_map[channel_id] = channel_data
                    break
                except OperationalError as e:
                    self.reconnect()
                finally:
                    tries += 1
                    print('tries:', tries)

            if tries >= self.TRIES_AFTER_RECONNECT:
                print('TOO MANY TRIES')
                continue

            slice_position += slice_size
            if slice_position > len(self.channel_ids):
                break

    def add_score(self, channel_id, key, value):
        """
        save the a channel's score to the channel score map
        """
        channel_scores = self.channel_score_map.get(channel_id, {})
        channel_scores[key] = value
        self.channel_score_map[channel_id] = channel_scores

    def get_channels(self):
        query = QueryBuilder().build().must().terms().field("main.id").value(self.channel_ids).get()
        search = self.channel_manager.search(query=query.to_dict())
        return search.execute().hits

    def get_channel_id(self, channel_url: str):
        split = channel_url.split("/")
        return split.pop()

    def get_url(self, row: list):
        return row[0]

    def set_filename(self, *args, **kwargs):
        try:
            self.filename = kwargs["filename"]
        except KeyError:
            raise ValidationError("Argument 'filename' is required.")
        print("using filename:", self.filename)
