import csv
import logging
from io import BytesIO
from typing import Dict
from typing import List

import xlsxwriter
from django.core.management import BaseCommand

from audit_tool.dmo import VideoDMO
from audit_tool.keywords import Keywords
from audit_tool.youtube import Youtube
from brand_safety.models import BadWord

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    filename = None
    CATEGORIES = {
        "1": "Film & Animation",
        "2": "Autos & Vehicles",
        "10": "Music",
        "15": "Pets & Animals",
        "17": "Sports",
        "18": "Short Movies",
        "19": "Travel & Events",
        "20": "Gaming",
        "21": "Videoblogging",
        "22": "People & Blogs",
        "23": "Comedy",
        "24": "Entertainment",
        "25": "News & Politics",
        "26": "Howto & Style",
        "27": "Education",
        "28": "Science & Technology",
        "29": "Nonprofits & Activism",
        "30": "Movies",
        "31": "Anime/Animation",
        "32": "Action/Adventure",
        "33": "Classics",
        "34": "Comedy",
        "35": "Documentary",
        "36": "Drama",
        "37": "Family",
        "38": "Foreign",
        "39": "Horror",
        "40": "Sci-Fi/Fantasy",
        "41": "Thriller",
        "42": "Shorts",
        "43": "Shows",
        "44": "Trailers",
    }

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--filename",
            dest="filename",
            type=str,
        )

    def handle(self, *args, **options) -> None:
        self.filename = options.get("filename")
        logger.error("Starting audit VIQ-326")

        reports = {}
        for row in self.load_file():
            reports[row.get("ExternalVideoId")] = row
        video_ids = reports.keys()
        logger.error("loaded file")

        youtube = Youtube()

        # get video-data from Data API
        youtube.download(video_ids)
        videos = [i for i in youtube.get_all_items()]
        logger.error("downloaded videos from youtube")

        # get channel-data from Data API
        channels_ids = set([v.channel_id for v in videos])
        youtube.download_channels(channels_ids)
        channels = {}
        for chunk in youtube.chunks:
            for item in chunk.get("items"):
                channels[item.get("id")] = item
        logger.error("downloaded channels from youtube")

        # get KW_Category
        self.KW_CATEGORY = {}
        logger.error("start downloading categories")
        bad_words = BadWord.objects.all().values()
        for row in bad_words:
            name = row.get("name")
            category = row.get("category")
            if name not in self.KW_CATEGORY:
                self.KW_CATEGORY[name] = set()
            self.KW_CATEGORY[name].add(category)
        logger.error("done")

        # parse by keywords
        logger.error("start parsing")
        self.parse_videos_by_keywords(videos)

        # save results
        self.save_csv(videos, channels, reports)
        self.save_xlsx(videos, channels, reports)

        logger.info("Done")

    def load_file(self):
        with open(self.filename) as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row

    @staticmethod
    def parse_videos_by_keywords(videos: List[VideoDMO]) -> None:
        logger.info("Parsing {} video(s)".format(len(videos)))
        keywords = Keywords()
        keywords.load_from_sdb()
        keywords.compile_regexp()
        texts = [video.get_text() for video in videos]
        found = keywords.parse_all(texts)
        for idx, video in enumerate(videos):
            video.found = found[idx]
        logger.info("Parsed {} video(s)".format(len(videos)))

    def save_csv(self, videos: List[VideoDMO],
                 channels: Dict[str, dict],
                 reports: Dict[str, list]) -> None:

        logger.info("Storing CSV")
        sorted_videos = sorted(videos, key=lambda _: -len(_.found))
        fields = [
            "Channel Name",
            "Channel URL",
            "Channel Subscribers",
            "Video Name",
            "Video URL",
            "Category",
            "Sentiment",
            "Impressions",
            "Word Hits",
            "Word Category",
            "Word",
        ]
        with open(self.filename + "_result_negative.csv", "w") as f_negative:
            negative_writer = csv.writer(f_negative)
            negative_writer.writerow(fields)

            with open(self.filename + "_result_positive.csv", "w") as f_positive:
                positive_writer = csv.writer(f_positive)
                positive_writer.writerow(fields[:-3])

                for item in sorted_videos:
                    hits = len(item.found)

                    data = reports[item.id]
                    impressions = data.get("Impressions")
                    words = ",".join(set(item.found))
                    words_category = set()
                    for kw in set(item.found):
                        words_category |= self.KW_CATEGORY.get(kw)

                    row = [
                        item.channel_title,
                        item.channel_url,
                        channels.get(item.channel_id, {}).get("subscribers"),
                        item.title,
                        item.url,
                        self.CATEGORIES.get(item.category_id),
                        item.sentiment,
                        impressions,
                        hits,
                        ",".join(words_category),
                        words,
                    ]

                    if hits >= 5:
                        negative_writer.writerow(row)
                    else:
                        positive_writer.writerow(row[:-3])

        logger.info("Done")

    def save_xlsx(self, videos: List[VideoDMO],
                  channels: Dict[str, dict],
                  reports: Dict[str, list]) -> None:

        logger.info("Storing Results")

        # create workbook
        output = BytesIO()
        workbook = xlsxwriter.Workbook(output, {
            "in_memory": True,
            "strings_to_urls": False,
        })
        header_format = workbook.add_format({
            "bold": True,
            "align": "center",
            "bg_color": "#C0C0C0",
            "border": True,
        })
        numberic_format = workbook.add_format({
            "align": "right",
            "num_format": "0",
        })
        percentage_format = workbook.add_format({
            "align": "right",
            "num_format": "0.00%",
        })

        sorted_videos = sorted(videos, key=lambda _: -len(_.found))

        # add sheet: Negative Audit
        worksheet = workbook.add_worksheet("Negative Audit")
        fields = (
            ("Channel Name", 45),
            ("Channel URL", 40),
            ("Channel Subscribers", 14),
            ("Video Name", 90),
            ("Video URL", 40),
            ("Category", 13),
            ("Sentiment", 16),
            ("Impressions", 10),
            ("Word Hits", 10),
            ("Word Category", 10),
            ("Word", 10)
        )
        for x, field in enumerate(fields):
            worksheet.write(0, x, field[0], header_format)
            worksheet.set_column(x, x, field[1])

        for y, item in enumerate(sorted_videos):
            hits = len(item.found)
            if hits < 5:
                continue
            data = reports[item.id]
            impressions = data.get("Impressions")
            words = ",".join(set(item.found))
            words_category = set()
            for kw in set(item.found):
                words_category |= self.KW_CATEGORY.get(kw)

            worksheet.write(y + 1, 0, item.channel_title)
            worksheet.write(y + 1, 1, "'" + item.channel_url)
            worksheet.write(y + 1, 2, channels.get(item.channel_id, {}).get("subscribers"), numberic_format)
            worksheet.write(y + 1, 3, item.title)
            worksheet.write(y + 1, 4, "'" + item.url)
            worksheet.write(y + 1, 5, self.CATEGORIES.get(item.category_id))
            worksheet.write(y + 1, 6, item.sentiment, percentage_format)
            worksheet.write(y + 1, 7, impressions, numberic_format)
            worksheet.write(y + 1, 8, hits, numberic_format)
            worksheet.write(y + 1, 9, ",".join(words_category))
            worksheet.write(y + 1, 10, words)

        # add sheet: Positive Audit
        worksheet = workbook.add_worksheet("Positive Audit")
        fields = (
            ("Channel Name", 45),
            ("Channel URL", 40),
            ("Channel Subscribers", 14),
            ("Video Name", 90),
            ("Video URL", 40),
            ("Category", 13),
            ("Sentiment", 16),
            ("Impressions", 10),
        )
        for x, field in enumerate(fields):
            worksheet.write(0, x, field[0], header_format)
            worksheet.set_column(x, x, field[1])

        for y, item in enumerate([v for v in sorted_videos if len(v.found) < 5]):
            data = reports[item.id]
            impressions = data.get("Impressions")
            words_category = set()
            for kw in set(item.found):
                words_category |= self.KW_CATEGORY.get(kw)

            worksheet.write(y + 1, 0, item.channel_title)
            worksheet.write(y + 1, 1, "'" + item.channel_url)
            worksheet.write(y + 1, 2, channels.get(item.channel_id, {}).get("subscribers"), numberic_format)
            worksheet.write(y + 1, 3, item.title)
            worksheet.write(y + 1, 4, "'" + item.url)
            worksheet.write(y + 1, 5, self.CATEGORIES.get(item.category_id))
            worksheet.write(y + 1, 6, item.sentiment, percentage_format)
            worksheet.write(y + 1, 7, impressions, numberic_format)

        workbook.close()
        xlsx_data = output.getvalue()
        logger.info("XLSX is ready")

        with open(self.filename + "_result.xlsx", "wb") as f:
            f.write(xlsx_data)
