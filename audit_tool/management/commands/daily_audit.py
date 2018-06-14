import csv
from django.core.management import BaseCommand
import logging

from audit_tool.adwords import AdWords
from audit_tool.youtube import Youtube
from audit_tool.keywords import Keywords

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info("Starting daily audit")
        adwords = AdWords(download=True)
        videos = adwords.get_videos()

        keywords = Keywords()
        keywords.load_from_sdb()
        keywords.compile_regexp()

        youtube = Youtube()
        youtube.download(videos.keys())

        items = [i for i in youtube.get_all_items()]

        logger.info("Parsing {} video(s)".format(len(items)))
        for n, item in enumerate(items):
            if n % 1000 == 0:
                logger.info("  {}".format(n))
            item.found_tags = keywords.parse(item.get_text())

        logger.info("Sorting results")
        items = sorted(items, key=lambda x: -len(x.found_tags))

        logger.info("Storing results")
        with open("daily_audit.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(["Url", "ChannelId", "Impressions", "Hits"])
            for item in items:
                data = videos[item.id]
                writer.writerow([
                    data[0]["Url"],
                    data[0]["ChannelId"],
                    sum([int(r.get("Impressions")) for r in data]),
                    len(item.found_tags),
                ])

        logger.info("Done")
