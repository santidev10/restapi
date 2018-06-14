import csv
from django.core.management import BaseCommand
import logging

from audit_tool.adwords import AdWords
from audit_tool.youtube import Youtube
from audit_tool.keywords import Keywords

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info("Starting custom audit (SAAS-2639)")
        adwords = AdWords(
            date_min=20000101,
            date_max=20181231,
            account_ids=[7331564558],
            download=True,
        )
        videos = adwords.get_videos()

        keywords = Keywords()
        keywords.load_from_file(
            "audit_tool/management/commands/"\
            "custom_audit_SAAS_2639.bad_words"
        )
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
        items = sorted(items, key=lambda x: len(x.found_tags))

        logger.info("Storing results")
        with open("custom_audit_SAAS_2639.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(["Url", "ChannelId", "Impressions", "Hits"])
            for item in items:
                data = videos[item.id]
                writer.writerow([
                    data[0]["Url"],
                    item.channel_id,
                    sum([int(r.get("Impressions")) for r in data]),
                    len(item.found_tags),
                ])

        logger.info("Done")
