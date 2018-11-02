import csv
import logging
from typing import List

from django.core.management import BaseCommand

from audit_tool.adwords import AdWords
from audit_tool.dmo import AccountDMO
from audit_tool.dmo import VideoDMO
from audit_tool.keywords import Keywords
from audit_tool.youtube import Youtube
from aw_reporting.models import AWConnection
from aw_reporting.models import Account

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    def handle(self, *args, **options) -> None:
        logger.info("Starting custom audit [SAAS-2926]")

        self.account_ids = ["5654527374"]
        self.accounts = self.load_accounts()
        self.accounts_dict = {_.account_id: _.name for _ in self.accounts}

        # get data from AdWords API
        adwords = AdWords(accounts=self.accounts,
                          date_start="20000101",
                          date_finish="20180901",
                          download=True,
                          save_filename=None,
                          load_filename=None,
                          fields=("Url",
                                  "Impressions",
                                  "CampaignName",
                                  "VideoViews"))
        reports = adwords.get_video_reports()
        video_ids = reports.keys()

        # get data from Data API
        youtube = Youtube()
        youtube.download(video_ids)
        videos = [i for i in youtube.get_all_items()]

        # parse by keywords
        self.parse_videos_by_keywords(videos)

        items = youtube.get_all_items()
        f_positive = open("audit_20180801_positive.csv", "w")
        writer_positive = csv.DictWriter(f_positive,
                                         fieldnames=["Video Link",
                                                     "Video Name",
                                                     "Channel Link",
                                                     "Channel Name",
                                                     "Impressions",
                                                     "Views"]
                                         )
        writer_positive.writeheader()
        f_negative = open("audit_20180801_negative.csv", "w")
        writer_negative = csv.DictWriter(f_negative,
                                         fieldnames=["Video Link",
                                                     "Video Name",
                                                     "Channel Link",
                                                     "Channel Name",
                                                     "Impressions",
                                                     "Views",
                                                     "Bad word that hit",]
                                         )
        writer_negative.writeheader()
        for item in items:
            rec = {
                "Video Link": item.url,
                "Video Name": item.title,
                "Channel Link": item.channel_url,
                "Channel Name": item.channel_title,
                "Impressions": sum([int(r["Impressions"]) for r in reports[item.id]]),
                "Views": sum([int(r["VideoViews"]) for r in reports[item.id]]),
            }

            if item.found:
                rec["Bad word that hit"] = ",".join(item.found)
                writer_negative.writerow(rec)
            else:
                writer_positive.writerow(rec)

        f_positive.close()
        f_negative.close()

        logger.info("Done")

    def load_accounts(self) -> List[AccountDMO]:
        logger.info("Loading accounts")
        accounts = []

        # load names and managers
        names = {}
        managers = {}
        queryset = Account.objects.filter(can_manage_clients=False)\
                                  .values("id", "name", "managers")\
                                  .order_by("id")

        if self.account_ids is not None:
            queryset = queryset.filter(id__in=self.account_ids)

        for account in queryset:
            account_id = account["id"]
            name = account["name"]
            names[account_id] = name
            if account_id not in managers:
                managers[account_id] = []
            managers[account_id].append(account["managers"])

        # load connections
        refresh_tokens = {}
        queryset = AWConnection.objects.filter(
            mcc_permissions__can_read=True,
            revoked_access=False,
        ).values("mcc_permissions__account", "refresh_token")

        for connection in queryset:
            account_id = connection["mcc_permissions__account"]
            refresh_tokens[account_id] = connection["refresh_token"]

        # prepare accounts
        for account_id in sorted(managers.keys()):
            tokens = [refresh_tokens[_] for _ in managers[account_id]
                      if _ in refresh_tokens]
            accounts.append(
                AccountDMO(
                    account_id=account_id,
                    name=names[account_id],
                    refresh_tokens=tokens,
                )
            )
        logger.info("Loaded {} account(s)".format(len(accounts)))
        return accounts

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
