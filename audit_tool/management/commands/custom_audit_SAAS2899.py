import csv
from django.core.management import BaseCommand
import logging
from typing import List

from audit_tool.adwords import AdWords
from audit_tool.youtube import Youtube
from audit_tool.keywords import Keywords
from audit_tool.dmo import AccountDMO
from audit_tool.dmo import VideoDMO
from aw_reporting.models import Account
from aw_reporting.models import AWConnection


logger = logging.getLogger(__name__)

CAMPAIGNS_LIST = {
    "test PL006413 BBQ & Grilling",
    "test PL006413 Baked Goods",
    "test PL006413 Bakeries",
    "test PL006413 Beef",
    "test PL006413 Beverages",
    "test PL006413 Bottled Water",
    "test PL006413 Breakfast Foods",
    "test PL006413 Burgers",
    "test PL006413 Candy & Sweets",
    "test PL006413 Caribbean Cuisine",
    "test PL006413 Cheese",
    "test PL006413 Chinese Cuisine",
    "test PL006413 Coffee",
    "test PL006413 Coffee & Tea",
    "test PL006413 Condiments & Dressings",
    "test PL006413 Cooking Fats & Oils",
    "test PL006413 Cuisines",
    "test PL006413 Dairy & Eggs",
    "test PL006413 Delicatessens",
    "test PL006413 Desserts",
    "test PL006413 East Asian Cuisine",
    "test PL006413 Eastern European Cuisine",
    "test PL006413 Fast Food",
    "test PL006413 Fine Dining",
    "test PL006413 Fish & Seafood",
    "test PL006413 Food",
    "test PL006413 Food & Drink",
    "test PL006413 Food & Grocery Delivery",
    "test PL006413 Food & Grocery Retailers",
    "test PL006413 French Cuisine",
    "test PL006413 Fruits & Vegetables",
    "test PL006413 German Cuisine",
    "test PL006413 Gourmet & Specialty Foods",
    "test PL006413 Grains & Pasta",
    "test PL006413 Greek Cuisine",
    "test PL006413 Healthy Eating",
    "test PL006413 Herbs & Spices",
    "test PL006413 Indian Cuisine",
    "test PL006413 Italian Cuisine",
    "test PL006413 Japanese Cuisine",
    "test PL006413 Juice",
    "test PL006413 Korean Cuisine",
    "test PL006413 Latin American Cuisine",
    "test PL006413 Meat & Seafood",
    "test PL006413 Mediterranean Cuisine",
    "test PL006413 Mexican Cuisine",
    "test PL006413 Middle Eastern Cuisine",
    "test PL006413 North American Cuisine",
    "test PL006413 Organic & Natural Foods",
    "test PL006413 Pizzerias",
    "test PL006413 Pork",
    "test PL006413 Restaurants",
    "test PL006413 Salads",
    "test PL006413 Snack Foods",
    "test PL006413 Soft Drinks",
    "test PL006413 Soups & Stews",
    "test PL006413 South American Cuisine",
    "test PL006413 South Asian Cuisine",
    "test PL006413 Southeast Asian Cuisine",
    "test PL006413 Spanish Cuisine",
    "test PL006413 Tea",
    "test PL006413 Thai Cuisine",
    "test PL006413 Vegetarian Cuisine",
    "test PL006413 Vietnamese Cuisine",
}

class Command(BaseCommand):
    def handle(self, *args, **options) -> None:
        logger.info("Starting custom audit [SAAS-2899]")

        self.account_ids = ["6888014947"]
        self.accounts = self.load_accounts()
        self.accounts_dict = {_.account_id: _.name for _ in self.accounts}

        # get data from AdWords API
        adwords = AdWords(accounts=self.accounts,
                          date_start="20000101",
                          date_finish="20180801",
                          download=True,
                          save_filename=None,
                          load_filename=None,
                          fields=("Url",
                                  "Impressions",
                                  "CampaignName",
                                  "VideoViews"))
        reports = adwords.get_video_reports()

        # Filter by campaigns
        videos_to_delete = []
        for video_id, report in reports.items():
            report = [r for r in reports[video_id] if r.get("CampaignName") in CAMPAIGNS_LIST]
            if report:
                reports[video_id] = report
            else:
                videos_to_delete.append(video_id)
        for video_id in videos_to_delete:
            del reports[video_id]

        video_ids = reports.keys()

        # get data from Data API
        youtube = Youtube()
        youtube.download(video_ids)
        videos = [i for i in youtube.get_all_items()]

        # parse by keywords
        self.parse_videos_by_keywords(videos)

        items = youtube.get_all_items()
        f_positive = open("audit_20180730_positive.csv", "w")
        writer_positive = csv.DictWriter(f_positive,
                                         fieldnames=["Video Link",
                                                     "Video Name",
                                                     "Channel Link",
                                                     "Channel Name",
                                                     "Impressions",
                                                     "Views"]
                                         )
        writer_positive.writeheader()
        f_negative = open("audit_20180730_negative.csv", "w")
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
