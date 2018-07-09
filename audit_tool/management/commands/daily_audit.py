from datetime import timedelta
from django.conf import settings
from django.core.mail import EmailMessage
from django.core.management import BaseCommand
from django.http import QueryDict
from io import BytesIO
import logging
from typing import Dict
from typing import List
from typing import Set
import xlsxwriter

from audit_tool.adwords import AdWords
from audit_tool.youtube import Youtube
from audit_tool.keywords import Keywords
from audit_tool.dmo import AccountDMO
from audit_tool.dmo import VideoDMO
from aw_reporting.models import Account
from aw_reporting.models import AWConnection
from singledb.connector import  SingleDatabaseApiConnector
from utils.datetime import now_in_default_tz


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    # CL arguments --->
    date_start = None
    date_finish = None
    account_ids = None
    # <--- CL arguments

    accounts = None
    accounts_dict = None

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--date_start",
            dest="date_start",
            help="Date start",
            type=str,
            default=None,
        )
        parser.add_argument(
            "--date_finish",
            dest="date_finish",
            help="Date finish",
            type=str,
            default=None,
        )
        parser.add_argument(
            "--account_ids",
            dest="account_ids",
            help="Account IDs as a comma separated string",
            type=str,
            default=None,
        )

    def load_arguments(self, *args, **options) -> None:
        # argument: date_start
        self.date_start = options.get("date_start")
        if not self.date_start:
            yesterday = now_in_default_tz() - timedelta(days=1)
            yesterday = yesterday.date()
            self.date_start = yesterday.strftime("%Y%m%d")

        # argument: date_finish
        self.date_finish = options.get("date_finish")
        if not self.date_finish:
            self.date_finish = self.date_start

        # argument: account_ids
        account_ids = options.get("account_ids")
        if account_ids:
            self.account_ids = account_ids.split(",")

    def handle(self, *args, **options) -> None:
        self.load_arguments(*args, **options)

        logger.info("Starting daily audit")
        self.accounts = self.load_accounts()
        self.accounts_dict = {_.account_id: _.name for _ in self.accounts}

        # get data from AdWords API
        adwords = AdWords(accounts=self.accounts,
                          date_start=self.date_start,
                          date_finish=self.date_finish,
                          download=True)
        reports = adwords.get_video_reports()
        video_ids = reports.keys()

        # get data from Data API
        youtube = Youtube()
        youtube.download(video_ids)
        videos = [i for i in youtube.get_all_items()]

        # parse by keywords
        self.parse_videos_by_keywords(videos)

        # get preferred channels list
        preferred_channels = self.load_preferred_channels()

        # send report
        self.create_workbook_and_send(videos,
                                      reports,
                                      preferred_channels)

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
    def load_preferred_channels() -> Set[str]:
        logger.info("Loading preferred channels list")
        singledb = SingleDatabaseApiConnector()
        response = singledb.execute_get_call(
            "channels/",
            QueryDict("fields=channel_id&preferred__term=1&size=10000")
        )
        preferred_channels = set([_['channel_id'] for _ in response['items']])
        count = len(preferred_channels)
        logger.info("Loaded {} preferred channel(s)".format(count))
        return preferred_channels

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

    def create_workbook_and_send(self,
                                 videos: List[VideoDMO],
                                 reports: Dict[str, list],
                                 preferred_channels: Set[str]) -> None:

        logger.info("Storing XLSX")

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
        text_format = workbook.add_format({
            "text_wrap": True
        })

        # add sheet: Keywords Hits
        worksheet = workbook.add_worksheet("Keywords Hits")
        fields = (
            ("VideoTitle", 90),
            ("VideoUrl", 40),
            ("ChannelTitle", 45),
            ("ChannelUrl", 55),
            ("Google Preferred", 14),
            ("Impressions", 10),
            ("Hits", 10),
            ("Words that hit", 20),
            ("Account Info", 200),
        )
        for x, field in enumerate(fields):
            worksheet.write(0, x, field[0], header_format)
            worksheet.set_column(x, x, field[1])

        sorted_videos = sorted(videos, key=lambda _: -len(_.found))
        for y, item in enumerate(sorted_videos):
            hits = len(item.found)
            if y >= 5000 or hits == 0:
                break
            data = reports[item.id]
            worksheet.write(y+1, 0, item.title)
            worksheet.write(y+1, 1, "'" + item.url)
            worksheet.write(y+1, 2, item.channel_title)
            worksheet.write(y+1, 3, "'" + item.channel_url)
            if item.channel_id in preferred_channels:
                worksheet.write(y+1, 4, "YES")
            worksheet.write(y+1, 5, sum([int(r.get("Impressions")) for r in data]), numberic_format)
            worksheet.write(y+1, 6, hits, numberic_format)
            worksheet.write(y+1, 7, ",".join(item.found))

            account_info = self._get_account_info(data)
            worksheet.write(y+1, 8, "\n".join(account_info), text_format)
            if len(account_info) > 1:
                worksheet.set_row(y+1, 15*len(account_info))

        # add sheet: Keywords
        worksheet = workbook.add_worksheet("Keywords")
        fields = (
            ("Keyword", 45),
            ("Videos", 10),
            ("Impressions", 10),
        )
        for x, field in enumerate(fields):
            worksheet.write(0, x, field[0], header_format)
            worksheet.set_column(x, x, field[1])

        keyword_videos = {}
        for item in videos:
            for keyword in item.found:
                if keyword not in keyword_videos:
                    keyword_videos[keyword] = set()
                keyword_videos[keyword].add(item.id)
        result = []
        for keyword, videos_info in keyword_videos.items():
            impressions = 0
            for video in videos_info:
                impressions += sum(
                    [int(r.get("Impressions")) for r in reports[video]]
                )
            result.append((keyword, len(videos_info), impressions))
        result = sorted(result, key=lambda _: -_[1])
        for y, _ in enumerate(result):
            keyword, videos_count, impressions = _
            worksheet.write(y+1, 0, keyword)
            worksheet.write(y+1, 1, videos_count, numberic_format)
            worksheet.write(y+1, 2, impressions, numberic_format)

        # add sheet: Google Preferred
        worksheet = workbook.add_worksheet("Google Preferred")
        fields = (
            ("VideoTitle", 90),
            ("VideoUrl", 40),
            ("ChannelTitle", 45),
            ("ChannelUrl", 55),
            ("Impressions", 10),
        )
        for x, field in enumerate(fields):
            worksheet.write(0, x, field[0], header_format)
            worksheet.set_column(x, x, field[1])

        sorted_videos = sorted(videos, key=lambda x: -len(x.found))
        y = 0
        for item in sorted_videos:
            if item.channel_id not in preferred_channels:
                continue
            data = reports[item.id]
            impressions = sum([int(r.get("Impressions")) for r in data])
            worksheet.write(y+1, 0, item.title)
            worksheet.write(y+1, 1, item.url)
            worksheet.write(y+1, 2, item.channel_title)
            worksheet.write(y+1, 3, item.channel_url)
            worksheet.write(y+1, 4, impressions, numberic_format)
            y += 1

        # close workbook
        workbook.close()
        xlsx_data = output.getvalue()
        logger.info("XLSX is ready")

        # calculate totals
        logger.info("Calculating totals")
        totals = {"impressions": 0}
        for report in reports.values():
            impressions = sum([int(_.get("Impressions")) for _ in report])
            totals["impressions"] += impressions
        totals["videos"] = len(set(_.id for _ in videos))
        totals["channels"] = len(set(_.channel_id for _ in videos))

        # prepare E-mail
        date_str = self.date_start\
            if self.date_start == self.date_finish\
            else "{}_{}".format(self.date_start, self.date_finish)
        subject = "Daily Audit {}".format(date_str)
        body = "Total impressions: {impressions}\n" \
               "Total videos: {videos}\n" \
               "Total channels: {channels}\n".format(**totals)

        # E-mail
        logger.info("Sending E-mail")
        from_email = settings.SENDER_EMAIL_ADDRESS
        to = settings.AUDIT_TOOL_EMAIL_ADDRESSES
        bcc = []
        replay_to = ""

        filename = "daily_audit_{}.xlsx".format(date_str)
        content_type = "application" \
                       "/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        email = EmailMessage(
            subject=subject,
            body=body,
            from_email=from_email,
            to=to,
            bcc=bcc,
            reply_to=replay_to,
        )
        email.attach(filename, xlsx_data, content_type)
        email.send(fail_silently=False)

    def _get_account_info(self, data):
        info = {}
        for row in data:
            account_id = row.get("AccountId")
            campaign_name = row.get("CampaignName")
            impressions = int(row.get("Impressions"))

            if account_id not in info:
                info[account_id] = {}

            if campaign_name not in info[account_id]:
                info[account_id][campaign_name] = 0

            info[account_id][campaign_name] += impressions

        lines = []
        for account_id, campaign_info in info.items():
            for campaign_name, impressions in campaign_info.items():
                    line = "{}: {} ---> {}".format(
                        impressions,
                        self.accounts_dict[account_id],
                        campaign_name,
                    )
                    lines.append(line)
        return lines

