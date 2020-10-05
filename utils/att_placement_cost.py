import os
import csv
from random import shuffle
from aw_reporting.adwords_reports import placement_performance_report
from aw_reporting.models import AWAccountPermission
from aw_reporting.models.ad_words.account import Account
from aw_reporting.adwords_api import get_web_app_client
from aw_reporting.adwords_reports import AccountInactiveError
from datetime import datetime


class ATTPlacementCost:
    video_ids = []
    clients = []
    account_ids_to_skip = []
    video_data = {}
    refresh_tokens = []
    accounts = None
    from_date = None
    to_date = None

    def __init__(
        self,
        mcc_id="3386233102",
        read_filename="att_videos_34k.csv",
        write_filename="att_cost_data.csv",
        skip_filename="att_skip_accounts.csv"
    ):
        self.mcc_id = mcc_id
        self.read_filename = read_filename
        self.write_filename = write_filename
        self.skip_filename = skip_filename

    def run(self):
        self.load_video_ids()
        self.load_accounts()
        self.load_skip_accounts()
        self.load_refresh_tokens()

        counter = 0
        accounts_count = len(self.accounts)
        for account in self.accounts:
            counter += 1
            if account.id in self.account_ids_to_skip:
                print(f"SKIPPING account {counter} of {accounts_count}")
                continue

            if not counter % 10:
                self.write_skip_accounts()

            print(f"processing account {counter} of {accounts_count}")
            clients = self.get_clients(account=account)
            shuffle(clients)
            client = clients.pop()
            dates = self.get_dates()
            # predicates = self.get_predicates()
            try:
                report = placement_performance_report(client, dates=dates)
            except AccountInactiveError:
                self.account_ids_to_skip.append(account.id)
                continue

            if not len(report):
                self.account_ids_to_skip.append(account.id)
                continue

            self.process_report(report)
            # TODO debug only
            # if len(self.video_data.keys()) > 1000:
            #     break

        self.write_skip_accounts()
        self.write_csv()
        print("Done!")

    def write_csv(self):
        try:
            os.remove(self.write_filename)
        except OSError:
            pass
        with open(self.write_filename, "w", encoding="utf-8") as write_file:
            writer = csv.writer(write_file)
            writer.writerow(["Video ID", "Cost", "Cost (micro)", "Impressions", "Views", "Cost (autos)", "Cost (no bid)"])
            for video_id, video_data in self.video_data.items():
                row = []
                row.append(video_id)
                cost_micro = sum(video_data.get("cost_micro", []))
                cost = cost_micro / 1000000
                row.append(cost)
                row.append(cost_micro)
                impressions = sum(video_data.get("impressions", []))
                row.append(impressions)
                views = sum(video_data.get("views", []))
                row.append(views)
                cost_auto = video_data.get("cost_auto", [])
                row.append(", ".join(cost_auto))
                cost_no_bid = video_data.get("cost_no_bid", [])
                row.append(", ".join(cost_no_bid))
                writer.writerow(row)

    def write_skip_accounts(self):
        try:
            os.remove(self.skip_filename)
        except OSError:
            pass
        with open(self.skip_filename, "w", encoding="utf-8") as write_file:
            writer = csv.writer(write_file)
            self.account_ids_to_skip = list(set(self.account_ids_to_skip))
            for account_id in self.account_ids_to_skip:
                writer.writerow([account_id])

    def process_report(self, report: list):
        if not len(report):
            return

        print("processing report data...")
        for row in report:
            criteria = row.Criteria
            if "youtube.com/" in criteria:
                criteria = criteria.split("/")[-1]
            if criteria not in self.video_ids:
                continue
            date = row.Date
            format = "%Y-%m-%d" if "-" in date else "%-m-%r-d-%y"
            dt = datetime.strptime(date, format)
            if dt < self.get_from_date() or dt > self.get_to_date():
                print("date is out of range!")
                continue

            # print(f"found criteria: {criteria}! Saving...")
            video_data = self.video_data.get(criteria, {})
            # save cost
            cost = row.Cost
            cost_type = self.get_cost_type(cost)
            video_costs = video_data.get(cost_type, [])
            if cost_type == "cost_micro":
                cost = int(cost)
            video_costs.append(cost)
            video_data[cost_type] = video_costs
            # save date
            dates = video_data.get("dates", [])
            dates.append(date)
            video_data["dates"] = dates
            # save impressions, VideoViews
            impressions = video_data.get("impressions", [])
            impressions.append(int(row.Impressions))
            video_data["impressions"] = impressions
            # save views
            views = video_data.get("views", [])
            views.append(int(row.VideoViews))
            video_data["views"] = views
            # save video data, keyed to criteria
            self.video_data[criteria] = video_data

    def get_cost_type(self, cost):
        if "auto" in cost:
            return "cost_auto"
        if "--" in cost:
            return "cost_no_bid"
        return "cost_micro"

    def get_date_format(self):
        return "%Y-%m-%d"

    def get_from_date(self):
        if self.from_date:
            return self.from_date
        self.from_date = datetime.strptime("2020-08-25", self.get_date_format())
        return self.from_date

    def get_to_date(self):
        if self.to_date:
            return self.to_date
        self.to_date = datetime.strptime("2020-09-14", self.get_date_format())
        return self.to_date

    def get_dates(self):
        from_date = self.get_from_date()
        to_date = self.get_to_date()
        return [from_date, to_date]

    def get_predicates(self):
        # predicates = predicates or [{"field": "AdNetworkType1", "operator": "EQUALS", "values": ["YOUTUBE_WATCH"]}, ]
        predicates = [
            {
                "field": "VideoId",
                "operator": "IN",
                "values": self.video_ids[:50]
            },
        ]
        # return predicates
        return []

    def get_clients(self, account):
        clients = []
        for token in self.refresh_tokens:
            client = get_web_app_client(
                refresh_token=token,
                client_customer_id=account.id,
            )
            clients.append(client)
        return clients

    def load_refresh_tokens(self):
        cf_account = Account.objects.get(id=self.mcc_id)
        permissions = AWAccountPermission.objects.filter(
            account=cf_account
        )
        permissions = permissions.filter(can_read=True, aw_connection__revoked_access=False, )
        for permission in permissions:
            aw_connection = permission.aw_connection
            self.refresh_tokens.append(aw_connection.refresh_token)

    def load_video_ids(self):
        with open(self.read_filename, "r") as video_list:
            reader = csv.reader(video_list)
            next(reader)
            for row in reader:
                self.video_ids.append(row[0])

    def load_skip_accounts(self):
        if not os.path.exists(self.skip_filename):
            return

        with open(self.skip_filename, "r") as video_list:
            reader = csv.reader(video_list)
            next(reader)
            for row in reader:
                self.account_ids_to_skip.append(int(row[0]))

    def load_accounts(self):
        mcc = Account.objects.get(id=self.mcc_id)
        self.accounts = mcc.managers.filter(
                can_manage_clients=False,
                is_active=True
            ) \
            .order_by("-update_time") \
            .all()
        print(f"loaded {len(self.accounts)} accounts.")