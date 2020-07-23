from django.db import connection
from django.core.management import BaseCommand
import csv


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--env",
        )

    def handle(self, *args, **options):
        env = options["env"].lower()
        select = "SELECT * from schedulers_daily_quota"
        with connection.cursor() as cursor:
            cursor.execute(select)
            data = cursor.fetchall()
        with open(f"/Users/kennethoh/Documents/schedulers/scheduler_quota_{env}.csv", mode="a") as file:
            writer = csv.writer(file)
            for item in data:
                row = [
                    item[0],
                    item[1],
                    str(item[2])
                ]
                writer.writerow(row)
            writer.writerow(["----", "----", "----"])
