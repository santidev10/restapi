import logging
import csv

from django.core.management.base import BaseCommand
from pid import PidFile
from pid import PidFileError

from es_components.connections import init_es_connection
from es_components.managers.channel import ChannelManager
from es_components.models.channel import Channel
from es_components.constants import Sections


logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            "--filename",
            help="Name of TaskUs .csv file to import data from."
        )

    def handle(self, *args, **options):
        file_name = options["filename"]
        try:
            with PidFile("import_taskus_data", piddir=".") as pid:
                with open(file_name, "r") as f:
                    reader = csv.reader(f)
                    next(reader)
                    for row in reader:
                        channel_id = row[0].split('/')[-2]

        except PidFileError:
            pass