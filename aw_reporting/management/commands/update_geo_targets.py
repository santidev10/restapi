import csv
import logging
import os

from django.conf import settings
from django.core.management import BaseCommand

from aw_reporting.models import GeoTarget

logging.basicConfig(format="%(asctime)s - %(message)s", level="INFO")
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info("Start updating google geo targets")
        self.process_location_criteria_csv()
        logger.info("End updating google geo targets")

    def process_location_criteria_csv(self):
        update_errors_counter = 0
        failed_to_update_objects_ids = []
        with open(
                os.path.join(
                    settings.BASE_DIR,
                    "aw_reporting/fixtures/geo_locations.csv")) as f:
            reader = csv.reader(f, delimiter=",")
            raw_keys = next(reader)
            keys = ["id"] + [
                key.lower().replace(" ", "_") for key in raw_keys[1:]]
            not_created_objects = []
            not_updated_objects = []
            processed_counter = 0
            for row in reader:
                if not row:
                    continue
                obj_data = dict(zip(keys, row))
                obj_as_queryset = GeoTarget.objects.filter(
                    id=obj_data.get("id"))
                if obj_as_queryset.exists():
                    try:
                        obj_as_queryset.update(**obj_data)
                    except:
                        logger.warning(
                            "Unable to UPDATE object with the next data: {}"
                            "Skipped".format(obj_data))
                        not_updated_objects.append(obj_data)
                else:
                    try:
                        GeoTarget.objects.create(**obj_data)
                    except:
                        logger.warning(
                            "Unable to CREATE object with the next data: {} ."
                            "Skipped".format(obj_data))
                        not_created_objects.append(GeoTarget(**obj_data))
                processed_counter += 1
                if not processed_counter % 5000:
                    logger.info(
                        "Processed {} objects".format(processed_counter))
            logger.info(
                "Processed {} objects".format(processed_counter))
            logger.info("Perform skipped objects create")
            try:
                GeoTarget.objects.bulk_create(not_created_objects)
            except Exception as e:
                logger.critical(
                    "Bulk create failed. Something went completely wrong!"
                    " Critical fail! Abort!. Original error: {}".format(e))
                return
            logger.info("Skipped objects create finished. Success.")
            logger.info("Perform skipped objects update")
            for obj_data in not_updated_objects:
                obj_as_queryset = GeoTarget.objects.filter(
                    id=obj_data.get("id"))
                try:
                    obj_as_queryset.update(**obj_data)
                except:
                    logger.critical(
                        "Unable to UPDATE object with the next data: {}"
                        .format(obj_data))
                    failed_to_update_objects_ids.append(obj_data.get("id"))
                    update_errors_counter += 1
        if update_errors_counter:
            logger.error(
                "Process delivered {} update errors."
                " Ids, failed to update: {}".format(
                    update_errors_counter, ", ".join(
                        failed_to_update_objects_ids)))
        else:
            logger.info("Skipped objects update finished. Success.")
