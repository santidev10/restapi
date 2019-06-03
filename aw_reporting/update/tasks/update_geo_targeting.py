import csv
import logging

import requests

from aw_reporting.models import GeoTarget

logger = logging.getLogger(__name__)


def update_geo_targeting(url):
    update_errors_counter = 0
    failed_to_update_objects_ids = []
    not_created_objects = []
    not_updated_objects = []
    processed_counter = 0
    for obj_data in parse_from_link(url):
        if not obj_data:
            continue
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


def parse_from_link(url):
    response = requests.get(url)
    lines = response.text.splitlines()
    reader = csv.DictReader(lines)
    for row in reader:
        yield dict(
            id=row["Criteria ID"],
            name=row["Name"],
            canonical_name=row["Canonical Name"],
            parent_id=row["Parent ID"],
            country_code=row["Country Code"],
            target_type=row["Target Type"],
            status=row["Status"],
        )
