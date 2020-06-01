import csv
import tempfile
from collections import defaultdict
from shutil import rmtree

from django.conf import settings

from es_components.query_builder import QueryBuilder
from es_components.constants import Sections
from elasticsearch_dsl import Q
from utils.utils import chunks_generator
from segment.utils.bulk_search import bulk_search
from utils.lang import merge_sort

from segment.utils.generate_segment_utils import GenerateSegmentUtils


FILE_MAX_SIZE = 100000


def generate_with_source(segment, query):
    fp = "/Users/kennethoh/Documents/mixed_ids.csv"
    with open(fp, mode="r") as file:
        reader = csv.reader(file)
        source = [row[0] for row in reader]

    temp_dir = tempfile.mkdtemp(dir=settings.TEMPDIR)
    generate_utils = GenerateSegmentUtils()
    seen = 0
    monetized_files = []
    non_monetized_files = []
    aggregations = defaultdict(int)

    config = generate_utils.get_default_search_config(segment.segment_type)
    for index, option in enumerate(config["options"]):
        curr_file = tempfile.mkstemp(dir=temp_dir)[1]
        for batch in chunks_generator(source, size=3):
            batch = list(batch)
            context = generate_utils.get_default_serialization_context()
            vetting_data = generate_utils.get_vetting_data(segment, batch)
            context["vetting"] = vetting_data
            full_query = Q(dict(ids=dict(type="_doc", values=batch)))
            curr_option = [option]
            for batch in bulk_search(segment.es_manager.model, full_query, segment.SORT_KEY, config["cursor_field"],
                                     options=curr_option, batch_size=10000, source=segment.SOURCE_FIELDS,
                                     include_cursor_exclusions=False):
                batch = list(batch)
                rows = [r for r in batch]
                generate_utils.write_to_file(rows, curr_file, segment, context, aggregations)
                seen += len(batch)
        if index == 0:
            monetized_files.append(curr_file)
        else:
            non_monetized_files.append(curr_file)

    fieldnames = segment.serializer.columns
    sorted_monetized = _sort_files(monetized_files, temp_dir, "Subscribers", fieldnames)
    merged_monetized = _write_merge(sorted_monetized, temp_dir, headers=fieldnames)

    sorted_non_monetized = _sort_files(non_monetized_files, temp_dir, "Subscribers", fieldnames)
    merged_non_monetized = _write_merge(sorted_non_monetized, temp_dir)

    with open(merged_monetized, mode="a") as dest_file,\
            open(merged_non_monetized, mode="r") as read_file:
        csv_writer = csv.writer(dest_file)
        csv_reader = csv.reader(read_file)
        rows = [row for row in csv_reader]
        csv_writer.writerows(rows)

    statistics = {
        "items_count": seen,
        "top_three_items": [],
        **aggregations,
    }
    s3_key = segment.get_s3_key()
    segment.s3_exporter.export_file_to_s3(merged_monetized, s3_key)
    download_url = segment.s3_exporter.generate_temporary_url(s3_key, time_limit=3600 * 24 * 7)
    results = {
        "statistics": statistics,
        "download_url": download_url,
        "s3_key": s3_key,
    }

    # Upload files
    rmtree(temp_dir)

    return results



def _sort_files(filenames, dir, sort_key, fieldnames):
    sorted_files = []
    for file_name in filenames:
        curr_file = tempfile.mkstemp(dir=dir)[1]
        with open(file_name, "r") as file:
            csv_reader = csv.DictReader(file, fieldnames=fieldnames)
            rows = [row for row in csv_reader]
            rows.sort(key=lambda row: int(row.get(sort_key, 0)), reverse=True)
        with open(curr_file, "w") as file:
            csv_writer = csv.DictWriter(file, fieldnames=fieldnames)
            csv_writer.writerows(rows)
        sorted_files.append(curr_file)
    return sorted_files


def _write_merge(filenames, dir, headers=None):
    merge_dest = tempfile.mkstemp(dir=dir)[1]
    to_combine = [open(file_name, mode="r") for file_name in filenames]
    readers = [csv.reader(file) for file in to_combine]
    with open(merge_dest, mode="w") as file:
        csv_writer = csv.writer(file)
        if headers:
            csv_writer.writerow(headers)
        for row in merge_sort(readers):
            csv_writer.writerow(row)
    for file in to_combine:
        file.close()
    return merge_dest
