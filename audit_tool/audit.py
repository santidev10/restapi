from brand_safety.models import BadWord
from .auditor import AuditService
from singledb.connector import SingleDatabaseApiConnector as Connector
import csv
import re
from multiprocessing import Pool
from . import audit_constants as constants

class AuditProvider(object):
    max_process_count = 5
    video_chunk_size = 10000
    video_batch_size = 30000
    channel_batch_size = 1000
    channel_chunk_size = 10
    channel_row_data = {}
    max_csv_export_count = 50000
    video_id_regexp = re.compile('(?<=watch\?v=).*')
    channel_id_regexp = re.compile('(?<=channel/).*')
    username_regexp = re.compile('(?<=user/).*')
    csv_pages = {
        constants.BRAND_SAFETY_PASS_VIDEOS: {'count': 0, 'page': 1},
        constants.BRAND_SAFETY_FAIL_VIDEOS: {'count': 0, 'page': 1},
        constants.BLACKLIST_VIDEOS: {'count': 0, 'page': 1},
        constants.WHITELIST_VIDEOS: {'count': 0, 'page': 1},

        constants.BRAND_SAFETY_FAIL_CHANNELS: {'count': 0, 'page': 1},
        constants.BRAND_SAFETY_PASS_CHANNELS: {'count': 0, 'page': 1},
        constants.WHITELIST_CHANNELS: {'count': 0, 'page': 1},
        constants.BLACKLIST_CHANNELS: {'count': 0, 'page': 1},
    }

    video_csv_headers = ['Channel Name', 'Channel URL', 'Channel Subscribers', 'Video Name', 'Video URL', 'Emoji Y/N', 'Views', 'Description', 'Category', 'Language', 'Country', 'Likes', 'Dislikes', 'Keyword Hits']
    channel_csv_headers = ['Channel Title', 'Channel URL', 'Language', 'Category', 'Videos', 'Channel Subscribers', 'Total Views', 'Audited Videos', 'Total Likes', 'Total Dislikes', 'Country', 'Keyword Hits']

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.audit_type = kwargs.get('type')
        self.csv_source_file_path = kwargs.get('file')
        self.csv_export_dir = kwargs.get('export')
        self.csv_export_title = kwargs.get('title')

        # self.brand_safety_regexp = self.compile_audit_regexp(self.get_all_bad_words())
        self.brand_safety_regexp = self.get_brand_safety_regexp()
        self.whitelist_regexp = self.read_and_create_keyword_regexp(kwargs['whitelist']) if kwargs.get('whitelist') else None
        self.blacklist_regexp = self.read_and_create_keyword_regexp(kwargs['blacklist']) if kwargs.get('blacklist') else None

        audits = [
            {
                'type': constants.BRAND_SAFETY,
                'regexp': self.brand_safety_regexp,

            },
            {
                'type': constants.BLACKLIST,
                'regexp': self.blacklist_regexp,

            },
            {
                'type': constants.WHITELIST,
                'regexp':self.whitelist_regexp,

            },
        ]

        self.audits = [audit for audit in audits if audit['regexp'] is not None]

    def run(self):
        print('Starting audit...')
        target, data_generator, result_processor, chunk_size = self.get_audit_config()
        self.process(target, data_generator, result_processor, chunk_size=chunk_size)

    def get_audit_config(self):
        if self.audit_type == 'video':
            return self.process_videos, self.video_data_generator, self.process_results, self.video_chunk_size

        elif self.audit_type == 'channel':
            return self.process_channels, self.channel_data_generator, self.process_results, self.channel_chunk_size

        else:
            print('Audit type not supported: {}'.format(self.audit_type))

    def process(self, target, generator, result_processor, chunk_size=5000):
        pool = Pool(processes=self.max_process_count)
        items_seen = 0

        for batch in generator():
            chunks = self.chunks(batch, chunk_size)
            all_results = pool.map(target, chunks)

            result_processor(all_results)

            items_seen += len(batch)
            print('Seen {} {}s'.format(items_seen, self.audit_type))

        print('Complete!')

    def process_results(self, results):
        video_results = {}
        channel_results = {}

        for batch_result in results:
            video_audits = batch_result['video_audit_results']
            channel_audits = batch_result['channel_audit_results']

            for audit in self.audits:
                audit_type = audit['type']
                cursor = 0

                while cursor <= len(video_audits) or cursor <= len(channel_audits):
                    try:
                        video_audit = video_audits[cursor]
                        video_results[audit_type] = video_results.get(audit_type, [])
                        video_results[audit_type].append(video_audit)

                    except IndexError:
                        pass

                    try:
                        channel_audit = channel_audits[cursor]
                        channel_results[audit_type] = channel_results.get(audit_type, [])
                        channel_results[audit_type].append(channel_audit)
                    except IndexError:
                        pass

                    cursor += 1

        for audit in self.audits:
            audit_type = audit['type']

            if audit_type == constants.BRAND_SAFETY:
                self.prepare_brand_safety_results(video_results[audit_type], data_type='video', audit_type=audit_type)
                self.prepare_brand_safety_results(channel_results[audit_type], data_type='channel', audit_type=audit_type)

            else:
                self.prepare_results(video_results[audit_type], 'video', audit_type)
                self.prepare_results(channel_results[audit_type], 'channel', audit_type)

    def process_videos(self, csv_videos):
        """
        Manager to handle video audit process for video csv data
        :param csv_videos: (generator) Yields list of csv video data
        :return:
        """
        final_results = {}

        auditor = AuditService()
        auditor.set_audits(self.audits)

        video_ids = [
            video['video_id'] if video.get('video_id')
            else re.search(self.video_id_regexp, video['video_url']).group()
            for video in csv_videos
        ]

        video_audit_results = auditor.audit_videos(video_ids=video_ids)
        channel_audit_results = auditor.audit_channels(video_audit_results)

        final_results['video_audit_results'] = video_audit_results
        final_results['channel_audit_results'] = channel_audit_results

        return final_results

    def process_channels(self, csv_channels):
        final_results = {}
        auditor = AuditService(self.audits)

        channel_ids = [
            channel['channel_id'] if channel.get('channel_id')
            else re.search(self.channel_id_regexp, channel['channel_url']).group()
            for channel in csv_channels
        ]

        video_audit_results = auditor.audit_videos(channel_ids=channel_ids)
        channel_audit_results = auditor.audit_channels(video_audit_results)

        final_results['video_audit_results'] = video_audit_results
        final_results['channel_audit_results'] = channel_audit_results

        return final_results

    def prepare_results(self, data, data_type, audit_type):
        if data_type == 'video':
            if audit_type == constants.BLACKLIST:
                csv_ref = self.csv_pages[constants.BLACKLIST_VIDEOS]

            if audit_type == constants.WHITELIST:
                csv_ref = self.csv_pages[constants.WHITELIST_VIDEOS]

        if data_type == 'channel':
            if audit_type == constants.BLACKLIST:
                csv_ref = self.csv_pages[constants.BLACKLIST_CHANNELS]

            if audit_type == constants.WHITELIST:
                csv_ref = self.csv_pages[constants.WHITELIST_CHANNELS]

        csv_export_path = self.get_export_path(csv_ref['page'], self.csv_export_dir, self.csv_export_title, data_type, audit_type)
        self.write_data(data, csv_export_path, csv_ref, data_type, audit_type)

    def prepare_brand_safety_results(self, data, data_type='video', audit_type=constants.BRAND_SAFETY):
        csv_pass_ref = self.csv_pages[constants.BRAND_SAFETY_PASS_VIDEOS] if data_type == 'video' else self.csv_pages[constants.BRAND_SAFETY_PASS_CHANNELS]
        csv_fail_ref = self.csv_pages[constants.BRAND_SAFETY_FAIL_VIDEOS] if data_type == 'video' else self.csv_pages[constants.BRAND_SAFETY_FAIL_CHANNELS]

        brand_safety_pass_path = self.get_export_path(csv_pass_ref['page'], self.csv_export_dir, self.csv_export_title, data_type, audit_type, identifier='PASS')
        brand_safety_fail_path = self.get_export_path(csv_fail_ref['page'], self.csv_export_dir, self.csv_export_title, data_type, audit_type, identifier='FAIL')

        self.write_data(data, brand_safety_pass_path, csv_pass_ref, data_type, audit_type, brand_safety=constants.BRAND_SAFETY_PASS)
        self.write_data(data, brand_safety_fail_path, csv_fail_ref, data_type, audit_type, brand_safety=constants.BRAND_SAFETY_FAIL)

    def get_export_path(self, csv_page, export_dir, title, data_type, audit_type, identifier=''):
        export_path = '{dir}Page{page}{title}{data_type}{audit_type}{identifier}.csv'.format(
            page=csv_page,
            dir=export_dir,
            title=title,
            data_type=data_type,
            audit_type=audit_type.capitalize(),
            identifier=identifier
        )
        return export_path

    def write_data(self, data, export_path, csv_ref, data_type, audit_type, brand_safety=constants.BRAND_SAFETY_PASS):
        if not data:
            print('No data for: {} {}'.format(data_type, audit_type))
            return

        # Sort items by channel title
        data.sort(key=lambda audit: audit.metadata['channel_title'])

        while True:
            next_page = False

            with open(export_path, mode='a') as export_file:
                writer = csv.writer(export_file, delimiter=',')

                if csv_ref['count'] == 0:
                    if data_type == 'video':
                        writer.writerow(self.video_csv_headers)

                    else:
                        writer.writerow(self.channel_csv_headers)

                for index, audit in enumerate(data):
                    # If brand safety pass and results contains brand safety keyword hits, do not write
                    if brand_safety == constants.BRAND_SAFETY_PASS and audit.results[constants.BRAND_SAFETY]:
                        continue

                    # If brand safety fail and results do not contain brand safety hits, do not write
                    elif brand_safety == constants.BRAND_SAFETY_FAIL and not audit.results[constants.BRAND_SAFETY]:
                        continue

                    row = audit.get_export_row(audit_type)

                    writer.writerow(row)
                    csv_ref['count'] += 1

                    # Create a new page (csv file) to write results to if count exceeds csv export limit
                    if csv_ref['count'] >= self.max_csv_export_count:
                        csv_ref['page'] += 1
                        data = data[index + 1:]
                        next_page = True
                        break

            if next_page is False:
                break

    def video_data_generator(self):
        """
        Yields each row of csv
        :return: (dict)
        """
        with open(self.csv_source_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            rows_batch = []

            for row in csv_reader:
                video_id = self.video_id_regexp.search(row.pop('video_url')).group()
                row['video_id'] = video_id
                rows_batch.append(row)

                if len(rows_batch) >= self.video_batch_size:
                    yield rows_batch
                    rows_batch.clear()

            yield rows_batch

    def channel_data_generator(self):
        """
        Yields each row of csv
        :return: (dict)
        """
        with open(self.csv_source_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            batch = []
            csv_reader = csv.DictReader(csv_file)

            for row in csv_reader:
                batch.append(row)

                if len(batch) >= self.channel_batch_size:
                    yield batch
                    batch.clear()

            yield batch

    @staticmethod
    def chunks(iterable, length):
        """
        Generator that yields equal sized lists
        """
        for i in range(0, len(iterable), length):
            yield iterable[i:i + length]

    @staticmethod
    def get_all_bad_words():
        bad_words_names = BadWord.objects.values_list("name", flat=True)
        bad_words_names = list(set(bad_words_names))

        return bad_words_names

    @staticmethod
    def compile_audit_regexp(keywords: list):
        """
        Compiles regular expression with given keywords
        :param keywords: List of keyword strings
        :return: Compiled Regular expression
        """
        regexp = re.compile(
            "({})".format("|".join([r"\b{}\b".format(re.escape(word)) for word in keywords]))
        )
        return regexp

    @staticmethod
    def read_and_create_keyword_regexp(csv_path):
        with open(csv_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)
            keywords = list(csv_reader)

            keyword_regexp = re.compile(
                '|'.join([word[0] for word in keywords]),
                re.IGNORECASE
            )

        return keyword_regexp

    def get_brand_safety_regexp(self):
        connector = Connector()
        keywords = [item['name'] for item in connector.get_bad_words_list({})]
        compiled = self.compile_audit_regexp(keywords)

        return compiled