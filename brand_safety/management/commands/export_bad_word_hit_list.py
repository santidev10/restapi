import csv
import os

from django.conf import settings
from django.core.mail import EmailMessage
from django.core.management.base import BaseCommand

from brand_safety.models.bad_word import BadWord
from es_components.constants import Sections
from es_components.managers import ChannelManager


class Command(BaseCommand):
    help = 'Produce an export of all bad word hits with stats'
    words = {}

    WORD_SLICE_SIZE = 5000
    CHANNEL_SCAN_LIMIT = 0
    CHANNEL_SCAN_OUTPUT_INTERVAL = 1000

    def handle(self, *args, **options):
        self.gather_bad_word_data()
        self.gather_channel_data()
        self.finalize_export_data()
        csv_context = self.write_export()
        self.email_export(csv_context)
        print("All done!")

    def finalize_export_data(self):
        """
        uniquify all lists
        """
        print("Finalizing export data...")
        finalized = {}
        for word, word_data in self.words.items():
            for list_name in ['categories', 'languages', 'scores']:
                word_data[list_name] = list(set(word_data[list_name]))
            finalized[word] = word_data
        self.words = finalized

    def gather_bad_word_data(self):
        """
        get word, categories, languages, scores data for every bad word in PG
        """
        print("Gathering bad word data...")
        for batch in self.bad_word_generator():
            for bad_word in batch:
                self.add_bad_word_data(
                    word=bad_word.name,
                    categories=[bad_word.category.name],
                    languages=[bad_word.language.language],
                    scores=[bad_word.negative_score],
                )

    def gather_channel_data(self):
        """
        get hit count data from every channel in ES. Channel hits include channel meta and video hits
        """
        print("Gathering channel data...")
        channel_manager = ChannelManager(sections=[Sections.BRAND_SAFETY,])
        total = channel_manager._search().count()
        count = 0
        for channel in channel_manager._search().scan():
            self.add_channel_data(channel)
            count += 1
            if not count % self.CHANNEL_SCAN_OUTPUT_INTERVAL:
                print(f'channel data progress: {count}/{total}, {round((count/total)*100, 2)}%')
            if self.CHANNEL_SCAN_LIMIT and count >= self.CHANNEL_SCAN_LIMIT:
                break

    def add_channel_data(self, channel):
        categories = channel.brand_safety.categories.to_dict()
        for category_id, category in categories.items():
            keywords = category.get("keywords", [])
            for keyword_data in keywords:
                keyword = keyword_data.get('keyword', None)
                hits = keyword_data.get('hits', None)
                if not keyword or not hits:
                    continue
                self.add_bad_word_data(keyword, hit_count=hits)

    def write_export(self):
        print("Writing export...")
        filename = 'bad_word_hit_list'
        with open(filename, 'w', encoding='utf-8') as write_file:
            writer = csv.writer(write_file)
            writer.writerow(['Word', 'Categories', 'Languages', 'Scores', 'Total Hits'])
            for word, word_data in self.words.items():
                row = list()
                row.append(word)
                row.append('|'.join(word_data.get('categories', [])))
                row.append('|'.join(word_data.get('languages', [])))
                row.append('|'.join([str(number) for number in word_data.get('scores', [])]))
                row.append(word_data['hit_count'])
                writer.writerow(row)

        read_file = open(filename, 'r', encoding='utf-8')
        data = read_file.read()
        os.remove(filename)
        return data

    def email_export(self, csv_context):
        print("Emailing export...")
        to = ['andrew.wong@channelfactory.com']
        msg = EmailMessage(
            subject='bad word hit list',
            body='attached is the bad word hit list',
            from_email=settings.EXPORTS_EMAIL_ADDRESS,
            to=to,
        )

        msg.attach("export.csv", csv_context, "text/csv")
        try:
            msg.send(fail_silently=False)
        # pylint: disable=broad-except
        except Exception as e:
            # pylint: enable=broad-except
            print("Emailing export to %s failed. Error: %s", str(to), e)

    def add_bad_word_data(self, word, categories=[], languages=[], scores=[], hit_count=0):
        # word, exclusion category, language, severity, hit count
        word_cache = self.words.get(word, None)
        if not word_cache:
            word_cache = {
                "word": word,
                "categories": categories,
                "languages": languages,
                "scores": scores,
                "hit_count": hit_count,
            }
        else:
            word_cache['categories'].extend(categories)
            word_cache['languages'].extend(languages)
            word_cache['scores'].extend(scores)
            word_cache['hit_count'] += hit_count
        self.words[word] = word_cache

    def bad_word_generator(self):
        count = BadWord.objects.count()
        position = 0
        while True:
            query = BadWord.objects.order_by("name").prefetch_related("category", "language")
            if position + self.WORD_SLICE_SIZE < count:
                print('processing bad word slice: {}:{}'.format(position, position + self.WORD_SLICE_SIZE))
                yield query[position:position + self.WORD_SLICE_SIZE]
                position += self.WORD_SLICE_SIZE + 1
                continue
            print('processing final bad word slice: {}:{}'.format(position, count))
            yield query[position:count]
            break
