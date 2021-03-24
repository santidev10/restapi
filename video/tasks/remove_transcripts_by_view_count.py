from time import sleep

from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.query_builder import QueryBuilder
from elasticsearch.exceptions import ConnectionTimeout

from utils.utils import chunks_generator


# 10M
VIEW_COUNT_THRESHOLD = 10000000
CHUNK_SIZE = 500
MAX_RETRIES = 100


def run_with_retries(max_retries=MAX_RETRIES):
    """
    catches connection timeout and retries max_retries times
    :param max_retries:
    :return:
    """
    tries = 0
    instance = TranscriptsTrimmer()
    while True:
        tries += 1
        print(f"tries: {tries}")
        try:
            instance.run()
        except ConnectionTimeout:
            pass

        if tries > max_retries:
            break

        sleep(60)


class TranscriptsTrimmer:
    """
    clear transcripts from videos that have transcripts, and whose view count is less than some threshold
    process by views desc. with the idea that vids with higher view counts are less likely to be deleted
    """

    def __init__(self):
        self.manager = VideoManager(sections=(Sections.MAIN,), upsert_sections=(Sections.CUSTOM_CAPTIONS,))

        self.processed_count = 0
        self.upsert_queue = []

    def run(self):
        """
        entrypoint method
        :return:
        """
        # query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.views").gte(VIEW_COUNT_THRESHOLD).get()
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.views").lt(VIEW_COUNT_THRESHOLD).get()
        query &= QueryBuilder().build().must().exists().field(f"{Sections.CUSTOM_CAPTIONS}.items").get()

        sort = [{f"{Sections.STATS}.views": {"order": "desc"}}]
        search = self.manager.search(query=query, sort=sort)
        print(f"trimming transcripts from: {search.count():,} videos")

        for chunk in chunks_generator(search.scan(), size=CHUNK_SIZE):
            self._handle_chunk(chunk)
            print(f"processed: {self.processed_count:,} videos")

    def _handle_chunk(self, chunk):
        """
        handle a chunk of videos
        :param chunk:
        :return:
        """
        for video in chunk:
            video.populate_custom_captions(items=[{}])
            self.upsert_queue.append(video)

        self._do_upsert()
        self.processed_count += len(self.upsert_queue)
        self.upsert_queue = []

    def _do_upsert(self):
        """
        upsert the videos without captions
        :return:
        """
        self.manager.upsert(self.upsert_queue)
