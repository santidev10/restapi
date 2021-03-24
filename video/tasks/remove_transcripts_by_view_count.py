from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.query_builder import QueryBuilder

from utils.utils import chunks_generator


# 10M
VIEW_COUNT_THRESHOLD = 10000000
CHUNK_SIZE = 500


class TranscriptsTrimmer:
    """
    clear transcripts from videos that have transcripts, and whose view count is less than some threshold
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
