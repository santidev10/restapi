from concurrent.futures import ThreadPoolExecutor
import logging
import requests
import time

from utils.utils import chunks_generator

from .dmo import VideosChunkDMO


logger = logging.getLogger(__name__)

logging.getLogger("requests")\
    .setLevel(logging.WARNING)


class Youtube:
    MAX_WORKERS = 50
    DATA_API_KEY = "AIzaSyA0flV0CBNOoN6qwdkGHoZytMu8y4ThIjs"
    DATA_API_URL = "https://www.googleapis.com/youtube/v3/videos" \
                   "?key={key}&part=id,snippet&id={ids}"

    chunks = None

    def download(self, ids):
        self.chunks = []
        for chunk in chunks_generator(iterable=ids, size=50):
            ids_str = ",".join(chunk)
            url = self.DATA_API_URL.format(key=self.DATA_API_KEY, ids=ids_str)
            self.chunks.append(VideosChunkDMO(url=url))

        def worker(dmo, n):
            if n % 50 == 0:
                logger.info("  chunk {} / {}".format(n, len(self.chunks)))
            for i in range(5):
                try:
                    r = requests.get(dmo.url)
                    data = r.json()
                    dmo.parse_page_to_items(data)
                except Exception as e:
                    logger.error("Requests Error (try 1/5):" + str(e))
                    time.sleep(15)
                else:
                    break

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as e:
            for n, v in enumerate(self.chunks):
                e.submit(worker, v, n)
        logger.info("Done")

    def get_all_items(self):
        if not self.chunks:
            return
        for chunk in self.chunks:
            if chunk.items is None:
                continue
            for item in chunk.items:
                yield item
