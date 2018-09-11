import re


class BaseDMO:
    def __init__(self, **kwargs):
        for name, value in kwargs.items():
            if not hasattr(self, name):
                raise AttributeError("Unknown attribute")
            setattr(self, name, value)


class AccountDMO(BaseDMO):
    account_id = None
    name = None
    refresh_tokens = None
    client = None
    url_performance_report = None


class VideosChunkDMO(BaseDMO):
    url = None
    items = None

    def parse_page_to_items(self, page):
        self.items = []

        if page is None:
            return

        for item in page.get("items", []):
            snippet = item.get("snippet", {})
            statistics = item.get("statistics", {})
            self.items.append(
                VideoDMO(
                    id=item.get("id"),
                    title=snippet.get("title"),
                    description=snippet.get("description"),
                    channel_id=snippet.get("channelId"),
                    channel_title=snippet.get("channelTitle"),
                    tags=snippet.get("tags"),
                    likes=statistics.get("likeCount"),
                    dislikes=statistics.get("dislikeCount"),
                )
            )


class VideoDMO(BaseDMO):
    id = None
    title = None
    description = None
    channel_id = None
    channel_title = None
    likes = None
    dislikes = None
    tags = None

    RE_CLEANUP = re.compile("\W+")

    def get_text(self, clean=True):
        lines = [
            self.title or "",
            self.description or "",
        ] + (self.tags or [])
        if clean:
            text = "\n".join([
                self.RE_CLEANUP.sub(" ", i).lower().strip() for i in lines
            ])
        else:
            text = "\n".join(lines)
        return text

    @property
    def url(self):
        return "http://www.youtube.com/video/" + self.id

    @property
    def channel_url(self):
        return "http://www.youtube.com/channel/" + self.channel_id

    @property
    def sentiment(self):
        if self.likes is None or self.dislikes is None:
            return None
        likes = int(self.likes)
        dislikes = int(self.dislikes)
        if likes + dislikes == 0:
            return None
        sentiment = likes / (likes + dislikes)
        return sentiment
