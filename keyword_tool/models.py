import json
import logging

from django.contrib.postgres.fields import JSONField
from django.db import models
from django.db import transaction

from aw_reporting.models.base import BaseModel
from .tasks import update_keywords_stats

logger = logging.getLogger(__name__)

AVAILABLE_KEYWORD_LIST_CATEGORIES = (
    "yt",
    "private",
    "chf",
    "blacklist"
)


class Interest(models.Model):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=200)

    def __str__(self):
        return "%s" % self.name


class Query(models.Model):
    text = models.CharField(max_length=250, primary_key=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return "%s: %d kws" % (self.text, self.keywords.count())

    @classmethod
    def create_from_aw_response(cls, query, response):
        # models
        interest_relation = KeyWord.interests.through
        query_relation = KeyWord.queries.through

        # get ids
        interest_ids = set(
            Interest.objects.all().values_list('id', flat=True)
        )
        keywords_ids = set(
            KeyWord.objects.filter(
                text__in=[i['keyword_text'] for i in response]
            ).values_list('text', flat=True)
        )

        # create items
        kws = []
        update_kws = []
        interest_relations = []
        query_relations = []
        for k in response:
            keyword_text = k['keyword_text']
            if keyword_text in keywords_ids:
                update_kws.append(k)
            else:
                kws.append(
                    KeyWord(
                        text=keyword_text,
                        average_cpc=k.get('average_cpc'),
                        competition=k.get('competition'),
                        _monthly_searches=json.dumps(
                            k.get('monthly_searches', [])
                        ),
                        search_volume=k.get('search_volume'),
                    )
                )
                for interest_id in k['interests']:
                    if interest_id in interest_ids:
                        interest_relations.append(
                            interest_relation(
                                keyword_id=keyword_text,
                                interest_id=interest_id,
                            )
                        )
            query_relations.append(
                query_relation(
                    keyword_id=k['keyword_text'],
                    query_id=query,
                )
            )

        query_obj = cls.objects.create(text=query)

        if kws:
            KeyWord.objects.safe_bulk_create(kws)
        if interest_relations:
            interest_relation.objects.bulk_create(interest_relations)
        if query_relations:
            query_relation.objects.bulk_create(query_relations)

        if update_kws:
            update_keywords_stats.delay(update_kws)

        logger.debug("Save")

        return query_obj


class KeyWord(BaseModel):
    text = models.CharField(max_length=250, primary_key=True)
    interests = models.ManyToManyField(Interest)
    queries = models.ManyToManyField(Query, related_name="keywords")
    average_cpc = models.FloatField(null=True)
    competition = models.FloatField(null=True)
    updated_at = models.DateTimeField(auto_now=True)
    _monthly_searches = models.TextField(null=True)
    search_volume = models.IntegerField(null=True)
    category = models.CharField(max_length=255, null=True, blank=True,
                                db_index=True)
    daily_views = models.BigIntegerField(default=0, db_index=True)
    weekly_views = models.BigIntegerField(default=0, db_index=True)
    thirty_days_views = models.BigIntegerField(default=0, db_index=True)
    views = models.BigIntegerField(default=0, db_index=True)

    def get_monthly_searches(self):
        raw = self._monthly_searches
        if raw:
            return json.loads(raw)

    def set_monthly_searches(self, value):
        self._monthly_searches = json.dumps(value)

    monthly_searches = property(get_monthly_searches, set_monthly_searches)

    def __str__(self):
        return self.text

    @property
    def interests_top_kw(self):
        top_kw_interests = {}
        interests_ids = self.interests.all().values_list('id', flat=True)
        for interests_id in interests_ids:
            keywords = Interest.objects.get(id=interests_id).keyword_set.all() \
                           .exclude(text=self.text).order_by(
                '-search_volume').values_list('text', flat=True)[:5]
            top_kw_interests[interests_id] = keywords
        return top_kw_interests

    class Meta:
        ordering = ["pk"]


class KeywordsList(BaseModel):
    name = models.TextField()
    user_email = models.EmailField(db_index=True)
    keywords = models.ManyToManyField(KeyWord, related_name='lists')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    category = models.CharField(max_length=255, null=True, blank=True)

    # De normalized fields
    num_keywords = models.IntegerField(default=0)
    average_volume = models.BigIntegerField(default=0)
    average_cpc = models.FloatField(default=0)
    competition = models.FloatField(default=0)
    average_cpv = models.FloatField(default=0)
    average_view_rate = models.FloatField(default=0)
    average_ctrv = models.FloatField(default=0)
    top_keywords = JSONField(null=True, blank=True)
    cum_average_volume = JSONField(null=True, blank=True)
    cum_average_volume_per_kw = JSONField(null=True, blank=True)

    class Meta:
        ordering = ['-updated_at']

    @property
    def top_keywords_data(self):
        return self.top_keywords

    @property
    def cum_average_volume_data(self):
        return self.cum_average_volume

    @property
    def cum_average_volume_per_kw_data(self):
        return self.cum_average_volume_per_kw


class ViralKeywords(BaseModel):
    keyword = models.ForeignKey(KeyWord, related_name='viral_keyword')
