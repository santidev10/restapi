from collections import Counter

from saas import celery_app
from django.db.models import Avg


@celery_app.task
def update_keywords_stats(data):
    # pylint: disable=import-outside-toplevel
    from .models import KeyWord
    from .models import Interest
    # pylint: enable=import-outside-toplevel
    interest_relation = KeyWord.interests.through

    for k in data:
        try:
            kw = KeyWord.objects.get(pk=k["keyword_text"])
        except KeyWord.DoesNotExist:
            pass
        else:
            kw.average_cpc = k.get("average_cpc")
            kw.competition = k.get("competition")
            kw.monthly_searches = k.get("monthly_searches", [])
            kw.search_volume = k.get("search_volume")
            kw.save()

            # manage interests
            old_ids = set(
                kw.interests.all().values_list("id", flat=True)
            )
            new_ids = set(
                Interest.objects.filter(
                    id__in=k["interests"]
                ).values_list("id", flat=True)
            )

            # delete interests
            delete = old_ids - new_ids
            if delete:
                interest_relation.objects.filter(
                    keyword_id=kw.pk,
                    interest_id__in=delete
                )

            # add interests
            add = new_ids - old_ids
            if add:
                interest_relations = []
                for interest_id in add:
                    interest_relations.append(
                        interest_relation(
                            keyword_id=kw.pk,
                            interest_id=interest_id,
                        )
                    )
                interest_relation.objects.bulk_create(interest_relations)


@celery_app.task
def update_kw_list_stats(obj, _kw_class):
    cum_counter = Counter()
    kw_query = obj.keywords.through.objects.filter(keywordslist_id=obj.id)
    kw_ids = kw_query.values_list("keyword__text", flat=True)
    count_data = kw_query.aggregate(average_volume=Avg("keyword__search_volume"),
                                    average_cpc=Avg("keyword__average_cpc"),
                                    competition=Avg("keyword__competition"))
    obj_kw = _kw_class.objects.filter(text__in=kw_ids).order_by("-search_volume")
    for item in obj_kw:
        if item.monthly_searches:
            for date_search in item.monthly_searches:
                cum_counter.update(**{date_search["label"]: date_search["value"]})
    top_keywords = obj_kw[:10]

    obj.num_keywords = kw_query.count()
    obj.average_volume = count_data["average_volume"] or 0
    obj.average_cpc = count_data["average_cpc"] or 0
    obj.competition = count_data["competition"] or 0
    obj.top_keywords = [{"keyword": kw.text,
                         "value": kw.search_volume} for kw in top_keywords]
    obj.cum_average_volume = dict(cum_counter)
    obj.cum_average_volume_per_kw = {k: v / int(obj.num_keywords) for k, v in cum_counter.items()}
    # TODO Update adword fiels in future
    obj.save()
