from celery import task
from django.db.models import Avg


@task
def update_keywords_stats(data):
    from .models import KeyWord, Interest
    interest_relation = KeyWord.interests.through

    for k in data:
        try:
            kw = KeyWord.objects.get(pk=k['keyword_text'])
        except KeyWord.DoesNotExist:
            pass
        else:
            kw.average_cpc = k.get('average_cpc')
            kw.competition = k.get('competition')
            kw.monthly_searches = k.get('monthly_searches', [])
            kw.search_volume = k.get('search_volume')
            kw.save()

            # manage interests
            old_ids = set(
                kw.interests.all().values_list('id', flat=True)
            )
            new_ids = set(
                Interest.objects.filter(
                    id__in=[interest_id for interest_id in k['interests']]
                ).values_list('id', flat=True)
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

    return


@task
def update_kw_list_stats(obj):
    kw_querry = obj.keywords.through.objects.filter(keywordslist_id=obj.id)
    obj.num_keywords = kw_querry.count()
    count_data = kw_querry.aggregate(average_volume=Avg('keyword__search_volume'),
                                             average_cpc=Avg('keyword__average_cpc'),
                                             competition=Avg('keyword__competition'))
    obj.average_volume = count_data['average_volume']
    obj.average_cpc = count_data['average_cpc']
    obj.competition = count_data['competition']
    # TODO Update adword fiels in future
    obj.save()
    return
