from celery import task


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
            kw.save()  # lol

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

