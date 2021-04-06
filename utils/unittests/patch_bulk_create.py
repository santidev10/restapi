from django.db import IntegrityError

def patch_bulk_create(model, objs, *_, **__):
    model.objects.bulk_create(objs)


def patch_safe_bulk_create(objs, *_, **__):
    for obj in objs:
        try:
            obj.save()
        except IntegrityError:
            pass
