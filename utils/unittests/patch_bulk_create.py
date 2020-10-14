def patch_bulk_create(model, objs):
    model.objects.bulk_create(objs)
