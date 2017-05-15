class SingledbRouter:
    def db_for_read(self, model, **hints):
        if model._meta.app_label == 'singledb':
            return 'data'
        return None

    def db_for_write(self, model, **hints):
        if model._meta.app_label == 'singledb':
            return 'data'
        return None
