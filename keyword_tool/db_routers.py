
class KWToolRouter:
    """
    A router to control all database operations on models in the
    aw_campaign application.
    """
    @staticmethod
    def db_for_read(model, **_):
        if model._meta.app_label == 'keyword_tool':
            return 'aw_campaign'
        return None

    @staticmethod
    def db_for_write(model, **_):
        if model._meta.app_label == 'keyword_tool':
            return 'aw_campaign'
        return None

    # @staticmethod
    # def allow_migrate(db, app_label, **_):
    #     if app_label == 'aw_campaign':
    #         return db == 'aw_campaign'
