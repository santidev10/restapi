import os


APP_NAMES = ['audit_tool', 'brand_safety', 'transcripts']
DB_NAME = os.getenv('AUDIT_DB_NAME', 'audit')


class AuditDBRouter:
    """
    A router to control all database operations on models in the
    audit_tool application.
    """
    def db_for_read(self, model, **hints):
        if model._meta.app_label in APP_NAMES:
            return DB_NAME

    def db_for_write(self, model, **hints):
        if model._meta.app_label in APP_NAMES:
            return DB_NAME

    def allow_relation(self, obj1, obj2, **hints):
        if obj1._meta.app_label in APP_NAMES and obj2._meta.app_label in APP_NAMES:
            return True
        elif all([app_name not in [obj1._meta.app_label, obj2._meta.app_label] for app_name in APP_NAMES]):
            return None
        return False

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if app_label in APP_NAMES:
            # audit_tool app should be migrated only on the audit database.
            return db == DB_NAME
        elif db == DB_NAME:
            # ensure that all other apps don't get migrated on the audit database.
            return False
