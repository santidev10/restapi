class PersistentEntityModelMixin:
    _is_demo_expressions = None

    @classmethod
    def persistent_items(cls):
        return cls._get_demo_items()

    @classmethod
    def _get_demo_items(cls):
        return cls.objects.filter(cls._is_demo_expressions)
