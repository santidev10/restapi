class DemoEntityModelMixin:
    _is_demo_expressions = None

    @classmethod
    def demo_items(cls):
        return cls._get_demo_items()

    @classmethod
    def _get_demo_items(cls):
        return cls.objects.filter(cls._is_demo_expressions)

    @classmethod
    def not_demo_items(cls):
        return cls.objects.exclude(cls._is_demo_expressions)
