from aw_creation.demo import views as aw_creation_views
from aw_reporting.demo import views as aw_reporting_views
from utils.lang import merge_dicts

DEMO_MODULES = (
    aw_creation_views,
    aw_reporting_views
)
DEMO_MODULES_EXPORTS = merge_dicts(*[module.__dict__ for module in DEMO_MODULES])


def demo_view_decorator(wrapped_class):
    if wrapped_class.__name__ in DEMO_MODULES_EXPORTS:
        mock_class = DEMO_MODULES_EXPORTS[wrapped_class.__name__]

        for method_name in dir(mock_class):
            if not method_name.startswith('__'):
                mock_method = getattr(mock_class, method_name)
                original_method = getattr(wrapped_class, method_name)
                setattr(wrapped_class, method_name,
                        mock_method(original_method))
    return wrapped_class
