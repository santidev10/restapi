from .views import *
from aw_creation.demo.views import *


def demo_view_decorator(wrapped_class):
    local_vars = globals()
    if wrapped_class.__name__ in local_vars:
        mock_class = local_vars[wrapped_class.__name__]

        for method_name in dir(mock_class):
            if not method_name.startswith('__'):
                mock_method = getattr(mock_class, method_name)
                original_method = getattr(wrapped_class, method_name)
                setattr(wrapped_class, method_name,
                        mock_method(original_method))
    return wrapped_class
