from .views import *


def demo_view_decorator(wrapped_class):
    local_vars = globals()
    if wrapped_class.__name__ in local_vars:
        mock_class = local_vars[wrapped_class.__name__]

        for method_name, mock_method in mock_class.__dict__.items():
            if not method_name.startswith('__'):
                original_method = getattr(wrapped_class, method_name)
                setattr(wrapped_class, method_name,
                        mock_method.__func__(original_method))
    return wrapped_class
