#!/usr/bin/env python
import os
import sys

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "saas.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError:
        # The above import may fail for some other reason. Ensure that the
        # issue is really that Django is missing to avoid masking other
        # exceptions on Python 2.
        try:
            # pylint: disable=unused-import
            import django
            # pylint: enable=unused-import
        except ImportError:
            raise ImportError(
                "Couldn't import Django. Are you sure it's installed and "
                "available on your PYTHONPATH environment variable? Did you "
                "forget to activate a virtual environment?"
            )
        raise
    try:
        execute_from_command_line(sys.argv)
    # pylint: disable=broad-except
    except Exception as ex:
    # pylint: enable=broad-except
        import logging

        logging.getLogger(__name__).exception(ex)
        raise ex
