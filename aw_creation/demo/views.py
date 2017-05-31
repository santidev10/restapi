from aw_reporting.demo.models import DemoAccount


class OptimizationAccountListApiView:

    @staticmethod
    def get(original_method):
        from rest_framework.status import HTTP_200_OK

        def method(view, request, **kwargs):
            response = original_method(view, request, **kwargs)
            if response.status_code == HTTP_200_OK:
                demo = DemoAccount()
                response.data['items'].insert(0, demo.creation_details)
                response.data['items_count'] += 1
            return response
        return method
