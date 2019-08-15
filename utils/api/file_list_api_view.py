from django.http import FileResponse
from rest_framework.generics import ListAPIView


class FileListApiView(ListAPIView):
    filename = None
    batch_size = 1000

    def data_generator(self, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        for item in queryset:
            yield self.get_serializer(item).data

    def list(self, request, *args, **kwargs):
        renderer, content_type = self.perform_content_negotiation(request)
        rendered_data_generator = renderer.render(self.data_generator(*args, **kwargs))
        response = FileResponse(rendered_data_generator, content_type=content_type)
        response = self.add_filename(response)
        return response

    def add_filename(self, response):
        """
        Appends Content-Disposition header.
        In Django 2.1 FileResponse does this automatically
        """
        response["Content-Disposition"] = "attachment; filename=\"{}\"".format(self.filename)
        return response
