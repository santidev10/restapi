from rest_framework.parsers import FileUploadParser

from utils.api.exceptions import PayloadTooLarge


class FileSizeLimitedParser(FileUploadParser):
    size_limit_mb = None

    @property
    def size_limit(self):
        return int(self.size_limit_mb * (1024 ** 2)) \
            if self.size_limit_mb is not None \
            else None

    def _validate_by_header(self, parser_context):
        request = parser_context['request']
        meta = request.META
        try:
            content_length = int(meta.get('HTTP_CONTENT_LENGTH',
                                          meta.get('CONTENT_LENGTH', 0)))
        except (ValueError, TypeError):
            content_length = None

        if content_length is not None \
                and self.size_limit is not None \
                and self.size_limit < content_length:
            raise PayloadTooLarge(self.size_limit, content_length)

    def _validate_by_content_size(self, file):
        if self.size_limit is not None \
                and file.size > self.size_limit:
            raise PayloadTooLarge(self.size_limit, file.size)

    def parse(self, stream, media_type=None, parser_context=None):
        self._validate_by_header(parser_context)

        result = super(FileSizeLimitedParser, self).parse(stream, media_type, parser_context)

        self._validate_by_content_size(result.files["file"])
        return result
