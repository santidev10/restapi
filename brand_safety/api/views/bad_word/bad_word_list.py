import string

from rest_framework.generics import ListCreateAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.serializers import ValidationError

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.api.views.pagination import BrandSafetyPaginator
from brand_safety.models import BadWord
from distutils.util import strtobool
from utils.utils import remove_tags_punctuation
from utils.lang import is_english


class BadWordListApiView(ListCreateAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordSerializer
    pagination_class = BrandSafetyPaginator
    MIN_SEARCH_LENGTH = 3

    def do_filters(self, queryset):
        filters = {}

        search = self.request.query_params.get("search")
        if search and is_english(search):
            if len(search) < self.MIN_SEARCH_LENGTH:
                raise ValidationError("English search term must be at least {} characters.".format(self.MIN_SEARCH_LENGTH))
            filters["name__icontains"] = search

        category = self.request.query_params.get("category")
        if category:
            try:
                category_id = int(category)
                filters["category_id"] = category_id
            except ValueError:
                raise ValidationError("Category filter param must be Category ID value. Received: {}.".format(category))

        language = self.request.query_params.get("language")
        if language:
            filters["language__language"] = language

        negative_scores = self.request.query_params.get("negative_score")
        if negative_scores:
            negative_scores = negative_scores.split(',')
            filters["negative_score__in"] = negative_scores

        meta_scoring = self.request.query_params.get("meta_scoring")
        if meta_scoring is not None:
            if isinstance(meta_scoring, bool):
                filters["meta_scoring"] = meta_scoring
            else:
                try:
                    filters["meta_scoring"] = strtobool(meta_scoring)
                except ValueError:
                    raise ValidationError("Meta_scoring filter param must be True or False. Received: {}.".format(meta_scoring))

        comment_scoring = self.request.query_params.get("comment_scoring")
        if comment_scoring is not None:
            if isinstance(comment_scoring, bool):
                filters["comment_scoring"] = comment_scoring
            else:
                try:
                    filters["comment_scoring"] = strtobool(comment_scoring)
                except ValueError:
                    raise ValidationError("Comment_scoring filter param must be True or False. Received: {}.".format(comment_scoring))


        if filters:
            queryset = queryset.filter(**filters)
        return queryset

    def get_queryset(self):
        queryset = BadWord.objects.select_related("category", "language").all().order_by("name")
        queryset = self.do_filters(queryset)
        return queryset

    def get(self, request, *args, **kwargs):
        page = request.query_params.get("page")
        if page is None:
            self.pagination_class = None
        result = super().get(request, *args, **kwargs)
        return result

    def post(self, request):
        serializers = []
        try:
            tag_names = request.data["name"].split(",")
        except Exception as e:
            raise ValidationError("'name' field is required for BadWord object.")

        for tag_name in tag_names:
            tag_data = dict(request.data)
            tag_data["name"] = remove_tags_punctuation(tag_name.lower().strip())
            serializer = BadWordSerializer(data=tag_data, context={'request': request})
            serializers.append(serializer)
        results = []
        for serializer in serializers:
            if serializer.is_valid():
                try:
                    validated_data = serializer.validated_data
                    existing_word = BadWord.all_objects.get(name=validated_data["name"], language=validated_data["language"])
                    # If the word has been soft deleted, reset its deleted_at
                    if existing_word.deleted_at is not None:
                        existing_word.deleted_at = None
                        existing_word.save(update_fields=['deleted_at'])
                        existing_word_serializer = self.serializer_class(existing_word, data=tag_data)
                        existing_word_serializer.is_valid(raise_exception=True)
                        existing_word_serializer.save()
                        result = existing_word_serializer.data
                        results.append(result)
                        status = HTTP_201_CREATED
                    else:
                        # Reject trying to create a word that has not been soft deleted
                        result = "{} and {} must make a unique set.".format(validated_data["name"], str(validated_data["language"]))
                        results.append(result)
                        status = HTTP_400_BAD_REQUEST
                except BadWord.DoesNotExist:
                    serializer.save()
                    results.append(serializer.data)
                    status = HTTP_201_CREATED
            else:
                result = serializer.errors
                results.append(result)
                status = HTTP_400_BAD_REQUEST
        return Response(data=results, status=status)

