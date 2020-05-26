from rest_framework.filters import OrderingFilter


class FreeFieldOrderingFilter(OrderingFilter):
    ordering_param = "sort"

    def remove_invalid_fields(self, queryset, fields, view, request):
        valid_fields = [item[0] for item in self.get_valid_fields(queryset, view, {"request": request})]
        return [term for term in fields if term in valid_fields]
