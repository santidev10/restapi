from typing import List

from django.db import models

from userprofile.constants import StaticPermissions


class UserRelatedManagerMixin:
    _account_id_ref = None

    def get_queryset_for_user(self, user=None):
        queryset = self.get_queryset()
        if user is None:
            return queryset
        queryset = self.__filter_by_user(queryset, user)
        return queryset

    def __filter_by_user(self, queryset: models.QuerySet, user):
        if self.__is_account_filter_applicable(user):
            account_ids = user.get_visible_accounts_list()
            queryset = self.__filter_by_account_ids(queryset, account_ids)
        return queryset

    def __is_account_filter_applicable(self, user):
        global_visibility = user.has_permission(StaticPermissions.MANAGED_SERVICE__GLOBAL_ACCOUNT_VISIBILITY)
        visible_all_accounts = user.has_permission(StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS)
        return global_visibility & (not visible_all_accounts)

    def __filter_by_account_ids(self, queryset, account_ids: List[int]):
        if self._account_id_ref is None:
            raise NotImplementedError("_account_id_ref should be defined")
        return queryset \
            .filter(**{self._account_id_ref + "__in": account_ids}) \
            .distinct()
