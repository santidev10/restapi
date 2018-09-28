from typing import List

from django.db import models

from userprofile.models import UserProfile
from userprofile.models import UserSettingsKey
from userprofile.models import get_default_settings


class UserRelatedManagerMixin:
    _account_id_ref = None

    def get_queryset_for_user(self, user=None):
        queryset = self.get_queryset()
        if user is None:
            return queryset
        queryset = self.__filter_by_user(queryset, user)
        return queryset

    def __filter_by_user(self, queryset: models.QuerySet, user: UserProfile):
        if self.__is_account_filter_applicable(user):
            account_ids = user.get_aw_settings() \
                .get(UserSettingsKey.VISIBLE_ACCOUNTS)
            queryset = self.__filter_by_account_ids(queryset, account_ids)
        return queryset

    def __is_account_filter_applicable(self, user: UserProfile):
        user_settings = user.aw_settings \
            if hasattr(user, "aw_settings") else get_default_settings()
        global_visibility = user_settings.get(
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY, False)
        visible_all_accounts = user_settings.get(
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS, False)

        return global_visibility & (not visible_all_accounts)

    def __filter_by_account_ids(self, queryset, account_ids: List[str]):
        if self._account_id_ref is None:
            raise NotImplementedError("_account_id_ref should be defined")
        return queryset \
            .filter(**{self._account_id_ref + "__in": account_ids}) \
            .distinct()
