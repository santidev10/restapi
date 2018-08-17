from typing import List

from django.db import models

from aw_reporting.models.base import BaseQueryset
from userprofile.models import UserProfile, get_default_settings, \
    UserSettingsKey, logger
from utils.registry import registry


class UserRelatedManager(models.Manager.from_queryset(BaseQueryset)):
    _account_id_ref = None

    def __filter_by_account_ids(self, queryset, account_ids: List[str]):
        if self._account_id_ref is None:
            raise NotImplementedError("_account_id_ref should be defined")
        return queryset \
            .filter(**{self._account_id_ref + "__in": account_ids}) \
            .distinct()

    def __is_account_filter_applicable(self, user: UserProfile):
        user_settings = user.aw_settings \
            if hasattr(user, "aw_settings") else get_default_settings()
        global_visibility = user_settings.get(
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY, False)
        visible_all_accounts = user_settings.get(
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS, False)

        return global_visibility & (not visible_all_accounts)

    def __filter_by_user(self, queryset: models.QuerySet, user: UserProfile):
        if self.__is_account_filter_applicable(user):
            account_ids = user.get_aw_settings() \
                .get(UserSettingsKey.VISIBLE_ACCOUNTS)
            queryset = self.__filter_by_account_ids(queryset, account_ids)
        return queryset

    def get_queryset(self, ignore_user=False):
        queryset = super(UserRelatedManager, self).get_queryset()
        user = registry.user
        if user is None:
            logger.debug("%s is used with no user in context",
                         type(self).__name__)
        elif not ignore_user:
            queryset = self.__filter_by_user(queryset, user)
        return queryset