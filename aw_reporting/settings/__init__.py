import logging
import os

import yaml
from django.conf import settings

logger = logging.getLogger(__name__)


class FileSettings:
    file_name = None
    data = {}

    @staticmethod
    def load(stream):
        raise NotImplementedError

    @staticmethod
    def dump(stream, data):
        raise NotImplementedError

    def reset_settings(self):
        try:
            os.remove(self.local_file)
        except OSError:
            pass

    def __init__(self):
        self.data = {k: v for k, v in self.data.items()}

        if settings.IS_TEST:
            mask = "test_%s.yml"
        else:
            mask = "%s.yml"

        directory = os.path.dirname(__file__)
        self.local_file = os.path.join(directory, mask % self.file_name)

        if os.path.isfile(self.local_file):
            with open(self.local_file, 'r') as stream:
                try:
                    data = self.load(stream)
                    self.data.update(data)
                except Exception as exc:
                    logger.critical(exc)
                else:
                    self.data.update(data)

    def get(self, name=None):
        return self.data.get(name) if name is not None else self.data

    def update(self, data=None, **kwargs):
        data = data or {}
        data.update(kwargs)
        assert isinstance(data, type(self.data)), "Wrong format"

        assert len(set(data.keys()) - set(self.data.keys())) == 0, \
            "Wrong data keys"

        for k, v in data.items():
            assert isinstance(v, type(self.data[k])), \
                "Wrong data types"

        self.data.update(data)

        with open(self.local_file, "w") as stream:
            self.dump(stream, self.data)

        return self.data


class YAMLSettings(FileSettings):

    @staticmethod
    def load(stream):
        return yaml.load(stream)

    @staticmethod
    def dump(stream, data):
        return yaml.dump(data, stream, default_flow_style=False)


class InstanceSettingsKey:
    HIDE_REMARKETING = "dashboard_remarketing_tab_is_hidden"
    VISIBLE_ACCOUNTS = "visible_accounts"


class InstanceSettings(YAMLSettings):
    data = {
        'dashboard_campaigns_segmented': False,
        'dashboard_ad_words_rates': False,
        'demo_account_visible': False,
        InstanceSettingsKey.HIDE_REMARKETING: False,
        'dashboard_costs_are_hidden': False,
        'show_conversions': False,
        InstanceSettingsKey.VISIBLE_ACCOUNTS: [],
        'hidden_campaign_types': {},
        'global_account_visibility': False,
    }

    file_name = "instance"
