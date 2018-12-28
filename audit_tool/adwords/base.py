import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List

import yaml
from googleads import adwords
from googleads import oauth2
from oauth2client.client import HttpAccessTokenRefreshError
from suds import WebFault

from utils.utils import safe_exception
from ..dmo import AccountDMO

logger = logging.getLogger(__name__)


class AdwordsBase:
    API_VERSION = "v201809"
    MAX_WORKERS = 50

    # instance properties --->
    accounts = None
    client_options = None
    # <---

    def __init__(self, accounts: List[AccountDMO]):
        self.accounts = accounts
        self.load_client_options()
        self.resolve_clients()

    def load_client_options(self) -> None:
        with open('aw_reporting/ad_words_web.yaml', 'r') as f:
            self.client_options = yaml.load(f)

    def resolve_clients(self) -> None:
        assert self.accounts is not None
        logger.info("Resolving clients")

        @safe_exception(logger)
        def worker(dmo: AccountDMO) -> None:
            self._resolve_client(dmo)

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for _ in self.accounts:
                executor.submit(worker, _)

        clients_count = len([1 for a in self.accounts if a.client is not None])
        logger.info("Resolved {} client(s)".format(clients_count))

    def _resolve_client(self, dmo: AccountDMO) -> None:
        for refresh_token in dmo.refresh_tokens:
            try:
                dmo.client = self._get_client_by_token(
                    dmo.account_id,
                    refresh_token,
                )
            except (HttpAccessTokenRefreshError, WebFault):
                continue
            else:
                return
        raise Exception("No valid refresh tokens found")

    def _get_client_by_token(self,
                             account_id: str,
                             refresh_token: str) -> adwords.AdWordsClient:
        assert self.client_options is not None

        oauth2_client = oauth2.GoogleRefreshTokenClient(
            self.client_options["client_id"],
            self.client_options["client_secret"],
            refresh_token,
        )

        client = adwords.AdWordsClient(
            self.client_options["developer_token"],
            oauth2_client,
            user_agent=self.client_options["user_agent"],
            client_customer_id=account_id,
            enable_compression=True,
        )

        return client
