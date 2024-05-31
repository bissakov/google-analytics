import dataclasses
import logging
import os
from typing import MutableSequence, cast

from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.admin_v1alpha.types import ListPropertiesRequest
from google.api_core.datetime_helpers import DatetimeWithNanoseconds

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Account:
    id: str
    name: str
    region_code: str
    create_time: str
    update_time: str
    properties: MutableSequence["Property"]

    def __post_init__(self) -> None:
        self.id = self.id.split("/")[-1]
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if not isinstance(value, str):
                continue
            setattr(self, field.name, value.strip())


@dataclasses.dataclass
class Property:
    id: str
    account_id: str
    name: str
    type: str
    industry_category: str
    time_zone: str
    currency_code: str
    create_time: str
    update_time: str

    def __post_init__(self) -> None:
        self.id = self.id.split("/")[-1]
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if not isinstance(value, str):
                continue
            setattr(self, field.name, value.strip())


def fetch_account_and_properties(
    admin_credentials: str,
) -> MutableSequence[Account]:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = admin_credentials

    client = AnalyticsAdminServiceClient()

    account_results = client.list_accounts()
    accounts = [
        Account(
            id=account.name,
            name=account.display_name,
            region_code=account.region_code,
            create_time=cast(
                DatetimeWithNanoseconds, account.create_time
            ).isoformat(timespec="seconds"),
            update_time=cast(
                DatetimeWithNanoseconds, account.update_time
            ).isoformat(timespec="seconds"),
            properties=[],
        )
        for account in account_results
    ]

    logger.info(f"Found {len(accounts)} accounts")

    if accounts:
        for account in accounts:
            property_results = client.list_properties(
                request=ListPropertiesRequest(
                    filter=f"parent:accounts/{account.id}"
                )
            )

            for property in property_results:
                account.properties.append(
                    Property(
                        id=property.name,
                        account_id=account.id,
                        name=property.display_name,
                        type=property.property_type.name,
                        industry_category=property.industry_category.name,
                        time_zone=property.time_zone,
                        currency_code=property.currency_code,
                        create_time=cast(
                            DatetimeWithNanoseconds, property.create_time
                        ).isoformat(timespec="seconds"),
                        update_time=cast(
                            DatetimeWithNanoseconds, property.update_time
                        ).isoformat(timespec="seconds"),
                    )
                )

            logger.info(
                f"Found {len(account.properties)} properties for account {account.name}"
            )

    logger.info("Accounts and properties fetched successfully")

    return accounts
