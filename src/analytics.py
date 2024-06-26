import dataclasses
import logging
import os
import pathlib
import pickle
import sys
import uuid
from collections import deque
from dataclasses import fields
from datetime import datetime
from typing import Any, Dict, MutableSequence, Optional, TypeVar, Union

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    OrderBy,
)
from google.analytics.data_v1beta.types import Row as ResponseRow
from google.analytics.data_v1beta.types import RunReportRequest
from google.analytics.data_v1beta.types.data import (
    DimensionHeader,
    DimensionValue,
    MetricHeader,
    MetricValue,
)

from src.account_properties import Account
from src.error import handle_google_api_exception

logger = logging.getLogger(__name__)


Dimensions = MutableSequence[Dimension]
Metrics = MutableSequence[Metric]
DateRanges = MutableSequence[DateRange]
OrderBys = MutableSequence[OrderBy]

DimensionHeaders = MutableSequence[DimensionHeader]
MetricHeaders = MutableSequence[MetricHeader]
Headers = Union[DimensionHeaders, MetricHeaders]

DimensionValues = MutableSequence[DimensionValue]
MetricValues = MutableSequence[MetricValue]
Values = Union[DimensionValues, MetricValues]

T = TypeVar("T")


class Deque(deque[T]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.size = 0

    def append(self, item: Any) -> None:
        super().append(item)
        self.size += 1

    def __len__(self) -> int:
        return self.size


@dataclasses.dataclass
class Report:
    property_id: str
    dimensions: Dimensions
    metrics: Metrics
    date_ranges: DateRanges
    order_bys: OrderBys
    property: Optional[str] = None

    def __post_init__(self) -> None:
        self.property = f"properties/{self.property_id}"

    def as_dict(
        self,
    ) -> Dict[str, Any]:
        if self.property is None:
            raise ValueError("property_id is not set")
        return {
            field.name: getattr(self, field.name)
            for field in fields(self)
            if field.name != "property_id"
        }


@dataclasses.dataclass
class AnalyticsRow:
    date: str
    city: str
    city_id: str
    country: str
    country_code: str
    sessions: int
    new_users: int
    total_users: int
    bounce_rate: float
    user_engagement_duration: int
    uuid: Optional[str] = None  # unique UUID for potential duplication in DB

    def __post_init__(self) -> None:
        row_identifier = f"{self.date}_{self.city_id}_{self.country_code}"
        self.uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, row_identifier))
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if not isinstance(value, str):
                continue
            setattr(self, field.name, value.strip())


def process_response_columns(
    row_data: Dict[str, Any],
    keys: Headers,
    values: Values,
) -> Dict[str, Any]:
    for key, value in zip(keys, values):
        row_data[key.name] = value.value
    return row_data


def process_response_row(
    response_row: ResponseRow,
    dimension_headers: DimensionHeaders,
    metric_headers: MetricHeaders,
) -> AnalyticsRow:
    row_data = process_response_columns(
        {}, dimension_headers, response_row.dimension_values
    )
    row_data = process_response_columns(
        row_data, metric_headers, response_row.metric_values
    )

    return AnalyticsRow(
        date=datetime.strptime(row_data["date"], "%Y%m%d").strftime("%Y-%m-%d"),
        city=row_data["city"],
        city_id=row_data["cityId"],
        country=row_data["country"],
        country_code=row_data["countryId"],
        sessions=int(row_data["sessions"]),
        new_users=int(row_data["newUsers"]),
        total_users=int(row_data["totalUsers"]),
        bounce_rate=float(row_data["bounceRate"]),
        user_engagement_duration=int(row_data["userEngagementDuration"]),
    )


@handle_google_api_exception
def run_report(request: RunReportRequest) -> Deque[AnalyticsRow]:
    client = BetaAnalyticsDataClient()

    response = client.run_report(request=request)

    analytics_queue = Deque()

    for row in response.rows:
        analytics_row = process_response_row(
            row,
            response.dimension_headers,
            response.metric_headers,
        )
        analytics_queue.append(analytics_row)

    return analytics_queue


def fetch_analytics(
    service_credentials: str,
    accounts: MutableSequence[Account],
    date_range: DateRange,
) -> MutableSequence[Deque[AnalyticsRow]]:
    pickle_dir = os.environ.get("PICKLE_DIR")
    if not pickle_dir:
        raise EnvironmentError("Missing 'PICKLE_DIR' environment variable")

    analytics_pickle = pathlib.Path(
        pickle_dir,
        f"analytics_{date_range.start_date}_{date_range.end_date}.pkl",
    ).as_posix()
    if os.path.exists(analytics_pickle):
        logger.info(
            f"Loading analytics from '{analytics_pickle}' instead of fetching from API"
        )
        with open(analytics_pickle, "rb") as f:
            analytics = pickle.load(f)

        logger.info(f"Loaded {sum(len(a) for a in analytics)} rows in total")
        return analytics

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_credentials

    dimensions = ["date", "city", "cityId", "country", "countryId"]
    metrics = [
        "sessions",
        "newUsers",
        "totalUsers",
        "bounceRate",
        "userEngagementDuration",
    ]

    analytics = []

    for account in accounts:
        logger.info(f"Fetching analytics for account '{account.name}'")
        for property in account.properties:
            logger.info(f"Fetching analytics for property '{property.name}'")

            report = Report(
                property_id=property.id,
                dimensions=[
                    Dimension({"name": dimension}) for dimension in dimensions
                ],
                metrics=[Metric({"name": metric}) for metric in metrics],
                date_ranges=[date_range],
                order_bys=[
                    OrderBy(
                        {
                            "dimension": {"dimension_name": "date"},
                            "desc": False,
                        }
                    )
                ],
            )
            request = RunReportRequest(report.as_dict())

            analytics_queue = run_report(request)
            analytics.append(analytics_queue)

            logger.info(
                f"Fetched {len(analytics_queue)} rows for property '{property.name}'"
            )

    logger.info(f"Fetched {sum(len(a) for a in analytics)} rows in total")

    try:
        logger.info(f"Saving analytics to '{analytics_pickle}'")
        with open(analytics_pickle, "wb") as f:
            pickle.dump(analytics, f)
    except Exception as exc:
        logger.error(f"Failed to save analytics to '{analytics_pickle}': {exc}")
        if os.path.exists(analytics_pickle):
            os.remove(analytics_pickle)
        sys.exit(1)

    return analytics
