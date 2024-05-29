import dataclasses
import itertools
import os
from dataclasses import fields
from datetime import datetime, timedelta
from typing import Dict, MutableSequence, Union

import rich
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    OrderBy,
    RunReportRequest,
)
from rich.console import Console
from rich.table import Table


@dataclasses.dataclass
class Report:
    property: str
    dimensions: MutableSequence[Dimension]
    metrics: MutableSequence[Metric]
    date_ranges: MutableSequence[DateRange]
    order_bys: MutableSequence[OrderBy]
    keep_empty_rows: bool = True

    def as_dict(
        self,
    ) -> Dict[
        str,
        Union[
            str,
            MutableSequence[Dimension]
            | MutableSequence[Metric]
            | MutableSequence[DateRange]
            | MutableSequence[OrderBy]
            | bool,
        ],
    ]:
        return {field.name: getattr(self, field.name) for field in fields(self)}


def sample_run_report(report: Report) -> None:
    client = BetaAnalyticsDataClient()

    rich.print(report.as_dict())
    request = RunReportRequest(report.as_dict())
    response = client.run_report(request)

    table = Table(title="Report Data")

    for dimension in response.dimension_headers:
        table.add_column(dimension.name, justify="center", no_wrap=True)
    for metric in response.metric_headers:
        table.add_column(metric.name, justify="center", no_wrap=True)

    for row in response.rows:
        table.add_row(
            *itertools.chain(
                [x.value for x in row.dimension_values],
                [x.value for x in row.metric_values],
            )
        )

    console = Console()
    console.print(table)


def main() -> None:
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    secret_dir = os.path.join(project_dir, "secret")
    credentials_json = os.path.join(secret_dir, "service_credentials.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_json

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    dimensions = ["date", "city", "cityId", "country", "countryId"]
    metrics = [
        "sessions",
        "newUsers",
        "totalUsers",
        "bounceRate",
        "userEngagementDuration",
    ]

    report = Report(
        property="properties/400543665",
        dimensions=[Dimension({"name": dimension}) for dimension in dimensions],
        metrics=[Metric({"name": metric}) for metric in metrics],
        date_ranges=[
            DateRange({"start_date": "2024-01-01", "end_date": yesterday_str})
        ],
        order_bys=[OrderBy({"dimension": {"dimension_name": "date"}, "desc": False})],
    )
    sample_run_report(report)


if __name__ == "__main__":
    main()
