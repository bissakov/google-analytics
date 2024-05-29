import dataclasses
import os
from datetime import datetime, timedelta
from typing import Dict, MutableSequence, Union

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    OrderBy,
    RunReportRequest,
)

MutableSequenceType = (
    MutableSequence[Dimension]
    | MutableSequence[Metric]
    | MutableSequence[DateRange]
    | MutableSequence[OrderBy]
)


@dataclasses.dataclass
class Report:
    property: str
    dimensions: MutableSequence[Dimension]
    metrics: MutableSequence[Metric]
    date_ranges: MutableSequence[DateRange]
    order_bys: MutableSequence[OrderBy]

    def as_dict(
        self,
    ) -> Dict[
        str,
        Union[str, MutableSequenceType],
    ]:
        return {
            "property": self.property,
            "dimensions": self.dimensions,
            "metrics": self.metrics,
            "date_ranges": self.date_ranges,
            "order_bys": self.order_bys,
        }


def sample_run_report(report: Report) -> None:
    client = BetaAnalyticsDataClient()

    request = RunReportRequest(report.as_dict())
    response = client.run_report(request)

    print("Report result:")
    for row in response.rows:
        print(row)


def main() -> None:
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    secret_dir = os.path.join(project_dir, "secret")
    credentials_json = os.path.join(secret_dir, "service_credentials.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_json

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    print(f"Yesterday: {yesterday_str}")

    report = Report(
        property="properties/400543665",
        dimensions=[
            Dimension({"name": "date"}),
            Dimension({"name": "city"}),
            Dimension({"name": "cityId"}),
            Dimension({"name": "country"}),
            Dimension({"name": "countryId"}),
        ],
        metrics=[
            Metric({"name": "sessions"}),
            Metric({"name": "newUsers"}),
            Metric({"name": "totalUsers"}),
            Metric({"name": "bounceRate"}),
            Metric({"name": "userEngagementDuration"}),
        ],
        date_ranges=[
            DateRange({"start_date": "2024-01-01", "end_date": yesterday_str})
        ],
        order_bys=[OrderBy({"dimension": {"dimension_name": "date"}, "desc": False})],
    )
    sample_run_report(report)


if __name__ == "__main__":
    main()
