import logging
import os
from datetime import datetime, timedelta

from google.analytics.data_v1beta.types import DateRange

from src.account_properties import fetch_account_and_properties
from src.analytics import fetch_analytics
from src.error import handle_global_exception

logger = logging.getLogger(__name__)


def date_range_str(self: DateRange) -> str:
    start_date = getattr(self, "start_date", None)
    end_date = getattr(self, "end_date", None)
    return f"DateRange(start_date={start_date}, end_date={end_date})"


# @handle_global_exception
def main() -> None:
    today = datetime.today()
    logger.info(f"Starting the process for '{today.strftime("%Y-%m-%d")}'")

    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    os.environ["YESTERDAY"] = yesterday_str

    month_year = yesterday.strftime("%Y/%B")

    DateRange.__str__ = date_range_str
    DateRange.__repr__ = DateRange.__str__
    date_range = DateRange(
        {
            # "start_date": "2023-01-01",
            "start_date": yesterday_str,
            "end_date": yesterday_str,
        }
    )
    logger.info(f"Fetching Google Analytics for {date_range}")

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    pickle_dir = os.path.join(project_dir, "pickle", month_year)
    os.makedirs(pickle_dir, exist_ok=True)
    os.environ["PICKLE_DIR"] = pickle_dir

    secret_dir = os.path.join(project_dir, "secret")
    if not os.path.exists(secret_dir):
        logger.error(f"Missing secret directory: '{secret_dir}'")
        raise FileNotFoundError(f"Missing secret directory: '{secret_dir}'")

    admin_credentials = os.path.join(secret_dir, "admin_credentials.json")
    if not os.path.exists(admin_credentials):
        logger.error(
            f"Missing admin credentials JSON file: '{admin_credentials}'"
        )
        raise FileNotFoundError(
            f"Missing admin credentials JSON file: '{admin_credentials}'"
        )

    accounts = fetch_account_and_properties(admin_credentials)

    service_credentials = os.path.join(secret_dir, "service_credentials.json")
    if not os.path.exists(service_credentials):
        logger.error(
            f"Missing service credentials JSON file: '{service_credentials}'"
        )
        raise FileNotFoundError(
            f"Missing service credentials JSON file: '{service_credentials}'"
        )

    analytics = fetch_analytics(
        service_credentials,
        accounts,
        date_range,
    )

    # for analytics_queue in analytics:
    #     for row in analytics_queue:
    #         logger.info(row)


if __name__ == "__main__":
    main()
