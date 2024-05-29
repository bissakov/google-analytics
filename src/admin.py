import os
from typing import Optional

from google.analytics.admin import AnalyticsAdminServiceClient


def list_accounts(transport: Optional[str] = None):
    client = AnalyticsAdminServiceClient(transport=transport)

    results = client.list_accounts()

    print("Result:")
    for account in results:
        print(account)


def main():
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    secret_dir = os.path.join(project_dir, "secret")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(
        secret_dir, "admin_credentials.json"
    )

    list_accounts()


if __name__ == "__main__":
    main()
