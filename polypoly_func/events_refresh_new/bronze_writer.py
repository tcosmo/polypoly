"""
Bronze layer writer for ADLS.
"""
import json
import logging
from datetime import datetime, timezone

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from events_refresh_new.config import (
    ADLS_ACCOUNT_NAME_BRONZE,
    BRONZE_CONTAINER,
    BRONZE_PATH_TEMPLATE,
)

logger = logging.getLogger(__name__)


class BronzeWriter:
    def __init__(self):
        self.credential = DefaultAzureCredential()
        self.datalake_client = DataLakeServiceClient(
            account_url=f"https://{ADLS_ACCOUNT_NAME_BRONZE}.dfs.core.windows.net",
            credential=self.credential,
        )
        logger.debug(f"Initialized BronzeWriter for account {ADLS_ACCOUNT_NAME_BRONZE}")

    def write_events(self, events: list[dict], offset: int, limit: int) -> str:
        """
        Write events to ADLS bronze layer as JSONL.

        Returns:
            Full ADLS path of written file
        """
        now = datetime.now(timezone.utc)
        ingest_date = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H%M%S")

        path = BRONZE_PATH_TEMPLATE.format(
            ingest_date=ingest_date,
            offset=offset,
            limit=limit,
            time=time_str,
        )

        logger.debug(f"Writing {len(events)} events to {path}")

        # Serialize to JSONL
        jsonl_content = "\n".join(json.dumps(event) for event in events)
        content_bytes = jsonl_content.encode("utf-8")

        # Upload to ADLS
        file_system_client = self.datalake_client.get_file_system_client(BRONZE_CONTAINER)
        file_client = file_system_client.get_file_client(path)
        file_client.upload_data(content_bytes, overwrite=True)

        full_path = f"abfss://{BRONZE_CONTAINER}@{ADLS_ACCOUNT_NAME_BRONZE}.dfs.core.windows.net/{path}"
        logger.debug(f"Wrote {len(content_bytes)} bytes to {full_path}")

        return full_path
