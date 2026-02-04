"""
Databricks cluster client for Delta table operations.

Uses the command execution API to run Spark operations on a cluster.
All Delta operations use direct paths (no metastore registration).
"""
import time
from datetime import timedelta
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language, State

from events_backfill.config import (
    DATABRICKS_HOST,
    DATABRICKS_TOKEN,
    DATABRICKS_CLUSTER_ID,
    ADLS_ACCOUNT_NAME,
)

# ADLS setup code to run on cluster via Python context
ADLS_SETUP_CODE = f'''
storage_account = "{ADLS_ACCOUNT_NAME}"

client_id = dbutils.secrets.get(scope="keyv-polybot", key="sp-client-id")
client_secret = dbutils.secrets.get(scope="keyv-polybot", key="sp-client-secret")
tenant_id = dbutils.secrets.get(scope="keyv-polybot", key="tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{{storage_account}}.dfs.core.windows.net", "OAuth")
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{{storage_account}}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(f"fs.azure.account.oauth2.client.id.{{storage_account}}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{{storage_account}}.dfs.core.windows.net", client_secret)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{{storage_account}}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{{tenant_id}}/oauth2/token",
)
print("ADLS credentials configured")
'''


class DeltaClient:
    """Execute Delta operations on a Databricks cluster using direct paths."""

    def __init__(self, cluster_id: Optional[str] = None):
        self.client = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
        self.cluster_id = cluster_id or DATABRICKS_CLUSTER_ID
        self._python_context_id: Optional[str] = None
        self._adls_configured: bool = False

        if not self.cluster_id:
            self.cluster_id = self._find_cluster()

    def _find_cluster(self) -> str:
        """Find first available cluster."""
        clusters = list(self.client.clusters.list())
        if not clusters:
            raise RuntimeError("No clusters found. Create one or set DATABRICKS_CLUSTER_ID.")

        for c in clusters:
            if c.state == State.RUNNING:
                print(f"Using cluster: {c.cluster_name} ({c.cluster_id})")
                return c.cluster_id

        c = clusters[0]
        print(f"Using cluster: {c.cluster_name} ({c.cluster_id}) - may need to start")
        return c.cluster_id

    def ensure_cluster_running(self):
        """Start cluster if not running."""
        cluster = self.client.clusters.get(self.cluster_id)
        if cluster.state == State.TERMINATED:
            print(f"Starting cluster {self.cluster_id}...")
            self.client.clusters.start(self.cluster_id)

        if cluster.state not in (State.RUNNING,):
            print(f"Waiting for cluster to start (state: {cluster.state})...")
            while True:
                cluster = self.client.clusters.get(self.cluster_id)
                if cluster.state == State.RUNNING:
                    break
                if cluster.state in (State.ERROR, State.TERMINATED):
                    raise RuntimeError(f"Cluster failed to start: {cluster.state}")
                time.sleep(10)
            print("Cluster is running.")

    def _get_context(self) -> str:
        """Get or create a Python execution context with ADLS configured."""
        if self._python_context_id:
            return self._python_context_id

        self.ensure_cluster_running()

        wait = self.client.command_execution.create(
            cluster_id=self.cluster_id,
            language=Language.PYTHON,
        )
        if hasattr(wait, 'result'):
            response = wait.result(timeout=timedelta(minutes=5))
            self._python_context_id = response.id
        elif hasattr(wait, 'id'):
            self._python_context_id = wait.id
        else:
            raise RuntimeError(f"Unexpected response from create: {wait}")

        if not self._adls_configured:
            self._run_python(ADLS_SETUP_CODE)
            self._adls_configured = True
            print("ADLS credentials configured on cluster")

        return self._python_context_id

    def _run_python(self, code: str, timeout_seconds: int = 300) -> str:
        """Execute Python code and return printed output."""
        context_id = self._python_context_id or self._get_context()

        wait = self.client.command_execution.execute(
            cluster_id=self.cluster_id,
            context_id=context_id,
            language=Language.PYTHON,
            command=code,
        )

        if hasattr(wait, 'result'):
            result = wait.result(timeout=timedelta(seconds=timeout_seconds))
            if result.status and result.status.value == "Error":
                error_msg = result.results.cause if result.results else "Unknown error"
                raise RuntimeError(f"Python execution failed: {error_msg}")
            if result.results and result.results.data:
                return str(result.results.data)
            return ""
        else:
            command_id = wait.id
            start_time = time.time()

            while True:
                status = self.client.command_execution.command_status(
                    cluster_id=self.cluster_id,
                    context_id=context_id,
                    command_id=command_id,
                )
                if status.status.value in ("Finished", "Cancelled", "Error"):
                    break
                if time.time() - start_time > timeout_seconds:
                    raise TimeoutError(f"Execution timed out after {timeout_seconds}s")
                time.sleep(1)

            if status.status.value == "Error":
                error_msg = status.results.cause if status.results else "Unknown error"
                raise RuntimeError(f"Python execution failed: {error_msg}")

            if status.results and status.results.data:
                return str(status.results.data)
            return ""

    def run_delta_operation(self, code: str, timeout_seconds: int = 300) -> str:
        """Run a Delta operation (Python code) on the cluster."""
        self._get_context()
        return self._run_python(code, timeout_seconds)

    def close(self):
        """Destroy the execution context."""
        if self._python_context_id:
            try:
                self.client.command_execution.destroy(
                    cluster_id=self.cluster_id,
                    context_id=self._python_context_id,
                )
            except Exception:
                pass
            self._python_context_id = None
            self._adls_configured = False


# Singleton for reuse
_client: Optional[DeltaClient] = None


def get_delta_client() -> DeltaClient:
    """Get shared Delta client instance."""
    global _client
    if _client is None:
        _client = DeltaClient()
    return _client
