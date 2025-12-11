"""
Tableau Operator for Airflow.

Provides operator for backing up Tableau workbooks to S3.
Automatically handles credentials using shared utilities.
"""

import logging
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Sequence

from airflow.models import BaseOperator
from tableauserverclient import (
    Filter,
    Pager,
    PersonalAccessTokenAuth,
    RequestOptions,
    Server,
    ServerResponseError,
    WorkbookItem,
)

from above.common.utils import copy_file_to_s3
from tableau.utils.tableau_utils import get_tableau_credentials

RETRY_DOWNLOAD_LIMIT: int = 3
RETRY_WAIT_SECS: int = 5

logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TableauOperator(BaseOperator):
    """
    Operator for backing up Tableau workbooks to S3.

    Connects to Tableau Server API, filters workbooks by update time,
    and uploads them to S3. Credentials are automatically retrieved from
    Airflow Variables unless explicitly provided.

    Args:
        updated_since: Workbooks updated after this time will be included.
            Time is in UTC in the format '%Y-%m-%dT%H:%M:%SZ'.
        site_id: Site ID used for API Auth
        server_url: Tableau Server URL used for API Auth
        s3_bucket: Name of the S3 bucket to back up Tableau into
        s3_directory: Directory in S3 bucket to save workbooks to
        s3_conn_id: S3 Connection ID (optional)
        token_name: Name of Tableau Personal Token (optional, auto-retrieved if not provided)
        personal_access_token: Token Secret (optional, auto-retrieved if not provided)

    Reference:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_filtering_and_sorting.htm
    """

    template_fields: Sequence[str] = ("updated_since", "s3_directory")

    def __init__(
        self,
        updated_since: str,
        site_id: str,
        server_url: str,
        s3_bucket: str,
        s3_directory: str,
        s3_conn_id: str | None = None,
        token_name: str | None = None,
        personal_access_token: str | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialize TableauOperator.

        If token_name and personal_access_token are not provided, they will be
        automatically retrieved from Airflow Variables using get_tableau_credentials().
        """
        super().__init__(*args, **kwargs)

        self.updated_since = updated_since
        self.s3_bucket: str = s3_bucket
        self.s3_directory: str = s3_directory
        self.s3_conn_id: str | None = s3_conn_id
        self.site_id = site_id
        self.server_url = server_url

        # Auto-retrieve credentials if not provided
        if token_name is None or personal_access_token is None:
            logger.info("Retrieving Tableau credentials from Airflow Variables")
            credentials = get_tableau_credentials()
            self.token_name = credentials["TOKEN_NAME"]
            self.personal_access_token = credentials["TOKEN_SECRET"]
        else:
            self.token_name = token_name
            self.personal_access_token = personal_access_token

        self.server: Server
        self.retry_download_count: int = 0

    def _connect(self) -> Server:
        tableau_auth: PersonalAccessTokenAuth = PersonalAccessTokenAuth(
            token_name=self.token_name,
            personal_access_token=self.personal_access_token,
            site_id=self.site_id,
        )
        server: Server = Server(self.server_url, use_server_version=True)
        server.auth.sign_in(tableau_auth)

        return server

    def _populate_workbook_with_views_and_connections(
        self, server: Server, workbook: WorkbookItem
    ) -> None:
        """
        Populate the Views and Connections for ``workbook``.

        The reference below talks about how to populate connections and views for each workbook --- the API doesn't do this automatically. The API wrapper does this in kind of a weird way:

        ``self.server`` is the main client for interacting with tableau.
        ``self.server.workbooks` is a collection of methods for workbooks.
        These two methods (``populate_views``` and ``populate_connections``) directly mutate the passed workbook. (See the code here.)

        This method (``_populate_workbook_with_views_and_connections``) populates the stub views and connections that are initially pulled from the API.

        Reference: https://tableau.github.io/server-client-python/docs/populate-connections-views.html

        :param workbook: Workbook to be populated.
        :type workbook: WorkbookItem
        """
        server.workbooks.populate_views(workbook_item=workbook)
        server.workbooks.populate_connections(workbook_item=workbook)

    def _create_request_options_filter(self) -> RequestOptions:
        """
        Create a ResquestOptions object to filter API Workbook results by ones updated recently.

        REF: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_filtering_and_sorting.htm

        :return: RequestOptions for the Tableau API.
        :rtype: RequestOptions
        """
        # Creates and adds a filter to the request options.
        recent_update_filter = Filter(
            field=RequestOptions.Field.UpdatedAt,
            operator=RequestOptions.Operator.GreaterThanOrEqual,
            value=self.updated_since,
        )
        request_opts = RequestOptions()
        request_opts.filter.add(recent_update_filter)
        return request_opts

    def _populate_workbook_views_and_connections(self, workbook: WorkbookItem) -> None:
        """
        Populate the Views and Connections for ``workbook``.

        Reference: https://tableau.github.io/server-client-python/docs/populate-connections-views.html

        :param workbook: Workbook to be populated.
        :type workbook: WorkbookItem
        """
        try:
            self.server.workbooks.populate_views(workbook_item=workbook)
            self.server.workbooks.populate_connections(workbook_item=workbook)
        except ServerResponseError as error:
            logger.error(error)

    def _download_workbook(self, workbook: WorkbookItem, dest: str) -> str:
        """Download workbook and return file path of downloaded workbook."""
        file_path: str

        try:
            file_path = self.server.workbooks.download(workbook.id, filepath=dest)

        # There has been an issue with 'content-disposition' not being in a header
        # and causing a break in the DAG runs.  The solution is usually "wait a few
        # seconds and retry".  This logic is implemented here.
        except KeyError as error:
            if "content-disposition" in error.args:
                self.retry_download_count += 1

                if self.retry_download_count >= RETRY_DOWNLOAD_LIMIT:
                    logger.error(f"Download retry limit exceeded for {workbook}.")
                    raise error

                logger.debug(error)
                logger.debug(
                    f"Retrying download... (Retry number {self.retry_download_count} of {RETRY_DOWNLOAD_LIMIT})"
                )

                time.sleep(RETRY_WAIT_SECS**self.retry_download_count)
                file_path = self._download_workbook(workbook=workbook, dest=dest)

            else:
                raise error

        self.retry_download_count = 0  # Reset value if workbook downloads correctly.
        return file_path

    def _upload_workbook_binary_to_s3(self, workbook: WorkbookItem) -> None:
        """
        Uploads workbook binary file to S3.

        :param workbook: Workbook to upload.
        :type workbook: WorkbookItem
        """
        with TemporaryDirectory() as dest:
            file_path: str = self._download_workbook(workbook=workbook, dest=dest)
            file_name: str = Path(file_path).name

            copy_file_to_s3(
                file_path=file_path,
                s3_conn_id=self.s3_conn_id,
                s3_bucket=self.s3_bucket,
                s3_key=f"{self.s3_directory}/{file_name}",
            )

    def _get_workbooks_from_pager(self, pager: Pager) -> list[WorkbookItem]:
        """Clean out non-workbooks from a Pager item."""

        # Eager load from pager to separate workbooks and non-workbooks initially.
        # None _should_ be non-workbooks, but check for safety.
        workbooks: list[WorkbookItem] = []
        non_workbooks: list[Any] = []

        # Separate out workbook items from non-workbook items.
        for item in pager:
            if isinstance(item, WorkbookItem):
                workbooks.append(item)
            else:
                non_workbooks.append(item)

        if len(non_workbooks) > 0:
            logger.error(
                f"{len(non_workbooks)} items `{non_workbooks}` are not a valid `WorkbookItem` items."
            )

        # List all workbooks in debug, for human reference.
        logger.debug(
            f"Found {len(workbooks)} workbooks:"
            + "\n".join(str(workbook) for workbook in workbooks)
        )

        return workbooks

    def execute(self, context: dict[str, Any]) -> None:
        self.server = self._connect()
        request_opts: RequestOptions = self._create_request_options_filter()

        # Gets workbooks with the filtered criteria, returns workbook pager (functionally a generator).
        # REF: https://tableau.github.io/server-client-python/docs/page-through-results
        pager: Pager = Pager(endpoint=self.server.workbooks, request_opts=request_opts)
        workbooks = self._get_workbooks_from_pager(pager=pager)

        for workbook in workbooks:
            self._populate_workbook_views_and_connections(workbook=workbook)
            self._upload_workbook_binary_to_s3(workbook=workbook)
