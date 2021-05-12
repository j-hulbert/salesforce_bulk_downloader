"""
Salesforce bulk downloader that outputs the results as gzipped json lines file

Provides the ability to chunk up the results using Salesforce pk chunking
and chunking via pagination on a field like LastModifiedDate
"""

import logging
import json
import gzip
import os
import argparse
from time import sleep
from datetime import datetime, timezone, timedelta
from salesforce_bulk import SalesforceBulk
from simple_salesforce import Salesforce
from salesforce_bulk.util import IteratorBytesIO
from typing import List
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class SalesforceBatchStatus:
    batch_id: str
    job_id: str
    is_done: bool = False


class SalesforceDownload:
    def __init__(self, username, password, token):
        self.bulk = SalesforceBulk(
            username=username, password=password, security_token=token
        )
        self.sf = Salesforce(username=username, password=password, security_token=token)

    def get_column_names(self, salesforce_object_name: str) -> List[str]:
        """Get the column names for salesforce object

        Excludes columns that can't be returned by bulk API (compound and others)

        Args:
            salesforce_object_name (str): salesforce object name i.e. Account

        Returns:
            List[str]: List of column names
        """
        desc = self.sf.__getattr__(salesforce_object_name).describe()

        # These columns can't be extracted from bulk API but still gets returned by column list api
        # Doesn't seem to be an indication on the describe API but Bulk Download fails if these are included
        exclude_columns = [
            "ActivityMetricRollupId",
            "ActivityMetricId",
            "PhotoUrl",
            "Jigsaw",
            "JigsawContactId",
            "IndividualId",
        ]

        # compound fields can't be returned by the bulk API
        compound_fields = {
            field["compoundFieldName"]
            for field in desc["fields"]
            if field["compoundFieldName"] is not None
        }
        return [
            field["name"]
            for field in desc["fields"]
            if field["name"] not in compound_fields
            and field["name"] not in exclude_columns
        ]

    @staticmethod
    def paginate_statement(
        start_date_str: str,
        end_date_str: str,
        max_days: int = 30,
        offset_hours: int = 0,
        field_to_paginate: str = "LastModifiedDate",
    ) -> List[str]:
        """Generates paginated between statements for use in Salesforce API calls

        Given a start and end date, generates start/end increments plus required formatting for use in SF API

        Args:
            start_date_str (str): Date with no timestamp - 2021-01-10
            end_date_str (str): Date with no timestamp - 2021-02-15
            max_days (int, optional): Number of days for each pagination window. Defaults to 30.
            offset_hours (int, optional): Adjusts start date and end date this many hours. Defaults to 0.
            field_to_paginate (str, optional): Column to paginate on. Defaults to 'LastModifiedDate'.

        Returns:
            list(str): paginate between statement that can be used in call to SNOW API
        """
        end_of_day = timedelta(hours=23, minutes=59, seconds=59)
        sf_timestamp_format = "%Y-%m-%dT%H:%M:%SZ"
        offset = timedelta(hours=offset_hours)
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d") + offset
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d") + offset
        curr_date = start_date
        output = []
        while curr_date < end_date + end_of_day:
            end_of_curr = (
                min(curr_date + timedelta(days=max_days - 1), end_date) + end_of_day
            )
            btwn_str = (
                f"{field_to_paginate} >= {curr_date.strftime(sf_timestamp_format)}"
                f" AND {field_to_paginate} <= {end_of_curr.strftime(sf_timestamp_format)}"
            )
            output.append(btwn_str)
            curr_date = min(
                curr_date + timedelta(days=max_days - 1), end_date
            ) + timedelta(days=1)
        return output

    def get_table_incremental(
        self,
        table_name: str,
        start_date_str: str,
        end_date_str: str,
        filepath,
        field_to_paginate: str = "LastModifiedDate",
        offset_hours: int = 0,
        batch_max_days: int = 30,
        pk_chunking: bool = True,
    ):
        """Get all columns from table for a given time range and writes to json lines (jsonl.gz)

        Paginates the time range to chunk up the results
        All results are written to jsonl.gz file (merge of all chunks)

        Args:
            table_name (str): salesforce object/table name
            start_date_str (str): start date in format YYYY-MM-DD ("2021-01-01")
            end_date_str (str): end date in format YYYY-MM-DD ("2021-01-05")
            filepath ([type]): filepath to save results

        Returns:
            filepath: returns filepath jsonl.gz results are saved
        """
        query_filters = self.paginate_statement(
            start_date_str=start_date_str,
            end_date_str=end_date_str,
            field_to_paginate=field_to_paginate,
            offset_hours=offset_hours,
            max_days=batch_max_days,
        )
        try:
            for filter in query_filters:
                log.info(f"Processing query filter: {filter}")
                batch_list = self.execute_query_job(
                    table_name=table_name, query_filter=filter, pk_chunking=pk_chunking
                )
                self.write_batch_results(batch_list=batch_list, filepath=filepath)
        except Exception:
            if os.path.exists(filepath):
                log.error("Error processing at least one set, removing partial file")
                os.remove(filepath)
            raise

        return filepath

    def get_table_full(self, table_name: str, filepath, pk_chunking: bool = True):
        """Get all columns from table writes to jsonl.gz

        Args:
            table_name (str): salesforce object/table name
            filepath ([type]): filepath to save results

        Returns:
            filepath: returns filepath jsonl.gz results are saved
        """
        batch_list = self.execute_query_job(
            table_name=table_name, pk_chunking=pk_chunking
        )
        self.write_batch_results(batch_list=batch_list, filepath=filepath)
        return filepath

    def check_pk_chunking_success(
        self, batch: SalesforceBatchStatus
    ) -> SalesforceBatchStatus:
        """Check that batch has reached NOT_STARTED status, for use with batch started via pk_chunking

        When pk_chunking is enabled the first batch is set to NOT_STARTED when chunking is ready
        then the other batches are processed. This checks that the first batch is ready (NOT_STARTED state)

        Args:
            batch_id: batch id
            job_id: job id

        Raises:
            Exception: If the batch fails to reach NOT_STARTED state
            Exception: If the batch time exceeds the timeout
        """
        BATCH_TIMEOUT_SECS = 6000
        SLEEP_SECS = 15
        batch_start = datetime.now(timezone.utc)
        wait_until = batch_start + timedelta(seconds=BATCH_TIMEOUT_SECS)
        log.info("Checking pk chunking batch status")
        while True:
            status = self.bulk.batch_state(
                batch_id=batch.batch_id, job_id=batch.job_id, reload=True
            )
            if status == "NotProcessed":
                log.info("PK chunking batch is ready")
                batch.is_done = True
                return batch
            elif status != "Queued":
                raise Exception(f"Error chunking results, first batch status {status}")

            if datetime.now(timezone.utc) >= wait_until:
                raise Exception(
                    f"Runtime for job exceeds job timeout, batch status {status}"
                )

            sleep(SLEEP_SECS)

    def check_all_batches_complete(
        self, batch_list: List[SalesforceBatchStatus]
    ) -> List[SalesforceBatchStatus]:
        """Checks for the list of batches that they are all completed processing

        If timeout window has elapsed and all batches are not completed then will raise exception

        Args:
            batch_list (list of batches): list of batches to check status (batch_id and job_id required)

        Raises:
            Exception: exception if timeout exceeded
        """
        JOB_TIMEOUT_SECS = 1800
        SLEEP_SECS = 30
        job_start = datetime.now(timezone.utc)
        wait_until = job_start + timedelta(seconds=JOB_TIMEOUT_SECS)
        log.info("Checking all batches completed")
        while True:
            # check if batch is done for all tbe batches
            # is_batch_done raises an exception if a batch hits an error state
            for batch in batch_list:
                if not batch.is_done:
                    is_done = self.bulk.is_batch_done(
                        batch_id=batch.batch_id, job_id=batch.job_id
                    )
                    batch.is_done = is_done

            if all(batch_check.is_done for batch_check in batch_list):
                log.info(f"All batches completed, batch_status: {batch_list}")
                break

            if datetime.now(timezone.utc) >= wait_until:
                log.error(
                    f"Batches not completed within timeout, batch status: {batch_list}"
                )
                raise Exception("Runtime for job exceeds job timeout")
            sleep(SLEEP_SECS)
        return batch_list

    def execute_query_job(
        self, table_name: str, query_filter: str = None, pk_chunking: bool = True
    ) -> List[SalesforceBatchStatus]:
        """Execute a salesforce batch query job for the given table name and query filter

        After generating the job this will call function to check that job has completed
        and will return when that is completed

        Args:
            table_name (str): salesforce object/table name
            query_filter (str, optional): Query filter to appear after WHERE clause. Usually this should paginate a field like LastModifiedDate
                Defaults to None and None will query all rows of table
            pk_chunking (bool): breaks up table by pk into chunks, and executes as multiple batches

        Returns:
            batch_list : list of one or more batches completed batches generated by query
        """
        column_names = self.get_column_names(table_name)

        if query_filter:
            query = f"select {', '.join(column_names)} from {table_name} where {query_filter}"
        else:
            query = f"select {', '.join(column_names)} from {table_name}"

        # Create the job
        job = self.bulk.create_query_job(
            table_name, contentType="JSON", pk_chunking=pk_chunking
        )
        batch_original = self.bulk.query(
            job,
            query,
        )
        self.bulk.close_job(job)

        # when pk_chunking=True, the original batch gets set to NOT_STARTED when chunking is complete
        # then can check the other batches that get created
        if pk_chunking:
            self.check_pk_chunking_success(
                batch=SalesforceBatchStatus(batch_id=batch_original, job_id=job)
            )
        # Get the batches that salesforce creates (could be more than 1)
        batch_list = self.bulk.get_batch_list(job_id=job)
        log.info(f"Batches launched: {batch_list}")
        # when pk_chunking=True, the original batch gets set to NOT_STARTED and shouldn't be included
        batch_processing_list = [
            SalesforceBatchStatus(
                batch_id=batch["id"], job_id=batch["jobId"]
            )
            for batch in batch_list
            if batch["id"] != batch_original or not pk_chunking
        ]

        return self.check_all_batches_complete(batch_processing_list)

    def write_batch_results(self, batch_list: List[SalesforceBatchStatus], filepath):
        """Write the results of a list of batches to a gzip jsonl file

        Args:
            batch_list (list of batches): list of batches with job_id and batch_id
            filepath: filepath for jsonl.gz file
        """
        log.info(f"Writing results to file: {filepath}")
        with gzip.open(filepath, "at") as f:
            for batch in batch_list:
                for result in self.bulk.get_all_results_for_query_batch(
                    batch_id=batch.batch_id, job_id=batch.job_id
                ):
                    result = json.load(IteratorBytesIO(result))
                    log.info(f"Recorcds in batch {len(result)}")
                    for record in result:
                        # don't need attributes (table name and url, so that result is a flat json)
                        del record["attributes"]
                        json_record = json.dumps(record, ensure_ascii=False)
                        f.write(json_record + "\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--table_name", required=True, help="Table to extract (e.g Contact)"
    )
    parser.add_argument(
        "--start_date", default=None, help="Start Date (e.g. 2019-12-25)"
    )
    parser.add_argument("--end_date", default=None, help="End Date (e.g. 2019-12-25)")
    parser.add_argument(
        "--paginate_field", default="LastModifiedDate", help="Field to paginate on"
    )
    parser.add_argument(
        "--filepath",
        default="output.jsonl.gz",
        help="Output filepath (jsonl.gz format)",
    )
    parser.add_argument(
        "--no_pk_chunking",
        action="store_true",
        help="Don't use pk chunking (not supported by some tables like ContactHistory",
    )
    parser.add_argument(
        "--sf_username", required=True, help="Salesforce username"
    )
    parser.add_argument(
        "--sf_password", required=True, help="Salesforce password"
    )
    parser.add_argument(
        "--sf_security_token", required=True, help="Salesforce security token"
    )
    
    args = parser.parse_args()
    table_name = args.table_name
    start_date = args.start_date
    end_date = args.end_date
    paginate_field = args.paginate_field
    filepath = args.filepath
    pk_chunking = not args.no_pk_chunking
    sf_user = args.sf_username
    sf_password = args.sf_password
    sf_token = args.sf_security_token

    if (start_date and not end_date) or (not start_date and end_date):
        parser.error("If dates given, must supply both --start_date and --end_date")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = f"{table_name}_{timestamp}.jsonl.gz"

    sf = SalesforceDownload(username=sf_user, password=sf_password, token=sf_token)

    if start_date and end_date and paginate_field:
        sf.get_table_incremental(
            table_name=table_name,
            start_date_str=start_date,
            end_date_str=end_date,
            filepath=filepath,
            field_to_paginate=paginate_field,
            pk_chunking=pk_chunking,
        )
    else:
        sf.get_table_full(
            table_name=table_name, filepath=filepath, pk_chunking=pk_chunking
        )
