#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from functools import partial
from typing import Any, Callable, Dict, List, Literal, Optional, Union

import snowflake.snowpark
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.types import StructType
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    KafkaIngestNode,
    MatchByColumnNameMode,
)


ALLOWED_FORMATS = ["kafka"]


class StreamingQueryException(Exception):
    pass


class StreamingQuery:
    """ """

    def __init__(self, job: Callable) -> None:
        self._job = job

    @property
    def id(self) -> str:
        raise NotImplementedError()

    @property
    def runId(self) -> str:
        raise NotImplementedError()

    @property
    def name(self) -> str:
        raise NotImplementedError()

    @property
    def isActive(self) -> bool:
        raise NotImplementedError()

    def awaitTermination(self, timeout: Optional[int] = None) -> Optional[bool]:
        raise NotImplementedError()

    @property
    def status(self) -> Dict[str, Any]:
        raise NotImplementedError()

    @property
    def recentProgress(self) -> List[Dict[str, Any]]:
        raise NotImplementedError()

    @property
    def lastProgress(self) -> Optional[Dict[str, Any]]:
        raise NotImplementedError()

    def processAllAvailable(self) -> None:
        raise NotImplementedError()

    def stop(self) -> None:
        raise NotImplementedError()

    def explain(self, extended: bool = False) -> None:
        raise NotImplementedError()

    def exception(self) -> Optional[StreamingQueryException]:
        raise NotImplementedError()


class DataStreamReader:
    """ """

    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self._session = session
        self._user_schema = None
        self._format = None
        self._options = {}

    def format(
        self,
        format: Literal["kafka"],
    ) -> "DataStreamReader":
        fmt = format.strip().lower()
        if fmt not in ALLOWED_FORMATS:
            raise ValueError(
                f"Invalid format '{format}'. Supported formats are {ALLOWED_FORMATS}."
            )
        self._format = fmt

    def schema(self, schema: Union[StructType, str]) -> "DataStreamReader":
        self._user_schema = schema
        return self

    def option(self, key: str, value: Any) -> "DataStreamReader":
        raise NotImplementedError()

    def options(self, **options: Any) -> "DataStreamReader":
        raise NotImplementedError()

    def load(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        schema: Optional[Union[StructType, str]] = None,
        **options: Any,
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        if self._format is None:
            raise ValueError(
                "Please specify the format of the file(s) to load using the format() method."
            )

        loader = {
            "kafka": partial(self._with_parameters_converted(self.kafka, self._options))
        }.get(self._format)
        if loader is None:
            raise ValueError(f"Invalid format '{self._format}'.")

        return loader()

    @classmethod
    def _with_parameters_converted(func, params):
        return func(**{k.replace(".", "_").strip().lower(): v for k, v in params})

    def kafka(
        self,
        table_name,
        pipe_name,
        subscribe=None,
        kafka_bootstrap_servers=None,
        kafka_group_id=None,
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        pipe_plan = KafkaIngestNode(
            pipe_name, table_name, True, MatchByColumnNameMode.CASE_INSENSITIVE
        )

        return DataFrame(self._session, pipe_plan)
        # import os
        #
        # props = {}
        #
        # with open(os.path.expanduser("~/.ssh/rsa_key.p8")) as key_file:
        #     key_data = key_file.read()
        #
        # props = {
        #     **CONNECTION_PARAMETERS,
        #     **{
        #         "ssl": "on",
        #         "url": "https://sfctest0.snowflakecomputing.com:443",
        #         "private_key": key_data,
        #         "port": 443,
        #         "host": "sfctest0.snowflakecomputing.com",
        #         "scheme": "https",
        #         "ROWSET_DEV_VM_TEST_MODE": "false",
        #     },
        # }
        #
        #
        # session.sql(
        #     f"""
        # CREATE OR REPLACE PIPE {pipe_name}
        # AS
        #   COPY INTO {raw_table_name}
        #   FROM TABLE(
        #       DATA_SOURCE(
        #           TYPE => 'STREAMING'
        #   )
        # )
        # MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        # """
        # ).collect()
        # return props

    def table(self, tableName: str) -> "snowflake.snowpark.dataframe.DataFrame":
        raise NotImplementedError()
