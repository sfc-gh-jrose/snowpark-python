#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from functools import partial
from typing import Any, Callable, Dict, List, Literal, Optional, Union

import snowflake.snowpark
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.types import StructType
from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted
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
        return self

    def schema(self, schema: Union[StructType, str]) -> "DataStreamReader":
        self._user_schema = schema
        return self

    def option(self, key: str, value: Any) -> "DataStreamReader":
        self._options[key] = value
        return self

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
            "kafka": partial(
                self.kafka, **self._with_parameters_converted(self._options)
            )
        }.get(self._format)
        if loader is None:
            raise ValueError(f"Invalid format '{self._format}'.")

        return loader()

    @classmethod
    def _with_parameters_converted(self, params):
        return {k.replace(".", "_").strip().lower(): v for k, v in params.items()}

    def kafka(
        self,
        table_name,
        pipe_name,
        key_path,
        subscribe=None,
        kafka_bootstrap_servers=None,
        kafka_group_id=None,
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        account = unquote_if_quoted(self._session.get_current_account())
        database = unquote_if_quoted(self._session.get_current_database())
        schema = unquote_if_quoted(self._session.get_current_schema())
        user = unquote_if_quoted(self._session.get_current_user())

        pipe_plan = KafkaIngestNode(
            pipe_name,
            table_name,
            True,
            MatchByColumnNameMode.CASE_INSENSITIVE,
            account,
            key_path,
            subscribe,
            database,
            schema,
            user,
        )
        return DataFrame(self._session, pipe_plan)

    def table(self, tableName: str) -> "snowflake.snowpark.dataframe.DataFrame":
        raise NotImplementedError()
