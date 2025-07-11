from typing import TYPE_CHECKING, Dict, List, Optional, Union, cast

from ..exception import ContributionsAcceptedError
from .types import StructType


from ..errors import PySparkNotImplementedError, PySparkTypeError

PrimitiveType = Union[bool, float, int, str]
OptionalPrimitiveType = Optional[PrimitiveType]

if TYPE_CHECKING:
    from duckdb.experimental.spark.sql.dataframe import DataFrame
    from duckdb.experimental.spark.sql.session import SparkSession


class OptionUtils:
    def __init__(self) -> None:
        self._options: Dict[str, PrimitiveType] = {}

    def option(self, key: str, value: PrimitiveType) -> "OptionUtils":
        self._options[key] = value
        return self

    def options(self, **options: PrimitiveType) -> "OptionUtils":
        self._options.update(options)
        return self
    
    def _set_opts(
        self,
        schema: Optional[Union[StructType, str]] = None,
        unsupported_options: Optional[List[str]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        """
        Set named options (filter out those the value is None)
        """
        if schema is not None:
            self.schema(schema)  # type: ignore[attr-defined]
        for k, v in options.items():
            if v is not None:
                if unsupported_options is not None and k in unsupported_options:
                    raise ContributionsAcceptedError(f"Option '{k}' is not yet supported.")
                self.option(k, v)  # type: ignore[attr-defined]


class DataFrameWriter:
    def __init__(self, dataframe: "DataFrame"):
        self.dataframe = dataframe

    def saveAsTable(self, table_name: str) -> None:
        relation = self.dataframe.relation
        relation.create(table_name)

    def parquet(
        self,
        path: str,
        mode: Optional[str] = None,
        partitionBy: Union[str, List[str], None] = None,
        compression: Optional[str] = None,
    ) -> None:
        relation = self.dataframe.relation
        if mode:
            raise NotImplementedError
        if partitionBy:
            raise NotImplementedError

        relation.write_parquet(path, compression=compression)

    def csv(
        self,
        path: str,
        mode: Optional[str] = None,
        compression: Optional[str] = None,
        sep: Optional[str] = None,
        quote: Optional[str] = None,
        escape: Optional[str] = None,
        header: Optional[Union[bool, str]] = None,
        nullValue: Optional[str] = None,
        escapeQuotes: Optional[Union[bool, str]] = None,
        quoteAll: Optional[Union[bool, str]] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = None,
        ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = None,
        charToEscapeQuoteEscaping: Optional[str] = None,
        encoding: Optional[str] = None,
        emptyValue: Optional[str] = None,
        lineSep: Optional[str] = None,
    ):
        if mode not in (None, "overwrite"):
            raise NotImplementedError
        if escapeQuotes:
            raise NotImplementedError
        if ignoreLeadingWhiteSpace:
            raise NotImplementedError
        if ignoreTrailingWhiteSpace:
            raise NotImplementedError
        if charToEscapeQuoteEscaping:
            raise NotImplementedError
        if emptyValue:
            raise NotImplementedError
        if lineSep:
            raise NotImplementedError
        relation = self.dataframe.relation
        relation.write_csv(
            path,
            sep=sep,
            na_rep=nullValue,
            quotechar=quote,
            compression=compression,
            escapechar=escape,
            header=header if isinstance(header, bool) else header == "True",
            encoding=encoding,
            quoting=quoteAll,
            date_format=dateFormat,
            timestamp_format=timestampFormat,
        )


class DataFrameReader(OptionUtils):
    _CSV_UNSUPPORTED_OPTIONS = [
            "comment",
            "inferSchema",
            "ignoreLeadingWhiteSpace",
            "ignoreTrailingWhiteSpace",
            "nanValue",
            "positiveInf",
            "negativeInf",
            "maxColumns",
            "maxCharsPerColumn",
            "maxMalformedLogPerPartition",
            "mode",
            "columnNameOfCorruptRecord",
            "multiLine",
            "charToEscapeQuoteEscaping",
            "samplingRatio",
            "enforceSchema",
            "emptyValue",
            "locale",
            "pathGlobFilter",
            "recursiveFileLookup",
            "modifiedBefore",
            "modifiedAfter",
            "unescapedQuoteHandling",
    ]

    def __init__(self, session: "SparkSession"):
        super().__init__()
        self.session = session

    def load(
        self,
        path: Optional[Union[str, List[str]]] = None,
        format: Optional[str] = None,
        schema: Optional[Union[StructType, str]] = None,
        **options: OptionalPrimitiveType,
    ) -> "DataFrame":
        from duckdb.experimental.spark.sql.dataframe import DataFrame

        if not isinstance(path, str):
            raise NotImplementedError
        
        if options:
            raise ContributionsAcceptedError

        rel = None
        if format:
            format = format.lower()
            if format == "csv" or format == "tsv":
                rel = self.session.conn.read_csv(path)
            elif format == "json":
                rel = self.session.conn.read_json(path)
            elif format == "parquet":
                rel = self.session.conn.read_parquet(path)
            else:
                raise ContributionsAcceptedError
        else:
            rel = self.session.conn.sql(f"select * from {path}")
        df = DataFrame(rel, self.session)
        if schema:
            if not isinstance(schema, StructType):
                raise ContributionsAcceptedError
            schema = cast(StructType, schema)
            types, names = schema.extract_types_and_names()
            df = df._cast_types(types)
            df = df.toDF(names)
        raise NotImplementedError

    def csv(
        self,
        path: Union[str, List[str]],
        schema: Optional[Union[StructType, str]] = None,
        sep: Optional[str] = None,
        encoding: Optional[str] = None,
        quote: Optional[str] = None,
        escape: Optional[str] = None,
        comment: Optional[str] = None,
        header: Optional[Union[bool, str]] = None,
        inferSchema: Optional[Union[bool, str]] = None,
        ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = None,
        ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = None,
        nullValue: Optional[str] = None,
        nanValue: Optional[str] = None,
        positiveInf: Optional[str] = None,
        negativeInf: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        maxColumns: Optional[Union[int, str]] = None,
        maxCharsPerColumn: Optional[Union[int, str]] = None,
        maxMalformedLogPerPartition: Optional[Union[int, str]] = None,
        mode: Optional[str] = None,
        columnNameOfCorruptRecord: Optional[str] = None,
        multiLine: Optional[Union[bool, str]] = None,
        charToEscapeQuoteEscaping: Optional[str] = None,
        samplingRatio: Optional[Union[float, str]] = None,
        enforceSchema: Optional[Union[bool, str]] = None,
        emptyValue: Optional[str] = None,
        locale: Optional[str] = None,
        lineSep: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        modifiedBefore: Optional[Union[bool, str]] = None,
        modifiedAfter: Optional[Union[bool, str]] = None,
        unescapedQuoteHandling: Optional[str] = None,
    ) -> "DataFrame":
        if not isinstance(path, str):
            raise NotImplementedError
        
        if schema and not isinstance(schema, StructType):
            raise ContributionsAcceptedError
        
        if lineSep:
            # We have support for custom newline, just needs to be ported to 'read_csv'
            raise NotImplementedError
        
        self._set_opts(
            schema=schema,
            unsupported_options=self._CSV_UNSUPPORTED_OPTIONS,
            sep=sep,
            encoding=encoding,
            quote=quote,
            escape=escape,
            comment=comment,
            header=header,
            inferSchema=inferSchema,
            ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace,
            nullValue=nullValue,
            nanValue=nanValue,
            positiveInf=positiveInf,
            negativeInf=negativeInf,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            maxColumns=maxColumns,
            maxCharsPerColumn=maxCharsPerColumn,
            maxMalformedLogPerPartition=maxMalformedLogPerPartition,
            mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord,
            multiLine=multiLine,
            charToEscapeQuoteEscaping=charToEscapeQuoteEscaping,
            samplingRatio=samplingRatio,
            enforceSchema=enforceSchema,
            emptyValue=emptyValue,
            locale=locale,
            lineSep=lineSep,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter,
            unescapedQuoteHandling=unescapedQuoteHandling,
        )

        dtype = None
        names = None
        if schema:
            schema = cast(StructType, schema)
            dtype, names = schema.extract_types_and_names()

        header = self._options.get("header")
        rel = self.session.conn.read_csv(
            path,
            header=header if isinstance(header, bool) else header == "True",
            sep=self._options.get("sep"),
            dtype=dtype,
            na_values=self._options.get("nullValue"),
            quotechar=self._options.get("quote"),
            escapechar=self._options.get("escape"),
            encoding=self._options.get("encoding"),
            date_format=self._options.get("dateFormat"),
            timestamp_format=self._options.get("timestampFormat"),
        )
        from ..sql.dataframe import DataFrame

        df = DataFrame(rel, self.session)
        if names:
            df = df.toDF(*names)
        return df

    def parquet(self, *paths: str, **options: "OptionalPrimitiveType") -> "DataFrame":
        input = list(paths)
        if len(input) != 1:
            raise NotImplementedError("Only single paths are supported for now")
        option_amount = len(options.keys())
        if option_amount != 0:
            raise ContributionsAcceptedError("Options are not supported")
        path = input[0]
        rel = self.session.conn.read_parquet(path)
        from ..sql.dataframe import DataFrame

        df = DataFrame(rel, self.session)
        return df

    def json(
        self,
        path: Union[str, List[str]],
        schema: Optional[Union[StructType, str]] = None,
        primitivesAsString: Optional[Union[bool, str]] = None,
        prefersDecimal: Optional[Union[bool, str]] = None,
        allowComments: Optional[Union[bool, str]] = None,
        allowUnquotedFieldNames: Optional[Union[bool, str]] = None,
        allowSingleQuotes: Optional[Union[bool, str]] = None,
        allowNumericLeadingZero: Optional[Union[bool, str]] = None,
        allowBackslashEscapingAnyCharacter: Optional[Union[bool, str]] = None,
        mode: Optional[str] = None,
        columnNameOfCorruptRecord: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        multiLine: Optional[Union[bool, str]] = None,
        allowUnquotedControlChars: Optional[Union[bool, str]] = None,
        lineSep: Optional[str] = None,
        samplingRatio: Optional[Union[float, str]] = None,
        dropFieldIfAllNull: Optional[Union[bool, str]] = None,
        encoding: Optional[str] = None,
        locale: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        modifiedBefore: Optional[Union[bool, str]] = None,
        modifiedAfter: Optional[Union[bool, str]] = None,
        allowNonNumericNumbers: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        """
        Loads JSON files and returns the results as a :class:`DataFrame`.

        `JSON Lines <http://jsonlines.org/>`_ (newline-delimited JSON) is supported by default.
        For JSON (one record per file), set the ``multiLine`` parameter to ``true``.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str, list or :class:`RDD`
            string represents path to the JSON dataset, or a list of paths,
            or RDD of Strings storing JSON objects.
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            an optional :class:`pyspark.sql.types.StructType` for the input schema or
            a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.json(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """

        if schema is not None:
            raise ContributionsAcceptedError("The 'schema' option is not supported")
        if primitivesAsString is not None:
            raise ContributionsAcceptedError(
                "The 'primitivesAsString' option is not supported"
            )
        if prefersDecimal is not None:
            raise ContributionsAcceptedError(
                "The 'prefersDecimal' option is not supported"
            )
        if allowComments is not None:
            raise ContributionsAcceptedError(
                "The 'allowComments' option is not supported"
            )
        if allowUnquotedFieldNames is not None:
            raise ContributionsAcceptedError(
                "The 'allowUnquotedFieldNames' option is not supported"
            )
        if allowSingleQuotes is not None:
            raise ContributionsAcceptedError(
                "The 'allowSingleQuotes' option is not supported"
            )
        if allowNumericLeadingZero is not None:
            raise ContributionsAcceptedError(
                "The 'allowNumericLeadingZero' option is not supported"
            )
        if allowBackslashEscapingAnyCharacter is not None:
            raise ContributionsAcceptedError(
                "The 'allowBackslashEscapingAnyCharacter' option is not supported"
            )
        if mode is not None:
            raise ContributionsAcceptedError("The 'mode' option is not supported")
        if columnNameOfCorruptRecord is not None:
            raise ContributionsAcceptedError(
                "The 'columnNameOfCorruptRecord' option is not supported"
            )
        if dateFormat is not None:
            raise ContributionsAcceptedError("The 'dateFormat' option is not supported")
        if timestampFormat is not None:
            raise ContributionsAcceptedError(
                "The 'timestampFormat' option is not supported"
            )
        if multiLine is not None:
            raise ContributionsAcceptedError("The 'multiLine' option is not supported")
        if allowUnquotedControlChars is not None:
            raise ContributionsAcceptedError(
                "The 'allowUnquotedControlChars' option is not supported"
            )
        if lineSep is not None:
            raise ContributionsAcceptedError("The 'lineSep' option is not supported")
        if samplingRatio is not None:
            raise ContributionsAcceptedError(
                "The 'samplingRatio' option is not supported"
            )
        if dropFieldIfAllNull is not None:
            raise ContributionsAcceptedError(
                "The 'dropFieldIfAllNull' option is not supported"
            )
        if encoding is not None:
            raise ContributionsAcceptedError("The 'encoding' option is not supported")
        if locale is not None:
            raise ContributionsAcceptedError("The 'locale' option is not supported")
        if pathGlobFilter is not None:
            raise ContributionsAcceptedError(
                "The 'pathGlobFilter' option is not supported"
            )
        if recursiveFileLookup is not None:
            raise ContributionsAcceptedError(
                "The 'recursiveFileLookup' option is not supported"
            )
        if modifiedBefore is not None:
            raise ContributionsAcceptedError(
                "The 'modifiedBefore' option is not supported"
            )
        if modifiedAfter is not None:
            raise ContributionsAcceptedError(
                "The 'modifiedAfter' option is not supported"
            )
        if allowNonNumericNumbers is not None:
            raise ContributionsAcceptedError(
                "The 'allowNonNumericNumbers' option is not supported"
            )

        if isinstance(path, str):
            path = [path]
        if  isinstance(path, list):
            if len(path) == 1:
                rel = self.session.conn.read_json(path[0])
                from .dataframe import DataFrame

                df = DataFrame(rel, self.session)
                return df
            raise PySparkNotImplementedError(
                message="Only a single path is supported for now"
            )
        else:
            raise PySparkTypeError(
                error_class="NOT_STR_OR_LIST_OF_RDD",
                message_parameters={
                    "arg_name": "path",
                    "arg_type": type(path).__name__,
                },
            )


__all__ = ["DataFrameWriter", "DataFrameReader"]
