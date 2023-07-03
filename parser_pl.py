import polars as pl
from configparser import ConfigParser
import pandas as pd
import datetime
import pytz


class ParserPl:
    def __init__(self, file_path: str, config: ConfigParser) -> None:
        self.store_status: pl.LazyFrame = pl.scan_csv(file_path)
        time_zone: pl.DataFrame = pl.read_csv(config["ASSETS"]["timezone"])
        """Converts the timezone string to utc offsets which are later used to convert local time to utc time
        """
        self.updated_time_zone = time_zone.select(
            pl.col("store_id"),
            pl.col("timezone_str").apply(
                lambda x: pytz.timezone(x)
                .localize(datetime.datetime(2023, 1, 21))
                .utcoffset()  # control date
            ),
        )
        self.open_hours_df: pl.LazyFrame = pl.scan_csv(config["ASSETS"]["open_hours"])
        self.config: ConfigParser = config

    def setup_data(self):
        joined_frame: pl.LazyFrame = self._join_frames()
        no_null_frame: pl.LazyFrame = self._preprocess_frame(joined_frame=joined_frame)
        ready_frame: pl.LazyFrame = self._convert_timezone_v2(
            no_null_frame=no_null_frame
        )
        self._filter_bussiness_hours_and_save_to_csv(ready_frame=ready_frame)
        print("in calc directory")

    def _join_frames(self) -> pl.LazyFrame:
        """left joins the scanned csvs on store status.csv

        Returns:
            pl.LazyFrame: returns the output of the joining operation
        """
        joined_frame: pl.LazyFrame = (
            self.store_status.lazy()
            .join(
                self.open_hours_df.lazy(),
                how="left",
                left_on="store_id",
                right_on="store_id",
            )
            .lazy()
            .join(
                self.updated_time_zone.lazy(),
                how="left",
                left_on="store_id",
                right_on="store_id",
            )
        )
        return joined_frame

    def _preprocess_frame(self, joined_frame: pl.LazyFrame) -> pl.LazyFrame:
        """Replaces all null values with the given default values

        Args:
            joined_frame (pl.LazyFrame): takes the frame after the joining operation

        Returns:
            pl.LazyFrame: returns a frame after completion of operations
        """
        no_null_final: pl.LazyFrame = (
            joined_frame.lazy()
            .with_columns(
                [
                    pl.col("day").fill_null(6),
                    pl.col("start_time_local")
                    .fill_null("00:00:00")
                    .str.strptime(pl.Datetime, format="%H:%M:%S"),
                    pl.col("end_time_local")
                    .fill_null("23:59:59")
                    .str.strptime(pl.Datetime, format="%H:%M:%S"),
                    pl.col("timezone_str").fill_null(datetime.timedelta(hours=-6)),
                    pl.col("timestamp_utc")
                    .str.replace(" UTC", "")
                    .str.strptime(pl.Datetime, format="%F %H:%M:%S%.6f")
                    .dt.replace_time_zone("UTC"),
                ]
            )
            .lazy()
            .filter(
                (pl.col("status").is_not_null())
                & (pl.col("timestamp_utc").is_not_null())
            )  # Conversion of timezone from local to UTC depends on the month due to DST
            # therefore dates from the columns next to start_time_local and end_time_local are
            # added to start_time_local and end_time_local
            .lazy()
            .select(
                [
                    pl.col("*"),
                    (
                        pl.col("start_time_local").dt.year()
                        + pl.col("timestamp_utc").dt.year()
                        - 1
                    ).alias("year_start"),
                    (
                        pl.col("start_time_local").dt.month()
                        + pl.col("timestamp_utc").dt.month()
                        - 1
                    ).alias("month_start"),
                    (
                        pl.col("start_time_local").dt.day()
                        + pl.col("timestamp_utc").dt.day()
                        - 1
                    ).alias("day_start"),
                    (
                        pl.col("end_time_local").dt.year()
                        + pl.col("timestamp_utc").dt.year()
                        - 1
                    ).alias("year_end"),
                    (
                        pl.col("end_time_local").dt.month()
                        + pl.col("timestamp_utc").dt.month()
                        - 1
                    ).alias("month_end"),
                    (
                        pl.col("end_time_local").dt.day()
                        + pl.col("timestamp_utc").dt.day()
                        - 1
                    ).alias("day_end"),
                ]
            )
            .lazy()
            .select(
                [
                    pl.col(["store_id", "status", "timestamp_utc", "day"]),
                    pl.datetime(
                        "year_start",
                        "month_start",
                        "day_start",
                        pl.col("start_time_local").dt.strftime(format="%H"),
                        pl.col("start_time_local").dt.strftime(format="%M"),
                        pl.col("start_time_local").dt.strftime(format="%S"),
                    )
                    .dt.strftime("%Y-%m-%d %H:%M:%S")
                    .str.strptime(pl.Datetime, format="%F %H:%M:%S")
                    .alias("start_time_local"),
                    pl.datetime(
                        "year_end",
                        "month_end",
                        "day_end",
                        pl.col("end_time_local").dt.strftime(format="%H"),
                        pl.col("end_time_local").dt.strftime(format="%M"),
                        pl.col("end_time_local").dt.strftime(format="%S"),
                    )
                    .dt.strftime("%Y-%m-%d %H:%M:%S")
                    .str.strptime(pl.Datetime, format="%F %H:%M:%S")
                    .alias("end_time_local"),
                    pl.col("timezone_str"),
                ]
            )
        )
        return no_null_final

    # BOTTLENECK
    # def _convert_timezone(self, no_null_frame: pl.LazyFrame) -> pl.LazyFrame:
    #     pandas_df = no_null_frame.lazy().select(pl.col("*")).collect().to_pandas()
    #     pandas_df["start_time_local"] = pd.to_datetime(pandas_df["start_time_local"])
    #     pandas_df["end_time_local"] = pd.to_datetime(pandas_df["end_time_local"])
    #     temp_df = (
    #         pandas_df.groupby("timezone_str")["start_time_local"]
    #         .apply(lambda x: x.dt.tz_localize(x.name).dt.tz_convert("UTC"))
    #         .reset_index()
    #     )
    #     temp_df["end_time_local"] = (
    #         pandas_df.groupby("timezone_str")["end_time_local"]
    #         .apply(lambda x: x.dt.tz_localize(x.name).dt.tz_convert("UTC"))
    #         .reset_index()["end_time_local"]
    #     )
    #     new_df = pandas_df.merge(
    #         temp_df, how="left", left_index=True, right_on="level_1"
    #     )
    #     back_to_polar = pl.from_pandas(new_df).lazy()
    #     ready_frame = (
    #         back_to_polar.lazy()
    #         .drop(
    #             ["start_time_local_x", "end_time_local_x", "level_1", "timezone_str_x"]
    #         )
    #         .lazy()
    #         .rename(
    #             {
    #                 "start_time_local_y": "start_time_local",
    #                 "end_time_local_y": "end_time_local",
    #                 "timezone_str_y": "timezone_str",
    #             }
    #         )
    #     )
    #     return ready_frame
    def _convert_timezone_v2(self, no_null_frame: pl.LazyFrame) -> pl.LazyFrame:
        """Converts timezone by adding the UTC offset calculated in __init__

        Args:
            no_null_frame (pl.LazyFrame): takes a frame with no null values

        Returns:
            pl.LazyFrame: returns a timezone aware frame
        """
        aware_frame: pl.LazyFrame = no_null_frame.select(
            pl.col(["store_id", "status", "timestamp_utc", "day"]),
            (pl.col("start_time_local") - pl.col("timezone_str")).dt.replace_time_zone(
                "UTC"
            ),
            (pl.col("end_time_local") - pl.col("timezone_str")).dt.replace_time_zone(
                "UTC"
            ),
            pl.col("timezone_str"),
        ).drop("timezone_str")
        return aware_frame

    def _filter_bussiness_hours_and_save_to_csv(self, ready_frame: pl.LazyFrame):
        """filters rows using timestamp_utc which are between start_time_local and end_time_local
        and also the day is equal to the timestamp_utc weekday

        Args:
            ready_frame (pl.LazyFrame): takes a timezone aware frame
        """

        """Filters rows if the gap between start_time_local and end_time_local is 24 hours"""
        filter_247 = ready_frame.lazy().filter(
            (
                pl.col("end_time_local") - pl.col("start_time_local")
                == pl.duration(hours=23, minutes=59, seconds=59)
            )
            & (
                (
                    (pl.col("end_time_local") >= pl.col("timestamp_utc"))
                    & (pl.col("start_time_local") <= pl.col("timestamp_utc"))
                    | (
                        (pl.col("start_time_local") >= pl.col("timestamp_utc"))
                        & (pl.col("end_time_local") <= pl.col("timestamp_utc"))
                    )
                )
            )
            & (pl.col("day") == (pl.col("timestamp_utc").dt.weekday() - 1))
        )

        filter_non_247 = ready_frame.lazy().filter(
            (
                pl.col("end_time_local") - pl.col("start_time_local")
                != pl.duration(hours=23, minutes=59, seconds=59)
            )
            & (
                (
                    (pl.col("end_time_local") >= pl.col("timestamp_utc"))
                    & (pl.col("start_time_local") <= pl.col("timestamp_utc"))
                )
            )
            & (pl.col("day") == (pl.col("timestamp_utc").dt.weekday() - 1))
        )
        # filtered_frame = filter_non_247.collect().extend(filter_247.collect())
        filtered_frame = pl.concat([filter_247, filter_non_247], parallel=True)
        grouped = filtered_frame.groupby("store_id").agg(
            pl.col(["timestamp_utc", "status", "day"])
        )
        with_max_date = grouped.select(
            pl.all(),
            pl.col("timestamp_utc").list.max().alias("max date"),
        )

        normal = with_max_date.explode(pl.col(["timestamp_utc", "status", "day"]))
        # temp = f.select(pl.col('timestamp_utc').dt.weekday()-1)
        middle = normal.select(
            pl.all(),
            (pl.col("timestamp_utc").dt.weekday() - 1).cast(pl.Int64).alias("weekday"),
        )
        # io is the bottleneck takes around 30 seconds
        with open(self.config["ASSETS"]["checkpoint_output_parser"], "wb") as file:
            middle.collect().write_csv(file)

        # def _uptime_or_downtime_in_hours(
        #     self, middle: pl.LazyFrame, report_type: ReportType
        # ) -> dict:
        #     if report_type:
        #         with_till_date = with_max_date.select(
        #             pl.all(),
        #             (
        #                 ((pl.col("max date") - datetime.timedelta(hours=1)).cast(int))
        #                 .cast(pl.Datetime)
        #                 .dt.cast_time_unit("ns")
        #                 .dt.replace_time_zone("UTC")
        #             ).alias("till date"),
        #         )
