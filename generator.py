import polars as pl
from configparser import ConfigParser
import datetime
import json
from concurrent.futures import Future, ThreadPoolExecutor
import time


class Generator:
    def __init__(self, request_id: str, config: ConfigParser) -> None:
        self.request_id = request_id
        self.config = config
        self.output_file = pl.read_csv(
            self.config["ASSETS"]["checkpoint_output_parser"],
            dtypes=[
                pl.Int64,
                pl.Datetime(time_unit="ns", time_zone="UTC"),
                pl.Utf8,
                pl.Int32,
                pl.Datetime(time_unit="ns", time_zone="UTC"),
                pl.Int32,
            ],
        )

    def parser_data(self):
        print("task started")
        start = time.time()
        # jobs = []
        # week = multiprocessing.Process(target=self._uptime_or_downtime_week())
        # jobs.append(week)
        # week.start()
        # day = multiprocessing.Process(target=self._uptime_or_downtime_day())
        # jobs.append(day)
        # day.start()
        # hour = multiprocessing.Process(target=self._uptime_or_downtime_hour())
        # jobs.append(hour)
        # hour.start()
        with ThreadPoolExecutor() as executor:
            # tasks are run in seperate threads to lower the compilation time its around 35 seconds right now
            week: Future[dict] = executor.submit(self._uptime_or_downtime_week)
            day: Future[dict] = executor.submit(self._uptime_or_downtime_day)
            hour: Future[dict] = executor.submit(self._uptime_or_downtime_hour)

            week_result: dict = week.result()
            day_result: dict = day.result()
            hour_result: dict = hour.result()

            final_dict: dict = {}
            sub_dict: dict = {}
            for val in list(week_result.keys()):
                sub_dict["week"] = week_result[val]
                if val in day_result:
                    sub_dict["day"] = day_result[val]
                if val in hour_result:
                    sub_dict["hour"] = hour_result[val]
                final_dict[val] = sub_dict
                sub_dict = {}
        end = time.time()
        # for job in jobs:
        #     job.join()
        print("task finished in: ", end - start)
        # after the task is finished the dictionary is converted to json and wrote to a file with request_id as the name
        with open(
            f"{self.config['FLASK']['request_output']}/{self.request_id}.json", "w"
        ) as file:
            json.dump(final_dict, file)
        file.close()
        # request_id is set to true in request_ids.json
        self._set_request_id_to_true()

    def _set_request_id_to_true(self) -> None:
        """opens request_ids.json and sets the request_id = True when the task is completed
        """
        data: dict = None
        with open(f"{self.config['FLASK']['request_ids']}", "r") as file:
            data: dict = json.load(file)
            data[self.request_id] = True
        file.close()
        with open(f"{self.config['FLASK']['request_ids']}", "w") as file:
            json.dump(data, file)
        file.close()

    def _uptime_or_downtime_hour(self) -> dict:
        """calculate uptime and downtime in the last hour

        Returns:
            dict: returns a dict of the structure {store_id: [active_minutes, inactive_minutes]}
        """
        df: pl.DataFrame = self.output_file.select(
            pl.all(),
            (
                ((pl.col("max date") - datetime.timedelta(hours=1)).cast(int))
                .cast(pl.Datetime)
                .dt.cast_time_unit("ns")
                .dt.replace_time_zone("UTC")
            ).alias("till date"),
        )
        finale: dict = self._get_resulting_dict(frame=df)
        return finale

    def _uptime_or_downtime_day(self) -> dict:
        """calculate uptime and downtime in the last day

        Returns:
            dict: returns a dict of the structure {store_id: [active_minutes, inactive_minutes]}
        """
        df: pl.DataFrame = self.output_file.select(
            pl.all(),
            (
                ((pl.col("max date") - datetime.timedelta(days=1)).cast(int))
                .cast(pl.Datetime)
                .dt.cast_time_unit("ns")
                .dt.replace_time_zone("UTC")
            ).alias("till date"),
        )
        finale: dict = self._get_resulting_dict(frame=df)
        return finale

    def _uptime_or_downtime_week(self) -> dict:
        """calculate uptime and downtime in the last week

        Returns:
            dict: returns a dict of the structure {store_id: [active_minutes, inactive_minutes]}
        """
        df: pl.DataFrame = self.output_file.select(
            pl.all(),
            (
                ((pl.col("max date") - datetime.timedelta(days=7)).cast(int))
                .cast(pl.Datetime)
                .dt.cast_time_unit("ns")
                .dt.replace_time_zone("UTC")
            ).alias("till date"),
        )
        finale: dict = self._get_resulting_dict(frame=df)
        return finale

    def _get_resulting_dict(self, frame: pl.DataFrame) -> dict:
        """First -  we iterate over all the unique ids and filter out a subset of the dataframe
        Second - we upsample the data by filling in gaps with time interval of 1 minute
        Third - Then we further filter out the dataframe by selecting rows which have timestamp_utc between max and till dates
        Fourth - Then we group the dataframe by id and calculate the occurences of active and inactive per id
        Fifth - Number of occurences of active = active minutes since data is upsampled to an interval of 1 minute
        
        #### upsampling requires atleast two datapoints
    
        Args:
            frame (pl.DataFrame): takes a data frame with till date calculated

        Returns:
            dict: returns a dict of the structure {store_id: [active_minutes, inactive_minutes]}
        """
        unique_store_ids: pl.DataFrame = frame.select(pl.col("store_id").unique())
        finale: dict = {}
        active_minutes: int = 0
        inactive_minutes: int = 0
        for value in unique_store_ids.iter_rows():
            required_frame: pl.DataFrame = frame.filter(pl.col("store_id") == value[0])
            required_sorted_frame: pl.DataFrame = required_frame.sort(by="timestamp_utc", descending=False)
            upsampled_frame: pl.DataFrame = required_sorted_frame.upsample(time_column="timestamp_utc", every="1m").fill_null(
                strategy="forward"
            )
            # pl.concat([finale, k], how="align")
            within_business_hours_frame: pl.LazyFrame = upsampled_frame.lazy().filter(
                (
                    (pl.col("timestamp_utc") > pl.col("till date"))
                    & (pl.col("timestamp_utc") <= pl.col("max date"))
                )
                & (pl.col("weekday") == pl.col("day"))
            )
            grouped_frame_within_hours: pl.LazyFrame = (
                within_business_hours_frame.lazy()
                .groupby("store_id")
                .agg(pl.col(["timestamp_utc", "status", "max date", "till date"]))
            )
            frame_with_count: pl.LazyFrame = grouped_frame_within_hours.lazy().select(
                pl.all(),
                pl.col("status").list.count_match("active").alias("active_count"),
                pl.col("status").list.count_match("inactive").alias("inactive_count"),
            )
            collected_frame: pl.DataFrame = frame_with_count.collect()
            # print(collected_l)

            try:
                active_minutes: int = pl.first(collected_frame["active_count"])
            except IndexError:
                active_minutes: int = 0
            try:
                pl.first(collected_frame["inactive_count"])
            except IndexError:
                inactive_minutes: int = 0
            finale[value[0]] = [active_minutes, inactive_minutes]

        return finale

    # def parse_ready_data(self):
