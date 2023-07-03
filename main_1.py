import time
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import configparser
from parser_pl import ParserPl
import multiprocessing
from io import TextIOWrapper

# For optimization purposes the initial data is only computed when a change in the store status.csv is detected
# otherwise when this script is run it computes the result and stores it in ./calc/middle.csv
# and whenever a change is detected it spawns a process which computes the data and stores it in the same file
## RUN THIS SCRIPT AND LET IT FINISH ONCE BEFORE STARTING THE SERVER - OVERSIGHT
class MyHandler(FileSystemEventHandler):
    def __init__(self):
        self.last_modified: datetime = datetime.now()
        spawn_process()

    # ASSUMPTION - File change interval is greater than 1 minute
    def on_modified(self, event):
        if datetime.now() - self.last_modified < timedelta(seconds=1):
            return
        else:
            self.last_modified: datetime = datetime.now()
        if (
            event.event_type == "modified"
            and event.src_path == config["ASSETS"]["store_status"]
        ):
            print("\nchanges detected spawing process\n")
            spawn_process()

def spawn_process():
    # spawns the process which recalculates the data
    h = ParserPl(file_path, config)
    start = time.time()
    proc = multiprocessing.get_context('spawn').Process(
        target=h.setup_data
    )
    proc.start()
    proc.join()
    end = time.time()
    print(end - start)


if __name__ == "__main__":
    config: configparser.ConfigParser = configparser.ConfigParser()
    config.read("project.config")
    file_path: str = f"{config['ASSETS']['store_status']}"
    file: TextIOWrapper = open(file_path, "r")
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(
        event_handler, path=config["ASSETS"]["dataset_directory"], recursive=False
    )
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
