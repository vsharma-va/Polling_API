from flask_wtf.csrf import Blueprint, request
from flask import Response
from flask_cors import CORS
import uuid
import json
import configparser
from generator import Generator
from threading import Thread


data_bp = Blueprint("data_endpoint", __name__, url_prefix="/api/v1/data")

CORS(data_bp)
config = configparser.ConfigParser()
config.read("./project.config")


@data_bp.route("/trigger_report", methods=["GET"])
def trigger_report_generation():
    # manages which requests are ongoing by setting that request_id = False
    # if the report is completed it has the value request_id = True
    if request.method == "GET":
        id_for_request: str = uuid.uuid4().hex
        try:
            try:
                with open("./request_ids.json", "r") as file:
                    data: dict = json.load(file)
                    temp: dict = {}
                    temp[id_for_request] = False
                    data.update(temp)
                file.close()
                with open("./request_ids.json", "w") as file:
                    json.dump(data, file)
                file.close()
            except json.decoder.JSONDecodeError:
                with open(config["FLASK"]["request_ids"], "w") as file:
                    temp: dict = {}
                    temp[id_for_request] = False
                    json.dump(temp, file)
        except FileNotFoundError:
            temp: dict = {}
            temp[id_for_request] = False
            open("./request_ids.json", "a").close()
            with open("./request_ids.json", "r+") as file:
                json.dump(temp, file)
            file.close()
        # start the report generation in a seperate thread
        generator = Generator(request_id=id_for_request, config=config)
        Thread(target=generator.parser_data).start()
        # spawn_report_generation_process(request_id=id_for_request)
        return json.dumps(
            {"status": "success", "data": {"request_id": f"{id_for_request}"}}
        )


@data_bp.route("/get_report/<report_id>", methods=["GET"])
def get_report_if_done(report_id):
    if request.method == "GET":
        with open(config["FLASK"]["request_ids"]) as file:
            data: dict = json.load(file)
            if report_id in data:
                if data[report_id] == True:
                    data = None
                    csv_string: str = convert_dict_to_csv(report_id)
                    print(csv_string)
                    return Response(
                        csv_string,
                        mimetype="text/csv",
                        headers={
                            "Content-disposition": "attachment; filename=result.csv"
                        },
                    )

                else:
                    return json.dumps(
                        {
                            "status": "success",
                            "data": {"information": "task is running"},
                        }
                    )
            else:
                return json.dumps(
                    {
                        "status": "fail",
                        "data": {
                            "title": "request a report id with /trigger_report endpoint"
                        },
                    }
                )


def convert_dict_to_csv(request_id: str) -> str:
    """Converts the dictionary in ./calc/results/{request_id}.json to a csv string

    Arguments str: takes the request_id sent with the get request to /get_report

    returns str: returns the converted csv string
    """

    data = None
    with open(f'{config["FLASK"]["request_output"]}/{request_id}.json', "r") as file:
        data: dict = json.load(file)
    csv_string: str = "store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week\n"
    for key in list(data.keys()):
        csv_string += f"{key}, "
        """ {'store_id': {'week': [active minutes, inactive minutes], 'day': [active minutes, inactive minutes], 'hour': [active minutes, inactive minutes]}}  """
        if data[key]["hour"][0] == 0 and data[key]["hour"][1] == 0:
            """This means that the polling data doesn't contain any data for this store between this time period"""
            """Assuming that the restaurant is inactive"""
            data[key]["hour"][1] = 60
        elif data[key]["hour"][0] < 60 and data[key]["hour"][1] == 0:
            """Not enough data present in the polling data to interpolate"""
            """Assuming that the restaurant is inactive for rest of the time period"""
            data[key]["hour"][1] = 60 - data[key]["hour"][0]
        elif data[key]["hour"][1] < 60 and data[key]["hour"][1] == 0:
            """Not enough data present in the polling data to interpolate"""
            """Assuming that the restaurant is inactive for rest of the time period"""
            data[key]["hour"][1] = 60
        csv_string += f"{data[key]['hour'][0]}, "

        if data[key]["day"][0] == 0 and data[key]["day"][1] == 0:
            """This means that the polling data doesn't contain any data for this store between this time period"""
            """Assuming that the restaurant is inactive"""
            data[key]["day"][1] = 1440
        elif data[key]["day"][0] < 1440 and data[key]["day"][1] == 0:
            """Not enough data present in the polling data to interpolate"""
            """Assuming that the restaurant is inactive for rest of the time period"""
            data[key]["day"][1] = 1440 - data[key]["day"][0]
        elif data[key]["day"][1] < 1440 and data[key]["day"][0] == 0:
            """Not enough data present in the polling data to interpolate"""
            """Assuming that the restaurant is inactive for rest of the time period"""
            data[key]["day"][1] = 1440
        csv_string += f"{data[key]['day'][0]/(60)}, "

        if data[key]["week"][0] == 0 and data[key]["week"][1] == 0:
            """This means that the polling data doesn't contain any data for this store between this time period"""
            """Assuming that the restaurant is inactive"""
            data[key]["week"][1] = 10080
        elif data[key]["week"][0] < 10080 and data[key]["week"][1] == 0:
            """Not enough data present in the polling data to interpolate"""
            """Assuming that the restaurant is inactive for rest of the time period"""
            data[key]["week"][1] = 10080 - data[key]["week"][0]
        elif data[key]["week"][1] < 10080 and data[key]["week"][0] == 0:
            """Not enough data present in the polling data to interpolate"""
            """Assuming that the restaurant is inactive for rest of the time period"""
            data[key]["week"][1] = 10080
        csv_string += f"{data[key]['week'][0]/(60*24)}, "

        csv_string += f"{data[key]['hour'][1]}, "
        csv_string += f"{data[key]['day'][1]/(60)}, "
        csv_string += f"{data[key]['week'][1]/(60*24)}\n"
    return csv_string


# def spawn_report_generation_process(request_id: str):
#     print('starting report generation')
