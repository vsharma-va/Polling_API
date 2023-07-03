# Polling_API

## Requirements ->
All the packages used are listed in requirements.txt file. No additional downloads required

## Folder Structure ->
![image](https://github.com/vsharma-va/Polling_API/assets/78730763/857582ad-ed94-4fc1-bb77-b7653ee577c5)

## Flow (Program is divided into two parts)->

* ###  main_1.py ->
  * It is responsible for readying the data for report generation.
  * For optimization purposes the data is only recalculated when a change is detected in **store status.csv** file.
  * The operations take around a second and writting to a csv file takes around 30 seconds
  * **Data operation functions are present in parser_pl.py file**
### Video has been cut to reduce size. Actual time to compute is displayed in the console window
[main_1.webm](https://github.com/vsharma-va/Polling_API/assets/78730763/b3fad1a4-576f-4632-8cab-dcd06019b5ea)

* ### server.py ->
  * It is responsible for handling the requests from user.
  * It generates a report_id whenever a request is sent to /trigger_report
  * The report_id is saved in response_ids.json and the value is set to False
  * When the report generation for this report_id is completed the value is set to True in the response_ids.json file. The output is saved as a json in ./calc/results/{response_id}.json
  * When the user sends a request to /get_report/{response_id} the function checks if response_id = True in response_ids.json file.
  * If True it returns the data as a csv file
  * Otherwise returns "In Progress"
  * **Data operation functions are present in generator.py file**
  * **data_endpoint.py contains the api endpoints**
    
### Video has been cut to reduce size. Actual time to compute is displayed in the console window
https://github.com/vsharma-va/Polling_API/assets/78730763/fe99296c-fa6d-4d5e-8e80-8a5967670322

