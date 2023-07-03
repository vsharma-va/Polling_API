import configparser
from flask import Flask
from flask_cors import CORS
from waitress import serve
from data_endpoint import data_bp

config = configparser.ConfigParser()
config.read('./project.config')
app = Flask(__name__, static_folder="../")
app.config['SECRET_KEY'] = config['FLASK']['secret_key']

api_v1_cors_config = {
    "origins": ["http://localhost:8080", "http://localhost:5173", "http://10.29.51.15:8080", "http://10.29.90.96:8080", "http://172.20.15.5:3000"],
    "methods": ["GET", "POST"],
    "allow_headers": ["Authorization", "Content-Type"],
}

CORS(app, resources={
    r"/api/v1/*": api_v1_cors_config,
}, supports_credentials=True)

app.register_blueprint(data_bp)

if __name__ == '__main__':
    serve(app, host="0.0.0.0", port=5000, threads=20)
    # app.run(port=5000, debug=True, threaded=True, host="0.0.0.0")
