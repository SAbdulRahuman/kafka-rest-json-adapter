from flask import Flask, request, jsonify
from pymongo import MongoClient
import os

app = Flask(__name__)


@app.route("/ping")
def ping():
    return "pong"


@app.route("/tokafkarest", methods=['POST'])
def post_to_kafka_rest():
    try:
        jsonvalue = request.json
        kafka_json_format = jsonify(
            {
                "records": [
                    {
                        "key": "",
                        "value": jsonvalue
                    }
                ]
            }
        )
        kafka_url, kafka_port, kafka_topics = get_environments()
        return kafka_json_format, 200
    except Exception as exp:
        print("ABDEBUG : ", exp)
        return(
            jsonify({"status": "failed", "payload": "Internal Server Exception Occured"}),
            500
        )

def get_environments():
    kafka_rest_url = os.environ.get('KAFKA_REST_URL')
    kafka_rest_port = os.environ.get('KAFKA_REST_PORT')
    kafka_topics = os.environ.get('KAFKA_TOPICS')

    if not kafka_rest_url:
        kafka_rest_url = 'localhost'

    if not kafka_rest_port:
        kafka_rest_port = '8082'

    if not kafka_topics:
        kafka_topics = 'jsontest'

    return kafka_rest_url, kafka_rest_port, kafka_topics


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
