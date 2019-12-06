#!flask/bin/python
# from __future__ import absolute_import
# from google.api_core.protobuf_helpers import get_messages
from google.cloud import pubsub
from google.cloud import datastore

import logging
import base64
import json
import os

from flask import current_app, Flask, jsonify, make_response, request

import redis
#import psycopg2
#import sqlalchemy

# redis config
redis_host = os.environ.get('REDISHOST', 'localhost')
redis_port = int(os.environ.get('REDISPORT', 6379))
redis_client = redis.StrictRedis(host=redis_host, port=redis_port)

app = Flask(__name__)

# tasks list in memory - todo: tasks should go into a database.
tasks = [
    {
        'id': 1,
        'title': u'Update DVB-TA TM-IPI confluence page',
        'description': u'Update confluence page and create straw man doc for TM-IPI scope', 
        'done': False
    },
    {
        'id': 2,
        'title': u'Create a web service API example and deploy in App Engine',
        'description': u'Complete the web service with REST API methods', 
        'done': False
    }
]

# configure the following variables via app.yaml
# these variables are used in the push request handler
# to verify that request came from a trusted source
app.config['PUBSUB_VERIFICATION_TOKEN'] = os.environ['PUBSUB_VERIFICATION_TOKEN']
app.config['PUBSUB_TOPIC'] = os.environ['PUBSUB_TOPIC']
app.config['PROJECT'] = os.environ['GOOGLE_CLOUD_PROJECT']

# configure unix domain socket to connect to Cloud SQL instance
# the socket is found at /cloudsql/INSTANCE_CONNECTION_NAME
# for PostgreSQL add .s.PGSQL.5432
# i.e. /cloudsql/utv-adload:us-central1:utvadload-postgresql-instance1/.s.PGSQL.5432
#db = sqlalchemy.create_engine(
        # equivalent URL
        # postgres+pg8000://<db_user>:<db_pass>@/<db_name>?unix_socket=/cloudsql/utv-adload:us-central1:utvadload-postgresql-instance1/.s.PGSQL.5432
#        sqlalchemy.engine.url.URL(
#            drivername='postgres+pg8000',
#            username=postgres,
#            password=udemiR001,
#            database=utv_adload,
#            query={
#                'unix_sock': '/cloudsql/{}/.s.PGSQL.5432'.format(utv-adload:us-central1:utvadload-postgresql-instance1)
#            }

# global list to store pub/sub messages received
MESSAGES = []

# method for the root directory
@app.route('/')
def index():
    return "Hello Ulas, this is working!"

    if request.method == 'GET':
        return render_template('index.html', messages = MESSAGES)


# method to return the tasks list in json format
@app.route('/todo/v1.0/tasks', methods=['GET'])
def get_tasks():
    #cust_record = redis_client.hgetall('customerid:102')
    #return "Skip Offset: {}\n".format(cust_record)
    return jsonify({'tasks': tasks})


# method to return customer records from PostgreSQL
#@app.route('/todo/v1.0/records_psql/<int:cust:id>', methods=['GET'])
#def get_records_psql(cust_id):
#    psql_host = 'unix_sock': '/cloudsql/{}/.s.PGSQL.5432'.format(utv-adload:us-central1:utvadload-postgresql-instance1)
#    psql_cnx = psycopg2.connect(dbname=utv_adload, user=postgres, password=udemiR001, host=psql_host)
#    with psql_cnx.cursor() as cursor:
#        psql_cnx.cursor('SELECT * FROM adload;')
#        all_records = cursor.fetchall()
#        psql_cnx.commit()
#        psql_cnx.close()
#        return jsonify({'all_records': all_records })


# method to return customer records from firestore
@app.route('/todo/v1.0/records_firestore/<int:cust_id>', methods=['GET'])
def get_records_firestore(cust_id):
    client = datastore.Client()

    # this is to be able to return any query from firestore.
    # handle this functionality in separate functions 
    key = client.key('adload', str(cust_id))
    client_record = client.get(key)
    #query = client.query(kind='adload')
    #query.add_filter('no_ads', '=', 'true')
    #return "Cust records: {}\n".format(sorted(client_record))
    #return jsonify({'Client record': sorted(client_record)})
    return jsonify({'Client record': client_record})

#def list_command(client, args)
    # list all customers
#    print((list_tasks(client)))

# method to return customer records from redis
@app.route('/todo/v1.0/records_redis/<int:cust_id>', methods=['GET'])
def get_records_redis(cust_id):
    customerid = "customerid:" + str(cust_id)
    cust_record = redis_client.hgetall(customerid)
    return "Skip Offset: {}\n".format(cust_record)


# method to return individual tasks in jason format
@app.route('/todo/v1.0/task/<int:task_id>', methods=['GET'])
def get_task(task_id):
    task = [task for task in tasks if task['id'] == task_id]
#    for task in tasks:
#        if task['id'] == task_id:
#            task = task['id']

    if len(task) == 0:
        abort(404)
    return jsonify({'task': task})


# method to post a new task to be added to the list
@app.route('/todo/v1.0/tasks', methods=['POST'])
def create_task():
#    if not request.json or not 'title' in request.json:
#        abort(400)

    task = {
        'id': tasks[-1]['id'] + 1,
        'title': request.json['title'],
        'description': request.json.get('description', ""),
        'done': False 
    }
    tasks.append(task)
    return jsonify({'task': task})

# method to receive pushed messages to pub/sub topic
@app.route('/todo/v1.0/task/publish_message', methods=['POST'])
def pubsub_push():
    if(request.args.get('token', '') != current_app.config['PUBSUB_VERIFICATION_TOKEN']):
        return 'Invalid push request', 400

    envelope = json.loads(request.data.decode('utf-8'))
    payload = base64.b64decode(envelope['message']['data'])

    MESSAGES.append(payload)

    # returning 2xx status to indicate successful receipt of the message
    return 'OK', 200

#method to mark a task as 'done'
@app.route('/todo/v1.0/task/<int:task_id>', methods=['POST'])
def mark_done(task_id):
#    if not request.json or not 'title' in request.json:
#        abort(400)

    task = [task for task in tasks if task['id'] == task_id]

#    if len(task) == 0:
#        abort(404)
#    if not request.json:
#        abort(400)
#    if 'title' in request.json and type(request.json['title']) != unicode:
#        abort(400)
#    if 'description' in request.json and type(request.json['description']) is not unicode:
#        abort(400)
#    if 'done' in request.json and type(request.json['done']) is not bool:
#        abort(400)

    task[0]['done'] = request.json.get('done', task[0]['done'])

    return jsonify({'task': task[0]})

#method to delete a task from the tasks list
@app.route('/todo/v1.0/task/<int:task_id>', methods=['DELETE'])
def delete_task(task_id):
    task = [task for task in tasks if task['id'] == task_id]
    if len(task) == 0:
        abort(404)
    tasks.remove(task[0])
    return jesonify({'result': True})

# method to return 404 error message in json format
@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

# method to return 500 error message (can this be returned???)
@app.errorhandler(500)
def server_error(e):
    # log the error in stacktrace
    logging.exception("An error occured during the request")
    return "An error occured with the request", 500


if (__name__ == '__main__'):
#    parser = argparse.ArgumentParser()
#    subparsers = parser.add_subparsers()

#    parser.add_argument('--project-id', help='utv_adload')

#    list_parser = subparsers.add_parser('list', help=list_command.__doc__)
#    list_parser.set_defaults(func=list_command)

#    args = parser.parse_args()

#    client = create_client(args.utv_adload)
#    args.func(client, args)

    try:
        import googleclouddebugger
        googleclouddebugger.enable()
    except ImportError:
        pass

    app.run(debug=True)

