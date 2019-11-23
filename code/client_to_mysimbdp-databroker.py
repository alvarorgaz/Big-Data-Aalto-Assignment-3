import sys, pika, argparse, json, pandas, datetime

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client_id', type=str, help='Set the client ID.')
    parser.add_argument('--dataset_path', type=str, help='Set the path of the dataset to ingest including the extension.')
    parser.add_argument('--server_address_broker', type=str, default='35.228.105.231')
    return parser.parse_args()
args = parse_args()

def find_extension(filename):
    return filename.split('.')[-1]
args.file_extension = find_extension(args.dataset_path)

if args.file_extension=='csv':
    data = pandas.read_csv(args.dataset_path)
else:
    sys.exit('File extension not supported, ".csv" needed.')

rabbitmq_credentials = pika.PlainCredentials('user', 'CzZETuZ1giTb')
rabbitmq_parameters = pika.ConnectionParameters(args.server_address_broker, '5672', '/', rabbitmq_credentials)
rabbitmq_connection = pika.BlockingConnection(rabbitmq_parameters)
rabbitmq_channel = rabbitmq_connection.channel()
rabbitmq_topic = 'input_topic_'+args.client_id

start = datetime.datetime.now()
for row in data.index:
    row_to_ingest_json = data.loc[[row],].to_dict(orient='records')[0]
    message_json = {'data':row_to_ingest_json,'sending_time':datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    rabbitmq_channel.queue_declare(queue=rabbitmq_topic)
    rabbitmq_channel.basic_publish(exchange='', routing_key=rabbitmq_topic, body=json.dumps(message_json))

print('All data sent to message broker in', (datetime.datetime.now()-start).total_seconds(), 'seconds.')
rabbitmq_connection.close()