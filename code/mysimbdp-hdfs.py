import pika, json, argparse, threading, importlib, time, sys

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server_address_broker', type=str, default='35.228.105.231')
    return parser.parse_args()
args = parse_args()

rabbitmq_credentials = pika.PlainCredentials('user', 'CzZETuZ1giTb')
rabbitmq_parameters = pika.ConnectionParameters(args.server_address_broker, '5672', '/', rabbitmq_credentials)

class consume_hdfs_topic(threading.Thread):
    def __init__(self, client_id):
        threading.Thread.__init__(self)
        self.client_id = client_id
    def run(self):
        def callback(_, __, ___, body):
            message_json = json.loads(body.decode())
            sending_time, data = message_json['sending_time'], message_json['data']
            customer_app = importlib.import_module('customerstreamapp--'+self.client_id)
            report = customer_app.run(data)
            print('For the message of '+self.client_id+' sent at '+str(sending_time)+' the report is:\n', report, '\n',
                  file=open('logs/customerstreamapp.log', 'a'))
        rabbitmq_topic = 'hdfs_topic_'+self.client_id
        rabbitmq_connection = pika.BlockingConnection(rabbitmq_parameters)
        rabbitmq_channel = rabbitmq_connection.channel()
        rabbitmq_channel.queue_declare(queue=rabbitmq_topic)
        rabbitmq_channel.basic_consume(queue=rabbitmq_topic, on_message_callback=callback)
        rabbitmq_channel.start_consuming()

available_clients_id = ['client1','client2']
for client_id in available_clients_id:
    print('Starting new HDFS RabbitMQ topic for '+client_id)
    thread = consume_hdfs_topic(client_id)
    thread.daemon = True
    thread.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print('\nKeyboardInterrupt, closing all active threads.\n')
    sys.exit()