#!/usr/bin/env python
"""
Hoover Consumer implemented using the Pika asynchronous API.  Based on code
available at

http://pika.readthedocs.io/en/0.10.0/examples/asynchronous_consumer_example.html
"""
import os
import sys
import random
import json
import time
import logging
import pika
import urllib # for urllib.quote

import hoover
import StringIO

_DEFAULT_CONFIG_FILE = '/etc/opt/nersc/slurmd_log_rotate_mq.conf'
_DEFAULT_CONFIG_FILE = 'amqpcreds.conf'
_AMQP_URI_TEMPLATE = "amqp%(ssl)s://%(username)s:%(password)s@%(server)s:%(port)s/%(vhost)s"
_MAX_RECONNECT_DELAY = 10.0 * 60.0
_HOOVER_TYPE_OUTDIR_MAP = {
    "darshan":  "darshanlogs",
    "manifest": "manifests",
    "_default": "misc",
}

LOGGER = logging.getLogger(__name__)

class HooverConsumer(object):
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """

    def __init__(self, config_file):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """

        config = _read_config(config_file)
        LOGGER.debug("Loaded config:\n%s" % json.dumps(config,indent=4))
        ### public attributes to describe config
        try:
            self.servers = config['servers']
            self.port = config['port']
            self.vhost = config['vhost']
            self.username = config['username']
            self.password = config['password']
            self.exchange = config['exchange']
            self.exchange_type = config['exchange_type']
            self.queue = config['queue']
            self.routing_key = config['routing_key']
            self.max_transmit = config['max_transmit_size']
        except KeyError:
            raise Exception("incomplete/malformed config file")

        ### Optional public attributes
        self.ssl = False
        if 'use_ssl' in config and config['use_ssl'] != 0:
            self.ssl = True
        self.output_dir = os.getcwd()
        if 'output_dir' in config:
            self.output_dir = config['output_dir']
        self.type_outdir_map = _HOOVER_TYPE_OUTDIR_MAP
        if 'type_outdir_map' in config:
            self.type_outdir_map = config['type_outdir_map']

        ### private attributes to describe rabbitmq state
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        server_list = self.servers[:] # shallow copy
        while len(server_list) > 0:
            server = random.choice(server_list)
            server_list.remove(server)

            url = _AMQP_URI_TEMPLATE % ({
                'username': self.username,
                'password': self.password,
                'server': server,
                'port': self.port,
                'vhost': urllib.quote(self.vhost, safe=""),
                'ssl': "s" if self.ssl is True else "",
            })

            LOGGER.debug('Attempting to connect to %s' % url)
            try:
                conn = pika.SelectConnection(
                    parameters=pika.URLParameters(url),
                    on_open_callback=self.on_connection_open,
                    stop_ioloop_on_close=False)
                LOGGER.info('Connected to %s' % server)
                return conn
            except:
                LOGGER.error('Unexpected error: %s' % str(sys.exc_info()))
                continue

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        LOGGER.info('Connection opened')

        LOGGER.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)


    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)
            LOGGER.debug("Connection state: closed=%s, closing=%s, open=%s" % (
                str(self._connection.is_closed),
                str(self._connection.is_closing),
                str(self._connection.is_open)))

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        Do not attempt to simply revive the existing connection; restart the
        entire connection process so that we can select a new server from the
        HA cluster.
        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:
            self._connection = None
            delay = 5.0

            while True:
                # Create a new connection
                self.connect()
                if self._connection is None:
                    LOGGER.warning('Reconnect failed, tryin again in %d seconds' % int(delay))
                    time.sleep(delay)
                    delay = min(2.0 * delay, _MAX_RECONNECT_DELAY)
                else:
                    break

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info('Channel opened')
        self._channel = channel

        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

        LOGGER.info('Declaring exchange %s', self.exchange)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       self.exchange,
                                       self.exchange_type)


    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        LOGGER.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self._connection.close()

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        LOGGER.info('Exchange declared')
        LOGGER.info('Declaring queue %s', self.queue)
        self._channel.queue_declare(self.on_queue_declareok, self.queue)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        LOGGER.info('Binding %s to %s with %s',
                    self.exchange, self.queue, self.routing_key)
        self._channel.queue_bind(self.on_bindok, self.queue,
                                 self.exchange, self.routing_key)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        LOGGER.info('Queue bound')
        LOGGER.info('Issuing consumer related RPC commands')
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.queue)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        LOGGER.info('Received message # %s from %s',
                    basic_deliver.delivery_tag, properties.app_id )

        ### Figure out what to call this file
        if properties.headers is None or 'sha_hash' not in properties.headers:
            ### Messages without checksums at all are useless to us; discard
            LOGGER.error("No checksum provided in message header:\n%s" %
                json.dumps(properties.headers))
            return
        elif 'filename' not in properties.headers:
            ### No filename with checksum indicates a manifest being sent.
            ### Key manifests their expected contents so that if a manifest
            ### has to be re-sent, it is not duplicated on the consumer side
            output_file = os.path.basename('manifest_%s.json' % properties.headers['sha_hash'])
        else:
            ### Actual files have intended file names embedded
            output_file = os.path.basename(properties.headers['filename'])

        ### Figure out where to put this file.  If the global config is an
        ### absolute path, we use that and disregard output_dir entirely;
        ### otherwise, we take output_dir as a base then tack on the globally
        ### configured dir
        if 'type' not in properties.headers:
            parent_dir = ""
        elif properties.headers['type'] not in self.type_outdir_map:
            parent_dir = self.type_outdir_map['_default']
        else:
            parent_dir = self.type_outdir_map[properties.headers['type']]

        if not parent_dir.startswith(os.sep):
            parent_dir = os.path.join(self.output_dir, parent_dir)

        output_file = os.path.join(parent_dir, output_file)

        ### Start interacting with the system and keep an eye out for exceptions
        try: 
            if not os.path.isdir(parent_dir):
                LOGGER.info("Creating output dir [%s]" % parent_dir)
                os.makedirs(parent_dir)

            ### Write the message body into the intended file
            open( output_file, 'w+' ).write(body)
        except:
            LOGGER.error('Unexpected error: %s' % str(sys.exc_info()))
            self._channel.basic_nack(basic_deliver.delivery_tag)

        ### Calculate checksum and compare to manifest
        checksum = hoover.checksum( StringIO.StringIO(body) )
        if checksum == properties.headers['sha_hash']:
            LOGGER.info("Wrote output to %s (cksum: %s)" % (output_file, checksum))
            LOGGER.info('Acknowledging message %s', basic_deliver.delivery_tag)
            self._channel.basic_ack(basic_deliver.delivery_tag)
        else:
            LOGGER.error("Checksum mismatch for %s (cksum: %s, was expecting %s)" % 
                (output_file, checksum, properties.headers['sha_hash']))
            ### We assume that sha mismatch occurred on the network (unlikely)
            ### or at this client (e.g., out of space).
            self._channel.basic_nack(basic_deliver.delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        if self._connection is None:
            raise Exception('NULL connection')
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        LOGGER.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        LOGGER.info('Stopped')

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        LOGGER.info('Closing connection')
        self._connection.close()


def _read_config(filename):
    """
    Read a Hoover configuration file and return a dict of parameters

    :param str filename: path to the file which defines Hoover/RabbitMQ configs
    """
    LOGGER.debug("Loading config from %s" % filename)
    config = { 'use_ssl': False }
    with open(filename, 'r') as fp:
        for line in fp:
            line = line.strip()
            (key,value) = (x.strip() for x in line.split('=', 1))
            if key == 'servers':
                value = [x.strip() for x in value.split(',')]
            elif key == 'port':
                value = int(value)
            elif key == 'use_ssl':
                if int(value) != 0:
                    value = True
                else:
                    value = False
            elif key == 'type_outdir_map':
                value = json.reads(value)
            config[key] = value
    return config

def main():
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 2:
        consumer = HooverConsumer(_DEFAULT_CONFIG_FILE)
    else:
        consumer = HooverConsumer(sys.argv[1])

    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()

if __name__ == '__main__':
    main()
