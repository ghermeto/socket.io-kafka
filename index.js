/**
 * Kafka adapter for socket.io
 *
 * @example <caption>Example usage</caption>
 * var io = require('socket.io')(3000);
 * var kafka = require('socket.io-kafka');
 * io.adapter(kafka('localhost:2181'));
 *
 * @module socket.io-kafka
 * @see {@link https://www.npmjs.com/package/kafka-node|kafka-node}
 *
 * @author Guilherme Hermeto
 * @licence {@link http://opensource.org/licenses/MIT|MIT}
 */

/*jslint node: false */

'use strict';

var kafka = require('kafka-node'),
    Adapter = require('socket.io-adapter'),
    debug = require('debug')('socket.io-kafka'),
    async = require('async'),
    uid2 = require('uid2');

/**
 * Generator for the kafka Adapater
 *
 * @param {string} optional, zookeeper connection string
 * @param {object} adapter options
 * @return {Kafka} adapter
 * @api public
 */
function adapter(uri, options) {
    var opts = options || {},
        prefix = opts.key || 'socket.io',
        uid = uid2(6),
        client;

    // handle options only
    if ('object' === typeof uri) {
        opts = uri;
        uri = opts.uri || opts.host ? opts.host + ':' + opts.port : null;
        if (!uri) { throw new URIError('URI or host/port are required.'); }
    }

    // create producer and consumer if they weren't provided
    if (!opts.producer || !opts.consumer) {
        debug('creating new kafa client');
        client = new kafka.Client(uri, opts.clientId, { retries: 2 });
        if (!opts.producer) {
            debug('creating new kafa producer');
            opts.producer = new kafka.Producer(client);
        }
        if (!opts.consumer) {
            debug('creating new kafa consumer');
            opts.consumer = new kafka.Consumer(client, [], { groupId: prefix });
        }
    }
    /**
     * Kafka Adapter constructor.
     *
     * @constructor
     * @param {object} channel namespace
     * @api public
     */
    function Kafka(nsp) {
        var self = this,
            create = opts.createTopics;

        Adapter.call(this, nsp);

        this.uid = uid;
        this.options = opts;
        this.prefix = prefix;
        this.consumer = opts.consumer;
        this.producer = opts.producer;
        this.mainTopic = prefix + nsp.name;
        opts.createTopics = (create === undefined) ? true : create;

        opts.producer.on('ready', function () {
            debug('producer ready');
            self.createTopic(self.mainTopic);
            self.subscribe(self.mainTopic);

            // handle incoming messages to the channel
            self.consumer.on('message', self.onMessage.bind(self));
            self.consumer.on('error', self.onError.bind(self));
        });
    }

    // inherit from Adapter
    Kafka.prototype = Object.create(Adapter.prototype);
    Kafka.prototype.constructor = Kafka;

    /**
     * Emits the error.
     *
     * @param {object|string} error
     * @api private
     */
    Kafka.prototype.onError = function (err) {
        var self = this,
            arr = [].concat.apply([], arguments);

        if (err) {
            debug('emitting error', err);
            arr.forEach(function (error) { self.emit('error', error); });
        }
    };

    /**
     * Process a message received by a consumer. Ignores messages which come
     * from the same process.
     *
     * @param {object} kafka message
     * @api private
     */
    Kafka.prototype.onMessage = function (kafkaMessage) {
        var message, packet;

        try {
            message = JSON.parse(kafkaMessage.value);
            if (uid === message[0]) { return debug('ignore same uid'); }
            packet = message[1];

            if (packet && packet.nsp === undefined) {
                packet.nsp = '/';
            }

            if (!packet || packet.nsp !== this.nsp.name) {
                return debug('ignore different namespace');
            }

            this.broadcast(packet, message[2], true);
        } catch (err) {
            // failed to parse JSON?
            this.onError(err);
        }
    };

    /**
     * Converts a socket.io channel into a safe kafka topic name.
     *
     * @param {string} cahnnel name
     * @return {string} topic name
     * @api private
     */
    Kafka.prototype.safeTopicName = function (channel) {
        return channel.replace('/', '_');
    };

    /**
     * Uses the producer to create a new tpoic synchronously if
     * options.createTopics is true.
     *
     * @param {string} topic to create
     * @api private
     */
    Kafka.prototype.createTopic = function (channel) {
        var chn = this.safeTopicName(channel);

        debug('creating topic %s', chn);
        if (this.options.createTopics) {
            this.producer.createTopics(chn, this.onError.bind(this));
        }
    };

    /**
     * Uses the consumer to subscribe to a topic.
     *
     * @param {string} topic to subscribe to
     * @param {Kafka~subscribeCallback}
     * @api private
     */
    Kafka.prototype.subscribe = function (channel, callback) {
        var self = this,
            p = this.options.partition || 0,
            chn = this.safeTopicName(channel);

        debug('subscribing to %s', chn);
        self.consumer.addTopics([{topic: chn, partition: p}],
            function (err) {
                self.onError(err);
                if (callback) { callback(err); }
            });
    };

    /**
     * Uses the producer to send a message to kafka. Uses snappy compression.
     *
     * @param {string} topic to publish on
     * @param {object} packet to emit
     * @param {object} options
     * @api private
     */
    Kafka.prototype.publish = function (channel, packet, opts) {
        var self = this,
            msg = JSON.stringify([self.uid, packet, opts]),
            chn = this.safeTopicName(channel);

        this.producer.send([{ topic: chn, messages: [msg], attributes: 2 }],
            function (err, data) {
                debug('new offset in partition:', data);
                self.onError(err);
            });
    };

    /**
     * Broadcasts a packet.
     *
     * If remote is true, it will broadcast the packet. Else, it will also
     * produces a new message in one of the kafka topics (channel or rooms).
     *
     * @param {object} packet to emit
     * @param {object} options
     * @param {Boolean} whether the packet came from another node
     * @api public
     */
    Kafka.prototype.broadcast = function (packet, opts, remote) {
        var self = this,
            channel;

        debug('broadcasting packet', packet, opts);
        Adapter.prototype.broadcast.call(this, packet, opts);

        if (!remote) {
            channel = self.safeTopicName(self.mainTopic);
            self.publish(channel, packet, opts);
        }
    };

    return Kafka;
}

module.exports = adapter;
