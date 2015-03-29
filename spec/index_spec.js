/*jslint node: false, unparam: false */
/*globals jasmine: true, describe: true, it: true, beforeEach: true,
  expect: true, spyOn: true */

'use strict';

var kafkaAdapter = require('../index.js'),
    kafka = require('kafka-node');

describe('module:socket.io-kafka', function () {
    var opts, Adapter, instance;

    beforeEach(function () {
        opts = {};
    });

    describe('adapter', function () {
        it('should return a Kafka function', function () {
            var adapter = kafkaAdapter('localhost:2821');
            expect(adapter.name).toEqual('Kafka');
            expect(adapter).toEqual(jasmine.any(Function));
        });

        it('should create a kafka.Producer instance if producer not provided',
            function () {
                kafkaAdapter('localhost:2821', opts);
                expect(opts.producer).toEqual(jasmine.any(kafka.Producer));
            });

        it('should create a kafka.Consumer instance if consumer not provided',
            function () {
                kafkaAdapter('localhost:2821', opts);
                expect(opts.consumer).toEqual(jasmine.any(kafka.Consumer));
            });

        describe('only options object', function () {
            it('should accept only the options object', function () {
                opts.host = 'localhost';
                opts.port = 2821;
                kafkaAdapter(opts);
                expect(opts.producer).toBeDefined();
            });

            it('should throw if uri or host/port are defined', function () {
                expect(function () { kafkaAdapter(opts); }).toThrow();
            });
        });

    });

    describe('adapter.Kafka', function () {
        describe('without producer', function () {
            beforeEach(function () {
                opts.producer = jasmine.createSpyObj('kafka.Producer', ['on']);
                Adapter = kafkaAdapter('localhost:2821', opts);
                instance = new Adapter({name: '/'});
            });

            it('should have a uid', function () {
                expect(instance.uid).toEqual(jasmine.any(String));
            });

            it('should have a kafka.Consumer', function () {
                expect(instance.consumer).toEqual(jasmine.any(kafka.Consumer));
            });

            it('should have a kafka.Producer', function () {
                delete opts.producer;
                Adapter = kafkaAdapter('localhost:2821', opts);
                instance = new Adapter({name: '/'});
                expect(instance.producer).toEqual(jasmine.any(kafka.Producer));
            });

            it('should default mainTopic to socket.io/', function () {
                expect(instance.mainTopic).toEqual('socket.io/');
            });

            it('should default createTopics to true', function () {
                expect(instance.options.createTopics).toEqual(true);
            });

            it('should expect the ready event on consumer', function () {
                expect(opts.producer.on)
                    .toHaveBeenCalledWith('ready', jasmine.any(Function));
            });
        });

        describe('producer ready', function () {
            beforeEach(function () {
                opts.consumer = jasmine.createSpyObj('kafka.Consumer', ['on']);
                Adapter = kafkaAdapter('localhost:2821', opts);
                instance = new Adapter({name: '/'});
                spyOn(instance, 'createTopic');
                spyOn(instance, 'subscribe');
                opts.producer.emit('ready');
            });

            it('should call createTopic', function () {
                expect(instance.createTopic)
                    .toHaveBeenCalledWith(instance.mainTopic);
            });

            it('should call subscribe', function () {
                expect(instance.subscribe)
                    .toHaveBeenCalledWith(instance.mainTopic);
            });
        });
    });

    describe('adapter.Kafka#onError', function () {
        beforeEach(function () {
            opts.producer = jasmine.createSpyObj('kafka.Producer', ['on']);
            Adapter = kafkaAdapter('localhost:2821', opts);
            instance = new Adapter({name: '/'});
            spyOn(instance, 'emit');
        });

        it('should not emit error if param is falsy', function () {
            instance.onError();
            expect(instance.emit).not.toHaveBeenCalled();
        });

        it('should emit error if param is truthy', function () {
            instance.onError('my error');
            expect(instance.emit).toHaveBeenCalled();
        });
    });

    describe('adapter.Kafka#onMessage', function () {
        var msg, kafkaMsg;
        beforeEach(function () {
            msg = ['abc', {message: 'test'}, {}];
            kafkaMsg = { value: JSON.stringify(msg) };

            Adapter = kafkaAdapter('localhost:2821', opts);
            instance = new Adapter({name: '/'});
            spyOn(instance, 'broadcast');
            spyOn(instance, 'onError');
        });

        it('should add a default nsp if none in packet', function () {
            msg[1].nsp = '/';
            instance.onMessage(kafkaMsg);
            expect(instance.broadcast)
                .toHaveBeenCalledWith(msg[1], msg[2], true);
        });

        it('should throw an error if param is not a valid JSON', function () {
            kafkaMsg.value = '{abcd}';
            instance.onMessage(kafkaMsg);
            expect(instance.onError).toHaveBeenCalled();
        });
    });

    describe('adapter.Kafka#safeTopicName', function () {
        beforeEach(function () {
            Adapter = kafkaAdapter('localhost:2821', opts);
            instance = new Adapter({name: '/'});
        });

        it('should remove the slashes from the channel name', function () {
            var name = instance.safeTopicName('socket.io/test');
            expect(name).toEqual('socket.io_test');
        });
    });

    describe('adapter.Kafka#createTopic', function () {
        beforeEach(function () {
            opts.producer = jasmine.createSpyObj('kafka.Producer', [
                'on', 'createTopics']);

            Adapter = kafkaAdapter('localhost:2821', opts);
            instance = new Adapter({name: '/'});
        });

        it('should create a new topic in kafka', function () {
            instance.createTopic('socket.io/test');
            expect(opts.producer.createTopics)
                .toHaveBeenCalledWith('socket.io_test', jasmine.any(Function));
        });

        it('should not create topic if createTopics is false', function () {
            opts.createTopics = false;
            instance.createTopic('socket.io/test');
            expect(opts.producer.createTopics).not.toHaveBeenCalled();
        });
    });

    describe('adapter.Kafka#subscribe', function () {
        beforeEach(function () {
            opts.consumer = jasmine.createSpyObj('kafka.Consumer', [
                'addTopics'
            ]);

            Adapter = kafkaAdapter('localhost:2821', opts);
            instance = new Adapter({name: '/'});
        });

        it('should add a new topic to the consumer', function () {
            instance.subscribe('socket.io/test');
            expect(opts.consumer.addTopics).toHaveBeenCalledWith([{
                topic: 'socket.io_test',
                partition: 0
            }], jasmine.any(Function));
        });
    });

    describe('adapter.Kafka#publish', function () {
        beforeEach(function () {
            opts.producer = jasmine.createSpyObj('kafka.Producer', [
                'on',
                'send'
            ]);
            Adapter = kafkaAdapter('localhost:2821', opts);
            instance = new Adapter({name: '/'});
        });

        it('should send a message to kafka', function () {
            var packet = {message: 'test'},
                msg = JSON.stringify([instance.uid, packet, {}]);

            instance.publish('socket.io/test', packet, {});
            expect(opts.producer.send).toHaveBeenCalledWith([{
                topic: 'socket.io_test',
                messages: [msg],
                attributes: 2
            }], jasmine.any(Function));
        });
    });

    describe('adapter.Kafka#broadcast', function () {
        beforeEach(function () {
            opts.producer = jasmine.createSpyObj('kafka.Producer', [
                'on',
                'send'
            ]);
            Adapter = kafkaAdapter('localhost:2821', opts);
            instance = new Adapter({name: '/'});
        });

        it('should add nsp key to the packet', function () {
            var packet = {message: 'test'};

            instance.broadcast(packet, {}, true);
            expect(packet.nsp).toEqual('/');
        });

        it('should send a message when remote is falsy', function () {
            var packet = {message: 'test', nsp: '/'},
                msg = JSON.stringify([instance.uid, packet, {}]);

            instance.broadcast(packet, {});
            expect(opts.producer.send).toHaveBeenCalledWith([{
                topic: 'socket.io_',
                messages: [msg],
                attributes: 2
            }], jasmine.any(Function));
        });

        it('should not send a message when remote is true', function () {
            var packet = {message: 'test'};

            instance.broadcast(packet, {}, true);
            expect(opts.producer.send).not.toHaveBeenCalled();
        });
    });

});