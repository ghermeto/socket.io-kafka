# socket.io-kafka

## How to use

```node
var io = require('socket.io')(3000);
var kafka = require('socket.io-kafka');
io.adapter(kafka('localhost:2181'));
```

By running [socket.io](http://socket.io/) with `socket.io-kafka` you can scale
your socket.io app horizontally using multiple ports or servers.

This adapter aims to be an alternative to the
[socket.io-redis adapter](https://www.npmjs.com/package/socket.io-redis) which
allow multiple socket.io instances to communicate using
[Kafka](http://kafka.apache.org/) instead of Redis.

### Why Kafka?

Kafka is a very fast, scalable, distributed message bus and was designed to
handle a huge amount of data with low latency. It was originally developed by
LinkedIn and is currently part of the Apache Project.

As Kafka provides built-in partitioning, replication, and fault-tolerance,
it feels like a natural choice to build scalable socket.io applications with
high throughput or which will require a great number of instances talking
amongst themselves.

## API

### adapter(uri[, opts])

`uri` is a zookeper connection string like `localhost:2821` where your
zookeeper cluster is located.

The adapater uses the [kafka-node](https://www.npmjs.com/package/kafka-node)
and accepts a comma separated `host:port` pairs, each represents a zookeeper
server.

For a list of options see below.

### adapter(opts)

The following options are allowed:

- `key`: the name of the key prefix the kafka topic (`socket.io`)
- `host`: zookeeper host
- `port`: zookeeper port
- `uri`: substitute for `host` and `port`, zookeeper connection string
- `consumer`: optional, a [kafka.Consumer](https://www.npmjs.com/package/kafka-node#consumer) instance
- `producer`: optional, a [kafka.Producer](https://www.npmjs.com/package/kafka-node#producer) instance
- `createTopics`: optional, if we should try to create a new Kafka topic (`true`)
- `partition`: partition to read and write to (`0`)

## Running tests

`npm test` will run `jasmine` with `istanbul` code coverage. The command expects
the modules to be installed as global.

## TODO

- add CI (crrently code is linted, tested and coverage is above 85%)
- read from multiple partitions
- allow configuration to set consumer options
- allow configuration to set compression (currently snappy is hardcoded)
- benchmark (find out real world limits)

## License

[MIT](http://opensource.org/licenses/MIT)