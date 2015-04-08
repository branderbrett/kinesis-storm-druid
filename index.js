'use strict';

var fs = require('fs');
var Schema = require('protobuf').Schema;
var kinesis = require('./lib/aws').kinesis;
var async = require('async');
var _ = require('lodash');

// "schema" contains all message types defined in buftest.proto|desc.
var eventSchema = new Schema(fs.readFileSync('./node_modules/ax.schemas/gen/desc/event_container.desc'));

// The "BufTest" message.
var EventContainer = eventSchema['com.bb.EventProto'];

var transaction = {
  type: 'TRANSACTION',
  transaction: {
    orderNumber: 'order 123',
    summary: {
      total: 10.10,
      promos: [
        {
          "description": "bogo bb",
          "id": "bb22",
          "value": 1000000
        }
      ]
    },
    buckets: [
      {
        products: [
          {
            "title": "product 1",
            "id": "123",
            "quantity": 2,
            "price": {
              "unitCost": 4000000,
              "totalCost": 8000000,
              "currency": "USD"
            }
          }
        ]
      }
    ]
  }
};

var proto = EventContainer.serialize(transaction);
console.log(typeof proto);
console.log('proto.length:', proto.length);
console.log('serialised:', proto);


fs.writeFileSync('./test.txt', proto.toString('base64') + '\n');
return;

proto = EventContainer.serialize(transaction);
fs.writeFileSync('./test.txt', proto);

var outOb = EventContainer.parse(proto);
console.log('unserialised:', JSON.stringify(outOb));

//async.waterfall([putRecord, retrieveRecord], function(error, results) {
//async.waterfall([putRecord], function(error, results) {
async.waterfall([], function(error, results) {
  if(error) {
    return console.error(error);
  }

  //console.info(JSON.stringify(results, null, ' '));
});

function putRecord(cb) {
  kinesis.putRecord({
    StreamName: 'ax-data',
    PartitionKey: '1', 
    Data: proto
  }, function(error, data) {
    if(error) {
      cb(error);
    }

    console.info(JSON.stringify(data, null, ' '));
    cb(null, data.SequenceNumber);
  });
}

function retrieveRecord(sequenceNumber, cb) {
  kinesis.getShardIterator({
    ShardId: 'shardId-000000000001', 
    ShardIteratorType: 'AT_SEQUENCE_NUMBER', 
    StartingSequenceNumber: sequenceNumber,
    StreamName: 'ax-data' 
  }, function(error, shardData) {
    if(error) {
      return cb(error);
    }

    console.info(JSON.stringify(shardData, null, ' '));

    console.log(shardData.ShardIterator);
    kinesis.getRecords({
      ShardIterator: shardData.ShardIterator
    }, function(error, recordData) {
      if(error) {
        return cb(error);
      }

      console.log('kinesis retrieved');
      _.each(recordData.Records, function(record) {
        var container = EventContainer.parse(record.Data);
        console.log(JSON.stringify(container, null, ' '));
      });

      cb(null, recordData);
    });
  });
}

