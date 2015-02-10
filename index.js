'use strict';

var fs = require('fs');
var Schema = require('protobuf').Schema;
var kinesis = require('./lib/aws').kinesis;
var async = require('async');
var _ = require('lodash');

// "schema" contains all message types defined in buftest.proto|desc.
var eventSchema = new Schema(fs.readFileSync('./models/desc/eventContainer.desc'));

// The "BufTest" message.
var EventContainer = eventSchema['com.nicktate.EventContainer'];

var transaction = {
  type: 'TRANSACTION',
  transaction: {
    orderNumber: 'order 123',
    total: 10.10
  }
};

var pageview = {
  type: 'PAGEVIEW',
  email: 'test@example.com',
  timestamp: 1522458478197,
  pageview: {
    url: 'http://www.ae.com/product/123',
    referrer: 'http://www.google.com/my/search#cool?why=now'
  }
};

var proto = EventContainer.serialize(pageview);
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

