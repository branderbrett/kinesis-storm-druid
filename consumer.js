'use strict';

var fs = require('fs');
var path = require('path');
var Schema = require('protobuf').Schema;
var kinesis = require('./lib/aws').kinesis;
var async = require('async');
var _ = require('lodash');

var eventContainerSchema = new Schema(fs.readFileSync(path.resolve(
    __dirname, '.', 'node_modules', 'ax.schemas', 'gen', 'desc', 'event_container.desc'
)));
var EventContainer = eventContainerSchema['com.bb.EventProto'];

var STREAM = 'ax-data-sandbox';

function describeStream(cb) {
  kinesis.describeStream({
    StreamName: STREAM,
    Limit: 2
  }, function(error, data) {
    if(error) {
      return cb(error);
    }

    //console.info(JSON.stringify(data, null, ' '));

    _.each(data.StreamDescription.Shards, function(shard) {
      // set initial checkpoints
      shardCheckpoint[shard.ShardId] = shard.SequenceNumberRange.StartingSequenceNumber;

      kinesis.getShardIterator({
        ShardId: shard.ShardId,
        ShardIteratorType: 'AFTER_SEQUENCE_NUMBER', //'AT_SEQUENCE_NUMBER',
        StreamName: STREAM,
        StartingSequenceNumber: shardCheckpoint[shard.ShardId]
      }, function(error, iterator) {
        if(error) {
          return cb(error);
        }

        getRecords(shard.ShardId, iterator.ShardIterator);
        cb();
      });
    });

  });
}

describeStream(function(error) {
 if(error) {
   console.error(error); 
 }
});

var shardCheckpoint = {};

function getRecords(id, iterator) {
  kinesis.getRecords({
    ShardIterator: iterator,
    Limit: 25
  }, function(error, data) {
    if(error) {
      console.error(error);
    }

    _.each(data.Records, function(record) {
      shardCheckpoint[id] = record.SequenceNumber;
      var container = EventContainer.parse(record.Data);
      //console.log(record);
       console.log(id + ': ' + JSON.stringify(container, null, ' '));
       fs.writeFileSync(path.join(__dirname, 'protos.txt'), record.Data, {flag: 'a'});
       fs.writeFileSync(path.join(__dirname, 'protos-base64.txt'), record.Data.toString('base64') + '\n', {flag: 'a'});
    });

    if(!data.NextShardIterator) {
      setTimeout(getRecords, 2500, id, iterator);
    } else {
      getRecords(id, data.NextShardIterator);
    }
  });
}
