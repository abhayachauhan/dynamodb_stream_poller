var DynamoDBStream = require('./index.js');
fs = require('fs');

var file = 'shardData.json';

var streamArn = 'arn:aws:dynamodb:ap-southeast-2:047651431481:table/TestStreamDR/stream/2016-04-22T04:42:31.057';
var callback = function(err, data) {
    if (err) console.log('Error %s', err);
    console.log('Data: %s', JSON.stringify(data));
}

var stream;
var counter = 0;
var shards = {};

fs.readFile(file, 'utf8', function(err, data) {
    if (err) shards = {};
    else shards = JSON.parse(data);
    stream = new DynamoDBStream('TestDR', {}, streamArn, shards);

    stream.onRecord(function(tableName, shard, record, next) {
        var data = {
                tableName: tableName,
                shard: shard,
                record: record,
                next: next
            }
            // console.log('Data: %s', JSON.stringify(data));
        console.log('Received data: %s from Shard: %s', record.dynamodb.SequenceNumber, shard.ShardId);
        console.log(counter++);
        next();
    });

    stream.onShardUpdate(function(tableName, shard, seq, next) {
        shards[shard.ShardId] = {seq: seq };

        fs.writeFile(file, JSON.stringify(shards), function(err) {
            if (err) return console.log(err);
            next();
        });
    });

    stream.Run(callback);
});
