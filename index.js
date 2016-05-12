//==============================
//SETUP
//==============================

const POLL_DELAY_TIME_QUICK = 200;
const POLL_DELAY_TIME_SLOW  = 500;
const POLL_DELAY_TIME_RESTART = 10000;
const POLL_HEARTBEAT_TIME = 60000;

// Libraries
var fs = require('fs'),
    AWS = require('aws-sdk'),
    async = require('async');



//==============================
//CLASSES
//==============================

/* The class to be exported
** Params:
**     config: table containing accessKeyId, secretAccessKey, region (table)
**     streamArn: the ARN for the stream wanting to be read (string)
**     previousShards: table containing shards and their sequence numbers; key: shardId, val: sequenceNumber (table)
*/
//::::::::::::::::::::::::::::::
module.exports = DynamoDBStream

function DynamoDBStream(tableName, config, streamArn, previousShards) {

    // Set AWS Config
    //var creds = new AWS.Credentials(config.accessKeyId, config.secretAccessKey);

    // Initialise DynamoDB Object and DynamoDB Stream Object
    this._tableName = tableName;
    this._dynamodbstreams = new AWS.DynamoDBStreams();

    // Attributes
    this._streamArn = streamArn;
    this._shardSequenceNumbers = (typeof previousShards == "object") ? previousShards : {};

    // event handlers
    this._onRecord = null;        // To be called whenever a record is received
    this._onShardUpdate = null;   // To be called whenever a shard record needs updating

    // default limit, run forever
    this._endTime = null;
    this._heartBeatTime = null;
	this._heartBeatTimer = setInterval(this._checkHeartBeat.bind(this), 1000);

}
//::::::::::::::::::::::::::::::


//==============================
// PUBLIC METHODS
//==============================


//::::::::::::::::::::::::::::::
// Function that sets the "_onRecord" callback
DynamoDBStream.prototype.onRecord = function(callback) {
    this._onRecord = callback;
}
//::::::::::::::::::::::::::::::


//::::::::::::::::::::::::::::::
// Function that sets the "_onShardUpdate" callback
DynamoDBStream.prototype.onShardUpdate = function(callback) {
    this._onShardUpdate = callback;
}
//::::::::::::::::::::::::::::::


//::::::::::::::::::::::::::::::
// The main function which executes the other functions
DynamoDBStream.prototype.Run = function(end_time, finished) {

    // Set end time for loop with a 10% wiggle
    if (typeof end_time == "function") {
        finished = end_time;
        end_time = undefined;
    } 
	this._endTime = end_time || null;

    // Make sure we have default handlers for all the callbacks
    if (!this._onRecord) {
        this._onRecord = function(tableName, shard, record, next) { if (next) next() }
    }
    if (!this._onShardUpdate) {
        this._onShardUpdate = function(tableName, shard, seq, next) { if (next) next() }
    }

    this._startShards(function(err) {
		// Stop the heartbeart timer
		if (this._heartBeatTimer) clearInterval(this._heartBeatTimer);
		finished(err);
	}.bind(this));

	return this;

}
//::::::::::::::::::::::::::::::


//==============================
// PRIVATE METHODS
//==============================


//::::::::::::::::::::::::::::::
// Check for new shards in the stream
DynamoDBStream.prototype._startShards = function(parentId, callback) {

    if (typeof parentId == "function") {
        callback = parentId;
        parentId = null;
    }

    // Get all the shards
    this._getShards(function(err, shards) {

        if (err) return callback(err);

        // Loop through all the shards and see if there are any new shards for this parent shard
        async.each(shards, 

            //..............................
            // Function that iterates over each shard
            function _forEachShard(shard, next) {

                if (parentId == null || (shard.ParentShardId && shard.ParentShardId == parentId)) {
                    // We have a matching shard
                    this._pollShard(shard, next);
                } else {
					// console.log("---- filtered:", "Table:", this._tableName, "Shard:", shard.ShardId, "Parent:", shard.ParentShardId, "Filter:", parentId, "####")
                    next();
                }

            }.bind(this),

            //..............................
            // When all the shards are finished, check if we are all done
            function _noMoreShards(err) {

                // If we are finish polling all shards, then we are done with this stream
                callback(err);

            }.bind(this)

        );

        // We have finished looking for new shards
    }.bind(this));

}
//::::::::::::::::::::::::::::::



//::::::::::::::::::::::::::::::
// Get all the shards in the stream
DynamoDBStream.prototype._getShards = function(callback) {

    // Params for describeStream function
    var params = {
        StreamArn: this._streamArn
    };

    // Get list of Shards and Shard iterators
    this._dynamodbstreams.describeStream(params, function(err, data) {

        // List of all shards in an array
        if (data && "StreamDescription" in data && "Shards" in data.StreamDescription) {
			this._beatHeart();
            callback(err, data.StreamDescription.Shards);
        } else {
            callback(err);
        }

    }.bind(this));

}
//::::::::::::::::::::::::::::::


//::::::::::::::::::::::::::::::
// Get the iterator for a shard
DynamoDBStream.prototype._getIterator = function(shard, callback) {

    // If the Shard is already in this._shardSequenceNumbers, generate iterator from the SequenceNumber
    // else generate the iterator using ShardIteratorType: 'TRIM_HORIZON'
    var params = {
        ShardId: shard.ShardId,
        StreamArn: this._streamArn
    };

    var seq = null;
    if (shard.ShardId in this._shardSequenceNumbers) {
        seq = this._shardSequenceNumbers[shard.ShardId];
    }

    if (seq == null || seq == "new" || seq == "closed") {

        // Set params to for getting shard iterator
        params.ShardIteratorType = 'TRIM_HORIZON';
        this._shardSequenceNumbers[shard.ShardId] = "new";

    } else {
        // Set params for generating shard iterator
        params.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
        params.SequenceNumber = this._shardSequenceNumbers[shard.ShardId];

    }

    this._dynamodbstreams.getShardIterator(params, function(err, data) {

        if (data && "ShardIterator" in data) {
			this._beatHeart();
            callback(err, data.ShardIterator);
        } else {
            callback(err);
        }

    }.bind(this));

}
//::::::::::::::::::::::::::::::


//::::::::::::::::::::::::::::::
// Get all the new records for a shard
DynamoDBStream.prototype._getRecords = function(iterator, callback) {

    var params = {
        ShardIterator: iterator
    };

    // Get Records for the shard
    this._dynamodbstreams.getRecords(params, function(err, data) {

        if (data) {
			this._beatHeart();
            var records = ("Records" in data) ? data.Records : null;
            var nextIterator = ("NextShardIterator" in data) ? data.NextShardIterator : null;
            callback(err, records, nextIterator);
        } else {
            callback(err);
        }

    }.bind(this));

}
//::::::::::::::::::::::::::::::


//::::::::::::::::::::::::::::::
// Polls the shard until the end of time.
DynamoDBStream.prototype._pollShard = function(shard, next) {

    console.log('Polling shard: %s', JSON.stringify(shard));

    // Ignore closed shards
    if (shard.ShardId in this._shardSequenceNumbers && this._shardSequenceNumbers[shard.ShardId] == "closed") {
        console.log("---- closed shard:", "Table:", this._tableName, "Shard:", shard.ShardId, "Parent:", shard.ParentShardId, "####")
        return next();
    }

    // Does this shard have an active parent?
    if (shard.ParentShardId && shard.ParentShardId in this._shardSequenceNumbers && this._shardSequenceNumbers[shard.ParentShardId] != "closed") {
        console.log("---- active parent:", "Table:", this._tableName, "Shard:", shard.ShardId, "Parent:", shard.ParentShardId, "####")
        return next();
    }

    if (!(shard.ShardId in this._shardSequenceNumbers) || this._shardSequenceNumbers[shard.ShardId] == "new") {

        // This is a new shard
        console.log("++++ new shard:", "Table:", this._tableName, "Shard:", shard.ShardId, "Parent:", shard.ParentShardId, "####")
        this._shardSequenceNumbers[shard.ShardId] = "new";
        this._onShardUpdate(this._tableName, shard, "new", function() {
            this._doPollShard(shard, next);
        }.bind(this));

    } 
    else {

        // This is an existing shard
        console.log("++++ restart shard:", "Table:", this._tableName, "Shard:", shard.ShardId, "Parent:", shard.ParentShardId, "####")
        this._doPollShard(shard, next);

    }


}
//::::::::::::::::::::::::::::::


//::::::::::::::::::::::::::::::
// Polls the shard until the end of time.
DynamoDBStream.prototype._doPollShard = function(shard, next) {

    // Get the initial state of the iterator
    this._getIterator(shard, function(err, iterator) {

        if (err) return next(err);

        // KEep looping until the iterator runs dry
        async.doWhilst(

            //..............................
            // With each iterators grab all the records
            function _whileValidIterator(callback) {

                this._getRecords(iterator, function(err, records, nextIterator) {
                    
                    if (err) console.log('OMG ERROR %s', JSON.stringify(err));

                    console.log('Checking shard %s', shard.ShardId);

                    // Store the next iterator if there is one
                    iterator = nextIterator;

                    var has_recorded_event = false;

                    // Process each record in order
                    async.eachSeries(records, 

                        //..............................
                        // Iterate over each record
                        function _recordEvent(record, next) {

                            has_recorded_event = true;

                            var cb = function() {
                                this._shardSequenceNumbers[shard.ShardId] = record.dynamodb.SequenceNumber;
                                this._onShardUpdate(this._tableName, shard, record.dynamodb.SequenceNumber, next);
                            }.bind(this);

                            this._onRecord(this._tableName, shard, record, cb);

                        }.bind(this),

                        //..............................
                        // All records have been drained from this iterator
                        function _recordCallback(err) {

                            // Delay the starting of a new iterator for a moment (reduces load)
                            setTimeout(function() {
                                callback(err);
                            }.bind(this), has_recorded_event ? POLL_DELAY_TIME_QUICK : POLL_DELAY_TIME_SLOW)

                        }.bind(this)
                    )

                }.bind(this));
            }.bind(this),

            //..............................
            // Check if there are any more iterators to process ... or if we have run out of time
            function _whileValidIteratorTest() {
                if (this._endTime == null || new Date().getTime() < this._endTime) {
                    // We still have time, has the shard finished?
                    return !(iterator == null || iterator == undefined);
                } else {
                    // Timeout
                    return false;
                }
            }.bind(this),

            //..............................
            // All finished with this shard.
            function _whenIteratorRunsDry(err) {
                
                // We are finished polling this shard
                if (iterator == null || iterator == undefined) {
                    // This shard has closed
                    this._shardSequenceNumbers[shard.ShardId] = "closed";
                    this._onShardUpdate(this._tableName, shard, "closed", function() {

                        // Check for more shards after a short delay
						setTimeout(function() {
							this._startShards(shard.ShardId, next);                        
						}.bind(this), POLL_DELAY_TIME_RESTART);

                    }.bind(this));
                } else {
                    // This process has timed out
                    next(err);                    
                }

            }.bind(this)

        )
    
    }.bind(this))

}

//::::::::::::::::::::::::::::::
// Beat the heart (it's alive)
DynamoDBStream.prototype._beatHeart = function() {
    this._heartBeatTime = new Date().getTime() + POLL_HEARTBEAT_TIME;
}

//::::::::::::::::::::::::::::::
// Check the heart (is it alive?)
DynamoDBStream.prototype._checkHeartBeat = function() {
	if (this._heartBeatTime != null && new Date().getTime() > this._heartBeatTime) {
		console.error(new Date(), "ERROR", "HEART ATTACK!", this._tableName);
		process.exit();
	}
}

//::::::::::::::::::::::::::::::
