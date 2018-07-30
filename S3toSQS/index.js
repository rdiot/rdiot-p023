var AWS = require("aws-sdk");
var sqs = new AWS.SQS();
var QUEUE_URL = 'https://sqs.ap-northeast-2.amazonaws.com/996409770277/jobQueue';

exports.handler = (event, context, callback) => {

   var sqsParams = {
      MessageBody: JSON.stringify(event),
      QueueUrl: QUEUE_URL
    };
    console.log(sqsParams)

    var sqsdata = sqs.sendMessage(sqsParams, function(err, data) {
      if (err) {
        console.log('ERR', err);
      }
      console.log(data);
      context.succeed('Exit');

    });
    console.log('message sent')
};