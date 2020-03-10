'use strict';

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const sns = new AWS.SNS();
const sqs = new AWS.SQS();

const doc = require('dynamodb-doc');
const dynamo = new doc.DynamoDB();

// #region DynamoDB

/**
 * Get an element from DynamoDB
 */
exports.dynamoDbGetItem = function (tablename, keyobject, callback) {
    const params = {
        TableName: tablename,
        Key: keyobject
    };

    // Get an element from the DynamoDB Table
    dynamo.getItem(params, (err, data) => {
        if (err) {
            callback(`Error: ${err}`);
        }
        if (data) {
            callback(null, data);
        }
    });
}

/**
 * Put an element into DynamoDB
 */
exports.dynamoDbPutItem = function (tablename, item, callback) {
    const params = {
        TableName: tablename,
        Item: item,
    };

    dynamo.putItem(params, (err, data) => {
        if (err) {
            callback(`Error: ${JSON.stringify(err, null, 2)}`);
        } else {
            callback(null, data);
        }
    });
}

/**
 * Query DynamoDB
 */
exports.dynamoDbQuery = function (tableName, attributes, callback) {
    const params = {
        TableName: tableName,
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
        KeyConditionExpression: null,
    };

    attributes.forEach(element => {
        switch (element.AttributeCondition) {
            case 'equals':
                if (params.KeyConditionExpression === null) {
                    params.KeyConditionExpression = `#${element.AttributeName} = :${element.AttributeValue}`;
                } else {
                    params.KeyConditionExpression += ` AND #${element.AttributeName} = :${element.AttributeValue}`;
                }
                break;

            case 'begins_with':
                if (params.KeyConditionExpression === null) {
                    params.KeyConditionExpression = `begins_with(#${element.AttributeName}, :${element.AttributeValue})`;
                } else {
                    params.KeyConditionExpression += ` AND begins_with(#${element.AttributeName}, :${element.AttributeValue})`;
                }
                break;

            default:
                break;
        }
    });

    dynamo.query(params, (err, data) => {
        if (err) {
            callback('Unable to scan DynamoDB Error:', JSON.stringify(err, null, 2));
        } else {
            callback(data.Items);
        }
    });
}

/**
 * Update element DyanmoDB
 */
exports.dynamoDbUpdateItem = function (tablename, keyobject, attributes, callback) {
    const params = {
        TableName: tablename,
        Key: keyobject,
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
        UpdateExpression: null,
    };

    Object.keys(attributes).forEach(key => {
        params.ExpressionAttributeNames[`#${key}`] = key;
        params.ExpressionAttributeValues[`:${key}`] = attributes[key];
        if (params.UpdateExpression === null) {
            params.UpdateExpression = `set #${key} = :${key}`;
        } else {
            params.UpdateExpression += `, #${key} = :${key}`;
        }
    });

    // Cerca elemento se presente su DyanomoDB
    dynamo.updateItem(params, (err, data) => {
        if (err) {
            callback(`Error read DynamoDB operation: "${err.message}"`);
        }
        if (data) {
            callback(null, 'Successful update Item');
        }
    });
}

//#endregion

// #region S3

/**
 * Get metadata from S3
 */
exports.s3HeadElement = function (keyname, bucket, etag, callback) {
    s3.headObject({
        Bucket: bucket,
        Key: keyname,
    }, (headErr, headData) => {
        if (headErr) {
            callback(`Error: head S3 operation: "${headErr.message}"`);
        } else if (headData.ETag && headData.ETag === etag) {
            callback(null, headData);
        } else {
            callback('Error: ETag not equal');
        }
    });
}

/**
 * Read element from S3
 */
exports.s3ReadElement = function (keyname, bucket, s3response, callback) {
    const params = {
        Bucket: bucket,
        Key: keyname,
    };
    s3.getObject(params, (err, data) => {
        if (err) callback(`Error read S3 operation: "${err.message}"`);
        else if (s3response) {
            callback(null, data);
        } else {
            callback(null, JSON.parse(data.Body.toString()));
        }
    });
}

/**
 * Save JSON element from S3
 */
exports.s3PutJson = function (json, keyname, bucket, callback) {
    s3.putObject({
        Bucket: bucket,
        Key: keyname,
        Body: JSON.stringify(json, null, 2),
        ContentType: 'application/json',
    }, callback);
}


//#endregion

// #region SQS

/**
 * Send a series SQS
 */
exports.ricorsiveSqsSend = function (eventsCollection, timeout, callback) {
    (function updateOneRicorsive() {
        var event = eventsCollection.splice(0, 1)[0];

        sqsSend(event);

        if (eventsCollection.length === 0) {
            callback(null, "Finish push to SQS");
        } else {
            setTimeout(updateOneRicorsive, timeout);
        }
    })();
}

/**
 * Send a request to SQS
 */
exports.sqsSend = function (event, attributes, queueurl, callback) {
    var params = {
        DelaySeconds: 0,
        MessageAttributes: attributes,
        MessageBody: JSON.stringify(event),
        QueueUrl: queueurl
    };

    sqs.sendMessage(params, function (err, data) {
        if (err) {
            callback(`Error: ${JSON.stringify(err, null, 2)}`);
        } else {
            console.log(null, data.MessageId);
        }
    });
}

//#endregion

// #region SNS

exports.checktags = function (topicarn, item, callback) {
    sns.publish({
        TopicArn: topicarn,
        Message: JSON.stringify(item),
    }, callback);
}

//#endregion

// #region Utility

/**
 * Function that create a new randomGUID
 */
exports.createNewGuid = function () {
    function s4() {
        return Math.floor((1 + Math.random()) * 0x10000)
            .toString(16)
            .substring(1);
    }

    return `${s4() + s4()}-${s4()}-${s4()}-${
        s4()}-${s4()}${s4()}${s4()}`;
}

/**
 * Function that group by an array
 */
exports.groupBy = function (xs, key) {
    return xs.reduce((rv, x) => {
        (rv[x[key]] = rv[x[key]] || []).push(x);
        return rv;
    }, {});
}

/**
 * Function that group by an array
 */
exports.roughSizeOfObject = function (object) {

    var objectList = [];
    var stack = [object];
    var bytes = 0;

    while (stack.length) {
        var value = stack.pop();

        if (typeof value === 'boolean') {
            bytes += 4;
        } else if (typeof value === 'string') {
            bytes += value.length * 2;
        } else if (typeof value === 'number') {
            bytes += 8;
        } else if
        (
            typeof value === 'object'
            && objectList.indexOf(value) === -1
        ) {
            objectList.push(value);

            for (var i in value) {
                stack.push(value[i]);
            }
        }
    }
    return bytes / 1024;
}


/**
 * Function log for Lambda
 */
exports._log = function (caption, object) {
    console.log(caption + JSON.stringify(object, true, '  '));
}
/**
 * Function error 400 for Lambda
 */
exports._error = function (caption, err, callback) {
    console.error(caption);
    var error = {
        message: caption,
        error: err
    };
    // The output from a Lambda proxy integration must be
    // of the following JSON object. The 'headers' property
    // is for custom response headers in addition to standard
    // ones. The 'body' property  must be a JSON string. For
    // base64-encoded payload, you must also set the 'isBase64Encoded'
    // property to 'true'.
    let response = {
        statusCode: 400,
        body: JSON.stringify(error),
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    };
    callback(null, response);

}
/**
 * Function done 200 for Lambda
 */
exports._done = function (dataObj, callback) {
    console.info(dataObj);
    // The output from a Lambda proxy integration must be
    // of the following JSON object. The 'headers' property
    // is for custom response headers in addition to standard
    // ones. The 'body' property  must be a JSON string. For
    // base64-encoded payload, you must also set the 'isBase64Encoded'
    // property to 'true'.
    var contentType = 'application/json';

    var result = {
        message: dataObj
    };

    let response = {
        statusCode: 200,
        body: typeof dataObj === 'object' ? JSON.stringify(dataObj) : JSON.stringify(result),
        headers: {
            'Content-Type': contentType,
            'Access-Control-Allow-Origin': '*'
        }
    };
    callback(null, response);

}

//#endregion
