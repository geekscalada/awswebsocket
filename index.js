import { ApiGatewayManagementApiClient, PostToConnectionCommand } from "@aws-sdk/client-apigatewaymanagementapi";
import { DynamoDBClient, PutItemCommand, GetItemCommand, DeleteItemCommand, ScanCommand } from "@aws-sdk/client-dynamodb";

const dynamoDbClient = new DynamoDBClient({ region: 'us-west-2' });
const connectionsTable = 'connectionsTableLimpia';

export const handler = async (event) => {
    try {

        console.log('GiantEvent:', JSON.stringify(event, null, 2));

        if (!event.requestContext && !event.Records) {
            console.error('Event does not contain requestContext. This may be a direct invocation.');
            return { statusCode: 400, body: 'Invalid invocation context.' };
        }

        let dynamoDBMessage = "";
        let connectionId = "";
        let agentId = "";
        let routeKey = "";
        let endPointUrl = "";
        let stage = "";
        let domainName = "";

        if (event.Records) {
            for (const record of event.Records) {
                console.log('DynamoDB Record:', JSON.stringify(record, null, 2));

                if (record.eventName === 'INSERT') {
                    const newRecord = record.dynamodb.NewImage;
                    dynamoDBMessage = newRecord.message.S;
                    console.log(`Mensaje recibido: ${dynamoDBMessage}`);
                }
            }
            //return { statusCode: 200, body: 'Stream event processed.' };
        }
       

        if (event.requestContext && event.requestContext.connectionId) {
            connectionId = event.requestContext.connectionId;

            const queryParams = event.queryStringParameters;
            if (queryParams && queryParams.agentId) {
                agentId = queryParams.agentId;
            } else {
                return { statusCode: 400, body: 'Missing agentId in query parameters.' };
            }

            routeKey = event.requestContext.routeKey;
            domainName = event.requestContext.domainName;
            stage = event.requestContext.stage;
            endPointUrl = `https://${domainName}/${stage}`;

            if (!connectionId || !domainName || !stage) {
                console.error('Missing required properties from requestContext.');
                return { statusCode: 400, body: 'Invalid event structure.' };
            }
        }

        


        const sendMessage = async (connectionId, message) => {

            const apiClient = new ApiGatewayManagementApiClient({ endpoint: endPointUrl });

            try {
                const command = new PostToConnectionCommand({
                    ConnectionId: connectionId,
                    Data: Buffer.from(JSON.stringify(message)),
                });
                await apiClient.send(command);
            } catch (error) {
                console.error('Error sending message:', error);
                throw error;
            }
        };

        if (event.Records) {
            for (const record of event.Records) {
                console.log('DynamoDB Record:', JSON.stringify(record, null, 2));
                

                if (record.eventName === 'INSERT') {
                    const newRecord = record.dynamodb.NewImage;
                    dynamoDBMessage = newRecord.message.S;
                    let agentId = newRecord.agentId.S;

                    console.log(`Mensaje recibido: ${dynamoDBMessage} para el agente ${agentId}`);

                    try {
                        const result = await dynamoDbClient.send(new ScanCommand({
                            TableName: connectionsTable,
                            FilterExpression: 'agentId = :agentId',
                            ExpressionAttributeValues: { ':agentId': { S: agentId } }
                        }));

                        if (!result.Items || result.Items.length === 0) {
                            console.log(`No se encontró un connectionId para el agentId: ${agentId}`);
                            return null;
                        }
                        
                        console.log("ConnectionTableData", result.Items[0]);

                        connectionId = result.Items[0].connectionId.S
                        endPointUrl = result.Items[0].endPointUrl.S

                        
                    } catch (error) {
                        console.error('Error buscando connectionId en DynamoDB:', error);
                        return { statusCode: 400, body: 'Unhandled route.' };
                    }

                    await sendMessage(connectionId, {
                        action: 'echo',
                        message: dynamoDBMessage,
                        timestamp: new Date().toISOString()
                    });
                }
            }
            return { statusCode: 200, body: 'Stream event processed.' };
        }

        switch (routeKey) {
            case '$connect':
                try {
                    await dynamoDbClient.send(new PutItemCommand({
                        TableName: connectionsTable,
                        Item: {
                            connectionId: { S: connectionId },
                            agentId: { S: agentId },
                            endPointUrl: { S: endPointUrl }
                        }
                    }));
                    return { statusCode: 200, body: 'Connected.' };
                } catch (error) {
                    console.error('Error saving connection to DynamoDB:', error);
                    return { statusCode: 500, body: 'Error saving connection.' };
                }

            case '$disconnect':
                return { statusCode: 200, body: 'Disconnected.' };

            case 'echo':
                try {
                    const body = JSON.parse(event.body);
                    await sendMessage(connectionId, {
                        action: 'echo',
                        message: dynamoDBMessage,
                        timestamp: new Date().toISOString()
                    });
                    return { statusCode: 200, body: 'Echo message sent.' };
                } catch (error) {
                    console.error('Error processing echo message:', error);
                    return { statusCode: 400, body: 'Invalid message format.' };
                }

            case '$default':
                try {
                    const body = JSON.parse(event.body);
                    await sendMessage(connectionId, {
                        action: 'default',
                        message: 'Received your message on the default route',
                        timestamp: new Date().toISOString()
                    });
                    return { statusCode: 200, body: 'Default message processed.' };
                } catch (error) {
                    console.error('Error processing default message:', error);
                    return { statusCode: 400, body: 'Invalid message format.' };
                }

            default:
                return { statusCode: 400, body: 'Unhandled route.' };
        }
    } catch (error) {
        console.error('Unhandled error in handler:', error);
        return { statusCode: 500, body: 'Internal Server Error' };
    }
};
