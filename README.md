A Go web application which stores a complex json document in a key-value store and supports CRUD operations. HTTP requests are authenticated using O Auth 2.0. The web app also supports complex queries and searching capabilities using Elastic Search and Kibana. 

    The key-value store used is DynamoDB
    The queue used for pub/sub is AWS SQS/ElasticMQ
    ElasticSearch is used and visualised using Kibana
    The web application stores a complex json document(nested JSON objects) as individual objects in the kv store to support querying based on types.
    All Services used in the project uses a local copy, for ex - AWS local dynamodb, AWS local SQS - ElasticMQ
    An approach to GraphQL is also visualised.
    All HTTP requests are authorised by O Auth 2.0 Implicit Grant Flow.
Sample Commands:

Create Table:
   
    aws dynamodb create-table \
    --table-name TableName \
    --attribute-definitions AttributeName=PrimaryKey,AttributeType=S \
    --key-schema AttributeName=PrimaryKey,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
    --endpoint-url http://localhost:8000


Put Item in table:

    aws dynamodb put-item --table-name PLAN_TABLE --item file://item.json --endpoint-url http://localhost:8000


Delete item:

    aws dynamodb delete-item --table-name PLAN_TABLE --key '{"objectId": {"S": "27283xvx9asdff-104"}}' --endpoint-url http://localhost:8000 --debug

GetItem:

    aws dynamodb get-item --table-name PLAN_TABLE --key '{"objectId": {"S": "27283xvx9asdff-104"}}' --endpoint-url http://localhost:8000 --debug

Describe Table:

    aws dynamodb describe-table --table-name PLAN_TABLE --endpoint-url http://localhost:8000

Delete table:

    aws dynamodb delete-table --table-name TableName --endpoint-url http://localhost:8000


List all tables in db:

    aws dynamodb list-tables --endpoint-url http://localhost:8000

Create a queue

    curl -X POST "http://localhost:9324/?Action=CreateQueue&QueueName=MyTestQueue

Recive fromm queue:

    aws --endpoint-url=http://localhost:9324 sqs receive-message --queue-url http://localhost:9324/000000000000/MyTestQueue-1

Delete a table

    aws dynamodb delete-table --table-name PLAN_TABLE --endpoint-url http://localhost:8000

dynamo db UI

    http://localhost:8001/tables/PLAN_TABLE

elasticMQ UI

    http://0.0.0.0:9325/

Check SQS queue

    aws --endpoint-url=http://localhost:9324 sqs receive-message --queue-url http://localhost:9324/000000000000/MyTestQueue-1
