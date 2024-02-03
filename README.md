#AWS CLI

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








// },
    // "creationDate": {
    //     "S": "12-12-2017"
    // }


Tasks:
The delete should return 204 -> No content and then 404(WITH respwct to REDDIS)
Creation Time
Modularise