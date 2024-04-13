package queue

import (
	"bytes"
	"context"
	"crud_with_dynamodb/connector"
	"crud_with_dynamodb/models"
	"io/ioutil"
	"strings"
	"time"

	// "crud_with_dynamodb/queue"
	"encoding/json"
	"fmt"
	"log"

	// "github.com/elastic/go-elasticsearch"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// type ElasticsearchDocument struct {
// 	Org          string            `json:"org"`
// 	ObjectId     string            `json:"objectId"`
// 	ObjectType   string            `json:"objectType"`
// 	PlanType     string            `json:"planType,omitempty"`
// 	CreationDate string            `json:"creationDate,omitempty"`
// 	Name         string            `json:"name,omitempty"`
// 	Deductible   int               `json:"deductible"`
// 	Copay        int               `json:"copay"`
// 	MyJoinField  map[string]string `json:"my_join_field"`
// }

// Initialize the Elasticsearch client
func getElasticsearchClient() (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating the Elasticsearch client: %w", err)
	}
	return es, nil
}

func setupElasticsearchIndex(esClient *elasticsearch.Client) error {
	ctx := context.Background()
	indexName := "my-index-000007"

	// Define the mapping
	mapping := `{
        "mappings": {
            "properties": {
                "org": { "type": "keyword" },
                "objectId": { "type": "keyword" },
                "objectType": { "type": "keyword" },
                "planType": { "type": "keyword" },
                "creationDate": { "type": "date", "format": "dd-MM-yyyy" },
                "my_join_field": { 
                    "type": "join",
                    "relations": {
                        "plan": ["planCostShares", "linkedPlanServices"],
                        "linkedPlanServices": ["linkedService", "planserviceCostShares"]
                    }
                }
            }
        }
    }`

	// Check if index already exists
	exists, err := esClient.Indices.Exists([]string{indexName})
	if err != nil {
		return fmt.Errorf("error checking if index exists: %w", err)
	}

	if exists.StatusCode == 200 {
		fmt.Println("Index already exists, skipping creation.")
		return nil
	}

	// Create the index with the specified mapping
	createIndexResponse, err := esClient.Indices.Create(
		indexName,
		esClient.Indices.Create.WithContext(ctx),
		esClient.Indices.Create.WithBody(strings.NewReader(mapping)),
	)

	if err != nil {
		return fmt.Errorf("error creating the index: %w", err)
	}
	defer createIndexResponse.Body.Close()

	if createIndexResponse.IsError() {
		responseBody, _ := ioutil.ReadAll(createIndexResponse.Body)
		return fmt.Errorf("error response from Elasticsearch: %s", string(responseBody))
	}

	fmt.Println("Index created successfully")
	return nil
}

func PollMessagesAndInsertToElasticsearch() {
	// Initialize Elasticsearch client
	esClient, err := getElasticsearchClient()
	if err != nil {
		log.Fatalf("Error setting up Elasticsearch client: %v", err)
	}

	go setupElasticsearchIndex(esClient)

	// Your existing AWS SQS configuration code...
	// // ElasticMQ endpoint and dummy credentials
	// endpoint := "http://localhost:9324"
	// region := "elasticmq"
	// accessKey := "dummy"
	// secretKey := "dummy"

	// // Load the AWS SDK configuration
	// cfg, err := config.LoadDefaultConfig(context.TODO(),
	// 	config.WithRegion(region),
	// 	config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	// 	config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
	// 		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
	// 			return aws.Endpoint{
	// 				URL:           endpoint,
	// 				SigningRegion: region,
	// 			}, nil
	// 		},
	// 	)),
	// 	config.WithClientLogMode(aws.LogSigning|aws.LogRequestWithBody),
	// )
	// if err != nil {
	// 	log.Fatalf("Unable to load SDK config, %v", err)
	// }

	sqsClient := connector.ConnectElasticSQS()
	queueUrl := "http://localhost:9324/000000000000/MyTestQueue-1" // Your queue URL

	for {
		// Receive messages from SQS
		msgOutput, err := sqsClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueUrl),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			log.Printf("Failed to fetch SQS message: %v", err)
			continue
		}

		// if err := processAndInsertPlan(esClient, msgOutput.Messages); err != nil {
		// 	log.Fatalf("Error processing and inserting plan: %v", err)
		// }

		for _, message := range msgOutput.Messages {
			// Extract the Plan from the message
			// var plan models.Plan
			fmt.Printf("Message Body: %s\n", *message.Body)
			// if err := json.Unmarshal([]byte(*message.Body), &plan); err != nil {
			// 	log.Printf("Failed to unmarshal plan from message: %v", err)
			// 	continue
			// }

			// Pass the plan to the processAndInsertPlan function
			if err := processAndInsertPlan(esClient, message); err != nil {
				log.Fatalf("Error processing and inserting plan: %v", err)
				continue
			}

			log.Printf("inserted message into elastic db")

			// Delete the message from SQS queue after successful processing
			_, err := sqsClient.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueUrl),
				ReceiptHandle: message.ReceiptHandle,
			})
			if err != nil {
				log.Printf("Failed to delete message from SQS: %v", err)
			}
			log.Printf("deleted message from sqs queue")
		}

		// Process each message
		// for _, message := range msgOutput.Messages {
		// 	fmt.Printf("Message Body: %s\n", *message.Body) // Ensure message.Body is not nil before dereferencing
		// 	// Insert message into Elasticsearch
		// 	if err := processAndInsertMessage(esClient, message); err != nil {
		// 		log.Printf("Failed to insert message into Elasticsearch: %v", err)
		// 		continue // Skip deleting the message if there was an error processing it
		// 	}
		// 	log.Printf("inserted message into elastic db")

		// 	// Delete the message from SQS queue after successful processing
		// 	_, err := sqsClient.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
		// 		QueueUrl:      aws.String(queueUrl),
		// 		ReceiptHandle: message.ReceiptHandle,
		// 	})
		// 	if err != nil {
		// 		log.Printf("Failed to delete message from SQS: %v", err)
		// 	}
		// 	log.Printf("deleted message from sqs queue")
		// }
		// for _, msg := range msgOutput.Messages {
		// 	processedDocs := make(map[string]*Document)
		// 	var data map[string]interface{}
		// 	json.Unmarshal([]byte(*msg.Body), &data)
		// 	splitDocuments("", data, processedDocs)

		// 	for _, doc := range processedDocs {
		// 		indexDocument(esClient, "my-index-000007", doc)
		// 	}

		// 	// Acknowledge the message as processed
		// 	_, err := sqsClient.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
		// 		QueueUrl:      aws.String(queueUrl),
		// 		ReceiptHandle: msg.ReceiptHandle,
		// 	})
		// 	if err != nil {
		// 		log.Printf("Failed to delete message: %v", err)
		// 	}
		// }

		// time.Sleep(1 * time.Second)
		// }

		time.Sleep(5 * time.Second) // Wait before polling again
	}
}

// // func processAndInsertMessage(esClient *elasticsearch.Client, message types.Message) error {
// // 	if message.Body == nil {
// // 		return fmt.Errorf("message body is nil")
// // 	}

// // 	var msgBody Message
// // 	if err := json.Unmarshal([]byte(*message.Body), &msgBody); err != nil {
// // 		return fmt.Errorf("error unmarshalling message body: %w", err)
// // 	}

// // 	fmt.Printf("Operation: %s\n", msgBody.Operation)

// // 	var plan models.Plan
// // 	if err := json.Unmarshal(msgBody.Data, &plan); err != nil {
// // 		return fmt.Errorf("error unmarshalling plan data: %w", err)
// // 	}

// // 	log.Println("Object ID", plan.ObjectId)

// // 	// Based on the operation, decide whether to insert or update the document in Elasticsearch
// // 	if msgBody.Operation == "create" {
// // 		// Insert the parent document (plan)
// // 		if err := insertParentDocument(context.Background(), esClient, "my-index-000007", &plan); err != nil {
// // 			return err
// // 		}

// // 		// Insert the planCostShares as a child document
// // 		if err := insertChildDocument(context.Background(), esClient, "my-index-000007", plan.PlanCostShares, plan.ObjectId); err != nil {
// // 			return err
// // 		}

// // 		// Iterate over linkedPlanServices and insert each as a child document
// // 		for _, service := range plan.LinkedPlanServices {
// // 			if err := insertChildDocument(context.Background(), esClient, "my-index-000007", service, plan.ObjectId); err != nil {
// // 				return err
// // 			}
// // 		}
// // 		// Insert nested child documents within each service here, similar to above
// // 		// // Insert document logic here
// // 		// req := esapi.IndexRequest{
// // 		// 	Index:      "my-index-000007",
// // 		// 	DocumentID: plan.ObjectId,
// // 		// 	// Body:       bytes.NewReader([]byte(*message.Body)),
// // 		// 	Body:    bytes.NewReader(msgBody.Data),
// // 		// 	Refresh: "true",
// // 		// }

// // 		// res, err := req.Do(context.Background(), esClient)
// // 		// if err != nil {
// // 		// 	log.Printf("Error performing indexing operation: %v", err)
// // 		// 	return fmt.Errorf("error indexing document: %w", err)
// // 		// }
// // 		// defer res.Body.Close()

// // 		// Log the response status and body for debugging
// // 		// log.Printf("Document indexed successfully, response status: %s", res.Status())
// // 	} else if msgBody.Operation == "update" {
// // 		// Update document logic here
// // 		docData := msgBody.Data

// // 		req := esapi.UpdateRequest{
// // 			Index:      "my-index-000007",
// // 			DocumentID: plan.ObjectId,
// // 			Body:       strings.NewReader(`{"doc":` + string(docData) + `}`),
// // 			Refresh:    "true",
// // 		}

// // 		res, err := req.Do(context.Background(), esClient)
// // 		if err != nil {
// // 			log.Printf("Error performing indexing operation for update : %v", err)
// // 			return fmt.Errorf("error indexing document for update : %w", err)
// // 		}
// // 		defer res.Body.Close()

// // 		// Log the response status and body for debugging
// // 		// log.Printf("Document indexed successfully for update , response status: %s", res.Status())
// // 		// Check the response body for more details on the error
// // 		if res.IsError() {
// // 			responseBody, _ := ioutil.ReadAll(res.Body)
// // 			log.Printf("Error response from Elasticsearch: %s", responseBody)
// // 			return fmt.Errorf("elasticsearch responded with error: %s", res.Status())
// // 		}

// // 		log.Printf("Document indexed successfully, response status: %s", res.Status())
// // 	}
// // 	return nil
// // }

// // func insertParentDocument(ctx context.Context, esClient *elasticsearch.Client, indexName string, plan *models.Plan) error {
// // 	// Convert the plan to JSON for the document body
// // 	docBody, err := json.Marshal(plan)
// // 	if err != nil {
// // 		return fmt.Errorf("error marshalling plan data: %w", err)
// // 	}

// // 	req := esapi.IndexRequest{
// // 		Index:      indexName,
// // 		DocumentID: plan.ObjectId, // Use the objectId as the Document ID
// // 		Body:       bytes.NewReader(docBody),
// // 		Refresh:    "true",
// // 	}

// // 	// Perform the indexing operation
// // 	res, err := req.Do(ctx, esClient)
// // 	if err != nil {
// // 		return fmt.Errorf("error indexing parent document: %w", err)
// // 	}
// // 	defer res.Body.Close()

// // 	if res.IsError() {
// // 		responseBody, _ := ioutil.ReadAll(res.Body)
// // 		log.Printf("Error response from Elasticsearch: %s", string(responseBody))
// // 		return fmt.Errorf("elasticsearch responded with error: %s", res.Status())
// // 	}

// // 	log.Printf("Parent document indexed successfully, response status: %s", res.Status())
// // 	return nil
// // }

// // // func insertChildDocument(ctx context.Context, esClient *elasticsearch.Client, indexName string, plan *models.Plan, ) error {
// // // 	// Assuming `plan` has a field `ParentId` that indicates the parent document's ID
// // // 	// and the join field is structured appropriately within the plan

// // // 	// Convert the plan to JSON for the document body
// // // 	docBody, err := json.Marshal(plan)
// // // 	if err != nil {
// // // 		return fmt.Errorf("error marshalling child plan data: %w", err)
// // // 	}

// // // 	req := esapi.IndexRequest{
// // // 		Index:      indexName,
// // // 		DocumentID: plan.ObjectId,
// // // 		Routing:    plan.ObjectId,
// // // 		Body:       bytes.NewReader(docBody),
// // // 		Refresh:    "true",
// // // 	}

// // // 	// Perform the indexing operation
// // // 	res, err := req.Do(ctx, esClient)
// // // 	if err != nil {
// // // 		return fmt.Errorf("error indexing child document: %w", err)
// // // 	}
// // // 	defer res.Body.Close()

// // // 	if res.IsError() {
// // // 		responseBody, _ := ioutil.ReadAll(res.Body)
// // // 		log.Printf("Error response from Elasticsearch: %s", string(responseBody))
// // // 		return fmt.Errorf("elasticsearch responded with error: %s", res.Status())
// // // 	}

// // // 	log.Printf("Child document indexed successfully, response status: %s", res.Status())
// // // 	return nil
// // // }

// // // func insertChildDocument(ctx context.Context, esClient *elasticsearch.Client, indexName string, childDoc interface{}, parentID string) error {
// // // 	var docBody []byte
// // // 	var err error
// // // 	var docID, joinField string

// // // 	// Determine the type of the child document and marshal it into JSON
// // // 	switch v := childDoc.(type) {
// // // 	case models.CostShares:
// // // 		docBody, err = json.Marshal(v)
// // // 		docID = v.ObjectId
// // // 		joinField = "planCostShares" // This should match your Elasticsearch relation name
// // // 	case models.PlanService:
// // // 		docBody, err = json.Marshal(v)
// // // 		docID = v.ObjectId
// // // 		joinField = "linkedPlanServices" // Adjust according to your relation name
// // // 	case models.Service:
// // // 		docBody, err = json.Marshal(v)
// // // 		docID = v.ObjectId
// // // 		joinField = "linkedService"
// // // 	// Add cases for other child document types
// // // 	default:
// // // 		return fmt.Errorf("unsupported document type")
// // // 	}

// // // 	if err != nil {
// // // 		return fmt.Errorf("error marshalling child document: %w", err)
// // // 	}

// // // 	// Add the join field to the request
// // // 	reqBody := map[string]interface{}{
// // // 		"doc": json.RawMessage(docBody),
// // // 		"my_join_field": map[string]interface{}{
// // // 			"name":   joinField,
// // // 			"parent": parentID,
// // // 		},
// // // 	}

// // // 	// Marshal reqBody to JSON
// // // 	reqBodyJSON, err := json.Marshal(reqBody)
// // // 	if err != nil {
// // // 		return fmt.Errorf("error marshalling request body: %w", err)
// // // 	}

// // // 	// Prepare the Elasticsearch request
// // // 	req := esapi.IndexRequest{
// // // 		Index:      indexName,
// // // 		DocumentID: docID,
// // // 		Routing:    parentID, // Use the parent document's ID for routing
// // // 		Body:       bytes.NewReader(reqBodyJSON),
// // // 		Refresh:    "true",
// // // 		// You might need to set the pipeline or other parameters depending on your setup
// // // 	}

// // // 	// // Use the marshaled JSON as the request body
// // // 	// req.Body = ioutil.NopCloser(bytes.NewReader(reqBodyJSON))
// // // 	// // req.Body = ioutil.NopCloser(bytes.NewReader(reqBody))

// // // 	// Perform the indexing operation
// // // 	res, err := req.Do(ctx, esClient)
// // // 	if err != nil {
// // // 		return fmt.Errorf("error indexing child document: %w", err)
// // // 	}
// // // 	defer res.Body.Close()

// // // 	if res.IsError() {
// // // 		responseBody, _ := ioutil.ReadAll(res.Body)
// // // 		log.Printf("Error response from Elasticsearch: %s", string(responseBody))
// // // 		return fmt.Errorf("elasticsearch responded with error: %s", res.Status())
// // // 	}

// // // 	log.Printf("Child document indexed successfully, document ID: %s, response status: %s", docID, res.Status())
// // // 	return nil
// // // }

// // func insertChildDocument(ctx context.Context, esClient *elasticsearch.Client, indexName string, childDoc interface{}, parentID string) error {
// // 	var err error
// // 	var reqBodyJSON []byte

// // 	// Determine the type of the child document and prepare the request body
// // 	switch v := childDoc.(type) {
// // 	case models.CostShares:
// // 		// Assuming models.CostShares matches the structure for "membercostshare"
// // 		if v.ObjectId == "1234vxc2324sdf-501" {
// // 			reqBody := map[string]interface{}{
// // 				"deductible":    v.Deductible,
// // 				"org":           "example.com",
// // 				"copay":         v.Copay,
// // 				"objectId":      v.ObjectId,
// // 				"objectType":    "membercostshare",
// // 				"my_join_field": map[string]interface{}{"name": "planCostShares", "parent": parentID},
// // 			}
// // 			reqBodyJSON, err = json.Marshal(reqBody)
// // 		} else {
// // 			reqBody := map[string]interface{}{
// // 				"deductible":    v.Deductible,
// // 				"org":           "example.com",
// // 				"copay":         v.Copay,
// // 				"objectId":      v.ObjectId,
// // 				"objectType":    "membercostshare",
// // 				"my_join_field": map[string]interface{}{"name": "planserviceCostShares", "parent": "27283xvx9asdff-504"},
// // 			}
// // 			reqBodyJSON, err = json.Marshal(reqBody)
// // 		}

// // 	case models.PlanService:
// // 		// Assuming models.PlanService matches the structure for "planservice"
// // 		reqBody := map[string]interface{}{
// // 			"org":           "example.com",
// // 			"objectId":      v.ObjectId,
// // 			"objectType":    "planservice",
// // 			"my_join_field": map[string]interface{}{"name": "linkedPlanServices", "parent": parentID},
// // 		}
// // 		reqBodyJSON, err = json.Marshal(reqBody)
// // 	case models.Service:
// // 		// Assuming models.Service matches the structure for "service"
// // 		reqBody := map[string]interface{}{
// // 			"org":           "example.com",
// // 			"objectId":      v.ObjectId,
// // 			"objectType":    "service",
// // 			"name":          v.Name,
// // 			"my_join_field": map[string]interface{}{"name": "linkedService", "parent": "27283xvx9asdff-504"},
// // 		}
// // 		reqBodyJSON, err = json.Marshal(reqBody)
// // 	// Add cases for other child document types if necessary
// // 	default:
// // 		return fmt.Errorf("unsupported document type")
// // 	}

// // 	if err != nil {
// // 		return fmt.Errorf("error marshalling request body: %w", err)
// // 	}

// // 	// Prepare the Elasticsearch request
// // 	req := esapi.IndexRequest{
// // 		Index:   indexName,
// // 		Body:    bytes.NewReader(reqBodyJSON),
// // 		Refresh: "true",
// // 	}

// // 	// If routing is required, set it here
// // 	if parentID != "" {
// // 		req.Routing = parentID
// // 	}

// // 	// Perform the indexing operation
// // 	res, err := req.Do(ctx, esClient)
// // 	if err != nil {
// // 		return fmt.Errorf("error indexing document: %w", err)
// // 	}
// // 	defer res.Body.Close()

// // 	if res.IsError() {
// // 		responseBody, _ := ioutil.ReadAll(res.Body)
// // 		log.Printf("Error response from Elasticsearch: %s", string(responseBody))
// // 		return fmt.Errorf("elasticsearch responded with error: %s", res.Status())
// // 	}

// // 	log.Printf("Document indexed successfully, response status: %s", res.Status())
// // 	return nil
// // }

// func processAndInsertMessage(esClient *elasticsearch.Client, message types.Message) error {
// 	if message.Body == nil {
// 		return fmt.Errorf("message body is nil")
// 	}

// 	var msgBody Message
// 	if err := json.Unmarshal([]byte(*message.Body), &msgBody); err != nil {
// 		return fmt.Errorf("error unmarshalling message body: %w", err)
// 	}

// 	fmt.Printf("Operation: %s\n", msgBody.Operation)

// 	// Unmarshal the message body into a generic map to examine its contents
// 	// var plan map[string]interface{}
// 	// if err := json.Unmarshal([]byte(msgBody.Data), &plan); err != nil {
// 	// 	return fmt.Errorf("error unmarshalling message body: %w", err)
// 	// }

// 	var plan models.Plan
// 	if err := json.Unmarshal([]byte(msgBody.Data), &plan); err != nil {
// 		return fmt.Errorf("error unmarshalling message body: %w", err)
// 	}

// 	// Index the plan
// 	if err := indexDocument(esClient, "my-index-000007", plan, plan.ObjectId, "", plan.ObjectId); err != nil {
// 		return err
// 	}

// 	// Index each linked plan service
// 	for _, service := range plan.LinkedPlanServices {
// 		if err := indexDocument(esClient, "my-index-000007", service, service.ObjectId, plan.ObjectId, plan.ObjectId); err != nil {
// 			return err
// 		}
// 		// Index linked services and cost shares as children
// 		if err := indexDocument(esClient, "my-index-000007", service.LinkedService, service.LinkedService.ObjectId, service.ObjectId, plan.ObjectId); err != nil {
// 			return err
// 		}
// 		if err := indexDocument(esClient, "my-index-000007", service.PlanServiceCostShares, service.PlanServiceCostShares.ObjectId, service.ObjectId, plan.ObjectId); err != nil {
// 			return err
// 		}
// 	}

// 	return nil

// 	// // Determine the document type and process accordingly
// 	// objectType, ok := plan["objectType"].(string)
// 	// if !ok {
// 	// 	return fmt.Errorf("objectType not found or is not a string")
// 	// }

// 	// fmt.Println("Object Type", objectType)

// 	// var indexName = "my-index-000007"
// 	// var err error

// 	// switch objectType {
// 	// case "plan":
// 	// 	// This is a parent document
// 	// 	err = insertParentDocument(context.Background(), esClient, indexName, plan)
// 	// case "membercostshare", "planservice", "service":
// 	// 	// These are child documents, which require a parent ID for routing
// 	// 	parentID, ok := plan["my_join_field"].(map[string]interface{})["parent"].(string)
// 	// 	if !ok {
// 	// 		return fmt.Errorf("parent ID not found or is not a string")
// 	// 	}
// 	// 	err = insertChildDocument(context.Background(), esClient, indexName, plan, parentID)
// 	// default:
// 	// 	return fmt.Errorf("unsupported objectType: %s", objectType)
// 	// }

// 	// switch objectType {
// 	// case "plan":
// 	// 	// This is a parent document
// 	// 	err = insertParentDocument(context.Background(), esClient, indexName, plan)
// 	// 	log.Printf("Document PARENT inserted into Elasticsearch successfully")
// 	// case "membercostshare":
// 	// 	// membercostshare can be a child of plan or linkedplanservice
// 	// 	myJoinField, ok := plan["my_join_field"].(map[string]interface{})
// 	// 	if !ok {
// 	// 		return fmt.Errorf("my_join_field not found or is not the correct type")
// 	// 	}
// 	// 	parentID, ok := myJoinField["parent"].(string)
// 	// 	if !ok {
// 	// 		return fmt.Errorf("parent ID not found or is not a string")
// 	// 	}
// 	// 	// Determine the parent type (plan or linkedplanservice) based on the name field in my_join_field
// 	// 	parentType, ok := myJoinField["name"].(string)
// 	// 	if !ok {
// 	// 		return fmt.Errorf("parent type not found or is not a string")
// 	// 	}
// 	// 	if parentType == "planCostShares" || parentType == "planserviceCostShares" {
// 	// 		err = insertChildDocument(context.Background(), esClient, indexName, plan, parentID)
// 	// 		log.Printf("Document CHILD inserted into Elasticsearch successfully")
// 	// 	} else {
// 	// 		return fmt.Errorf("unsupported parent type for membercostshare: %s", parentType)
// 	// 	}
// 	// case "planservice", "service":
// 	// 	// These are straightforward child documents, which require a parent ID for routing
// 	// 	parentID, ok := plan["my_join_field"].(map[string]interface{})["parent"].(string)
// 	// 	if !ok {
// 	// 		return fmt.Errorf("parent ID not found or is not a string")
// 	// 	}
// 	// 	err = insertChildDocument(context.Background(), esClient, indexName, plan, parentID)
// 	// 	log.Printf("Document CHILLDDDD inserted into Elasticsearch successfully")
// 	// default:
// 	// 	return fmt.Errorf("unsupported objectType: %s", objectType)
// 	// }

// 	// if err != nil {
// 	// 	log.Printf("Failed to insert document into Elasticsearch: %v", err)
// 	// 	return err
// 	// }

// 	// log.Printf("Document inserted into Elasticsearch successfully")
// 	// return nil
// }

// // func insertParentDocument(ctx context.Context, esClient *elasticsearch.Client, indexName string, doc map[string]interface{}) error {
// // 	docID, ok := doc["objectId"].(string)
// // 	if !ok {
// // 		return fmt.Errorf("objectId not found or is not a string")
// // 	}

// // 	// Marshal the document back to JSON for insertion
// // 	docBody, err := json.Marshal(doc)
// // 	if err != nil {
// // 		return fmt.Errorf("error marshalling document: %w", err)
// // 	}

// // 	req := esapi.IndexRequest{
// // 		Index:      indexName,
// // 		DocumentID: docID,
// // 		Body:       bytes.NewReader(docBody),
// // 		Refresh:    "true",
// // 	}

// // 	return executeESRequest(ctx, esClient, &req)
// // }

// // func insertChildDocument(ctx context.Context, esClient *elasticsearch.Client, indexName string, doc map[string]interface{}, parentID string) error {
// // 	docID, ok := doc["objectId"].(string)
// // 	if !ok {
// // 		return fmt.Errorf("objectId not found or is not a string")
// // 	}

// // 	// Marshal the document back to JSON for insertion
// // 	docBody, err := json.Marshal(doc)
// // 	if err != nil {
// // 		return fmt.Errorf("error marshalling document: %w", err)
// // 	}

// // 	req := esapi.IndexRequest{
// // 		Index:      indexName,
// // 		DocumentID: docID,
// // 		Body:       bytes.NewReader(docBody),
// // 		Refresh:    "true",
// // 		Routing:    parentID, // Use parentID for routing to ensure parent-child colocation
// // 	}

// // 	return executeESRequest(ctx, esClient, &req)
// // }

// // func executeESRequest(ctx context.Context, esClient *elasticsearch.Client, req *esapi.IndexRequest) error {
// // 	res, err := req.Do(ctx, esClient)
// // 	if err != nil {
// // 		return fmt.Errorf("error executing Elasticsearch request: %w", err)
// // 	}
// // 	defer res.Body.Close()

// // 	if res.IsError() {
// // 		responseBody, _ := ioutil.ReadAll(res.Body)
// // 		return fmt.Errorf("elasticsearch responded with error: %s", string(responseBody))
// // 	}

// // 	return nil
// // }

// // func indexDocument(client *elasticsearch.Client, index string, doc interface{}, id, parent, routing string) error {
// // 	body, err := json.Marshal(doc)
// // 	if err != nil {
// // 		return err
// // 	}

// // 	req := esapi.IndexRequest{
// // 		Index:      index,
// // 		DocumentID: id,
// // 		Body:       strings.NewReader(string(body)),
// // 		Refresh:    "true",
// // 	}

// // 	if routing != "" {
// // 		req.Routing = routing
// // 	}

// // 	res, err := req.Do(context.Background(), client)
// // 	if err != nil {
// // 		return err
// // 	}
// // 	defer res.Body.Close()

// // 	if res.IsError() {
// // 		return fmt.Errorf("error indexing document ID=%s: %s", id, res.String())
// // 	}
// // 	fmt.Printf("Document ID=%s indexed.\n", id)
// // 	return nil
// // }

// func indexDocument(client *elasticsearch.Client, doc ElasticsearchDocument, routing string) error {
// 	req := esapi.IndexRequest{
// 		Index:      "my-index-000007",
// 		DocumentID: doc.ObjectId,
// 		Body:       esapi.NewJSONReader(doc),
// 		Refresh:    "true",
// 		Routing:    routing,
// 	}

// 	res, err := req.Do(context.Background(), client)
// 	if err != nil {
// 		return err
// 	}
// 	defer res.Body.Close()

// 	if res.IsError() {
// 		return fmt.Errorf("error indexing document ID=%s: %s", doc.ObjectId, res.String())
// 	}
// 	fmt.Printf("Document ID=%s indexed successfully.\n", doc.ObjectId)
// 	return nil
// }

// func handleAndIndexData(client *elasticsearch.Client, jsonData []byte) error {
// 	var plan models.Plan
// 	if err := json.Unmarshal(jsonData, &plan); err != nil {
// 		return err
// 	}

// 	// Index the main plan document
// 	planDoc := ElasticsearchDocument{
// 		Org:          plan.Org,
// 		ObjectId:     plan.ObjectId,
// 		ObjectType:   plan.ObjectType,
// 		PlanType:     plan.PlanType,
// 		CreationDate: plan.CreationDate,
// 		MyJoinField:  map[string]string{"name": "plan"},
// 	}
// 	if err := indexDocument(client, planDoc, plan.ObjectId); err != nil {
// 		return err
// 	}

// 	// Index plan cost shares as a child of the plan
// 	if plan.PlanCostShares != nil {
// 		plan.PlanCostShares.MyJoinField = map[string]string{"name": "planCostShares", "parent": plan.ObjectId}
// 		if err := indexDocument(client, *plan.PlanCostShares, plan.ObjectId); err != nil {
// 			return err
// 		}
// 	}

// 	// Index linked plan services and their children
// 	for _, service := range plan.LinkedPlanServices {
// 		service.MyJoinField = map[string]string{"name": "linkedPlanServices", "parent": plan.ObjectId}
// 		if err := indexDocument(client, *service, plan.ObjectId); err != nil {
// 			return err
// 		}

// 		// Index nested children like linked services and cost shares
// 		// Repeat similar steps as above for each child type under linkedPlanServices
// 	}

// 	return nil
// }

// // func handleData(client *elasticsearch.Client, data []byte) error {
// // 	var plan models.Plan
// // 	if err := json.Unmarshal(data, &plan); err != nil {
// // 		return err
// // 	}

// // 	// Index the plan
// // 	if err := indexDocument(client, "my-index-000007", plan, plan.ObjectID, "", plan.ObjectID); err != nil {
// // 		return err
// // 	}

// // 	// Index each linked plan service
// // 	for _, service := range plan.LinkedPlanServices {
// // 		if err := indexDocument(client, "my-index-000007", service, service.ObjectID, plan.ObjectID, plan.ObjectID); err != nil {
// // 			return err
// // 		}

// // 		// Index linked services and cost shares as children
// // 		if err := indexDocument(client, "my-index-000007", service.LinkedService, service.LinkedService.ObjectID, service.ObjectID, plan.ObjectID); err != nil {
// // 			return err
// // 		}
// // 		if err := indexDocument(client, "my-index-000007", service.PlanServiceCostShares, service.PlanServiceCostShares.ObjectID, service.ObjectID, plan.ObjectID); err != nil {
// // 			return err
// // 		}
// // 	}
// // 	return nil
// // }

type ElasticsearchDocument struct {
	Org          string            `json:"org"`
	ObjectId     string            `json:"objectId"`
	ObjectType   string            `json:"objectType"`
	PlanType     string            `json:"planType,omitempty"`
	CreationDate string            `json:"creationDate,omitempty"`
	Name         string            `json:"name,omitempty"`
	Deductible   int               `json:"deductible,omitempty"`
	Copay        int               `json:"copay,omitempty"`
	MyJoinField  map[string]string `json:"my_join_field"`
}

// func getElasticsearchClient() (*elasticsearch.Client, error) {
//     cfg := elasticsearch.Config{
//         Addresses: []string{"http://localhost:9200"},
//     }
//     return elasticsearch.NewClient(cfg)
// }

func indexDocument(client *elasticsearch.Client, doc ElasticsearchDocument, routing string) error {
	req := esapi.IndexRequest{
		Index:      "my-index-000007",
		DocumentID: doc.ObjectId,
		Body:       bytes.NewReader(marshalDoc(doc)),
		Refresh:    "true",
		Routing:    routing,
	}

	res, err := req.Do(context.Background(), client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		json.NewDecoder(res.Body).Decode(&e) // Ignoring errors on purpose
		return fmt.Errorf("error indexing document ID=%s: %s", doc.ObjectId, e)
	}
	fmt.Printf("Document ID=%s indexed.\n", doc.ObjectId)
	return nil
}

func marshalDoc(doc interface{}) []byte {
	jsonDoc, err := json.Marshal(doc)
	if err != nil {
		log.Fatalf("Error marshalling document: %v", err)
	}
	return jsonDoc
}

func processAndInsertPlan(esClient *elasticsearch.Client, message types.Message) error {

	if message.Body == nil {
		return fmt.Errorf("message body is nil")
	}

	var msgBody Message
	if err := json.Unmarshal([]byte(*message.Body), &msgBody); err != nil {
		return fmt.Errorf("error unmarshalling message body: %w", err)
	}

	fmt.Printf("Operation: %s\n", msgBody.Operation)

	var plan models.Plan
	if err := json.Unmarshal(msgBody.Data, &plan); err != nil {
		return fmt.Errorf("error unmarshalling plan data: %w", err)
	}

	log.Println("Object ID", plan.ObjectId)

	// Index the main plan
	planDoc := ElasticsearchDocument{
		Org:          plan.Org,
		ObjectId:     plan.ObjectId,
		ObjectType:   plan.ObjectType,
		PlanType:     plan.PlanType,
		CreationDate: plan.CreationDate,
		MyJoinField:  map[string]string{"name": "plan"},
	}
	if err := indexDocument(esClient, planDoc, ""); err != nil {
		return err
	}

	// Index plan cost shares if present
	fmt.Println("plan.PlanCostShares.ObjectId ", plan.PlanCostShares.ObjectId)
	if plan.PlanCostShares.ObjectId != "" {
		costShareDoc := ElasticsearchDocument{
			Org:        plan.PlanCostShares.Org,
			ObjectId:   plan.PlanCostShares.ObjectId,
			ObjectType: plan.PlanCostShares.ObjectType,
			Deductible: *plan.PlanCostShares.Deductible,
			Copay:      *plan.PlanCostShares.Copay,
			MyJoinField: map[string]string{
				"name":   "planCostShares",
				"parent": plan.ObjectId,
			},
		}
		if err := indexDocument(esClient, costShareDoc, plan.ObjectId); err != nil {
			return err
		}
	}

	// Index linked plan services
	for _, service := range plan.LinkedPlanServices {
		serviceDoc := ElasticsearchDocument{
			Org:        service.Org,
			ObjectId:   service.ObjectId,
			ObjectType: service.ObjectType,
			MyJoinField: map[string]string{
				"name":   "linkedPlanServices",
				"parent": plan.ObjectId,
			},
		}
		if err := indexDocument(esClient, serviceDoc, plan.ObjectId); err != nil {
			return err
		}

		// Index linked service
		fmt.Println("service.LinkedService.ObjectId ", service.LinkedService.ObjectId)
		if service.LinkedService.ObjectId != "" {
			linkedServiceDoc := ElasticsearchDocument{
				Org:        service.LinkedService.Org,
				ObjectId:   service.LinkedService.ObjectId,
				ObjectType: service.LinkedService.ObjectType,
				Name:       service.LinkedService.Name,
				MyJoinField: map[string]string{
					"name":   "linkedService",
					"parent": service.ObjectId,
				},
			}
			if err := indexDocument(esClient, linkedServiceDoc, plan.ObjectId); err != nil {
				return err
			}
		}

		// Index plan service cost shares
		fmt.Println("service.PlanServiceCostShares.ObjectId ", service.PlanServiceCostShares.ObjectId)
		if service.PlanServiceCostShares.ObjectId != "" {
			costSharesDoc := ElasticsearchDocument{
				Org:        service.PlanServiceCostShares.Org,
				ObjectId:   service.PlanServiceCostShares.ObjectId,
				ObjectType: service.PlanServiceCostShares.ObjectType,
				Deductible: *service.PlanServiceCostShares.Deductible,
				Copay:      *service.PlanServiceCostShares.Copay,
				MyJoinField: map[string]string{
					"name":   "planserviceCostShares",
					"parent": service.ObjectId,
				},
			}
			if err := indexDocument(esClient, costSharesDoc, plan.ObjectId); err != nil {
				return err
			}
		}
	}

	return nil
}
