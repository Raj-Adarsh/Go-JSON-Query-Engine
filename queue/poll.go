package queue

import (
	"bytes"
	"context"
	"crud_with_dynamodb/models"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	// "github.com/elastic/go-elasticsearch"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// Simplified document structure for testing
type SimpleDoc struct {
	Status string `json:"status"`
}

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

func PollMessagesAndInsertToElasticsearch() {
	// Initialize Elasticsearch client
	esClient, err := getElasticsearchClient()
	if err != nil {
		log.Fatalf("Error setting up Elasticsearch client: %v", err)
	}

	// Your existing AWS SQS configuration code...
	// ElasticMQ endpoint and dummy credentials
	endpoint := "http://localhost:9324"
	region := "elasticmq"
	accessKey := "dummy"
	secretKey := "dummy"

	// Load the AWS SDK configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:           endpoint,
					SigningRegion: region,
				}, nil
			},
		)),
		config.WithClientLogMode(aws.LogSigning|aws.LogRequestWithBody),
	)
	if err != nil {
		log.Fatalf("Unable to load SDK config, %v", err)
	}

	sqsClient := sqs.NewFromConfig(cfg)
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

		// Process each message
		for _, message := range msgOutput.Messages {
			fmt.Printf("Message Body: %s\n", *message.Body) // Ensure message.Body is not nil before dereferencing
			// Insert message into Elasticsearch
			if err := processAndInsertMessage(esClient, message); err != nil {
				log.Printf("Failed to insert message into Elasticsearch: %v", err)
				continue // Skip deleting the message if there was an error processing it
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

		time.Sleep(5 * time.Second) // Wait before polling again
	}
}

// func processAndInsertMessage(esClient *elasticsearch.Client, message *sqs.Message) error {
// 	var buf bytes.Buffer
// 	if err := json.NewEncoder(&buf).Encode(message.Body); err != nil {
// 		return fmt.Errorf("error encoding message body: %w", err)
// 	}

// 	res, err := esClient.Index(
// 		"plans", // Index name
// 		&buf,    // Document to index
// 		esClient.Index.WithContext(context.Background()),
// 		esClient.Index.WithRefresh("true"),
// 	)

// 	if err != nil {
// 		return fmt.Errorf("error indexing document: %w", err)
// 	}

// 	defer res.Body.Close()
// 	fmt.Println(res)
// 	return nil
// }

// func processAndInsertMessage(esClient *elasticsearch.Client, messageInput *sqs.SendMessageInput) error {
// 	if messageInput.MessageBody == nil {
// 		return fmt.Errorf("message body is nil")
// 	}

// 	var buf bytes.Buffer
// 	if err := json.NewEncoder(&buf).Encode(*messageInput.MessageBody); err != nil {
// 		return fmt.Errorf("error encoding message body: %w", err)
// 	}

// 	res, err := esClient.Index(
// 		"messages", // Adjust the index name as needed
// 		&buf,
// 		esClient.Index.WithContext(context.Background()),
// 		esClient.Index.WithRefresh("true"),
// 	)
// 	if err != nil {
// 		return fmt.Errorf("error indexing document: %w", err)
// 	}
// 	defer res.Body.Close()

// 	// Additional logic to handle the response from Elasticsearch can be added here

// 	return nil
// }

// func processAndInsertMessage(esClient *elasticsearch.Client, message types.Message) error {
// 	if message.Body == nil {
// 		return fmt.Errorf("message body is nil")
// 	}

// 	var buf bytes.Buffer
// 	if err := json.NewEncoder(&buf).Encode(*message.Body); err != nil {
// 		return fmt.Errorf("error encoding message body: %w", err)
// 	}

// 	// Create an IndexRequest
// 	req := esapi.IndexRequest{
// 		Index:      "plan", // Adjust the index name as needed
// 		DocumentID: "",     // Optionally, set a specific document ID
// 		Body:       &buf,
// 		Refresh:    "true", // Ensure the document is searchable immediately
// 	}

// 	// Perform the indexing operation
// 	res, err := req.Do(context.Background(), esClient)
// 	if err != nil {
// 		return fmt.Errorf("error indexing document: %w", err)
// 	}

// 	defer res.Body.Close()

// 	// Additional logic to handle the response from Elasticsearch can be added here

// 	return nil
// }

func processAndInsertMessage(esClient *elasticsearch.Client, message types.Message) error {
	if message.Body == nil {
		return fmt.Errorf("message body is nil")
	}

	// // Simplify the document for debugging purposes
	// simpleDoc := map[string]string{"status": "testing"}
	// var buf bytes.Buffer
	// if err := json.NewEncoder(&buf).Encode(simpleDoc); err != nil {
	// 	return fmt.Errorf("error encoding simple document: %w", err)
	// }

	// var buf bytes.Buffer
	var plan models.Plan
	if err := json.Unmarshal([]byte(*message.Body), &plan); err != nil {
		return fmt.Errorf("error unmarshalling message body: %w", err)
	}

	// if err := json.NewEncoder(&buf).Encode(*message.Body); err != nil {
	// 	return fmt.Errorf("error encoding message body: %w", err)
	// }

	req := esapi.IndexRequest{
		Index:      "plans",
		DocumentID: plan.ObjectId,
		Body:       bytes.NewReader([]byte(*message.Body)),
		Refresh:    "true",
	}

	// // Create a simplified IndexRequest
	// req := esapi.IndexRequest{
	// 	Index:      "plan", // Use a test index
	// 	DocumentID: "",     // Let Elasticsearch generate the document ID
	// 	Body:       &buf,
	// 	Refresh:    "true",
	// }

	// Perform the indexing operation with detailed logging
	res, err := req.Do(context.Background(), esClient)
	if err != nil {
		log.Printf("Error performing indexing operation: %v", err)
		return fmt.Errorf("error indexing document: %w", err)
	}
	defer res.Body.Close()

	// Log the response status and body for debugging
	responseBody, _ := ioutil.ReadAll(res.Body)
	log.Printf("Indexing operation response status: %s, body: %s", res.Status(), responseBody)

	return nil
}

// // Modified function to insert a simple string into Elasticsearch
// func InsertSimpleStringToElasticsearch() error {
// 	esClient, err := getElasticsearchClient()
// 	if err != nil {
// 		log.Fatalf("Error setting up Elasticsearch client: %v", err)
// 	}

// 	statusMessage := "Test String"

// 	simpleDoc := SimpleDoc{Status: statusMessage}
// 	var buf bytes.Buffer
// 	if err := json.NewEncoder(&buf).Encode(simpleDoc); err != nil {
// 		return fmt.Errorf("error encoding simple document: %w", err)
// 	}

// 	req := esapi.IndexRequest{
// 		Index:      "test-index", // Specify your index name
// 		DocumentID: "",           // Let Elasticsearch generate the document ID
// 		Body:       &buf,
// 		Refresh:    "true",
// 	}

// 	res, err := req.Do(context.Background(), esClient)
// 	if err != nil {
// 		log.Printf("Error performing indexing operation: %v", err)
// 		return fmt.Errorf("error indexing document: %w", err)
// 	}
// 	defer res.Body.Close()

// 	responseBody, _ := ioutil.ReadAll(res.Body)
// 	log.Printf("Indexing operation response status: %s, body: %s", res.Status(), responseBody)

// 	return nil
// }
