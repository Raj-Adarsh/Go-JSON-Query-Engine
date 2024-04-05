package queue

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func ConnectSQS() {
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

	// Create an SQS client
	sqsClient := sqs.NewFromConfig(cfg)

	// Create a queue
	queueName := "MyTestQueue-1" // Specify your queue name here
	_, err = CreateQueue(sqsClient, queueName)
	if err != nil {
		log.Fatalf("Unable to create queue, %v", err)
	} else {
		log.Printf("Queue %s created successfully", queueName)
		// err = insertSimpleStringToElasticsearch(sqsClient, "testing insertion")
		// if err != nil {
		// 	log.Printf("Failed to insert simple string into Elasticsearch: %v", err)
		// } else {
		// 	log.Println("Successfully inserted simple string into Elasticsearch")
		// }
	}

	// Example: Listing SQS queues
	listQueuesOutput, err := sqsClient.ListQueues(context.TODO(), &sqs.ListQueuesInput{})
	if err != nil {
		log.Fatalf("Unable to list queues, %v", err)
	}

	log.Println("Queues:", listQueuesOutput.QueueUrls)
}

func CreateQueue(sqsClient *sqs.Client, queueName string) (*sqs.CreateQueueOutput, error) {
	createQueueInput := &sqs.CreateQueueInput{
		QueueName: &queueName,
	}

	return sqsClient.CreateQueue(context.TODO(), createQueueInput)
}
