package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"crud_with_dynamodb/connector"
	"crud_with_dynamodb/models"
)

type Message struct {
	Operation string          `json:"operation"`
	Data      json.RawMessage `json:"data"`
}

func sendMessage(sqsClient *sqs.Client, queueUrl string, messageBody Message) error {
	jsonMessage, err := json.Marshal(messageBody)
	if err != nil {
		return fmt.Errorf("error marshaling message to JSON: %w", err)
	}

	out, err := sqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
		QueueUrl:    &queueUrl,
		MessageBody: aws.String(string(jsonMessage)),
	})
	if err != nil {
		return fmt.Errorf("sendMessage() - failed to send message: %w", err)
	}
	log.Printf("Message sent successfully: %s", aws.ToString(out.MessageId))
	return nil
}

func PublishPlanCreation(plan models.Plan, operation string) {
	// // ElasticMQ endpoint and dummy credentials
	// endpoint := "http://localhost:9324"
	// region := "elasticmq"
	// accessKey := "dummy"
	// secretKey := "dummy"

	// // Load the AWS SDK configuration with custom settings for ElasticMQ
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
	// )
	// if err != nil {
	// 	log.Fatalf("Error loading AWS configuration: %v", err)
	// }

	// sqsClient := sqs.NewFromConfig(cfg)
	sqsClient := connector.ConnectElasticSQS()
	queueUrl := "http://localhost:9324/000000000000/MyTestQueue-1"

	// Marshal the plan object into JSON
	planJSON, err := json.Marshal(plan)
	if err != nil {
		log.Fatalf("Failed to marshal plan: %v", err)
	}

	messageBody := Message{
		Operation: operation,
		Data:      planJSON,
	}

	// Send the message
	if err := sendMessage(sqsClient, queueUrl, messageBody); err != nil {
		log.Fatalf("Failed to send message: %v", err)
	} else {
		log.Println("Plan creation message sent successfully")
	}
}
