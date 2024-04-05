package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"crud_with_dynamodb/models"
)

type Message struct {
	Operation string      `json:"operation"`
	Data      models.Plan `json:"data"`
}

// func sendMessage(sqsClient *sqs.Client, queueUrl string, messageBody Message) error {
// 	// Convert the messageBody struct to JSON
// 	jsonMessage, err := json.Marshal(messageBody)
// 	if err != nil {
// 		return fmt.Errorf("error marshaling message to JSON: %w", err)
// 	}

// 	// Send the JSON message as a string to SQS
// 	_, err = sqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
// 		QueueUrl:    &queueUrl,
// 		MessageBody: aws.String(string(jsonMessage)),
// 	})
// 	return err
// }

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

// func PublishPlanCreation(plan models.Plan) {
// 	cfg, err := config.LoadDefaultConfig(context.TODO())
// 	if err != nil {
// 		log.Fatalf("Error loading AWS configuration: %v", err)
// 	}

// 	sqsClient := sqs.NewFromConfig(cfg)
// 	queueUrl := "http://localhost:9324/000000000000/MyTestQueue-1"

// 	messageBody := Message{
// 		Operation: "create",
// 		Data:      plan,
// 	}

// 	if err := sendMessage(sqsClient, queueUrl, messageBody); err != nil {
// 		log.Fatalf("Failed to send message: %v", err)
// 	} else {
// 		log.Println("Plan creation message sent successfully")
// 	}
// }

// func PublishPlanCreation(plan models.Plan) {
// 	// ElasticMQ endpoint and dummy credentials
// 	endpoint := "http://localhost:9324"
// 	region := "elasticmq"
// 	accessKey := "dummy"
// 	secretKey := "dummy"

// 	// Load the AWS SDK configuration with custom settings for ElasticMQ
// 	cfg, err := config.LoadDefaultConfig(context.TODO(),
// 		config.WithRegion(region),
// 		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
// 		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
// 			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
// 				return aws.Endpoint{
// 					URL:           endpoint,
// 					SigningRegion: region,
// 				}, nil
// 			},
// 		)),
// 	)
// 	if err != nil {
// 		log.Fatalf("Error loading AWS configuration: %v", err)
// 	}

// 	sqsClient := sqs.NewFromConfig(cfg)
// 	queueUrl := "http://localhost:9324/000000000000/MyTestQueue-1"

// 	messageBody := Message{
// 		Operation: "create",
// 		Data:      plan,
// 	}

// 	if err := sendMessage(sqsClient, queueUrl, messageBody); err != nil {
// 		log.Fatalf("Failed to send message: %v", err)
// 	} else {
// 		log.Println("Plan creation message sent successfully")
// 	}
// }

func PublishPlanCreation(plan models.Plan) {
	// ElasticMQ endpoint and dummy credentials
	endpoint := "http://localhost:9324"
	region := "elasticmq"
	accessKey := "dummy"
	secretKey := "dummy"

	// Load the AWS SDK configuration with custom settings for ElasticMQ
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
	)
	if err != nil {
		log.Fatalf("Error loading AWS configuration: %v", err)
	}

	sqsClient := sqs.NewFromConfig(cfg)
	queueUrl := "http://localhost:9324/000000000000/MyTestQueue-1"

	messageBody := Message{
		Operation: "create",
		Data:      plan,
	}

	// Send the message
	if err := sendMessage(sqsClient, queueUrl, messageBody); err != nil {
		log.Fatalf("Failed to send message: %v", err)
	} else {
		log.Println("Plan creation message sent successfully")

		// Check message visibility in ElasticMQ
		msgVisibility, err := checkMessageVisibility(sqsClient, queueUrl, messageBody)
		if err != nil {
			log.Printf("Error checking message visibility: %v", err)
		} else {
			log.Printf("Message visibility in queue: %v", msgVisibility)
		}
	}
}

func checkMessageVisibility(sqsClient *sqs.Client, queueUrl string, messageBody Message) (bool, error) { //messageBody types.Message
	// Check if the message is visible in the queue
	msgOutput, err := sqsClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl:            &queueUrl,
		MaxNumberOfMessages: 1,
	})
	if err != nil {
		return false, fmt.Errorf("error receiving message: %w", err)
	}

	// Check if the received message matches the sent message
	if len(msgOutput.Messages) > 0 {
		receivedMessage := msgOutput.Messages[0]
		receivedBody, err := json.Marshal(receivedMessage)
		if err != nil {
			return false, fmt.Errorf("error marshaling received message: %w", err)
		}

		sentBody, err := json.Marshal(messageBody)
		if err != nil {
			return false, fmt.Errorf("error marshaling sent message: %w", err)
		}

		return string(receivedBody) == string(sentBody), nil
	}

	// If no message was received, it doesn't match the sent message
	return false, nil
}
