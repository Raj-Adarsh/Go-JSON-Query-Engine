package connector

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func ConnectElasticSQS() *sqs.Client {
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

	return sqsClient
}
