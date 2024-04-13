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

		for _, message := range msgOutput.Messages {
			fmt.Printf("Message Body: %s\n", *message.Body)
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
		time.Sleep(5 * time.Second) // Wait before polling again
	}
}

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

	if msgBody.Operation == "create" {
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
	} else if msgBody.Operation == "update" {
		var updatedPlan models.Plan
		if err := json.Unmarshal(msgBody.Data, &updatedPlan); err != nil {
			return fmt.Errorf("error unmarshalling updated plan data: %w", err)
		}

		// // Find the existing plan in the Elasticsearch index
		// existingPlan, err := getPlanFromElasticsearch(esClient, updatedPlan.ObjectId)
		// if err != nil {
		// 	return fmt.Errorf("error getting existing plan from Elasticsearch: %w", err)
		// }

		// Update the existing plan with the updated plan
		updatedPlanDoc := ElasticsearchDocument{
			Org:          updatedPlan.Org,
			ObjectId:     updatedPlan.ObjectId,
			ObjectType:   updatedPlan.ObjectType,
			PlanType:     updatedPlan.PlanType,
			CreationDate: updatedPlan.CreationDate,
			MyJoinField:  map[string]string{"name": "plan"},
		}
		if err := indexDocument(esClient, updatedPlanDoc, ""); err != nil {
			return err
		}

		// Index plan cost shares if present
		fmt.Println("updatedPlan.PlanCostShares.ObjectId ", updatedPlan.PlanCostShares.ObjectId)
		if updatedPlan.PlanCostShares.ObjectId != "" {
			costShareDoc := ElasticsearchDocument{
				Org:        updatedPlan.PlanCostShares.Org,
				ObjectId:   updatedPlan.PlanCostShares.ObjectId,
				ObjectType: updatedPlan.PlanCostShares.ObjectType,
				Deductible: *updatedPlan.PlanCostShares.Deductible,
				Copay:      *updatedPlan.PlanCostShares.Copay,
				MyJoinField: map[string]string{
					"name":   "planCostShares",
					"parent": updatedPlan.ObjectId,
				},
			}
			if err := indexDocument(esClient, costShareDoc, updatedPlan.ObjectId); err != nil {
				return err
			}
		}

		// Index linked plan services
		for _, service := range updatedPlan.LinkedPlanServices {
			existingService, err := getServiceFromElasticsearch(esClient, service.ObjectId)
			if err != nil {
				return fmt.Errorf("error getting existing service from Elasticsearch: %w", err)
			}

			// If the service already exists, update it
			if existingService != nil {
				// ... update the existing service ...
				existingService.Org = service.Org
				existingService.ObjectId = service.ObjectId
				existingService.ObjectType = service.ObjectType
				// Add or update any other fields as necessary

				// Now, re-index the updated document in Elasticsearch
				if err := indexDocument(esClient, *existingService, existingService.ObjectId); err != nil {
					return fmt.Errorf("error updating existing service in Elasticsearch: %w", err)
				}
			} else {
				serviceDoc := ElasticsearchDocument{
					Org:        service.Org,
					ObjectId:   service.ObjectId,
					ObjectType: service.ObjectType,
					MyJoinField: map[string]string{
						"name":   "linkedPlanServices",
						"parent": updatedPlan.ObjectId,
					},
				}
				if err := indexDocument(esClient, serviceDoc, updatedPlan.ObjectId); err != nil {
					return err
				}
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
				if err := indexDocument(esClient, linkedServiceDoc, updatedPlan.ObjectId); err != nil {
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
				if err := indexDocument(esClient, costSharesDoc, updatedPlan.ObjectId); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func getServiceFromElasticsearch(esClient *elasticsearch.Client, objectId string) (*ElasticsearchDocument, error) {
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"objectId": objectId,
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("error encoding query: %w", err)
	}

	res, err := esClient.Search(
		esClient.Search.WithContext(context.Background()),
		esClient.Search.WithIndex("my-index-000007"),
		esClient.Search.WithBody(&buf),
		esClient.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		return nil, fmt.Errorf("error getting response from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return nil, fmt.Errorf("error parsing the response body: %w", err)
		} else {
			// Elasticsearch error (e.g., index not found)
			return nil, fmt.Errorf("error searching for service: %v", e)
		}
	}

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("error parsing the response body: %w", err)
	}

	// Check hits are found
	hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
	if len(hits) == 0 {
		return nil, nil // No service found
	}

	// Assuming only one match is relevant, adjust as necessary
	serviceHit := hits[0].(map[string]interface{})["_source"]
	serviceBytes, err := json.Marshal(serviceHit)
	if err != nil {
		return nil, fmt.Errorf("error marshalling service: %w", err)
	}

	var service ElasticsearchDocument
	if err := json.Unmarshal(serviceBytes, &service); err != nil {
		return nil, fmt.Errorf("error unmarshalling service: %w", err)
	}

	return &service, nil
}
