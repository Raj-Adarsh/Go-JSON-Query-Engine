package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gorilla/mux"
)

type Plan struct {
	PlanCostShares     CostShares    `json:"planCostShares"`
	LinkedPlanServices []PlanService `json:"linkedPlanServices"`
	Org                string        `json:"_org"`
	ObjectId           string        `json:"objectId"`
	ObjectType         string        `json:"objectType"`
	PlanType           string        `json:"planType"`
	// CreationDate       CustomDate    `json:"creationDate"`
}

type CostShares struct {
	Deductible int    `json:"deductible"`
	Org        string `json:"_org"`
	Copay      int    `json:"copay"`
	ObjectId   string `json:"objectId"`
	ObjectType string `json:"objectType"`
}

type PlanService struct {
	LinkedService         Service    `json:"linkedService"`
	PlanServiceCostShares CostShares `json:"planserviceCostShares"`
	Org                   string     `json:"_org"`
	ObjectId              string     `json:"objectId"`
	ObjectType            string     `json:"objectType"`
}

type Service struct {
	Org        string `json:"_org"`
	ObjectId   string `json:"objectId"`
	ObjectType string `json:"objectType"`
	Name       string `json:"name"`
}

// type CustomDate struct {
// 	time.Time
// }

var tableName string = "PLAN_TABLE"

func main() {
	// Using the SDK's default configuration, loading additional config
	// and credentials values from the environment variables, shared
	// credentials, and shared configuration files
	// tableName := "PLAN_TABLE"

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-west-2"), // Or any other preferred region
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithEndpointResolver(aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:           "http://localhost:8000", // Endpoint for DynamoDB Local
				SigningRegion: "us-west-2",
			}, nil
		})),
		config.WithClientLogMode(aws.LogSigning|aws.LogRequestWithBody),
	)

	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Using the Config value, create the DynamoDB client
	svc := dynamodb.NewFromConfig(cfg)

	// Step 1: Check if the table exists
	_, err = svc.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})

	if err != nil {
		var notFoundException *types.ResourceNotFoundException
		if errors.As(err, &notFoundException) {
			// Step 2: Table does not exist, so create it
			createTable(svc, tableName)
		} else {
			// Other error occurred
			log.Fatalf("Failed to describe table: %s", err)
		}
	} else {
		// Table exists
		fmt.Printf("Table %s already exists.\n", tableName)
		// deleteTable(svc, tableName)
	}

	// //HTTP
	r := mux.NewRouter()

	r.HandleFunc("/v1/plan", createItemHandler(svc)).Methods("POST")
	r.HandleFunc("/v1/plan/{ObjectId}", getItemHandler(svc)).Methods("GET")
	// r.HandleFunc("/item/{objectId}", updateItemHandler(svc)).Methods("PATCH")
	r.HandleFunc("/v1/plan/{ObjectId}", deleteItemHandler(svc)).Methods("DELETE")

	http.ListenAndServe(":8081", r)
}

func validatePlan(plan Plan) error {
	// Validate PlanCostShares
	if plan.PlanCostShares.Org == "" || plan.PlanCostShares.ObjectId == "" || plan.PlanCostShares.ObjectType == "" {
		return errors.New("missing required fields in PlanCostShares")
	}

	// Validate each LinkedPlanService
	if len(plan.LinkedPlanServices) == 0 {
		return errors.New("linkedPlanServices is required")
	}

	for _, service := range plan.LinkedPlanServices {
		if service.Org == "" || service.ObjectId == "" || service.ObjectType == "" ||
			service.LinkedService.Org == "" || service.LinkedService.ObjectId == "" ||
			service.LinkedService.ObjectType == "" || service.LinkedService.Name == "" ||
			service.PlanServiceCostShares.Org == "" || service.PlanServiceCostShares.ObjectId == "" ||
			service.PlanServiceCostShares.ObjectType == "" {
			return errors.New("missing required fields in LinkedPlanServices")
		}
	}

	// Validate top-level Plan fields
	if plan.Org == "" || plan.ObjectId == "" || plan.ObjectType == "" || plan.PlanType == "" {
		return errors.New("missing required top-level Plan fields")
	}

	return nil
}

func createItemHandler(svc *dynamodb.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Read the entire request body
		bodyBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusInternalServerError)
			return
		}

		// Generate ETag for the item using SHA-256 of the request body
		eTag := fmt.Sprintf(`"%x"`, sha256.Sum256(bodyBytes))
		w.Header().Set("ETag", eTag)

		// Use the read body to unmarshal into the Plan struct
		var item Plan
		if err := json.Unmarshal(bodyBytes, &item); err != nil {
			fmt.Println("Error unmarshalling the json request")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Proceed with validation and other logic as before
		if err := validatePlan(item); err != nil {
			fmt.Println("Validation error:", err)
			http.Error(w, "Bad request: "+err.Error(), http.StatusBadRequest)
			return
		}

		av, err := attributevalue.MarshalMap(item)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if _, exists := av["ObjectId"]; !exists || av["ObjectId"].(*types.AttributeValueMemberS).Value == "" {
			http.Error(w, "createItem() - ObjectId (primary key) is missing or empty", http.StatusInternalServerError)
			return
		}

		// Store the ETag along with the item in DynamoDB
		av["ETag"] = &types.AttributeValueMemberS{Value: eTag}
		if err := createItem(svc, tableName, av, eTag); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Println("ETAG in POST:", eTag)

		w.WriteHeader(http.StatusCreated)
	}
}

func getItemHandler(svc *dynamodb.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		objectId := vars["ObjectId"]
		key := map[string]types.AttributeValue{
			"ObjectId": &types.AttributeValueMemberS{Value: objectId},
		}

		// Retrieve the item and its ETag from DynamoDB
		item, eTag, err := getItem(svc, tableName, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Set the ETag header in the response
		w.Header().Set("ETag", eTag)

		fmt.Println("ETAG retrieved from DB", eTag)

		// Check if the request header includes an If-None-Match field
		ifMatch := r.Header.Get("If-None-Match")
		fmt.Println("ETAG from GET", ifMatch)

		if ifMatch == eTag {
			w.WriteHeader(http.StatusNotModified)
			return
		}

		// If the ETag does not match, encode and return the item as usual
		json.NewEncoder(w).Encode(item)
	}
}

func updateItemHandler(svc *dynamodb.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		objectId := vars["objectId"]
		// You would typically parse the body for the update expression and attribute values
		// This is a simplified example
		updateExpression := "SET #attrName = :attrValue"
		expressionAttributeValues := map[string]types.AttributeValue{
			":attrValue": &types.AttributeValueMemberS{Value: "newValue"},
		}
		key := map[string]types.AttributeValue{
			"objectId": &types.AttributeValueMemberS{Value: objectId},
		}

		if err := updateItem(svc, tableName, key, updateExpression, expressionAttributeValues); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

func deleteItemHandler(svc *dynamodb.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		objectId := vars["ObjectId"]
		key := map[string]types.AttributeValue{
			"ObjectId": &types.AttributeValueMemberS{Value: objectId},
		}

		if err := deleteItem(svc, tableName, key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

func createTable(svc *dynamodb.Client, tableName string) {
	// Call waitForTableToBeReady after creating the table
	// err := waitForTableToBeReady(svc, tableName)
	// if err != nil {
	// 	log.Fatalf("Failed to wait for table readiness: %s", err)
	// }
	//Create a table
	input := &dynamodb.CreateTableInput{
		TableName: aws.String("PLAN_TABLE"), // Replace with your actual table name
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("ObjectId"),
				KeyType:       types.KeyTypeHash, // Use the types.KeyTypeHash constant
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("ObjectId"),
				AttributeType: types.ScalarAttributeTypeS, // Use the types.ScalarAttributeTypeS constant
			},
		},
		BillingMode: types.BillingModeProvisioned, // Use BillingMode instead of ProvisionedThroughput if it's deprecated
		ProvisionedThroughput: &types.ProvisionedThroughput{ // Ensure ProvisionedThroughput is correctly referenced
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}

	_, err := svc.CreateTable(context.TODO(), input)
	if err != nil {
		log.Fatalf("Got error calling CreateTable: %s", err)
	}

	// Your existing CreateTable logic here
	fmt.Printf("Creating table %s...\n", *input.TableName)
}

// waitForTableToBeReady waits for the DynamoDB table to be in the ACTIVE state.
func waitForTableToBeReady(svc *dynamodb.Client, tableName string) error {
	for {
		resp, err := svc.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			return err
		}

		if resp.Table.TableStatus == types.TableStatusActive {
			fmt.Printf("Table %s is now ACTIVE.\n", tableName)
			break
		}

		fmt.Printf("Waiting for table %s to be ACTIVE...\n", tableName)
		time.Sleep(5 * time.Second) // Wait for 5 seconds before checking again
	}

	return nil
}

// // Custom UnmarshalJSON method to handle the date format
// func (cd *CustomDate) UnmarshalJSON(input []byte) error {
// 	strInput := string(input)
// 	strInput = strInput[1 : len(strInput)-1] // Strip quotes
// 	newTime, err := time.Parse("02-01-2006", strInput)
// 	if err != nil {
// 		return err
// 	}
// 	cd.Time = newTime
// 	return nil
// }

func createItem(svc *dynamodb.Client, tableName string, av map[string]types.AttributeValue, eTag string) error {
	av["ETag"] = &types.AttributeValueMemberS{Value: eTag}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}

	formatted, err := json.MarshalIndent(input, "", "    ") // Indent with four spaces
	if err != nil {
		log.Fatalf("Error formatting input: %v", err)
	}

	fmt.Printf("INPUT: %s\n", string(formatted))

	_, err = svc.PutItem(context.TODO(), input)
	return err
}

func getItem(svc *dynamodb.Client, tableName string, key map[string]types.AttributeValue) (*Plan, string, error) {
	input := &dynamodb.GetItemInput{
		Key:       key,
		TableName: aws.String(tableName),
	}

	result, err := svc.GetItem(context.TODO(), input)
	if err != nil {
		return nil, "", err
	}

	plan := Plan{}
	err = attributevalue.UnmarshalMap(result.Item, &plan)
	if err != nil {
		return nil, "", err
	}

	// Extract the ETag from the result item
	eTagValue, exists := result.Item["ETag"]
	if !exists {
		return &plan, "", nil // ETag might not exist for some items
	}
	eTag, ok := eTagValue.(*types.AttributeValueMemberS)
	if !ok {
		return &plan, "", errors.New("ETag format is invalid")
	}

	// return &plan, nil
	return &plan, eTag.Value, nil

}

func updateItem(svc *dynamodb.Client, tableName string, key map[string]types.AttributeValue, updateExpression string, expressionAttributeValues map[string]types.AttributeValue) error {
	input := &dynamodb.UpdateItemInput{
		Key:                       key,
		TableName:                 aws.String(tableName),
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionAttributeValues,
	}

	_, err := svc.UpdateItem(context.TODO(), input)
	return err
}

func deleteTable(svc *dynamodb.Client, tableName string) {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}

	_, err := svc.DeleteTable(context.TODO(), input)
	if err != nil {
		log.Fatalf("Got error calling DeleteTable: %s", err)
	}

	fmt.Printf("Deleted table %s\n", tableName)

	// Optionally, you can include code here to verify the table has been deleted,
	// similar to the waitForTableToBeReady function in your createTable logic.
}

func deleteItem(svc *dynamodb.Client, tableName string, key map[string]types.AttributeValue) error {
	input := &dynamodb.DeleteItemInput{
		Key:       key,
		TableName: aws.String(tableName),
	}

	_, err := svc.DeleteItem(context.TODO(), input)
	return err
}
