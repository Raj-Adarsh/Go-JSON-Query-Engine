package crud

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gin-gonic/gin"

	"crud_with_dynamodb/db"
	"crud_with_dynamodb/models"
	"crud_with_dynamodb/queue"
)

func validatePlan(plan models.Plan) error {
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

const TOP_LEVEL_OBJECTID = "12xvxc345ssdsds-508"

func CreateItemHandler(svc *dynamodb.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Step 1: Check if the table exists
		_, err := svc.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(db.TableName),
		})

		if err != nil {
			var notFoundException *types.ResourceNotFoundException
			if errors.As(err, &notFoundException) {
				// Step 2: Table does not exist, so create it
				CreateTable(svc, db.TableName)
			} else {
				log.Fatalf("Failed to describe table: %s", err)
			}
		} else {
			// Table exists
			fmt.Printf("Table %s already exists.\n", db.TableName)
		}

		bodyBytes, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, errors.New("error reading request body"))
			return
		}

		log.Printf("Raw JSON request body: %s\n", string(bodyBytes))

		var item models.Plan
		if err := json.Unmarshal(bodyBytes, &item); err != nil {
			c.AbortWithError(http.StatusBadRequest, errors.New("error unmarshalling the json request"))
			return
		}

		err = models.ValidateStruct(item)
		if err != nil {
			fmt.Println(err)
			c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bad request: %v", err))
			return
		}

		if item.PlanCostShares.Copay == nil || item.PlanCostShares.Deductible == nil {
			c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bad request: %v", err))
		}

		if err := validatePlan(item); err != nil {
			c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bad request: %v", err))
			return
		}

		canonicalBytes, err := json.Marshal(item)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling item to JSON: %v", err))
			return
		}

		eTag := fmt.Sprintf(`"%x"`, sha256.Sum256(canonicalBytes))
		c.Header("ETag", eTag)

		flatMap, err := FlattenPlan(item)
		if err != nil {
			log.Fatalf("Error flattening plan: %v", err)
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error flattening plan: %v", err))
			return
		}

		// Iterate over flatMap and store each block in the database
		for objectId, jsonBlock := range flatMap {
			fmt.Printf("ObjectId: %s, JSON Block: %s\n", objectId, string(jsonBlock))

			// Convert jsonBlock back to a map for attributevalue.MarshalMap
			var blockMap map[string]interface{}
			if err := json.Unmarshal(jsonBlock, &blockMap); err != nil {
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error preparing data for objectId %s: %v", objectId, err))
				return
			}

			av, err := attributevalue.MarshalMap(blockMap)
			if err != nil {
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling data for objectId %s: %v", objectId, err))
				return
			}

			avJSON, err := json.MarshalIndent(av, "", "    ")
			if err != nil {
				log.Printf("Error marshalling av to JSON: %v", err)
			} else {
				log.Printf("AV data structure: %s", avJSON)
			}

			// Perform the database operation to store the item
			if err := createItem(svc, db.TableName, av); err != nil {
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error storing item with objectId %s: %v", objectId, err))
				return
			}

		}

		queue.PublishPlanCreation(item, "create")

		c.Status(http.StatusCreated)
	}
}

func GetItemHandler(svc *dynamodb.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		if len(c.Request.URL.RawQuery) > 0 {
			c.Status(http.StatusBadRequest)
			return
		}
		objectId := TOP_LEVEL_OBJECTID
		key := map[string]types.AttributeValue{
			"objectId": &types.AttributeValueMemberS{Value: objectId},
		}

		currentItem, _, err := getItem(svc, db.TableName, key)
		if err != nil {
			c.AbortWithError(http.StatusNoContent, fmt.Errorf("error fetching current item: %v", err))
			return
		}

		// Generate ETag for the current item
		currentItemJSON, err := json.Marshal(currentItem)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling current item to JSON: %v", err))
			return
		}

		fmt.Println("Current item in get json", string(currentItemJSON))

		ifMatchETag := c.GetHeader("If-None-Match")
		fmt.Println("Etag recieved in GETALL req:", ifMatchETag)

		currentETag := fmt.Sprintf(`"%x"`, sha256.Sum256(currentItemJSON))

		fmt.Println("Current ETag in GETALL-computed", currentETag)
		// fmt.Println("Current ETag in Getall-computed", currentETag_func)
		// Compare the ETag from If-Match header with the current item's ETag
		if ifMatchETag == currentETag {
			c.Status(http.StatusNotModified)
			return
		}

		// Write the JSON response
		c.Data(http.StatusOK, "application/json", currentItemJSON)
	}
}

// func GetItemHandler(svc *dynamodb.Client) gin.HandlerFunc {
// 	return func(c *gin.Context) {
// 		// Check if the request is for scanning the table
// 		// if c.Request.URL.RawQuery == "scan=true" {
// 		// Perform a scan operation on the DynamoDB table
// 		input := &dynamodb.ScanInput{
// 			TableName: aws.String(db.TableName),
// 		}

// 		result, err := svc.Scan(context.TODO(), input)
// 		if err != nil {
// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error scanning table: %v", err))
// 			return
// 		}

// 		// Convert the result to JSON
// 		var itemsJSON []map[string]interface{}
// 		for _, item := range result.Items {
// 			var itemMap map[string]interface{}
// 			if err := attributevalue.UnmarshalMap(item, &itemMap); err != nil {
// 				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error converting item to JSON: %v", err))
// 				return
// 			}
// 			itemsJSON = append(itemsJSON, itemMap)
// 		}

// 		itemsJSONBytes, err := json.Marshal(itemsJSON)
// 		if err != nil {
// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error converting result to JSON: %v", err))
// 			return
// 		}

// 		c.Data(http.StatusOK, "application/json", itemsJSONBytes)

// 		// Handle other cases or return an error for invalid requests
// 		// c.Status(http.StatusBadRequest)
// 	}
// }

func fetchPlanItem(svc *dynamodb.Client, tableName string, objectId string) (*models.Plan, error) {
	key := map[string]types.AttributeValue{
		"objectId": &types.AttributeValueMemberS{Value: objectId},
	}

	log.Println("Fetching item with key:", key)

	plan, _, err := getItem(svc, db.TableName, key)
	if err != nil {
		if err.Error() == "item not found" {
			return nil, err
		}
		return nil, err
	}

	// Marshal the plan into JSON for logging
	planJSON, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		log.Println("Error marshalling plan to JSON:", err)
		return nil, err
	}

	log.Printf("DynamoDB attributes after unmarshal: %s\n", string(planJSON))

	return plan, nil
}

func GetItemByObjectIDHandler(svc *dynamodb.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		objectId := c.Param("objectId")
		if objectId == "" {
			c.AbortWithError(http.StatusBadRequest, errors.New("ObjectId must be provided"))
			return
		}

		// Construct the Scan input with a filter expression for the objectId
		scanInput := &dynamodb.ScanInput{
			TableName:        aws.String(db.TableName),
			FilterExpression: aws.String("objectId = :objectId"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":objectId": &types.AttributeValueMemberS{Value: objectId},
			},
		}

		// Perform the scan operation
		result, err := svc.Scan(context.TODO(), scanInput)
		if err != nil {
			log.Println("Error scanning table:", err)
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		// Check if any items matched the filter
		if len(result.Items) == 0 {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		// Assuming you only expect one item to match, otherwise you'll need to handle multiple items
		matchedItem := result.Items[0]

		var itemMap map[string]interface{}
		err = attributevalue.UnmarshalMap(matchedItem, &itemMap)
		if err != nil {
			log.Println("Error unmarshalling matched item:", err)
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		// Filter out null or empty fields
		filteredItem := make(map[string]interface{})
		for key, value := range itemMap {
			if value != nil && value != "" {
				filteredItem[key] = value
			}
		}

		// Marshal the filtered item into standard JSON
		matchedItemJSON, err := json.Marshal(filteredItem)
		if err != nil {
			log.Println("Error marshalling filtered item to JSON:", err)
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		c.Data(http.StatusOK, "application/json", matchedItemJSON)
	}
}

func UpdateItemHandler(svc *dynamodb.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		objectId := c.Param("objectId")
		if objectId == "" {
			c.AbortWithError(http.StatusBadRequest, errors.New("ObjectId must be provided"))
			return
		}

		bodyBytes, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, errors.New("error reading request body"))
			return
		}

		// Unmarshal the body into your struct
		var item models.Plan
		if err := json.Unmarshal(bodyBytes, &item); err != nil {
			c.AbortWithError(http.StatusBadRequest, errors.New("error unmarshalling the json request"))
			return
		}

		err = models.ValidateStruct(item)
		if err != nil {
			fmt.Println(err)
			c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bad request: %v", err))
			return
		}

		if item.PlanCostShares.Copay == nil || item.PlanCostShares.Deductible == nil {
			c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bad request: %v", err))
		}

		if err := validatePlan(item); err != nil {
			c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bad request: %v", err))
			return
		}

		canonicalBytes, err := json.Marshal(item)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling item to JSON: %v", err))
			return
		}

		requestETag := fmt.Sprintf(`"%x"`, sha256.Sum256(canonicalBytes))

		log.Printf("Plan after unmarshalling JSON: %s\n", string(canonicalBytes))

		// Retrieve the ETag for the current state of the resource on the server
		currentETag, err := getCurrentResourceETag(svc, db.TableName, TOP_LEVEL_OBJECTID)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error retrieving current resource ETag: %v", err))
			return
		}

		fmt.Println("Current ETag", currentETag)

		ifMatch := c.GetHeader("If-Match")
		fmt.Println("ETag for if-match", ifMatch)

		// Compare If-Match ETag with current resource's ETag
		if ifMatch != currentETag {
			// ETags do not match, the resource has changed since the ETag was generated
			c.AbortWithStatus(http.StatusPreconditionFailed)
			return
		}

		// Flatten the plan to get the individual items
		flatMap, err := FlattenPlan(item)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error flattening plan: %v", err))
			return
		}

		// Perform a Scan to get all the item keys
		scanOutput, err := svc.Scan(context.TODO(), &dynamodb.ScanInput{
			TableName:       aws.String(db.TableName),
			AttributesToGet: []string{"objectId"},
		})
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error scanning table: %v", err))
			return
		}

		// Use BatchWriteItem to delete the scanned items
		var writeRequests []types.WriteRequest
		for _, item := range scanOutput.Items {
			writeRequests = append(writeRequests, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: item,
				},
			})
		}

		// Delete items in batches of 25
		for len(writeRequests) > 0 {
			endIndex := 25
			if len(writeRequests) < 25 {
				endIndex = len(writeRequests)
			}

			batchInput := &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					db.TableName: writeRequests[:endIndex],
				},
			}
			_, err = svc.BatchWriteItem(context.TODO(), batchInput)
			if err != nil {
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error in batch delete: %v", err))
				return
			}
			writeRequests = writeRequests[endIndex:]
		}
		// Iterate over flatMap and store each block in the database
		for objectId, jsonBlock := range flatMap {
			fmt.Printf("ObjectId: %s, JSON Block: %s\n", objectId, string(jsonBlock))

			// Convert jsonBlock back to a map for attributevalue.MarshalMap
			var blockMap map[string]interface{}
			if err := json.Unmarshal(jsonBlock, &blockMap); err != nil {
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error preparing data for objectId %s: %v", objectId, err))
				return
			}

			av, err := attributevalue.MarshalMap(blockMap)
			if err != nil {
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling data for objectId %s: %v", objectId, err))
				return
			}

			avJSON, err := json.MarshalIndent(av, "", "    ")
			if err != nil {
				log.Printf("Error marshalling av to JSON: %v", err)
			} else {
				log.Printf("AV data structure: %s", avJSON)
			}
			if err := createItem(svc, db.TableName, av); err != nil {
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error storing item with objectId %s: %v", objectId, err))
				return
			}
		}

		c.Header("ETag", requestETag)

		c.Status(http.StatusOK)
	}
}

func getCurrentResourceETag(svc *dynamodb.Client, tableName, objectId string) (string, error) {
	plan, err := fetchPlanItem(svc, db.TableName, objectId)
	if err != nil {
		return "", fmt.Errorf("error in fetching plan item: %w", err)
	}

	// Marshal the plan into JSON
	planJSON, err := json.Marshal(plan)
	if err != nil {
		log.Println("Error marshalling plan to JSON:", err)
		// c.AbortWithError(http.StatusInternalServerError, err)
		return "", fmt.Errorf("failed to marshal item to JSON: %w", err)
	}

	log.Printf("Plan after unmarshalling JSON: %s\n", string(planJSON))

	eTag := fmt.Sprintf(`"%x"`, sha256.Sum256(planJSON))
	fmt.Println("ETAG in getCurrentResourceETag: ", eTag)

	return eTag, nil
}

func DeleteItemHandler(svc *dynamodb.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		if len(c.Request.URL.RawQuery) > 0 {
			c.Status(http.StatusBadRequest)
			return
		}
		objectId := c.Param("objectId")
		key := map[string]types.AttributeValue{
			"objectId": &types.AttributeValueMemberS{Value: objectId},
		}

		if err := deleteItem(svc, db.TableName, key); err != nil {
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		c.Status(http.StatusNoContent)
	}
}

func CreateTable(svc *dynamodb.Client, tableName string) {
	input := &dynamodb.CreateTableInput{
		TableName: aws.String("PLAN_TABLE"),
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("objectId"),
				KeyType:       types.KeyTypeHash,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("objectId"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}

	_, err := svc.CreateTable(context.TODO(), input)
	if err != nil {
		log.Fatalf("Got error calling CreateTable: %s", err)
	}

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
		time.Sleep(5 * time.Second)
	}

	return nil
}

func createItem(svc *dynamodb.Client, tableName string, av map[string]types.AttributeValue) error {

	// Check if the item already exists
	existingItem, _, err := getItem(svc, tableName, map[string]types.AttributeValue{
		"objectId": av["objectId"],
	})
	if err != nil && err.Error() != "item not found" {
		// An error occurred while checking for the item, other than not finding it
		return fmt.Errorf("error checking for existing item: %v", err)
	}
	if existingItem != nil {
		// Item already exists, handle accordingly
		return fmt.Errorf("item with objectId %s already exists", av["objectId"].(*types.AttributeValueMemberS).Value)
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}

	formatted, err := json.MarshalIndent(input, "", "    ")
	if err != nil {
		log.Fatalf("Error formatting input: %v", err)
	}

	fmt.Printf("INPUT in the DB: %s\n", string(formatted))

	_, err = svc.PutItem(context.TODO(), input)
	return err
}

func getItem(svc *dynamodb.Client, tableName string, key map[string]types.AttributeValue) (*models.Plan, string, error) {
	input := &dynamodb.GetItemInput{
		Key:       key,
		TableName: aws.String(tableName),
	}

	result, err := svc.GetItem(context.TODO(), input)
	if err != nil {
		return nil, "", err
	}

	if len(result.Item) == 0 {
		return nil, "", errors.New("item not found")
	}

	var plan models.Plan
	err = attributevalue.UnmarshalMap(result.Item, &plan)
	if err != nil {
		return nil, "", err
	}

	return &plan, "", nil

}

func updateItem(svc *dynamodb.Client, tableName string, key map[string]types.AttributeValue, updateExpression string, expressionAttributeValues map[string]types.AttributeValue, expressionAttributeNames map[string]string) error {
	input := &dynamodb.UpdateItemInput{
		Key:                       key,
		TableName:                 aws.String(tableName),
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionAttributeValues,
		ExpressionAttributeNames:  expressionAttributeNames, // Add this line
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

}

func deleteItem(svc *dynamodb.Client, tableName string, key map[string]types.AttributeValue) error {
	input := &dynamodb.DeleteItemInput{
		Key:       key,
		TableName: aws.String(tableName),
	}

	_, err := svc.DeleteItem(context.TODO(), input)
	return err
}

func FlattenPlan(plan models.Plan) (map[string]json.RawMessage, error) {
	flatMap := make(map[string]json.RawMessage)

	// Serialize the top-level plan and add it to the map
	planJSON, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}
	flatMap[plan.ObjectId] = planJSON

	// Serialize PlanCostShares
	costSharesJSON, err := json.Marshal(plan.PlanCostShares)
	if err != nil {
		return nil, err
	}
	flatMap[plan.PlanCostShares.ObjectId] = costSharesJSON

	// Iterate over LinkedPlanServices and serialize each one along with its LinkedService and PlanServiceCostShares
	for _, service := range plan.LinkedPlanServices {
		serviceJSON, err := json.Marshal(service)
		if err != nil {
			return nil, err
		}
		flatMap[service.ObjectId] = serviceJSON

		linkedServiceJSON, err := json.Marshal(service.LinkedService)
		if err != nil {
			return nil, err
		}
		flatMap[service.LinkedService.ObjectId] = linkedServiceJSON

		costSharesServiceJSON, err := json.Marshal(service.PlanServiceCostShares)
		if err != nil {
			return nil, err
		}
		flatMap[service.PlanServiceCostShares.ObjectId] = costSharesServiceJSON
	}

	return flatMap, nil
}

func PatchItemHandler(svc *dynamodb.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		objectId := c.Param("objectId")
		if objectId == "" {
			c.AbortWithError(http.StatusBadRequest, errors.New("ObjectId must be provided"))
			return
		}

		key := map[string]types.AttributeValue{
			"objectId": &types.AttributeValueMemberS{Value: objectId},
		}

		// Read the If-Match header to get the ETag sent by the client
		ifMatchETag := c.GetHeader("If-Match")
		fmt.Println("Etag recieved in PATCH:", ifMatchETag)

		currentItem, _, err := getItem(svc, db.TableName, key)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error fetching current item: %v", err))
			return
		}

		// Generate ETag for the current item
		currentItemJSON, err := json.Marshal(currentItem)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling current item to JSON: %v", err))
			return
		}

		fmt.Println("Current item in patch json", string(currentItemJSON))
		currentETag := fmt.Sprintf(`"%x"`, sha256.Sum256(currentItemJSON))
		fmt.Println("Current ETag in Patch-computed 1", currentETag)
		// Compare the ETag from If-Match header with the current item's ETag
		// if ifMatchETag != currentETag {
		// 	c.AbortWithStatus(http.StatusPreconditionFailed)
		// 	return
		// }

		bodyBytes, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, errors.New("error reading request body"))
			return
		}

		var item models.Plan
		if err := json.Unmarshal(bodyBytes, &item); err != nil {
			c.AbortWithError(http.StatusBadRequest, errors.New("error unmarshalling the json request"))
			return
		}

		err = models.ValidateStruct(item)
		if err != nil {
			fmt.Println(err)
			c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bad request: %v", err))
			return
		}

		if item.PlanCostShares.Copay == nil || item.PlanCostShares.Deductible == nil {
			c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bad request: %v", err))
		}

		if err := validatePlan(item); err != nil {
			c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bad request: %v", err))
			return
		}

		fmt.Println("object id", item.ObjectId)
		fmt.Println("passed object id", objectId)

		// Fetch the current plan item from the database
		currentPlan, err := fetchPlanItem(svc, db.TableName, TOP_LEVEL_OBJECTID)
		if err != nil {
			log.Fatalf("Error fetching current plan: %v", err)
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error fetching current plan: %v", err))
			return
		}

		// for _, newService := range item.LinkedPlanServices {
		// 	// isNew := true
		// 	// for _, currentService := range currentItem.LinkedPlanServices {
		// 	// 	if newService.ObjectId == currentService.ObjectId {
		// 	// 		// This is an existing service, not a new one
		// 	// 		if err := updateItemInDynamoDB(svc, db.TableName, newService); err != nil {
		// 	// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error updating item with objectId %s: %v", newService.ObjectId, err))
		// 	// 			return
		// 	// 		}
		// 	// 		isNew = false
		// 	// 		break
		// 	// 	}
		// 	// }
		// 	exists, err := checkItemExists(svc, db.TableName, newService.ObjectId)
		// 	if err != nil {
		// 		c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error checking if item exists: %v", err))
		// 		return
		// 	}
		// 	if exists {
		// 		// Item exists, so update it
		// 		fmt.Println("Item already exists, update it ..")
		// 		if err := updateItemInDynamoDB(svc, db.TableName, newService); err != nil {
		// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error updating item with objectId %s: %v", newService.ObjectId, err))
		// 			return
		// 		}
		// 	} else {
		// 		// This is a new service, so do something with its ID
		// 		fmt.Println("New service ID:", newService.ObjectId)
		// 		// Use the custom marshaling function instead of attributevalue.MarshalMap
		// 		av, err := marshalMapUsingJSONTags(newService.LinkedService)
		// 		if err != nil {
		// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling linkedService: %v", err))
		// 			return
		// 		}

		// 		avAttributeValue, err := attributevalue.MarshalMap(av)
		// 		if err != nil {
		// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error converting to AttributeValue map: %v", err))
		// 			return
		// 		}
		// 		avAttributeValue["objectId"] = &types.AttributeValueMemberS{Value: newService.LinkedService.ObjectId}

		// 		if err := createItem(svc, db.TableName, avAttributeValue); err != nil {
		// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error creating linkedService: %v", err))
		// 			return
		// 		}

		// 		av, err = marshalMapUsingJSONTags(newService.PlanServiceCostShares)
		// 		if err != nil {
		// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling PlanServiceCostShares: %v", err))
		// 			return
		// 		}

		// 		avAttributeValue, err = attributevalue.MarshalMap(av)
		// 		if err != nil {
		// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error converting to AttributeValue map: %v", err))
		// 			return
		// 		}
		// 		avAttributeValue["objectId"] = &types.AttributeValueMemberS{Value: newService.PlanServiceCostShares.ObjectId}

		// 		if err := createItem(svc, db.TableName, avAttributeValue); err != nil {
		// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error creating PlanServiceCostShares: %v", err))
		// 			return
		// 		}

		// 		av, err = marshalMapUsingJSONTags(newService)
		// 		if err != nil {
		// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling newService: %v", err))
		// 			return
		// 		}
		// 		avAttributeValue, err = attributevalue.MarshalMap(av)
		// 		if err != nil {
		// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error converting to AttributeValue map: %v", err))
		// 			return
		// 		}
		// 		avAttributeValue["objectId"] = &types.AttributeValueMemberS{Value: newService.ObjectId}

		// 		if err := createItem(svc, db.TableName, avAttributeValue); err != nil {
		// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error creating newService: %v", err))
		// 			return
		// 		}
		// 	}
		// }
		newServices := item.LinkedPlanServices
		for _, newService := range newServices {
			// Check if the service already exists in currentPlan
			serviceExists := false
			for i, existingService := range currentPlan.LinkedPlanServices {
				if existingService.ObjectId == newService.ObjectId {
					// Update the existing service
					currentPlan.LinkedPlanServices[i] = newService
					serviceExists = true
					break
				}
			}
			if !serviceExists {
				// Service does not exist, so add it to the LinkedPlanServices
				currentPlan.LinkedPlanServices = append(currentPlan.LinkedPlanServices, newService)
			}
		}

		// Update the top-level object in the database with the updated LinkedPlanServices
		// UpdateItemInDynamoDB function is assumed to update the item in DynamoDB
		if err := updateItemInDynamoDB(svc, db.TableName, currentPlan); err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error updating top-level object: %v", err))
			return
		}

		// // Check if the patch is for linkedPlanServices
		// if len(item.LinkedPlanServices) > 0 {
		// 	// Add the new linkedPlanServices to the current plan's array
		// 	currentPlan.LinkedPlanServices = append(currentPlan.LinkedPlanServices, item.LinkedPlanServices...)
		// }

		// flatMap, err := FlattenPlan(item)
		// if err != nil {
		// 	log.Fatalf("Error flattening plan: %v", err)
		// 	c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error flattening plan: %v", err))
		// 	return
		// }

		// // This example will update the item with the new flattened data
		// for _, jsonBlock := range flatMap {
		// 	// Convert jsonBlock back to a map for attributevalue.MarshalMap
		// 	var updateMap map[string]interface{}
		// 	if err := json.Unmarshal(jsonBlock, &updateMap); err != nil {
		// 		c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error preparing update data: %v", err))
		// 		return
		// 	}

		// 	av, err := attributevalue.MarshalMap(updateMap)
		// 	if err != nil {
		// 		c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling update data: %v", err))
		// 		return
		// 	}

		// 	key := map[string]types.AttributeValue{
		// 		"objectId": &types.AttributeValueMemberS{Value: objectId},
		// 	}

		// 	updateExpression := "SET"
		// 	expressionAttributeNames := make(map[string]string) // Correctly initialize the map
		// 	expressionAttributeValues := map[string]types.AttributeValue{}
		// 	i := 0
		// 	for k, v := range av {
		// 		if k == "objectId" {
		// 			continue
		// 		}
		// 		// Use expression attribute names for reserved keywords
		// 		expressionAttributeName := fmt.Sprintf("#attr%d", i)
		// 		expressionAttributeValue := fmt.Sprintf(":val%d", i)

		// 		updateExpression += fmt.Sprintf(" %s = %s,", expressionAttributeName, expressionAttributeValue)
		// 		expressionAttributeValues[expressionAttributeValue] = v
		// 		expressionAttributeNames[expressionAttributeName] = k
		// 		i++
		// 	}
		// 	updateExpression = strings.TrimSuffix(updateExpression, ",")

		// 	err = updateItem(svc, db.TableName, key, updateExpression, expressionAttributeValues, expressionAttributeNames)
		// 	if err != nil {
		// 		c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error updating item: %v", err))
		// 		return
		// 	}

		// }

		updatedItem, _, err := getItem(svc, db.TableName, map[string]types.AttributeValue{
			"objectId": &types.AttributeValueMemberS{Value: objectId},
		})
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error fetching updated item: %v", err))
			return
		}

		// Generate ETag for the updated item
		updatedItemJSON, err := json.Marshal(updatedItem)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling updated item to JSON: %v", err))
			return
		}

		updatedETag := fmt.Sprintf(`"%x"`, sha256.Sum256(updatedItemJSON))
		fmt.Println("Updated ETag in Patch-computed 2", updatedETag)
		c.Header("ETag", updatedETag)
		queue.PublishPlanCreation(item, "update")
		c.Status(http.StatusOK)
	}
}

func marshalMapUsingJSONTags(v interface{}) (map[string]interface{}, error) {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	typ := val.Type()

	result := make(map[string]interface{})
	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" {
			continue
		}
		key := strings.Split(jsonTag, ",")[0]
		value := val.Field(i).Interface()
		result[key] = value
	}
	return result, nil
}

func updateItemInDynamoDB(svc *dynamodb.Client, tableName string, newService interface{}) error {
	// First, marshal the newService into a map[string]AttributeValue
	av, err := attributevalue.MarshalMap(newService)
	if err != nil {
		return fmt.Errorf("failed to marshal new service: %w", err)
	}

	// Extract the objectId from the newService as it's needed for the update key
	objectId, ok := av["ObjectId"]
	if !ok {
		return fmt.Errorf("new service does not contain an objectId")
	}

	// Prepare the update expression
	updateExpression := "SET"
	expressionAttributeValues := make(map[string]types.AttributeValue)
	i := 0
	for key, value := range av {
		if key == "objectId" {
			// Skip the objectId since it's used as the key
			continue
		}
		updateExpression += fmt.Sprintf(" %s = :val%d,", key, i)
		expressionAttributeValues[fmt.Sprintf(":val%d", i)] = value
		i++
	}
	updateExpression = updateExpression[:len(updateExpression)-1] // Remove the trailing comma

	// Perform the update
	_, err = svc.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"objectId": objectId,
		},
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionAttributeValues,
		ReturnValues:              types.ReturnValueUpdatedNew,
	})
	if err != nil {
		return fmt.Errorf("failed to update item: %w", err)
	}

	return nil
}

func checkItemExists(svc *dynamodb.Client, tableName string, objectId string) (bool, error) {
	// Construct the key to search for
	key := map[string]types.AttributeValue{
		"objectId": &types.AttributeValueMemberS{Value: objectId},
	}

	// Call GetItem
	output, err := svc.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       key,
	})
	if err != nil {
		return false, fmt.Errorf("error fetching item: %w", err)
	}

	// If output.Item is not empty, the item exists
	fmt.Println("length of existing item output", len(output.Item))
	return len(output.Item) > 0, nil
}
