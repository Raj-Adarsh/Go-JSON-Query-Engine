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
)

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

		// After potentially modifying the item, marshal it back to JSON
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
			// Handle the error appropriately
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

			// // Add the objectId to the attribute values if not already present
			// if _, exists := av["objectId"]; !exists {
			// 	av["objectId"] = &types.AttributeValueMemberS{Value: objectId}
			// }

			// av["ETag"] = &types.AttributeValueMemberS{Value: eTag}

			// Perform the database operation to store the item
			if err := createItem(svc, db.TableName, av); err != nil {
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error storing item with objectId %s: %v", objectId, err))
				return
			}
		}

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

		// key := map[string]types.AttributeValue{
		// 	"objectId": &types.AttributeValueMemberS{Value: objectId},
		// }

		plan, err := fetchPlanItem(svc, db.TableName, objectId)

		// plan, _, err := getItem(svc, db.TableName, key)
		if err != nil {
			if err.Error() == "item not found" {
				c.AbortWithStatus(http.StatusNotFound)
				return
			}
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		// Marshal the plan into JSON
		planJSON, err := json.Marshal(plan)
		if err != nil {
			log.Println("Error marshalling plan to JSON:", err)
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		log.Printf("Plan after unmarshalling JSON: %s\n", string(planJSON))

		eTag := fmt.Sprintf(`"%x"`, sha256.Sum256(planJSON))
		fmt.Println("ETAG in GET: ", eTag)

		// // c.Header("ETag", eTag)

		ifMatch := c.GetHeader("If-None-Match")
		fmt.Println("ifMatch: ", ifMatch)
		if ifMatch == eTag {
			c.Status(http.StatusNotModified)
			return
		}

		// Write the JSON response
		c.Data(http.StatusOK, "application/json", planJSON)
		// c.JSON(http.StatusOK, item)

	}
}

func fetchPlanItem(svc *dynamodb.Client, tableName string, objectId string) (*models.Plan, error) {
	key := map[string]types.AttributeValue{
		"objectId": &types.AttributeValueMemberS{Value: objectId},
	}

	log.Println("Fetching item with key:", key)

	plan, _, err := getItem(svc, db.TableName, key)
	if err != nil {
		if err.Error() == "item not found" {
			// c.AbortWithStatus(http.StatusNotFound)
			return nil, err
		}
		// c.AbortWithError(http.StatusInternalServerError, err)
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

func findObjectByID(obj interface{}, objectId string) (interface{}, bool) {
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	// // Function to set _org field if missing or empty
	// setOrgField := func(obj interface{}) {
	// 	v := reflect.ValueOf(obj)
	// 	if v.Kind() == reflect.Ptr {
	// 		v = v.Elem()
	// 	}
	// 	if v.Kind() == reflect.Struct {
	// 		orgField := v.FieldByName("_org")
	// 		if orgField.IsValid() && orgField.CanSet() && orgField.Kind() == reflect.String {
	// 			if orgField.String() == "" {
	// 				orgField.SetString("example.com")
	// 			}
	// 		}
	// 	}
	// }

	// Check if the object itself matches the objectId
	if val.Kind() == reflect.Struct {
		for i := 0; i < val.NumField(); i++ {
			field := val.Field(i)
			typeField := val.Type().Field(i)
			if typeField.Name == "ObjectId" && field.Interface() == objectId {
				// setOrgField(obj)
				return obj, true
			}
		}
	}

	// Recursively check nested structs and slices
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		// Before recursing, check if the field is of kind struct or ptr (which could point to a struct)
		if field.Kind() == reflect.Struct || (field.Kind() == reflect.Ptr && field.Elem().Kind() == reflect.Struct) {
			if foundObj, found := findObjectByID(field.Interface(), objectId); found {
				fmt.Printf("Found object 1: %+v\n", foundObj)
				// setOrgField(foundObj)
				return foundObj, true
			}
		} else if field.Kind() == reflect.Slice {
			for j := 0; j < field.Len(); j++ {
				sliceElem := field.Index(j)
				// For slices, check if the element is a struct or a pointer to a struct before recursing
				if sliceElem.Kind() == reflect.Struct || (sliceElem.Kind() == reflect.Ptr && sliceElem.Elem().Kind() == reflect.Struct) {
					if foundObj, found := findObjectByID(sliceElem.Interface(), objectId); found {
						fmt.Printf("Found object 2: %+v\n", foundObj)
						// setOrgField(foundObj)
						return foundObj, true
					}
				}
			}
		}
	}

	return nil, false
}

func GetItemByObjectIDHandler(svc *dynamodb.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		objectId := c.Param("objectId")
		if objectId == "" {
			c.AbortWithError(http.StatusBadRequest, errors.New("ObjectId must be provided"))
			return
		}

		// object_id := TOP_LEVEL_OBJECTID
		// key := map[string]types.AttributeValue{
		// 	"objectId": &types.AttributeValueMemberS{Value: object_id},
		// }

		// plan, _, err := getItem(svc, db.TableName, key)
		// if err != nil {
		// 	if err.Error() == "item not found" {
		// 		c.AbortWithStatus(http.StatusNotFound)
		// 		return
		// 	}
		// 	c.AbortWithError(http.StatusInternalServerError, err)
		// 	return
		// }

		plan, err := fetchPlanItem(svc, db.TableName, TOP_LEVEL_OBJECTID) // Use the top-level objectId for fetching
		if err != nil {
			log.Println("Error fetching json data for TOP_LEVEL_OBJECTID")
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		// // Marshal the plan into JSON for logging
		// planJSON, err := json.MarshalIndent(plan, "", "    ")
		// if err != nil {
		// 	log.Println("Error marshalling plan to JSON:", err)
		// 	return
		// }

		// log.Printf("DynamoDB attributes after unmarshal in object by ID: %s\n", string(planJSON))

		// log.Println("Plan is:", plan)

		if response, found := findObjectByID(plan, objectId); found {
			log.Println("Found ObjectID Match")

			// Assuming you want to inspect or modify the response before sending it back
			responseJSON, err := json.Marshal(response)
			if err != nil {
				log.Println("Error marshalling found object to JSON:", err)
				c.AbortWithError(http.StatusInternalServerError, err)
				return
			}

			// Optionally, log the JSON or inspect it
			log.Printf("Response JSON: %s\n", string(responseJSON))

			// If you need to modify the response, unmarshal it into a map or struct here
			// For example, unmarshal into a map to inspect or modify
			var responseMap map[string]interface{}
			err = json.Unmarshal(responseJSON, &responseMap)
			if err != nil {
				log.Println("Error unmarshalling response JSON:", err)
				c.AbortWithError(http.StatusInternalServerError, err)
				return
			}

			// Perform any necessary checks or modifications
			// For example, check if 'org' field is present and not empty
			if org, ok := responseMap["_org"].(string); !ok || org == "" {
				log.Println("Org field is missing or empty")
				// Handle the missing or empty 'org' field as needed
				// responseMap["_org"] = "example.com"
			}

			// Finally, send the response
			// If modifications were made, marshal the map back into JSON
			modifiedResponseJSON, err := json.Marshal(responseMap)
			if err != nil {
				log.Println("Error marshalling modified response:", err)
				c.AbortWithError(http.StatusInternalServerError, err)
				return
			}

			c.Data(http.StatusOK, "application/json", modifiedResponseJSON)
			return
		}

		c.AbortWithStatus(http.StatusNotFound)
	}
}

// func UpdateItemHandler(svc *dynamodb.Client) gin.HandlerFunc {
// 	return func(c *gin.Context) {
// 		objectId := c.Param("objectId")
// 		if objectId == "" {
// 			c.AbortWithError(http.StatusBadRequest, errors.New("ObjectId must be provided"))
// 			return
// 		}

// 		bodyBytes, err := ioutil.ReadAll(c.Request.Body)
// 		if err != nil {
// 			c.AbortWithError(http.StatusInternalServerError, errors.New("error reading request body"))
// 			return
// 		}

// 		var item models.Plan
// 		if err := json.Unmarshal(bodyBytes, &item); err != nil {
// 			c.AbortWithError(http.StatusBadRequest, errors.New("error unmarshalling the json request"))
// 			return
// 		}

// 		flatMap, err := FlattenPlan(item)
// 		if err != nil {
// 			log.Fatalf("Error flattening plan: %v", err)
// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error flattening plan: %v", err))
// 			return
// 		}

// 		// Iterate over flatMap and update each block in the database
// 		for _, jsonBlock := range flatMap {
// 			// Convert jsonBlock back to a map for attributevalue.MarshalMap
// 			var updateMap map[string]interface{}
// 			if err := json.Unmarshal(jsonBlock, &updateMap); err != nil {
// 				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error preparing update data: %v", err))
// 				return
// 			}

// 			av, err := attributevalue.MarshalMap(updateMap)
// 			if err != nil {
// 				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling update data: %v", err))
// 				return
// 			}

// 			// Construct the update expression and attribute values
// 			updateExpression := "SET"
// 			expressionAttributeValues := map[string]types.AttributeValue{}
// 			i := 0
// 			for k, v := range av {
// 				updateExpression += fmt.Sprintf(" %s = :val%d,", k, i)
// 				expressionAttributeValues[fmt.Sprintf(":val%d", i)] = v
// 				i++
// 			}
// 			updateExpression = updateExpression[:len(updateExpression)-1] // Remove the last comma

// 			// Perform the update operation
// 			key := map[string]types.AttributeValue{
// 				"objectId": &types.AttributeValueMemberS{Value: objectId},
// 			}
// 			err = updateItem(svc, db.TableName, key, updateExpression, expressionAttributeValues)
// 			if err != nil {
// 				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error updating item: %v", err))
// 				return
// 			}
// 		}

// 		c.Status(http.StatusOK)
// 	}
// }

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

		_, _, err := getItem(svc, db.TableName, key)
		if err != nil {
			if err.Error() == "item not found" {
				c.AbortWithStatus(http.StatusNotFound)
				return
			}
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		// // Set ETag header before deleting
		// c.Header("ETag", eTag)

		// // Check If-Match header for ETag validation
		// ifMatch := c.GetHeader("If-Match")
		// if ifMatch != eTag {
		// 	c.AbortWithStatus(http.StatusPreconditionFailed)
		// 	return
		// }

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
	// av["ETag"] = &types.AttributeValueMemberS{Value: eTag}

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

// func updateItem(svc *dynamodb.Client, tableName string, key map[string]types.AttributeValue, updateExpression string, expressionAttributeValues map[string]types.AttributeValue) error {
// 	input := &dynamodb.UpdateItemInput{
// 		Key:                       key,
// 		TableName:                 aws.String(tableName),
// 		UpdateExpression:          aws.String(updateExpression),
// 		ExpressionAttributeValues: expressionAttributeValues,
// 	}

// 	_, err := svc.UpdateItem(context.TODO(), input)
// 	return err
// }

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
		// objectId := c.Param("objectId")
		// if objectId == "" {
		// 	c.AbortWithError(http.StatusBadRequest, errors.New("ObjectId must be provided"))
		// 	return
		// }

		objectId := TOP_LEVEL_OBJECTID
		if objectId == "" {
			c.AbortWithError(http.StatusBadRequest, errors.New("ObjectId must be provided"))
			return
		}

		key := map[string]types.AttributeValue{
			"objectId": &types.AttributeValueMemberS{Value: objectId},
		}

		// Read the If-Match header to get the ETag sent by the client
		ifMatchETag := c.GetHeader("If-Match")
		// Fetch the current state of the item to compare ETags
		// currentItem, _, err := getItem(svc, db.TableName, map[string]types.AttributeValue{"objectId": &types.AttributeValueMemberS{Value: objectId}})
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
		currentETag := fmt.Sprintf(`"%x"`, sha256.Sum256(currentItemJSON))
		fmt.Println("Current ETag in Patch-computed", currentETag)
		// Compare the ETag from If-Match header with the current item's ETag
		if ifMatchETag != currentETag {
			c.AbortWithStatus(http.StatusPreconditionFailed)
			return
		}

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

		// Check if objectId is being updated
		if item.ObjectId != "" && item.ObjectId != objectId {
			// Create a new item with the new objectId
			// Note: You might need to adjust this to ensure all necessary data is included
			if err := createNewItemWithNewObjectId(svc, item); err != nil {
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error creating item with new objectId: %v", err))
				return
			}

			// Delete the old item
			if err := deleteItem(svc, db.TableName, map[string]types.AttributeValue{"objectId": &types.AttributeValueMemberS{Value: objectId}}); err != nil {
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error deleting old item: %v", err))
				return
			}

			c.Status(http.StatusOK)
			return
		}

		// Fetch the current plan item from the database
		currentPlan, err := fetchPlanItem(svc, db.TableName, TOP_LEVEL_OBJECTID)
		if err != nil {
			log.Fatalf("Error fetching current plan: %v", err)
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error fetching current plan: %v", err))
			return
		}

		// Check if the patch is for linkedPlanServices
		if len(item.LinkedPlanServices) > 0 {
			// Add the new linkedPlanServices to the current plan's array
			currentPlan.LinkedPlanServices = append(currentPlan.LinkedPlanServices, item.LinkedPlanServices...)
		} else {
			// Handle other updates normally
			// This part of the code will handle updates to planCostShares and plan
			// You can add logic here to replace the entire object with new data as per your requirement
			if item.PlanCostShares != (models.CostShares{}) {
				currentPlan.PlanCostShares = item.PlanCostShares
			}
			// Update top-level plan attributes if they are present in the request
			if item.Org != "" {
				currentPlan.Org = item.Org
			}
			if item.ObjectType != "" {
				currentPlan.ObjectType = item.ObjectType
			}
			if item.PlanType != "" {
				currentPlan.PlanType = item.PlanType
			}
			if item.CreationDate != "" {
				currentPlan.CreationDate = item.CreationDate
			}
			// if item.ObjectId != "" {
			// 	currentPlan.ObjectId = item.ObjectId
			// }
		}

		flatMap, err := FlattenPlan(item)
		if err != nil {
			log.Fatalf("Error flattening plan: %v", err)
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error flattening plan: %v", err))
			return
		}

		// This example will update the item with the new flattened data
		for _, jsonBlock := range flatMap {
			// Convert jsonBlock back to a map for attributevalue.MarshalMap
			var updateMap map[string]interface{}
			if err := json.Unmarshal(jsonBlock, &updateMap); err != nil {
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error preparing update data: %v", err))
				return
			}

			av, err := attributevalue.MarshalMap(updateMap)
			if err != nil {
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling update data: %v", err))
				return
			}

			key := map[string]types.AttributeValue{
				"objectId": &types.AttributeValueMemberS{Value: objectId},
			}

			updateExpression := "SET"
			expressionAttributeNames := make(map[string]string) // Correctly initialize the map
			expressionAttributeValues := map[string]types.AttributeValue{}
			i := 0
			// for k, v := range av {
			// 	updateExpression += fmt.Sprintf(" %s = :val%d,", k, i)
			// 	expressionAttributeValues[fmt.Sprintf(":val%d", i)] = v
			// 	i++
			// }

			// for k, v := range av {
			// 	if k == "objectId" {
			// 		continue // Skip attempting to update objectId
			// 	}
			// 	updateExpression += fmt.Sprintf(" %s = :val%d,", k, i)
			// 	expressionAttributeValues[fmt.Sprintf(":val%d", i)] = v
			// 	i++
			// }

			// updateExpression = updateExpression[:len(updateExpression)-1] // Remove the last comma

			// err = updateItem(svc, db.TableName, key, updateExpression, expressionAttributeValues)
			// if err != nil {
			// 	c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error updating item: %v", err))
			// 	return
			// }
			for k, v := range av {
				if k == "objectId" {
					continue // Skip attempting to update objectId
				}
				// Use expression attribute names for reserved keywords
				expressionAttributeName := fmt.Sprintf("#attr%d", i)
				expressionAttributeValue := fmt.Sprintf(":val%d", i)

				updateExpression += fmt.Sprintf(" %s = %s,", expressionAttributeName, expressionAttributeValue)
				expressionAttributeValues[expressionAttributeValue] = v
				// Map the expression attribute name to the actual attribute name
				if expressionAttributeNames == nil {
					expressionAttributeNames = make(map[string]string)
				}
				expressionAttributeNames[expressionAttributeName] = k
				i++
			}
			updateExpression = strings.TrimSuffix(updateExpression, ",") // A more idiomatic way to remove the last comma

			// Ensure your updateItem call includes expressionAttributeNames:
			err = updateItem(svc, db.TableName, key, updateExpression, expressionAttributeValues, expressionAttributeNames)
			if err != nil {
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error updating item: %v", err))
				return
			}

		}

		c.Status(http.StatusOK)
	}
}

func createNewItemWithNewObjectId(svc *dynamodb.Client, item models.Plan) error {
	// Convert the item to a map[string]types.AttributeValue
	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return err
	}

	// Create the new item in DynamoDB
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(db.TableName),
	}

	_, err = svc.PutItem(context.TODO(), input)
	return err
}
