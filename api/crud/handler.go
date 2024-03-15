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

// func GetItemHandler(svc *dynamodb.Client) gin.HandlerFunc {
// 	return func(c *gin.Context) {
// 		// Perform the scan operation without a filter expression to get all items
// 		scanInput := &dynamodb.ScanInput{
// 			TableName: aws.String(db.TableName),
// 		}

// 		result, err := svc.Scan(context.TODO(), scanInput)
// 		if err != nil {
// 			log.Println("Error scanning table:", err)
// 			c.AbortWithError(http.StatusInternalServerError, err)
// 			return
// 		}

// 		// Check if any items were found
// 		if len(result.Items) == 0 {
// 			c.AbortWithStatus(http.StatusNotFound)
// 			return
// 		}

// 		// Convert the items to a slice of map[string]interface{} for flexible handling
// 		items := make([]map[string]interface{}, len(result.Items))
// 		for i, item := range result.Items {
// 			var itemMap map[string]interface{}
// 			err = attributevalue.UnmarshalMap(item, &itemMap)
// 			if err != nil {
// 				log.Println("Error unmarshalling item:", err)
// 				c.AbortWithError(http.StatusInternalServerError, err)
// 				return
// 			}

// 			// Filter out null or empty fields
// 			filteredItem := make(map[string]interface{})
// 			for key, value := range itemMap {
// 				if value != nil && value != "" {
// 					filteredItem[key] = value
// 				}
// 			}

// 			items[i] = filteredItem
// 		}

// 		// Marshal the items into standard JSON
// 		itemsJSON, err := json.Marshal(items)
// 		if err != nil {
// 			log.Println("Error marshalling items to JSON:", err)
// 			c.AbortWithError(http.StatusInternalServerError, err)
// 			return
// 		}

// 		c.Data(http.StatusOK, "application/json", itemsJSON)
// 	}
// }

// func GetItemHandler(svc *dynamodb.Client) gin.HandlerFunc {
// 	return func(c *gin.Context) {
// 		// Scan operation (note: consider using Query if you have a large dataset)
// 		scanOutput, err := svc.Scan(context.TODO(), &dynamodb.ScanInput{
// 			TableName: aws.String(db.TableName),
// 		})
// 		if err != nil {
// 			log.Println("Error scanning table:", err)
// 			c.AbortWithError(http.StatusInternalServerError, err)
// 			return
// 		}

// 		// Convert the scan result to your model
// 		var items []models.Plan
// 		err = attributevalue.UnmarshalListOfMaps(scanOutput.Items, &items)
// 		if err != nil {
// 			log.Println("Error unmarshalling scan result:", err)
// 			c.AbortWithError(http.StatusInternalServerError, err)
// 			return
// 		}

// 		// Marshal and return the items as JSON
// 		itemsJSON, err := json.Marshal(items)
// 		if err != nil {
// 			log.Println("Error marshalling items to JSON:", err)
// 			c.AbortWithError(http.StatusInternalServerError, err)
// 			return
// 		}

// 		c.Data(http.StatusOK, "application/json", itemsJSON)
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

// func GetItemByObjectIDHandler(svc *dynamodb.Client) gin.HandlerFunc {
// 	return func(c *gin.Context) {
// 		objectId := c.Param("objectId")
// 		if objectId == "" {
// 			c.AbortWithError(http.StatusBadRequest, errors.New("ObjectId must be provided"))
// 			return
// 		}

// 		// object_id := TOP_LEVEL_OBJECTID
// 		// key := map[string]types.AttributeValue{
// 		// 	"objectId": &types.AttributeValueMemberS{Value: object_id},
// 		// }

// 		// plan, _, err := getItem(svc, db.TableName, key)
// 		// if err != nil {
// 		// 	if err.Error() == "item not found" {
// 		// 		c.AbortWithStatus(http.StatusNotFound)
// 		// 		return
// 		// 	}
// 		// 	c.AbortWithError(http.StatusInternalServerError, err)
// 		// 	return
// 		// }

// 		plan, err := fetchPlanItem(svc, db.TableName, TOP_LEVEL_OBJECTID) // Use the top-level objectId for fetching
// 		if err != nil {
// 			log.Println("Error fetching json data for TOP_LEVEL_OBJECTID")
// 			c.AbortWithError(http.StatusInternalServerError, err)
// 			return
// 		}

// 		// Marshal the plan into JSON for logging
// 		planJSON, err := json.MarshalIndent(plan, "", "    ")
// 		if err != nil {
// 			log.Println("Error marshalling plan to JSON:", err)
// 			return
// 		}

// 		log.Printf("DynamoDB attributes after unmarshal in object by ID: %s\n", string(planJSON))

// 		log.Println("Plan is:", plan)

// 		// Fetch the item directly using the provided objectId
// 		// plan, err := fetchPlanItem(svc, db.TableName, objectId)
// 		// if err != nil {
// 		// 	var notFoundException *types.ResourceNotFoundException
// 		// 	if errors.As(err, &notFoundException) {
// 		// 		c.AbortWithStatus(http.StatusNotFound)
// 		// 	} else {
// 		// 		log.Println("Error fetching item:", err)
// 		// 		c.AbortWithError(http.StatusInternalServerError, err)
// 		// 	}
// 		// 	return
// 		// }

// 		// // Marshal the item into JSON for the response
// 		// itemJSON, err := json.Marshal(plan)
// 		// if err != nil {
// 		// 	log.Println("Error marshalling item to JSON:", err)
// 		// 	c.AbortWithError(http.StatusInternalServerError, err)
// 		// 	return
// 		// }

// 		// log.Printf("DynamoDB attributes after unmarshal in object by ID: %s\n", itemJSON)

// 		// c.Data(http.StatusOK, "application/json", itemJSON)

// 		if response, found := findObjectByID(plan, objectId); found {
// 			log.Println("Found ObjectID Match")

// 			// Assuming you want to inspect or modify the response before sending it back
// 			responseJSON, err := json.Marshal(response)
// 			if err != nil {
// 				log.Println("Error marshalling found object to JSON:", err)
// 				c.AbortWithError(http.StatusInternalServerError, err)
// 				return
// 			}

// 			// Optionally, log the JSON or inspect it
// 			log.Printf("Response JSON: %s\n", string(responseJSON))

// 			// If you need to modify the response, unmarshal it into a map or struct here
// 			// For example, unmarshal into a map to inspect or modify
// 			var responseMap map[string]interface{}
// 			err = json.Unmarshal(responseJSON, &responseMap)
// 			if err != nil {
// 				log.Println("Error unmarshalling response JSON:", err)
// 				c.AbortWithError(http.StatusInternalServerError, err)
// 				return
// 			}

// 			// Perform any necessary checks or modifications
// 			// For example, check if 'org' field is present and not empty
// 			if org, ok := responseMap["_org"].(string); !ok || org == "" {
// 				log.Println("Org field is missing or empty")
// 				// Handle the missing or empty 'org' field as needed
// 				// responseMap["_org"] = "example.com"
// 			}

// 			// Finally, send the response
// 			// If modifications were made, marshal the map back into JSON
// 			modifiedResponseJSON, err := json.Marshal(responseMap)
// 			if err != nil {
// 				log.Println("Error marshalling modified response:", err)
// 				c.AbortWithError(http.StatusInternalServerError, err)
// 				return
// 			}

// 			c.Data(http.StatusOK, "application/json", modifiedResponseJSON)
// 			return
// 		}

// 		c.AbortWithStatus(http.StatusNotFound)
// 	}
// }

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
			c.AbortWithStatus(http.StatusNotFound)
			return
		}

		// Assuming you only expect one item to match, otherwise you'll need to handle multiple items
		matchedItem := result.Items[0]

		// // Convert the matched item to JSON
		// matchedItemJSON, err := json.Marshal(matchedItem)
		// if err != nil {
		// 	log.Println("Error marshalling matched item to JSON:", err)
		// 	c.AbortWithError(http.StatusInternalServerError, err)
		// 	return
		// }

		// // Send the matched item as the response
		// c.Data(http.StatusOK, "application/json", matchedItemJSON)

		// Convert the matched item to a map[string]interface{}
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
			// Check if the value is not nil and not an empty string
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

		////////////////
		// Marshal the plan into JSON
		// After potentially modifying the item, marshal it back to JSON
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
		// No If-Match header present means we can't perform the conditional check
		// if ifMatch == "" {
		// 	c.AbortWithError(http.StatusBadRequest, errors.New("If-Match header must be provided"))
		// 	return
		// }
		fmt.Println("ETag for if-match", ifMatch)

		// Compare If-Match ETag with current resource's ETag
		if ifMatch != currentETag {
			// ETags do not match, the resource has changed since the ETag was generated
			c.AbortWithStatus(http.StatusPreconditionFailed)
			return
		}

		////////////////

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

		c.Header("ETag", requestETag)

		c.Status(http.StatusOK)
		////////////////////////////////////

		// var item models.Plan
		// if err := json.Unmarshal(bodyBytes, &item); err != nil {
		// 	c.AbortWithError(http.StatusBadRequest, errors.New("error unmarshalling the json request"))
		// 	return
		// }

		// flatMap, err := FlattenPlan(item)
		// if err != nil {
		// 	log.Fatalf("Error flattening plan: %v", err)
		// 	c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error flattening plan: %v", err))
		// 	return
		// }

		// // Iterate over flatMap and update each block in the database
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

		// 	// Construct the update expression and attribute values
		// 	updateExpression := "SET"
		// 	expressionAttributeValues := map[string]types.AttributeValue{}
		// 	i := 0
		// 	for k, v := range av {
		// 		updateExpression += fmt.Sprintf(" %s = :val%d,", k, i)
		// 		expressionAttributeValues[fmt.Sprintf(":val%d", i)] = v
		// 		i++
		// 	}
		// 	updateExpression = updateExpression[:len(updateExpression)-1] // Remove the last comma

		// 	// Perform the update operation
		// 	key := map[string]types.AttributeValue{
		// 		"objectId": &types.AttributeValueMemberS{Value: objectId},
		// 	}
		// 	err = updateItem(svc, db.TableName, key, updateExpression, expressionAttributeValues)
		// 	if err != nil {
		// 		c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error updating item: %v", err))
		// 		return
		// 	}
		// }

		// c.Status(http.StatusOK)
	}
}

func getCurrentResourceETag(svc *dynamodb.Client, tableName, objectId string) (string, error) {
	// Construct the key to fetch the item from DynamoDB
	// key := map[string]types.AttributeValue{
	// 	"objectId": &types.AttributeValueMemberS{Value: objectId},
	// }

	// // Fetch the item from DynamoDB
	// output, err := svc.GetItem(context.TODO(), &dynamodb.GetItemInput{
	// 	TableName: aws.String(tableName),
	// 	Key:       key,
	// })
	// if err != nil {
	// 	return "", fmt.Errorf("failed to get item from DynamoDB: %w", err)
	// }

	// if output.Item == nil {
	// 	return "", fmt.Errorf("no item found with objectId: %s", objectId)
	// }

	// // Marshal the item to JSON
	// itemMap := make(map[string]interface{})
	// if err := attributevalue.UnmarshalMap(output.Item, &itemMap); err != nil {
	// 	return "", fmt.Errorf("failed to unmarshal DynamoDB item to map: %w", err)
	// }

	// itemJSON, err := json.Marshal(itemMap)
	// if err != nil {
	// 	return "", fmt.Errorf("failed to marshal item to JSON: %w", err)
	// }

	// // Compute the ETag for the marshaled JSON
	// eTag := fmt.Sprintf(`"%x"`, sha256.Sum256(itemJSON))

	// return eTag, nil
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

// 		// Assuming the incoming JSON represents the entire structure to be stored as flat JSON.
// 		var updates map[string]interface{}
// 		if err := json.Unmarshal(bodyBytes, &updates); err != nil {
// 			c.AbortWithError(http.StatusBadRequest, errors.New("error unmarshalling the json request"))
// 			return
// 		}

// 		// Fetch the current state of the item to compare with the incoming updates.
// 		currentItem, err := fetchPlanItem(svc, db.TableName, objectId)
// 		if err != nil {
// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error fetching current item: %v", err))
// 			return
// 		}

// 		// Assuming `fetchPlanItem` returns the item as a map[string]interface{} for easy comparison.
// 		// You would then compare `currentItem` with `updates` to determine what needs to be changed.
// 		// This step is simplified here; actual implementation would depend on your specific requirements.

// 		// Prepare the update payload. This might involve merging `currentItem` and `updates`,
// 		// handling deletions, additions, and modifications as needed.
// 		updatedItem, err := prepareUpdatePayload(currentItem, updates)
// 		if err != nil {
// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error preparing update payload: %v", err))
// 			return
// 		}

// 		// Convert the updated structure back to a flat JSON for storage.
// 		av, err := attributevalue.MarshalMap(updatedItem)
// 		if err != nil {
// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling updates: %v", err))
// 			return
// 		}

// 		av["objectId"] = &types.AttributeValueMemberS{Value: objectId}

// 		// // Update the item in DynamoDB.
// 		// key := map[string]types.AttributeValue{
// 		//     "objectId": &types.AttributeValueMemberS{Value: objectId},
// 		// }

// 		input := &dynamodb.PutItemInput{
// 			Item:      av,
// 			TableName: aws.String(db.TableName),
// 		}

// 		_, err = svc.PutItem(context.TODO(), input)
// 		if err != nil {
// 			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error updating item: %v", err))
// 			return
// 		}

// 		c.Status(http.StatusOK)
// 	}
// }

// // prepareUpdatePayload merges updates into the currentItem and returns the merged item.
// // It handles any special cases as needed and applies updates to currentItem.
// func prepareUpdatePayload(currentItem *models.Plan, updates map[string]interface{}) (*models.Plan, error) {
// 	// Convert currentItem to a map for easier manipulation
// 	currentItemMap := make(map[string]interface{})
// 	currentItemBytes, err := json.Marshal(currentItem)
// 	if err != nil {
// 		return nil, err
// 	}
// 	err = json.Unmarshal(currentItemBytes, &currentItemMap)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Iterate over the updates map and apply changes to currentItemMap
// 	for key, updateValue := range updates {
// 		currentItemMap[key] = updateValue
// 	}

// 	// Convert the updated map back to a models.Plan
// 	updatedItemBytes, err := json.Marshal(currentItemMap)
// 	if err != nil {
// 		return nil, err
// 	}
// 	err = json.Unmarshal(updatedItemBytes, currentItem)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return currentItem, nil
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

		// _, _, err := getItem(svc, db.TableName, key)
		// if err != nil {
		// 	if err.Error() == "item not found" {
		// 		c.AbortWithStatus(http.StatusNotFound)
		// 		return
		// 	}
		// 	c.AbortWithError(http.StatusInternalServerError, err)
		// 	return
		// }

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

	// var ErrItemNotFound = errors.New("item not found")

	// if err != nil {
	// 	// Assuming getItem returns a specific error for item not found
	// 	if !errors.Is(err, ErrItemNotFound) {
	// 		return fmt.Errorf("error checking for existing item: %v", err)
	// 	}
	// 	// Item not found, proceed with creation
	// } else {
	// 	// Item exists, handle accordingly
	// 	return fmt.Errorf("item with objectId %s already exists", av["objectId"].(*types.AttributeValueMemberS).Value)
	// }

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
		objectId := c.Param("objectId")
		if objectId == "" {
			c.AbortWithError(http.StatusBadRequest, errors.New("ObjectId must be provided"))
			return
		}

		// objectId := TOP_LEVEL_OBJECTID
		// if objectId == "" {
		// 	c.AbortWithError(http.StatusBadRequest, errors.New("ObjectId must be provided"))
		// 	return
		// }

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

		fmt.Println("object id", item.ObjectId)
		fmt.Println("passed object id", objectId)

		canonicalBytes, err := json.Marshal(item)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling item to JSON: %v", err))
			return
		}

		requestETag := fmt.Sprintf(`"%x"`, sha256.Sum256(canonicalBytes))

		// // If the item has a new objectId, create it as a new independent entry
		// if item.ObjectId != "" && item.ObjectId != objectId {
		// 	newItem := item                  // Assuming you want to create a new item based on the modified item
		// 	newItem.ObjectId = item.ObjectId // Explicitly set the new object ID, if not already set
		// 	fmt.Println("new object id", newItem.ObjectId)

		// 	err := createNewItemWithNewObjectId(svc, item)
		// 	if err != nil {
		// 		c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error creating item with new objectId: %v", err))
		// 		return
		// 	}
		// 	// c.Status(http.StatusOK) // Consider returning the newly created item or its ID
		// 	// return
		// }

		// // Check if objectId is being updated
		// if item.ObjectId != "" {
		// 	newItem := item                  // Assuming you want to create a new item based on the modified item
		// 	newItem.ObjectId = item.ObjectId // Explicitly set the new object ID, if not already set
		// 	fmt.Println("new object id", newItem.ObjectId)
		// 	// Create a new item with the new objectId
		// 	// Note: You might need to adjust this to ensure all necessary data is included
		// 	if err := createNewItemWithNewObjectId(svc, newItem); err != nil {
		// 		c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error creating item with new objectId: %v", err))
		// 		return
		// 	}

		// 	// Delete the old item
		// 	if err := deleteItem(svc, db.TableName, map[string]types.AttributeValue{"objectId": &types.AttributeValueMemberS{Value: objectId}}); err != nil {
		// 		c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error deleting old item: %v", err))
		// 		return
		// 	}

		// 	c.Status(http.StatusOK)
		// 	return
		// }

		// Fetch the current plan item from the database
		currentPlan, err := fetchPlanItem(svc, db.TableName, TOP_LEVEL_OBJECTID)
		if err != nil {
			log.Fatalf("Error fetching current plan: %v", err)
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error fetching current plan: %v", err))
			return
		}

		// Assuming fetchPlanItem returns a pointer to a Plan and an error
		// Compare the current item with the PATCH data
		// This example focuses on the LinkedPlanServices array
		for _, newService := range item.LinkedPlanServices {
			isNew := true
			for _, currentService := range currentItem.LinkedPlanServices {
				if newService.ObjectId == currentService.ObjectId {
					// This is an existing service, not a new one
					isNew = false
					break
				}
			}
			if isNew {
				// This is a new service, so do something with its ID
				fmt.Println("New service ID:", newService.ObjectId)
				// For example, add the new service to the database
				// err := createLinkedPlanService(svc, item.ObjectId, newService)
				// if err != nil {
				// 	c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error creating new linked plan service: %v", err))
				// 	return
				// }
				////////////////////////////////
				// Use the custom marshaling function instead of attributevalue.MarshalMap
				av, err := marshalMapUsingJSONTags(newService.LinkedService)
				if err != nil {
					c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling linkedService: %v", err))
					return
				}
				// Since marshalMapUsingJSONTags returns a map[string]interface{}, you need to convert it to map[string]types.AttributeValue
				avAttributeValue, err := attributevalue.MarshalMap(av)
				if err != nil {
					c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error converting to AttributeValue map: %v", err))
					return
				}
				avAttributeValue["objectId"] = &types.AttributeValueMemberS{Value: newService.LinkedService.ObjectId}

				if err := createItem(svc, db.TableName, avAttributeValue); err != nil {
					c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error creating linkedService: %v", err))
					return
				}

				////
				av, err = marshalMapUsingJSONTags(newService.PlanServiceCostShares)
				if err != nil {
					c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling PlanServiceCostShares: %v", err))
					return
				}
				// Since marshalMapUsingJSONTags returns a map[string]interface{}, you need to convert it to map[string]types.AttributeValue
				avAttributeValue, err = attributevalue.MarshalMap(av)
				if err != nil {
					c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error converting to AttributeValue map: %v", err))
					return
				}
				avAttributeValue["objectId"] = &types.AttributeValueMemberS{Value: newService.PlanServiceCostShares.ObjectId}

				if err := createItem(svc, db.TableName, avAttributeValue); err != nil {
					c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error creating PlanServiceCostShares: %v", err))
					return
				}

				//////
				av, err = marshalMapUsingJSONTags(newService)
				if err != nil {
					c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling newService: %v", err))
					return
				}
				// Since marshalMapUsingJSONTags returns a map[string]interface{}, you need to convert it to map[string]types.AttributeValue
				avAttributeValue, err = attributevalue.MarshalMap(av)
				if err != nil {
					c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error converting to AttributeValue map: %v", err))
					return
				}
				avAttributeValue["objectId"] = &types.AttributeValueMemberS{Value: newService.ObjectId}

				if err := createItem(svc, db.TableName, avAttributeValue); err != nil {
					c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error creating newService: %v", err))
					return
				}
				/////////////////////////////////

				// av, err := attributevalue.MarshalMap(newService.LinkedService)
				// av["objectId"] = &types.AttributeValueMemberS{Value: newService.LinkedService.ObjectId}

				// if err != nil {
				// 	c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling linkedService: %v", err))
				// 	return
				// }
				// if err := createItem(svc, db.TableName, av); err != nil {
				// 	c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error creating linkedService: %v", err))
				// 	return
				// }

				// av, err = attributevalue.MarshalMap(newService.PlanServiceCostShares)
				// av["objectId"] = &types.AttributeValueMemberS{Value: newService.PlanServiceCostShares.ObjectId}

				// if err != nil {
				// 	c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling planserviceCostShares: %v", err))
				// 	return
				// }
				// if err := createItem(svc, db.TableName, av); err != nil {
				// 	c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error creating PlanServiceCostShares: %v", err))
				// 	return
				// }

				// av, err = attributevalue.MarshalMap(newService)
				// av["objectId"] = &types.AttributeValueMemberS{Value: newService.ObjectId}

				// if err != nil {
				// 	c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error marshalling planservice: %v", err))
				// 	return
				// }
				// if err := createItem(svc, db.TableName, av); err != nil {
				// 	c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("error creating planservice: %v", err))
				// 	return
				// }
			}
		}

		// Check if the patch is for linkedPlanServices
		if len(item.LinkedPlanServices) > 0 {
			// Add the new linkedPlanServices to the current plan's array
			currentPlan.LinkedPlanServices = append(currentPlan.LinkedPlanServices, item.LinkedPlanServices...)
		}
		// else {
		// 	// Handle other updates normally
		// 	// This part of the code will handle updates to planCostShares and plan
		// 	// You can add logic here to replace the entire object with new data as per your requirement
		// 	if item.PlanCostShares != (models.CostShares{}) {
		// 		currentPlan.PlanCostShares = item.PlanCostShares
		// 	}
		// 	// Update top-level plan attributes if they are present in the request
		// 	if item.Org != "" {
		// 		currentPlan.Org = item.Org
		// 	}
		// 	if item.ObjectType != "" {
		// 		currentPlan.ObjectType = item.ObjectType
		// 	}
		// 	if item.PlanType != "" {
		// 		currentPlan.PlanType = item.PlanType
		// 	}
		// 	if item.CreationDate != "" {
		// 		currentPlan.CreationDate = item.CreationDate
		// 	}
		// 	// if item.ObjectId != "" {
		// 	// 	currentPlan.ObjectId = item.ObjectId
		// 	// }
		// }

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
				// if expressionAttributeNames == nil {
				// 	expressionAttributeNames = make(map[string]string)
				// }
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

		c.Header("ETag", requestETag)
		c.Status(http.StatusOK)
	}
}

func createNewItemWithNewObjectId(svc *dynamodb.Client, item models.Plan) error {
	// Convert the item to a map[string]types.AttributeValue
	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return err
	}

	// Ensure the objectId is correctly set in the attribute values map
	// This step might be redundant if MarshalMap already includes the objectId, but it's a good check
	av["objectId"] = &types.AttributeValueMemberS{Value: item.ObjectId}

	// Create the new item in DynamoDB
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(db.TableName),
	}

	_, err = svc.PutItem(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to create new item with objectId %s: %v", item.ObjectId, err)
	}

	return nil
}

func createLinkedPlanService(svc *dynamodb.Client, planObjectId string, service models.PlanService) error {
	// Directly set the ObjectId from the service if it's not already a string
	objectId := service.ObjectId // Assuming ObjectId is a *string, dereference it

	// Manually construct the attribute value map for complex types
	av := map[string]types.AttributeValue{
		"objectId": &types.AttributeValueMemberS{Value: objectId},
		// For other fields, ensure they are correctly set.
		// This is a simplified example; you'll need to adjust it based on your actual struct fields
	}

	// Debugging: Log the attribute values map to verify all required keys and values are present
	log.Printf("Attempting to insert item with attributes: %+v\n", av)

	// Create the new item in DynamoDB
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(db.TableName),
	}

	_, err := svc.PutItem(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to create new linkedPlanService with objectId %s: %v", objectId, err)
	}

	return nil
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
		key := strings.Split(jsonTag, ",")[0] // Ignore omitempty and other options
		value := val.Field(i).Interface()
		result[key] = value
	}
	return result, nil
}
