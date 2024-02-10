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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gin-gonic/gin"

	"crud_with_dynamodb/db"
	"crud_with_dynamodb/models"
)

// func validatePlan(plan models.Plan) error {
// 	// Validate PlanCostShares
// 	if plan.PlanCostShares.Org == "" || plan.PlanCostShares.ObjectId == "" || plan.PlanCostShares.ObjectType == "" {
// 		return errors.New("missing required fields in PlanCostShares")
// 	}

// 	// Validate each LinkedPlanService
// 	if len(plan.LinkedPlanServices) == 0 {
// 		return errors.New("linkedPlanServices is required")
// 	}

// 	for _, service := range plan.LinkedPlanServices {
// 		if service.Org == "" || service.ObjectId == "" || service.ObjectType == "" ||
// 			service.LinkedService.Org == "" || service.LinkedService.ObjectId == "" ||
// 			service.LinkedService.ObjectType == "" || service.LinkedService.Name == "" ||
// 			service.PlanServiceCostShares.Org == "" || service.PlanServiceCostShares.ObjectId == "" ||
// 			service.PlanServiceCostShares.ObjectType == "" {
// 			return errors.New("missing required fields in LinkedPlanServices")
// 		}
// 	}

// 	// Validate top-level Plan fields
// 	if plan.Org == "" || plan.ObjectId == "" || plan.ObjectType == "" || plan.PlanType == "" {
// 		return errors.New("missing required top-level Plan fields")
// 	}

// 	return nil
// }

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

		eTag := fmt.Sprintf(`"%x"`, sha256.Sum256(bodyBytes))
		c.Header("ETag", eTag)

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

		// if err := validatePlan(item); err != nil {
		// 	c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bad request: %v", err))
		// 	return
		// }

		av, err := attributevalue.MarshalMap(item)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		if _, exists := av["ObjectId"]; !exists || av["ObjectId"].(*types.AttributeValueMemberS).Value == "" {
			c.AbortWithError(http.StatusInternalServerError, errors.New("createItem() - ObjectId (primary key) is missing or empty"))
			return
		}

		av["ETag"] = &types.AttributeValueMemberS{Value: eTag}
		if err := createItem(svc, db.TableName, av, eTag); err != nil {
			c.AbortWithError(http.StatusInternalServerError, err)
			return
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
		objectId := c.Param("ObjectId")
		key := map[string]types.AttributeValue{
			"ObjectId": &types.AttributeValueMemberS{Value: objectId},
		}

		item, eTag, err := getItem(svc, db.TableName, key)
		if err != nil {
			if err.Error() == "item not found" {
				c.AbortWithStatus(http.StatusNotFound)
				return
			}
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		c.Header("ETag", eTag)

		ifMatch := c.GetHeader("If-None-Match")
		if ifMatch == eTag {
			c.Status(http.StatusNotModified)
			return
		}

		c.JSON(http.StatusOK, item)
	}
}

func GetAllItemHandler(svc *dynamodb.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		if len(c.Request.URL.RawQuery) > 0 {
			c.Status(http.StatusBadRequest)
			return
		}
		scanInput := &dynamodb.ScanInput{
			TableName: aws.String(db.TableName),
		}

		scanResult, err := svc.Scan(context.TODO(), scanInput)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("failed to fetch items: %v", err))
			return
		}

		// Unmarshal the results into your model slice
		var items []models.Plan
		err = attributevalue.UnmarshalListOfMaps(scanResult.Items, &items)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("failed to unmarshal items: %v", err))
			return
		}
		c.JSON(http.StatusOK, items)
	}
}

func UpdateItemHandler(svc *dynamodb.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		objectId := c.Param("objectId")
		updateExpression := "SET #attrName = :attrValue"
		expressionAttributeValues := map[string]types.AttributeValue{
			":attrValue": &types.AttributeValueMemberS{Value: "newValue"},
		}
		key := map[string]types.AttributeValue{
			"objectId": &types.AttributeValueMemberS{Value: objectId},
		}

		if err := updateItem(svc, db.TableName, key, updateExpression, expressionAttributeValues); err != nil {
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		c.Status(http.StatusOK)
	}
}

func DeleteItemHandler(svc *dynamodb.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		if len(c.Request.URL.RawQuery) > 0 {
			c.Status(http.StatusBadRequest)
			return
		}
		objectId := c.Param("ObjectId")
		key := map[string]types.AttributeValue{
			"ObjectId": &types.AttributeValueMemberS{Value: objectId},
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
				AttributeName: aws.String("ObjectId"),
				KeyType:       types.KeyTypeHash,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("ObjectId"),
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

func createItem(svc *dynamodb.Client, tableName string, av map[string]types.AttributeValue, eTag string) error {
	av["ETag"] = &types.AttributeValueMemberS{Value: eTag}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}

	formatted, err := json.MarshalIndent(input, "", "    ")
	if err != nil {
		log.Fatalf("Error formatting input: %v", err)
	}

	fmt.Printf("INPUT: %s\n", string(formatted))

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

	plan := models.Plan{}
	err = attributevalue.UnmarshalMap(result.Item, &plan)
	if err != nil {
		return nil, "", err
	}

	// Extract the ETag from the result item
	eTagValue, exists := result.Item["ETag"]
	if !exists {
		return &plan, "", nil
	}
	eTag, ok := eTagValue.(*types.AttributeValueMemberS)
	if !ok {
		return &plan, "", errors.New("ETag format is invalid")
	}

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

}

func deleteItem(svc *dynamodb.Client, tableName string, key map[string]types.AttributeValue) error {
	input := &dynamodb.DeleteItemInput{
		Key:       key,
		TableName: aws.String(tableName),
	}

	_, err := svc.DeleteItem(context.TODO(), input)
	return err
}
