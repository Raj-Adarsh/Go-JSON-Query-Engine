package interfaces

import (
	"crud_with_dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DBService interface {
	CreateItem(tableName string, av map[string]types.AttributeValue, eTag string) error
	GetItem(tableName string, key map[string]types.AttributeValue) (*models.Plan, string, error)
	DeleteItem(tableName string, key map[string]types.AttributeValue) error
}
