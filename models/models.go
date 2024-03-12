package models

import "github.com/go-playground/validator"

var validate = validator.New()

type Plan struct {
	PlanCostShares     CostShares    `json:"planCostShares" validate:"required"`
	LinkedPlanServices []PlanService `json:"linkedPlanServices" validate:"required,dive"`
	Org                string        `json:"_org" validate:"required"`
	ObjectId           string        `json:"objectId" validate:"required"`
	ObjectType         string        `json:"objectType" validate:"required"`
	PlanType           string        `json:"planType" validate:"required"`
	CreationDate       string        `json:"creationDate" validate:"required"`
}

type CostShares struct {
	Deductible *int   `json:"deductible" validate:"required,gt=-1"`
	Org        string `json:"_org" validate:"required"`
	Copay      *int   `json:"copay" validate:"required"`
	ObjectId   string `json:"objectId" validate:"required,gt=-1"`
	ObjectType string `json:"objectType" validate:"required"`
}

type PlanService struct {
	LinkedService         Service    `json:"linkedService" validate:"required"`
	PlanServiceCostShares CostShares `json:"planserviceCostShares" validate:"required"`
	Org                   string     `json:"_org" validate:"required"`
	ObjectId              string     `json:"objectId" validate:"required"`
	ObjectType            string     `json:"objectType" validate:"required"`
}

type Service struct {
	Org        string `json:"_org" validate:"required"`
	ObjectId   string `json:"objectId" validate:"required"`
	ObjectType string `json:"objectType" validate:"required"`
	Name       string `json:"name" validate:"required"`
}

func ValidateStruct(s interface{}) error {
	return validate.Struct(s)
}
