package models

type Plan struct {
	PlanCostShares     CostShares    `json:"planCostShares"`
	LinkedPlanServices []PlanService `json:"linkedPlanServices"`
	Org                string        `json:"_org"`
	ObjectId           string        `json:"objectId"`
	ObjectType         string        `json:"objectType"`
	PlanType           string        `json:"planType"`
	CreationDate       string        `json:"creationDate"`
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
