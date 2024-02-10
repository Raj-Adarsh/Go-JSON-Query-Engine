package router

import (
	"crud_with_dynamodb/api/crud"
	"crud_with_dynamodb/db"

	"github.com/gin-gonic/gin"
)

func InitRouter() *gin.Engine {
	r := gin.Default()

	svc := db.ConnectToDB()

	r.POST("/v1/plan", crud.CreateItemHandler(svc))
	r.GET("/v1/plan/:ObjectId", crud.GetItemHandler(svc))
	r.GET("/v1/plan", crud.GetAllItemHandler(svc))
	r.DELETE("/v1/plan/:ObjectId", crud.DeleteItemHandler(svc))

	return r
}
