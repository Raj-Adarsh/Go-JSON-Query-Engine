package router

import (
	"crud_with_dynamodb/api/crud"
	"crud_with_dynamodb/auth"
	"crud_with_dynamodb/db"

	"github.com/gin-gonic/gin"
	"golang.org/x/oauth2"
)

func InitRouter(oauthConfig *oauth2.Config) *gin.Engine {
	r := gin.Default()

	svc := db.ConnectToDB()

	// Protected routes
	api := r.Group("/v1", auth.AuthMiddleware(oauthConfig))
	{
		api.POST("/plan", crud.CreateItemHandler(svc))
		api.GET("/plan/all", crud.GetItemHandler(svc))
		api.DELETE("/plan/del/:objectId", crud.DeleteItemHandler(svc))
		api.PATCH("/plan/patch/:objectId", crud.PatchItemHandler(svc))
		// api.PUT("/items/:objectId", crud.UpdateItemHandler(svc))
		api.GET("/plan/:objectId", crud.GetItemByObjectIDHandler(svc))
	}
	return r
}
