package router

// import (
// 	"crud_with_dynamodb/api/crud"
// 	"crud_with_dynamodb/db"

// 	"github.com/gin-gonic/gin"
// )

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

	// // OAuth routes
	// r.GET("/auth/google/login", func(c *gin.Context) {
	// 	url := oauthConfig.AuthCodeURL("state", oauth2.AccessTypeOffline)
	// 	c.Redirect(http.StatusTemporaryRedirect, url)
	// })

	// r.GET("/auth/google/callback", func(c *gin.Context) {
	// 	// Handle the exchange code to initiate a transport.
	// 	code := c.Query("code")
	// 	token, err := oauthConfig.Exchange(c, code)
	// 	if err != nil {
	// 		c.AbortWithError(http.StatusInternalServerError, err)
	// 		return
	// 	}

	// 	// Use token to get user info and proceed with your application logic
	// 	c.JSON(http.StatusOK, gin.H{"access_token": token.AccessToken})
	// })

	// Protected routes
	api := r.Group("/v1", auth.AuthMiddleware(oauthConfig))
	{
		api.POST("/plan", crud.CreateItemHandler(svc))
		api.GET("/plan/:ObjectId", crud.GetItemHandler(svc))
		api.DELETE("/plan/del/:ObjectId", crud.DeleteItemHandler(svc))
		api.PATCH("/plan/patch/:ObjectId", crud.PatchItemHandler(svc))
		api.PUT("/items/:ObjectId", crud.UpdateItemHandler(svc))
		api.GET("/plan/id/:ObjectId", crud.GetItemByObjectIDHandler(svc))
	}

	// r.POST("/v1/plan", crud.CreateItemHandler(svc))
	// r.GET("/v1/plan/:ObjectId", crud.GetItemHandler(svc))
	// r.DELETE("/v1/plan/:ObjectId", crud.DeleteItemHandler(svc))

	return r
}

// func AuthMiddleware(oauthConfig *oauth2.Config) gin.HandlerFunc {
// 	return func(c *gin.Context) {
// 		// Extract the token from the Authorization header
// 		authHeader := c.GetHeader("Authorization")
// 		if authHeader == "" {
// 			c.JSON(http.StatusUnauthorized, gin.H{"error": "Authorization header is missing"})
// 			c.Abort()
// 			return
// 		}

// 		// Typically, the Authorization header is in the format `Bearer <token>`
// 		var token string
// 		_, err := fmt.Sscanf(authHeader, "Bearer %s", &token)
// 		if err != nil {
// 			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid Authorization header format"})
// 			c.Abort()
// 			return
// 		}

// 		isValid := validateIDToken(token, oauthConfig.ClientID)
// 		if !isValid {
// 			// Handle invalid token
// 			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired token"})
// 			c.Abort()
// 			return
// 		}
// 	}
// }

// func validateIDToken(idToken, clientID string) bool {
// 	ctx := context.Background()

// 	// Validate the ID token
// 	payload, err := idtoken.Validate(ctx, idToken, clientID)
// 	if err != nil {
// 		fmt.Printf("Error validating ID token: %v\n", err)
// 		return false
// 	}

// 	// Optionally, you can inspect the payload of the token
// 	fmt.Printf("Token validated for audience: %v\n", payload.Audience)

// 	return true
// }
