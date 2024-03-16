package auth

import (
	"fmt"
	"net/http"

	"context"

	"github.com/gin-gonic/gin"
	"golang.org/x/oauth2"
	"google.golang.org/api/idtoken"
)

func AuthMiddleware(oauthConfig *oauth2.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Extract the token from the Authorization header
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Authorization header is missing"})
			c.Abort()
			return
		}

		var token string
		_, err := fmt.Sscanf(authHeader, "Bearer %s", &token)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid Authorization header format"})
			c.Abort()
			return
		}

		isValid := validateIDToken(token, oauthConfig.ClientID)
		if !isValid {
			// Handle invalid token
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired token"})
			c.Abort()
			return
		}
	}
}

func validateIDToken(idToken, clientID string) bool {
	ctx := context.Background()

	// Validate the ID token
	payload, err := idtoken.Validate(ctx, idToken, clientID)
	if err != nil {
		fmt.Printf("Error validating ID token: %v\n", err)
		return false
	}

	fmt.Printf("Token validated for audience: %v\n", payload.Audience)

	return true
}
