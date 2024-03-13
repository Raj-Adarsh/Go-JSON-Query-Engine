package main

import (
	"crud_with_dynamodb/router"
	"os"

	"golang.org/x/oauth2"
)

var googleOauthConfig = &oauth2.Config{
	// RedirectURL:  "http://localhost:8081/auth/google/callback",
	ClientID: os.Getenv("GOOGLE_CLIENT_ID"), // Set these in your environment or replace directly
	// ClientSecret: os.Getenv("GOOGLE_CLIENT_SECRET"), // Set these in your environment or replace directly
	// Scopes:       []string{"https://www.googleapis.com/auth/userinfo.email"},
	// Endpoint:     google.Endpoint,
}

func main() {
	r := router.InitRouter(googleOauthConfig)
	r.Run(":8081")
}
