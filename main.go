package main

import (
	"crud_with_dynamodb/queue"
	"crud_with_dynamodb/router"
	"os"

	"golang.org/x/oauth2"
)

var googleOauthConfig = &oauth2.Config{
	ClientID: os.Getenv("GOOGLE_CLIENT_ID"),
}

func main() {
	go queue.ConnectSQS()
	go queue.PollMessagesAndInsertToElasticsearch()
	// go queue.InsertSimpleStringToElasticsearch()
	r := router.InitRouter(googleOauthConfig)
	r.Run(":8081")
}
