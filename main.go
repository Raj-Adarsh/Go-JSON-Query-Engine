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
	//create the elastic queue
	go queue.CreateElasticQueue()
	//poll the queue
	go queue.PollMessagesAndInsertToElasticsearch()

	//start the webapp routees
	r := router.InitRouter(googleOauthConfig)
	r.Run(":8081")
}
