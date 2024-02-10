package main

import "crud_with_dynamodb/router"

func main() {
	r := router.InitRouter()
	r.Run(":8081")
}
