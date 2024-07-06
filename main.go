package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"log"
	"os"

	"github.com/elastic/go-elasticsearch/v8"
)

func main() {
	// Set the environment variable programmatically
	err := os.Setenv("ELASTIC_PASSWORD", "ELASTIC_PASSWORD")
	if err != nil {
		log.Fatalf("Error setting environment variable: %s", err)
	}

	// Get the Elasticsearch password from the environment variable
	password := os.Getenv("ELASTIC_PASSWORD")
	if password == "" {
		log.Fatal("ELASTIC_PASSWORD environment variable is not set")
	}

	// Configure the Elasticsearch client
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
		Username: "elastic",
		Password: password,
	}

	// Create a new Elasticsearch client
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	// Get cluster information
	getClusterInfo(es)

	// Create an index
	indexName := "my_index"
	//	createIndex(es, indexName)

	// Post data to the index
	data := []map[string]interface{}{
		{
			"id": "38118545",
			"categories": map[string]interface{}{
				"subcategory": "1407",
			},
			"title": map[string]interface{}{
				"ro": "Teren sub constructie in apropiere de Vadul lui Voda",
				"ru": "Teren sub constructie in apropiere de Vadul lui Voda",
			},
			"type":   "standard",
			"posted": 1486556302.101039,
		},
		{
			"id": "38784049",
			"categories": map[string]interface{}{
				"subcategory": "1404",
			},
			"title": map[string]interface{}{
				"ro": "Центр рышкановки. 3х комнатная",
				"ru": "Центр рышкановки. 3х комнатная",
			},
			"type":   "standard",
			"posted": 1488274575.697526,
		},
	}

	for _, d := range data {
		postData(es, indexName, d)
	}

	// Retrieve data from the index
	getData(es, indexName, "38118545")
	getData(es, indexName, "38784049")

}
func getClusterInfo(es *elasticsearch.Client) {
	res, err := es.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	// Print cluster information
	if res.IsError() {
		log.Fatalf("Error: %s", res.String())
	} else {
		fmt.Println(res)
	}
}

func createIndex(es *elasticsearch.Client, indexName string) {
	req := esapi.IndicesCreateRequest{
		Index: indexName,
	}
	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error creating the index: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("Error: %s", res.String())
	} else {
		log.Printf("Index %s created successfully", indexName)
	}
}

func postData(es *elasticsearch.Client, indexName string, data map[string]interface{}) {
	// Marshal the data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Fatalf("Error marshaling the data: %s", err)
	}

	// Create the request
	req := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: fmt.Sprintf("%v", data["id"]),
		Body:       bytes.NewReader(jsonData),
		Refresh:    "true",
	}

	// Execute the request
	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error indexing the data: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("Error $: %s", res.String())
	} else {
		log.Printf("Document %v indexed successfully", data["_id"])
	}
}

func getData(es *elasticsearch.Client, indexName, docID string) {
	// Create the request
	req := esapi.GetRequest{
		Index:      indexName,
		DocumentID: docID,
	}

	// Execute the request
	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error getting the data: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("Error: %s", res.String())
	} else {
		var result map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
		}
		fmt.Printf("Document %s: %s\n", docID, result["_source"])
	}
}
