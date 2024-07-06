package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type Client struct {
	cfg elasticsearch.Config
}
type Server struct {
	client *Client
}

func NewClient(host string) *Client {
	err := os.Setenv("ELASTIC_PASSWORD", "ELASTIC_PASSWORD")
	if err != nil {
		log.Fatalf("Error setting environment variable: %s", err)
	}

	// Get the Elasticsearch password from the environment variable
	password := os.Getenv("ELASTIC_PASSWORD")
	if password == "" {
		log.Fatal("ELASTIC_PASSWORD environment variable is not set")
	}
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
		Username: "elastic",
		Password: password,
	}
	return &Client{cfg}
}

func (c *Client) CheckHealth() error {

	req, err := http.NewRequest("GET", c.cfg.Addresses[0], nil)
	if err != nil {
		return err
	}
	// Add basic authentication header
	req.SetBasicAuth(c.cfg.Username, c.cfg.Password)

	// Perform the request
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed check Elasticsearch health: %v", err)
	}

	log.Println("debug health check response: ", string(responseBody))

	return nil
}

type Employee struct {
	Id      int     `json:"id"`
	Name    string  `json:"name"`
	Address string  `json:"address"`
	Salary  float64 `json:"salary"`
}

func (c *Client) CreateIndex() error {
	body := `
	{
		"mappings": {
			"properties": {
				"id": {
					"type": "integer"
				},
				"name": {
					"type": "text"
				},
				"address": {
					"type": "text"
				},
				"salary": {
					"type": "float"
				}
			}
		}
	}
	`

	req, err := http.NewRequest("PUT", c.cfg.Addresses[0]+"/employee", strings.NewReader(body))
	req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
	if err != nil {
		return fmt.Errorf("failed to make a create index request: %v", err)
	}

	httpClient := http.Client{}
	req.Header.Add("Content-type", "application/json")
	response, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make a http call to create an index: %v", err)
	}
	defer response.Body.Close()

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed read create index response: %v", err)
	}

	log.Println("debug create index response: ", string(responseBody))

	return nil
}

func (c *Client) InsertData(e *Employee) error {
	body, _ := json.Marshal(e)

	id := strconv.Itoa(e.Id)
	req, err := http.NewRequest("PUT", c.cfg.Addresses[0]+"/employee/_doc/"+id, bytes.NewBuffer(body))
	req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
	if err != nil {
		return fmt.Errorf("failed to make a insert data request: %v", err)
	}

	httpClient := http.Client{}
	req.Header.Add("Content-type", "application/json")
	response, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make a http call to insert data: %v", err)
	}
	defer response.Body.Close()

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed read insert data response: %v", err)
	}

	log.Println("debug insert data response: ", string(responseBody))

	return nil
}

func (c *Client) SeedingData(idStart, n int) error {
	for i := idStart; i < n; i++ {
		if err := c.InsertData(&Employee{
			Id:      i,
			Name:    "person" + strconv.Itoa(i),
			Address: "address" + strconv.Itoa(i),
			Salary:  float64(i * 100),
		}); err != nil {
			return fmt.Errorf("failed seeding data with id %d: %v", i, err)
		}
	}
	return nil
}

type SearchHits struct {
	Hits struct {
		Hits []*struct {
			Source *Employee `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

func (c *Client) SearchData(keyword string) ([]*Employee, error) {
	query := fmt.Sprintf(`
	{
		"query": {
			"match": {
				"name": "%s"
			}
		}
	}
	`, keyword)

	req, err := http.NewRequest("GET", c.cfg.Addresses[0]+"/employee/_search", strings.NewReader(query))
	req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to make a search data request: %v", err)
	}

	httpClient := http.Client{}
	req.Header.Add("Content-type", "application/json")
	response, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make a http call to search data: %v", err)
	}
	defer response.Body.Close()

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("failed read insert data response: %v", err)
	}

	var searchHits SearchHits
	if err := json.Unmarshal(responseBody, &searchHits); err != nil {
		return nil, fmt.Errorf("failed read unmarshal data response: %v", err)
	}

	var employees []*Employee
	for _, hit := range searchHits.Hits.Hits {
		employees = append(employees, hit.Source)
	}
	fmt.Printf("Name is: %v", employees[0].Name)
	return employees, nil
}

func (c *Client) UpdateData(e *Employee) error {
	body, _ := json.Marshal(map[string]*Employee{
		"doc": e,
	})

	id := strconv.Itoa(e.Id)
	req, err := http.NewRequest("POST", c.cfg.Addresses[0]+"/employee/_update/"+id, bytes.NewBuffer(body))
	req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
	if err != nil {
		return fmt.Errorf("failed to make a update data request: %v", err)
	}

	httpClient := http.Client{}
	req.Header.Add("Content-type", "application/json")
	response, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make a http call to update data: %v", err)
	}
	defer response.Body.Close()

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed read update data response: %v", err)
	}

	log.Println("debug update data response: ", string(responseBody))

	return nil
}

func (c *Client) DeleteData(id int) error {

	req, err := http.NewRequest("DELETE", c.cfg.Addresses[0]+"/employee/_doc/"+strconv.Itoa(id), nil)
	req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
	if err != nil {
		return fmt.Errorf("failed to make a delete data request: %v", err)
	}

	httpClient := http.Client{}
	req.Header.Add("Content-type", "application/json")
	response, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make a http call to delete data: %v", err)
	}
	defer response.Body.Close()

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed read delete data response: %v", err)
	}

	log.Println("debug delete data response: ", string(responseBody))

	return nil
}

func main() {
	c := NewClient("http://localhost:9200")
	if err := c.CheckHealth(); err != nil {
		log.Fatal("failed to health check", err)
	}
	if err := c.CreateIndex(); err != nil {
		log.Fatal("failed to create index", err)
	}

	server := Server{client: c}

	http.HandleFunc("/insert", server.InsertDataHandler)
	http.HandleFunc("/update", server.UpdateDataHandler)
	http.HandleFunc("/delete", server.DeleteDataHandler)
	http.HandleFunc("/search", server.SearchDataHandler)
	http.HandleFunc("/health", server.HealthCheckHandler)

	log.Println("listening server on port 8080")
	http.ListenAndServe(":8080", nil)
}

func (s *Server) InsertDataHandler(w http.ResponseWriter, r *http.Request) {
	var employee *Employee
	json.NewDecoder(r.Body).Decode(&employee)
	if err := s.client.InsertData(employee); err != nil {
		writeResponseInternalError(w, err)
		return
	}
	writeResponseOK(w, employee)
}

func (s *Server) UpdateDataHandler(w http.ResponseWriter, r *http.Request) {
	var employee *Employee
	json.NewDecoder(r.Body).Decode(&employee)
	if err := s.client.UpdateData(employee); err != nil {
		writeResponseInternalError(w, err)
		return
	}
	writeResponseOK(w, employee)
}

func (s *Server) DeleteDataHandler(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.Atoi(r.FormValue("id"))
	if err := s.client.DeleteData(id); err != nil {
		writeResponseInternalError(w, err)
		return
	}
	writeResponseOK(w, Employee{Id: id})
}

func (s *Server) SearchDataHandler(w http.ResponseWriter, r *http.Request) {
	keyword := r.FormValue("keyword")
	employees, err := s.client.SearchData(keyword)
	if err != nil {
		writeResponseInternalError(w, err)
		return
	}
	writeResponseOK(w, employees)
}

func (s *Server) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	if err := s.client.CheckHealth(); err != nil {
		writeResponseInternalError(w, err)
		return
	}
	writeResponseOK(w, map[string]string{
		"status": "OK",
	})
}

func writeResponseOK(w http.ResponseWriter, response interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	writeResponse(w, response)
}

func writeResponseInternalError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	writeResponse(w, map[string]interface{}{
		"error": err,
	})
}

func writeResponse(w http.ResponseWriter, response interface{}) {
	json.NewEncoder(w).Encode(response)
}
