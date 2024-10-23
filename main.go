package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

// Event structure definition
type Event struct {
	ApiVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Metadata struct {
		Name              string            `json:"name"`
		Labels            map[string]string `json:"labels,omitempty"`
		DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
		Reason            string            `json:"reason,omitempty"`
		Message           string            `json:"message,omitempty"`
	} `json:"metadata"`
	InvolvedObject struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
		UUID string `json:"uuid,omitempty"`
	} `json:"involvedObject"`
	Action        string `json:"action"`
	EventTime     string `json:"eventTime"`
	Count         int    `json:"count,omitempty"`
	Type          string `json:"type"`
	CurrentStatus string `json:"currentStatus"`
	CorrelationID string `json:"correlationId"`
	UserID        string `json:"userId,omitempty"`
	OrgUUID       string `json:"orgUuId,omitempty"`
}

// MetricData represents the structure for storing metrics
type MetricData struct {
	Timestamp   string            `json:"@timestamp"`
	MetricName  string            `json:"metric_name"`
	Duration    float64           `json:"duration,omitempty"`
	EventName   string            `json:"event_name"`
	EventType   string            `json:"event_type"`
	ObjectKind  string            `json:"object_kind"`
	Status      string            `json:"status,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	ErrorRate   int               `json:"error_rate,omitempty"`
	IsWarning   bool              `json:"is_warning"`
	Type 	    string            `json:"type"`
	EventCount int `json:"event_count,omitempty"`
	Event Event `json:"event,omitempty"`
}

// Server structure to hold dependencies
type Server struct {
	client            *opensearch.Client
	eventCounter      metric.Int64Counter
	durationHistogram metric.Float64Histogram 
	statusCounter     metric.Int64Counter
	errorRateCounter  metric.Int64Counter
}

// Function to create metrics index mapping
func createMetricsIndexMapping(ctx context.Context, client *opensearch.Client) error {
	mapping := `{
		"mappings": {
			"properties": {
				"apiVersion": { 
					"type": "keyword"
				},
				"kind": { 
					"type": "keyword"
				},
				"metadata": {
					"properties": {
						"name": { "type": "keyword" },
						"labels": { "type": "object" },
						"deletionTimestamp": { 
							"type": "date" 
						},
						"reason": { 
							"type": "text",
							"fields": {
								"keyword": {
									"type": "keyword"
								}
							}
						},
						"message": { 
							"type": "text",
							"fields": {
								"keyword": {
									"type": "keyword"
								}
							}
						}
					}
				},
				"involvedObject": {
					"properties": {
						"kind": { "type": "keyword" },
						"name": { "type": "keyword" },
						"uuid": { "type": "keyword" }
					}
				},
				"action": { "type": "keyword" },
				"eventTime": { "type": "date" },
				"count": { 
					"type": "integer",
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					}
				},
				"type": { "type": "keyword" },
				"currentStatus": { "type": "keyword" },
				"correlationId": { "type": "keyword" },
				"userId": { "type": "keyword" },
				"orgUuId": { "type": "keyword" },

				"@timestamp": { "type": "date" },
				"metric_name": { "type": "keyword" },
				"duration": { 
					"type": "float",
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					}
				},
				"event_name": { "type": "keyword" },
				"event_type": { "type": "keyword" },
				"object_kind": { "type": "keyword" },
				"status": { "type": "keyword" },
				"labels": { "type": "object" },
				"error_rate": { 
					"type": "integer",
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					}
				},
				"is_warning": { "type": "boolean" },
				"event_count": { 
					"type": "integer",
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					}
				}
			}
		}
	}`

	req := opensearchapi.IndicesCreateRequest{
		Index: "metrics2",
		Body:  strings.NewReader(mapping),
	}

	res, err := req.Do(ctx, client)
	if err != nil {
		return fmt.Errorf("error creating metrics index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode != 400 { // 400 means index already exists
			respBody, _ := io.ReadAll(res.Body)
			return fmt.Errorf("error creating metrics index: Status: %d, Response: %s", res.StatusCode, string(respBody))
		}
	}
	return nil
}

// Function to track event duration
func (s *Server) trackEventDuration(ctx context.Context, event Event) error {

	if event.EventTime == "" {
 		event.EventTime = time.Now().Format(time.RFC3339)
 	}

	eventTime, err := time.Parse(time.RFC3339, event.EventTime)
	if err != nil {
        fmt.Println("Error parsing event time:", err)
        return err
    }
	// Send duration metric to OpenSearch
	metricData := MetricData{
		Timestamp:  time.Now().Format(time.RFC3339),
		MetricName: "event_duration",
		EventName:  event.Metadata.Name,
		EventType:  event.Action,
		ObjectKind: event.InvolvedObject.Kind,
		Labels:     event.Metadata.Labels,
		Duration:   time.Since(eventTime).Seconds(),
	}

	if err := sendMetricToOpenSearch(ctx, s.client, metricData); err != nil {
		return fmt.Errorf("failed to send duration metric to OpenSearch: %w", err)
	}
	log.Printf("Tracked duration for event: %s, type: %s", event.Metadata.Name, event.Action)
	return nil
}

// Function to track event status distribution
func (s *Server) trackEventStatus(ctx context.Context, event Event) error {
	attrs := []attribute.KeyValue{
		attribute.String("status", event.Type),
		attribute.String("event_type", event.Action),
	}

	s.statusCounter.Add(ctx, 1, metric.WithAttributes(attrs...))

	metricData := MetricData{
		Timestamp:  time.Now().Format(time.RFC3339),
		MetricName: "event_status_distribution",
		EventName:  event.Metadata.Name,
		EventType:  event.Action,
		ObjectKind: event.InvolvedObject.Kind,
		Status:     event.CurrentStatus,
		Labels:     event.Metadata.Labels,
		Type:       event.Type,
	}

	if err := sendMetricToOpenSearch(ctx, s.client, metricData); err != nil {
		return fmt.Errorf("failed to send status metric to OpenSearch: %w", err)
	}

	log.Printf("Tracked status distribution %s for event: %s, type: %s", event.Metadata.Name, event.CurrentStatus, event.Action)
	return nil
}

// Function to track error rate
func (s *Server) trackErrorRate(ctx context.Context, event Event) error {
	if event.Type == "Warning" {
		attrs := []attribute.KeyValue{
			attribute.String("reason", event.Metadata.Reason),
			attribute.String("involved_object_kind", event.InvolvedObject.Kind),
		}
		var errorRateCounterValue int
		s.errorRateCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
		errorRateCounterValue += 1
		// Send error rate metric to OpenSearch
		metricData := MetricData{
			Timestamp:   time.Now().Format(time.RFC3339),
			MetricName:  "error_rate",
			EventName:  event.Metadata.Name,
			EventType:  event.Type,
			ObjectKind: event.InvolvedObject.Kind,
			Labels:     event.Metadata.Labels,
			ErrorRate: errorRateCounterValue, // Increment the error rate counter by 1
			IsWarning:  true,
		}

		if err := sendMetricToOpenSearch(ctx, s.client, metricData); err != nil {
			return fmt.Errorf("failed to send error rate metric to OpenSearch: %w", err)
		}

		log.Printf("Tracked warning event: %s, reason: %s, object kind: %s",
			event.Metadata.Name, event.Metadata.Reason, event.InvolvedObject.Kind)
	}

	return nil
}

var eventCounterValue int
// Function to track event frequency
func trackEventFrequency(ctx context.Context, event Event, eventCounter metric.Int64Counter, client *opensearch.Client) error {
	attrs := []attribute.KeyValue{
		//attribute.String("event_name", event.Metadata.Name),
		attribute.String("event_type", event.Action),
		//attribute.String("involved_object_kind", event.InvolvedObject.Kind),
	}

	eventCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
	eventCounterValue += 1


	if event.EventTime == "" {
 		event.EventTime = time.Now().Format(time.RFC3339)
 	}

	// Parse event time
    eventTime, err := time.Parse(time.RFC3339, event.EventTime)
    if err != nil {
        log.Printf("Error parsing event time: %v", err)
        // If we can't parse the time, we'll use 0 for duration
        eventTime = time.Now()
    }

    // Calculate duration
    duration := time.Since(eventTime).Seconds()

 

	metricData := MetricData{
		Timestamp:  time.Now().Format(time.RFC3339),
		MetricName: "event_frequency",
		EventName:  event.Metadata.Name,
		EventType:  event.Action,
		ObjectKind: event.InvolvedObject.Kind,
		Labels:     event.Metadata.Labels,
		EventCount: eventCounterValue, // Increment the event counter by 1
		Duration:  duration,
		IsWarning:  false,
        Type:       event.Type,
		Event:      event,
	}

	if err := sendMetricToOpenSearch(ctx, client, metricData); err != nil {
		return fmt.Errorf("failed to send metric to OpenSearch: %w", err)
	}

	log.Printf("Tracked event frequency for event: %s, type: %s", event.Metadata.Name, event.Action)

	return nil
}

// Function to send metrics to OpenSearch
func sendMetricToOpenSearch(ctx context.Context, client *opensearch.Client, metric MetricData) error {

	var event Event

	if event.EventTime == "" {
 		event.EventTime = time.Now().Format(time.RFC3339)
 	}

	jsonData, err := json.Marshal(metric)
	if err != nil {
		return fmt.Errorf("error marshaling metric data: %w", err)
	}

	req := opensearchapi.IndexRequest{
		Index: "metrics2",
		Body:  strings.NewReader(string(jsonData)),
	}

	res, err := req.Do(ctx, client)
	if err != nil {
		return fmt.Errorf("error sending metric to OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		respBody, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("error reading error response: %w", err)
		}
		return fmt.Errorf("error indexing metric: Status: %d, Response: %s", res.StatusCode, string(respBody))
	}

	return nil
}


// Function to send event to OpenSearch
func sendEvent(ctx context.Context, event Event, client *opensearch.Client) error {
	// Set event time if not provided
	if event.EventTime == "" {
		event.EventTime = time.Now().Format(time.RFC3339)
	}

	jsonData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling event data: %w", err)
	}

	req := opensearchapi.IndexRequest{
		Index: "events",
		Body:  strings.NewReader(string(jsonData)),
	}

	res, err := req.Do(ctx, client)
	if err != nil {
		return fmt.Errorf("error sending event to OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		respBody, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("error reading error response: %w", err)
		}
		return fmt.Errorf("error indexing document: Status: %d, Response: %s", res.StatusCode, string(respBody))
	}

	return nil
}

// Handler for processing incoming events
func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if err := trackEventFrequency(ctx, event, s.eventCounter, s.client); err != nil {
		log.Printf("Failed to track event frequency: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Track event duration (new functionality)
	if err := s.trackEventDuration(ctx, event); err != nil {
		log.Printf("Failed to track event duration: %v", err)
		// Continue processing even if duration tracking fails
	}

	// Track event status distribution (new functionality)
	if err := s.trackEventStatus(ctx, event); err != nil {
		log.Printf("Failed to track event status: %v", err)
	}

	// Track error rate (new)
	if err := s.trackErrorRate(ctx, event); err != nil {
		log.Printf("Failed to track error rate: %v", err)
	}

	if err := sendEvent(ctx, event, s.client); err != nil {
		log.Printf("Failed to send event to OpenSearch: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Event processed successfully",
	})
}

// Health check handler
func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "healthy",
		"message": "Service is running",
	})
}

func main() {
	ctx := context.Background()

	// Initialize OpenSearch client
	cfg := opensearch.Config{
		Addresses: []string{"https://localhost:9200"},
		Username:  "admin",
		Password:  "Wasuki@1999",
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	client, err := opensearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %v", err)
	}

	// Create metrics index mapping
	if err := createMetricsIndexMapping(ctx, client); err != nil {
		log.Printf("Warning: Failed to create metrics index mapping: %v", err)
	}
	// Initialize metrics
	// meter := otel.GetMeterProvider().Meter("event-metrics")
	meter := otel.GetMeterProvider().Meter("event-frequency")
	eventCounter, err := meter.Int64Counter(
		"event_frequency",
		metric.WithDescription("Number of times an event has occurred"),
	)
	if err != nil {
		log.Fatalf("Failed to create event counter: %v", err)
	}

	// Create duration histogram (new)
	durationHistogram, err := meter.Float64Histogram(
		"event_duration",
		metric.WithDescription("Duration of event series in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		log.Fatalf("Failed to create duration histogram: %v", err)
	}

	// Create status counter (new)
	statusCounter, err := meter.Int64Counter(
		"event_status_distribution",
		metric.WithDescription("Distribution of event statuses"),
		metric.WithUnit("1"),
	)
	if err != nil {
		log.Fatalf("Failed to create status counter: %v", err)
	}

	// Create error rate counter (new)
	errorRateCounter, err := meter.Int64Counter(
		"error_rate",
		metric.WithDescription("Rate of warning events"),
		metric.WithUnit("1"),
	)
	if err != nil {
		log.Fatalf("Failed to create error rate counter: %v", err)
	}


	// Initialize server
	server := &Server{
		client:            client,
		eventCounter:      eventCounter,
		durationHistogram: durationHistogram,
		statusCounter:     statusCounter,
		errorRateCounter:  errorRateCounter,
	}

	// Set up HTTP routes
	http.HandleFunc("/event", server.handleEvent)
	http.HandleFunc("/health", server.handleHealthCheck)

	// Start the server
	port := ":8080"
	log.Printf("Starting server on port %s", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}


// package main

// import (
// 	"context"
// 	"crypto/tls"
// 	"encoding/json"
// 	"fmt"
// 	"io"
// 	"log"
// 	"net/http"
// 	"strings"
// 	"time"

// 	"github.com/opensearch-project/opensearch-go"
// 	"github.com/opensearch-project/opensearch-go/opensearchapi"
// )

// // Event structure definition
// type Event struct {
// 	APIVersion string `json:"apiVersion,omitempty"`
// 	Kind       string `json:"kind,omitempty"`
// 	Metadata   struct {
// 		Name              string            `json:"name"`
// 		Labels            map[string]string `json:"labels,omitempty"`
// 		DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
// 		Reason            string            `json:"reason,omitempty"`
// 		Message           string            `json:"message,omitempty"`
// 	} `json:"metadata"`
// 	InvolvedObject struct {
// 		Kind string `json:"kind"`
// 		Name string `json:"name"`
// 		UUID string `json:"uuid,omitempty"`
// 	} `json:"involvedObject"`
// 	Action        string `json:"action"`
// 	EventTime     string `json:"eventTime"`
// 	Count         int    `json:"count,omitempty"`
// 	Type          string `json:"type"`
// 	CurrentStatus string `json:"currentStatus"`
// 	CorrelationID string `json:"correlationId"`
// 	UserID        string `json:"userId,omitempty"`
// 	OrgUUID       string `json:"orgUuId,omitempty"`
// }

// // Server structure to hold dependencies
// type Server struct {
// 	client *opensearch.Client
// }

// // Function to create events index mapping
// func createEventsIndexMapping(ctx context.Context, client *opensearch.Client) error {
// 	mapping := `{
// 		"mappings": {
// 			"properties": {
// 				"apiVersion": { "type": "keyword" },
// 				"kind": { "type": "keyword" },
// 				"metadata": {
// 					"properties": {
// 						"name": { "type": "keyword" },
// 						"labels": { "type": "object" },
// 						"deletionTimestamp": { "type": "date" },
// 						"reason": { "type": "text" },
// 						"message": { "type": "text" }
// 					}
// 				},
// 				"involvedObject": {
// 					"properties": {
// 						"kind": { "type": "keyword" },
// 						"name": { "type": "keyword" },
// 						"uuid": { "type": "keyword" }
// 					}
// 				},
// 				"action": { "type": "keyword" },
// 				"eventTime": { "type": "date" },
// 				"count": { "type": "integer" },
// 				"type": { "type": "keyword" },
// 				"currentStatus": { "type": "keyword" },
// 				"correlationId": { "type": "keyword" },
// 				"userId": { "type": "keyword" },
// 				"orgUuId": { "type": "keyword" }
// 			}
// 		}
// 	}`

// 	req := opensearchapi.IndicesCreateRequest{
// 		Index: "events",
// 		Body:  strings.NewReader(mapping),
// 	}

// 	res, err := req.Do(ctx, client)
// 	if err != nil {
// 		return fmt.Errorf("error creating events index: %w", err)
// 	}
// 	defer res.Body.Close()

// 	if res.IsError() {
// 		if res.StatusCode != 400 { // 400 means index already exists
// 			respBody, _ := io.ReadAll(res.Body)
// 			return fmt.Errorf("error creating events index: Status: %d, Response: %s", res.StatusCode, string(respBody))
// 		}
// 	}
// 	return nil
// }

// // Function to send event to OpenSearch
// func sendEventToOpenSearch(ctx context.Context, client *opensearch.Client, event Event) error {
// 	// Set event time if not provided
// 	if event.EventTime == "" {
// 		event.EventTime = time.Now().Format(time.RFC3339)
// 	}

// 	jsonData, err := json.Marshal(event)
// 	if err != nil {
// 		return fmt.Errorf("error marshaling event data: %w", err)
// 	}

// 	req := opensearchapi.IndexRequest{
// 		Index: "events",
// 		Body:  strings.NewReader(string(jsonData)),
// 	}

// 	res, err := req.Do(ctx, client)
// 	if err != nil {
// 		return fmt.Errorf("error sending event to OpenSearch: %w", err)
// 	}
// 	defer res.Body.Close()

// 	if res.IsError() {
// 		respBody, err := io.ReadAll(res.Body)
// 		if err != nil {
// 			return fmt.Errorf("error reading error response: %w", err)
// 		}
// 		return fmt.Errorf("error indexing event: Status: %d, Response: %s", res.StatusCode, string(respBody))
// 	}

// 	return nil
// }

// // Handler for processing incoming events
// func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
// 	if r.Method != http.MethodPost {
// 		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
// 		return
// 	}

// 	var event Event
// 	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
// 		http.Error(w, "Invalid request body", http.StatusBadRequest)
// 		return
// 	}

// 	ctx := r.Context()
// 	if err := sendEventToOpenSearch(ctx, s.client, event); err != nil {
// 		log.Printf("Failed to send event to OpenSearch: %v", err)
// 		http.Error(w, "Internal server error", http.StatusInternalServerError)
// 		return
// 	}

// 	w.WriteHeader(http.StatusOK)
// 	json.NewEncoder(w).Encode(map[string]string{
// 		"status":  "success",
// 		"message": "Event processed successfully",
// 	})
// }

// // Health check handler
// func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
// 	if r.Method != http.MethodGet {
// 		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
// 		return
// 	}

// 	w.WriteHeader(http.StatusOK)
// 	json.NewEncoder(w).Encode(map[string]string{
// 		"status":  "healthy",
// 		"message": "Service is running",
// 	})
// }

// func main() {
// 	ctx := context.Background()

// 	// Initialize OpenSearch client
// 	cfg := opensearch.Config{
// 		Addresses: []string{"https://localhost:9200"},
// 		Username:  "admin",
// 		Password:  "Wasuki@1999",
// 		Transport: &http.Transport{
// 			TLSClientConfig: &tls.Config{
// 				InsecureSkipVerify: true,
// 			},
// 		},
// 	}

// 	client, err := opensearch.NewClient(cfg)
// 	if err != nil {
// 		log.Fatalf("Error creating the client: %v", err)
// 	}

// 	// Create events index mapping
// 	if err := createEventsIndexMapping(ctx, client); err != nil {
// 		log.Printf("Warning: Failed to create events index mapping: %v", err)
// 	}

// 	// Initialize server
// 	server := &Server{
// 		client: client,
// 	}

// 	// Set up HTTP routes
// 	http.HandleFunc("/event", server.handleEvent)
// 	http.HandleFunc("/health", server.handleHealthCheck)

// 	// Start the server
// 	port := ":8080"
// 	log.Printf("Starting server on port %s", port)
// 	if err := http.ListenAndServe(port, nil); err != nil {
// 		log.Fatalf("Failed to start server: %v", err)
// 	}
// }