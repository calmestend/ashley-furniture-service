package db

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

const DatabaseName string = "ashley.db"

// Core interfaces
type DatabaseEntity interface {
	GetSKU() string
}

type Fetchable[T DatabaseEntity] interface {
	FetchPage(config APIConfig, page int) (*GenericAPIResponse[T], error)
	Transform(entity T) DatabaseEntity
	GetBucketName() string
	GetEndpoint() string
}

// Generic structures
type Link struct {
	Rel  string `json:"rel"`
	Href string `json:"href"`
}

type Metadata struct {
	TotalRecords       int `json:"totalRecords"`
	CurrentPageRecords int `json:"currentPageRecords"`
}

type GenericAPIResponse[T any] struct {
	Links    []Link   `json:"links"`
	Metadata Metadata `json:"metadata"`
	Entities []T      `json:"entities"`
}

type APIConfig struct {
	BaseURL       string
	Authorization string
	ClientID      string
	Customer      string
	Limit         int
}

// Product types
type Product struct {
	ConsumerDescription      string  `json:"consumerDescription"`
	Sku                      string  `json:"sku"`
	ItemSalesCategoryCodeKey string  `json:"itemSalesCategoryCodeKey"`
	SeriesId                 string  `json:"seriesId"`
	ChairQtyPerCarton        int     `json:"chairQtyPerCarton"`
	ItemsPerCase             int     `json:"itemsPerCase"`
	Status                   string  `json:"status"`
	UnitHeightMm             float64 `json:"unitHeightMm"`
	UnitWidthMm              float64 `json:"unitWidthMm"`
	UnitDepthMm              float64 `json:"unitDepthMm"`
	ItemWeightKg             float64 `json:"itemWeightKg"`
}

func (p Product) GetSKU() string { return p.Sku }

type ProductRequestData struct {
	ConsumerDescription      string  `json:"consumerDescription"`
	Sku                      string  `json:"sku"`
	ItemSalesCategoryCodeKey string  `json:"itemSalesCategoryCodeKey"`
	ItemSeries               string  `json:"itemSeries"`
	SeriesId                 string  `json:"seriesId"`
	Price                    float64 `json:"price"`
	SellPrice                float64 `json:"sellPrice"`
	TotalNetPrice            float64 `json:"totalNetPrice"`
	Supplier                 string  `json:"supplier"`
	ChairQtyPerCarton        int     `json:"chairQtyPerCarton"`
	ItemsPerCase             int     `json:"itemsPerCase"`
	Status                   string  `json:"status"`
	UnitHeightMm             float64 `json:"unitHeightMm"`
	UnitWidthMm              float64 `json:"unitWidthMm"`
	UnitDepthMm              float64 `json:"unitDepthMm"`
	ItemWeightKg             float64 `json:"itemWeightKg"`
}

func (p ProductRequestData) GetSKU() string { return p.Sku }

type ProductAPIResponse struct {
	Links    []Link    `json:"links"`
	Metadata Metadata  `json:"metadata"`
	Entities []Product `json:"entities"`
}

type ProductResponseData struct {
	Nombre             string  `json:"nombre"`             // ConsumerDescription
	Clave              string  `json:"clave"`              // Sku
	Categoria          string  `json:"categoria"`          // ItemSalesCategoryCodeKey
	Modelo             string  `json:"modelo"`             // ItemSeries + SeriesId
	Costo              float64 `json:"costo"`              // SellPrice
	Costo2             float64 `json:"costo2"`             // TotalNetPrice
	Proveedor          string  `json:"proveedor"`          // Supplier
	CantidadSillas     int     `json:"cantidadSillas"`     // ChairQtyPerCarton
	CantidadPorPaquete int     `json:"cantidadPorPaquete"` // ItemsPerCase
	Descontinuado      string  `json:"descontinuado"`      // Status
	Alto               float64 `json:"alto"`               // UnitHeightMm
	Largo              float64 `json:"largo"`              // UnitWidthMm
	Ancho              float64 `json:"ancho"`              // UnitDepthMm
	Peso               float64 `json:"peso"`               // ItemWeightKg
}

// Price types
type Price struct {
	Description           string `json:"description"`
	Sku                   string `json:"sku"`
	BasePrice             string `json:"basePrice"`
	SellPrice             string `json:"sellPrice"`
	Surcharge             string `json:"surcharge"`
	FobPoint              string `json:"fobPoint"`
	Discount              string `json:"discount"`
	DfiDiscount           string `json:"dfiDiscount"`
	NetPriceBeforeFreight string `json:"netPriceBeforeFreight"`
	Freight               string `json:"freight"`
	ExpressFreight        string `json:"expressFreight"`
	TotalNetPrice         string `json:"totalNetPrice"`
	ContainerPrice        string `json:"containerPrice"`
}

func (p Price) GetSKU() string { return p.Sku }

type PriceRequestData struct {
	Description           string  `json:"description"`
	Sku                   string  `json:"sku"`
	BasePrice             float64 `json:"basePrice"`
	SellPrice             float64 `json:"sellPrice"`
	Surcharge             float64 `json:"surcharge"`
	FobPoint              string  `json:"fobPoint"`
	Discount              float64 `json:"discount"`
	DfiDiscount           float64 `json:"dfiDiscount"`
	NetPriceBeforeFreight float64 `json:"netPriceBeforeFreight"`
	Freight               float64 `json:"freight"`
	ExpressFreight        float64 `json:"expressFreight"`
	TotalNetPrice         float64 `json:"totalNetPrice"`
	ContainerPrice        float64 `json:"containerPrice"`
}

func (p PriceRequestData) GetSKU() string { return p.Sku }

type PriceAPIResponse struct {
	Links    []Link   `json:"links"`
	Metadata Metadata `json:"metadata"`
	Entities []Price  `json:"entities"`
}

// Fetcher implementations
type ProductFetcher struct{}

func (pf ProductFetcher) FetchPage(config APIConfig, page int) (*GenericAPIResponse[Product], error) {
	url := fmt.Sprintf("%s/products?customer=%s&Limit=%d&Page=%d",
		config.BaseURL, config.Customer, config.Limit, page)

	response, err := makeHTTPRequest[ProductAPIResponse](url, config)
	if err != nil {
		return nil, err
	}

	return &GenericAPIResponse[Product]{
		Links:    response.Links,
		Metadata: response.Metadata,
		Entities: response.Entities,
	}, nil
}

func (pf ProductFetcher) Transform(entity Product) DatabaseEntity {
	return ProductRequestData{
		ConsumerDescription:      entity.ConsumerDescription,
		Sku:                      entity.Sku,
		ItemSalesCategoryCodeKey: entity.ItemSalesCategoryCodeKey,
		SeriesId:                 entity.SeriesId,
		Supplier:                 "Ashley Furniture",
		ChairQtyPerCarton:        entity.ChairQtyPerCarton,
		ItemsPerCase:             entity.ItemsPerCase,
		Status:                   entity.Status,
		UnitHeightMm:             entity.UnitHeightMm,
		UnitWidthMm:              entity.UnitWidthMm,
		UnitDepthMm:              entity.UnitDepthMm,
		ItemWeightKg:             entity.ItemWeightKg,
	}
}

func (pf ProductFetcher) GetBucketName() string { return "products" }
func (pf ProductFetcher) GetEndpoint() string   { return "products" }

type PriceFetcher struct{}

func (pf PriceFetcher) FetchPage(config APIConfig, page int) (*GenericAPIResponse[Price], error) {
	url := fmt.Sprintf("%s/Prices?Customer=%s&Limit=%d&Page=%d",
		config.BaseURL, config.Customer, config.Limit, page)

	response, err := makeHTTPRequest[PriceAPIResponse](url, config)
	if err != nil {
		return nil, err
	}

	return &GenericAPIResponse[Price]{
		Links:    response.Links,
		Metadata: response.Metadata,
		Entities: response.Entities,
	}, nil
}

func (pf PriceFetcher) Transform(entity Price) DatabaseEntity {
	result := PriceRequestData{
		Description: entity.Description,
		Sku:         entity.Sku,
		FobPoint:    entity.FobPoint,
	}

	// Parse price fields with error handling
	result.BasePrice, _ = parseFloat(entity.BasePrice)
	result.SellPrice, _ = parseFloat(entity.SellPrice)
	result.Surcharge, _ = parseFloat(entity.Surcharge)
	result.Discount, _ = parseFloat(entity.Discount)
	result.DfiDiscount, _ = parseFloat(entity.DfiDiscount)
	result.NetPriceBeforeFreight, _ = parseFloat(entity.NetPriceBeforeFreight)
	result.Freight, _ = parseFloat(entity.Freight)
	result.ExpressFreight, _ = parseFloat(entity.ExpressFreight)
	result.TotalNetPrice, _ = parseFloat(entity.TotalNetPrice)
	result.ContainerPrice, _ = parseFloat(entity.ContainerPrice)

	return result
}

func (pf PriceFetcher) GetBucketName() string { return "prices" }
func (pf PriceFetcher) GetEndpoint() string   { return "Prices" }

// Generic HTTP request function with improved error handling
func makeHTTPRequest[T any](url string, config APIConfig) (*T, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Authorization", config.Authorization)
	req.Header.Set("Client_Id", config.ClientID)
	req.Header.Set("Accept-Language", "en")
	req.Header.Set("Accept-Encoding", "gzip, deflate")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		// Check if it's a timeout or network error (retryable)
		if isRetryableError(err) {
			return nil, fmt.Errorf("retryable network error: %v", err)
		}
		return nil, fmt.Errorf("non-retryable request error: %v", err)
	}
	defer resp.Body.Close()

	// Check for retryable HTTP status codes
	if isRetryableStatusCode(resp.StatusCode) {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("retryable HTTP error - status %d: %s", resp.StatusCode, string(body))
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("non-retryable HTTP error - status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %v", err)
	}

	var result T
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %v", err)
	}

	return &result, nil
}

// isRetryableError determines if an error is worth retrying
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())
	retryableErrors := []string{
		"timeout",
		"connection refused",
		"connection reset",
		"no such host",
		"network is unreachable",
		"temporary failure",
		"i/o timeout",
		"context deadline exceeded",
	}

	for _, retryable := range retryableErrors {
		if strings.Contains(errStr, retryable) {
			return true
		}
	}

	return false
}

// isRetryableStatusCode determines if an HTTP status code is worth retrying
func isRetryableStatusCode(statusCode int) bool {
	retryableCodes := []int{
		http.StatusRequestTimeout,      // 408
		http.StatusTooManyRequests,     // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout,      // 504
	}

	for _, code := range retryableCodes {
		if statusCode == code {
			return true
		}
	}

	return false
}

// Generic fetch function with retry logic
func FetchAllEntities[T DatabaseEntity](config APIConfig, fetcher Fetchable[T]) error {
	// Initialize database and bucket
	if err := initBucket(fetcher.GetBucketName()); err != nil {
		return err
	}

	db, err := bolt.Open(DatabaseName, 0600, &bolt.Options{Timeout: 3 * time.Second})
	if err != nil {
		return fmt.Errorf("error opening database: %v", err)
	}
	defer db.Close()

	page := 1
	totalEntities := 0

	for {
		log.Printf("Fetching %s page %d...", fetcher.GetEndpoint(), page)

		// Retry logic for fetching page
		response, err := fetchPageWithRetry(config, fetcher, page, 3)
		if err != nil {
			return fmt.Errorf("error fetching %s page %d after retries: %v", fetcher.GetEndpoint(), page, err)
		}

		// Transform and save entities
		err = saveEntitiesToDatabase(db, fetcher.GetBucketName(), response.Entities, fetcher.Transform)
		if err != nil {
			return fmt.Errorf("error saving %s to database: %v", fetcher.GetEndpoint(), err)
		}

		totalEntities += len(response.Entities)
		log.Printf("Page %d: %d %s processed. Total: %d", page, len(response.Entities), fetcher.GetEndpoint(), totalEntities)

		if isLastPage(response.Links) {
			log.Printf("Reached last page. Total %s processed: %d", fetcher.GetEndpoint(), totalEntities)
			break
		}

		page++
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// fetchPageWithRetry attempts to fetch a page with retry logic
func fetchPageWithRetry[T DatabaseEntity](config APIConfig, fetcher Fetchable[T], page int, maxRetries int) (*GenericAPIResponse[T], error) {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		response, err := fetcher.FetchPage(config, page)
		if err == nil {
			// Success, return the response
			if attempt > 1 {
				log.Printf("Successfully fetched %s page %d on attempt %d", fetcher.GetEndpoint(), page, attempt)
			}
			return response, nil
		}

		lastErr = err
		log.Printf("Attempt %d/%d failed for %s page %d: %v", attempt, maxRetries, fetcher.GetEndpoint(), page, err)

		// If this isn't the last attempt, wait before retrying
		if attempt < maxRetries {
			// Exponential backoff: wait 2^attempt seconds
			backoffTime := time.Duration(1<<uint(attempt)) * time.Second
			log.Printf("Waiting %v before retry %d for %s page %d", backoffTime, attempt+1, fetcher.GetEndpoint(), page)
			time.Sleep(backoffTime)
		}
	}

	// All retries failed
	return nil, fmt.Errorf("failed after %d attempts: %v", maxRetries, lastErr)
}

// Generic save function
func saveEntitiesToDatabase[T DatabaseEntity](db *bolt.DB, bucketName string, entities []T, transformer func(T) DatabaseEntity) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))

		for _, entity := range entities {
			// Transform entity
			transformed := transformer(entity)

			// Serialize to JSON
			data, err := json.Marshal(transformed)
			if err != nil {
				return fmt.Errorf("error marshaling entity %s: %v", entity.GetSKU(), err)
			}

			// Save using SKU as key
			err = bucket.Put([]byte(entity.GetSKU()), data)
			if err != nil {
				return fmt.Errorf("error saving entity %s: %v", entity.GetSKU(), err)
			}
		}

		return nil
	})
}

// Generic get functions
func GetEntity[T DatabaseEntity](bucketName, sku string) (*T, error) {
	db, err := bolt.Open(DatabaseName, 0600, &bolt.Options{Timeout: 3 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}
	defer db.Close()

	var entity T
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		data := bucket.Get([]byte(sku))

		if data == nil {
			return fmt.Errorf("entity not found for SKU %s", sku)
		}

		return json.Unmarshal(data, &entity)
	})

	if err != nil {
		return nil, err
	}

	return &entity, nil
}

func GetAllEntities[T DatabaseEntity](bucketName string) ([]T, error) {
	db, err := bolt.Open(DatabaseName, 0600, &bolt.Options{Timeout: 3 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}
	defer db.Close()

	var entities []T
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))

		return bucket.ForEach(func(k, v []byte) error {
			var entity T
			err := json.Unmarshal(v, &entity)
			if err != nil {
				return err
			}
			entities = append(entities, entity)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	return entities, nil
}

// Utility functions
func initBucket(bucketName string) error {
	db, err := bolt.Open(DatabaseName, 0600, &bolt.Options{Timeout: 3 * time.Second})
	if err != nil {
		return fmt.Errorf("error opening database: %v", err)
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
}

func isLastPage(links []Link) bool {
	var selfHref, lastHref string

	for _, link := range links {
		switch strings.ToLower(link.Rel) {
		case "self":
			selfHref = link.Href
		case "last":
			lastHref = link.Href
		}
	}

	return selfHref != "" && lastHref != "" && selfHref == lastHref
}

func parseFloat(s string) (float64, error) {
	if s == "" {
		return 0.0, nil
	}
	return strconv.ParseFloat(s, 64)
}

// Public API - Backward compatibility
func Init() {
	for _, bucketName := range []string{"products", "prices"} {
		if err := initBucket(bucketName); err != nil {
			log.Fatal(err)
		}
	}
}

func FetchAllProducts(config APIConfig) error {
	fetcher := ProductFetcher{}
	return FetchAllEntities(config, fetcher)
}

func FetchAllPrices(config APIConfig) error {
	fetcher := PriceFetcher{}
	return FetchAllEntities(config, fetcher)
}

func GetProduct(sku string) (*ProductRequestData, error) {
	return GetEntity[ProductRequestData]("products", sku)
}

func GetAllProducts() ([]ProductRequestData, error) {
	return GetAllEntities[ProductRequestData]("products")
}

func GetPrice(sku string) (*PriceRequestData, error) {
	return GetEntity[PriceRequestData]("prices", sku)
}

func GetAllPrices() ([]PriceRequestData, error) {
	return GetAllEntities[PriceRequestData]("prices")
}

// HTTP Handlers
// GetAllProductsHandler serves all products in ProductResponseData format
func GetAllProductsHandler(w http.ResponseWriter, r *http.Request) {
	// Fetch all products from the database
	products, err := GetAllProducts()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error fetching products: %v", err), http.StatusInternalServerError)
		return
	}

	// Fetch all prices from the database
	prices, err := GetAllPrices()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error fetching prices: %v", err), http.StatusInternalServerError)
		return
	}

	// Create a map of SKU to price data for quick lookup
	priceMap := make(map[string]PriceRequestData)
	for _, price := range prices {
		priceMap[price.Sku] = price
	}

	// Transform products into ProductResponseData format
	var response []ProductResponseData
	for _, product := range products {
		// Get corresponding price data
		price, priceExists := priceMap[product.Sku]

		// Create response object
		respData := ProductResponseData{
			Nombre:             product.ConsumerDescription,
			Clave:              product.Sku,
			Categoria:          product.ItemSalesCategoryCodeKey,
			Modelo:             fmt.Sprintf("%s %s", product.ItemSeries, product.SeriesId),
			Proveedor:          product.Supplier,
			CantidadSillas:     product.ChairQtyPerCarton,
			CantidadPorPaquete: product.ItemsPerCase,
			Descontinuado:      product.Status,
			Alto:               product.UnitHeightMm,
			Largo:              product.UnitWidthMm,
			Ancho:              product.UnitDepthMm,
			Peso:               product.ItemWeightKg,
		}

		// Add price data if available
		if priceExists {
			respData.Costo = price.SellPrice
			respData.Costo2 = price.TotalNetPrice
		}

		response = append(response, respData)
	}

	// Set response headers
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Encode and send response
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}
}

// StartServer starts the HTTP server with the products endpoint
func StartServer(port string) error {
	http.HandleFunc("/products", GetAllProductsHandler)
	log.Printf("Starting server on port %s...", port)
	return http.ListenAndServe(":"+port, nil)
}
