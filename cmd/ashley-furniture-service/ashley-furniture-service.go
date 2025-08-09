package main

import (
	"github.com/calmestend/ashley-furniture-service/internal/db"
	"github.com/go-co-op/gocron/v2"
	"log"
	"time"
)

func main() {
	log.Print("Ashley Furniture Service Starting...")
	log.Print("Starting DB Connection")

	// Initialize both products and prices buckets
	db.Init()
	config := db.APIConfig{
		BaseURL:       "https://apigw3.ashleyfurniture.com/productinformation",
		Authorization: "Basic QURfU2lzdGVtYXMzOkNvbG9yb3JhbmdlMjAyNQ==",
		ClientID:      "92a3dada36874cf6a67c0325a32e1645",
		Customer:      "3423300",
		Limit:         1000,
	}

	// Create a scheduler
	scheduler, err := gocron.NewScheduler()
	if err != nil {
		log.Fatalf("Error creating scheduler: %v", err)
	}

	// Add a job to fetch products and prices every 6 hours
	_, err = scheduler.NewJob(
		gocron.DurationJob(
			// 6*time.Hour,
			15*time.Minute,
		),
		gocron.NewTask(
			func(config db.APIConfig) {
				log.Print("Starting product fetch...")
				err := db.FetchAllProducts(config)
				if err != nil {
					log.Printf("Error fetching products: %v", err)
					return
				}
				log.Print("Products fetched successfully!")

				log.Print("Starting price fetch...")
				err = db.FetchAllPrices(config)
				if err != nil {
					log.Printf("Error fetching prices: %v", err)
					return
				}
				log.Print("Prices fetched successfully!")

				// Verify products
				allProducts, err := db.GetAllProducts()
				if err != nil {
					log.Printf("Error getting all products from database: %v", err)
				} else {
					log.Printf("Total products stored in database: %d", len(allProducts))
					if len(allProducts) > 0 {
						log.Printf("First product example: %+v", allProducts[0])
					}
				}

				// Verify prices
				allPrices, err := db.GetAllPrices()
				if err != nil {
					log.Printf("Error getting all prices from database: %v", err)
				} else {
					log.Printf("Total prices stored in database: %d", len(allPrices))
					if len(allPrices) > 0 {
						log.Printf("First price example: %+v", allPrices[0])
					}
				}
			},
			config,
		),
	)
	if err != nil {
		log.Fatalf("Error creating cron job: %v", err)
	}

	// Run fetch immediately before scheduler starts
	go func() {
		log.Print("Running initial fetch of products and prices...")

		err := db.FetchAllProducts(config)
		if err != nil {
			log.Printf("Error fetching products: %v", err)
		} else {
			log.Print("Products fetched successfully!")
		}

		err = db.FetchAllPrices(config)
		if err != nil {
			log.Printf("Error fetching prices: %v", err)
		} else {
			log.Print("Prices fetched successfully!")
		}

		// Optional: verify what was fetched
		allProducts, err := db.GetAllProducts()
		if err != nil {
			log.Printf("Error getting all products: %v", err)
		} else {
			log.Printf("Total products: %d", len(allProducts))
			if len(allProducts) > 0 {
				log.Printf("First product: %+v", allProducts[0])
			}
		}

		allPrices, err := db.GetAllPrices()
		if err != nil {
			log.Printf("Error getting all prices: %v", err)
		} else {
			log.Printf("Total prices: %d", len(allPrices))
			if len(allPrices) > 0 {
				log.Printf("First price: %+v", allPrices[0])
			}
		}
	}()

	// Start the scheduler
	log.Print("Starting scheduler for product and price fetching every 6 hours...")
	scheduler.Start()

	// Start the HTTP server
	log.Print("Starting HTTP server...")
	err = db.StartServer("8080")
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
