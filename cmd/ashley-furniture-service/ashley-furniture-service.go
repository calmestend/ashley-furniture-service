package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/calmestend/ashley-furniture-service/internal/db"
	"github.com/go-co-op/gocron/v2"
	"github.com/joho/godotenv"
)

func main() {
	log.Print("Ashley Furniture Service Starting...")

	// Load environment variables from .env
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	// Parse API_LIMIT as int
	limit, err := strconv.Atoi(os.Getenv("API_LIMIT"))
	if err != nil {
		log.Fatalf("Invalid API_LIMIT: %v", err)
	}

	// Initialize both products and prices buckets
	db.Init()

	config := db.APIConfig{
		BaseURL:       os.Getenv("API_BASE_URL"),
		Authorization: os.Getenv("API_AUTHORIZATION"),
		ClientID:      os.Getenv("API_CLIENT_ID"),
		Customer:      os.Getenv("API_CUSTOMER"),
		Limit:         limit,
	}

	// Create a scheduler
	scheduler, err := gocron.NewScheduler()
	if err != nil {
		log.Fatalf("Error creating scheduler: %v", err)
	}

	// Job to fetch every 15 minutes (change to 6*time.Hour in prod)
	_, err = scheduler.NewJob(
		gocron.DurationJob(6*time.Hour),
		gocron.NewTask(
			func(config db.APIConfig) {
				log.Print("Starting product fetch...")
				if err := db.FetchAllProducts(config); err != nil {
					log.Printf("Error fetching products: %v", err)
					return
				}
				log.Print("Products fetched successfully!")

				log.Print("Starting price fetch...")
				if err := db.FetchAllPrices(config); err != nil {
					log.Printf("Error fetching prices: %v", err)
					return
				}
				log.Print("Prices fetched successfully!")
			},
			config,
		),
	)
	if err != nil {
		log.Fatalf("Error creating cron job: %v", err)
	}

	// Run an initial fetch
	go func() {
		log.Print("Running initial fetch of products and prices...")
		if err := db.FetchAllProducts(config); err != nil {
			log.Printf("Error fetching products: %v", err)
		}
		if err := db.FetchAllPrices(config); err != nil {
			log.Printf("Error fetching prices: %v", err)
		}
	}()

	// Start the scheduler
	log.Print("Starting scheduler...")
	scheduler.Start()

	// Start HTTP server
	log.Print("Starting HTTP server...")
	if err := db.StartServer("8080"); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
