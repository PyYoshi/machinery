package integration_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
)

func TestGCPPubSubRedis(t *testing.T) {
	// start Cloud Pub/Sub emulator
	// $ LANG=C gcloud beta emulators pubsub start
	// $ eval $(LANG=C gcloud beta emulators pubsub env-init)

	pubsubURL := os.Getenv("GCPPUBSUB_URL")
	if pubsubURL == "" {
		t.Skip("GCPPUBSUB_URL is not defined")
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	// Redis broker, Redis result backend
	server := testSetup(&config.Config{
		Broker:        pubsubURL,
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("redis://%v", redisURL),
	})
	worker := server.NewWorker("test_worker", 0)
	go worker.Launch()
	testAll(server, t)
	worker.Quit()
}
