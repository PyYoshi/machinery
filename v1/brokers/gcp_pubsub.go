package brokers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// GCPPubSubBroker represents an Google Cloud Pub/Sub broker
type GCPPubSubBroker struct {
	Broker

	service *pubsub.Client
	sub     *pubsub.Subscription

	processingWG sync.WaitGroup
}

// NewGCPPubSubBroker creates new GCPPubSubBroker instance
func NewGCPPubSubBroker(cnf *config.Config, projectID, subscriptionName string) (Interface, error) {
	b := &GCPPubSubBroker{Broker: New(cnf)}

	if cnf.GCPPubSub != nil && cnf.GCPPubSub.Client != nil {
		b.service = cnf.GCPPubSub.Client
	} else {
		pubsubClient, err := pubsub.NewClient(context.Background(), projectID)
		if err != nil {
			return nil, err
		}
		b.service = pubsubClient
	}

	ctx := context.Background()
	b.sub = b.service.Subscription(subscriptionName)
	subscriptionExists, err := b.sub.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !subscriptionExists {
		return nil, fmt.Errorf("subscription does not exist, instead got %s", subscriptionName)
	}

	return b, nil
}

// StartConsuming enters a loop and waits for incoming messages
func (b *GCPPubSubBroker) StartConsuming(consumerTag string, concurrency int, taskProcessor TaskProcessor) (bool, error) {
	b.startConsuming(consumerTag, taskProcessor)
	deliveries := make(chan *pubsub.Message)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.stopChan:
				cancel()
				return
			default:
				err := b.sub.Receive(ctx, func(_ctx context.Context, msg *pubsub.Message) {
					deliveries <- msg
				})

				if err != nil {
					log.ERROR.Printf("Error when receiving messages. Error: %v", err)
					continue
				}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.retry, err
	}

	return b.retry, nil
}

// StopConsuming quits the loop
func (b *GCPPubSubBroker) StopConsuming() {
	b.stopConsuming()

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()
}

// Publish places a new message on the default queue
func (b *GCPPubSubBroker) Publish(signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	AdjustRoutingKey(b, signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	ctx := context.Background()

	topic := b.service.Topic(b.cnf.DefaultQueue)
	defer topic.Stop()

	topicExists, err := topic.Exists(ctx)
	if err != nil {
		return err
	}
	if !topicExists {
		return fmt.Errorf("topic does not exist, instead got %s", b.cnf.DefaultQueue)
	}

	result := topic.Publish(ctx, &pubsub.Message{
		Data: msg,
	})

	id, err := result.Get(ctx)
	if err != nil {
		log.ERROR.Printf("Error when sending a message: %v", err)
		return err
	}

	log.INFO.Printf("Sending a message successfully, server-generated message ID %v", id)
	return nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *GCPPubSubBroker) consume(deliveries <-chan *pubsub.Message, concurrency int, taskProcessor TaskProcessor) error {
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	errorsChan := make(chan error)

	for {
		select {
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			if concurrency > 0 {
				// get worker from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give worker back to pool
					pool <- struct{}{}
				}
			}()
		case <-b.stopChan:
			return nil
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *GCPPubSubBroker) consumeOne(delivery *pubsub.Message, taskProcessor TaskProcessor) error {
	if len(delivery.Data) == 0 {
		delivery.Nack()
		log.ERROR.Printf("received an empty message, the delivery was %v", delivery)
		return errors.New("Received an empty message")
	}

	sig := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewBuffer(delivery.Data))
	decoder.UseNumber()
	if err := decoder.Decode(sig); err != nil {
		delivery.Nack()
		log.ERROR.Printf("unmarshal error. the delivery is %v", delivery)
		return err
	}

	// If the task is not registered return an error
	// and leave the message in the queue
	if !b.IsTaskRegistered(sig.Name) {
		delivery.Nack()
		return fmt.Errorf("task %s is not registered", sig.Name)
	}

	err := taskProcessor.Process(sig)
	if err != nil {
		delivery.Nack()
		return err
	}

	// Call Ack() after successfully consuming and processing the message
	delivery.Ack()

	return err
}
