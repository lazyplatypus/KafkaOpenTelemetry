// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	brokers = flag.String("brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma separated list")
)

// tracerProvider creates a new trace provider instance and registers it as global trace provider.
func tracerProvider() (*sdktrace.TracerProvider, error) {
	// Create the Jaeger exporter

	ctx := context.Background()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", "OTel-Kafka-Consumer"),
		),
	)

	exporter, err := otlptrace.New(
		ctx,
		otlptracegrpc.NewClient(),
	)
	if err != nil {
		log.Fatalf("%s: %v", "failed to create metric exporter", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp, nil

}

func main() {

	tp, tperr := tracerProvider()

	if tperr != nil {
		log.Fatal(tperr)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cleanly shutdown and flush telemetry when the application exits.
	defer func(ctx context.Context) {
		// Do not make the application hang when it is shutdown.
		ctx, cancel = context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			log.Fatal(err)
		}
	}(ctx)

	flag.Parse()

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	startConsumerGroup(brokerList)

	select {}
}

func startConsumerGroup(brokerList []string) {
	consumerGroupHandler := Consumer{}
	// Wrap instrumentation
	propagators := propagation.TraceContext{}
	handler := otelsarama.WrapConsumerGroupHandler(&consumerGroupHandler, otelsarama.WithPropagators(propagators))

	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Create consumer group
	consumerGroup, err := sarama.NewConsumerGroup(brokerList, "example", config)
	if err != nil {
		log.Fatalln("Failed to start sarama consumer group:", err)
	}

	topicName, exists := os.LookupEnv("KAFKA_TOPIC")
	if !exists {
		log.Println("Using default topic name kafkademo")
		topicName = "kafkademo"
	}

	err = consumerGroup.Consume(context.Background(), []string{topicName}, handler)
	if err != nil {
		log.Fatalln("Failed to consume via handler:", err)
	}
}

func printMessage(msg *sarama.ConsumerMessage) {
	// Extract tracing info from message

	propagators := propagation.TraceContext{}
	ctx := propagators.Extract(context.Background(), otelsarama.NewConsumerMessageCarrier(msg))
	log.Println("HEADERS:", msg.Headers)
	tr := otel.Tracer("consumer")

	// Create a span.printMessage

	_, span := tr.Start(ctx, "print message")

	defer span.End()

	// Inject current span context, so any further processing can use it to propagate span.
	propagators.Inject(ctx, otelsarama.NewConsumerMessageCarrier(msg))

	// Emulate Work Loads (or any further processing as needed)
	time.Sleep(35 * time.Second)

	span.SetAttributes(attribute.String("test-consumer-span-key", "test-consumer-span-value"))

	// Set any additional attributes that might make sense
	// span.SetAttributes(attribute.String("consumed message at offset",strconv.FormatInt(int64(msg.Offset),10)))
	// span.SetAttributes(attribute.String("consumed message to partition",strconv.FormatInt(int64(msg.Partition),10)))
	span.SetAttributes(attribute.String("message_bus.destination", msg.Topic))

	log.Println("Successful to read message: ", string(msg.Value), "at offset of ", msg.Offset)
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Setup has been triggered")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		time.Sleep(1 * time.Second)
		printMessage(message)
		session.MarkMessage(message, "")
	}

	return nil
}