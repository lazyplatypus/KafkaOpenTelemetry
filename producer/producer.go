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
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"

	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"go.opentelemetry.io/otel/propagation"
	otrace "go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel/exporters/trace/jaeger"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	brokers = flag.String("brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma separated list")
)

// initTracer creates a new trace provider instance and registers it as global trace provider.
func tracerProvider(url string) (*sdktrace.TracerProvider, error) {
	// Create the Jaeger exporter
	exp, err := jaeger.NewRawExporter(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			attribute.String("service.name", "kafka-producer"),
			attribute.String("exporter", "jaeger"),
		)),
	)
	otel.SetTracerProvider(tp)
	return tp, nil

}

func kafka(name string, question string) {
	tp, tperr := tracerProvider("http://127.0.0.1:14268/api/traces")
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

	topicName, exists := os.LookupEnv("KAFKA_TOPIC")
	if !exists {
		log.Println("Using default topic name kafkademo")
		topicName = "kafkademo"
	}
	// Create root span encompassing prior work + producing to the kafka topic

	tr := tp.Tracer("producer")
	ctx, span := tr.Start(context.Background(), "produce message")
	defer span.End()
	propagators := propagation.TraceContext{}

	producer := newAccessLogProducer(brokerList, topicName, otel.GetTracerProvider(), propagators)

	rand.Seed(time.Now().Unix())

	// Inject tracing info into message
	msg := sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("question"),
		Value: sarama.StringEncoder(fmt.Sprintf("✋%s: %s", name, question)),
	}

	propagators.Inject(ctx, otelsarama.NewProducerMessageCarrier(&msg))
	producer.Input() <- &msg
	successMsg := <-producer.Successes()
	log.Println("Successful to write message, offset:", successMsg.Offset)

	span.SetAttributes(attribute.String("test-producer-span-key", "test-producer-span-value"))
	// span.SetAttributes(attribute.String("sent message at offset",strconv.FormatInt(int64(successMsg.Offset),10)))
	// span.SetAttributes(attribute.String("sent message to partition",strconv.FormatInt(int64(successMsg.Partition),10)))

	err := producer.Close()
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.Fatalln("Failed to close producer:", err)
	}
}

func Parse(w http.ResponseWriter, req *http.Request) {
	//Get Email Values
	name := req.FormValue("name")
	question := req.FormValue("question")
	kafka(name, question)
}

func main() {
	http.HandleFunc("/", Parse)
	err := http.ListenAndServe(":3003", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func newAccessLogProducer(brokerList []string, topicName string, tracerProvider otrace.TracerProvider, propagators propagation.TraceContext) sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	// Wrap instrumentation - pass in the tracer provider and the appropriate propagator
	producer = otelsarama.WrapAsyncProducer(config, producer, otelsarama.WithTracerProvider(tracerProvider), otelsarama.WithPropagators(propagators))
	log.Println("propogators:", producer)

	// We will log to STDOUT if we're not able to produce messages.
	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write message:", err)
		}
	}()

	return producer
}
