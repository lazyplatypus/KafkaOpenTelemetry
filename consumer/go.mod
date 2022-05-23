module go-tracing-kafka/consumer

go 1.15

require (
	github.com/Shopify/sarama v1.28.0
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dghubble/go-twitter v0.0.0-20220319054129-995614af6514 // indirect
	github.com/dghubble/oauth1 v0.7.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama v0.19.0
	go.opentelemetry.io/otel v0.20.0
	go.opentelemetry.io/otel/exporters/trace/jaeger v0.20.0
	go.opentelemetry.io/otel/sdk v0.20.0
)
