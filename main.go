package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

const connStringRegex = `Endpoint=sb://(?P<endpoint>.+)/;SharedAccessKeyName=.+;SharedAccessKey=.+`

type detectingLogger struct {
	out     io.Writer
	level   kgo.LogLevel
	sawBug  atomic.Bool
	sawJoin atomic.Bool
}

func (l *detectingLogger) Level() kgo.LogLevel { return l.level }

func (l *detectingLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	fmt.Fprintf(l.out, "[%s] %s", level, msg)
	for i := 0; i+1 < len(keyvals); i += 2 {
		fmt.Fprintf(l.out, " %v=%v", keyvals[i], keyvals[i+1])
	}
	fmt.Fprintln(l.out)

	if strings.Contains(msg, "fetch offsets failed with group-level error") {
		l.sawBug.Store(true)
	}
	if strings.Contains(msg, "successful join") {
		l.sawJoin.Store(true)
	}
}

func main() {
	conn := os.Getenv("EVENTHUB_CONNECTION_STRING")
	topic := os.Getenv("EVENTHUB_TOPIC")
	group := os.Getenv("EVENTHUB_GROUP")

	flag.StringVar(&topic, "topic", topic, "Event Hub name (Kafka topic). Env: EVENTHUB_TOPIC")
	flag.StringVar(&group, "group", group, "Consumer group ID. Env: EVENTHUB_GROUP")
	dur := flag.Duration("duration", 30*time.Second, "How long to keep polling before declaring a verdict")
	flag.Parse()

	if conn == "" || topic == "" || group == "" {
		log.Fatal("required: EVENTHUB_CONNECTION_STRING (env), EVENTHUB_TOPIC (env or -topic), EVENTHUB_GROUP (env or -group)")
	}

	matches := regexp.MustCompile(connStringRegex).FindStringSubmatch(conn)
	if len(matches) != 2 {
		log.Fatal("connection string format invalid; expected Endpoint=sb://NS.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...")
	}
	endpoint := matches[1] + ":9093"

	logger := &detectingLogger{out: os.Stdout, level: kgo.LogLevelDebug}

	v := kversion.Stable()
	v.SetMaxKeyVersion(9, 7)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(endpoint),
		kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "$ConnectionString", Pass: conn}, nil
		})),
		kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.MinVersions(kversion.V1_0_0()),
		kgo.MaxVersions(v),
		kgo.WithLogger(logger),
	)
	if err != nil {
		log.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	pollCtx, pollCancel := context.WithTimeout(ctx, *dur)
	defer pollCancel()

	fmt.Printf("\n=== polling %s/%s as group %s for %s ===\n\n", endpoint, topic, group, *dur)
	fetches := cl.PollFetches(pollCtx)

	fmt.Println()
	fmt.Println("=== verdict ===")
	if logger.sawBug.Load() {
		fmt.Println("FAIL: saw \"fetch offsets failed with group-level error\" — franz-go#1312 reproduced.")
		fmt.Println("      (this is the broken behavior; expected on unpatched v1.21.0 against Event Hubs)")
		os.Exit(2)
	}
	fmt.Printf("PASS: no group-level OffsetFetch error during %s\n", *dur)
	fmt.Printf("      records polled: %d, group successfully joined: %v\n", fetches.NumRecords(), logger.sawJoin.Load())
}
