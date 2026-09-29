// pulsar_tool is a minimal CLI to inspect / poke an Apache Pulsar topic.
//
// Subcommands:
//
//	status  show the current cursor (last acked msg id) of a subscription
//	        on every partition of a topic
//	seek    move the subscription cursor to earliest / latest / a time /
//	        an explicit msg id
//	recv    receive exactly one message and print it
//	send    send exactly one message (payload from stdin, a file, or a literal)
//
// The tool treats message payloads as opaque bytes; it does not know about
// any application-level schema.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/away/pulsar_tool/pulsarutil"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch cmd {
	case "status":
		err = cmdStatus(ctx, args)
	case "seek":
		err = cmdSeek(ctx, args)
	case "recv":
		err = cmdRecv(ctx, args)
	case "send":
		err = cmdSend(ctx, args)
	case "version", "-v", "--version":
		fmt.Println(version)
		return
	case "help", "-h", "--help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("cancelled")
			return
		}
		slog.Error(cmd+" failed", "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `pulsar_tool %s - minimal Apache Pulsar inspection CLI

Usage:
  pulsar_tool <subcommand> [flags]

Subcommands:
  status   show the last-acked msg id of a subscription on every partition
  seek     move a subscription cursor to earliest / latest / time / msg id
  recv     receive exactly one message and print it
  send     send exactly one message

Run 'pulsar_tool <subcommand> -h' to see the flags of a subcommand.
`, version)
}

// -----------------------------------------------------------------------------
// Common flag helpers
// -----------------------------------------------------------------------------

// commonFlags are the flags every subcommand accepts.
type commonFlags struct {
	url          string
	topic        string
	subscription string
	consumer     string
	subType      int
}

func (c *commonFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&c.url, "url", "pulsar://localhost:6650", "pulsar service url")
	fs.StringVar(&c.topic, "topic", "", "topic name (required)")
	fs.StringVar(&c.subscription, "sub", "pulsar-tool", "subscription name")
	fs.StringVar(&c.consumer, "consumer", "", "consumer name (optional)")
	fs.IntVar(&c.subType, "sub-type", 0, "subscription type: 0=exclusive 1=shared 2=failover 3=key_shared")
}

func (c *commonFlags) validate() error {
	if c.topic == "" {
		return errors.New("-topic is required")
	}
	return nil
}

// newClient builds a PulsarClient with the common consumer / subType applied.
func (c *commonFlags) newClient() (*pulsarutil.PulsarClient, error) {
	client, err := pulsarutil.NewPulsarClient(c.url)
	if err != nil {
		return nil, fmt.Errorf("new pulsar client: %w", err)
	}
	client.SetConsumerName(c.consumer)
	client.SetSubscriberType(c.subType)
	return client, nil
}

// -----------------------------------------------------------------------------
// status
// -----------------------------------------------------------------------------

func cmdStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	var cf commonFlags
	cf.register(fs)
	timeout := fs.Duration("timeout", 2*time.Second, "per-partition receive timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := cf.validate(); err != nil {
		return err
	}

	client, err := cf.newClient()
	if err != nil {
		return err
	}
	defer client.Close()

	parts, err := client.TopicPartitions(cf.topic)
	if err != nil {
		return fmt.Errorf("get topic partitions: %w", err)
	}
	maxWidth := 0
	for _, p := range parts {
		if len(p) > maxWidth {
			maxWidth = len(p)
		}
	}
	for i, p := range parts {
		if err := ctx.Err(); err != nil {
			return err
		}
		consumer, err := client.CreateConsumer(p, cf.subscription)
		if err != nil {
			slog.Error("subscribe failed", "partition", p, "err", err)
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, *timeout)
		msg, err := consumer.Receive(rctx)
		cancel()
		if err != nil {
			slog.Warn("no message received (empty topic or timeout)", "partition", p, "err", err)
			consumer.Close()
			continue
		}
		id := pulsarutil.CloneMsgIDSetPartition(msg.ID(), i)
		fmt.Printf("%-*s  msgid:%s  time:%s  size:%d\n",
			maxWidth, p,
			pulsarutil.MsgIDToString(id),
			msg.PublishTime().Format("2006-01-02 15:04:05"),
			len(msg.Payload()),
		)
		consumer.Close()
	}
	return nil
}

// -----------------------------------------------------------------------------
// seek
// -----------------------------------------------------------------------------

func cmdSeek(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("seek", flag.ExitOnError)
	var cf commonFlags
	cf.register(fs)
	to := fs.String("to", "", "target: earliest | latest | ledger:entry:partition:batch")
	when := fs.String("time", "", "seek by wall-clock time (format: 2006-01-02 15:04:05, local timezone)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := cf.validate(); err != nil {
		return err
	}
	if (*to == "" && *when == "") || (*to != "" && *when != "") {
		return errors.New("exactly one of -to or -time is required")
	}

	client, err := cf.newClient()
	if err != nil {
		return err
	}
	defer client.Close()

	switch {
	case *when != "":
		t, err := time.ParseInLocation("2006-01-02 15:04:05", *when, time.Local)
		if err != nil {
			return fmt.Errorf("parse time %q: %w", *when, err)
		}
		slog.Info("seek by time", "topic", cf.topic, "time", t)
		if err := client.SeekByTime(cf.topic, cf.subscription, t); err != nil {
			return fmt.Errorf("seek by time: %w", err)
		}
	case *to == "earliest":
		slog.Info("seek to earliest", "topic", cf.topic)
		if err := client.SeekEarliest(cf.topic, cf.subscription); err != nil {
			return fmt.Errorf("seek earliest: %w", err)
		}
	case *to == "latest":
		slog.Info("seek to latest", "topic", cf.topic)
		if err := client.SeekLatest(cf.topic, cf.subscription); err != nil {
			return fmt.Errorf("seek latest: %w", err)
		}
	default:
		target := pulsarutil.ResolveMsgID(*to)
		if target == nil {
			return fmt.Errorf("cannot parse msgid %q (want ledger:entry:partition:batch)", *to)
		}
		ptopic, mid, err := client.PrepareSeekInfo(cf.topic, target)
		if err != nil {
			return fmt.Errorf("prepare seek info: %w", err)
		}
		consumer, err := client.CreateConsumer(ptopic, cf.subscription)
		if err != nil {
			return fmt.Errorf("create consumer: %w", err)
		}
		defer consumer.Close()
		slog.Info("seek to msgid", "topic", ptopic, "msgid", pulsarutil.MsgIDToString(mid))
		if err := consumer.Seek(mid); err != nil {
			return fmt.Errorf("seek: %w", err)
		}
	}
	fmt.Println("seek done")
	return nil
}

// -----------------------------------------------------------------------------
// recv (exactly one message)
// -----------------------------------------------------------------------------

func cmdRecv(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("recv", flag.ExitOnError)
	var cf commonFlags
	cf.register(fs)
	from := fs.String("from", "", "optional: seek before receive; same syntax as 'seek -to'")
	ack := fs.Bool("ack", false, "ack the received message")
	timeout := fs.Duration("timeout", 5*time.Second, "receive timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := cf.validate(); err != nil {
		return err
	}

	client, err := cf.newClient()
	if err != nil {
		return err
	}
	defer client.Close()

	topic := cf.topic
	var seekTo pulsar.MessageID
	if *from != "" {
		target := pulsarutil.ResolveMsgID(*from)
		if target == nil {
			return fmt.Errorf("cannot parse -from %q", *from)
		}
		ptopic, mid, err := client.PrepareSeekInfo(cf.topic, target)
		if err != nil {
			return fmt.Errorf("prepare seek info: %w", err)
		}
		topic, seekTo = ptopic, mid
	}

	consumer, err := client.CreateConsumer(topic, cf.subscription)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	defer consumer.Close()

	if seekTo != nil {
		if err := consumer.Seek(seekTo); err != nil {
			return fmt.Errorf("seek: %w", err)
		}
	}

	rctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	msg, err := consumer.Receive(rctx)
	if err != nil {
		return fmt.Errorf("receive: %w", err)
	}

	printMsg(topic, msg)

	if *ack {
		if err := consumer.Ack(msg); err != nil {
			return fmt.Errorf("ack: %w", err)
		}
	}
	return nil
}

func printMsg(topic string, msg pulsar.Message) {
	id := msg.ID()
	payload := msg.Payload()
	fmt.Println(strings.Repeat("-", 60))
	fmt.Println("topic:  ", topic)
	fmt.Println("msgid:  ", pulsarutil.MsgIDToString(id))
	fmt.Println("time:   ", msg.PublishTime())
	fmt.Println("key:    ", msg.Key())
	fmt.Println("size:   ", len(payload))
	if props := msg.Properties(); len(props) > 0 {
		fmt.Println("props:  ", props)
	}
	if isPrintable(payload) {
		fmt.Printf("payload (utf-8):\n%s\n", payload)
	} else {
		fmt.Printf("payload (hex):\n%s\n", hex.EncodeToString(payload))
	}
}

func isPrintable(b []byte) bool {
	if len(b) == 0 {
		return true
	}
	for _, c := range b {
		if c == '\n' || c == '\r' || c == '\t' {
			continue
		}
		if c < 0x20 || c == 0x7f {
			return false
		}
	}
	return true
}

// -----------------------------------------------------------------------------
// send (exactly one message)
// -----------------------------------------------------------------------------

func cmdSend(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	var cf commonFlags
	cf.register(fs)
	payload := fs.String("payload", "", "message payload (literal string)")
	fromFile := fs.String("file", "", "read payload from file ('-' means stdin)")
	fromHex := fs.String("hex", "", "message payload as hex string")
	key := fs.String("key", "", "message key")
	timeout := fs.Duration("timeout", 10*time.Second, "send timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := cf.validate(); err != nil {
		return err
	}

	data, err := readSendPayload(*payload, *fromFile, *fromHex)
	if err != nil {
		return err
	}

	client, err := cf.newClient()
	if err != nil {
		return err
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: cf.topic})
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer producer.Close()

	sctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	id, err := producer.Send(sctx, &pulsar.ProducerMessage{
		Payload:   data,
		Key:       *key,
		EventTime: time.Now(),
	})
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}
	fmt.Println("sent msgid:", pulsarutil.MsgIDToString(id), "size:", len(data))
	return nil
}

func readSendPayload(literal, file, hexStr string) ([]byte, error) {
	n := 0
	for _, s := range []string{literal, file, hexStr} {
		if s != "" {
			n++
		}
	}
	if n != 1 {
		return nil, errors.New("exactly one of -payload / -file / -hex is required")
	}
	switch {
	case literal != "":
		return []byte(literal), nil
	case hexStr != "":
		data, err := hex.DecodeString(hexStr)
		if err != nil {
			return nil, fmt.Errorf("decode -hex: %w", err)
		}
		return data, nil
	case file == "-":
		return io.ReadAll(os.Stdin)
	default:
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read -file: %w", err)
		}
		return data, nil
	}
}
