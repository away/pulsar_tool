# pulsar_tool

A minimal CLI for poking Apache Pulsar topics. Payloads are treated as opaque
bytes; the tool does not know about any application-level schema.

## Subcommands

| Command  | Purpose                                                              |
|----------|----------------------------------------------------------------------|
| `status` | Show the current cursor (last message) on every partition of a topic |
| `seek`   | Move a subscription cursor to earliest / latest / a time / a msg id  |
| `recv`   | Receive **exactly one** message and print it                         |
| `send`   | Send **exactly one** message                                         |

## Install

```bash
go install github.com/away/pulsar_tool@latest
```

Or clone and build:

```bash
git clone https://github.com/away/pulsar_tool.git
cd pulsar_tool
go build -o pt .
```

## Usage

### Common flags (every subcommand)

| Flag         | Default                    | Description                                 |
|--------------|----------------------------|---------------------------------------------|
| `-url`       | `pulsar://localhost:6650`  | Pulsar service url                          |
| `-topic`     | (required)                 | Topic name                                  |
| `-sub`       | `pulsar-tool`              | Subscription name                           |
| `-consumer`  |                            | Consumer name                               |
| `-sub-type`  | `0`                        | 0=exclusive 1=shared 2=failover 3=key\_shared |

### Examples

```bash
# Show the last message on every partition of a topic
pt status -topic my-topic

# Seek a subscription to the earliest / latest message
pt seek -topic my-topic -to earliest
pt seek -topic my-topic -to latest

# Seek to an explicit message id
pt seek -topic my-topic -to 12345:6:0:0

# Seek by wall-clock time (local timezone)
pt seek -topic my-topic -time "2024-06-01 12:00:00"

# Receive one message, ack it
pt recv -topic my-topic -ack

# Receive one message starting from a specific msg id, without acking
pt recv -topic my-topic -from 12345:6:0:0

# Send one message from stdin
echo hello | pt send -topic my-topic -file -

# Send one message from a literal / a hex string / a file
pt send -topic my-topic -payload "hello world" -key mykey
pt send -topic my-topic -hex 68656c6c6f
pt send -topic my-topic -file payload.bin
```

## Development

```bash
go build ./...
go vet   ./...
gofmt -l .
```

## License

MIT.
