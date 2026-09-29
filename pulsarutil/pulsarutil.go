// Package pulsarutil holds small helpers around apache/pulsar-client-go used
// by pulsar_tool: message-id parsing / formatting and subscription-seek
// helpers.
package pulsarutil

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"
)

// MsgIDToString formats a message id as "ledger:entry:partition:batch".
func MsgIDToString(msgID pulsar.MessageID) string {
	return fmt.Sprintf("%d:%d:%d:%d",
		msgID.LedgerID(), msgID.EntryID(), msgID.PartitionIdx(), msgID.BatchIdx())
}

// CloneMsgIDSetPartition returns a copy of msgID with its partition index
// replaced by ptid.
func CloneMsgIDSetPartition(msgID pulsar.MessageID, ptid int) pulsar.MessageID {
	return pulsar.NewMessageID(msgID.LedgerID(), msgID.EntryID(), msgID.BatchIdx(), int32(ptid))
}

// ResolveMsgID accepts any of:
//
//	"ledger:entry:partition:batch"
//	"earliest"
//	"latest"
//
// It returns nil when msgidstr is empty or cannot be parsed as any of the
// above.
func ResolveMsgID(msgidstr string) pulsar.MessageID {
	if msgidstr == "" {
		return nil
	}
	var msgID pulsar.MessageID
	switch {
	case msgidstr == "earliest":
		msgID = pulsar.EarliestMessageID()
	case msgidstr == "latest":
		msgID = pulsar.LatestMessageID()
	case strings.Contains(msgidstr, ":"):
		msgID = msgIDFromString(msgidstr)
	}
	if msgID != nil {
		log.Infoln("ResolveMsgID got:", MsgIDToString(msgID))
	}
	return msgID
}

func msgIDFromString(msgids string) pulsar.MessageID {
	parts := strings.Split(msgids, ":")
	var (
		ledgerID  int64
		entryID   int64
		partition int32
		batchIdx  int32
	)
	if len(parts) > 0 {
		v, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			log.Errorln("msgIDFromString fail:", err, msgids)
			return nil
		}
		ledgerID = v
	}
	if len(parts) > 1 {
		v, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			log.Errorln("msgIDFromString fail:", err, msgids)
			return nil
		}
		entryID = v
	}
	if len(parts) > 2 {
		v, err := strconv.ParseInt(parts[2], 10, 32)
		if err != nil {
			log.Errorln("msgIDFromString fail:", err, msgids)
			return nil
		}
		partition = int32(v)
	}
	if len(parts) > 3 {
		v, err := strconv.ParseInt(parts[3], 10, 32)
		if err != nil {
			log.Errorln("msgIDFromString fail:", err, msgids)
			return nil
		}
		batchIdx = int32(v)
	}
	return pulsar.NewMessageID(ledgerID, entryID, batchIdx, partition)
}

// PulsarClient is a thin wrapper around pulsar.Client that remembers the
// consumer name / subscription type between CreateConsumer calls.
type PulsarClient struct {
	csmName string
	subType pulsar.SubscriptionType
	pulsar.Client
}

// NewPulsarClient constructs a PulsarClient connected to url.
func NewPulsarClient(url string) (*PulsarClient, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: url})
	if err != nil {
		log.Errorln("PulsarClient NewClient fail:", err)
		return nil, err
	}
	return &PulsarClient{Client: client}, nil
}

// SetConsumerName sets the consumer name used for future CreateConsumer calls.
func (pcc *PulsarClient) SetConsumerName(name string) { pcc.csmName = name }

// SetSubscriberType sets the subscription type used for future CreateConsumer
// calls (see pulsar.SubscriptionType).
func (pcc *PulsarClient) SetSubscriberType(subType int) {
	pcc.subType = pulsar.SubscriptionType(subType)
}

// CreateConsumer subscribes to topic with subName using the client's
// configured consumer name and subscription type.
func (pcc *PulsarClient) CreateConsumer(topic, subName string) (pulsar.Consumer, error) {
	consumer, err := pcc.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subName,
		Name:             pcc.csmName,
		Type:             pcc.subType,
	})
	if err != nil {
		log.Errorln("PulsarClient Subscribe fail:", err)
		return nil, err
	}
	return consumer, nil
}

// SeekLatest seeks subName on every partition of topic to LatestMessageID.
func (pcc *PulsarClient) SeekLatest(topic, subName string) error {
	return pcc.seek(topic, subName, pulsar.LatestMessageID())
}

// SeekEarliest seeks subName on every partition of topic to EarliestMessageID.
func (pcc *PulsarClient) SeekEarliest(topic, subName string) error {
	return pcc.seek(topic, subName, pulsar.EarliestMessageID())
}

func (pcc *PulsarClient) seek(topic, subName string, msgID pulsar.MessageID) error {
	topicPts, err := pcc.TopicPartitions(topic)
	if err != nil {
		log.Errorln("TopicPartitions fail:", err, topic)
		return err
	}
	seekMsgID := pulsar.NewMessageID(msgID.LedgerID(), msgID.EntryID(), 0, 0)
	for _, ptopic := range topicPts {
		consumer, err := pcc.CreateConsumer(ptopic, subName)
		if err != nil {
			return err
		}
		if err := consumer.Seek(seekMsgID); err != nil {
			log.Errorln("seek fail:", err)
			consumer.Close()
			return err
		}
		consumer.Close()
	}
	return nil
}

// SeekByTime seeks subName on every partition of topic to the given timestamp.
func (pcc *PulsarClient) SeekByTime(topic, subName string, stime time.Time) error {
	topicPts, err := pcc.TopicPartitions(topic)
	if err != nil {
		log.Errorln("SeekByTime TopicPartitions fail:", err, topic)
		return err
	}
	for _, ptopic := range topicPts {
		consumer, err := pcc.CreateConsumer(ptopic, subName)
		if err != nil {
			return err
		}
		if err := consumer.SeekByTime(stime); err != nil {
			log.Errorln("seek fail:", err)
			consumer.Close()
			return err
		}
		consumer.Close()
	}
	return nil
}

// PrepareSeekInfo translates target into the concrete partitioned topic and
// message id that a consumer can Seek to. It returns the partitioned topic
// name and a msg id whose partition index has been normalised to 0 (which is
// what pulsar.Consumer.Seek expects for a single-partition consumer).
func (pcc *PulsarClient) PrepareSeekInfo(topic string, target pulsar.MessageID) (string, pulsar.MessageID, error) {
	topicPts, err := pcc.TopicPartitions(topic)
	if err != nil {
		log.Errorln("get TopicPartitions fail:", err, topic)
		return topic, nil, err
	}
	ptid := int(target.PartitionIdx())
	if ptid == -1 { // earliest / latest
		ptid = 0
	}
	batchIdx := target.BatchIdx()
	if batchIdx > -1 {
		batchIdx--
	}
	msgID := pulsar.NewMessageID(target.LedgerID(), target.EntryID(), batchIdx, 0)
	if ptid >= len(topicPts) {
		return topic, msgID, fmt.Errorf("partition id %d out of range [0, %d)", ptid, len(topicPts))
	}
	return topicPts[ptid], msgID, nil
}
