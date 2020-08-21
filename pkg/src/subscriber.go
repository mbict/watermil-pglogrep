package src

import (
	"context"
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/mbict/watermill-pglogrep/pkg/src/pgoutput"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"

	"github.com/pkg/errors"
	"sync"
)

type Payload struct {
	Id        string
	Schema    map[string]string
	Table     string
	Operation string
	Before    map[string]interface{}
	After     map[string]interface{}
}

type Subscriber struct {
	conn          *pgconn.PgConn
	clientXLogPos pglogrepl.LSN

	subscribeWg *sync.WaitGroup
	closing     chan struct{}
	closed      bool

	logger watermill.LoggerAdapter
	sync   bool
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	parts := strings.SplitN(topic, ".", 2)
	if len(parts) != 2 {
		return nil, errors.New("need topic in the format of `slotName.pulicationName`")
	}
	slotName := parts[0]
	publicationName := parts[1]

	logger := s.logger.With(watermill.LogFields{
		"topic":           topic,
		"slotName":        slotName,
		"publicationName": publicationName,
	})

	var err error
	s.clientXLogPos, err = getConfirmedFlushLsnForSlot(ctx, s.conn, slotName)
	if err != nil {
		logger.Error("cannot determine confirmed LSN for slot, will resume from 0", err, nil)
	} else {
		logger.Info("received confirmed LSN for slot", watermill.LogFields{"confirmed_lsn": s.clientXLogPos})
	}

	pluginArguments := []string{"publication_names '" + publicationName + "'", "proto_version '1'"}
	err = pglogrepl.StartReplication(ctx,
		s.conn,
		slotName,
		s.clientXLogPos,
		pglogrepl.StartReplicationOptions{
			Mode:       pglogrepl.LogicalReplication,
			PluginArgs: pluginArguments,
		})

	if err != nil {
		//replication start failed
		logger.Error("start replication failed", err, nil)
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	out := make(chan *message.Message)

	s.subscribeWg.Add(1)
	go func() {
		s.consume(ctx, logger, out)
		close(out)
		cancel()
	}()

	return out, nil
}

func (s *Subscriber) consume(ctx context.Context, logger watermill.LoggerAdapter, out chan *message.Message) {
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	set := pgoutput.NewRelationSet(nil)

	sendStandbyStatusUpdate := func() bool {
		standbyStatusUpdate := pglogrepl.StandbyStatusUpdate{
			WALWritePosition: s.clientXLogPos,
			WALFlushPosition: s.clientXLogPos,
			WALApplyPosition: s.clientXLogPos,
			ReplyRequested:   false,
		}
		logger.Debug("sending standby status update", watermill.LogFields{"WALWritePosition": standbyStatusUpdate.WALWritePosition})
		err := pglogrepl.SendStandbyStatusUpdate(context.Background(), s.conn, standbyStatusUpdate)
		if err != nil {
			logger.Error("send standby status update failed", err, watermill.LogFields{"WALWritePosition": standbyStatusUpdate.WALWritePosition})
			return false
		}

		nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		logger.Debug("standby status update send", watermill.LogFields{"WALWritePosition": standbyStatusUpdate.WALWritePosition, "nexStandbyStatusDeadline": nextStandbyMessageDeadline})
		return true
	}

	deliverMessage := func(payload *Payload, lsn pglogrepl.LSN) bool {

		logMessageData := watermill.LogFields{"id": payload.Id}
		data, _ := json.Marshal(payload)
		msg := message.NewMessage(payload.Id, data)

		//send message
		out <- msg
		for {
			select {

			case <-msg.Acked():
				logger.Trace("message acknowledged", logMessageData)

				//message acked, we advance the client position to this message LSN
				s.clientXLogPos = lsn

				//send message
				if sendStandbyStatusUpdate() == false {
					return false
				}
				return true

			case <-msg.Nacked():
				//we redeliver same message on nack
				logger.Trace("message nacked", logMessageData)
				out <- msg.Copy()

			case <-ctx.Done():
				logger.Trace("request stopped without ACK received", logMessageData)
				return false

			case <-s.closing:
				logger.Debug("subscription is closing", logMessageData)
				return false

			case <-time.After(standbyMessageTimeout):
				logger.Debug("send standby message, during message delivery", logMessageData)
				if sendStandbyStatusUpdate() == false {
					//fatal stop
					logger.Debug("failed to send standby status", logMessageData)
					return false
				}
			}
		}
	}

	generateTableName := func(relationId uint32) string {
		r, ok := set.Get(relationId)
		if !ok {
			logger.Debug("error generating table name: relation not found", nil)
			return ""
		}
		if len(r.Namespace) == 0 {
			return r.Name
		}
		return r.Namespace + "." + r.Name
	}

	generateDataMap := func(relation uint32, row []pgoutput.Tuple) map[string]interface{} {
		values, err := set.Values(relation, row)
		if err != nil {
			logger.Error("error parsing values", err, nil)
			return nil
		}

		res := make(map[string]interface{})
		for name, value := range values {
			res[name] = value.Get()
		}
		return res
	}

	generateSchemaMap := func(relation uint32, row []pgoutput.Tuple) map[string]string {
		values, err := set.Values(relation, row)
		if err != nil {
			logger.Error("error parsing values", err, nil)
			return nil
		}

		res := make(map[string]string)
		for name, value := range values {
			res[name] = reflect.TypeOf(value.Get()).String()
		}
		return res
	}

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			logger.Debug("sending status update, timeout in main loop receive message", nil)
			if sendStandbyStatusUpdate() == false {
				return
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		//try get message
		logger.Debug("poll for message", nil)
		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err := s.conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				logger.Debug("connection context timeout", nil)
				continue
			}
			logger.Error("polling for message failed with error", err, nil)
			return
		}

		var payload *Payload
		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {

			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					logger.Error("parse primary keepalive message failed with error", err, nil)
				}
				logger.Trace("Primary Keepalive Message", watermill.LogFields{
					"ServerWALEnd":   pkm.ServerWALEnd,
					"ServerTime":     pkm.ServerTime,
					"ReplyRequested": pkm.ReplyRequested,
				})

				if pkm.ReplyRequested {
					sendStandbyStatusUpdate()
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					logger.Error("ParseXLogData failed", err, nil)
					return
				}

				m, err := pgoutput.Parse(xld.WALData)
				if err != nil {
					logger.Error("ParseXLogData failed:", err, nil)
					return
				}

				var nextLsn = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

				//get to the same point of the last message
				switch wm := m.(type) {
				case pgoutput.Relation:
					logger.Trace("Received relation data", nil)
					set.Add(wm)
				}

				//get to the point of the same message
				if s.sync == false {
					s.sync = nextLsn >= s.clientXLogPos
					continue
				}

				switch wm := m.(type) {

				case pgoutput.Update:
					logger.Trace("received update", watermill.LogFields{"lsn": nextLsn})
					payload = &Payload{
						Id:        nextLsn.String(),
						Schema:    generateSchemaMap(wm.RelationID, wm.Row),
						Table:     generateTableName(wm.RelationID),
						Operation: "UPDATE",
						Before:    generateDataMap(wm.RelationID, wm.OldRow),
						After:     generateDataMap(wm.RelationID, wm.Row),
					}

				case pgoutput.Insert:
					logger.Trace("received insert", watermill.LogFields{"lsn": nextLsn})
					payload = &Payload{
						Id:        nextLsn.String(),
						Schema:    generateSchemaMap(wm.RelationID, wm.Row),
						Table:     generateTableName(wm.RelationID),
						Operation: "INSERT",
						Before:    nil,
						After:     generateDataMap(wm.RelationID, wm.Row),
					}

				case pgoutput.Delete:
					logger.Trace("received delete", watermill.LogFields{"lsn": nextLsn})
					payload = &Payload{
						Id:        nextLsn.String(),
						Schema:    generateSchemaMap(wm.RelationID, wm.Row),
						Table:     generateTableName(wm.RelationID),
						Operation: "DELETE",
						Before:    generateDataMap(wm.RelationID, wm.Row),
						After:     nil,
					}

				case pgoutput.Commit:
					logger.Trace("received commit", watermill.LogFields{"lsn": nextLsn})
					s.clientXLogPos = nextLsn
					sendStandbyStatusUpdate()
				}

				if payload != nil {
					if deliverMessage(payload, nextLsn) == false {
						return
					}
				}
			}
		default:
			logger.Debug("received unexpected message", watermill.LogFields{"message": msg})
		}

	}
}

func (s *Subscriber) Close() error {
	s.logger.Debug("closing subscriber", nil)
	if s.closed {
		return nil
	}

	s.closed = true

	close(s.closing)
	s.subscribeWg.Wait()

	return nil
}

func NewSubscriber(conn *pgconn.PgConn, logger watermill.LoggerAdapter) (*Subscriber, error) {

	return &Subscriber{
		conn:          conn,
		clientXLogPos: 0,
		sync:          false,

		subscribeWg: &sync.WaitGroup{},
		closing:     make(chan struct{}),
		closed:      false,

		logger: logger,
	}, nil
}

func getConfirmedFlushLsnForSlot(ctx context.Context, conn *pgconn.PgConn, slot string) (pglogrepl.LSN, error) {
	return parseConfirmedFlushLsn(conn.Exec(ctx, `SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '`+slot+`'`))
}

func parseConfirmedFlushLsn(mrr *pgconn.MultiResultReader) (pglogrepl.LSN, error) {
	results, err := mrr.ReadAll()
	if err != nil {
		return 0, err
	}

	if len(results) != 1 {
		return 0, errors.Errorf("expected 1 result set, got %d", len(results))
	}

	result := results[0]
	if len(result.Rows) != 1 {
		return 0, errors.Errorf("expected 1 result row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if len(row) != 1 {
		return 0, errors.Errorf("expected 1 result columns, got %d", len(row))
	}

	var lsn pglogrepl.LSN
	lsn, err = pglogrepl.ParseLSN(string(row[0]))
	if err != nil {
		return 0, errors.Errorf("failed to parse confirmed_flush_lsn as LSN: %w", err)
	}

	return lsn, nil
}
