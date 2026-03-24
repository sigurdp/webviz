package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func main() {
	connStr := mustEnv("SERVICEBUS_CONNECTION_STRING")
	queueName := "test-queue"

	// Tuning knobs
	const (
		// maxConcurrent = 8
		// batchSize     = 20
		maxConcurrent = 1
		batchSize     = 1
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := azservicebus.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		log.Fatalf("NewClient: %v", err)
	}
	defer client.Close(ctx)

	receiver, err := client.NewReceiverForQueue(queueName, &azservicebus.ReceiverOptions{
		// PeekLock is the default behavior (recommended): process, then Complete().
		// ReceiveMode: azservicebus.ReceiveModePeekLock,
	})
	if err != nil {
		log.Fatalf("NewReceiverForQueue: %v", err)
	}
	defer receiver.Close(ctx)

	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	log.Printf("Worker started (queue=%s, maxConcurrent=%d)", queueName, maxConcurrent)

	for ctx.Err() == nil {
		// Short receive timeout so we can react to shutdown quickly
		receiveCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		msgs, err := receiver.ReceiveMessages(receiveCtx, batchSize, nil)
		cancel()

		if err != nil {
			// Context cancellation/timeout is normal.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			log.Printf("ReceiveMessages error: %v", err)
			time.Sleep(2 * time.Second) // basic backoff
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		for _, msg := range msgs {
			sem <- struct{}{}
			wg.Add(1)

			go func(m *azservicebus.ReceivedMessage) {
				defer wg.Done()
				defer func() { <-sem }()

				// If processing can take longer than the lock duration,
				// consider renewing the lock while you work.
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				go renewLockUntilDone(renewCtx, receiver, m, 20*time.Second)

				if err := handleMessage(ctx, m); err != nil {
					log.Printf("handleMessage failed (msgID=%s): %v", m.MessageID, err)

					// Strategy:
					// - Abandon for transient failures so it can be retried
					// - DeadLetter for poison messages (after your own retry count / inspection)
					abandonErr := receiver.AbandonMessage(ctx, m, nil)
					if abandonErr != nil {
						log.Printf("AbandonMessage failed (msgID=%s): %v", m.MessageID, abandonErr)
					}
					return
				}

				if err := receiver.CompleteMessage(ctx, m, nil); err != nil {
					log.Printf("CompleteMessage failed (msgID=%s): %v", m.MessageID, err)
					return
				}

				log.Printf("Completed msgID=%s", m.MessageID)
			}(msg)
		}
	}

	log.Printf("Shutting down... waiting for in-flight work")
	wg.Wait()
	log.Printf("Stopped")
}

func handleMessage(ctx context.Context, msg *azservicebus.ReceivedMessage) error {
	// Your real work here (deserialize, call downstream services, etc.)
	// msg.Body is [][]byte (AMQP sections). Often you want the first section:
	if len(msg.Body) == 0 {
		return fmt.Errorf("empty body")
	}
	payload := msg.Body

	// Convert body to string for logging/demo purposes (be mindful of large payloads in production!)
	payloadStr := string(payload)

	// Example “work”
	log.Printf("Processing msgID=%s len=%d payloadStr=%s", msg.MessageID, len(payload), payloadStr)
	time.Sleep(2 * time.Second)

	return nil
}

func renewLockUntilDone(ctx context.Context, r *azservicebus.Receiver, msg *azservicebus.ReceivedMessage, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// If the message is already completed/abandoned/dead-lettered, renew will fail — safe to ignore.
			if err := r.RenewMessageLock(ctx, msg, nil); err != nil {
				// Logging at debug level is typical; keep it quiet to avoid noise.
				return
			}
		}
	}
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing env var %s", k)
	}
	return v
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
