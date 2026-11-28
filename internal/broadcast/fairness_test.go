package broadcast

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/webhook"
)

func TestDeliveryFairnessAndIsolation(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	broker := NewWebhookBroker(logger)
	dispatcher := webhook.NewDispatcher(2, logger)

	subSlow := &WebhookSubscription{ID: 1, TenantID: 1, Endpoint: "http://slow.test", Active: true}
	subFast := &WebhookSubscription{ID: 2, TenantID: 2, Endpoint: "http://fast.test", Active: true}

	broker.RegisterSubscription(subSlow)
	broker.RegisterSubscription(subFast)

	ctx := context.Background()
	err := broker.Broadcast(ctx, Event{ID: "evt-1", ContractID: "C1", Ledger: 100, Data: []byte("{}")})
	require.NoError(t, err)

	errFast := dispatcher.Deliver(ctx, subFast.TenantID, subFast.Endpoint, []byte("{}"))
	assert.NoError(t, errFast)

	assert.Equal(t, int64(1), broker.TenantBacklog(1))
	assert.Equal(t, int64(1), broker.TenantBacklog(2))
}
