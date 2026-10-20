package bridge

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.mau.fi/whatsmeow/types"
)

var testHistorySyncLogger = logrus.NewEntry(logrus.StandardLogger())

func historySyncTestPlan(anchors, exact int) *recentHistorySyncPlan {
	chat := types.NewJID("15550000000", types.DefaultUserServer)
	plan := &recentHistorySyncPlan{}
	for i := 0; i < anchors; i++ {
		plan.HistoryAnchors = append(plan.HistoryAnchors, &types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat},
			ID:            types.MessageID(fmt.Sprintf("anchor-%d", i)),
		})
	}
	for i := 0; i < exact; i++ {
		plan.ExactMessages = append(plan.ExactMessages, &types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, IsFromMe: true},
			ID:            types.MessageID(fmt.Sprintf("exact-%d", i)),
		})
	}
	return plan
}

func TestSendRecentHistorySyncPlanGivesEachRequestItsOwnTimeout(t *testing.T) {
	plan := historySyncTestPlan(2, 2)
	var deadlines []time.Time
	send := func(ctx context.Context, _ *types.MessageInfo) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("request context has no deadline")
		}
		deadlines = append(deadlines, deadline)
		time.Sleep(20 * time.Millisecond)
		return nil
	}

	result := sendRecentHistorySyncPlan(context.Background(), testHistorySyncLogger, plan, 50*time.Millisecond, send, send)

	// 4 requests × 20ms = 80ms total, longer than the 50ms per-request timeout:
	// a single shared budget would have expired before the last requests.
	if result.historyOK != 2 || result.exactOK != 2 || result.aborted {
		t.Fatalf("result = %+v, want all 4 requests ok and not aborted", result)
	}
	if !deadlines[3].After(deadlines[0]) {
		t.Fatalf("deadlines not per request: first=%v last=%v", deadlines[0], deadlines[3])
	}
}

func TestSendRecentHistorySyncPlanStopsBatchAfterTimeout(t *testing.T) {
	plan := historySyncTestPlan(3, 4)
	calls := 0
	history := func(ctx context.Context, _ *types.MessageInfo) error {
		calls++
		if calls == 2 {
			<-ctx.Done()
			// whatsmeow formats the cause with %v, so errors.Is cannot see it.
			return fmt.Errorf("failed to encrypt peer message: %v", ctx.Err())
		}
		return nil
	}
	exact := func(context.Context, *types.MessageInfo) error {
		t.Fatal("exact requests must not run after the batch timed out")
		return nil
	}

	result := sendRecentHistorySyncPlan(context.Background(), testHistorySyncLogger, plan, 10*time.Millisecond, history, exact)

	if !result.aborted {
		t.Fatal("expected batch to be aborted after a request timed out")
	}
	if result.historyOK != 1 || result.historyFailed != 1 {
		t.Fatalf("history ok/failed = %d/%d, want 1/1", result.historyOK, result.historyFailed)
	}
	if result.skipped != 5 {
		t.Fatalf("skipped = %d, want 5 (1 anchor + 4 exact)", result.skipped)
	}
	if result.abortErr == nil {
		t.Fatal("expected the timeout error to be recorded")
	}
}

func TestSendRecentHistorySyncPlanContinuesAfterNonTimeoutError(t *testing.T) {
	plan := historySyncTestPlan(2, 2)
	history := func(context.Context, *types.MessageInfo) error { return errors.New("server rejected") }
	exact := func(context.Context, *types.MessageInfo) error { return nil }

	result := sendRecentHistorySyncPlan(context.Background(), testHistorySyncLogger, plan, time.Second, history, exact)

	if result.aborted {
		t.Fatal("a non-timeout error must not abort the batch")
	}
	if result.historyFailed != 2 || result.exactOK != 2 || result.skipped != 0 {
		t.Fatalf("result = %+v, want history failed 2, exact ok 2, skipped 0", result)
	}
}

func TestSendRecentHistorySyncPlanStopsWhenParentCancelled(t *testing.T) {
	plan := historySyncTestPlan(2, 1)
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	send := func(context.Context, *types.MessageInfo) error {
		t.Fatal("no request should run once the parent is cancelled")
		return nil
	}

	result := sendRecentHistorySyncPlan(parent, testHistorySyncLogger, plan, time.Second, send, send)

	if !result.aborted || result.skipped != 3 {
		t.Fatalf("result = %+v, want aborted with 3 skipped", result)
	}
}
