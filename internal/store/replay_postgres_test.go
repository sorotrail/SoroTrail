package store

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestEventIDs(t *testing.T) {
	events := []EventDecoding{
		{ID: "evt-1", Topics: json.RawMessage(`["a"]`), Value: json.RawMessage(`{"v":1}`)},
		{ID: "evt-2", Topics: json.RawMessage(`["b"]`), Value: json.RawMessage(`{"v":2}`)},
	}
	if got, want := eventIDs(events), []string{"evt-1", "evt-2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("eventIDs() = %#v, want %#v", got, want)
	}
	if got := len(eventIDs(nil)); got != 0 {
		t.Fatalf("eventIDs(nil) len = %d, want 0", got)
	}
}

func TestEventTopics(t *testing.T) {
	events := []EventDecoding{
		{ID: "a", Topics: json.RawMessage(`["x"]`)},
		{ID: "b", Topics: json.RawMessage(`["y","z"]`)},
	}
	got := eventTopics(events)
	if len(got) != 2 || string(got[0]) != `["x"]` || string(got[1]) != `["y","z"]` {
		t.Fatalf("eventTopics() = %q, want [[\"x\"] [\"y\",\"z\"]]", got)
	}
	if got := len(eventTopics(nil)); got != 0 {
		t.Fatalf("eventTopics(nil) len = %d, want 0", got)
	}
}

func TestEventValues(t *testing.T) {
	events := []EventDecoding{
		{ID: "a", Value: json.RawMessage(`{"v":1}`)},
		{ID: "b", Value: json.RawMessage(`{"v":2}`)},
	}
	got := eventValues(events)
	if len(got) != 2 || string(got[0]) != `{"v":1}` || string(got[1]) != `{"v":2}` {
		t.Fatalf("eventValues() = %q, want [{\"v\":1} {\"v\":2}]", got)
	}
	if got := len(eventValues(nil)); got != 0 {
		t.Fatalf("eventValues(nil) len = %d, want 0", got)
	}
}
