package spec

import (
	"testing"
)

func TestParseOverrideSpec_Valid(t *testing.T) {
	data := []byte(`{
		"events": [
			{
				"name": "transfer",
				"doc": "Token transfer event",
				"topic_specs": [
					{"name": "to", "type": "address"},
					{"name": "from", "type": "address"}
				],
				"value_spec": {"name": "amount", "type": "i128"}
			},
			{
				"name": "approval"
			}
		]
	}`)

	spec, err := ParseOverrideSpec(data)
	if err != nil {
		t.Fatalf("expected valid spec, got error: %v", err)
	}
	if len(spec.Events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(spec.Events))
	}
	if spec.Events[0].Name != "transfer" {
		t.Errorf("expected event name %q, got %q", "transfer", spec.Events[0].Name)
	}
	if len(spec.Events[0].TopicSpecs) != 2 {
		t.Fatalf("expected 2 topic specs, got %d", len(spec.Events[0].TopicSpecs))
	}
	if spec.Events[0].TopicSpecs[0].Name != "to" || spec.Events[0].TopicSpecs[0].Type != "address" {
		t.Errorf("unexpected topic spec: %+v", spec.Events[0].TopicSpecs[0])
	}
	if spec.Events[0].ValueSpec == nil || spec.Events[0].ValueSpec.Name != "amount" {
		t.Errorf("expected value_spec named %q, got %+v", "amount", spec.Events[0].ValueSpec)
	}
	if spec.Events[1].Name != "approval" {
		t.Errorf("expected event name %q, got %q", "approval", spec.Events[1].Name)
	}
	if spec.Events[1].ValueSpec != nil {
		t.Errorf("expected nil value_spec for bare event, got %+v", spec.Events[1].ValueSpec)
	}
}

func TestParseOverrideSpec_EmptyEventsAllowed(t *testing.T) {
	spec, err := ParseOverrideSpec([]byte(`{"events": []}`))
	if err != nil {
		t.Fatalf("expected empty events array to be valid, got error: %v", err)
	}
	if len(spec.Events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(spec.Events))
	}
}

func TestParseOverrideSpec_RejectsInvalid(t *testing.T) {
	cases := []struct {
		name string
		data string
	}{
		{"not json", `this is not json`},
		{"json array instead of object", `[1,2,3]`},
		{"missing events", `{}`},
		{"events not an array", `{"events": "nope"}`},
		{"event missing name", `{"events": [{"doc": "no name"}]}`},
		{"event empty name", `{"events": [{"name": ""}]}`},
		{"event name not a string", `{"events": [{"name": 42}]}`},
		{"duplicate event names", `{"events": [{"name": "a"}, {"name": "a"}]}`},
		{"topic spec missing type", `{"events": [{"name": "a", "topic_specs": [{"name": "x"}]}]}`},
		{"topic spec missing name", `{"events": [{"name": "a", "topic_specs": [{"type": "u64"}]}]}`},
		{"value spec missing type", `{"events": [{"name": "a", "value_spec": {"name": "v"}}]}`},
		{"value spec empty name", `{"events": [{"name": "a", "value_spec": {"name": "", "type": "u64"}}]}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := ParseOverrideSpec([]byte(tc.data))
			if err == nil {
				t.Fatalf("expected error for %s, got valid spec: %+v", tc.name, spec)
			}
		})
	}
}

func TestCachePutAndGetOverrideKey(t *testing.T) {
	cache := NewCache(nil, WithTTL(0)) // TTL 0 disables expiry anyway
	key := "override:CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

	if s := cache.Get(key); s != nil {
		t.Fatalf("expected cache miss, got %+v", s)
	}

	want := &ContractSpec{
		ContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		Events:     []EventSpec{{Name: "transfer"}},
	}
	cache.Put(key, want)

	got := cache.Get(key)
	if got == nil {
		t.Fatal("expected cache hit after Put")
	}
	if got.ContractID != want.ContractID || len(got.Events) != 1 {
		t.Fatalf("unexpected spec from cache: %+v", got)
	}

	// The override key must never collide with a real wasm hash namespace
	// entry: Put under the override key must not be visible via a different key.
	if s := cache.Get("bogus-hash"); s != nil {
		t.Fatalf("expected miss for unrelated key, got %+v", s)
	}
}
