package otredis

import (
	"context"
	"os"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
)

var client *redis.Client
var tracer *mocktracer.MockTracer

func init() {
	tracer = mocktracer.New()
	opentracing.SetGlobalTracer(tracer)
}

func TestMain(m *testing.M) {
	// in-memory redis
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()
	client = redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	os.Exit(m.Run())
}

func TestSet(t *testing.T) {
	ctx := context.Background()

	span, ctx := opentracing.StartSpanFromContext(ctx, "test-params")
	ctxClient := WrapRedisClient(ctx, client)
	callSet(t, ctxClient, "with span")
	span.Finish()

	spans := tracer.FinishedSpans()
	if len(spans) != 2 {
		t.Fatalf("should be 2 finished spans but there are %d: %v", len(spans), spans)
	}

	redisSpan := spans[0]
	if redisSpan.OperationName != "redis" {
		t.Errorf("first span operation should be redis but it's '%s'", redisSpan.OperationName)
	}

	testTags(t, redisSpan, map[string]string{
		"db.type":   "redis",
		"db.method": "set",
	})

	tracer.Reset()
}

func TestGet(t *testing.T) {
	ctx := context.Background()

	span, ctx := opentracing.StartSpanFromContext(ctx, "test-params")
	ctxClient := WrapRedisClient(ctx, client)
	callGet(t, ctxClient)
	span.Finish()

	spans := tracer.FinishedSpans()
	if len(spans) != 2 {
		t.Fatalf("should be 2 finished spans but there are %d: %v", len(spans), spans)
	}

	redisSpan := spans[0]
	if redisSpan.OperationName != "redis" {
		t.Errorf("first span operation should be redis but it's '%s'", redisSpan.OperationName)
	}

	testTags(t, redisSpan, map[string]string{
		"db.type":   "redis",
		"db.method": "get",
	})

	tracer.Reset()
}

func callSet(t *testing.T, c *redis.Client, value string) {
	_, err := c.Set("foo", value, 0).Result()
	if err != nil {
		t.Fatalf("Redis returned error: %v", err)
	}
}

func callGet(t *testing.T, c *redis.Client) {
	_, err := c.Get("foo").Result()
	if err != nil {
		t.Fatalf("Redis returned error: %v", err)
	}
}

func testTags(t *testing.T, redisSpan *mocktracer.MockSpan, expectedTags map[string]string) {
	redisTags := redisSpan.Tags()
	if len(redisTags) != len(expectedTags) {
		t.Errorf("redis span should have %d tags but it has %d", len(expectedTags), len(redisTags))
	}

	for name, expected := range expectedTags {
		value, ok := redisTags[name]
		if !ok {
			t.Errorf("redis span doesn't have tag '%s'", name)
			continue
		}
		if value != expected {
			t.Errorf("redis span tag '%s' should have value '%s' but it has '%s'", name, expected, value)
		}
	}
}
