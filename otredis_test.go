package otredis

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
)

var redisAddr string
var client *redis.Client
var tracer *mocktracer.MockTracer

func init() {
	tracer = mocktracer.New()
	opentracing.SetGlobalTracer(tracer)
}

func TestMain(m *testing.M) {
	// in-memory redis
	miniRedis, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer miniRedis.Close()

	redisAddr = miniRedis.Addr()

	client = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	os.Exit(m.Run())
}

// SET

func TestSet(t *testing.T) {
	ctx := context.Background()

	span, ctx := opentracing.StartSpanFromContext(ctx, "test-set")
	ctxClient := WrapRedisClient(ctx, client)
	callSet(t, ctxClient, "with span")
	span.Finish()

	spans := tracer.FinishedSpans()
	assert.Equal(t, 2, len(spans), "the number of finished spans is invalid")

	redisSpan := spans[0]
	assert.Equal(t, "redis-cmd", redisSpan.OperationName)

	expectedTags := buildExpectedTags("set")
	actualTags := assertTags(t, redisSpan, expectedTags)
	actualDbStatementValue := actualTags["db.statement"]
	assert.Equal(t, "set foo with span", actualDbStatementValue, "redis span tag 'db.statement' is invalid")

	tracer.Reset()
}

func TestSetPipeline(t *testing.T) {
	ctx := context.Background()

	span, ctx := opentracing.StartSpanFromContext(ctx, "test-set-pipeline")
	ctxClient := WrapRedisClient(ctx, client)
	setPipelineParams := make(map[string]string)
	setPipelineParams["foo"] = "with span on foo pipeline"
	setPipelineParams["bar"] = "with span on bar pipeline"
	callSetPipeline(t, ctxClient, setPipelineParams)
	span.Finish()

	spans := tracer.FinishedSpans()
	assert.Equal(t, 2, len(spans), "the number of finished spans is invalid")

	redisSpan := spans[0]
	assert.Equal(t, "redis-pipeline-cmd", redisSpan.OperationName)

	expectedTags := buildExpectedTags("set -> set")
	actualTags := assertTags(t, redisSpan, expectedTags)
	actualDbStatementValue := fmt.Sprint(actualTags["db.statement"])
	assert.True(t, strings.Contains(actualDbStatementValue, "set foo with span on foo pipeline"), "redis span tag 'db.statement' is invalid")
	assert.True(t, strings.Contains(actualDbStatementValue, "set bar with span on bar pipeline"), "redis span tag 'db.statement' is invalid")

	tracer.Reset()
}

func callSet(t *testing.T, client *redis.Client, value string) {
	_, err := client.Set("foo", value, 0).Result()
	assert.Nil(t, err, "Redis returned error: %v", err)
}

func callSetPipeline(t *testing.T, client *redis.Client, setPipelineParams map[string]string) {
	pipeline := client.Pipeline()
	for key, value := range setPipelineParams {
		pipeline.Set(key, value, 0)
	}
	_, err := pipeline.Exec()
	assert.Nil(t, err, "Redis returned error: %v", err)
}

// GET

func TestGet(t *testing.T) {
	ctx := context.Background()

	span, ctx := opentracing.StartSpanFromContext(ctx, "test-get")
	ctxClient := WrapRedisClient(ctx, client)
	callGet(t, ctxClient)
	span.Finish()

	spans := tracer.FinishedSpans()
	assert.Equal(t, 2, len(spans), "the number of finished spans is invalid")

	redisSpan := spans[0]
	assert.Equal(t, "redis-cmd", redisSpan.OperationName)

	expectedTags := buildExpectedTags("get")
	actualTags := assertTags(t, redisSpan, expectedTags)
	actualDbStatementValue := actualTags["db.statement"]
	assert.Equal(t, "get foo", actualDbStatementValue, "redis span tag 'db.statement' is invalid")

	tracer.Reset()
}

func TestGetPipeline(t *testing.T) {
	ctx := context.Background()

	span, ctx := opentracing.StartSpanFromContext(ctx, "test-get-pipeline")
	ctxClient := WrapRedisClient(ctx, client)
	getPipelineParams := []string{"foo", "bar"}
	callGetPipeline(t, ctxClient, getPipelineParams)
	span.Finish()

	spans := tracer.FinishedSpans()
	assert.Equal(t, 2, len(spans), "the number of finished spans is invalid")

	redisSpan := spans[0]
	assert.Equal(t, "redis-pipeline-cmd", redisSpan.OperationName)

	expectedTags := buildExpectedTags("get -> get")
	actualTags := assertTags(t, redisSpan, expectedTags)
	actualDbStatementValue := fmt.Sprint(actualTags["db.statement"])
	assert.True(t, strings.Contains(actualDbStatementValue, "get foo"), "redis span tag 'db.statement' is invalid")
	assert.True(t, strings.Contains(actualDbStatementValue, "get bar"), "redis span tag 'db.statement' is invalid")

	tracer.Reset()
}

func callGet(t *testing.T, client *redis.Client) {
	_, err := client.Get("foo").Result()
	assert.Nil(t, err, "Redis returned error: %v", err)
}

func callGetPipeline(t *testing.T, client *redis.Client, getPipelineParams []string) {
	pipeline := client.Pipeline()
	for _, key := range getPipelineParams {
		pipeline.Get(key)
	}
	_, err := pipeline.Exec()
	assert.Nil(t, err, "Redis returned error: %v", err)
}

// MISC

func buildExpectedTags(expectedDbMethod string) map[string]interface{} {
	expectedTags := make(map[string]interface{})
	expectedTags["db.type"] = "redis"
	expectedTags["db.method"] = expectedDbMethod
	expectedTags["db.statement"] = ""
	expectedTags["peer.address"] = redisAddr
	expectedTags["span.kind"] = ext.SpanKindEnum("client")
	return expectedTags
}

func assertTags(t *testing.T, redisSpan *mocktracer.MockSpan, expectedTags map[string]interface{}) map[string]interface{} {
	actualTags := redisSpan.Tags()
	assert.Equal(t, len(expectedTags), len(actualTags), "redis span tags number is invalid")
	for expectedTagKey, expectedTagValue := range expectedTags {
		actualTagValue, ok := actualTags[expectedTagKey]
		assert.True(t, ok, "redis span doesn't have tag '%s'", expectedTagKey)
		// db.statement tag is asserted outside as randomly generated because of redis
		if expectedTagKey != "db.statement" {
			assert.Equal(t, expectedTagValue, actualTagValue, "redis span tag '%s' is invalid", expectedTagKey)
		}
	}
	return actualTags
}
