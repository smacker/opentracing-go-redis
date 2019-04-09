package otredis

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type OpenTracingRedisTestSuite struct {
	suite.Suite
	miniRedis  *miniredis.Miniredis
	client     *redis.Client
	mockTracer *mocktracer.MockTracer
}

func TestOpenTracingRedisTestSuite(t *testing.T) {
	tests := new(OpenTracingRedisTestSuite)
	suite.Run(t, tests)
}

func (ts *OpenTracingRedisTestSuite) SetupSuite() {
	// Common
	redisAddr := "127.0.0.1:6379"
	// MiniRedis
	ts.miniRedis = miniredis.NewMiniRedis()
	ts.miniRedis.StartAddr(redisAddr)
	// Client
	ts.client = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	// MockTracer
	ts.mockTracer = mocktracer.New()
	opentracing.SetGlobalTracer(ts.mockTracer)
}

func (ts *OpenTracingRedisTestSuite) TearDownSuite() {
	// Client
	ts.client.Close()
	// MiniRedis
	ts.miniRedis.Close()
}

func (ts *OpenTracingRedisTestSuite) Test_ProcessExecution() {

	t := ts.T()
	ctx := context.Background()

	hmSetKey, hmSetValues := buildHMSetCmdData("PROCESS_EXEC")

	span, ctx := opentracing.StartSpanFromContext(ctx, "test-process-execution")

	_, err := WrapRedisClient(ctx, ts.client).HMSet(hmSetKey, hmSetValues).Result()
	assert.Nil(t, err, "redis execution failed: %+v", err)

	span.Finish()

	finishedSpans := ts.mockTracer.FinishedSpans()
	expectedFinishedSpansNumber := 2
	assert.Equal(t, expectedFinishedSpansNumber, len(finishedSpans), "the number of finished spans is invalid")

	redisSpan := finishedSpans[0]
	expectedOperationName := "redis-cmd"
	assert.Equal(t, expectedOperationName, redisSpan.OperationName)
	expectedTags := buildExpectedTags("PROCESS_EXEC")
	assertTags(t, redisSpan, expectedTags)

	ts.mockTracer.Reset()
}

func (ts *OpenTracingRedisTestSuite) Test_ProcessPipelineExecution() {

	t := ts.T()
	ctx := context.Background()

	hmSetKey, hmSetValues := buildHMSetCmdData("PROCESS_PIPELINE_EXEC")

	span, ctx := opentracing.StartSpanFromContext(ctx, "test-process-pipeline-execution")

	pipeline := WrapRedisClient(ctx, ts.client).TxPipeline()
	pipeline.HMSet(hmSetKey, hmSetValues)
	_, err := pipeline.Exec()
	assert.Nil(t, err, "redis pipeline execution failed: %+v", err)

	span.Finish()

	finishedSpans := ts.mockTracer.FinishedSpans()
	expectedFinishedSpansNumber := 2
	assert.Equal(t, expectedFinishedSpansNumber, len(finishedSpans), "the number of finished spans is invalid")

	redisSpan := finishedSpans[0]
	expectedOperationName := "redis-pipeline-cmd"
	assert.Equal(t, expectedOperationName, redisSpan.OperationName)
	expectedTags := buildExpectedTags("PROCESS_PIPELINE_EXEC")
	assertTags(t, redisSpan, expectedTags)

	ts.mockTracer.Reset()
}

func buildHMSetCmdData(testSuffix string) (string, map[string]interface{}) {

	key := "key:TEST_" + testSuffix
	values := make(map[string]interface{})
	values["TEST_KEY_"+testSuffix] = "TEST_VALUE_" + testSuffix

	return key, values
}

func buildExpectedTags(testSuffix string) map[string]interface{} {
	expectedTags := make(map[string]interface{})
	expectedTags["db.type"] = "redis"
	expectedTags["db.statement"] = "hmset key:TEST_" + testSuffix + " TEST_KEY_" + testSuffix + " TEST_VALUE_" + testSuffix + ": "
	expectedTags["peer.address"] = "127.0.0.1:6379"
	expectedTags["span.kind"] = ext.SpanKindEnum("client")
	return expectedTags
}

func assertTags(t *testing.T, redisSpan *mocktracer.MockSpan, expectedTags map[string]interface{}) {

	actualTags := redisSpan.Tags()
	assert.Equal(t, len(expectedTags), len(actualTags), "redis span tags number is invalid")

	for expectedTagKey, expectedTagValue := range expectedTags {
		actualTag, ok := actualTags[expectedTagKey]
		assert.True(t, ok, "redis span doesn't have tag '%s'", expectedTagKey)
		assert.Equal(t, expectedTagValue, actualTag, "redis span tag '%s' is invalid", expectedTagKey)
	}
}
