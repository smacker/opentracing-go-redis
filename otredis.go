package otredis

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-redis/redis"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

func WrapRedisClient(ctx context.Context, client *redis.Client) *redis.Client {
	if ctx == nil {
		return client
	}
	parentSpan := opentracing.SpanFromContext(ctx)
	if parentSpan == nil {
		return client
	}
	clientWithContext := client.WithContext(ctx)
	opts := clientWithContext.Options()
	clientWithContext.WrapProcess(process(parentSpan, opts))
	clientWithContext.WrapProcessPipeline(processPipeline(parentSpan, opts))
	return clientWithContext
}

func process(parentSpan opentracing.Span, opts *redis.Options) func(oldProcess func(cmd redis.Cmder) error) func(cmd redis.Cmder) error {
	return func(oldProcess func(cmd redis.Cmder) error) func(cmd redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			dbStatement := formatCommandAsDbStatement(cmd)
			doSpan(parentSpan, opts, "redis-cmd", dbStatement)
			return oldProcess(cmd)
		}
	}
}

func processPipeline(parentSpan opentracing.Span, opts *redis.Options) func(oldProcess func(cmds []redis.Cmder) error) func(cmds []redis.Cmder) error {
	return func(oldProcess func(cmds []redis.Cmder) error) func(cmds []redis.Cmder) error {
		return func(cmds []redis.Cmder) error {
			dbStatement := formatCommandsAsDbStatement(cmds)
			doSpan(parentSpan, opts, "redis-pipeline-cmd", dbStatement)
			return oldProcess(cmds)
		}
	}
}

func formatCommandAsDbStatement(cmd redis.Cmder) string {
	return fmt.Sprintf("%s", cmd)
}

func formatCommandsAsDbStatement(cmds []redis.Cmder) string {
	cmdsAsDbStatements := make([]string, len(cmds))
	for i, cmd := range cmds {
		cmdAsDbStatement := formatCommandAsDbStatement(cmd)
		cmdsAsDbStatements[i] = cmdAsDbStatement
	}
	return strings.Join(cmdsAsDbStatements, "\n")
}

func doSpan(parentSpan opentracing.Span, opts *redis.Options, operationName, dbStatement string) {
	tr := parentSpan.Tracer()
	span := tr.StartSpan(operationName, opentracing.ChildOf(parentSpan.Context()))
	defer span.Finish()
	ext.DBType.Set(span, "redis")
	ext.DBStatement.Set(span, dbStatement)
	ext.PeerAddress.Set(span, opts.Addr)
	ext.SpanKind.Set(span, ext.SpanKindEnum("client"))
}
