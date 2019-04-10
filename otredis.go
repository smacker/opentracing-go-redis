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
	ctxClient := client.WithContext(ctx)
	opts := ctxClient.Options()
	ctxClient.WrapProcess(process(parentSpan, opts))
	ctxClient.WrapProcessPipeline(processPipeline(parentSpan, opts))
	return ctxClient
}

func process(parentSpan opentracing.Span, opts *redis.Options) func(oldProcess func(cmd redis.Cmder) error) func(cmd redis.Cmder) error {
	return func(oldProcess func(cmd redis.Cmder) error) func(cmd redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			dbMethod, dbStatement := formatCommandAsDbTags(cmd)
			doSpan(parentSpan, opts, "redis-cmd", dbMethod, dbStatement)
			return oldProcess(cmd)
		}
	}
}

func processPipeline(parentSpan opentracing.Span, opts *redis.Options) func(oldProcess func(cmds []redis.Cmder) error) func(cmds []redis.Cmder) error {
	return func(oldProcess func(cmds []redis.Cmder) error) func(cmds []redis.Cmder) error {
		return func(cmds []redis.Cmder) error {
			dbMethod, dbStatement := formatCommandsAsDbTags(cmds)
			doSpan(parentSpan, opts, "redis-pipeline-cmd", dbMethod, dbStatement)
			return oldProcess(cmds)
		}
	}
}

func formatCommandAsDbTags(cmd redis.Cmder) (string, string) {
	dbMethod := cmd.Name()
	sprintArgs := make([]string, len(cmd.Args()))
	for i, arg := range cmd.Args() {
		sprintArgs[i] = fmt.Sprint(arg)
	}
	dbStatement := strings.Join(sprintArgs, " ")
	return dbMethod, dbStatement
}

func formatCommandsAsDbTags(cmds []redis.Cmder) (string, string) {
	cmdsAsDbMethods := make([]string, len(cmds))
	cmdsAsDbStatements := make([]string, len(cmds))
	for i, cmd := range cmds {
		dbMethod, dbStatement := formatCommandAsDbTags(cmd)
		cmdsAsDbMethods[i] = dbMethod
		cmdsAsDbStatements[i] = dbStatement
	}
	return strings.Join(cmdsAsDbMethods, " -> "), strings.Join(cmdsAsDbStatements, "\n")
}

func doSpan(parentSpan opentracing.Span, opts *redis.Options, operationName, dbMethod, dbStatement string) {
	tracer := parentSpan.Tracer()
	span := tracer.StartSpan(operationName, opentracing.ChildOf(parentSpan.Context()))
	defer span.Finish()
	ext.DBType.Set(span, "redis")
	span.SetTag("db.method", dbMethod)
	ext.DBStatement.Set(span, dbStatement)
	ext.PeerAddress.Set(span, opts.Addr)
	ext.SpanKind.Set(span, ext.SpanKindEnum("client"))
}
