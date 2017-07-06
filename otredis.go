package otredis

import (
	"context"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"gopkg.in/redis.v5"
)

// WrapRedisClient adds opentracing measurements for commands and returns cloned client
func WrapRedisClient(ctx context.Context, c *redis.Client) *redis.Client {
	if ctx == nil {
		return c
	}
	parentSpan := opentracing.SpanFromContext(ctx)
	if parentSpan == nil {
		return c
	}

	// clone using context
	copy := c.WithContext(c.Context())
	copy.WrapProcess(func(oldProcess func(cmd redis.Cmder) error) func(cmd redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			tr := parentSpan.Tracer()
			sp := tr.StartSpan("redis", opentracing.ChildOf(parentSpan.Context()))
			ext.DBType.Set(sp, "redis")
			sp.SetTag("db.method", strings.Split(cmd.String(), " ")[0])
			defer sp.Finish()

			return oldProcess(cmd)
		}
	})
	return copy
}
