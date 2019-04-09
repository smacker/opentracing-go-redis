# opentracing go-redis

[OpenTracing](http://opentracing.io/) instrumentation for [go-redis](https://github.com/go-redis/redis).

## Install

```
go get -u github.com/smacker/opentracing-go-redis
```

## Usage

Clone redis client `c := otredis.WrapRedisClient(ctx, c)` with a span.

Example:

```go
var client *redis.Client

func Handler(ctx context.Context) {
    span, ctx := opentracing.StartSpanFromContext(ctx, "handler")
    defer span.Finish()

    // clone redis with proper context
    client := otredis.WrapRedisClient(ctx, client)

    // make requests to redis
    client.Get("foo")
}
```

Call to the `Handler` function would create redis span as a child of handler span.

## License

[MIT](LICENSE)
