# Tiny Systems Example Module

Template repository for building your own Tiny Systems module. Fork this repo to get started.

## What's Included

A minimal Echo component that receives a message and passes it through:

```go
func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) any {
    if in, ok := msg.(InMessage); ok {
        return handler(ctx, OutPort, in.Context)
    }
    return fmt.Errorf("invalid message")
}
```

This demonstrates the core patterns:
- Component interface (`GetInfo`, `Handle`, `Ports`, `Instance`)
- Input/output ports with typed messages
- Handler response propagation (blocking I/O)
- `configurable:"true"` struct tag for edge data mapping

## Project Structure

```
cmd/main.go              # Entry point — registers components, runs CLI
components/echo/echo.go  # Example component
go.mod                   # SDK dependency (github.com/tiny-systems/module)
```

## Getting Started

1. **Use this template** — click "Use this template" on GitHub
2. **Rename the module** in `go.mod`
3. **Add your components** under `components/`
4. **Register them** via `init()` + `registry.Register()`

## Run Locally

```shell
go run cmd/main.go run \
  --name=my-org/my-module-v1 \
  --namespace=tinysystems \
  --version=1.0.0
```

## Build and Deploy

```shell
# Build container image
docker build -t myregistry/my-module:1.0.0 .
docker push myregistry/my-module:1.0.0

# Install via Helm
helm repo add tinysystems https://tiny-systems.github.io/module/
helm install my-module tinysystems/tinysystems-operator \
  --set controllerManager.manager.image.repository=myregistry/my-module
```

## Resources

- [Developer Guide](https://docs.tinysystems.io/developer-guide/getting-started/hello-world-component) — build your first component
- [Module SDK](https://github.com/tiny-systems/module) — core library
- [Component Examples](https://docs.tinysystems.io/examples/components/simple-transformer) — real-world patterns
- [Tiny Systems Platform](https://tinysystems.io) — visual editor and module directory

## License

This module's source code is MIT-licensed. It depends on the [Tiny Systems Module SDK](https://github.com/tiny-systems/module) (BSL 1.1). See [LICENSE](LICENSE) for details.
