# Migration guide

## Migration from v1.x.x to v2.x.x

* [Major changes](#major-changes-v2)
* [Updating import paths](#updating-import-paths)
* [go.mod](#gomod)
* [No API changes](#no-api-changes)

### <a id="major-changes-v2">Major changes</a>

The only breaking change in `v2.0.0` is the Go module path. Following the
[Go module versioning rules][go-modules-v2], a major version `v2` and above
must carry a `/vN` suffix in the module path:

```
github.com/tarantool/go-storage  →  github.com/tarantool/go-storage/v2
```

There are **no source-level API changes**: every package, type, function, and
method keeps the same name and signature as in `v1.6.0`. Migration is a
mechanical rewrite of import paths, so the upgrade is safe and reversible.

The minimum required Go version is unchanged (`1.25`).

### <a id="updating-import-paths">Updating import paths</a>

Add the `/v2` suffix to every `go-storage` import. For example:

Before:
```Go
import (
	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/connect"
	"github.com/tarantool/go-storage/driver/etcd"
	"github.com/tarantool/go-storage/integrity"
	"github.com/tarantool/go-storage/locker"
)
```

After:
```Go
import (
	storage "github.com/tarantool/go-storage/v2"
	"github.com/tarantool/go-storage/v2/connect"
	"github.com/tarantool/go-storage/v2/driver/etcd"
	"github.com/tarantool/go-storage/v2/integrity"
	"github.com/tarantool/go-storage/v2/locker"
)
```

> **Note:** the root package is still named `storage`. When the import path ends
> in `/v2`, `goimports` cannot infer the package name, so add an explicit
> `storage` alias on the root import (as shown above) to keep references like
> `storage.Storage` unambiguous.

You can rewrite all import paths across a module in one pass:

```sh
# preview the affected files
grep -rl 'tarantool/go-storage' --include='*.go' .

# rewrite v1 paths to v2 (skips paths already carrying the /v2 suffix)
grep -rl 'tarantool/go-storage' --include='*.go' . | xargs sed -i '' \
	-e 's#tarantool/go-storage/v2#tarantool/go-storage#g' \
	-e 's#tarantool/go-storage#tarantool/go-storage/v2#g'
```

On GNU `sed` drop the `''` after `-i`. The double substitution first normalizes
any already-migrated paths back to the bare form, then appends `/v2` to all of
them, so the command is idempotent and safe to run more than once.

### <a id="gomod">go.mod</a>

Pull the new major version with `go get`:

```sh
go get github.com/tarantool/go-storage/v2@latest
go mod tidy
```

Your own module path does not change. Only the `require` line for `go-storage`
gains the `/v2` suffix:

```
require github.com/tarantool/go-storage/v2 v2.0.0
```

### <a id="no-api-changes">No API changes</a>

`v2.0.0` ships the exact same API surface as `v1.6.0`. Once the import paths are
updated and `go mod tidy` succeeds, no further code changes are required — your
package will compile and behave identically.

[go-modules-v2]: https://go.dev/ref/mod#major-version-suffixes
