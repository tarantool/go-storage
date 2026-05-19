package locker_test

import (
	"context"
	"fmt"
	"log"

	storage "github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/driver/dummy"
	"github.com/tarantool/go-storage/locker"
)

// Example shows the basic Lock/Unlock cycle of a locker obtained through
// Storage. The dummy driver ships an in-memory locker that is convenient for
// tests and demos; production code wires the same interface through the etcd
// or TCS driver.
func Example() {
	ctx := context.Background()
	stor := storage.NewStorage(dummy.New())

	lock, err := stor.NewLocker(ctx, "/locks/example")
	if err != nil {
		log.Fatalf("new-locker: %v", err)
	}

	err = lock.Lock(ctx)
	if err != nil {
		log.Fatalf("lock: %v", err)
	}

	fmt.Println("held:", lock.Key())

	err = lock.Unlock(ctx)
	if err != nil {
		log.Fatalf("unlock: %v", err)
	}

	// Output:
	// held: /locks/example
}

// ExampleDo shows the typical "acquire, work, release" pattern. Do creates
// a Locker via the supplied Factory, acquires it, runs fn while held, then
// releases — even if fn returns an error.
func ExampleDo() {
	ctx := context.Background()
	stor := storage.NewStorage(dummy.New())

	err := locker.Do(ctx, stor.LockerFactory(), "/locks/do-example", func(_ context.Context) error {
		fmt.Println("work under lock")

		return nil
	})
	if err != nil {
		log.Fatalf("do: %v", err)
	}

	// Output:
	// work under lock
}

// ExamplePrefixed shows how locker.Prefixed lets two subcomponents share a
// backend while keeping their lock names in separate namespaces — both can
// acquire "/shared" concurrently because the effective inner names differ.
func ExamplePrefixed() {
	ctx := context.Background()
	inner := storage.NewStorage(dummy.New()).LockerFactory()

	vaultA, err := locker.Prefixed("/vault-a", inner)
	if err != nil {
		log.Fatalf("prefixed vault-a: %v", err)
	}

	vaultB, err := locker.Prefixed("/vault-b", inner)
	if err != nil {
		log.Fatalf("prefixed vault-b: %v", err)
	}

	lockA, err := vaultA.NewLocker(ctx, "/shared")
	if err != nil {
		log.Fatalf("vault-a /shared: %v", err)
	}

	lockB, err := vaultB.NewLocker(ctx, "/shared")
	if err != nil {
		log.Fatalf("vault-b /shared: %v", err)
	}

	err = lockA.TryLock(ctx)
	if err != nil {
		log.Fatalf("lockA: %v", err)
	}

	err = lockB.TryLock(ctx)
	if err != nil {
		log.Fatalf("lockB: %v", err)
	}

	fmt.Println(lockA.Key())
	fmt.Println(lockB.Key())

	_ = lockA.Unlock(ctx)
	_ = lockB.Unlock(ctx)

	// Output:
	// /vault-a/shared
	// /vault-b/shared
}
