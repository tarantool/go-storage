package locker_test

import (
	"context"
	"fmt"
	"log"

	storage "github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/driver/dummy"
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
