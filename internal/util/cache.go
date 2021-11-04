package util

import (
	"fmt"
	"sync"

	"github.com/skeema/skeema/internal/tengo"
)

var instanceCache struct {
	sync.Mutex
	instanceMap map[string]*tengo.Instance
}

func init() {
	instanceCache.instanceMap = make(map[string]*tengo.Instance)
}

// NewInstance wraps tengo.NewInstance such that two identical requests will
// return the same *tengo.Instance. This helps reduce excessive creation of
// redundant connections.
func NewInstance(driver, dsn string) (*tengo.Instance, error) {
	key := fmt.Sprintf("%s:%s", driver, dsn)
	instanceCache.Lock()
	defer instanceCache.Unlock()
	instance, already := instanceCache.instanceMap[key]
	if already {
		return instance, nil
	}
	instance, err := tengo.NewInstance(driver, dsn)
	if err != nil {
		return nil, err
	}
	instanceCache.instanceMap[key] = instance
	return instance, nil
}

// CloseCachedConnectionPools closes all connection pools in all cached
// Instances that were created via NewInstance.
func CloseCachedConnectionPools() {
	for _, inst := range instanceCache.instanceMap {
		inst.CloseAll()
	}
}

// FlushInstanceCache closes all connection pools in all cached Instances
// that were created via NewInstance, and then flushes the cache entirely.
func FlushInstanceCache() {
	instanceCache.Lock()
	defer instanceCache.Unlock()
	CloseCachedConnectionPools()
	instanceCache.instanceMap = make(map[string]*tengo.Instance)
}
