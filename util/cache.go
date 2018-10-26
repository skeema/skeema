package util

import (
	"fmt"
	"sync"

	"github.com/skeema/tengo"
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
