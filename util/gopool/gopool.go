// Copyright 2021 ByteDance Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gopool

import (
	"context"
	"fmt"
	"math"
	"sync"
)

// 暴露一个全局（默认）的 Pool，用户只需要调用工具函数（工具函数操作默认的 Pool），而不需要显式创建一个 Pool
// defaultPool is the global default pool.
var defaultPool Pool

// 保存多个 Poll，以 string 为 key
var poolMap sync.Map

// 加载该模块时，创建默认的 Poll
func init() {
	defaultPool = NewPool("gopool.DefaultPool", math.MaxInt32, NewConfig())
}

// Go is an alternative to the go keyword, which is able to recover panic.
//
//	gopool.Go(func(arg interface{}){
//	    ...
//	}(nil))
//
// 工具函数，用户可以直接调该函数，不需要显式创建 Pool
func Go(f func()) {
	CtxGo(context.Background(), f)
}

// 同上面的 Go API
// CtxGo is preferred than Go.
func CtxGo(ctx context.Context, f func()) {
	defaultPool.CtxGo(ctx, f)
}

// SetCap is not recommended to be called, this func changes the global pool's capacity which will affect other callers.
func SetCap(cap int32) {
	defaultPool.SetCap(cap)
}

// SetPanicHandler sets the panic handler for the global pool.
func SetPanicHandler(f func(context.Context, interface{})) {
	defaultPool.SetPanicHandler(f)
}

// WorkerCount returns the number of global default pool's running workers
func WorkerCount() int32 {
	return defaultPool.WorkerCount()
}

// RegisterPool registers a new pool to the global map.
// GetPool can be used to get the registered pool by name.
// returns error if the same name is registered.
// 注册 Pool
func RegisterPool(p Pool) error {
	_, loaded := poolMap.LoadOrStore(p.Name(), p)
	if loaded {
		return fmt.Errorf("name: %s already registered", p.Name())
	}
	return nil
}

// GetPool gets the registered pool by name.
// Returns nil if not registered.
func GetPool(name string) Pool {
	p, ok := poolMap.Load(name)
	if !ok {
		return nil
	}
	return p.(Pool)
}
