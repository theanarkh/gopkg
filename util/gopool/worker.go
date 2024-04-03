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
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/bytedance/gopkg/util/logger"
)

// 管理 worker 复用的 pool
var workerPool sync.Pool

func init() {
	// 实现创建 worker 的函数
	workerPool.New = newWorker
}

type worker struct {
	pool *pool
}

func newWorker() interface{} {
	return &worker{}
}

func (w *worker) run() {
	// 创建新的协程处理任务，而不是在调用者协程
	go func() {
		for {
			var t *task
			// 加锁获取任务
			w.pool.taskLock.Lock()
			// 获取头节点，即第一个任务
			if w.pool.taskHead != nil {
				t = w.pool.taskHead
				// 更新头节点
				w.pool.taskHead = w.pool.taskHead.next
				atomic.AddInt32(&w.pool.taskCount, -1)
			}
			// 没有任务了，退出协程，把 worker 放回 pool
			if t == nil {
				// if there's no task to do, exit
				w.close()
				w.pool.taskLock.Unlock()
				w.Recycle()
				return
			}
			w.pool.taskLock.Unlock()
			// 在单独的函数中处理任务，如果执行任务时出现 panic 会被下面 defer 函数中 recover 处理
			// 如果不在单独的函数处理任务的话，会导致整个协程退出
			func() {
				defer func() {
					if r := recover(); r != nil {
						if w.pool.panicHandler != nil {
							w.pool.panicHandler(t.ctx, r)
						} else {
							msg := fmt.Sprintf("GOPOOL: panic in pool: %s: %v: %s", w.pool.name, r, debug.Stack())
							logger.CtxErrorf(t.ctx, msg)
						}
					}
				}()
				t.f()
			}()
			// 把 task 放回 task pool 复用
			t.Recycle()
		}
	}()
}

func (w *worker) close() {
	w.pool.decWorkerCount()
}

func (w *worker) zero() {
	w.pool = nil
}

// 把 worker 放回 pool
func (w *worker) Recycle() {
	w.zero()
	workerPool.Put(w)
}
