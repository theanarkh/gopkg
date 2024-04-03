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
	"sync"
	"sync/atomic"
)

// 对外暴露的 API，由下面的 pool struct 实现
type Pool interface {
	// Name returns the corresponding pool name.
	Name() string
	// SetCap sets the goroutine capacity of the pool.
	SetCap(cap int32)
	// Go executes f.
	Go(f func())
	// CtxGo executes f and accepts the context.
	CtxGo(ctx context.Context, f func())
	// SetPanicHandler sets the panic handler.
	SetPanicHandler(f func(context.Context, interface{}))
	// WorkerCount returns the number of running workers
	WorkerCount() int32
}

// 处理 task 复用的 Pool
var taskPool sync.Pool

func init() {
	// 实现自定义创建 task 的函数
	taskPool.New = newTask
}

type task struct {
	ctx context.Context
	f   func()
	// 用于链表
	next *task
}

// 处理完从链表函数时重置字段
func (t *task) zero() {
	t.ctx = nil
	t.f = nil
	t.next = nil
}

// 放回 task pool
func (t *task) Recycle() {
	t.zero()
	taskPool.Put(t)
}

// 没有可复用的 task 时创建新的 task
func newTask() interface{} {
	return &task{}
}

type taskList struct {
	sync.Mutex
	taskHead *task
	taskTail *task
}

type pool struct {
	// The name of the pool
	name string

	// capacity of the pool, the maximum number of goroutines that are actually working
	cap int32
	// Configuration information
	config *Config
	// linked list of tasks

	// task 链表
	taskHead *task
	taskTail *task
	// 用于多个协程互斥访问 task
	taskLock sync.Mutex
	// task 数量
	taskCount int32

	// Record the number of running workers
	// worker 数量
	workerCount int32

	// This method will be called when the worker panic
	panicHandler func(context.Context, interface{})
}

// NewPool creates a new pool with the given name, cap and config.
func NewPool(name string, cap int32, config *Config) Pool {
	p := &pool{
		name:   name,
		cap:    cap,
		config: config,
	}
	return p
}

func (p *pool) Name() string {
	return p.name
}

func (p *pool) SetCap(cap int32) {
	atomic.StoreInt32(&p.cap, cap)
}

// 提交一个任务
func (p *pool) Go(f func()) {
	p.CtxGo(context.Background(), f)
}

func (p *pool) CtxGo(ctx context.Context, f func()) {
	// 获取一个 task
	t := taskPool.Get().(*task)
	// 把任务封装到 task
	t.ctx = ctx
	t.f = f
	// 加锁把 task 插入 task 链表
	p.taskLock.Lock()
	// 是第一个 task 则直接更新头尾指针指向它
	if p.taskHead == nil {
		p.taskHead = t
		p.taskTail = t
	} else {
		// 否则当前链表的最后一个 task 的 next 指针指向新的 task
		p.taskTail.next = t
		// 尾指针指向新的 task，task 为新的尾节点
		p.taskTail = t
	}
	p.taskLock.Unlock()
	// 任务数加一
	atomic.AddInt32(&p.taskCount, 1)
	// The following two conditions are met:
	// 1. the number of tasks is greater than the threshold.
	// 2. The current number of workers is less than the upper limit p.cap.
	// or there are currently no workers.
	// 创建新 worker 的条件
	// 当前还没有 worker
	// 任务数小于 ScaleThreshold（任务数不多则使用存量的 worker 处理），并且 worker 数还没有达到上限（保证 worker 数不要过多）
	if (atomic.LoadInt32(&p.taskCount) >= p.config.ScaleThreshold && p.WorkerCount() < atomic.LoadInt32(&p.cap)) || p.WorkerCount() == 0 {
		// worker 数加一
		p.incWorkerCount()
		// 获取一个 worker
		w := workerPool.Get().(*worker)
		// 关联到 pool
		w.pool = p
		// 创建协程处理任务
		w.run()
	}
}

// SetPanicHandler the func here will be called after the panic has been recovered.
func (p *pool) SetPanicHandler(f func(context.Context, interface{})) {
	p.panicHandler = f
}

func (p *pool) WorkerCount() int32 {
	return atomic.LoadInt32(&p.workerCount)
}

func (p *pool) incWorkerCount() {
	atomic.AddInt32(&p.workerCount, 1)
}

func (p *pool) decWorkerCount() {
	atomic.AddInt32(&p.workerCount, -1)
}
