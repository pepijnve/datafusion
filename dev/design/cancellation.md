# Query Cancellation

## Problem Statement

Asynchronous code in Rust is modeled using the `Future<T>` trait.
Contrary to some other ecosystems, a `Future<T>` does not represent code that is actively run on some other thread.
Instead, it represents the lazy calculation of some value as a single method named `poll`.
Calling the `poll` function of a `Future<T>` represents the action of advancing the lazy calculation either fully or partially.
A partial evaluation is represented using the return value `Poll::Pending`.
For the final evaluated result `Poll::Ready<T>` is used.
When a future returns `Pending`, the internal state of the future implementation will have been updated to represent the state where the evaluation stopped.
This internal state is used to resume the evaluation the next time `poll` is called.

Rust also supports an `async` keyword which can be assigned to functions, closures and blocks.
The translation is not exactly one-to-one, but a good enough mental model is that the compiler will translate both async functions and blocks into anonymous implementations of `Future`.
That makes the two more or less interchangeable.

A second related keyword is `await`.
Within an `async` scope, `.await` can be appended to any async item or `Future` to poll it.
Again not 100% accurate, but this is more or less the equivalent to `ready!(item.poll())`.

The `futures` crate extends the lazy evaluation model of `Future<T>` with a trait named `Stream` which represents a lazily evaluated sequence of values rather than just a single value.
`Stream` also has a single method `poll_next` which returns `Poll<Option<T>`.
Just like in `Future`, `Pending` signals that the next value of the sequence has not been fully evaluated.
One or more followup calls to `poll_next` is required to proceed.
`Ready(Some(_))` represent a fully evaluated value of the sequence.
`Ready(None)` signals the end of the sequence.

In DataFusion a query is first compiled to a tree of `ExecutionPlan` instances with a single root.
The query is executed by calling `ExecutionPlan::execute` which returns a `SendableRecordBatchStream` instance.
Simplifying a bit, `SendableRecordBatchStream` is a type alias for `Box<dyn Stream<RecordBatch>>`.
In other words, 'executing' a query results in a lazily evaluated sequence of `RecordBatch`, with a type that is not known at compile time.
The returned stream will typically be the root of a tree of `Box<dyn Stream<RecordBatch>>` where each child represents the input stream of rows that are processed by the parent.

The query is progressively executed each time `Stream::poll_next` is called.
Often the root stream will call `poll_next` on one of it's children, which may in turn do the same, and so on.
If somewhere in this chain of `poll_next` calls a pipeline blocking operation is encountered (aggregate, sort, some joins during their build phase, ...) the call to `poll_next` may have to perform work that takes a substantial amount of time before it can actually return a `RecordBatch`.
We'll come back to this in a minute.

DataFusion runs in the context of the Tokio asynchronous runtime environment.
In Tokio a task is an asynchronous green thread.
Tasks are executed by a cooperative scheduler which makes use of a fixed size pool of operating system threads to do the actual work.

Cooperative scheduling is in contrast to preemptive scheduling.
A preemptive scheduler has the ability to suspend a running task while it is active and switch to a different task.
A cooperative scheduler on the other hand requires that each task periodically, and explicitly yields to the scheduler.
The scheduler can only make scheduling decisions when a task yields.

Another consequence of cooperative scheduling is that tasks cannot be aborted while they are actively running.
When a task is spawned, Tokio returns a `JoinHandle` to the caller which represent a handle to the running task.
Calling `JoinHandle::abort` on this handle signals to the Tokio schedule that the task should be aborted.
Tokio will do whatever bookkeeping it needs to do to mark the task as aborted, but it will not forcibly terminate the operating system thread running the task if it's currently executing the task in question.
Instead, the scheduler will wait for the task to yield to the scheduler and only then will the task be discarded.

Coming back to DataFusion query execution now.
DataFusion queries are typically executed either using `tokio::runtime::Runtime::spawn` or `tokio::runtime::Runtime::block_on`.
For this discussion we'll look at `block_on`; `spawn` can be treated the same way.

The code snippet below paraphrases the query execution logic of the DataFusion CLI.
`next()` is an async wrapper around `poll_next`.
`signal::ctrl_c()` is an async function that completes when the process receives the `SIGINT` signal from the operating system.
`select!` polls each expression it is given until one completes.
Keep in mind that `select!` still needs to play by the cooperative scheduling rules described above.
It will not call `poll` on the two `Future` instance in parallel.
Instead, it calls it on each one sequentially, one after the other.
The first function call needs to return before the second one can start.

```rust
fn exec_query() {
    let runtime: tokio::runtime::Runtime = ...;
    let stream: SendableRecordBatchStream = ...;
    let cancellation_token = CancellationToken::new();
    
    runtime.block_on(async {
        tokio::select! {
            next_batch = stream.next() => ...
            _ = _ = signal::ctrl_c() => ...,
        }
    })
}
```

Imagine the query in question contains a long running, pipeline blocking operation.
What is going to happen when I now press ctrl-C?
The intended behavior is the `select!` completes, the `block_on` call returns, and the `exec_query` function exits.

But what would happen if the query contains an operator that is, for instance, calculating the billionth digit of pi?
The `poll_next` call on that stream may not return for a couple of hours.
In the meantime, Tokio has no way to actually poll the `ctrl_c` future, not can the `select!` macro return a value.
We effectively cannot cancel this query.

Because the Tokio environment is cooperative by design, and DataFusion runs in the context of Tokio, DataFusion needs to cooperate as well.
All parts of the library need to be aligned to ensure calls to `poll_next` on the `Stream` instances it creates do not block for an extended period of time.
Unfortunately, today that is not always the case in practice.
This document describes the current state of measures in the codebase to achieve this goal.
It also investigate possible future solutions to improve upon the current state.