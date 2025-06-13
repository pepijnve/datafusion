# Query Cancellation

## The Challenge of Cancelling Long-Running Queries

Have you ever tried to cancel a query that just wouldn't stop? This document explores why that happens in DataFusion and what we can do about it.

### Understanding Rust's Async Model

Rust's asynchronous programming model is built around the `Future<T>` trait, which works quite differently from async models in other languages.
Unlike systems where async code runs continuously on background threads, a Rust `Future<T>` represents a lazy calculation that only makes progress when explicitly polled.

When you call the `poll` function on a `Future<T>`, you're asking it to advance its calculation as much as possible.
The future responds with either:
- `Poll::Pending` if it needs to wait for something (like I/O) before continuing
- `Poll::Ready<T>` when it has completed and produced a value

When a future returns `Pending`, it saves its internal state so it can pick up where it left off the next time you poll it.
This state management is what makes Rust's futures memory-efficient and composable.

Rust's `async` keyword provides syntactic sugar over this model.
When you write an `async` function or block, the compiler transforms it into an implementation of the `Future` trait.
This transformation makes async code much more readable while maintaining the same underlying mechanics.

The `await` keyword complements this by letting you pause execution until a future completes.
When you write `.await` after a future, you're essentially telling the compiler to poll that future until it's ready, and then continue with the result.

### From Futures to Streams

The `futures` crate extends this model with the `Stream` trait, which represents a sequence of values produced asynchronously rather than just a single value.
A `Stream` has a `poll_next` method that returns:
- `Poll::Pending` when the next value isn't ready yet
- `Poll::Ready(Some(value))` when a new value is available
- `Poll::Ready(None)` when the stream is exhausted

### How DataFusion Executes Queries

In DataFusion, query execution follows this async pattern.
When you run a query:

1. The query is compiled into a tree of `ExecutionPlan` nodes
2. Calling `ExecutionPlan::execute` returns a `SendableRecordBatchStream` (essentially a `Box<dyn Stream<RecordBatch>>`)
3. This stream is actually the root of a tree of streams where each node processes data from its children

Query execution progresses each time you call `poll_next` on the root stream.
This call typically cascades down the tree, with each node calling `poll_next` on its children to get the data it needs to process.

Here's where things get tricky: some operations (like aggregations, sorts, or certain join phases) need to process a lot of data before producing any output.
When `poll_next` encounters one of these operations, it might need to do substantial work before returning.

### Tokio and Cooperative Scheduling

DataFusion runs on top of Tokio, which uses a cooperative scheduling model.
This is fundamentally different from preemptive scheduling:

- In preemptive scheduling, the system can interrupt a task at any time to run something else
- In cooperative scheduling, tasks must voluntarily yield control back to the scheduler

This distinction is crucial for understanding our cancellation problem.
When a Tokio task is running, it can't be forcibly interrupted - it must cooperate by periodically yielding control.

When you call `JoinHandle::abort()` on a Tokio task, you're not immediately stopping it. You're just telling Tokio: "When this task next yields control, don't resume it."
If the task never yields, it can't be cancelled.

### The Cancellation Problem

Let's look at how the DataFusion CLI tries to handle query cancellation.
The code below paraphrases what the CLI actually does:

```rust
fn exec_query() {
    let runtime: tokio::runtime::Runtime = ...;
    let stream: SendableRecordBatchStream = ...;
    let cancellation_token = CancellationToken::new();

    runtime.block_on(async {
        tokio::select! {
            next_batch = stream.next() => ...
            _ = signal::ctrl_c() => ...,
        }
    })
}
```

The `select!` macro is supposed to race these two futures and complete when either one finishes.
When you press Ctrl+C, the `signal::ctrl_c()` future should complete, allowing the query to be cancelled.

But there's a catch: `select!` still follows cooperative scheduling rules.
It polls each future in sequence, and if the first one (our query) gets stuck in a long computation, it never gets around to polling the cancellation signal.

Imagine a query that needs to calculate something intensive, like sorting billions of rows.
The `poll_next` call might run for hours without returning.
During this time, Tokio can't check if you've pressed Ctrl+C, and the query continues running despite your cancellation request.

### The Path Forward

Since DataFusion runs in Tokio's cooperative environment, we need to ensure all our operations play by the rules.
Every long-running operation must periodically yield control back to the scheduler, allowing cancellation checks to happen.

Unfortunately, not all parts of DataFusion currently do this consistently.
This document outlines our current approach to this problem and explores potential solutions to make DataFusion queries properly cancellable in all scenarios.

## Seeing the Problem in Action

### A Typical Blocking Operator

Let's examine a real-world example to better understand the cancellation challenge.
Here's a simplified implementation of a `COUNT(*)` aggregation - something you might use in a query like `SELECT COUNT(*) FROM table`:

```rust
struct BlockingStream {
    stream: SendableRecordBatchStream,
    count: usize,
    finished: bool,
}

impl Stream for BlockingStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            match ready!(self.stream.poll_next_unpin(cx)) {
                None => {
                    self.finished = true;
                    return Poll::Ready(Some(Ok(create_record_batch(self.count))));
                }
                Some(Ok(batch)) => self.count += batch.num_rows(),
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
            }
        }
    }
}
```

How does this code work? Let's break it down step by step:

1. **Initial check**: We first check if we've already finished processing. If so, we return `Ready(None)` to signal the end of our stream:
   ```rust
   if self.finished {
       return Poll::Ready(None);
   }
   ```

2. **Processing loop**: We then enter a loop to process all incoming batches from our child stream:
   ```rust
   loop {
       match ready!(self.stream.poll_next_unpin(cx)) {
           // Handle different cases...
       }
   }
   ```
   The `ready!` macro checks if the child stream returned `Pending` and if so, immediately returns `Pending` from our function too.

3. **End of input**: When the child stream is exhausted (returns `None`), we calculate our final result:
   ```rust
   None => {
       self.finished = true;
       return Poll::Ready(Some(Ok(create_record_batch(self.count))));
   }
   ```

4. **Processing data**: For each batch we receive, we simply add its row count to our running total:
   ```rust
   Some(Ok(batch)) => self.count += batch.num_rows(),
   ```

5. **Error handling**: If we encounter an error, we pass it along immediately:
   ```rust
   Some(Err(e)) => return Poll::Ready(Some(Err(e))),
   ```

This code looks perfectly reasonable at first glance.
But there's a subtle issue lurking here: what happens if the child stream processes a large amount of data without ever returning `Pending`?

In that case, our loop will keep running without ever yielding control back to Tokio's scheduler.
This means we could be stuck in a single `poll_next` call for minutes or even hours - exactly the scenario that prevents query cancellation from working!

### The bigger picture

Remember that query streams are just the tip of an iceberg (or should )


## How can we fix this?

It's essential that we return `Pending` every now and then.
There are a number of ways we can achieve this each with its own strengths and weaknesses.
Let's go over these.

### Being a responsible citizen 

One simple way to achieve this is using a loop counter.
We do the exact same thing as before, but on each loop iteration we decrement our counter.
If the counter hits zero we return `Pending`.
This ensures we iterate at most 128 times before yielding.

```rust
fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    if self.finished {
        return Poll::Ready(None);
    }

    let mut counter = 128;
    loop {
        match ready!(self.stream.poll_next_unpin(cx)) {
            ...
        }
        counter -= 1;
        if counter == 0 {
            return Poll::Pending;
        }
    }
}
```
