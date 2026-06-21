use std::{
    collections::BinaryHeap,
    sync::{Arc, Mutex},
};
use tokio::{
    spawn,
    time::{sleep, Duration, Instant},
};

// Struct to hold scheduled task with the time to execute it
pub struct ScheduledTask {
    pub execute_at: Instant,
    pub task: Box<dyn FnOnce() + Send + 'static>,
}

// Trait implementations for ScheduledTask for BinaryHeap
// ScheduledTask with smaller(earlier) execute_at will have be higher in priority
impl PartialEq for ScheduledTask {
    fn eq(&self, other: &Self) -> bool {
        self.execute_at == other.execute_at
    }
}

impl Eq for ScheduledTask {}

impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.execute_at.cmp(&self.execute_at)
    }
}

impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct TaskScheduler {
    pub tasks: Arc<Mutex<BinaryHeap<ScheduledTask>>>,
}

impl TaskScheduler {
    pub fn new() -> Self {
        TaskScheduler {
            tasks: Arc::new(Mutex::new(BinaryHeap::new())),
        }
    }

    pub fn schedule<F>(&self, delay: Duration, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let execution_time = Instant::checked_add(&Instant::now(), delay).unwrap();
        let mut lock = self.tasks.lock().unwrap();
        lock.push(ScheduledTask {
            execute_at: execution_time,
            task: Box::new(f),
        });
    }

    // Spawn a background task to process queue
    // It can include infinite loop
    pub fn start(&self) {
        let tasks = self.tasks.clone();
        spawn(async move {
            loop {
                let now = Instant::now();
                // Check if the next task is ready WITHOUT holding the lock for long
                let task_to_run = {
                    let mut lock = tasks.lock().unwrap();
                    if let Some(task) = lock.peek() {
                        if task.execute_at <= now {
                            Some(lock.pop().unwrap())
                        } else {
                            None
                        }
                    } else {
                        None // Queue is empty
                    }
                }; // Lock dropped

                // Execute or Wait
                if let Some(scheduled_task) = task_to_run {
                    spawn(async move {
                        (scheduled_task.task)();
                    });
                } else {
                    sleep(Duration::from_millis(10)).await;
                }
            }
        });
    }
}

#[tokio::test]
async fn test_scheduling() {
    let scheduler = TaskScheduler::new();
    let counter = Arc::new(Mutex::new(0));

    scheduler.start();

    {
        let counter = Arc::clone(&counter);
        scheduler.schedule(Duration::from_millis(50), move || {
            let mut num = counter.lock().unwrap();
            *num += 1;
        });
    }

    // Should not have run yet
    sleep(Duration::from_millis(10)).await;
    assert_eq!(*counter.lock().unwrap(), 0);

    // Should have run now
    sleep(Duration::from_millis(100)).await;
    assert_eq!(*counter.lock().unwrap(), 1);
}

#[tokio::test]
async fn test_multiple_tasks() {
    let scheduler = TaskScheduler::new();
    let result = Arc::new(Mutex::new(Vec::new()));

    scheduler.start();

    {
        let result = Arc::clone(&result);
        scheduler.schedule(Duration::from_millis(100), move || {
            result.lock().unwrap().push(2);
        });
    }
    {
        let result = Arc::clone(&result);
        scheduler.schedule(Duration::from_millis(20), move || {
            result.lock().unwrap().push(1);
        });
    }

    sleep(Duration::from_millis(150)).await;
    let final_res = result.lock().unwrap();
    assert_eq!(*final_res, vec![1, 2]); // Order based on delay
}

#[tokio::test]
async fn test_counter_starts_zero() {
    let counter = Arc::new(Mutex::new(0));
    assert_eq!(*counter.lock().unwrap(), 0);
}

#[tokio::test]
async fn test_two_tasks_run() {
    let scheduler = TaskScheduler::new();
    let counter = Arc::new(Mutex::new(0));
    scheduler.start();
    for _ in 0..2 {
        let counter = Arc::clone(&counter);
        scheduler.schedule(Duration::from_millis(20), move || {
            *counter.lock().unwrap() += 1;
        });
    }
    sleep(Duration::from_millis(100)).await;
    assert_eq!(*counter.lock().unwrap(), 2);
}

#[tokio::test]
async fn test_task_runs_after_delay() {
    let scheduler = TaskScheduler::new();
    let counter = Arc::new(Mutex::new(0));
    scheduler.start();
    let c = Arc::clone(&counter);
    scheduler.schedule(Duration::from_millis(50), move || {
        *c.lock().unwrap() += 10;
    });
    sleep(Duration::from_millis(5)).await;
    assert_eq!(*counter.lock().unwrap(), 0);
    sleep(Duration::from_millis(100)).await;
    assert_eq!(*counter.lock().unwrap(), 10);
}

#[tokio::test]
async fn test_task_not_run_immediately() {
    let scheduler = TaskScheduler::new();
    let counter = Arc::new(Mutex::new(0));
    scheduler.start();
    let c = Arc::clone(&counter);
    scheduler.schedule(Duration::from_millis(200), move || {
        *c.lock().unwrap() += 1;
    });
    assert_eq!(*counter.lock().unwrap(), 0);
}

#[tokio::test]
async fn test_five_tasks_run() {
    let scheduler = TaskScheduler::new();
    let counter = Arc::new(Mutex::new(0));
    scheduler.start();
    for _ in 0..5 {
        let counter = Arc::clone(&counter);
        scheduler.schedule(Duration::from_millis(20), move || {
            *counter.lock().unwrap() += 1;
        });
    }
    sleep(Duration::from_millis(150)).await;
    assert_eq!(*counter.lock().unwrap(), 5);
}

#[tokio::test]
async fn test_ordering_by_delay() {
    let scheduler = TaskScheduler::new();
    let result = Arc::new(Mutex::new(Vec::new()));
    scheduler.start();
    let r1 = Arc::clone(&result);
    scheduler.schedule(Duration::from_millis(80), move || {
        r1.lock().unwrap().push(2);
    });
    let r2 = Arc::clone(&result);
    scheduler.schedule(Duration::from_millis(30), move || {
        r2.lock().unwrap().push(1);
    });
    sleep(Duration::from_millis(150)).await;
    assert_eq!(*result.lock().unwrap(), vec![1, 2]);
}

#[tokio::test]
async fn test_accumulation() {
    let scheduler = TaskScheduler::new();
    let sum = Arc::new(Mutex::new(0));
    scheduler.start();
    for i in 1..=3u32 {
        let sum = Arc::clone(&sum);
        scheduler.schedule(Duration::from_millis((20 * i).into()), move || {
            *sum.lock().unwrap() += i;
        });
    }
    sleep(Duration::from_millis(200)).await;
    assert_eq!(*sum.lock().unwrap(), 6);
}

#[tokio::test]
async fn test_set_value() {
    let scheduler = TaskScheduler::new();
    let counter = Arc::new(Mutex::new(0));
    scheduler.start();
    let c = Arc::clone(&counter);
    scheduler.schedule(Duration::from_millis(30), move || {
        *c.lock().unwrap() = 42;
    });
    sleep(Duration::from_millis(100)).await;
    assert_eq!(*counter.lock().unwrap(), 42);
}
