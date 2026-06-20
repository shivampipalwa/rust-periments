use std::{
    cell::UnsafeCell,
    sync::atomic::{AtomicBool, Ordering},
};

const LOCKED: bool = true;
const UNLOCKED: bool = false;

pub struct Mutex<T> {
    locked: AtomicBool,
    v: UnsafeCell<T>,
}

unsafe impl<T> Sync for Mutex<T> where T: Send {}

impl<T> Mutex<T> {
    pub fn new(t: T) -> Self {
        Self {
            locked: AtomicBool::new(UNLOCKED),
            v: UnsafeCell::new(t),
        }
    }
    pub fn with_lock<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        while self.locked.load(Ordering::Relaxed) != UNLOCKED {}
        self.locked.store(LOCKED, Ordering::Relaxed);
        // Safety: We hold the lock, therefore we can create a mutable reference
        let ret = f(unsafe { &mut *self.v.get() });
        self.locked.store(UNLOCKED, Ordering::Relaxed);
        ret
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use super::*;

    #[test]
    fn it_works() {
        let counter = Arc::new(Mutex::new(0));
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let counter = Arc::clone(&counter);
                thread::spawn(move || {
                    for _ in 0..100 {
                        counter.with_lock(|v| *v += 1);
                    }
                })
            })
            .collect();
        for handle in handles {
            let _ = handle.join();
        }
        assert_eq!(counter.with_lock(|v| *v), 10 * 100);
    }
}
