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
        // looping on compare_exchange is very inefficient especially as the number of thread/cores increases
        // this is because all the cores will try to get the ownership of the memory location
        // compare_exchange is an inherently expensive proposition as it needs to coordinate exclusive access among core
        //
        // hence we use MESI protocol
        //
        // since we are using a separeate look, it is okay if compare_exchange fails for some other reason(`)
        while self
            .locked
            .compare_exchange_weak(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            // MESI protocol
            // this just needs shared access to the memory and not exclusive acess(as in compare_exchange)
            // hence it is more efficient
            while self.locked.load(Ordering::Relaxed) == LOCKED {}
        }
        self.locked.store(LOCKED, Ordering::Relaxed);
        // Safety: We hold the lock, therefore we can create a mutable reference
        let ret = f(unsafe { &mut *self.v.get() });
        self.locked.store(UNLOCKED, Ordering::Release);
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
        let handles: Vec<_> = (0..100)
            .map(|_| {
                let counter = Arc::clone(&counter);
                thread::spawn(move || {
                    for _ in 0..1000 {
                        counter.with_lock(|v| *v += 1);
                    }
                })
            })
            .collect();
        for handle in handles {
            let _ = handle.join();
        }
        assert_eq!(counter.with_lock(|v| *v), 100 * 1000);
    }
}
