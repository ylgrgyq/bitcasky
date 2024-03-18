use std::{
    fmt::Debug,
    ops::Deref,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::Arc;

pub trait Clock {
    fn now(&self) -> u64;
}

#[cfg(not(test))]
#[derive(Debug)]
pub struct SystemClock {}

#[cfg(not(test))]
impl Clock for SystemClock {
    fn now(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

#[cfg(test)]
#[derive(Debug)]
pub struct DebugClock {
    time: AtomicU64,
}

#[cfg(test)]
impl DebugClock {
    pub fn new(time: u64) -> DebugClock {
        DebugClock {
            time: AtomicU64::new(time),
        }
    }

    pub fn set(&self, time: u64) {
        self.time.store(time, Ordering::Release);
    }
}

#[cfg(test)]
impl Clock for DebugClock {
    fn now(&self) -> u64 {
        self.time.load(Ordering::Acquire)
    }
}

#[cfg(test)]
#[derive(Debug)]
pub struct BitcaskyClock {
    pub clock: Arc<DebugClock>,
}

#[cfg(not(test))]
#[derive(Debug)]
pub struct BitcaskyClock {
    pub clock: SystemClock,
}

impl Clock for BitcaskyClock {
    fn now(&self) -> u64 {
        self.clock.now()
    }
}

impl Deref for BitcaskyClock {
    #[cfg(not(test))]
    type Target = SystemClock;
    #[cfg(test)]
    type Target = DebugClock;

    fn deref(&self) -> &Self::Target {
        &self.clock
    }
}

impl Default for BitcaskyClock {
    fn default() -> Self {
        #[cfg(not(test))]
        return Self {
            clock: SystemClock {},
        };

        #[cfg(test)]
        Self {
            clock: Arc::new(DebugClock::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            )),
        }
    }
}
