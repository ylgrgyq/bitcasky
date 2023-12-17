use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

pub trait Clock {
    fn now(&self) -> u64;
}

#[derive(Debug)]
pub struct SystemClock {}

impl Clock for SystemClock {
    fn now(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

#[derive(Debug)]
pub struct DebugClock {
    time: AtomicU64,
}

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

impl Clock for DebugClock {
    fn now(&self) -> u64 {
        self.time.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub enum ClockImpl {
    System(SystemClock),
    Debug(Arc<DebugClock>),
}

impl Clock for ClockImpl {
    fn now(&self) -> u64 {
        match self {
            ClockImpl::System(c) => c.now(),
            ClockImpl::Debug(c) => c.now(),
        }
    }
}

#[derive(Debug)]
pub struct BitcaskClock {
    pub clock: ClockImpl,
}

impl Clock for BitcaskClock {
    fn now(&self) -> u64 {
        self.clock.now()
    }
}

impl Deref for BitcaskClock {
    type Target = ClockImpl;

    fn deref(&self) -> &Self::Target {
        &self.clock
    }
}

impl DerefMut for BitcaskClock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.clock
    }
}

impl Default for BitcaskClock {
    fn default() -> Self {
        Self {
            clock: ClockImpl::System(SystemClock {}),
        }
    }
}
