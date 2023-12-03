use std::{
    fmt::Debug,
    time::{SystemTime, UNIX_EPOCH},
};

pub trait Clock {
    fn now(&self) -> u64;
}

#[derive(Debug, Clone, Copy)]
pub struct SystemClock {}

impl Clock for SystemClock {
    fn now(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DebugClock {
    time: u64,
}

impl Clock for DebugClock {
    fn now(&self) -> u64 {
        self.time
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ClockImpl {
    System(SystemClock),
    Debug(DebugClock),
}

impl Clock for ClockImpl {
    fn now(&self) -> u64 {
        match self {
            ClockImpl::System(c) => c.now(),
            ClockImpl::Debug(c) => c.now(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BitcaskClock {
    pub clock: ClockImpl,
}

impl Clock for BitcaskClock {
    fn now(&self) -> u64 {
        self.clock.now()
    }
}

impl Default for BitcaskClock {
    fn default() -> Self {
        Self {
            clock: ClockImpl::System(SystemClock {}),
        }
    }
}
