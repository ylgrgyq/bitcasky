use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
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

impl DebugClock {
    pub fn new(time: u64) -> DebugClock {
        DebugClock { time }
    }
}

impl Clock for DebugClock {
    fn now(&self) -> u64 {
        self.time
    }
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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
