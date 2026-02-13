use std::time::Duration;

use rand::RngExt;

pub struct BackoffConfig {
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub max_retries: u32,
    pub multiplier: f64,
    pub jitter: bool,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_delay_ms: 100,
            max_delay_ms: 30_000,
            max_retries: 5,
            multiplier: 2.0,
            jitter: true,
        }
    }
}

pub struct Backoff {
    config: BackoffConfig,
    attempt: u32,
}

impl Backoff {
    pub fn new(config: BackoffConfig) -> Self {
        Self { config, attempt: 0 }
    }

    pub fn next_delay(&mut self) -> Option<Duration> {
        if self.attempt >= self.config.max_retries {
            return None;
        }

        let base =
            self.config.initial_delay_ms as f64 * self.config.multiplier.powi(self.attempt as i32);
        let capped = base.min(self.config.max_delay_ms as f64);

        // "Full jitter": uniform(0, capped). This is what AWS recommends for
        // decorrelating competing clients, but it means any attempt can return 0ms.
        let delay_ms = if self.config.jitter {
            let jittered = rand::rng().random_range(0.0..=capped);
            jittered as u64
        } else {
            capped as u64
        };

        self.attempt += 1;
        Some(Duration::from_millis(delay_ms))
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = BackoffConfig::default();
        assert_eq!(cfg.initial_delay_ms, 100);
        assert_eq!(cfg.max_delay_ms, 30_000);
        assert_eq!(cfg.max_retries, 5);
        assert_eq!(cfg.multiplier, 2.0);
        assert!(cfg.jitter);
    }

    #[test]
    fn test_returns_none_when_exhausted() {
        let mut b = Backoff::new(BackoffConfig {
            max_retries: 2,
            ..BackoffConfig::default()
        });

        assert!(b.next_delay().is_some());
        assert!(b.next_delay().is_some());
        assert!(b.next_delay().is_none());
    }

    #[test]
    fn test_no_jitter_exponential_increase() {
        let mut b = Backoff::new(BackoffConfig {
            initial_delay_ms: 100,
            max_delay_ms: 100_000,
            max_retries: 4,
            multiplier: 2.0,
            jitter: false,
        });

        assert_eq!(b.next_delay().unwrap(), Duration::from_millis(100));
        assert_eq!(b.next_delay().unwrap(), Duration::from_millis(200));
        assert_eq!(b.next_delay().unwrap(), Duration::from_millis(400));
        assert_eq!(b.next_delay().unwrap(), Duration::from_millis(800));
        assert!(b.next_delay().is_none());
    }

    #[test]
    fn test_respects_max_delay() {
        let mut b = Backoff::new(BackoffConfig {
            initial_delay_ms: 1000,
            max_delay_ms: 2000,
            max_retries: 5,
            multiplier: 10.0,
            jitter: false,
        });

        assert_eq!(b.next_delay().unwrap(), Duration::from_millis(1000));
        assert_eq!(b.next_delay().unwrap(), Duration::from_millis(2000));
        assert_eq!(b.next_delay().unwrap(), Duration::from_millis(2000));
    }

    #[test]
    fn test_jitter_stays_within_bounds() {
        for _ in 0..100 {
            let mut b = Backoff::new(BackoffConfig {
                initial_delay_ms: 1000,
                max_delay_ms: 5000,
                max_retries: 1,
                multiplier: 2.0,
                jitter: true,
            });

            let delay = b.next_delay().unwrap();
            assert!(delay <= Duration::from_millis(1000));
        }
    }

    #[test]
    fn test_reset_restarts_sequence() {
        let mut b = Backoff::new(BackoffConfig {
            max_retries: 1,
            jitter: false,
            ..BackoffConfig::default()
        });

        assert!(b.next_delay().is_some());
        assert!(b.next_delay().is_none());

        b.reset();
        assert_eq!(b.attempt(), 0);
        assert!(b.next_delay().is_some());
    }

    #[test]
    fn test_attempt_tracks_calls() {
        let mut b = Backoff::new(BackoffConfig {
            max_retries: 3,
            ..BackoffConfig::default()
        });

        assert_eq!(b.attempt(), 0);
        b.next_delay();
        assert_eq!(b.attempt(), 1);
        b.next_delay();
        assert_eq!(b.attempt(), 2);
    }

    #[test]
    fn test_zero_retries_returns_none_immediately() {
        let mut b = Backoff::new(BackoffConfig {
            max_retries: 0,
            ..BackoffConfig::default()
        });

        assert!(b.next_delay().is_none());
    }

    #[test]
    fn test_multiplier_of_one_gives_constant_delay() {
        let mut b = Backoff::new(BackoffConfig {
            initial_delay_ms: 500,
            max_delay_ms: 100_000,
            max_retries: 3,
            multiplier: 1.0,
            jitter: false,
        });

        assert_eq!(b.next_delay().unwrap(), Duration::from_millis(500));
        assert_eq!(b.next_delay().unwrap(), Duration::from_millis(500));
        assert_eq!(b.next_delay().unwrap(), Duration::from_millis(500));
    }
}
