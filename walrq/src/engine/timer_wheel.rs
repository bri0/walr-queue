use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct InFlightItem {
    pub id: Uuid,
    pub queue: String,
    pub receipt: String,
    pub expire_at: u64,
}

pub struct TimerWheel {
    slots: Vec<Vec<InFlightItem>>,
    size: usize,
    index_by_id: HashMap<Uuid, (usize, String)>, // id -> (slot_idx, receipt)
    last_checked_sec: u64,
}

impl TimerWheel {
    pub fn new(capacity_secs: usize) -> Self {
        let size = capacity_secs.max(60);
        Self {
            slots: vec![Vec::new(); size],
            size,
            index_by_id: HashMap::new(),
            last_checked_sec: 0,
        }
    }

    pub fn insert(&mut self, item: InFlightItem) {
        let slot = (item.expire_at as usize) % self.size;
        self.index_by_id.insert(item.id, (slot, item.receipt.clone()));
        self.slots[slot].push(item);
    }

    pub fn remove_if_valid(&mut self, id: &Uuid, receipt: &str) -> bool {
        if let Some(&(slot, ref valid_receipt)) = self.index_by_id.get(id) {
            if valid_receipt == receipt {
                self.index_by_id.remove(id);
                if let Some(pos) = self.slots[slot].iter().position(|it| it.id == *id) {
                    self.slots[slot].swap_remove(pos);
                }
                return true;
            }
        }
        false
    }

    pub fn collect_expired(&mut self, now_sec: u64) -> Vec<InFlightItem> {
        if self.index_by_id.is_empty() {
            self.last_checked_sec = now_sec;
            return Vec::new();
        }

        let mut expired = Vec::new();

        // If first check, initialize last_checked_sec
        if self.last_checked_sec == 0 {
            self.last_checked_sec = now_sec.saturating_sub(1);
        }

        // Only scan elapsed slot ticks between last_checked_sec and now_sec
        let elapsed_ticks = (now_sec.saturating_sub(self.last_checked_sec) as usize).min(self.size);

        if elapsed_ticks >= self.size {
            // Full sweep if elapsed time exceeded wheel capacity
            for slot in self.slots.iter_mut() {
                if slot.is_empty() {
                    continue;
                }
                let mut remaining = Vec::with_capacity(slot.len());
                for item in slot.drain(..) {
                    if item.expire_at <= now_sec {
                        self.index_by_id.remove(&item.id);
                        expired.push(item);
                    } else {
                        remaining.push(item);
                    }
                }
                *slot = remaining;
            }
        } else {
            // O(elapsed) targeted slot sweep instead of scanning all 3600 slots
            for t in 1..=elapsed_ticks {
                let sec_tick = self.last_checked_sec + t as u64;
                let slot_idx = (sec_tick as usize) % self.size;
                let slot = &mut self.slots[slot_idx];
                if slot.is_empty() {
                    continue;
                }

                let mut remaining = Vec::with_capacity(slot.len());
                for item in slot.drain(..) {
                    if item.expire_at <= now_sec {
                        self.index_by_id.remove(&item.id);
                        expired.push(item);
                    } else {
                        remaining.push(item);
                    }
                }
                *slot = remaining;
            }
        }

        self.last_checked_sec = now_sec;
        expired
    }
}
