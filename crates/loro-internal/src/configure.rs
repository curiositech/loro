use crate::sync::{Mutex, RwLock};
use loro_common::ContainerID;
use rustc_hash::FxHashSet;

pub use crate::container::richtext::config::{StyleConfig, StyleConfigMap};
use crate::LoroDoc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Configure {
    pub(crate) text_style_config: Arc<RwLock<StyleConfigMap>>,
    undo_binding_lock: Arc<parking_lot::ReentrantMutex<()>>,
    revision: Arc<AtomicU64>,
    record_timestamp: Arc<AtomicBool>,
    pub(crate) merge_interval_in_s: Arc<AtomicI64>,
    pub(crate) editable_detached_mode: Arc<AtomicBool>,
    pub(crate) deleted_root_containers: Arc<Mutex<FxHashSet<ContainerID>>>,
    pub(crate) hide_empty_root_containers: Arc<AtomicBool>,
}

impl LoroDoc {
    pub(crate) fn set_config(&self, config: &Configure) {
        // Take one coherent source snapshot before acquiring the destination
        // authority. This avoids both mixed-field snapshots and cross-config
        // lock nesting.
        let snapshot = config.fork();
        self.with_undo_binding_lock(|| {
            *self.config.text_style_config.write() = snapshot.text_style_config.read().clone();
            self.config.record_timestamp.store(
                snapshot.record_timestamp.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            self.config.merge_interval_in_s.store(
                snapshot.merge_interval_in_s.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            self.config.editable_detached_mode.store(
                snapshot.editable_detached_mode.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            *self.config.deleted_root_containers.lock() =
                snapshot.deleted_root_containers.lock().clone();
            self.config.hide_empty_root_containers.store(
                snapshot.hide_empty_root_containers.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            self.config.bump_revision();
        });
    }
}

impl Default for Configure {
    fn default() -> Self {
        Self {
            text_style_config: Arc::new(RwLock::new(StyleConfigMap::default_rich_text_config())),
            undo_binding_lock: Arc::new(parking_lot::ReentrantMutex::new(())),
            revision: Arc::new(AtomicU64::new(0)),
            record_timestamp: Arc::new(AtomicBool::new(false)),
            editable_detached_mode: Arc::new(AtomicBool::new(false)),
            merge_interval_in_s: Arc::new(AtomicI64::new(1000)),
            deleted_root_containers: Arc::new(Mutex::new(Default::default())),
            hide_empty_root_containers: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Configure {
    pub fn fork(&self) -> Self {
        let _guard = self.undo_binding_lock.lock();
        Self {
            text_style_config: Arc::new(RwLock::new(self.text_style_config.read().clone())),
            undo_binding_lock: Arc::new(parking_lot::ReentrantMutex::new(())),
            revision: Arc::new(AtomicU64::new(self.revision())),
            record_timestamp: Arc::new(AtomicBool::new(
                self.record_timestamp.load(Ordering::Relaxed),
            )),
            merge_interval_in_s: Arc::new(AtomicI64::new(
                self.merge_interval_in_s.load(Ordering::Relaxed),
            )),
            editable_detached_mode: Arc::new(AtomicBool::new(
                self.editable_detached_mode.load(Ordering::Relaxed),
            )),
            deleted_root_containers: Arc::new(Mutex::new(
                self.deleted_root_containers.lock().clone(),
            )),
            hide_empty_root_containers: Arc::new(AtomicBool::new(
                self.hide_empty_root_containers.load(Ordering::Relaxed),
            )),
        }
    }

    /// Return a read-only snapshot of the current rich-text style semantics.
    ///
    /// Mutate them with [`Self::set_text_style_config`] or
    /// [`Self::set_default_text_style`], which serialize with undo preview
    /// capture/application. The underlying writable lock is intentionally not
    /// exposed.
    pub fn text_style_config(&self) -> StyleConfigMap {
        let _guard = self.undo_binding_lock.lock();
        self.text_style_config.read().clone()
    }

    pub fn set_text_style_config(&self, config: StyleConfigMap) {
        let _guard = self.undo_binding_lock.lock();
        *self.text_style_config.write() = config;
        self.bump_revision();
    }

    pub fn set_default_text_style(&self, style: Option<StyleConfig>) {
        let _guard = self.undo_binding_lock.lock();
        self.text_style_config.write().default_style = style;
        self.bump_revision();
    }

    pub(crate) fn undo_binding_lock(&self) -> &Arc<parking_lot::ReentrantMutex<()>> {
        &self.undo_binding_lock
    }

    pub(crate) fn with_undo_binding_lock<R>(&self, f: impl FnOnce() -> R) -> R {
        let _guard = self.undo_binding_lock.lock();
        f()
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    fn bump_revision(&self) {
        self.revision
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |revision| {
                revision.checked_add(1)
            })
            .expect("configuration revision overflow");
    }

    pub(crate) fn mark_root_deleted(&self, cid: ContainerID) {
        let _guard = self.undo_binding_lock.lock();
        self.deleted_root_containers.lock().insert(cid);
        self.bump_revision();
    }

    pub(crate) fn unmark_root_deleted(&self, cid: &ContainerID) {
        let _guard = self.undo_binding_lock.lock();
        self.deleted_root_containers.lock().remove(cid);
        self.bump_revision();
    }

    pub fn record_timestamp(&self) -> bool {
        self.record_timestamp.load(Ordering::Relaxed)
    }

    pub fn set_record_timestamp(&self, record: bool) {
        let _guard = self.undo_binding_lock.lock();
        self.record_timestamp.store(record, Ordering::Relaxed);
        self.bump_revision();
    }

    pub fn detached_editing(&self) -> bool {
        self.editable_detached_mode.load(Ordering::Relaxed)
    }

    pub fn set_detached_editing(&self, mode: bool) {
        let _guard = self.undo_binding_lock.lock();
        self.editable_detached_mode.store(mode, Ordering::Relaxed);
        self.bump_revision();
    }

    pub fn merge_interval(&self) -> i64 {
        self.merge_interval_in_s.load(Ordering::Relaxed)
    }

    pub fn set_merge_interval(&self, interval: i64) {
        let _guard = self.undo_binding_lock.lock();
        self.merge_interval_in_s.store(interval, Ordering::Relaxed);
        self.bump_revision();
    }

    pub fn set_hide_empty_root_containers(&self, hide: bool) {
        let _guard = self.undo_binding_lock.lock();
        self.hide_empty_root_containers
            .store(hide, Ordering::Relaxed);
        self.bump_revision();
    }
}

#[derive(Debug)]
pub struct DefaultRandom;

#[cfg(test)]
static mut TEST_RANDOM: AtomicU64 = AtomicU64::new(0);

impl SecureRandomGenerator for DefaultRandom {
    fn fill_byte(&self, dest: &mut [u8]) {
        #[cfg(not(test))]
        getrandom::getrandom(dest).unwrap();

        #[cfg(test)]
        // SAFETY: this is only used in test
        unsafe {
            #[allow(static_mut_refs)]
            let bytes = TEST_RANDOM
                .fetch_add(1, std::sync::atomic::Ordering::Release)
                .to_le_bytes();
            dest.copy_from_slice(&bytes[..dest.len()]);
        }
    }
}

pub trait SecureRandomGenerator: Send + Sync {
    fn fill_byte(&self, dest: &mut [u8]);
    fn next_u64(&self) -> u64 {
        let mut buf = [0u8; 8];
        self.fill_byte(&mut buf);
        u64::from_le_bytes(buf)
    }

    fn next_u32(&self) -> u32 {
        let mut buf = [0u8; 4];
        self.fill_byte(&mut buf);
        u32::from_le_bytes(buf)
    }

    fn next_i64(&self) -> i64 {
        let mut buf = [0u8; 8];
        self.fill_byte(&mut buf);
        i64::from_le_bytes(buf)
    }

    fn next_i32(&self) -> i32 {
        let mut buf = [0u8; 4];
        self.fill_byte(&mut buf);
        i32::from_le_bytes(buf)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::Ordering, Mutex, OnceLock};

    use loro_common::{ContainerID, ContainerType, InternalString};

    use crate::container::richtext::ExpandType;

    use super::*;

    fn random_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn configure_default_values_and_setters_match_the_public_contract() {
        let config = Configure::default();

        assert!(!config.record_timestamp());
        assert!(!config.detached_editing());
        assert_eq!(config.merge_interval(), 1000);
        assert!(!config.hide_empty_root_containers.load(Ordering::Relaxed));
        assert!(config.deleted_root_containers.lock().is_empty());

        let styles = config.text_style_config.read();
        assert_eq!(
            styles.get(&InternalString::from("bold")),
            Some(StyleConfig {
                expand: ExpandType::After,
            })
        );
        assert_eq!(
            styles.get(&InternalString::from("italic")),
            Some(StyleConfig {
                expand: ExpandType::After,
            })
        );
        assert_eq!(
            styles.get(&InternalString::from("link")),
            Some(StyleConfig {
                expand: ExpandType::None,
            })
        );
        assert_eq!(styles.get(&InternalString::from("missing")), None);

        config.set_record_timestamp(true);
        config.set_detached_editing(true);
        config.set_merge_interval(42);
        config.set_hide_empty_root_containers(true);

        assert!(config.record_timestamp());
        assert!(config.detached_editing());
        assert_eq!(config.merge_interval(), 42);
        assert!(config.hide_empty_root_containers.load(Ordering::Relaxed));
    }

    #[test]
    fn configure_fork_copies_current_state_and_then_diverges() {
        let config = Configure::default();
        let shared = config.clone();
        let initial_revision = config.revision();
        config.set_record_timestamp(true);
        config.set_detached_editing(true);
        config.set_merge_interval(25);
        config.set_hide_empty_root_containers(true);
        let deleted_root = ContainerID::Root {
            name: InternalString::from("root"),
            container_type: ContainerType::Map,
        };
        config.mark_root_deleted(deleted_root.clone());
        let mut styles = config.text_style_config();
        styles.insert(
            InternalString::from("custom"),
            StyleConfig {
                expand: ExpandType::None,
            },
        );
        config.set_text_style_config(styles);

        assert!(config.revision() > initial_revision);
        assert_eq!(shared.revision(), config.revision());

        let forked = config.fork();
        let forked_revision = forked.revision();

        assert!(forked.record_timestamp());
        assert!(forked.detached_editing());
        assert_eq!(forked.merge_interval(), 25);
        assert!(forked.hide_empty_root_containers.load(Ordering::Relaxed));
        assert_eq!(forked.deleted_root_containers.lock().len(), 1);
        assert_eq!(
            forked
                .text_style_config
                .read()
                .get(&InternalString::from("custom")),
            Some(StyleConfig {
                expand: ExpandType::None,
            })
        );

        config.set_record_timestamp(false);
        config.set_detached_editing(false);
        config.set_merge_interval(99);
        config.set_hide_empty_root_containers(false);
        config.unmark_root_deleted(&deleted_root);
        let mut styles = config.text_style_config();
        styles.insert(InternalString::from("fork-only"), StyleConfig::default());
        config.set_text_style_config(styles);

        assert_eq!(forked.revision(), forked_revision);
        assert!(config.revision() > forked_revision);
        assert!(forked.record_timestamp());
        assert!(forked.detached_editing());
        assert_eq!(forked.merge_interval(), 25);
        assert!(forked.hide_empty_root_containers.load(Ordering::Relaxed));
        assert_eq!(forked.deleted_root_containers.lock().len(), 1);
        assert!(forked
            .text_style_config
            .read()
            .get(&InternalString::from("fork-only"))
            .is_none());
    }

    #[test]
    fn default_random_test_mode_uses_the_incrementing_counter_for_integer_helpers() {
        let _guard = random_lock().lock().unwrap();
        let random = DefaultRandom;

        let a = random.next_u64();
        let b = random.next_u32();
        let c = random.next_i64();
        let d = random.next_i32();

        assert_eq!(b as u64, (a + 1) as u32 as u64);
        assert_eq!(c as u64, a + 2);
        assert_eq!(d as u64, (a + 3) as u32 as u64);
    }
}
