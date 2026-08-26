use std::{
    cell::RefCell,
    collections::VecDeque,
    sync::{atomic::Ordering, Arc, Weak},
};

use crate::sync::{AtomicU64, Mutex};
use either::Either;
use loro_common::{
    ContainerID, Counter, CounterSpan, HasIdSpan, IdSpan, LoroError, LoroResult, LoroValue, PeerID,
};
use parking_lot::ReentrantMutex;
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::{debug_span, info_span, instrument};

use crate::{
    change::{get_sys_timestamp, Timestamp},
    configure::StyleConfigMap,
    cursor::{AbsolutePosition, Cursor},
    delta::TreeExternalDiff,
    event::{Diff, EventTriggerKind},
    loro::CommitOptions,
    version::{Frontiers, VersionVector},
    ContainerDiff, DiffEvent, DocDiff, LoroDoc, Subscription,
};

#[cfg(test)]
type BetweenUndoPopsHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type UndoTestHooks = std::sync::Mutex<std::collections::HashMap<usize, BetweenUndoPopsHook>>;

#[cfg(test)]
fn between_undo_pops_hook() -> &'static UndoTestHooks {
    static HOOK: std::sync::OnceLock<UndoTestHooks> = std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn run_between_undo_pops_hook(manager: usize) {
    let hook = { between_undo_pops_hook().lock().unwrap().remove(&manager) };
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn after_preview_validation_hook() -> &'static UndoTestHooks {
    static HOOK: std::sync::OnceLock<UndoTestHooks> = std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn run_after_preview_validation_hook(manager: usize) {
    let hook = {
        after_preview_validation_hook()
            .lock()
            .unwrap()
            .remove(&manager)
    };
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn after_preview_state_capture_hook() -> &'static UndoTestHooks {
    static HOOK: std::sync::OnceLock<UndoTestHooks> = std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn run_after_preview_state_capture_hook(manager: usize) {
    let hook = after_preview_state_capture_hook()
        .lock()
        .unwrap()
        .get(&manager)
        .cloned();
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn fail_after_stack_pop_target() -> &'static std::sync::Mutex<std::collections::HashSet<usize>> {
    static TARGET: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<usize>>> =
        std::sync::OnceLock::new();
    TARGET.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

#[cfg(test)]
fn take_fail_after_stack_pop(manager: usize) -> bool {
    fail_after_stack_pop_target()
        .lock()
        .unwrap()
        .remove(&manager)
}

#[cfg(test)]
fn after_undo_diff_applied_hook() -> &'static UndoTestHooks {
    static HOOK: std::sync::OnceLock<UndoTestHooks> = std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn run_after_undo_diff_applied_hook(manager: usize) {
    let hook = {
        after_undo_diff_applied_hook()
            .lock()
            .unwrap()
            .remove(&manager)
    };
    if let Some(hook) = hook {
        hook();
    }
}

/// A batch of diffs.
///
/// You can use `loroDoc.apply_diff(diff)` to apply the diff to the document.
#[derive(Debug, Clone, Default)]
pub struct DiffBatch {
    pub cid_to_events: FxHashMap<ContainerID, Diff>,
    pub order: Vec<ContainerID>,
}

impl DiffBatch {
    pub fn new(diff: Vec<DocDiff>) -> Self {
        let mut map: FxHashMap<ContainerID, Diff> = Default::default();
        let mut order: Vec<ContainerID> = Vec::with_capacity(diff.len());
        for d in diff.into_iter() {
            for item in d.diff.into_iter() {
                let old = map.insert(item.id.clone(), item.diff);
                assert!(old.is_none(), "Duplicate container ID in diff events");
                order.push(item.id.clone());
            }
        }

        Self {
            cid_to_events: map,
            order,
        }
    }

    pub fn compose(&mut self, other: &Self) {
        if other.cid_to_events.is_empty() {
            return;
        }

        for (id, diff) in other.iter() {
            if let Some(this_diff) = self.cid_to_events.get_mut(id) {
                this_diff.compose_ref(diff);
            } else {
                self.cid_to_events.insert(id.clone(), diff.clone());
                self.order.push(id.clone());
            }
        }
    }

    pub fn transform(&mut self, other: &Self, left_priority: bool) {
        if other.cid_to_events.is_empty() || self.cid_to_events.is_empty() {
            return;
        }

        for (idx, diff) in self.cid_to_events.iter_mut() {
            if let Some(b_diff) = other.cid_to_events.get(idx) {
                diff.transform(b_diff, left_priority);
            }
        }
    }

    pub fn clear(&mut self) {
        self.cid_to_events.clear();
        self.order.clear();
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ContainerID, &Diff)> + '_ {
        self.order
            .iter()
            .map(|cid| (cid, self.cid_to_events.get(cid).unwrap()))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn into_iter(self) -> impl Iterator<Item = (ContainerID, Diff)> {
        let mut cid_to_events = self.cid_to_events;
        self.order.into_iter().map(move |cid| {
            let d = cid_to_events.remove(&cid).unwrap();
            (cid, d)
        })
    }
}

fn transform_cursor(
    cursor_with_pos: &mut CursorWithPos,
    remote_diff: &DiffBatch,
    doc: &LoroDoc,
    container_remap: &FxHashMap<ContainerID, ContainerID>,
) {
    let mut container_changed = false;
    let mut cid = &cursor_with_pos.cursor.container;
    while let Some(new_cid) = container_remap.get(cid) {
        cid = new_cid;
        container_changed = true;
    }

    if cursor_with_pos.cursor.id.is_none() {
        // We don't need to transform a cursor that always points to the leftmost or rightmost position
        if container_changed {
            cursor_with_pos.cursor.container = cid.clone();
        }
        return;
    }

    if let Some(diff) = remote_diff.cid_to_events.get(cid) {
        let new_pos = diff.transform_cursor(cursor_with_pos.pos.pos, false);
        cursor_with_pos.pos.pos = new_pos;
    };

    let new_pos = cursor_with_pos.pos.pos;
    match doc.get_handler(cid.clone()).unwrap() {
        crate::handler::Handler::Text(h) => {
            let Some(new_cursor) = h.get_cursor_internal(new_pos, cursor_with_pos.pos.side, false)
            else {
                return;
            };

            cursor_with_pos.cursor = new_cursor;
        }
        crate::handler::Handler::List(h) => {
            let Some(new_cursor) = h.get_cursor(new_pos, cursor_with_pos.pos.side) else {
                return;
            };

            cursor_with_pos.cursor = new_cursor;
        }
        crate::handler::Handler::MovableList(h) => {
            let Some(new_cursor) = h.get_cursor(new_pos, cursor_with_pos.pos.side) else {
                return;
            };

            cursor_with_pos.cursor = new_cursor;
        }
        crate::handler::Handler::Map(_) => {}
        crate::handler::Handler::Tree(_) => {}
        crate::handler::Handler::Unknown(_) => {}
        #[cfg(feature = "counter")]
        crate::handler::Handler::Counter(_) => {}
    }
}

/// UndoManager is responsible for managing undo/redo from the current peer's perspective.
///
/// Undo/local is local: it cannot be used to undone the changes made by other peers.
/// If you want to undo changes made by other peers, you may need to use the time travel feature.
///
/// PeerID cannot be changed during the lifetime of the UndoManager
pub struct UndoManager {
    peer: Arc<AtomicU64>,
    container_remap: Arc<Mutex<FxHashMap<ContainerID, ContainerID>>>,
    /// Serializes manager operations without being acquired by document reads.
    /// Document-driven callbacks take locks in document -> operation -> inner
    /// order, so preview/apply can retain operation authority while releasing
    /// `inner` before touching document state.
    operation_lock: Arc<ReentrantMutex<()>>,
    inner: Arc<parking_lot::ReentrantMutex<RefCell<UndoManagerInner>>>,
    generation: Arc<AtomicU64>,
    _peer_id_change_sub: Subscription,
    _undo_sub: Subscription,
    doc: LoroDoc,
}

impl std::fmt::Debug for UndoManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UndoManager")
            .field("peer", &self.peer)
            .field("container_remap", &self.container_remap)
            .field("inner", &self.inner)
            .field("generation", &self.generation.load(Ordering::Relaxed))
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndoOrRedo {
    Undo,
    Redo,
}

impl UndoOrRedo {
    fn opposite(&self) -> UndoOrRedo {
        match self {
            Self::Undo => Self::Redo,
            Self::Redo => Self::Undo,
        }
    }
}

/// A read-only preview of the next undo or redo operation.
///
/// This value is also an opaque, state-bound token. Pass it back to
/// [`UndoManager::apply_preview`] with the same action. It cannot be used with a
/// different manager, and any intervening document, history, configuration,
/// peer, stack, or remapping change invalidates it.
///
/// Creating a preview performs the same implicit checkpoint as ordinary
/// undo/redo, then deep-forks the document and undo state. The preview itself
/// does not apply an undo or redo to the source document. Its time and space
/// complexity are O(n), where n includes the document history/materialized
/// state plus the undo/redo stacks and their accumulated remote diffs.
pub struct UndoPreview {
    token: UndoPreviewToken,
    meta: UndoItemMeta,
    pop_count: usize,
    peer_counter_advance: Counter,
    will_apply: bool,
}

impl std::fmt::Debug for UndoPreview {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UndoPreview")
            .field("action", &self.token.action)
            .field("meta", &self.meta)
            .field("pop_count", &self.pop_count)
            .field("peer_counter_advance", &self.peer_counter_advance)
            .field("will_apply", &self.will_apply)
            .finish_non_exhaustive()
    }
}

impl UndoPreview {
    /// The undo/redo action this preview represents.
    pub fn action(&self) -> UndoOrRedo {
        self.token.action
    }

    /// Metadata of the effective item.
    ///
    /// For a neutralized prefix this is the first later item that changes the
    /// document. If [`Self::will_apply`] is false, there is no effective item;
    /// this instead reports the final neutralized pop for diagnostics and must
    /// not be treated as an item to validate or apply.
    pub fn meta(&self) -> &UndoItemMeta {
        &self.meta
    }

    /// The exact number of stack items the operation will pop.
    pub fn pop_count(&self) -> usize {
        self.pop_count
    }

    /// The exact advancement of the bound peer's counter.
    pub fn peer_counter_advance(&self) -> Counter {
        self.peer_counter_advance
    }

    /// Whether applying this preview will create undo/redo operations.
    ///
    /// This is false when all candidate stack items have been neutralized by
    /// later changes. Applying such a preview still consumes those items.
    pub fn will_apply(&self) -> bool {
        self.will_apply
    }
}

struct UndoPreviewToken {
    manager: Weak<parking_lot::ReentrantMutex<RefCell<UndoManagerInner>>>,
    action: UndoOrRedo,
    binding: UndoPreviewBinding,
}

/// When a undo/redo item is pushed, the undo manager will call the on_push callback to get the meta data of the undo item.
/// The returned cursors will be recorded for a new pushed undo item.
pub type OnPush = Box<
    dyn for<'a> Fn(UndoOrRedo, CounterSpan, Option<DiffEvent<'a>>) -> UndoItemMeta + Send + Sync,
>;
pub type OnPop = Box<dyn Fn(UndoOrRedo, CounterSpan, UndoItemMeta) + Send + Sync>;

struct UndoManagerInner {
    next_counter: Option<Counter>,
    undo_stack: Stack,
    redo_stack: Stack,
    processing_undo: bool,
    last_undo_time: i64,
    merge_interval_in_ms: i64,
    max_stack_size: usize,
    exclude_origin_prefixes: Vec<Box<str>>,
    last_popped_selection: Option<Vec<CursorWithPos>>,
    on_push: Option<OnPush>,
    on_pop: Option<OnPop>,
    group: Option<UndoGroup>,
}

impl std::fmt::Debug for UndoManagerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UndoManagerInner")
            .field("latest_counter", &self.next_counter)
            .field("undo_stack", &self.undo_stack)
            .field("redo_stack", &self.redo_stack)
            .field("processing_undo", &self.processing_undo)
            .field("last_undo_time", &self.last_undo_time)
            .field("merge_interval", &self.merge_interval_in_ms)
            .field("max_stack_size", &self.max_stack_size)
            .field("exclude_origin_prefixes", &self.exclude_origin_prefixes)
            .field("group", &self.group)
            .finish()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct UndoGroup {
    start_counter: Counter,
    affected_cids: FxHashSet<ContainerID>,
}

impl UndoGroup {
    pub fn new(start_counter: Counter) -> Self {
        Self {
            start_counter,
            affected_cids: Default::default(),
        }
    }
}

#[derive(Debug)]
struct Stack {
    stack: VecDeque<(VecDeque<StackItem>, Arc<Mutex<DiffBatch>>)>,
    size: usize,
    revision: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct StackItem {
    span: CounterSpan,
    meta: UndoItemMeta,
}

/// The metadata of an undo item.
///
/// The cursors inside the metadata will be transformed by remote operations as well.
/// So that when the item is popped, users can restore the cursors position correctly.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct UndoItemMeta {
    pub value: LoroValue,
    pub cursors: Vec<CursorWithPos>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorWithPos {
    pub cursor: Cursor,
    pub pos: AbsolutePosition,
}

#[derive(Clone, PartialEq, Eq)]
struct DocConfigSnapshot {
    revision: u64,
    text_styles: StyleConfigMap,
    record_timestamp: bool,
    merge_interval: i64,
    detached_editing: bool,
    deleted_root_containers: FxHashSet<ContainerID>,
    hide_empty_root_containers: bool,
}

impl DocConfigSnapshot {
    fn capture(doc: &LoroDoc) -> Self {
        // `Configure::fork` holds the source authority while cloning every
        // field, so this cannot combine values from different revisions.
        let config = doc.config().fork();
        let deleted_root_containers = config.deleted_root_containers.lock().clone();
        Self {
            revision: config.revision(),
            text_styles: config.text_style_config(),
            record_timestamp: config.record_timestamp(),
            merge_interval: config.merge_interval(),
            detached_editing: config.detached_editing(),
            deleted_root_containers,
            hide_empty_root_containers: config.hide_empty_root_containers.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct DocPreviewBinding {
    oplog_vv: VersionVector,
    state_vv: VersionVector,
    oplog_frontiers: Frontiers,
    state_frontiers: Frontiers,
    peer: PeerID,
    peer_counter: Counter,
    can_edit: bool,
    is_detached: bool,
    auto_commit: bool,
    config: DocConfigSnapshot,
}

impl DocPreviewBinding {
    fn capture(doc: &LoroDoc, peer: PeerID) -> Self {
        Self {
            oplog_vv: doc.oplog_vv(),
            state_vv: doc.state_vv(),
            oplog_frontiers: doc.oplog_frontiers(),
            state_frontiers: doc.state_frontiers(),
            peer: doc.peer_id(),
            peer_counter: get_counter_end(doc, peer),
            can_edit: doc.can_edit(),
            is_detached: doc.is_detached(),
            auto_commit: doc.auto_commit_enabled(),
            config: DocConfigSnapshot::capture(doc),
        }
    }

    fn matches(&self, doc: &LoroDoc, peer: PeerID) -> bool {
        // Check frontiers first. A new uncommitted local transaction may have
        // state frontiers that the oplog cannot yet convert to a VersionVector.
        if self.state_frontiers != doc.state_frontiers()
            || self.oplog_frontiers != doc.oplog_frontiers()
        {
            return false;
        }

        Self::capture(doc, peer) == *self
    }
}

#[derive(Clone, PartialEq)]
struct UndoManagerPreviewBinding {
    generation: u64,
    peer: PeerID,
    next_counter: Option<Counter>,
    undo_revision: u64,
    undo_count: usize,
    redo_revision: u64,
    redo_count: usize,
    processing_undo: bool,
    last_undo_time: i64,
    merge_interval_in_ms: i64,
    max_stack_size: usize,
    exclude_origin_prefixes: Vec<Box<str>>,
    last_popped_selection: Option<Vec<CursorWithPos>>,
    has_on_push: bool,
    has_on_pop: bool,
    group: Option<UndoGroup>,
    container_remap: FxHashMap<ContainerID, ContainerID>,
}

#[derive(Clone, PartialEq)]
struct UndoPreviewBinding {
    doc: DocPreviewBinding,
    manager: UndoManagerPreviewBinding,
}

#[derive(Debug, PartialEq)]
struct PerformOutcome {
    executed: bool,
    effective_meta: Option<UndoItemMeta>,
    pop_count: usize,
    peer_counter_advance: Counter,
}

struct ProcessingUndoGuard {
    inner: Arc<parking_lot::ReentrantMutex<RefCell<UndoManagerInner>>>,
}

struct UndoManagerStateRollback {
    inner: Arc<parking_lot::ReentrantMutex<RefCell<UndoManagerInner>>>,
    container_remap: Arc<Mutex<FxHashMap<ContainerID, ContainerID>>>,
    generation: Arc<AtomicU64>,
    inner_snapshot: Option<UndoManagerInner>,
    remap_snapshot: Option<FxHashMap<ContainerID, ContainerID>>,
    generation_snapshot: u64,
}

impl UndoManagerStateRollback {
    fn capture(manager: &UndoManager) -> Self {
        let inner_snapshot = manager.inner.lock().borrow().deep_clone_for_preview();
        let remap_snapshot = manager.container_remap.lock().clone();
        Self {
            inner: manager.inner.clone(),
            container_remap: manager.container_remap.clone(),
            generation: manager.generation.clone(),
            inner_snapshot: Some(inner_snapshot),
            remap_snapshot: Some(remap_snapshot),
            generation_snapshot: manager.generation.load(Ordering::Relaxed),
        }
    }

    fn disarm(&mut self) {
        self.inner_snapshot = None;
        self.remap_snapshot = None;
    }
}

impl Drop for UndoManagerStateRollback {
    fn drop(&mut self) {
        let Some(inner) = self.inner_snapshot.take() else {
            return;
        };
        *self.inner.lock().borrow_mut() = inner;
        *self.container_remap.lock() = self
            .remap_snapshot
            .take()
            .expect("armed undo rollback requires a remap snapshot");
        self.generation
            .store(self.generation_snapshot, Ordering::Relaxed);
    }
}

impl ProcessingUndoGuard {
    fn start(inner: &Arc<parking_lot::ReentrantMutex<RefCell<UndoManagerInner>>>) -> Self {
        inner.lock().borrow_mut().processing_undo = true;
        Self {
            inner: inner.clone(),
        }
    }
}

impl Drop for ProcessingUndoGuard {
    fn drop(&mut self) {
        self.inner.lock().borrow_mut().processing_undo = false;
    }
}

fn stack_for(inner: &mut UndoManagerInner, kind: UndoOrRedo) -> &mut Stack {
    match kind {
        UndoOrRedo::Undo => &mut inner.undo_stack,
        UndoOrRedo::Redo => &mut inner.redo_stack,
    }
}

fn opposite_stack_for(inner: &mut UndoManagerInner, kind: UndoOrRedo) -> &mut Stack {
    stack_for(inner, kind.opposite())
}

impl UndoItemMeta {
    pub fn new() -> Self {
        Self {
            value: LoroValue::Null,
            cursors: Default::default(),
        }
    }

    /// It's assumed that the cursor is just acquired before the ops that
    /// need to be undo/redo.
    ///
    /// We need to rely on the validity of the original pos value
    pub fn add_cursor(&mut self, cursor: &Cursor) {
        self.cursors.push(CursorWithPos {
            cursor: cursor.clone(),
            pos: AbsolutePosition {
                pos: cursor.origin_pos,
                side: cursor.side,
            },
        });
    }

    pub fn set_value(&mut self, value: LoroValue) {
        self.value = value;
    }
}

impl Stack {
    pub fn new() -> Self {
        let mut stack = VecDeque::new();
        stack.push_back((VecDeque::new(), Arc::new(Mutex::new(Default::default()))));
        Stack {
            stack,
            size: 0,
            revision: 0,
        }
    }

    fn deep_clone(&self) -> Self {
        Self {
            stack: self
                .stack
                .iter()
                .map(|(items, remote_diff)| {
                    (
                        items.clone(),
                        Arc::new(Mutex::new(remote_diff.lock().clone())),
                    )
                })
                .collect(),
            size: self.size,
            revision: self.revision,
        }
    }

    fn bump_revision(&mut self) {
        self.revision = self.revision.wrapping_add(1);
    }

    /// Peek the top-most StackItem's metadata without modifying the stack.
    ///
    /// Returns None if the stack is empty.
    fn peek_top_meta(&self) -> Option<UndoItemMeta> {
        if self.is_empty() {
            return None;
        }

        for (items, _) in self.stack.iter().rev() {
            if let Some(item) = items.back() {
                return Some(item.meta.clone());
            }
        }

        None
    }

    pub fn pop(&mut self) -> Option<(StackItem, Arc<Mutex<DiffBatch>>)> {
        self.bump_revision();
        while self.stack.back().unwrap().0.is_empty() && self.stack.len() > 1 {
            let (_, diff) = self.stack.pop_back().unwrap();
            let diff = diff.lock();
            if !diff.cid_to_events.is_empty() {
                self.stack.back_mut().unwrap().1.lock().compose(&diff);
            }
        }

        if self.stack.len() == 1 && self.stack.back().unwrap().0.is_empty() {
            // If the stack is empty, we need to clear the remote diff
            self.stack.back_mut().unwrap().1.lock().clear();
            return None;
        }

        self.size -= 1;
        let last = self.stack.back_mut().unwrap();
        last.0.pop_back().map(|x| (x, last.1.clone()))
        // If this row in stack is empty, we don't pop it right away
        // Because we still need the remote diff to be available.
        // Cursor position transformation relies on the remote diff in the same row.
    }

    pub fn push(&mut self, span: CounterSpan, meta: UndoItemMeta) {
        self.push_with_merge(span, meta, false, None)
    }

    pub fn push_with_merge(
        &mut self,
        span: CounterSpan,
        meta: UndoItemMeta,
        can_merge: bool,
        group: Option<&UndoGroup>,
    ) {
        self.bump_revision();
        let last = self.stack.back_mut().unwrap();
        let last_remote_diff = last.1.lock();

        // Check if the remote diff is disjoint with the current undo group
        let is_disjoint_group = group.is_some_and(|g| {
            g.affected_cids.iter().all(|cid| {
                last_remote_diff
                    .cid_to_events
                    .get(cid)
                    .is_none_or(|diff| diff.is_empty())
            })
        });

        // Can't merge if remote diffs exist and it's not disjoint with the current undo group
        let should_create_new_entry =
            !last_remote_diff.cid_to_events.is_empty() && !is_disjoint_group;

        if should_create_new_entry {
            // Create a new entry in the stack
            drop(last_remote_diff);
            let mut v = VecDeque::new();
            v.push_back(StackItem { span, meta });
            self.stack
                .push_back((v, Arc::new(Mutex::new(DiffBatch::default()))));
            self.size += 1;
            return;
        }

        // Try to merge with the previous entry if allowed
        if can_merge {
            if let Some(last_span) = last.0.back_mut() {
                if last_span.span.end == span.start {
                    // Merge spans by extending the end of the last span
                    last_span.span.end = span.end;
                    return;
                }
            }
        }

        // Add as a new item to the existing entry
        self.size += 1;
        last.0.push_back(StackItem { span, meta });
    }

    pub fn compose_remote_event(&mut self, diff: &[&ContainerDiff]) {
        if self.is_empty() {
            return;
        }

        self.bump_revision();

        let remote_diff = &mut self.stack.back_mut().unwrap().1;
        let mut remote_diff = remote_diff.lock();
        for e in diff {
            if let Some(d) = remote_diff.cid_to_events.get_mut(&e.id) {
                d.compose_ref(&e.diff);
            } else {
                remote_diff
                    .cid_to_events
                    .insert(e.id.clone(), e.diff.clone());
                remote_diff.order.push(e.id.clone());
            }
        }
    }

    pub fn transform_based_on_this_delta(&mut self, diff: &DiffBatch) {
        if self.is_empty() {
            return;
        }
        self.bump_revision();
        let remote_diff = &mut self.stack.back_mut().unwrap().1;
        remote_diff.lock().transform(diff, false);
    }

    pub fn clear(&mut self) {
        let revision = self.revision.wrapping_add(1);
        self.stack = VecDeque::new();
        self.stack.push_back((VecDeque::new(), Default::default()));
        self.size = 0;
        self.revision = revision;
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn len(&self) -> usize {
        self.size
    }

    fn discard_empty_front_rows(&mut self) {
        // Undo pop can leave an empty front row that only carries remote diffs.
        // There is no older stack item for that diff to transform during trimming.
        while self
            .stack
            .front()
            .is_some_and(|(items, _)| items.is_empty())
        {
            self.stack.pop_front();
        }
    }

    fn ensure_trailing_empty_row(&mut self) {
        if self.stack.is_empty() {
            self.stack
                .push_back((VecDeque::new(), Arc::new(Mutex::new(Default::default()))));
        }
    }

    fn pop_front(&mut self) {
        if self.is_empty() {
            return;
        }

        self.bump_revision();

        self.discard_empty_front_rows();
        self.size -= 1;
        let first = self.stack.front_mut().unwrap();
        let f = first.0.pop_front();
        assert!(f.is_some());
        if first.0.is_empty() {
            self.stack.pop_front();
        }

        self.ensure_trailing_empty_row();
    }

    fn set_top_meta(&mut self, meta: UndoItemMeta) {
        self.bump_revision();
        let Some(top) = self.stack.back_mut() else {
            return;
        };
        let Some(last) = top.0.back_mut() else {
            return;
        };
        last.meta = meta;
    }
}

impl Default for Stack {
    fn default() -> Self {
        Stack::new()
    }
}

impl UndoManagerInner {
    fn new(last_counter: Counter) -> Self {
        Self {
            next_counter: Some(last_counter),
            undo_stack: Default::default(),
            redo_stack: Default::default(),
            processing_undo: false,
            merge_interval_in_ms: 0,
            last_undo_time: 0,
            max_stack_size: usize::MAX,
            exclude_origin_prefixes: vec![],
            last_popped_selection: None,
            on_pop: None,
            on_push: None,
            group: None,
        }
    }

    fn deep_clone_for_preview(&self) -> Self {
        Self {
            next_counter: self.next_counter,
            undo_stack: self.undo_stack.deep_clone(),
            redo_stack: self.redo_stack.deep_clone(),
            processing_undo: false,
            last_undo_time: self.last_undo_time,
            merge_interval_in_ms: self.merge_interval_in_ms,
            max_stack_size: self.max_stack_size,
            exclude_origin_prefixes: self.exclude_origin_prefixes.clone(),
            last_popped_selection: self.last_popped_selection.clone(),
            // Preview must not invoke externally observable callbacks. The
            // already-recorded item metadata is cloned with the stacks.
            on_push: None,
            on_pop: None,
            group: self.group.clone(),
        }
    }

    /// Returns true if a given container diff is disjoint with the current group.
    /// They are disjoint if they have no overlap in changed container ids.
    fn is_disjoint_with_group(&self, diff: &[&ContainerDiff]) -> bool {
        let Some(group) = &self.group else {
            return false;
        };

        diff.iter().all(|d| !group.affected_cids.contains(&d.id))
    }

    fn record_checkpoint(this: &RefCell<Self>, latest_counter: Counter, event: Option<DiffEvent>) {
        let previous_counter = this.borrow().next_counter;

        if Some(latest_counter) == this.borrow().next_counter {
            return;
        }

        if this.borrow().next_counter.is_none() {
            this.borrow_mut().next_counter = Some(latest_counter);
            return;
        }

        if let Some(group) = &mut this.borrow_mut().group {
            event.iter().for_each(|e| {
                e.events.iter().for_each(|e| {
                    group.affected_cids.insert(e.id.clone());
                })
            });
        }

        let now = get_sys_timestamp() as Timestamp;
        let span = CounterSpan::new(this.borrow().next_counter.unwrap(), latest_counter);
        let meta = this
            .borrow()
            .on_push
            .as_ref()
            .map(|x| x(UndoOrRedo::Undo, span, event))
            .unwrap_or_default();

        let mut this = this.borrow_mut();
        let this: &mut Self = &mut this;
        // Wether the change is within the accepted merge interval
        let in_merge_interval = now - this.last_undo_time < this.merge_interval_in_ms;

        // If group is active, but there is nothing in the group, don't merge
        // If the group is active and it's not the first push in the group, merge
        let group_should_merge = this.group.is_some()
            && match (
                previous_counter,
                this.group.as_ref().map(|g| g.start_counter),
            ) {
                (Some(previous), Some(active)) => previous != active,
                _ => true,
            };

        let should_merge = !this.undo_stack.is_empty() && (in_merge_interval || group_should_merge);

        if should_merge {
            this.undo_stack
                .push_with_merge(span, meta, true, this.group.as_ref());
        } else {
            this.last_undo_time = now;
            this.undo_stack.push(span, meta);
        }

        this.next_counter = Some(latest_counter);
        this.redo_stack.clear();
        while this.undo_stack.len() > this.max_stack_size {
            this.undo_stack.pop_front();
        }
    }
}

fn get_counter_end(doc: &LoroDoc, peer: PeerID) -> Counter {
    doc.oplog().lock().vv().get(&peer).cloned().unwrap_or(0)
}

impl UndoManager {
    fn capture_manager_binding(&self, inner: &UndoManagerInner) -> UndoManagerPreviewBinding {
        UndoManagerPreviewBinding {
            generation: self.generation.load(Ordering::Relaxed),
            peer: self.peer(),
            next_counter: inner.next_counter,
            undo_revision: inner.undo_stack.revision,
            undo_count: inner.undo_stack.len(),
            redo_revision: inner.redo_stack.revision,
            redo_count: inner.redo_stack.len(),
            processing_undo: inner.processing_undo,
            last_undo_time: inner.last_undo_time,
            merge_interval_in_ms: inner.merge_interval_in_ms,
            max_stack_size: inner.max_stack_size,
            exclude_origin_prefixes: inner.exclude_origin_prefixes.clone(),
            last_popped_selection: inner.last_popped_selection.clone(),
            has_on_push: inner.on_push.is_some(),
            has_on_pop: inner.on_pop.is_some(),
            group: inner.group.clone(),
            container_remap: self.container_remap.lock().clone(),
        }
    }

    fn capture_binding_doc_first(&self) -> LoroResult<UndoPreviewBinding> {
        self.doc.with_undo_barrier(|options, _release| {
            let txn = self.start_barrier_transaction(options)?;
            let peer = self.peer();
            let doc = DocPreviewBinding::capture(&self.doc, peer);
            let _operation_guard = self.operation_lock.lock();
            let lock = self.inner.lock();
            let manager = self.capture_manager_binding(&lock.borrow());
            drop(lock);
            *options = txn.commit()?;
            Ok(UndoPreviewBinding { doc, manager })
        })
    }

    fn capture_preview_state(
        &self,
    ) -> LoroResult<(
        UndoPreviewBinding,
        UndoManagerInner,
        FxHashMap<ContainerID, ContainerID>,
    )> {
        self.doc.with_undo_barrier(|options, _release| {
            let txn = self.start_barrier_transaction(options)?;
            let peer = self.peer();
            let doc = DocPreviewBinding::capture(&self.doc, peer);
            let _operation_guard = self.operation_lock.lock();
            let lock = self.inner.lock();
            let inner = lock.borrow();
            let manager = self.capture_manager_binding(&inner);
            let preview_inner = inner.deep_clone_for_preview();
            let preview_remap = self.container_remap.lock().clone();
            drop(inner);
            drop(lock);
            *options = txn.commit()?;
            Ok((
                UndoPreviewBinding { doc, manager },
                preview_inner,
                preview_remap,
            ))
        })
    }

    pub fn new(doc: &LoroDoc) -> Self {
        let peer = Arc::new(AtomicU64::new(doc.peer_id()));
        let peer_clone = peer.clone();
        let peer_clone2 = peer.clone();
        let inner = Arc::new(ReentrantMutex::new(RefCell::new(UndoManagerInner::new(
            get_counter_end(doc, doc.peer_id()),
        ))));
        let inner_clone = inner.clone();
        let inner_clone2 = inner.clone();
        let operation_lock = Arc::new(ReentrantMutex::new(()));
        let event_operation_lock = operation_lock.clone();
        let peer_operation_lock = operation_lock.clone();
        let remap_containers = Arc::new(Mutex::new(FxHashMap::default()));
        let remap_containers_clone = remap_containers.clone();
        let generation = Arc::new(AtomicU64::new(0));
        let event_generation = generation.clone();
        let undo_sub = doc.subscribe_root(Arc::new(move |event| {
            let _operation_guard = event_operation_lock.lock();
            match event.event_meta.by {
                EventTriggerKind::Local => {
                    // TODO: PERF undo can be significantly faster if we can get
                    // the DiffBatch for undo here
                    let lock = inner_clone.lock();
                    if lock.borrow().processing_undo {
                        return;
                    }
                    if let Some(id) =
                        event.event_meta.to.iter().find(|x| {
                            x.peer == peer_clone.load(std::sync::atomic::Ordering::Relaxed)
                        })
                    {
                        let should_exclude = lock
                            .borrow()
                            .exclude_origin_prefixes
                            .iter()
                            .any(|x| event.event_meta.origin.starts_with(&**x));
                        if should_exclude {
                            // If the event is from the excluded origin, we don't record it
                            // in the undo stack. But we need to record its effect like it's
                            // a remote event.
                            let mut inner = lock.borrow_mut();
                            inner.undo_stack.compose_remote_event(event.events);
                            inner.redo_stack.compose_remote_event(event.events);
                            inner.next_counter = Some(id.counter + 1);
                        } else {
                            UndoManagerInner::record_checkpoint(&lock, id.counter + 1, Some(event));
                        }
                        event_generation.fetch_add(1, Ordering::Relaxed);
                    }
                }
                EventTriggerKind::Import => {
                    let lock = inner_clone.lock();
                    let mut inner = lock.borrow_mut();

                    for e in event.events {
                        if let Diff::Tree(tree) = &e.diff {
                            for item in &tree.diff {
                                let target = item.target;
                                if let TreeExternalDiff::Create { .. } = &item.action {
                                    // If the concurrent event is a create event, it may bring the deleted tree node back,
                                    // so we need to remove it from the remap of the container.
                                    remap_containers_clone
                                        .lock()
                                        .remove(&target.associated_meta_container());
                                }
                            }
                        }
                    }

                    let is_import_disjoint = inner.is_disjoint_with_group(event.events);

                    inner.undo_stack.compose_remote_event(event.events);
                    inner.redo_stack.compose_remote_event(event.events);

                    // If the import is not disjoint, we end the active group
                    // all subsequent changes will be new undo items
                    if !is_import_disjoint {
                        inner.group = None;
                    }
                    event_generation.fetch_add(1, Ordering::Relaxed);
                }
                EventTriggerKind::Checkout => {
                    let lock = inner_clone.lock();
                    let mut inner = lock.borrow_mut();
                    inner.undo_stack.clear();
                    inner.redo_stack.clear();
                    inner.next_counter = None;
                    event_generation.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));

        let peer_generation = generation.clone();
        let sub = doc.subscribe_peer_id_change(Box::new(move |id| {
            let _operation_guard = peer_operation_lock.lock();
            let lock = inner_clone2.lock();
            let mut inner = lock.borrow_mut();
            inner.undo_stack.clear();
            inner.redo_stack.clear();
            inner.next_counter = Some(id.counter);
            peer_clone2.store(id.peer, std::sync::atomic::Ordering::Relaxed);
            peer_generation.fetch_add(1, Ordering::Relaxed);
            true
        }));

        UndoManager {
            peer,
            container_remap: remap_containers,
            operation_lock,
            inner,
            generation,
            _peer_id_change_sub: sub,
            _undo_sub: undo_sub,
            doc: doc.clone(),
        }
    }

    pub fn group_start(&self) -> LoroResult<()> {
        let _operation_guard = self.operation_lock.lock();
        let lock = self.inner.lock();
        let mut inner = lock.borrow_mut();

        if inner.group.is_some() {
            return Err(LoroError::UndoGroupAlreadyStarted);
        }

        inner.group =
            Some(UndoGroup::new(inner.next_counter.ok_or_else(|| {
                LoroError::Unknown("UndoManager is not ready".into())
            })?));

        self.generation.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    pub fn group_end(&self) {
        let _operation_guard = self.operation_lock.lock();
        self.inner.lock().borrow_mut().group = None;
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    pub fn peer(&self) -> PeerID {
        self.peer.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_merge_interval(&self, interval: i64) {
        let _operation_guard = self.operation_lock.lock();
        self.inner.lock().borrow_mut().merge_interval_in_ms = interval;
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_max_undo_steps(&self, size: usize) {
        let _operation_guard = self.operation_lock.lock();
        self.inner.lock().borrow_mut().max_stack_size = size;
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_exclude_origin_prefix(&self, prefix: &str) {
        let _operation_guard = self.operation_lock.lock();
        self.inner
            .lock()
            .borrow_mut()
            .exclude_origin_prefixes
            .push(prefix.into());
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_new_checkpoint(&self) -> LoroResult<()> {
        self.doc.with_undo_barrier(|options, _release| {
            let txn = self.start_barrier_transaction(options)?;
            let _operation_guard = self.operation_lock.lock();
            let counter = get_counter_end(&self.doc, self.peer());
            UndoManagerInner::record_checkpoint(&self.inner.lock(), counter, None);
            *options = txn.commit()?;
            Ok(())
        })
    }

    #[instrument(skip_all)]
    pub fn undo(&self) -> LoroResult<bool> {
        Ok(self.perform(UndoOrRedo::Undo, false)?.executed)
    }

    #[instrument(skip_all)]
    pub fn redo(&self) -> LoroResult<bool> {
        Ok(self.perform(UndoOrRedo::Redo, false)?.executed)
    }

    /// Preview the next undo operation without applying it to this manager's
    /// document or stacks.
    pub fn preview_undo(&self) -> LoroResult<Option<UndoPreview>> {
        self.preview(UndoOrRedo::Undo)
    }

    /// Preview the next redo operation without applying it to this manager's
    /// document or stacks.
    pub fn preview_redo(&self) -> LoroResult<Option<UndoPreview>> {
        self.preview(UndoOrRedo::Redo)
    }

    fn preview(&self, kind: UndoOrRedo) -> LoroResult<Option<UndoPreview>> {
        // Match ordinary undo/redo's checkpoint semantics before taking the
        // deep snapshot. This may finalize pending auto-commit operations, but
        // it never applies an undo or redo to the source document.
        self.record_new_checkpoint()?;

        // Deep-fork optimistically. Document edits may arrive through another
        // LoroDoc clone while O(n) cloning is in progress, so accept a snapshot
        // only when the complete state binding is unchanged on both sides.
        for _ in 0..4 {
            let (binding, mut preview_inner, preview_remap) = self.capture_preview_state()?;
            #[cfg(test)]
            run_after_preview_state_capture_hook(Arc::as_ptr(&self.inner) as usize);
            if binding.manager.has_on_push || binding.manager.has_on_pop {
                return Err(LoroError::UndoPreviewCallbacksInstalled);
            }
            if stack_for(&mut preview_inner, kind).is_empty() {
                return Ok(None);
            }
            let preview_doc = self.doc.fork();
            preview_doc.set_peer_id(self.peer())?;

            if self.capture_binding_doc_first()? != binding {
                continue;
            }

            let preview_manager = UndoManager::new(&preview_doc);
            *preview_manager.inner.lock().borrow_mut() = preview_inner;
            *preview_manager.container_remap.lock() = preview_remap;
            preview_manager.peer.store(self.peer(), Ordering::Relaxed);
            preview_manager
                .generation
                .store(binding.manager.generation, Ordering::Relaxed);

            let outcome = preview_manager.perform(kind, true)?;
            if self.capture_binding_doc_first()? != binding {
                continue;
            }

            let meta = outcome.effective_meta.ok_or_else(|| {
                LoroError::internal("non-empty undo stack produced no preview metadata")
            })?;
            return Ok(Some(UndoPreview {
                token: UndoPreviewToken {
                    manager: Arc::downgrade(&self.inner),
                    action: kind,
                    binding,
                },
                meta,
                pop_count: outcome.pop_count,
                peer_counter_advance: outcome.peer_counter_advance,
                will_apply: outcome.executed,
            }));
        }

        Err(LoroError::UndoPreviewStale)
    }

    /// Apply a previously created preview.
    ///
    /// The preview is consumed. Manager/action mismatches and stale tokens are
    /// rejected before the live stack pop at the shared document transaction
    /// boundary.
    pub fn apply_preview(&self, kind: UndoOrRedo, preview: UndoPreview) -> LoroResult<bool> {
        let expected_manager = Arc::downgrade(&self.inner);
        if !Weak::ptr_eq(&preview.token.manager, &expected_manager) {
            return Err(LoroError::UndoPreviewWrongManager);
        }
        if preview.token.action != kind {
            return Err(LoroError::UndoPreviewWrongAction);
        }

        let expected = preview.token.binding;
        self.doc.with_undo_barrier(|commit_options, release| {
            // Starting one explicit transaction makes `state.is_in_txn()` true
            // before validation and keeps it true through every candidate.
            // Competing explicit transactions therefore cannot interleave even
            // though they do not use the auto-transaction mutex.
            let barrier_txn = self
                .start_barrier_transaction(commit_options)
                .map_err(|_| LoroError::UndoPreviewStale)?;
            if !expected.doc.matches(&self.doc, self.peer()) {
                return Err(LoroError::UndoPreviewStale);
            }

            // Lock order is document transaction -> operation -> inner. Never
            // retain `inner` while acquiring a document lock: root subscription
            // delivery follows this same order.
            let Some(_operation_guard) = self.operation_lock.try_lock() else {
                return Err(LoroError::UndoPreviewStale);
            };
            {
                let Some(manager_guard) = self.inner.try_lock() else {
                    return Err(LoroError::UndoPreviewStale);
                };
                if self.capture_manager_binding(&manager_guard.borrow()) != expected.manager {
                    return Err(LoroError::UndoPreviewStale);
                }
            }

            #[cfg(test)]
            run_after_preview_validation_hook(Arc::as_ptr(&self.inner) as usize);

            // The token was produced by this same deterministic engine from
            // exactly the state checked above. Do not compare only after live
            // mutation: all rejectable mismatch checks are complete here.
            // The barrier transaction aborts on drop and this manager guard
            // restores stack/remap state on any later error or panic. Thus no
            // returned error can auto-commit a partial token application.
            let mut rollback = UndoManagerStateRollback::capture(self);
            let outcome =
                self.perform_loop(kind, false, Some((commit_options, barrier_txn, release)))?;
            rollback.disarm();
            Ok(outcome.executed)
        })
    }

    fn perform(&self, kind: UndoOrRedo, collect_preview_meta: bool) -> LoroResult<PerformOutcome> {
        self.record_new_checkpoint()?;
        let atomic = self.doc.with_undo_barrier(|commit_options, release| {
            let barrier_txn = self.start_barrier_transaction(commit_options)?;
            let _operation_guard = self.operation_lock.lock();
            let inner = self.inner.lock();
            let has_callbacks = inner.borrow().on_push.is_some() || inner.borrow().on_pop.is_some();
            drop(inner);
            if has_callbacks {
                *commit_options = barrier_txn.commit()?;
                return Ok(None);
            }

            let mut rollback = UndoManagerStateRollback::capture(self);
            let outcome = self.perform_loop(
                kind,
                collect_preview_meta,
                Some((commit_options, barrier_txn, release)),
            )?;
            rollback.disarm();
            Ok(Some(outcome))
        })?;
        if let Some(outcome) = atomic {
            return Ok(outcome);
        }

        // Existing callbacks may synchronously edit the document. Preserve
        // that behavior by using the legacy per-item transaction cadence only
        // for ordinary undo/redo with callbacks installed. Preview creation is
        // fail-closed in this state and token apply can never reach this path.
        self.perform_loop(kind, collect_preview_meta, None)
    }

    fn start_barrier_transaction(
        &self,
        options: &Option<CommitOptions>,
    ) -> LoroResult<crate::txn::Transaction> {
        let mut txn = self.doc.txn_with_origin_for_undo_barrier("undo")?;
        if let Some(options) = options.clone() {
            txn.set_options(options);
        }
        txn.set_default_options(CommitOptions::new().origin("undo"));
        Ok(txn)
    }

    /// The single undo/redo engine. `barrier_options` is present only for token
    /// apply, whose caller retains the shared document transaction guard across
    /// validation and the complete neutralized-prefix/effective-pop loop.
    fn perform_loop(
        &self,
        kind: UndoOrRedo,
        collect_preview_meta: bool,
        barrier: Option<(
            &mut Option<CommitOptions>,
            crate::txn::Transaction,
            &mut dyn FnMut(),
        )>,
    ) -> LoroResult<PerformOutcome> {
        let doc = &self.doc.clone();
        // When in the undo/redo loop, the new undo/redo stack item should restore the selection
        // to the state it was in before the item that was popped two steps ago from the stack.
        //
        //                          ┌────────────┐
        //                          │Selection 1 │
        //                          └─────┬──────┘
        //                                │   Some
        //                                ▼   ops
        //                          ┌────────────┐
        //                          │Selection 2 │
        //                          └─────┬──────┘
        //                                │   Some
        //                                ▼   ops
        //                          ┌────────────┐
        //                          │Selection 3 │◁ ─ ─ ─ ─ ─ ─ ─  Restore  ─ ─ ─
        //                          └─────┬──────┘                               │
        //                                │
        //                                │                                      │
        //                                │                              ┌ ─ ─ ─ ─ ─ ─ ─
        //           Enter the            │   Undo ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─▶   Push Redo   │
        //           undo/redo ─ ─ ─ ▶    ▼                              └ ─ ─ ─ ─ ─ ─ ─
        //             loop         ┌────────────┐                               │
        //                          │Selection 2 │◁─ ─ ─  Restore  ─
        //                          └─────┬──────┘                  │            │
        //                                │
        //                                │                         │            │
        //                                │                 ┌ ─ ─ ─ ─ ─ ─ ─
        //                                │   Undo ─ ─ ─ ─ ▶   Push Redo   │     │
        //                                ▼                 └ ─ ─ ─ ─ ─ ─ ─
        //                          ┌────────────┐                  │            │
        //                          │Selection 1 │
        //                          └─────┬──────┘                  │            │
        //                                │   Redo ◀ ─ ─ ─ ─ ─ ─ ─ ─
        //                                ▼                                      │
        //                          ┌────────────┐
        //         ┌   Restore   ─ ▷│Selection 2 │                               │
        //                          └─────┬──────┘
        //         │                      │                                      │
        // ┌ ─ ─ ─ ─ ─ ─ ─                │
        //    Push Undo   │◀─ ─ ─ ─ ─ ─ ─ │   Redo ◀ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
        // └ ─ ─ ─ ─ ─ ─ ─                ▼
        //         │                ┌────────────┐
        //                          │Selection 3 │
        //         │                └─────┬──────┘
        //          ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ▶ │   Undo
        //                                ▼
        //                          ┌────────────┐
        //                          │Selection 2 │
        //                          └────────────┘
        //
        // Because users may change the selections during the undo/redo loop, it's
        // more stable to keep the selection stored in the last stack item
        // rather than using the current selection directly.
        let end_counter = get_counter_end(doc, self.peer());
        let mut executed = false;
        let mut pop_count = 0;
        let mut effective_meta = None;
        let mut processing_guard = None;
        // Declared after `processing_guard` so an error drops/commits the
        // explicit transaction while `processing_undo` is still true.
        let (mut barrier_options, mut barrier_txn, mut release_barrier) = match barrier {
            Some((options, txn, release)) => (Some(options), Some(txn), Some(release)),
            None => (None, None, None),
        };

        loop {
            let mut prepare = || {
                if processing_guard.is_none() {
                    processing_guard = Some(ProcessingUndoGuard::start(&self.inner));
                }
                let lock = self.inner.lock();
                let mut inner = lock.borrow_mut();
                let Some((span, remote_diff)) = stack_for(&mut inner, kind).pop() else {
                    return Ok(None);
                };
                self.generation.fetch_add(1, Ordering::Relaxed);
                #[cfg(test)]
                if take_fail_after_stack_pop(Arc::as_ptr(&self.inner) as usize) {
                    return Err(LoroError::internal("undo stack pop failpoint"));
                }
                let remote_change_clone = remote_diff.lock().clone();
                Ok(Some((
                    IdSpan {
                        peer: self.peer(),
                        counter: span.span,
                    },
                    remote_change_clone,
                    (span, remote_diff),
                )))
            };
            let mut before_diff = |diff: &DiffBatch| {
                info_span!("transform remote diff").in_scope(|| {
                    let inner = self.inner.lock();
                    stack_for(&mut inner.borrow_mut(), kind).transform_based_on_this_delta(diff);
                });
            };
            let prepared = if let Some(txn) = barrier_txn.as_mut() {
                doc.undo_internal_with_barrier(
                    &mut prepare,
                    &mut self.container_remap.lock(),
                    &mut before_diff,
                    txn,
                )
            } else {
                let mut validate = || Ok(());
                doc.undo_internal_with(
                    &mut validate,
                    &mut prepare,
                    &mut self.container_remap.lock(),
                    &mut before_diff,
                )
                .map(|prepared| {
                    prepared.map(|(commit, payload)| {
                        drop(commit);
                        payload
                    })
                })
            };

            #[cfg(test)]
            if prepared.as_ref().is_ok_and(|prepared| prepared.is_some()) {
                run_after_undo_diff_applied_hook(Arc::as_ptr(&self.inner) as usize);
            }

            let Some((mut span, remote_diff)) = (match prepared {
                Ok(prepared) => prepared,
                Err(err) => return Err(err),
            }) else {
                break;
            };
            pop_count += 1;

            let mut next_push_selection = None;
            {
                let inner = self.inner.lock();
                let has_on_pop = inner.borrow().on_pop.is_some();
                if has_on_pop || collect_preview_meta {
                    for cursor in span.meta.cursors.iter_mut() {
                        // <cursor_transform> At this point <transform_delta>
                        // has transformed the row's remote diff as required.
                        transform_cursor(
                            cursor,
                            &remote_diff.lock(),
                            doc,
                            &self.container_remap.lock(),
                        );
                    }
                }
                effective_meta = Some(span.meta.clone());

                let has_on_pop = if let Some(on_pop) = inner.borrow().on_pop.as_ref() {
                    on_pop(kind, span.span, span.meta.clone());
                    true
                } else {
                    false
                };
                if has_on_pop {
                    let take = inner.borrow_mut().last_popped_selection.take();
                    next_push_selection = take;
                    inner.borrow_mut().last_popped_selection = Some(span.meta.cursors);
                }
            }
            let new_counter = if let Some(txn) = barrier_txn.as_ref() {
                end_counter + txn.len() as Counter
            } else {
                get_counter_end(doc, self.peer())
            };
            if end_counter != new_counter {
                let inner = self.inner.lock();
                let mut meta = inner
                    .borrow()
                    .on_push
                    .as_ref()
                    .map(|x| {
                        x(
                            kind.opposite(),
                            CounterSpan::new(end_counter, new_counter),
                            None,
                        )
                    })
                    .unwrap_or_default();

                if matches!(kind, UndoOrRedo::Undo)
                    && opposite_stack_for(&mut inner.borrow_mut(), kind).is_empty()
                {
                    // If it's the first undo, we use the cursors from the users
                } else if let Some(inner) = next_push_selection.take() {
                    // Otherwise, we use the cursors from the undo/redo loop
                    meta.cursors = inner;
                }

                opposite_stack_for(&mut inner.borrow_mut(), kind)
                    .push(CounterSpan::new(end_counter, new_counter), meta);
                inner.borrow_mut().next_counter = Some(new_counter);
                self.generation.fetch_add(1, Ordering::Relaxed);
                executed = true;
                break;
            }

            #[cfg(test)]
            run_between_undo_pops_hook(Arc::as_ptr(&self.inner) as usize);
        }

        let new_counter = if let Some(txn) = barrier_txn.as_ref() {
            end_counter + txn.len() as Counter
        } else {
            get_counter_end(doc, self.peer())
        };
        if let Some(txn) = barrier_txn.take() {
            let effective = !txn.is_empty();
            let id_span = txn.id_span();
            let mut txn = txn;
            let on_commit = if effective {
                txn.take_on_commit()
            } else {
                None
            };
            let options = txn.commit()?;
            if effective {
                release_barrier
                    .as_mut()
                    .expect("effective barrier transaction requires release")();
                if let Some(on_commit) = on_commit {
                    self.doc.emit_deferred_commit(on_commit, id_span);
                }
            }
            **barrier_options
                .as_mut()
                .expect("barrier transaction requires commit options") = options;
        }
        Ok(PerformOutcome {
            executed,
            effective_meta,
            pop_count,
            peer_counter_advance: new_counter - end_counter,
        })
    }

    pub fn can_undo(&self) -> bool {
        !self.inner.lock().borrow().undo_stack.is_empty()
    }

    pub fn can_redo(&self) -> bool {
        !self.inner.lock().borrow().redo_stack.is_empty()
    }

    pub fn undo_count(&self) -> usize {
        self.inner.lock().borrow().undo_stack.len()
    }

    pub fn redo_count(&self) -> usize {
        self.inner.lock().borrow().redo_stack.len()
    }

    /// Get the metadata of the top undo stack item, if any.
    pub fn top_undo_meta(&self) -> Option<UndoItemMeta> {
        self.inner.lock().borrow().undo_stack.peek_top_meta()
    }

    /// Get the metadata of the top redo stack item, if any.
    pub fn top_redo_meta(&self) -> Option<UndoItemMeta> {
        self.inner.lock().borrow().redo_stack.peek_top_meta()
    }

    /// Get the value associated with the top undo stack item, if any.
    pub fn top_undo_value(&self) -> Option<LoroValue> {
        self.top_undo_meta().map(|m| m.value)
    }

    /// Get the value associated with the top redo stack item, if any.
    pub fn top_redo_value(&self) -> Option<LoroValue> {
        self.top_redo_meta().map(|m| m.value)
    }

    pub fn set_on_push(&self, on_push: Option<OnPush>) {
        let _operation_guard = self.operation_lock.lock();
        self.inner.lock().borrow_mut().on_push = on_push;
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_on_pop(&self, on_pop: Option<OnPop>) {
        let _operation_guard = self.operation_lock.lock();
        self.inner.lock().borrow_mut().on_pop = on_pop;
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    pub fn clear(&self) {
        let _operation_guard = self.operation_lock.lock();
        self.inner.lock().borrow_mut().undo_stack.clear();
        self.inner.lock().borrow_mut().redo_stack.clear();
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Clear only the redo stack, preserving the undo stack.
    pub fn clear_redo(&self) {
        let _operation_guard = self.operation_lock.lock();
        self.inner.lock().borrow_mut().redo_stack.clear();
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Clear only the undo stack, preserving the redo stack.
    pub fn clear_undo(&self) {
        let _operation_guard = self.operation_lock.lock();
        self.inner.lock().borrow_mut().undo_stack.clear();
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_top_undo_meta(&self, meta: UndoItemMeta) {
        let _operation_guard = self.operation_lock.lock();
        self.inner.lock().borrow_mut().undo_stack.set_top_meta(meta);
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_top_redo_meta(&self, meta: UndoItemMeta) {
        let _operation_guard = self.operation_lock.lock();
        self.inner.lock().borrow_mut().redo_stack.set_top_meta(meta);
        self.generation.fetch_add(1, Ordering::Relaxed);
    }
}

/// Undo the given spans of operations.
///
/// # Parameters
///
/// - `spans`: A vector of tuples where each tuple contains an `IdSpan` and its associated `Frontiers`.
///   - `IdSpan`: Represents a span of operations identified by an ID.
///   - `Frontiers`: Represents the deps of the given id_span
/// - `latest_frontiers`: The latest frontiers of the document
/// - `calc_diff`: A closure that takes two `Frontiers` and calculates the difference between them, returning a `DiffBatch`.
///
/// # Returns
///
/// - `DiffBatch`: Applying this batch on the `latest_frontiers` will undo the ops in the given spans.
pub(crate) fn undo(
    spans: Vec<(IdSpan, Frontiers)>,
    last_frontiers_or_last_bi: Either<&Frontiers, &DiffBatch>,
    calc_diff: impl Fn(&Frontiers, &Frontiers) -> DiffBatch,
    on_last_event_a: &mut dyn FnMut(&DiffBatch),
) -> DiffBatch {
    // The process of performing undo is:
    //
    // 0. Split the span into a series of continuous spans. There is no external dep within each continuous span.
    //
    // For each continuous span_i:
    //
    // 1. a. Calculate the event of checkout from id_span.last to id_span.deps, call it Ai. It undo the ops in the current span.
    //    b. Calculate A'i = Ai + T(Ci-1, Ai) if i > 0, otherwise A'i = Ai.
    //       NOTE: A'i can undo the ops in the current span and the previous spans, if it's applied on the id_span.last version.
    // 2. Calculate the event of checkout from id_span.last to [the next span's last id] or [the latest version], call it Bi.
    // 3. Transform event A'i based on Bi, call it Ci
    // 4. If span_i is the last span, apply Ci to the current state.

    // -------------------------------------------------------
    // 0. Split the span into a series of continuous spans
    // -------------------------------------------------------

    let mut last_ci: Option<DiffBatch> = None;
    for i in 0..spans.len() {
        debug_span!("Undo", ?i, "Undo span {:?}", &spans[i]).in_scope(|| {
            let (this_id_span, this_deps) = &spans[i];
            // ---------------------------------------
            // 1.a Calc event A_i
            // ---------------------------------------
            let mut event_a_i = debug_span!("1. Calc event A_i").in_scope(|| {
                // Checkout to the last id of the id_span
                calc_diff(&this_id_span.id_last().into(), this_deps)
            });

            // println!("event_a_i: {:?}", event_a_i);

            // ---------------------------------------
            // 2. Calc event B_i
            // ---------------------------------------
            let stack_diff_batch;
            let event_b_i = 'block: {
                let next = if i + 1 < spans.len() {
                    spans[i + 1].0.id_last().into()
                } else {
                    match last_frontiers_or_last_bi {
                        Either::Left(last_frontiers) => last_frontiers.clone(),
                        Either::Right(right) => break 'block right,
                    }
                };
                stack_diff_batch = Some(calc_diff(&this_id_span.id_last().into(), &next));
                stack_diff_batch.as_ref().unwrap()
            };

            // println!("event_b_i: {:?}", event_b_i);

            // event_a_prime can undo the ops in the current span and the previous spans
            let mut event_a_prime = if let Some(mut last_ci) = last_ci.take() {
                // ------------------------------------------------------------------------------
                // 1.b Transform and apply Ci-1 based on Ai, call it A'i
                // ------------------------------------------------------------------------------
                last_ci.transform(&event_a_i, true);

                event_a_i.compose(&last_ci);
                event_a_i
            } else {
                event_a_i
            };
            if i == spans.len() - 1 {
                on_last_event_a(&event_a_prime);
            }
            // --------------------------------------------------
            // 3. Transform event A'_i based on B_i, call it C_i
            // --------------------------------------------------
            event_a_prime.transform(event_b_i, true);

            // println!("event_a_prime: {:?}", event_a_prime);

            let c_i = event_a_prime;
            last_ci = Some(c_i);
        });
    }

    last_ci.unwrap()
}

#[cfg(test)]
mod preview_concurrency_tests {
    use super::*;
    use crate::{cursor::PosType, handler::TextHandler, loro::ExportMode};
    use std::{
        sync::{mpsc, Barrier},
        thread,
        time::Duration,
    };

    fn sync(from: &LoroDoc, to: &LoroDoc) {
        from.commit_then_renew();
        let bytes = from.export(ExportMode::updates(&to.oplog_vv())).unwrap();
        to.import(&bytes).unwrap();
    }

    fn neutralized_prefix() -> (LoroDoc, UndoManager) {
        let remote = LoroDoc::new_auto_commit();
        remote.set_peer_id(1).unwrap();
        let nested = remote
            .get_map("map")
            .insert_container("nested", TextHandler::new_detached())
            .unwrap();
        nested.insert(0, "seed", PosType::Unicode).unwrap();
        remote.commit_then_renew();

        let local = LoroDoc::new_auto_commit();
        local.set_peer_id(2).unwrap();
        sync(&remote, &local);
        let undo = UndoManager::new(&local);
        undo.set_merge_interval(0);

        local
            .get_text("text")
            .insert(0, "effective", PosType::Unicode)
            .unwrap();
        local.commit_then_renew();
        undo.record_new_checkpoint().unwrap();

        let nested = local
            .get_by_str_path("map/nested")
            .unwrap()
            .into_handler()
            .unwrap()
            .into_text()
            .unwrap();
        nested
            .insert(nested.len_unicode(), " local", PosType::Unicode)
            .unwrap();
        local.commit_then_renew();
        undo.record_new_checkpoint().unwrap();

        sync(&local, &remote);
        remote.get_map("map").delete("nested").unwrap();
        remote.commit_then_renew();
        sync(&remote, &local);
        (local, undo)
    }

    fn install_between_pops_hook(manager: &UndoManager, hook: BetweenUndoPopsHook) {
        let target = Arc::as_ptr(&manager.inner) as usize;
        between_undo_pops_hook()
            .lock()
            .unwrap()
            .insert(target, hook);
    }

    fn install_after_validation_hook(manager: &UndoManager, hook: BetweenUndoPopsHook) {
        let target = Arc::as_ptr(&manager.inner) as usize;
        after_preview_validation_hook()
            .lock()
            .unwrap()
            .insert(target, hook);
    }

    fn install_after_preview_state_capture_hook(manager: &UndoManager, hook: BetweenUndoPopsHook) {
        let target = Arc::as_ptr(&manager.inner) as usize;
        after_preview_state_capture_hook()
            .lock()
            .unwrap()
            .insert(target, hook);
    }

    fn clear_after_preview_state_capture_hook(manager: &UndoManager) {
        let target = Arc::as_ptr(&manager.inner) as usize;
        after_preview_state_capture_hook()
            .lock()
            .unwrap()
            .remove(&target);
    }

    fn install_fail_after_stack_pop(manager: &UndoManager) {
        fail_after_stack_pop_target()
            .lock()
            .unwrap()
            .insert(Arc::as_ptr(&manager.inner) as usize);
    }

    fn install_after_undo_diff_applied_hook(manager: &UndoManager, hook: BetweenUndoPopsHook) {
        let target = Arc::as_ptr(&manager.inner) as usize;
        after_undo_diff_applied_hook()
            .lock()
            .unwrap()
            .insert(target, hook);
    }

    #[test]
    fn public_config_aba_retries_preview_capture() {
        let doc = LoroDoc::new_auto_commit();
        let undo = UndoManager::new(&doc);
        doc.get_text("text")
            .insert(0, "x", PosType::Unicode)
            .unwrap();
        doc.commit_then_renew();

        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hook_attempts = attempts.clone();
        let hook_doc = doc.clone();
        install_after_preview_state_capture_hook(
            &undo,
            Arc::new(move || {
                if hook_attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                    // Exercise the public Configure clone path. Values return
                    // to A, but the shared revision must expose the ABA.
                    hook_doc.config().set_record_timestamp(true);
                    hook_doc.config().set_record_timestamp(false);
                }
            }),
        );

        let result = undo.preview_undo();
        clear_after_preview_state_capture_hook(&undo);
        let preview = result.unwrap().unwrap();

        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert_eq!(
            preview.token.binding.doc.config.revision,
            doc.config().revision()
        );
        assert!(!doc.config().record_timestamp());
        assert!(undo.apply_preview(UndoOrRedo::Undo, preview).unwrap());
        assert_eq!(doc.get_text("text").to_string(), "");
    }

    #[test]
    fn token_apply_error_and_panic_abort_document_and_restore_manager() {
        let exercise = |panic_after_diff: bool| {
            let doc = LoroDoc::new();
            let undo = UndoManager::new(&doc);
            doc.get_text("text")
                .insert(0, "x", PosType::Unicode)
                .unwrap();
            doc.commit_then_renew();

            let preview = undo.preview_undo().unwrap().unwrap();
            let vv = doc.oplog_vv();
            let undo_count = undo.undo_count();
            if panic_after_diff {
                install_after_undo_diff_applied_hook(
                    &undo,
                    Arc::new(|| panic!("post-diff panic failpoint")),
                );
                let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _ = undo.apply_preview(UndoOrRedo::Undo, preview);
                }));
                assert!(panic.is_err());
            } else {
                install_fail_after_stack_pop(&undo);
                let error = undo
                    .apply_preview(UndoOrRedo::Undo, preview)
                    .expect_err("post-pop failpoint must return an error");
                assert!(error.to_string().contains("undo stack pop failpoint"));
            }

            assert_eq!(doc.oplog_vv(), vv);
            assert_eq!(doc.get_text("text").to_string(), "x");
            assert_eq!(undo.undo_count(), undo_count);
            assert!(
                !doc.undo_barrier_active.load(Ordering::Acquire),
                "undo barrier must be released"
            );
            assert!(undo.undo().unwrap(), "manager must remain usable");
            assert_eq!(doc.get_text("text").to_string(), "");
        };

        exercise(false);
        exercise(true);
    }

    #[test]
    fn explicit_transaction_is_rejected_between_neutralized_pops() {
        for use_preview in [false, true] {
            let (doc, undo) = neutralized_prefix();
            let preview = use_preview.then(|| undo.preview_undo().unwrap().unwrap());
            let gate = Arc::new(Barrier::new(2));
            let (result_tx, result_rx) = mpsc::channel::<bool>();
            let result_rx = Arc::new(std::sync::Mutex::new(result_rx));
            let hook_gate = gate.clone();
            install_between_pops_hook(
                &undo,
                Arc::new(move || {
                    hook_gate.wait();
                    assert!(result_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(2))
                        .unwrap());
                }),
            );

            let clone = doc.clone();
            let worker = thread::spawn(move || {
                gate.wait();
                let rejected = matches!(
                    clone.txn_with_origin("concurrent-explicit"),
                    Err(LoroError::DuplicatedTransactionError)
                );
                result_tx.send(rejected).unwrap();
            });

            let applied = if let Some(preview) = preview {
                undo.apply_preview(UndoOrRedo::Undo, preview).unwrap()
            } else {
                undo.undo().unwrap()
            };
            assert!(applied);
            worker.join().unwrap();

            // The exclusion flag is released after either operation.
            assert!(doc
                .with_undo_barrier(|options, _| {
                    let txn = doc.txn_with_origin_for_undo_barrier("after-undo")?;
                    *options = txn.commit()?;
                    Ok(())
                })
                .is_ok());
        }
    }

    #[test]
    fn explicit_waiter_that_passed_fast_check_is_rejected_during_undo_pause() {
        let (doc, undo) = neutralized_prefix();
        let preview = undo.preview_undo().unwrap().unwrap();
        let (passed_tx, passed_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let resume_rx = Arc::new(std::sync::Mutex::new(resume_rx));
        let hook_resume = resume_rx.clone();
        crate::txn::set_before_transaction_locks_hook_for_test(
            &doc,
            Arc::new(move || {
                passed_tx.send(()).unwrap();
                hook_resume
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap();
            }),
        );

        let (result_tx, result_rx) = mpsc::channel::<bool>();
        let result_rx = Arc::new(std::sync::Mutex::new(result_rx));
        let clone = doc.clone();
        let worker = thread::spawn(move || {
            let rejected = matches!(
                clone.txn_with_origin("passed-fast-check"),
                Err(LoroError::DuplicatedTransactionError)
            );
            result_tx.send(rejected).unwrap();
        });
        passed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("contender must pass the first flag check");

        let paused_result = result_rx.clone();
        crate::txn::set_undo_transaction_paused_hook_for_test(
            &doc,
            Arc::new(move || {
                resume_tx.send(()).unwrap();
                assert!(paused_result
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(2))
                    .expect("contender must finish while undo transaction is paused"));
            }),
        );

        assert!(undo.apply_preview(UndoOrRedo::Undo, preview).unwrap());
        worker.join().unwrap();
        assert_eq!(doc.get_text("text").to_string(), "");
        doc.get_text("after-race")
            .insert(0, "usable", PosType::Unicode)
            .unwrap();
        doc.commit_then_renew();
        assert_eq!(doc.get_text("after-race").to_string(), "usable");
    }

    #[test]
    fn preview_rejects_an_active_explicit_transaction_without_lock_inversion() {
        let (doc, undo) = neutralized_prefix();
        let undo = Arc::new(undo);
        let (options, auto_guard) = doc.implicit_commit_then_stop();
        let mut txn = doc.txn_with_origin("preview-racer").unwrap();
        doc.get_text("racing")
            .insert_with_txn(&mut txn, 0, "x", PosType::Unicode)
            .unwrap();

        let (started_tx, started_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let worker_undo = undo.clone();
        let worker = thread::spawn(move || {
            started_tx.send(()).unwrap();
            let rejected = matches!(
                worker_undo.preview_undo(),
                Err(LoroError::DuplicatedTransactionError)
            );
            result_tx.send(rejected).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        // Let preview take the document barrier while DocState still has the
        // explicit transaction. It must reject before taking manager state.
        drop(auto_guard);
        assert!(result_rx.recv_timeout(Duration::from_secs(2)).unwrap());
        worker.join().unwrap();

        let options = txn.commit().unwrap().or(options);
        doc.renew_txn_if_auto_commit(options);
        assert_eq!(doc.get_text("racing").to_string(), "x");
        assert!(undo.preview_undo().unwrap().is_some());
    }

    #[test]
    fn local_edits_and_imports_serialize_between_neutralized_pops() {
        for use_preview in [false, true] {
            for import in [false, true] {
                let (doc, undo) = neutralized_prefix();
                let preview = use_preview.then(|| undo.preview_undo().unwrap().unwrap());
                let concurrent_text = doc.get_text("concurrent");
                let import_bytes = if import {
                    let remote = LoroDoc::new_auto_commit();
                    remote.set_peer_id(99).unwrap();
                    remote.get_map("imported").insert("value", 1).unwrap();
                    remote.commit_then_renew();
                    Some(remote.export(ExportMode::all_updates()).unwrap())
                } else {
                    None
                };

                let gate = Arc::new(Barrier::new(2));
                let (attempted_tx, attempted_rx) = mpsc::channel();
                let (done_tx, done_rx) = mpsc::channel::<Result<(), String>>();
                let attempted_rx = Arc::new(std::sync::Mutex::new(attempted_rx));
                let done_rx = Arc::new(std::sync::Mutex::new(done_rx));
                let hook_gate = gate.clone();
                let hook_attempted = attempted_rx.clone();
                let hook_done = done_rx.clone();
                install_between_pops_hook(
                    &undo,
                    Arc::new(move || {
                        hook_gate.wait();
                        hook_attempted
                            .lock()
                            .unwrap()
                            .recv_timeout(Duration::from_secs(2))
                            .unwrap();
                        assert!(hook_done.lock().unwrap().try_recv().is_err());
                    }),
                );

                let clone = doc.clone();
                let worker = thread::spawn(move || {
                    gate.wait();
                    attempted_tx.send(()).unwrap();
                    let result = if let Some(bytes) = import_bytes {
                        clone.import(&bytes).map(|_| ())
                    } else {
                        concurrent_text
                            .insert(0, "serialized", PosType::Unicode)
                            .map(|_| clone.commit_then_renew())
                            .map(|_| ())
                    };
                    done_tx
                        .send(result.map_err(|error| error.to_string()))
                        .unwrap();
                });

                let applied = if let Some(preview) = preview {
                    undo.apply_preview(UndoOrRedo::Undo, preview).unwrap()
                } else {
                    undo.undo().unwrap()
                };
                assert!(applied);
                done_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap_or_else(|error| {
                        panic!(
                            "concurrent operation timed out: preview={use_preview} import={import}: {error}"
                        )
                    })
                    .unwrap_or_else(|error| {
                        panic!(
                            "concurrent operation failed: preview={use_preview} import={import}: {error}"
                        )
                    });
                worker.join().unwrap();
                assert_eq!(doc.get_text("text").to_string(), "");
                if import {
                    assert_eq!(doc.get_map("imported").get("value"), Some(1.into()));
                } else {
                    assert_eq!(doc.get_text("concurrent").to_string(), "serialized");
                }
            }
        }
    }

    #[test]
    fn manager_config_mutation_serializes_after_preview_validation() {
        let (doc, undo) = neutralized_prefix();
        let preview = undo.preview_undo().unwrap().unwrap();
        let undo = Arc::new(undo);
        let gate = Arc::new(Barrier::new(2));
        let (probe_tx, probe_rx) = mpsc::channel::<bool>();
        let probe_rx = Arc::new(std::sync::Mutex::new(probe_rx));
        let hook_gate = gate.clone();
        let hook_probe = probe_rx.clone();
        install_between_pops_hook(
            &undo,
            Arc::new(move || {
                hook_gate.wait();
                assert!(hook_probe
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(2))
                    .expect("manager contender must probe the operation lock"));
            }),
        );

        let worker_undo = undo.clone();
        let worker = thread::spawn(move || {
            gate.wait();
            let blocked = worker_undo.operation_lock.try_lock().is_none();
            probe_tx.send(blocked).unwrap();
            worker_undo.set_merge_interval(1234);
        });

        assert!(undo.apply_preview(UndoOrRedo::Undo, preview).unwrap());
        worker.join().unwrap();
        assert_eq!(doc.get_text("text").to_string(), "");
    }

    #[test]
    fn document_config_and_peer_changes_serialize_after_preview_validation() {
        for change_peer in [false, true] {
            let (doc, undo) = neutralized_prefix();
            let preview = undo.preview_undo().unwrap().unwrap();
            let gate = Arc::new(Barrier::new(2));
            let (probe_tx, probe_rx) = mpsc::channel::<bool>();
            let probe_rx = Arc::new(std::sync::Mutex::new(probe_rx));
            let hook_gate = gate.clone();
            let hook_probe = probe_rx.clone();
            install_after_validation_hook(
                &undo,
                Arc::new(move || {
                    hook_gate.wait();
                    assert!(hook_probe
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(2))
                        .expect("document contender must probe the binding lock"));
                }),
            );

            let clone = doc.clone();
            let worker = thread::spawn(move || {
                gate.wait();
                let blocked = clone.config().undo_binding_lock().try_lock().is_none();
                probe_tx.send(blocked).unwrap();
                if change_peer {
                    clone.set_peer_id(444).unwrap();
                } else {
                    clone.config().set_text_style_config(StyleConfigMap::new());
                }
            });

            assert!(undo.apply_preview(UndoOrRedo::Undo, preview).unwrap());
            worker.join().unwrap();
            assert_eq!(doc.get_text("text").to_string(), "");
            if change_peer {
                assert_eq!(doc.peer_id(), 444);
            } else {
                assert_eq!(doc.config().text_style_config(), StyleConfigMap::new());
            }
        }
    }
}
