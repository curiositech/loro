use loro::{LoroDoc, UndoManager, UndoOrRedo};
use rand::{rngs::StdRng, Rng, SeedableRng};

fn insert(text: &loro::LoroText, pos: usize, value: &str) {
    text.insert(pos, value).unwrap();
}

fn text(doc: &LoroDoc) -> String {
    doc.get_text("text").to_string()
}

fn sync(from: &LoroDoc, to: &LoroDoc) {
    let bytes = from
        .export(loro::ExportMode::updates(&to.oplog_vv()))
        .unwrap();
    to.import(&bytes).unwrap();
}

fn deep(doc: &LoroDoc) -> serde_json::Value {
    serde_json::to_value(doc.get_deep_value()).unwrap()
}

#[test]
fn public_preview_apply_contract_uses_only_loro_surface() {
    let doc = LoroDoc::new();
    doc.set_peer_id(7).unwrap();
    let mut undo = UndoManager::new(&doc);
    let value = doc.get_text("text");

    insert(&value, 0, "hello");
    doc.commit();
    let before_vv = doc.oplog_vv();
    let before_count = undo.undo_count();

    let preview = undo.preview_undo().unwrap().unwrap();
    assert_eq!(preview.action(), UndoOrRedo::Undo);
    assert!(preview.will_apply());
    assert_eq!(preview.pop_count(), 1);
    assert!(preview.peer_counter_advance() > 0);
    assert_eq!(text(&doc), "hello");
    assert_eq!(doc.oplog_vv(), before_vv);
    assert_eq!(undo.undo_count(), before_count);

    assert!(undo.apply_preview(UndoOrRedo::Undo, preview).unwrap());
    assert_eq!(text(&doc), "");

    let redo = undo.preview_redo().unwrap().unwrap();
    assert_eq!(redo.action(), UndoOrRedo::Redo);
    assert!(undo.apply_preview(UndoOrRedo::Redo, redo).unwrap());
    assert_eq!(text(&doc), "hello");
}

#[test]
fn stale_rejection_leaves_manager_usable() {
    let doc = LoroDoc::new();
    let mut undo = UndoManager::new(&doc);
    let value = doc.get_text("text");
    insert(&value, 0, "a");
    doc.commit();

    let stale = undo.preview_undo().unwrap().unwrap();
    insert(&value, 1, "b");
    doc.commit();
    let error = undo
        .apply_preview(UndoOrRedo::Undo, stale)
        .expect_err("new edit must stale the token");
    assert!(matches!(error, loro::LoroError::UndoPreviewStale));

    insert(&value, 2, "c");
    doc.commit();
    assert!(undo.undo().unwrap());
    assert_eq!(text(&doc), "ab");
    assert!(undo.undo().unwrap());
    assert_eq!(text(&doc), "a");
}

#[test]
fn wrong_manager_action_callbacks_and_setter_replacements_fail_closed() {
    let doc = LoroDoc::new();
    let mut undo = UndoManager::new(&doc);
    let mut other = UndoManager::new(&doc);
    insert(&doc.get_text("text"), 0, "x");
    doc.commit();

    let wrong_manager = undo.preview_undo().unwrap().unwrap();
    assert!(matches!(
        other.apply_preview(UndoOrRedo::Undo, wrong_manager),
        Err(loro::LoroError::UndoPreviewWrongManager)
    ));

    let wrong_action = undo.preview_undo().unwrap().unwrap();
    assert!(matches!(
        undo.apply_preview(UndoOrRedo::Redo, wrong_action),
        Err(loro::LoroError::UndoPreviewWrongAction)
    ));

    let replaced = undo.preview_undo().unwrap().unwrap();
    undo.set_on_pop(Some(Box::new(|_, _, _| {})));
    undo.set_on_pop(Some(Box::new(|_, _, _| {})));
    assert!(matches!(
        undo.apply_preview(UndoOrRedo::Undo, replaced),
        Err(loro::LoroError::UndoPreviewStale)
    ));
    assert!(matches!(
        undo.preview_undo(),
        Err(loro::LoroError::UndoPreviewCallbacksInstalled)
    ));

    undo.set_on_pop(None);
    undo.set_on_push(Some(Box::new(|_, _, _| loro::UndoItemMeta::default())));
    assert!(matches!(
        undo.preview_undo(),
        Err(loro::LoroError::UndoPreviewCallbacksInstalled)
    ));
}

#[test]
fn capacity_grouping_redo_clear_and_peer_changes_invalidate_tokens() {
    let doc = LoroDoc::new();
    let mut undo = UndoManager::new(&doc);
    undo.set_max_undo_steps(2);
    undo.set_merge_interval(0);
    let value = doc.get_text("text");
    for ch in ["a", "b", "c"] {
        insert(&value, value.len_unicode(), ch);
        doc.commit();
    }
    assert_eq!(undo.undo_count(), 2);

    undo.group_start().unwrap();
    insert(&value, value.len_unicode(), "d");
    doc.commit();
    insert(&value, value.len_unicode(), "e");
    doc.commit();
    undo.group_end();
    let grouped = undo.preview_undo().unwrap().unwrap();
    assert!(grouped.peer_counter_advance() > 0);
    assert!(undo.apply_preview(UndoOrRedo::Undo, grouped).unwrap());
    assert_eq!(text(&doc), "abc");

    let redo = undo.preview_redo().unwrap().unwrap();
    insert(&value, value.len_unicode(), "z");
    doc.commit();
    assert!(matches!(
        undo.apply_preview(UndoOrRedo::Redo, redo),
        Err(loro::LoroError::UndoPreviewStale)
    ));
    assert!(!undo.can_redo());

    let token = undo.preview_undo().unwrap().unwrap();
    doc.set_peer_id(99).unwrap();
    assert!(matches!(
        undo.apply_preview(UndoOrRedo::Undo, token),
        Err(loro::LoroError::UndoPreviewStale)
    ));
}

#[test]
fn public_config_authority_invalidates_tokens() {
    let doc = LoroDoc::new();
    let mut undo = UndoManager::new(&doc);
    insert(&doc.get_text("text"), 0, "x");
    doc.commit();

    let timestamp = undo.preview_undo().unwrap().unwrap();
    doc.config().set_record_timestamp(true);
    assert!(matches!(
        undo.apply_preview(UndoOrRedo::Undo, timestamp),
        Err(loro::LoroError::UndoPreviewStale)
    ));

    let styles = undo.preview_undo().unwrap().unwrap();
    doc.config()
        .set_text_style_config(loro::StyleConfigMap::new());
    assert!(matches!(
        undo.apply_preview(UndoOrRedo::Undo, styles),
        Err(loro::LoroError::UndoPreviewStale)
    ));
}

#[test]
fn replacing_style_config_preserves_its_default_style() {
    let doc = LoroDoc::new();
    doc.config_default_text_style(Some(loro::StyleConfig::new()));
    let mut replacement = doc.config().text_style_config();
    replacement.insert("custom".into(), loro::StyleConfig::new());

    doc.config_text_style(replacement.clone());

    assert_eq!(doc.config().text_style_config(), replacement);
    assert_eq!(
        doc.config().text_style_config().get(&"unlisted".into()),
        Some(loro::StyleConfig::new())
    );
}

fn neutralized_setup(include_effective_prefix: bool) -> (LoroDoc, UndoManager) {
    let remote = LoroDoc::new();
    remote.set_peer_id(1).unwrap();
    let nested = remote
        .get_map("map")
        .insert_container("nested", loro::LoroText::new())
        .unwrap();
    insert(&nested, 0, "seed");
    remote.commit();

    let local = LoroDoc::new();
    local.set_peer_id(2).unwrap();
    sync(&remote, &local);
    let mut undo = UndoManager::new(&local);
    undo.set_merge_interval(0);

    if include_effective_prefix {
        insert(&local.get_text("text"), 0, "effective");
        local.commit();
        undo.record_new_checkpoint().unwrap();
    }

    let local_nested = local
        .get_by_str_path("map/nested")
        .unwrap()
        .into_container()
        .unwrap()
        .into_text()
        .unwrap();
    insert(&local_nested, local_nested.len_unicode(), " local");
    local.commit();
    undo.record_new_checkpoint().unwrap();
    sync(&local, &remote);
    remote.get_map("map").delete("nested").unwrap();
    remote.commit();
    sync(&remote, &local);
    (local, undo)
}

#[test]
fn neutralized_prefix_and_all_neutralized_are_exact() {
    let (doc, mut undo) = neutralized_setup(true);
    let preview = undo.preview_undo().unwrap().unwrap();
    assert!(preview.will_apply());
    assert_eq!(preview.pop_count(), 2);
    assert!(preview.peer_counter_advance() > 0);
    assert!(undo.apply_preview(UndoOrRedo::Undo, preview).unwrap());
    assert_eq!(text(&doc), "");

    let (doc, mut undo) = neutralized_setup(false);
    let before = deep(&doc);
    let preview = undo.preview_undo().unwrap().unwrap();
    assert!(!preview.will_apply());
    assert_eq!(preview.pop_count(), 1);
    assert_eq!(preview.peer_counter_advance(), 0);
    // meta() is diagnostic-only when will_apply is false.
    let _diagnostic = preview.meta();
    assert!(!undo.apply_preview(UndoOrRedo::Undo, preview).unwrap());
    assert_eq!(deep(&doc), before);
    assert!(!undo.can_undo());
}

#[test]
fn preview_apply_matches_ordinary_for_neutralized_and_grouped_changes() {
    let (preview_doc, mut preview_undo) = neutralized_setup(true);
    let (ordinary_doc, mut ordinary_undo) = neutralized_setup(true);
    assert_eq!(deep(&preview_doc), deep(&ordinary_doc));

    let preview = preview_undo.preview_undo().unwrap().unwrap();
    let expected_pop_count = preview.pop_count();
    let expected_advance = preview.peer_counter_advance();
    let ordinary_applied = ordinary_undo.undo().unwrap();
    let preview_applied = preview_undo
        .apply_preview(UndoOrRedo::Undo, preview)
        .unwrap();
    assert_eq!(preview_applied, ordinary_applied);
    assert_eq!(expected_pop_count, 2);
    assert!(expected_advance > 0);
    assert_eq!(deep(&preview_doc), deep(&ordinary_doc));
    assert_eq!(preview_doc.oplog_vv(), ordinary_doc.oplog_vv());
    assert_eq!(preview_undo.undo_count(), ordinary_undo.undo_count());
    assert_eq!(preview_undo.redo_count(), ordinary_undo.redo_count());
}

#[test]
fn style_only_preview_apply_matches_ordinary_undo_redo() {
    let preview_doc = LoroDoc::new();
    let ordinary_doc = LoroDoc::new();
    preview_doc.set_peer_id(42).unwrap();
    ordinary_doc.set_peer_id(42).unwrap();
    preview_doc.config_text_style(loro::StyleConfigMap::default_rich_text_config());
    ordinary_doc.config_text_style(loro::StyleConfigMap::default_rich_text_config());
    let mut preview_undo = UndoManager::new(&preview_doc);
    let mut ordinary_undo = UndoManager::new(&ordinary_doc);
    for doc in [&preview_doc, &ordinary_doc] {
        let value = doc.get_text("text");
        insert(&value, 0, "styled");
        doc.commit();
        value.mark(0..6, "bold", true).unwrap();
        doc.commit();
    }

    for action in [UndoOrRedo::Undo, UndoOrRedo::Redo] {
        let preview = match action {
            UndoOrRedo::Undo => preview_undo.preview_undo().unwrap().unwrap(),
            UndoOrRedo::Redo => preview_undo.preview_redo().unwrap().unwrap(),
        };
        let ordinary = match action {
            UndoOrRedo::Undo => ordinary_undo.undo().unwrap(),
            UndoOrRedo::Redo => ordinary_undo.redo().unwrap(),
        };
        assert_eq!(
            preview_undo.apply_preview(action, preview).unwrap(),
            ordinary
        );
        assert_eq!(deep(&preview_doc), deep(&ordinary_doc));
        assert_eq!(
            preview_doc.get_text("text").get_richtext_value(),
            ordinary_doc.get_text("text").get_richtext_value()
        );
    }
}

fn populate_all_container_kinds(doc: &LoroDoc, undo: &mut UndoManager) {
    undo.set_merge_interval(0);
    undo.group_start().unwrap();

    doc.get_map("map").insert("key", "value").unwrap();
    doc.commit();

    let list = doc.get_list("list");
    list.insert(0, 1).unwrap();
    list.insert(1, 2).unwrap();
    doc.commit();

    let movable = doc.get_movable_list("movable");
    movable.insert(0, "first").unwrap();
    movable.insert(1, "second").unwrap();
    movable.mov(0, 1).unwrap();
    doc.commit();

    let tree = doc.get_tree("tree");
    let root = tree.create(None).unwrap();
    tree.create(root).unwrap();
    doc.commit();

    let richtext = doc.get_text("richtext");
    insert(&richtext, 0, "styled");
    richtext.mark(0..6, "bold", true).unwrap();
    doc.commit();
    undo.group_end();
}

#[test]
fn map_list_movable_tree_and_richtext_match_ordinary_engine() {
    let preview_doc = LoroDoc::new();
    let ordinary_doc = LoroDoc::new();
    preview_doc.set_peer_id(314).unwrap();
    ordinary_doc.set_peer_id(314).unwrap();
    preview_doc.config_text_style(loro::StyleConfigMap::default_rich_text_config());
    ordinary_doc.config_text_style(loro::StyleConfigMap::default_rich_text_config());
    let mut preview_undo = UndoManager::new(&preview_doc);
    let mut ordinary_undo = UndoManager::new(&ordinary_doc);
    populate_all_container_kinds(&preview_doc, &mut preview_undo);
    populate_all_container_kinds(&ordinary_doc, &mut ordinary_undo);
    assert_eq!(deep(&preview_doc), deep(&ordinary_doc));

    for action in [UndoOrRedo::Undo, UndoOrRedo::Redo] {
        let preview = match action {
            UndoOrRedo::Undo => preview_undo.preview_undo().unwrap().unwrap(),
            UndoOrRedo::Redo => preview_undo.preview_redo().unwrap().unwrap(),
        };
        assert_eq!(preview.pop_count(), 1);
        assert!(preview.peer_counter_advance() > 0);
        let ordinary_applied = match action {
            UndoOrRedo::Undo => ordinary_undo.undo().unwrap(),
            UndoOrRedo::Redo => ordinary_undo.redo().unwrap(),
        };
        assert_eq!(
            preview_undo.apply_preview(action, preview).unwrap(),
            ordinary_applied
        );
        assert_eq!(deep(&preview_doc), deep(&ordinary_doc));
        assert_eq!(preview_doc.oplog_vv(), ordinary_doc.oplog_vv());
        assert_eq!(
            preview_doc.get_text("richtext").get_richtext_value(),
            ordinary_doc.get_text("richtext").get_richtext_value()
        );
    }
}

#[test]
fn randomized_preview_apply_matches_ordinary_history_engine() {
    let preview_doc = LoroDoc::new();
    let ordinary_doc = LoroDoc::new();
    preview_doc.set_peer_id(77).unwrap();
    ordinary_doc.set_peer_id(77).unwrap();
    let _ = preview_doc.get_text("text");
    let _ = ordinary_doc.get_text("text");
    preview_doc.set_change_merge_interval(0);
    ordinary_doc.set_change_merge_interval(0);
    let mut preview_undo = UndoManager::new(&preview_doc);
    let mut ordinary_undo = UndoManager::new(&ordinary_doc);
    preview_undo.set_merge_interval(0);
    ordinary_undo.set_merge_interval(0);
    let mut rng = StdRng::seed_from_u64(0xD1FF_EA11);

    for step in 0..300 {
        let can_delete = !text(&preview_doc).is_empty();
        match rng.gen_range(0..100) {
            0..=54 => {
                let value = ((b'a' + (step % 26) as u8) as char).to_string();
                let len = preview_doc.get_text("text").len_unicode();
                let pos = rng.gen_range(0..=len);
                insert(&preview_doc.get_text("text"), pos, &value);
                insert(&ordinary_doc.get_text("text"), pos, &value);
                preview_doc.commit();
                ordinary_doc.commit();
            }
            55..=69 if can_delete => {
                let len = preview_doc.get_text("text").len_unicode();
                let pos = rng.gen_range(0..len);
                preview_doc.get_text("text").delete(pos, 1).unwrap();
                ordinary_doc.get_text("text").delete(pos, 1).unwrap();
                preview_doc.commit();
                ordinary_doc.commit();
            }
            70..=84 => {
                let preview = preview_undo.preview_undo().unwrap();
                let ordinary = ordinary_undo.undo().unwrap();
                let applied = match preview {
                    Some(preview) => preview_undo
                        .apply_preview(UndoOrRedo::Undo, preview)
                        .unwrap(),
                    None => false,
                };
                assert_eq!(applied, ordinary, "undo mismatch at step {step}");
            }
            _ => {
                let preview = preview_undo.preview_redo().unwrap();
                let ordinary = ordinary_undo.redo().unwrap();
                let applied = match preview {
                    Some(preview) => preview_undo
                        .apply_preview(UndoOrRedo::Redo, preview)
                        .unwrap(),
                    None => false,
                };
                assert_eq!(applied, ordinary, "redo mismatch at step {step}");
            }
        }

        assert_eq!(deep(&preview_doc), deep(&ordinary_doc), "step {step}");
        assert_eq!(
            preview_doc.oplog_vv(),
            ordinary_doc.oplog_vv(),
            "step {step}"
        );
        assert_eq!(preview_undo.undo_count(), ordinary_undo.undo_count());
        assert_eq!(preview_undo.redo_count(), ordinary_undo.redo_count());
    }
}
