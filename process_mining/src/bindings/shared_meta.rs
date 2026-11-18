//! Binding metadata with the schemas' definitions collected in one place

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{list_functions_meta, BindingMeta};

const LOCAL_DEFS: &str = "#/$defs/";
const SHARED_DEFS: &str = "#/definitions/";

/// Binding metadata whose schemas share their definitions, see [`list_functions_meta_shared`]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BindingsMetaShared {
    /// The schemas the bindings' schemas refer to as `#/definitions/<name>`
    pub definitions: BTreeMap<String, Value>,
    /// The bindings, sorted by id
    pub functions: Vec<BindingMeta>,
}

/// [`list_functions_meta`], with the definitions of all schemas in one place
///
/// Each schema of [`list_functions_meta`] is standalone, with its own `$defs`, so a type that
/// several bindings use is repeated in each of them. Here, the schemas refer to
/// `#/definitions/<name>` in [`BindingsMetaShared::definitions`] instead (the draft-07 location)
/// and have no `$schema`. The argument and return schemas themselves stay inline, so their
/// `title` is where [`list_functions_meta`] has it.
///
/// Different definitions under the same name are kept apart by a number suffix (`Node2`).
pub fn list_functions_meta_shared() -> BindingsMetaShared {
    let mut functions = list_functions_meta();
    functions.sort_by(|a, b| a.id.cmp(&b.id));
    let mut definitions = BTreeMap::new();
    for meta in &mut functions {
        for (_, schema) in &mut meta.args {
            share_definitions(schema, &mut definitions);
        }
        share_definitions(&mut meta.return_type, &mut definitions);
    }
    BindingsMetaShared {
        definitions,
        functions,
    }
}

/// Move the `$defs` of a standalone schema into `shared`, and point its references there.
fn share_definitions(schema: &mut Value, shared: &mut BTreeMap<String, Value>) {
    let Some(root) = schema.as_object_mut() else {
        return;
    };
    root.remove("$schema");
    let mut local: BTreeMap<String, Value> = match root.remove("$defs") {
        Some(Value::Object(defs)) => defs.into_iter().collect(),
        _ => BTreeMap::new(),
    };
    // schemars refers to a recursive root type as `#`, which in the shared document would be the
    // whole document, so the root also becomes a definition. A definition can refer to it too.
    let root_is_referenced = has_ref(schema, "#") || local.values().any(|d| has_ref(d, "#"));
    let root_name = root_is_referenced.then(|| {
        let title = schema
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("Root");
        let name = (1..)
            .map(|i| suffixed(title, i))
            .find(|name| !local.contains_key(name))
            .unwrap();
        local.insert(name.clone(), schema.clone());
        name
    });
    let root_name = root_name.as_deref();

    // A definition can reuse a shared one of its name (`Name`, `Name2`, ...) that is the same up
    // to its references, as long as those can reuse the shared ones referenced in their place.
    // Ruling out candidates until none goes leaves the largest such matching, also for recursive
    // definitions.
    let mut candidates: BTreeMap<&str, Vec<String>> = local
        .keys()
        .map(|name| {
            let existing = (1..)
                .map(|i| suffixed(name, i))
                .take_while(|c| shared.contains_key(c))
                .collect();
            (name.as_str(), existing)
        })
        .collect();
    loop {
        let mut ruled_out = false;
        for (name, body) in &local {
            let kept: Vec<String> = candidates[name.as_str()]
                .iter()
                .filter(|c| {
                    same_up_to_refs(body, &shared[*c], root_name, &|local, shared| {
                        candidates
                            .get(local)
                            .is_some_and(|cs| cs.iter().any(|c| c == shared))
                    })
                })
                .cloned()
                .collect();
            if kept.len() < candidates[name.as_str()].len() {
                candidates.insert(name.as_str(), kept);
                ruled_out = true;
            }
        }
        if !ruled_out {
            break;
        }
    }

    let mut mapping: BTreeMap<String, String> = BTreeMap::new();
    for (name, cs) in &candidates {
        if let Some(c) = cs.first() {
            mapping.insert(name.to_string(), c.clone());
        }
    }
    for name in local.keys() {
        if !mapping.contains_key(name) {
            let fresh = (1..)
                .map(|i| suffixed(name, i))
                .find(|c| !shared.contains_key(c) && !mapping.values().any(|m| m == c))
                .unwrap();
            mapping.insert(name.clone(), fresh);
        }
    }
    for (name, body) in local {
        let target = &mapping[&name];
        if !shared.contains_key(target) {
            let body = rewrite_refs(body, &mapping, root_name);
            shared.insert(target.clone(), body);
        }
    }
    *schema = rewrite_refs(schema.take(), &mapping, root_name);
}

fn suffixed(name: &str, index: usize) -> String {
    if index == 1 {
        name.to_string()
    } else {
        format!("{}{}", name, index)
    }
}

fn has_ref(value: &Value, target: &str) -> bool {
    match value {
        Value::Object(map) => {
            map.get("$ref").and_then(Value::as_str) == Some(target)
                || map.values().any(|v| has_ref(v, target))
        }
        Value::Array(items) => items.iter().any(|v| has_ref(v, target)),
        _ => false,
    }
}

/// The local definition `reference` points to, with `#` naming the root stored as `root_name`
fn local_target(reference: &str, root_name: Option<&str>) -> Option<String> {
    match reference {
        "#" => root_name.map(str::to_string),
        r => r.strip_prefix(LOCAL_DEFS).map(unescape_pointer),
    }
}

/// Whether the local `a` equals the shared `b`, where a local reference to `x` and a shared one
/// to `y` count as equal if `refs_match(x, y)`
fn same_up_to_refs(
    a: &Value,
    b: &Value,
    root_name: Option<&str>,
    refs_match: &dyn Fn(&str, &str) -> bool,
) -> bool {
    match (a, b) {
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter().all(|(key, va)| {
                    let Some(vb) = b.get(key) else {
                        return false;
                    };
                    let local = va.as_str().and_then(|r| local_target(r, root_name));
                    match (key.as_str(), local, vb.as_str()) {
                        ("$ref", Some(local), Some(shared)) => shared
                            .strip_prefix(SHARED_DEFS)
                            .is_some_and(|s| refs_match(&local, &unescape_pointer(s))),
                        _ => same_up_to_refs(va, vb, root_name, refs_match),
                    }
                })
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(va, vb)| same_up_to_refs(va, vb, root_name, refs_match))
        }
        _ => a == b,
    }
}

/// Point the local references to the shared definitions `mapping` names for them
fn rewrite_refs(
    mut value: Value,
    mapping: &BTreeMap<String, String>,
    root_name: Option<&str>,
) -> Value {
    match &mut value {
        Value::Object(map) => {
            let target = map
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|r| local_target(r, root_name))
                .and_then(|name| mapping.get(&name));
            if let Some(target) = target {
                let reference = format!("{}{}", SHARED_DEFS, escape_pointer(target));
                map.insert("$ref".to_string(), reference.into());
            }
            for (key, v) in map.iter_mut() {
                if key != "$ref" {
                    *v = rewrite_refs(v.take(), mapping, root_name);
                }
            }
        }
        Value::Array(items) => {
            for v in items.iter_mut() {
                *v = rewrite_refs(v.take(), mapping, root_name);
            }
        }
        _ => {}
    }
    value
}

fn unescape_pointer(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

fn escape_pointer(name: &str) -> String {
    name.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Every `$ref` in `value`
    fn refs<'a>(value: &'a Value, out: &mut Vec<&'a str>) {
        match value {
            Value::Object(map) => {
                if let Some(r) = map.get("$ref").and_then(Value::as_str) {
                    out.push(r);
                }
                map.values().for_each(|v| refs(v, out));
            }
            Value::Array(items) => items.iter().for_each(|v| refs(v, out)),
            _ => {}
        }
    }

    #[test]
    fn shared_meta_resolves_every_ref() {
        let standalone = list_functions_meta();
        let shared = list_functions_meta_shared();
        assert_eq!(shared.functions.len(), standalone.len());
        let doc = serde_json::to_value(&shared).unwrap();
        let mut all = Vec::new();
        refs(&doc, &mut all);
        assert!(!all.is_empty());
        for r in all {
            let name = r
                .strip_prefix(SHARED_DEFS)
                .unwrap_or_else(|| panic!("{} does not point to the definitions", r));
            assert!(
                shared.definitions.contains_key(&unescape_pointer(name)),
                "{} is not defined",
                r
            );
        }
        let text = doc.to_string();
        assert!(!text.contains("\"$defs\""));
        assert!(!text.contains("\"$schema\""));
        // The titles hosts match return types by stay where they were
        for meta in &shared.functions {
            let before = standalone.iter().find(|m| m.id == meta.id).unwrap();
            assert_eq!(
                meta.return_type.get("title"),
                before.return_type.get("title")
            );
        }
        assert!(text.len() < serde_json::to_string(&standalone).unwrap().len());

        for meta in &shared.functions {
            let before = standalone.iter().find(|m| m.id == meta.id).unwrap();
            let pairs = before
                .args
                .iter()
                .map(|(_, s)| s)
                .zip(meta.args.iter().map(|(_, s)| s));
            for (before_schema, schema) in pairs.chain([(&before.return_type, &meta.return_type)]) {
                let mut before_root = before_schema.clone();
                let before_defs = before_root.as_object_mut().and_then(|o| {
                    o.remove("$schema");
                    o.remove("$defs")
                });
                let local = |r: &str| match r {
                    "#" => Some(before_root.clone()),
                    r => before_defs
                        .as_ref()?
                        .get(unescape_pointer(r.strip_prefix(LOCAL_DEFS)?))
                        .cloned(),
                };
                let global = |r: &str| {
                    let name = unescape_pointer(r.strip_prefix(SHARED_DEFS)?);
                    shared.definitions.get(&name).cloned()
                };
                assert_eq!(
                    expand(&before_root, &local, 4),
                    expand(schema, &global, 4),
                    "{}",
                    meta.id
                );
            }
        }
    }

    /// `value` with each `$ref` replaced by what it points to, `depth` levels deep
    fn expand(value: &Value, lookup: &dyn Fn(&str) -> Option<Value>, depth: usize) -> Value {
        match value {
            Value::Object(map) => {
                if let Some(r) = map.get("$ref").and_then(Value::as_str) {
                    if depth == 0 {
                        return json!("<ref>");
                    }
                    let target = lookup(r).unwrap_or_else(|| panic!("{} does not resolve", r));
                    let mut out = map.clone();
                    out.remove("$ref");
                    out.insert("<target>".to_string(), expand(&target, lookup, depth - 1));
                    return expand(&Value::Object(out), lookup, depth);
                }
                Value::Object(
                    map.iter()
                        .map(|(k, v)| (k.clone(), expand(v, lookup, depth)))
                        .collect(),
                )
            }
            Value::Array(items) => {
                Value::Array(items.iter().map(|v| expand(v, lookup, depth)).collect())
            }
            _ => value.clone(),
        }
    }

    #[test]
    fn same_name_different_definitions_are_kept_apart() {
        let mut shared = BTreeMap::new();
        let mut a = json!({
            "$schema": "x",
            "$ref": "#/$defs/Node",
            "$defs": { "Node": { "type": "string" } }
        });
        let mut b = json!({
            "type": "array",
            "items": { "$ref": "#/$defs/Node" },
            "$defs": { "Node": { "type": "object", "properties": { "next": { "$ref": "#/$defs/Node" } } } }
        });
        let mut c = a.clone();
        share_definitions(&mut a, &mut shared);
        share_definitions(&mut b, &mut shared);
        share_definitions(&mut c, &mut shared);
        assert_eq!(a, json!({ "$ref": "#/definitions/Node" }));
        assert_eq!(c, a);
        assert_eq!(b["items"], json!({ "$ref": "#/definitions/Node2" }));
        assert_eq!(
            shared["Node2"]["properties"]["next"],
            json!({ "$ref": "#/definitions/Node2" })
        );
        assert_eq!(shared["Node"], json!({ "type": "string" }));
    }

    #[test]
    fn recursive_root_becomes_a_definition() {
        let mut shared = BTreeMap::new();
        let mut schema = json!({
            "title": "Tree",
            "type": "object",
            "properties": { "children": { "type": "array", "items": { "$ref": "#" } } }
        });
        share_definitions(&mut schema, &mut shared);
        let item = json!({ "$ref": "#/definitions/Tree" });
        assert_eq!(schema["title"], "Tree");
        assert_eq!(schema["properties"]["children"]["items"], item);
        assert_eq!(shared["Tree"]["properties"]["children"]["items"], item);
    }

    #[allow(dead_code)]
    #[derive(JsonSchema)]
    struct Forest {
        trees: Vec<Grove>,
    }

    #[allow(dead_code)]
    #[derive(JsonSchema)]
    struct Grove {
        inner: Option<Box<Forest>>,
    }

    #[test]
    fn root_referenced_only_from_a_definition_becomes_a_definition() {
        let mut schema = serde_json::to_value(schemars::schema_for!(Forest)).unwrap();
        assert!(!has_ref(&schema["properties"], "#"));
        assert!(has_ref(&schema["$defs"], "#"));
        let mut shared = BTreeMap::new();
        share_definitions(&mut schema, &mut shared);
        let mut all = Vec::new();
        refs(&schema, &mut all);
        shared.values().for_each(|d| refs(d, &mut all));
        for r in all {
            let name = r.strip_prefix(SHARED_DEFS).unwrap();
            assert!(shared.contains_key(name), "{} is not defined", r);
        }
        assert!(has_ref(&shared["Grove"], "#/definitions/Forest"));
    }
}
