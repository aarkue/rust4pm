//! Default values of binding arguments, as they appear in the argument schemas

use serde_json::Value;

/// Recursive schemas end here
const MAX_DEPTH: usize = 64;

/// `value` with every array sorted that `schema` declares a set (`uniqueItems`), so that a default
/// held in a `HashSet` is the same in every schema export.
pub fn sorted_sets(schema: &Value, mut value: Value) -> Value {
    sort_sets(schema, schema, &mut value, 0);
    value
}

fn sort_sets(schema: &Value, root: &Value, value: &mut Value, depth: usize) {
    if depth > MAX_DEPTH {
        return;
    }
    let schema = resolve(schema, root);
    match value {
        Value::Array(items) => {
            if let Some(item_schema) = schema.get("items") {
                for item in items.iter_mut() {
                    sort_sets(item_schema, root, item, depth + 1);
                }
            }
            if let Some(prefix) = schema.get("prefixItems").and_then(Value::as_array) {
                for (item, item_schema) in items.iter_mut().zip(prefix) {
                    sort_sets(item_schema, root, item, depth + 1);
                }
            }
            if schema.get("uniqueItems") == Some(&Value::Bool(true)) {
                items.sort_by_cached_key(|item| item.to_string());
            }
        }
        Value::Object(fields) => {
            if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
                for (name, field) in fields.iter_mut() {
                    if let Some(field_schema) = properties.get(name) {
                        sort_sets(field_schema, root, field, depth + 1);
                    }
                }
            }
        }
        _ => {}
    }
    for key in ["allOf", "anyOf", "oneOf"] {
        for variant in schema
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            sort_sets(variant, root, value, depth + 1);
        }
    }
}

/// The schema a `$ref` points to (`#` or `#/$defs/<name>`), or `schema` itself
fn resolve<'a>(schema: &'a Value, root: &'a Value) -> &'a Value {
    match schema.get("$ref").and_then(Value::as_str) {
        Some("#") => root,
        Some(reference) => reference
            .strip_prefix("#/$defs/")
            .and_then(|name| root.get("$defs")?.get(name))
            .unwrap_or(schema),
        None => schema,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sorts_sets_and_keeps_lists() {
        let schema = json!({
            "type": "object",
            "properties": {
                "set": { "type": "array", "uniqueItems": true, "items": { "type": "string" } },
                "list": { "type": "array", "items": { "type": "string" } },
                "nested": { "$ref": "#/$defs/Nested" }
            },
            "$defs": {
                "Nested": {
                    "type": "object",
                    "properties": {
                        "set": { "type": ["array", "null"], "uniqueItems": true, "items": { "type": "integer" } }
                    }
                }
            }
        });
        let value =
            json!({ "set": ["b", "a"], "list": ["b", "a"], "nested": { "set": [3, 1, 2] } });
        assert_eq!(
            sorted_sets(&schema, value),
            json!({ "set": ["a", "b"], "list": ["b", "a"], "nested": { "set": [1, 2, 3] } })
        );
    }
}
