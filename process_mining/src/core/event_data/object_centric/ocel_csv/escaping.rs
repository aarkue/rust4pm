//! Escaping for the characters that structure an `ot:<X>` cell.
//!
//! `/` separates references, `#` separates an object id from its qualifier, and `{` opens the
//! JSON attributes. A backslash escapes those three and itself. Anything else after a backslash
//! is a literal backslash, so a file written before this escape existed reads back unchanged.

/// The characters a backslash may escape.
const ESCAPABLE: [char; 4] = ['/', '#', '{', '\\'];

/// Writes an object id or qualifier so that [`unescape_reference_part`] gives it back.
pub fn escape_reference_part(part: &str) -> std::borrow::Cow<'_, str> {
    if !part.contains(ESCAPABLE) {
        return std::borrow::Cow::Borrowed(part);
    }
    let mut out = String::with_capacity(part.len() + 8);
    for c in part.chars() {
        if ESCAPABLE.contains(&c) {
            out.push('\\');
        }
        out.push(c);
    }
    std::borrow::Cow::Owned(out)
}

/// Reads back what [`escape_reference_part`] wrote.
pub fn unescape_reference_part(part: &str) -> std::borrow::Cow<'_, str> {
    if !part.contains('\\') {
        return std::borrow::Cow::Borrowed(part);
    }
    let mut out = String::with_capacity(part.len());
    let mut chars = part.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.clone().next() {
            Some(next) if ESCAPABLE.contains(&next) => {
                out.push(next);
                chars.next();
            }
            // A backslash that escapes nothing is itself.
            _ => out.push('\\'),
        }
    }
    std::borrow::Cow::Owned(out)
}

/// Byte index of the first unescaped `needle` in `haystack`.
pub(crate) fn find_unescaped(haystack: &str, needle: char) -> Option<usize> {
    let mut escaped = false;
    for (i, c) in haystack.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if c == '\\' {
            escaped = true;
        } else if c == needle {
            return Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reserved_character_survives_a_round_trip() {
        for part in [
            "plain",
            "a/b",
            "a#b",
            "a{b",
            "a\\b",
            "a/b#c{d\\e",
            "///",
            "\\",
            "ends with\\",
            "Ünïcödé",
        ] {
            let escaped = escape_reference_part(part);
            assert_eq!(
                unescape_reference_part(&escaped),
                part,
                "round trip of {part:?} through {escaped:?}"
            );
        }
    }

    /// A file written before the escape existed still reads the same.
    #[test]
    fn a_backslash_that_escapes_nothing_stays_a_backslash() {
        assert_eq!(unescape_reference_part("C:\\Users\\me"), "C:\\Users\\me");
        assert_eq!(unescape_reference_part("a\\nb"), "a\\nb");
        assert_eq!(unescape_reference_part("trailing\\"), "trailing\\");
    }

    #[test]
    fn a_separator_is_found_only_where_it_is_not_escaped() {
        assert_eq!(find_unescaped("a#b", '#'), Some(1));
        assert_eq!(find_unescaped("a\\#b", '#'), None);
        assert_eq!(find_unescaped("a\\#b#c", '#'), Some(4));
        assert_eq!(find_unescaped("\\\\#c", '#'), Some(2));
    }

    #[test]
    fn nothing_is_allocated_when_nothing_needs_escaping() {
        assert!(matches!(
            escape_reference_part("plain"),
            std::borrow::Cow::Borrowed(_)
        ));
        assert!(matches!(
            unescape_reference_part("plain"),
            std::borrow::Cow::Borrowed(_)
        ));
    }
}
