use std::fmt;

pub fn sanitize_component(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    write_sanitized_component(&mut out, input).expect("writing to String should not fail");
    out
}

pub fn write_sanitized_component<W: fmt::Write>(out: &mut W, input: &str) -> fmt::Result {
    for ch in input.chars() {
        if ch.is_whitespace() || ch == '|' || ch == '=' {
            out.write_char('_')?;
        } else {
            out.write_char(ch.to_ascii_lowercase())?;
        }
    }
    Ok(())
}

pub fn format_float(value: f64) -> String {
    format!("{value:.6}")
}

#[cfg(test)]
mod tests {
    use super::{format_float, sanitize_component};

    #[test]
    fn sanitize_component_lowercases_and_replaces_whitespace_and_delimiters() {
        let input = "HeLLo  WORLD\tOk|Key=Value";
        let output = sanitize_component(input);
        assert_eq!(output, "hello__world_ok_key_value");
    }

    #[test]
    fn sanitize_component_preserves_non_whitespace() {
        let input = "Metric-Name_123";
        let output = sanitize_component(input);
        assert_eq!(output, "metric-name_123");
    }

    #[test]
    fn format_float_formats_with_fixed_precision() {
        assert_eq!(format_float(1.2), "1.200000");
        assert_eq!(format_float(1.2345678), "1.234568");
    }
}
