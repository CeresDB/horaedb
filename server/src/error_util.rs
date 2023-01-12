// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub fn remove_backtrace_from_err(err_string: &str) -> &str {
    err_string
        .splitn(2, "Backtrace")
        .next()
        .unwrap_or(err_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_backtrace() {
        let cases = vec![
            (
                r#"Failed to execute select, err:Failed to execute logical plan, err:Failed to do physical optimization, err:DataFusion Failed to optimize physical plan, err:Error during planning.
Backtrace:
 0 <snafu::backtrace_shim::Backtrace as snafu::GenerateBacktrace>::generate::h996ee016dfa35e37"#,
                r#"Failed to execute select, err:Failed to execute logical plan, err:Failed to do physical optimization, err:DataFusion Failed to optimize physical plan, err:Error during planning.
"#,
            ),
            ("", ""),
            ("some error", "some error"),
        ];

        for (input, expected) in cases {
            assert_eq!(expected, remove_backtrace_from_err(&input));
        }
    }
}
