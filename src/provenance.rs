use crate::ingest::SessionClass;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MessageProvenance {
    pub authored_by: &'static str,
    pub sentiment_usable: &'static str,
    pub rule: &'static str,
}

pub(crate) fn classify_message(
    text: &str,
    repeated_template: bool,
    session_class: SessionClass,
    event_class: SessionClass,
) -> MessageProvenance {
    if event_class == SessionClass::Subagent || session_class == SessionClass::Subagent {
        return classified("agent", "no", "session.subagent");
    }
    if event_class == SessionClass::Automation || session_class == SessionClass::Automation {
        return classified("harness", "no", "session.automation");
    }

    let trimmed = text.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    for (prefix, authored_by, sentiment_usable, rule) in [
        ("<image name=", "human", "strip_wrapper", "tag.image"),
        (
            "<command-name>",
            "human",
            "strip_wrapper",
            "tag.command_name",
        ),
        ("<codex_delegation>", "agent", "no", "tag.codex_delegation"),
        (
            "<subagent_notification>",
            "harness",
            "no",
            "tag.subagent_notification",
        ),
        ("<turn_aborted>", "harness", "no", "tag.turn_aborted"),
        (
            "<environment_context>",
            "harness",
            "no",
            "tag.environment_context",
        ),
        ("<skill ", "harness", "no", "tag.skill"),
        (
            "<local-command-stdout>",
            "harness",
            "no",
            "tag.local_command_stdout",
        ),
        (
            "<local-command-caveat>",
            "harness",
            "no",
            "tag.local_command_caveat",
        ),
        (
            "<task-notification>",
            "harness",
            "no",
            "tag.task_notification",
        ),
        (
            "<user_shell_command>",
            "harness",
            "no",
            "tag.user_shell_command",
        ),
        ("<bash-stdout>", "harness", "no", "tag.bash_stdout"),
        ("<bash-input>", "harness", "no", "tag.bash_input"),
    ] {
        if lower.starts_with(prefix) {
            return classified(authored_by, sentiment_usable, rule);
        }
    }
    for (prefix, rule) in [
        ("caveat: the messages below", "prefix.claude_caveat"),
        (
            "the following tool was executed by the user",
            "prefix.user_tool_execution",
        ),
        (
            "warning: the maximum number of unified exec processes",
            "prefix.exec_limit",
        ),
    ] {
        if lower.starts_with(prefix) {
            return classified("harness", "no", rule);
        }
    }
    if repeated_template && text.chars().count() > 200 {
        return classified("agent", "no", "dup.template");
    }
    classified("human", "yes", "default.human")
}

fn classified(
    authored_by: &'static str,
    sentiment_usable: &'static str,
    rule: &'static str,
) -> MessageProvenance {
    MessageProvenance {
        authored_by,
        sentiment_usable,
        rule,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_all_machine_tag_and_prefix_rules() {
        for (text, authored_by, usable, rule) in [
            (
                "<subagent_notification>x",
                "harness",
                "no",
                "tag.subagent_notification",
            ),
            ("<turn_aborted>x", "harness", "no", "tag.turn_aborted"),
            (
                "<environment_context>x",
                "harness",
                "no",
                "tag.environment_context",
            ),
            ("<skill name='x'>", "harness", "no", "tag.skill"),
            (
                "<local-command-stdout>x",
                "harness",
                "no",
                "tag.local_command_stdout",
            ),
            (
                "<local-command-caveat>x",
                "harness",
                "no",
                "tag.local_command_caveat",
            ),
            (
                "<task-notification>x",
                "harness",
                "no",
                "tag.task_notification",
            ),
            ("<codex_delegation>x", "agent", "no", "tag.codex_delegation"),
            (
                "<user_shell_command>x",
                "harness",
                "no",
                "tag.user_shell_command",
            ),
            ("<bash-stdout>x", "harness", "no", "tag.bash_stdout"),
            ("<bash-input>x", "harness", "no", "tag.bash_input"),
            (
                "Caveat: The messages below were generated",
                "harness",
                "no",
                "prefix.claude_caveat",
            ),
            (
                "The following tool was executed by the user",
                "harness",
                "no",
                "prefix.user_tool_execution",
            ),
            (
                "Warning: The maximum number of unified exec processes was reached",
                "harness",
                "no",
                "prefix.exec_limit",
            ),
        ] {
            let result = classify_message(
                text,
                false,
                SessionClass::Interactive,
                SessionClass::Unknown,
            );
            assert_eq!(
                (result.authored_by, result.sentiment_usable, result.rule),
                (authored_by, usable, rule)
            );
        }
    }

    #[test]
    fn keeps_human_wrappers_but_marks_them_for_stripping() {
        for (text, rule) in [
            ("<image name='photo.png'>a real caption", "tag.image"),
            (
                "<command-name>/review</command-name>please focus on auth",
                "tag.command_name",
            ),
        ] {
            let result = classify_message(
                text,
                false,
                SessionClass::Interactive,
                SessionClass::Unknown,
            );
            assert_eq!(result.authored_by, "human");
            assert_eq!(result.sentiment_usable, "strip_wrapper");
            assert_eq!(result.rule, rule);
        }
    }

    #[test]
    fn session_override_precedes_message_rules() {
        let result = classify_message(
            "hello from a subagent",
            false,
            SessionClass::Subagent,
            SessionClass::Unknown,
        );
        assert_eq!(result.authored_by, "agent");
        assert_eq!(result.rule, "session.subagent");

        let result = classify_message(
            "<image name='x'>caption",
            false,
            SessionClass::Automation,
            SessionClass::Unknown,
        );
        assert_eq!(result.authored_by, "harness");
        assert_eq!(result.rule, "session.automation");
    }

    #[test]
    fn duplicate_rule_ignores_short_human_repeats() {
        let short = classify_message(
            "continue",
            true,
            SessionClass::Interactive,
            SessionClass::Unknown,
        );
        assert_eq!(short.rule, "default.human");

        let long = classify_message(
            &"repeated automation template ".repeat(12),
            true,
            SessionClass::Interactive,
            SessionClass::Unknown,
        );
        assert_eq!(long.rule, "dup.template");
        assert_eq!(long.authored_by, "agent");
    }
}
