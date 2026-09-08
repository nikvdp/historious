use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Action {
    Write {
        path: String,
        content: Option<String>,
    },
    Commit {
        cwd: Option<String>,
        message: MessageSource,
        followed_by_command: bool,
    },
    InvalidateWrites,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MessageSource {
    Inline(String),
    File(String),
    Unknown,
}

#[derive(Debug, Clone)]
struct Word {
    text: String,
    dynamic: bool,
    quoted: bool,
    unquoted: bool,
}

#[derive(Debug, Clone)]
enum Item {
    Word(Word),
    Op(String),
    Newline,
    HereDoc { content: Option<String> },
}

#[derive(Debug, Clone)]
struct PendingHereDoc {
    delimiter: String,
    literal: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedirectKind {
    Input,
    Truncate,
    Append,
}

#[derive(Debug, Clone)]
struct Redirect {
    kind: RedirectKind,
    target: Option<Word>,
    here_content: Option<Option<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Separator {
    And,
    Sequence,
}

#[derive(Debug, Clone)]
struct Segment {
    items: Vec<Item>,
    next: Option<Separator>,
}

/// Analyze the small, deliberately conservative shell subset used by coding agents.
///
/// This is not a shell interpreter. A command with unsupported control flow is a
/// write barrier rather than an invitation to guess which nested command ran.
pub(super) fn analyze(command: &str, cwd: Option<&str>) -> Vec<Action> {
    let items = match lex(command) {
        Ok(items) => items,
        Err(()) => return vec![Action::InvalidateWrites],
    };
    let segments = match split_segments(items) {
        Ok(segments) => segments,
        Err(()) => return vec![Action::InvalidateWrites],
    };

    let mut shell_cwd = cwd.and_then(normalize_cwd);
    let mut actions = Vec::new();
    for segment in segments {
        let start = actions.len();
        analyze_segment(&segment.items, &mut shell_cwd, &mut actions);
        for action in &mut actions[start..] {
            match action {
                Action::Write { content, .. } if segment.next == Some(Separator::Sequence) => {
                    *content = None;
                }
                Action::Commit {
                    followed_by_command,
                    ..
                } => {
                    *followed_by_command = segment.next.is_some();
                }
                _ => {}
            }
        }
    }
    actions
}

fn lex(input: &str) -> Result<Vec<Item>, ()> {
    let chars: Vec<char> = input.chars().collect();
    let mut items = Vec::new();
    let mut word = WordBuilder::default();
    let mut quote = None;
    let mut i = 0;
    let mut pending_heredoc = None;

    while i < chars.len() {
        let ch = chars[i];
        match quote {
            Some('\'') => {
                if ch == '\'' {
                    quote = None;
                } else {
                    word.text.push(ch);
                }
                i += 1;
            }
            Some('"') => {
                if ch == '"' {
                    quote = None;
                    i += 1;
                } else if ch == '\\' {
                    let Some(next) = chars.get(i + 1).copied() else {
                        return Err(());
                    };
                    if matches!(next, '$' | '`' | '"' | '\\' | '\n') {
                        if next != '\n' {
                            word.text.push(next);
                        }
                        i += 2;
                    } else {
                        word.text.push('\\');
                        word.text.push(next);
                        i += 2;
                    }
                } else {
                    if ch == '$' || ch == '`' {
                        word.dynamic = true;
                    }
                    word.text.push(ch);
                    i += 1;
                }
            }
            None => match ch {
                '\'' | '"' => {
                    word.quoted = true;
                    quote = Some(ch);
                    i += 1;
                }
                '\\' => {
                    let Some(next) = chars.get(i + 1).copied() else {
                        return Err(());
                    };
                    if next == '\n' {
                        i += 2;
                    } else {
                        word.text.push(next);
                        word.quoted = true;
                        i += 2;
                    }
                }
                ' ' | '\t' | '\r' => {
                    flush_word(&mut word, &mut items, &mut pending_heredoc)?;
                    i += 1;
                }
                '\n' => {
                    flush_word(&mut word, &mut items, &mut pending_heredoc)?;
                    if let Some(heredoc) = pending_heredoc.take() {
                        let (content, next) = consume_heredoc(&chars, i + 1, &heredoc.delimiter)?;
                        let has_chain_separator = items
                            .iter()
                            .any(|item| matches!(item, Item::Op(op) if op == "&&" || op == ";"));
                        let insert_at = items
                            .iter()
                            .position(
                                |item| matches!(item, Item::Op(op) if op == "&&" || op == ";"),
                            )
                            .unwrap_or(items.len());
                        items.insert(
                            insert_at,
                            Item::HereDoc {
                                content: heredoc.literal.then_some(content),
                            },
                        );
                        if !has_chain_separator {
                            items.push(Item::Newline);
                        }
                        i = next;
                    } else {
                        items.push(Item::Newline);
                        i += 1;
                    }
                }
                '&' => {
                    flush_word(&mut word, &mut items, &mut pending_heredoc)?;
                    if chars.get(i + 1) == Some(&'&') {
                        items.push(Item::Op("&&".to_string()));
                        i += 2;
                    } else {
                        return Err(());
                    }
                }
                '|' => {
                    flush_word(&mut word, &mut items, &mut pending_heredoc)?;
                    return Err(());
                }
                ';' => {
                    flush_word(&mut word, &mut items, &mut pending_heredoc)?;
                    items.push(Item::Op(";".to_string()));
                    i += 1;
                }
                '>' | '<' => {
                    let fd = word.fd_prefix();
                    flush_word(&mut word, &mut items, &mut pending_heredoc)?;
                    let (operator, consumed) = if ch == '>' {
                        if chars.get(i + 1) == Some(&'>') {
                            (">>".to_string(), 2)
                        } else if chars.get(i + 1) == Some(&'|') {
                            (">|".to_string(), 2)
                        } else {
                            (">".to_string(), 1)
                        }
                    } else if chars.get(i + 1) == Some(&'<') {
                        if chars.get(i + 2) == Some(&'<') {
                            return Err(());
                        }
                        if chars.get(i + 2) == Some(&'-') {
                            return Err(());
                        }
                        ("<<".to_string(), 2)
                    } else if chars.get(i + 1) == Some(&'>') {
                        return Err(());
                    } else {
                        ("<".to_string(), 1)
                    };
                    if let Some(fd) = fd {
                        items.push(Item::Op(format!("{fd}{operator}")));
                    } else {
                        items.push(Item::Op(operator.clone()));
                    }
                    if operator == "<<" {
                        if pending_heredoc.is_some() {
                            return Err(());
                        }
                        // The delimiter is installed when the next word is flushed.
                        pending_heredoc = Some(PendingHereDoc {
                            delimiter: String::new(),
                            literal: false,
                        });
                    }
                    i += consumed;
                }
                '`' | '$' | '*' | '?' | '[' | ']' | '~' => {
                    word.dynamic = true;
                    word.unquoted = true;
                    word.text.push(ch);
                    i += 1;
                }
                '#' => {
                    // Comments and shell syntax around them are intentionally not
                    // interpreted. Treating this as a dynamic word prevents a
                    // comment from accidentally becoming a recognized command.
                    word.dynamic = true;
                    word.unquoted = true;
                    word.text.push(ch);
                    i += 1;
                }
                '(' | ')' | '{' | '}' => {
                    flush_word(&mut word, &mut items, &mut pending_heredoc)?;
                    return Err(());
                }
                _ => {
                    word.unquoted = true;
                    if ch == '$' || ch == '`' {
                        word.dynamic = true;
                    }
                    word.text.push(ch);
                    i += 1;
                }
            },
            Some(_) => return Err(()),
        }
    }

    if quote.is_some() || pending_heredoc.is_some() {
        return Err(());
    }
    flush_word(&mut word, &mut items, &mut pending_heredoc)?;
    Ok(items)
}

#[derive(Default)]
struct WordBuilder {
    text: String,
    dynamic: bool,
    quoted: bool,
    unquoted: bool,
}

impl WordBuilder {
    fn fd_prefix(&self) -> Option<String> {
        (!self.text.is_empty() && self.unquoted && self.text.chars().all(|ch| ch.is_ascii_digit()))
            .then(|| self.text.clone())
    }

    fn take(&mut self) -> Word {
        let value = std::mem::take(self);
        Word {
            text: value.text,
            dynamic: value.dynamic,
            quoted: value.quoted,
            unquoted: value.unquoted,
        }
    }
}

fn flush_word(
    builder: &mut WordBuilder,
    items: &mut Vec<Item>,
    pending_heredoc: &mut Option<PendingHereDoc>,
) -> Result<(), ()> {
    if builder.text.is_empty() && !builder.quoted && !builder.unquoted {
        return Ok(());
    }
    let word = builder.take();
    if let Some(pending) = pending_heredoc.as_mut() {
        if pending.delimiter.is_empty() {
            if word.dynamic || word.text.is_empty() {
                return Err(());
            }
            pending.delimiter = word.text.clone();
            pending.literal = word.quoted && !word.unquoted;
        }
    }
    items.push(Item::Word(word));
    Ok(())
}

fn consume_heredoc(
    chars: &[char],
    mut start: usize,
    delimiter: &str,
) -> Result<(String, usize), ()> {
    let delimiter: Vec<char> = delimiter.chars().collect();
    let mut content = String::new();
    while start <= chars.len() {
        let mut end = start;
        while end < chars.len() && chars[end] != '\n' {
            end += 1;
        }
        let mut line_end = end;
        if line_end > start && chars[line_end - 1] == '\r' {
            line_end -= 1;
        }
        if chars[start..line_end] == delimiter[..] {
            let next = (end < chars.len()).then_some(end + 1).unwrap_or(end);
            return Ok((content, next));
        }
        content.extend(chars[start..end].iter());
        if end < chars.len() {
            content.push('\n');
        }
        if end == chars.len() {
            break;
        }
        start = end + 1;
    }
    Err(())
}

fn split_segments(items: Vec<Item>) -> Result<Vec<Segment>, ()> {
    let mut segments = Vec::new();
    let mut current = Vec::new();
    for item in items {
        match item {
            Item::Op(op) if op == "&&" || op == ";" => {
                if current.is_empty() {
                    return Err(());
                }
                segments.push(Segment {
                    items: std::mem::take(&mut current),
                    next: Some(if op == "&&" {
                        Separator::And
                    } else {
                        Separator::Sequence
                    }),
                });
            }
            Item::Newline => {
                if !current.is_empty() {
                    segments.push(Segment {
                        items: std::mem::take(&mut current),
                        next: Some(Separator::Sequence),
                    });
                }
            }
            item => current.push(item),
        }
    }
    if !current.is_empty() {
        segments.push(Segment {
            items: current,
            next: None,
        });
    } else if let Some(last) = segments.last_mut() {
        if last.next == Some(Separator::And) {
            return Err(());
        }
        // A trailing semicolon/newline does not introduce another command.
        last.next = None;
    }
    Ok(segments)
}

fn analyze_segment(segment: &[Item], shell_cwd: &mut Option<String>, actions: &mut Vec<Action>) {
    let (words, redirects) = match command_parts(segment) {
        Some(parts) => parts,
        None => {
            actions.push(Action::InvalidateWrites);
            return;
        }
    };
    if words.is_empty() {
        return;
    }

    let output_redirects: Vec<&Redirect> = redirects
        .iter()
        .filter(|redirect| redirect.kind == RedirectKind::Truncate)
        .collect();
    let append_redirects: Vec<&Redirect> = redirects
        .iter()
        .filter(|redirect| redirect.kind == RedirectKind::Append)
        .collect();

    let program = words[0].text.rsplit('/').next().unwrap_or(&words[0].text);
    match program {
        "cd" => {
            if !output_redirects.is_empty() || !append_redirects.is_empty() {
                if !emit_unknown_redirects(&redirects, shell_cwd.as_deref(), actions) {
                    actions.push(Action::InvalidateWrites);
                }
            }
            if words.len() != 2 || words[1].dynamic {
                *shell_cwd = None;
                actions.push(Action::InvalidateWrites);
                return;
            }
            let Some(next) = resolve_path(&words[1].text, shell_cwd.as_deref()) else {
                *shell_cwd = None;
                actions.push(Action::InvalidateWrites);
                return;
            };
            *shell_cwd = Some(next);
        }
        "git" => analyze_git(&words, &redirects, shell_cwd.as_deref(), actions),
        "cat" => analyze_cat(&words, &redirects, shell_cwd.as_deref(), actions),
        "printf" => analyze_printf(&words, &redirects, shell_cwd.as_deref(), actions),
        "echo" => analyze_echo(&words, &redirects, shell_cwd.as_deref(), actions),
        "pwd" => analyze_pwd(&words, &redirects, shell_cwd.as_deref(), actions),
        "true" | ":" | "read" | "test" => {
            if (!output_redirects.is_empty() || !append_redirects.is_empty())
                && !emit_unknown_redirects(&redirects, shell_cwd.as_deref(), actions)
            {
                actions.push(Action::InvalidateWrites);
            }
        }
        _ => {
            if (output_redirects.is_empty() && append_redirects.is_empty())
                || !emit_unknown_redirects(&redirects, shell_cwd.as_deref(), actions)
            {
                actions.push(Action::InvalidateWrites);
            }
        }
    }
}

fn command_parts(segment: &[Item]) -> Option<(Vec<Word>, Vec<Redirect>)> {
    let mut words = Vec::new();
    let mut redirects = Vec::new();
    let mut heredocs = segment.iter().filter_map(|item| match item {
        Item::HereDoc { content } => Some(content.clone()),
        _ => None,
    });
    let mut i = 0;
    while i < segment.len() {
        match &segment[i] {
            Item::Word(word) => words.push(word.clone()),
            Item::HereDoc { .. } => {}
            Item::Newline => return None,
            Item::Op(operator) => {
                let (kind, has_target) = match strip_fd(operator) {
                    Some((">", _)) | Some((">|", _)) => (Some(RedirectKind::Truncate), true),
                    Some((">>", _)) => (Some(RedirectKind::Append), true),
                    Some(("<", _)) => (Some(RedirectKind::Input), true),
                    Some(("<<", _)) => (Some(RedirectKind::Input), false),
                    _ => return None,
                };
                if operator.ends_with("<<") {
                    let Some(Item::Word(_delimiter)) = segment.get(i + 1) else {
                        return None;
                    };
                    redirects.push(Redirect {
                        kind: RedirectKind::Input,
                        target: None,
                        here_content: heredocs.next(),
                    });
                    i += 2;
                    continue;
                }
                if !has_target {
                    return None;
                }
                let target = segment.get(i + 1).and_then(|item| match item {
                    Item::Word(word) => Some(word.clone()),
                    _ => None,
                })?;
                redirects.push(Redirect {
                    kind: kind?,
                    target: Some(target),
                    here_content: None,
                });
                i += 2;
                continue;
            }
        }
        i += 1;
    }
    Some((words, redirects))
}

fn strip_fd(operator: &str) -> Option<(&str, Option<&str>)> {
    let mut split = 0;
    while split < operator.len() && operator.as_bytes()[split].is_ascii_digit() {
        split += 1;
    }
    let base = &operator[split..];
    matches!(base, ">" | ">|" | ">>" | "<" | "<<")
        .then_some((base, (split > 0).then_some(&operator[..split])))
}

fn analyze_git(
    words: &[Word],
    redirects: &[Redirect],
    shell_cwd: Option<&str>,
    actions: &mut Vec<Action>,
) {
    if !emit_unknown_redirects(redirects, shell_cwd, actions) {
        actions.push(Action::InvalidateWrites);
        return;
    }

    let Some((subcommand, commit_cwd, subcommand_index)) = git_subcommand(words, shell_cwd) else {
        actions.push(Action::InvalidateWrites);
        return;
    };
    match subcommand {
        "commit" => {
            let message =
                parse_commit_message(&words[subcommand_index + 1..], commit_cwd.as_deref());
            actions.push(Action::Commit {
                cwd: commit_cwd,
                message,
                followed_by_command: false,
            });
        }
        "add" | "status" | "diff" | "log" | "show" | "rev-parse" => {}
        _ => actions.push(Action::InvalidateWrites),
    }
}

fn git_subcommand<'a>(
    words: &'a [Word],
    shell_cwd: Option<&str>,
) -> Option<(&'a str, Option<String>, usize)> {
    if words.first()?.text.rsplit('/').next()? != "git" {
        return None;
    }
    let mut cwd = shell_cwd.map(str::to_string);
    let mut i = 1;
    while i < words.len() {
        let text = words[i].text.as_str();
        if text == "--" {
            return None;
        }
        if text == "-C" {
            let path = words.get(i + 1)?;
            cwd = (!path.dynamic)
                .then(|| resolve_path(&path.text, cwd.as_deref()))
                .flatten();
            i += 2;
            continue;
        }
        if let Some(path) = text.strip_prefix("-C").filter(|path| !path.is_empty()) {
            cwd = (!words[i].dynamic)
                .then(|| resolve_path(path, cwd.as_deref()))
                .flatten();
            i += 1;
            continue;
        }
        if text == "--no-pager" || text == "--paginate" || text == "--no-replace-objects" {
            i += 1;
            continue;
        }
        if text == "-c" || text == "--config-env" {
            i += 2;
            continue;
        }
        if text.starts_with('-') {
            i += 1;
            continue;
        }
        return Some((text, cwd, i));
    }
    None
}

fn parse_commit_message(words: &[Word], commit_cwd: Option<&str>) -> MessageSource {
    let mut messages = Vec::new();
    let mut file = None;
    let mut unknown = false;
    let mut i = 0;
    let mut end_options = false;

    while i < words.len() {
        let word = &words[i];
        let text = word.text.as_str();
        if end_options {
            i += 1;
            continue;
        }
        if text == "--" {
            end_options = true;
            i += 1;
            continue;
        }

        if text == "-m" || text == "--message" {
            if let Some(value) = words.get(i + 1) {
                if value.dynamic {
                    unknown = true;
                } else {
                    messages.push(value.text.clone());
                }
                i += 2;
            } else {
                unknown = true;
                i += 1;
            }
            continue;
        }
        if let Some(value) = text.strip_prefix("--message=") {
            if word.dynamic {
                unknown = true;
            } else {
                messages.push(value.to_string());
            }
            i += 1;
            continue;
        }
        if text == "-F" || text == "--file" {
            if let Some(value) = words.get(i + 1) {
                if value.dynamic || value.text == "-" {
                    unknown = true;
                } else {
                    file = resolve_path(&value.text, commit_cwd);
                    if file.is_none() {
                        unknown = true;
                    }
                }
                i += 2;
            } else {
                unknown = true;
                i += 1;
            }
            continue;
        }
        if let Some(value) = text.strip_prefix("--file=") {
            if word.dynamic || value == "-" {
                unknown = true;
            } else {
                file = resolve_path(value, commit_cwd);
                if file.is_none() {
                    unknown = true;
                }
            }
            i += 1;
            continue;
        }

        if !text.starts_with('-') {
            i += 1;
            continue;
        }

        if text == "-C" || text == "--reuse-message" || text == "--reedit-message" {
            unknown = true;
            i += 2;
            continue;
        }
        if text.starts_with("--reuse-message=")
            || text.starts_with("--reedit-message=")
            || text.starts_with("--template=")
            || text == "--template"
            || text.starts_with("--fixup")
            || text.starts_with("--squash")
        {
            unknown = true;
            i += usize::from(text == "--template");
            i += 1;
            continue;
        }

        if text.starts_with("--") {
            // Long options not carrying a recoverable message are safe to skip
            // here; provenance remains Unknown unless -m/-F supplied it.
            i += 1;
            continue;
        }

        if let Some(cluster) = text.strip_prefix('-') {
            let chars: Vec<char> = cluster.chars().collect();
            let mut j = 0;
            while j < chars.len() {
                match chars[j] {
                    'm' | 'F' => {
                        let attached: String = chars[j + 1..].iter().collect();
                        if attached.is_empty() {
                            if let Some(value) = words.get(i + 1) {
                                if chars[j] == 'm' {
                                    if value.dynamic {
                                        unknown = true;
                                    } else {
                                        messages.push(value.text.clone());
                                    }
                                } else if value.dynamic || value.text == "-" {
                                    unknown = true;
                                } else {
                                    file = resolve_path(&value.text, commit_cwd);
                                    if file.is_none() {
                                        unknown = true;
                                    }
                                }
                                i += 1;
                            } else {
                                unknown = true;
                            }
                        } else if chars[j] == 'm' {
                            if word.dynamic {
                                unknown = true;
                            } else {
                                messages.push(attached);
                            }
                        } else if word.dynamic || attached == "-" {
                            unknown = true;
                        } else {
                            file = resolve_path(&attached, commit_cwd);
                            if file.is_none() {
                                unknown = true;
                            }
                        }
                        break;
                    }
                    _ => j += 1,
                }
            }
        }
        i += 1;
    }

    if unknown || (file.is_some() && !messages.is_empty()) {
        MessageSource::Unknown
    } else if let Some(file) = file {
        MessageSource::File(file)
    } else if !messages.is_empty() {
        MessageSource::Inline(messages.join("\n\n"))
    } else {
        MessageSource::Unknown
    }
}

fn analyze_cat(
    words: &[Word],
    redirects: &[Redirect],
    shell_cwd: Option<&str>,
    actions: &mut Vec<Action>,
) {
    let outputs: Vec<&Redirect> = redirects
        .iter()
        .filter(|redirect| matches!(redirect.kind, RedirectKind::Truncate | RedirectKind::Append))
        .collect();
    let truncates = outputs
        .iter()
        .filter(|redirect| redirect.kind == RedirectKind::Truncate)
        .count();
    let appends = outputs
        .iter()
        .any(|redirect| redirect.kind == RedirectKind::Append);
    if outputs.is_empty() {
        return;
    }
    let Some(path) = one_redirect_path(&outputs, shell_cwd) else {
        actions.push(Action::InvalidateWrites);
        return;
    };
    let here = redirects
        .iter()
        .find_map(|redirect| redirect.here_content.as_ref());
    let only_cat_stdin = words.len() == 1 || (words.len() == 2 && words[1].text == "--");
    let content = if !appends && truncates == 1 && only_cat_stdin {
        here.cloned().flatten()
    } else {
        None
    };
    actions.push(Action::Write { path, content });
}

fn analyze_printf(
    words: &[Word],
    redirects: &[Redirect],
    shell_cwd: Option<&str>,
    actions: &mut Vec<Action>,
) {
    let outputs: Vec<&Redirect> = redirects
        .iter()
        .filter(|redirect| matches!(redirect.kind, RedirectKind::Truncate | RedirectKind::Append))
        .collect();
    let truncates = outputs
        .iter()
        .filter(|redirect| redirect.kind == RedirectKind::Truncate)
        .count();
    let appends = outputs
        .iter()
        .any(|redirect| redirect.kind == RedirectKind::Append);
    if outputs.is_empty() {
        return;
    }
    let Some(path) = one_redirect_path(&outputs, shell_cwd) else {
        actions.push(Action::InvalidateWrites);
        return;
    };
    let content = if !appends && truncates == 1 {
        parse_printf(words)
    } else {
        None
    };
    actions.push(Action::Write { path, content });
}

fn analyze_echo(
    words: &[Word],
    redirects: &[Redirect],
    shell_cwd: Option<&str>,
    actions: &mut Vec<Action>,
) {
    let outputs: Vec<&Redirect> = redirects
        .iter()
        .filter(|redirect| matches!(redirect.kind, RedirectKind::Truncate | RedirectKind::Append))
        .collect();
    let truncates = outputs
        .iter()
        .filter(|redirect| redirect.kind == RedirectKind::Truncate)
        .count();
    let appends = outputs
        .iter()
        .any(|redirect| redirect.kind == RedirectKind::Append);
    if outputs.is_empty() {
        return;
    }
    let Some(path) = one_redirect_path(&outputs, shell_cwd) else {
        actions.push(Action::InvalidateWrites);
        return;
    };
    let content = if !appends
        && truncates == 1
        && words[1..].iter().all(|word| !word.dynamic)
        && !words.get(1).is_some_and(|word| word.text.starts_with('-'))
    {
        let mut text = words[1..]
            .iter()
            .map(|word| word.text.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        text.push('\n');
        Some(text)
    } else {
        None
    };
    actions.push(Action::Write { path, content });
}

fn analyze_pwd(
    words: &[Word],
    redirects: &[Redirect],
    shell_cwd: Option<&str>,
    actions: &mut Vec<Action>,
) {
    let outputs: Vec<&Redirect> = redirects
        .iter()
        .filter(|redirect| matches!(redirect.kind, RedirectKind::Truncate | RedirectKind::Append))
        .collect();
    let truncates = outputs
        .iter()
        .filter(|redirect| redirect.kind == RedirectKind::Truncate)
        .count();
    let appends = outputs
        .iter()
        .any(|redirect| redirect.kind == RedirectKind::Append);
    if words.len() != 1 || outputs.is_empty() {
        if !outputs.is_empty() && !emit_unknown_redirects(redirects, shell_cwd, actions) {
            actions.push(Action::InvalidateWrites);
        }
        return;
    }
    let Some(path) = one_redirect_path(&outputs, shell_cwd) else {
        actions.push(Action::InvalidateWrites);
        return;
    };
    let content = if !appends && truncates == 1 {
        shell_cwd.map(|cwd| format!("{cwd}\n"))
    } else {
        None
    };
    actions.push(Action::Write { path, content });
}
fn parse_printf(words: &[Word]) -> Option<String> {
    let arguments = &words[1..];
    if arguments.is_empty() || arguments.iter().any(|word| word.dynamic) {
        return None;
    }
    let format = &arguments[0].text;
    if let Some(rest) = format.strip_prefix("%s") {
        if arguments.len() != 2 {
            return None;
        }
        let suffix = decode_printf_escapes(rest)?;
        return Some(format!("{}{}", arguments[1].text, suffix));
    }
    if format.contains('%') {
        return None;
    }
    decode_printf_escapes(format)
}

fn decode_printf_escapes(text: &str) -> Option<String> {
    let mut out = String::new();
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next()? {
            '\\' => out.push('\\'),
            'a' => out.push('\x07'),
            'b' => out.push('\x08'),
            'f' => out.push('\x0c'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'v' => out.push('\x0b'),
            _ => return None,
        }
    }
    Some(out)
}

fn one_redirect_path(redirects: &[&Redirect], shell_cwd: Option<&str>) -> Option<String> {
    let redirect = redirects.first()?;
    if redirects.len() != 1 {
        return None;
    }
    let target = redirect.target.as_ref()?;
    if target.dynamic {
        return None;
    }
    resolve_path(&target.text, shell_cwd)
}

fn emit_unknown_redirects(
    redirects: &[Redirect],
    shell_cwd: Option<&str>,
    actions: &mut Vec<Action>,
) -> bool {
    let mut known = true;
    for redirect in redirects {
        if redirect.kind != RedirectKind::Truncate && redirect.kind != RedirectKind::Append {
            continue;
        }
        let Some(target) = redirect.target.as_ref() else {
            known = false;
            continue;
        };
        let Some(path) = (!target.dynamic)
            .then(|| resolve_path(&target.text, shell_cwd))
            .flatten()
        else {
            known = false;
            continue;
        };
        actions.push(Action::Write {
            path,
            content: None,
        });
    }
    known
}

fn normalize_cwd(cwd: &str) -> Option<String> {
    let path = Path::new(cwd);
    path.is_absolute().then(|| clean_absolute(path))
}

fn resolve_path(path: &str, cwd: Option<&str>) -> Option<String> {
    if path.is_empty() {
        return None;
    }
    let path = Path::new(path);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        let cwd = cwd.filter(|cwd| Path::new(cwd).is_absolute())?;
        Path::new(cwd).join(path)
    };
    Some(clean_absolute(&absolute))
}

fn clean_absolute(path: &Path) -> String {
    let mut cleaned = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::Prefix(_) => cleaned.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if cleaned.components().count() > 1 {
                    cleaned.pop();
                }
            }
            Component::Normal(value) => cleaned.push(value),
        }
    }
    cleaned.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_evidence_command_file_quoted_filename() {
        let actions = analyze(
            "printf '%s' 'subject' > 'message file' && git commit -F 'message file' -- src/lib.rs",
            Some("/repo"),
        );
        assert_eq!(
            actions,
            vec![
                Action::Write {
                    path: "/repo/message file".to_string(),
                    content: Some("subject".to_string()),
                },
                Action::Commit {
                    cwd: Some("/repo".to_string()),
                    message: MessageSource::File("/repo/message file".to_string()),
                    followed_by_command: false,
                },
            ]
        );
    }

    #[test]
    fn commit_evidence_command_multiple_message_paragraphs() {
        let actions = analyze(
            "git commit -m 'first && paragraph' --message=second -- file.txt",
            Some("/repo"),
        );
        assert_eq!(
            actions,
            vec![Action::Commit {
                cwd: Some("/repo".to_string()),
                message: MessageSource::Inline("first && paragraph\n\nsecond".to_string()),
                followed_by_command: false,
            }]
        );
    }

    #[test]
    fn commit_evidence_command_git_c_and_chain_ordering() {
        let actions = analyze(
            "cd /workspace && printf '%s' 'body' > msg && git -C project commit --amend -m 'done'",
            Some("/tmp"),
        );
        assert_eq!(
            actions,
            vec![
                Action::Write {
                    path: "/workspace/msg".to_string(),
                    content: Some("body".to_string()),
                },
                Action::Commit {
                    cwd: Some("/workspace/project".to_string()),
                    message: MessageSource::Inline("done".to_string()),
                    followed_by_command: false,
                },
            ]
        );
    }

    #[test]
    fn commit_evidence_command_literal_heredoc() {
        let actions = analyze(
            "cat <<'EOF' > msg && git commit -F msg\nsubject\n\nbody\nEOF\n",
            Some("/repo"),
        );
        assert_eq!(
            actions,
            vec![
                Action::Write {
                    path: "/repo/msg".to_string(),
                    content: Some("subject\n\nbody\n".to_string()),
                },
                Action::Commit {
                    cwd: Some("/repo".to_string()),
                    message: MessageSource::File("/repo/msg".to_string()),
                    followed_by_command: false,
                },
            ]
        );
    }

    #[test]
    fn commit_evidence_command_semicolon_write_is_unknown() {
        let actions = analyze("printf '%s' 'old' > msg; git commit -F msg", Some("/repo"));
        assert_eq!(
            actions,
            vec![
                Action::Write {
                    path: "/repo/msg".to_string(),
                    content: None,
                },
                Action::Commit {
                    cwd: Some("/repo".to_string()),
                    message: MessageSource::File("/repo/msg".to_string()),
                    followed_by_command: false,
                },
            ]
        );
    }

    #[test]
    fn commit_evidence_command_unknown_mutation_barrier() {
        let actions = analyze(
            "printf '%s' 'old' > msg && eval 'printf new > msg' && git commit -F msg",
            Some("/repo"),
        );
        assert_eq!(
            actions,
            vec![
                Action::Write {
                    path: "/repo/msg".to_string(),
                    content: Some("old".to_string()),
                },
                Action::InvalidateWrites,
                Action::Commit {
                    cwd: Some("/repo".to_string()),
                    message: MessageSource::File("/repo/msg".to_string()),
                    followed_by_command: false,
                },
            ]
        );
    }

    #[test]
    fn commit_evidence_command_unsafe_substitution_has_no_fabricated_message() {
        let actions = analyze("git commit -m \"$(printf secret)\"", Some("/repo"));
        assert_eq!(
            actions,
            vec![Action::Commit {
                cwd: Some("/repo".to_string()),
                message: MessageSource::Unknown,
                followed_by_command: false,
            }]
        );
    }

    #[test]
    fn commit_evidence_command_pipeline_is_not_a_commit() {
        assert_eq!(
            analyze("git commit -m literal | tee log", Some("/repo")),
            vec![Action::InvalidateWrites]
        );
    }
}
