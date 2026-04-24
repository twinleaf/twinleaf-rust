//! Interactive RPC palette widget for embedding in ratatui TUIs.
//!
//! Hosts (e.g. `tio monitor`, `tio health`) own an [`RpcPalette`], route key
//! events to [`RpcPalette::handle_key`], and render it with
//! [`RpcPalette::render`]. The palette emits [`PaletteEvent`]s for submit /
//! exit / route-switch; the host owns the RPC worker plumbing and mode
//! transitions.

use std::cmp::min;

use nucleo_matcher::{Config, Matcher, Utf32Str};
use ratatui::{
    crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    layout::{Constraint, Direction, Layout, Rect},
    prelude::Stylize,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, Paragraph},
    Frame,
};
use tio::proto::DeviceRoute;
use tui_prompts::{State, TextState};
use twinleaf::{
    device::{RpcDescriptor, RpcRegistry},
    tio,
};

const RPCLIST_MAX_LEN: usize = 12;

#[derive(Debug, Clone)]
pub struct RpcReq {
    pub route: DeviceRoute,
    pub meta: Option<u16>,
    pub method: String,
    pub arg: Option<String>,
}

#[derive(Debug)]
pub struct RpcResp {
    pub result: Result<String, String>,
}

/// Outcome of [`RpcPalette::handle_key`]. The host interprets this to decide
/// whether to dispatch an RPC, switch routes, exit palette mode, or do nothing.
pub enum PaletteEvent {
    /// Key was consumed; no host-level action.
    Consumed,
    /// User asked to execute this RPC.
    Submit(RpcReq),
    /// User picked a different route from the route picker; host should
    /// navigate its selection cursor to that device.
    SelectRoute(DeviceRoute),
    /// User asked to exit the palette (Esc on empty input, or Ctrl+C).
    Exit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Zone {
    Empty,
    RpcName,
    Arg,
}

struct Suggestion {
    name: String,
    positions: Vec<u32>,
}

/// One pre-accept snapshot. `accepted_name` is the suggestion that was
/// accepted; on pop we re-select it in the restored list so users land
/// exactly where they were.
struct UndoEntry {
    input: String,
    accepted_name: String,
}

pub struct RpcPalette {
    pub input_state: TextState<'static>,
    suggestions: Vec<Suggestion>,
    selected: Option<usize>,
    scroll: usize,
    pub last_rpc_result: Option<(String, Color)>,
    pub last_rpc_command: String,
    in_flight: bool,
    history: Vec<String>,
    picker: Option<HistoryPicker>,
    route_picker: Option<RoutePicker>,
    /// Snapshots of input state pushed by each accept (Right/Tab). Left pops
    /// one to step back to a prior filter view.
    undo_stack: Vec<UndoEntry>,
    matcher: Matcher,
}

impl Default for RpcPalette {
    fn default() -> Self {
        Self {
            input_state: TextState::default(),
            suggestions: Vec::new(),
            selected: None,
            scroll: 0,
            last_rpc_result: None,
            last_rpc_command: String::new(),
            in_flight: false,
            history: Vec::new(),
            picker: None,
            route_picker: None,
            undo_stack: Vec::new(),
            matcher: Matcher::new(Config::DEFAULT),
        }
    }
}

/// Sub-mode: Ctrl+R opens a filterable list of past submitted commands.
#[derive(Debug, Default)]
struct HistoryPicker {
    /// Query text typed into the picker's own filter line.
    query: String,
    /// Indices into `RpcPalette.history`, newest-first, matching `query`.
    filtered: Vec<usize>,
    /// Position within `filtered`.
    selected: usize,
}

impl HistoryPicker {
    fn refilter(&mut self, history: &[String]) {
        let q = self.query.as_str();
        self.filtered = history
            .iter()
            .enumerate()
            .rev()
            .filter(|(_, h)| q.is_empty() || h.contains(q))
            .map(|(i, _)| i)
            .collect();
        self.selected = self.selected.min(self.filtered.len().saturating_sub(1));
    }

    fn selected_entry<'a>(&self, history: &'a [String]) -> Option<&'a str> {
        self.filtered
            .get(self.selected)
            .and_then(|&i| history.get(i))
            .map(|s| s.as_str())
    }
}

/// Sub-mode: Left on empty input opens this to pick an active device route.
struct RoutePicker {
    routes: Vec<DeviceRoute>,
    selected: usize,
}

impl RpcPalette {
    pub fn new() -> Self {
        Self::default()
    }

    /// Focus the input and repopulate suggestions. Call when entering RPC mode.
    pub fn enter(&mut self, registry: Option<&RpcRegistry>) {
        self.input_state.focus();
        self.update_suggestions(registry);
    }

    /// Blur the input. Call when leaving RPC mode.
    pub fn exit(&mut self) {
        self.input_state.blur();
        self.route_picker = None;
        self.undo_stack.clear();
    }

    /// Number of suggestion rows to display (without borders). Reflects the
    /// active sub-picker if one is open.
    pub fn suggestion_rows(&self) -> u16 {
        let len = if let Some(picker) = &self.picker {
            picker.filtered.len().max(1).min(RPCLIST_MAX_LEN)
        } else if let Some(rp) = &self.route_picker {
            rp.routes.len().max(1).min(RPCLIST_MAX_LEN)
        } else if self.current_zone() == Zone::Arg {
            1
        } else if self.suggestions.is_empty() {
            1
        } else {
            self.suggestions.len().min(RPCLIST_MAX_LEN)
        };
        len.try_into().unwrap_or(u16::MAX)
    }

    pub fn last_rpc_command(&self) -> &str {
        &self.last_rpc_command
    }

    pub fn set_rpc_result(&mut self, msg: String, color: Color) {
        self.last_rpc_result = Some((msg, color));
        self.in_flight = false;
    }

    /// Dispatch a key event. The host should call this for every key while the
    /// palette is active; unhandled readline-style edits are routed through to
    /// the underlying [`TextState`].
    pub fn handle_key(
        &mut self,
        key: KeyEvent,
        registry: Option<&RpcRegistry>,
        route: &DeviceRoute,
        available_routes: &[DeviceRoute],
        footer_height: u16,
    ) -> PaletteEvent {
        if key.kind != KeyEventKind::Press {
            return PaletteEvent::Consumed;
        }
        // Ctrl+C exits the palette regardless of sub-mode.
        if matches!(key.code, KeyCode::Char('c')) && key.modifiers == KeyModifiers::CONTROL {
            return PaletteEvent::Exit;
        }
        if self.picker.is_some() {
            self.handle_picker_key(key, registry);
            return PaletteEvent::Consumed;
        }
        if self.route_picker.is_some() {
            return self.handle_route_picker_key(key);
        }

        let zone = self.current_zone();
        match key.code {
            KeyCode::Char('r') if key.modifiers == KeyModifiers::CONTROL => {
                self.open_history_picker();
                PaletteEvent::Consumed
            }
            KeyCode::Esc => {
                if self.input_state.value().is_empty() {
                    PaletteEvent::Exit
                } else {
                    self.clear_input(registry);
                    PaletteEvent::Consumed
                }
            }
            KeyCode::Up => {
                if zone != Zone::Arg {
                    self.select_prev(footer_height);
                }
                PaletteEvent::Consumed
            }
            KeyCode::Down => {
                if zone != Zone::Arg {
                    self.select_next(footer_height);
                }
                PaletteEvent::Consumed
            }
            KeyCode::Left => match zone {
                Zone::Empty => {
                    self.open_route_picker(available_routes, route);
                    PaletteEvent::Consumed
                }
                Zone::RpcName => {
                    if !self.pop_undo(registry) {
                        self.delete_segment();
                        self.update_suggestions(registry);
                    }
                    PaletteEvent::Consumed
                }
                Zone::Arg => {
                    if self.has_arg_content() {
                        self.clear_arg(registry);
                    } else {
                        self.pop_undo(registry);
                    }
                    PaletteEvent::Consumed
                }
            },
            KeyCode::Right | KeyCode::Tab => match zone {
                Zone::Empty | Zone::RpcName => {
                    self.commit_to_arg(registry);
                    PaletteEvent::Consumed
                }
                Zone::Arg => {
                    self.input_state.handle_key_event(key);
                    PaletteEvent::Consumed
                }
            },
            KeyCode::Enter => match zone {
                Zone::Empty | Zone::RpcName => {
                    self.commit_to_arg(registry);
                    PaletteEvent::Consumed
                }
                Zone::Arg => match self.submit_command(route, registry) {
                    Some(req) => PaletteEvent::Submit(req),
                    None => PaletteEvent::Consumed,
                },
            },
            _ => {
                self.input_state.handle_key_event(key);
                self.update_suggestions(registry);
                PaletteEvent::Consumed
            }
        }
    }

    fn current_zone(&self) -> Zone {
        let input = self.input_state.value();
        if input.is_empty() {
            Zone::Empty
        } else if input.contains(' ') {
            Zone::Arg
        } else {
            Zone::RpcName
        }
    }

    fn rpc_name_part(&self) -> &str {
        let input = self.input_state.value();
        input.split_whitespace().next().unwrap_or("")
    }

    /// Trim the trailing dotted segment. `dev.foo.bar` → `dev.foo.`, then
    /// → `dev.`, then → `` — letting a user back out of a wrong prefix
    /// without many backspaces.
    fn delete_segment(&mut self) {
        let input = self.input_state.value();
        let new_val = if let Some(stripped) = input.strip_suffix('.') {
            match stripped.rfind('.') {
                Some(idx) => stripped[..=idx].to_string(),
                None => String::new(),
            }
        } else {
            match input.rfind('.') {
                Some(idx) => input[..=idx].to_string(),
                None => String::new(),
            }
        };
        self.input_state = TextState::new().with_value(new_val);
        self.input_state.focus();
        self.input_state.move_end();
    }

    fn open_history_picker(&mut self) {
        let mut picker = HistoryPicker::default();
        picker.refilter(&self.history);
        self.picker = Some(picker);
    }

    fn handle_picker_key(&mut self, key: KeyEvent, registry: Option<&RpcRegistry>) {
        let mut picker = match self.picker.take() {
            Some(p) => p,
            None => return,
        };
        match key.code {
            KeyCode::Esc => { /* drop picker */ }
            KeyCode::Enter => {
                if let Some(entry) = picker.selected_entry(&self.history) {
                    let entry = entry.to_string();
                    self.input_state = TextState::new().with_value(entry);
                    self.input_state.focus();
                    self.input_state.move_end();
                    self.update_suggestions(registry);
                } else {
                    self.picker = Some(picker);
                }
            }
            KeyCode::Up => {
                picker.selected = picker.selected.saturating_sub(1);
                self.picker = Some(picker);
            }
            KeyCode::Down => {
                let last = picker.filtered.len().saturating_sub(1);
                picker.selected = (picker.selected + 1).min(last);
                self.picker = Some(picker);
            }
            KeyCode::Backspace => {
                picker.query.pop();
                picker.refilter(&self.history);
                self.picker = Some(picker);
            }
            KeyCode::Char(c)
                if key.modifiers == KeyModifiers::NONE || key.modifiers == KeyModifiers::SHIFT =>
            {
                picker.query.push(c);
                picker.refilter(&self.history);
                self.picker = Some(picker);
            }
            _ => {
                self.picker = Some(picker);
            }
        }
    }

    fn open_route_picker(&mut self, routes: &[DeviceRoute], current: &DeviceRoute) {
        if routes.is_empty() {
            return;
        }
        let routes = routes.to_vec();
        let selected = routes.iter().position(|r| r == current).unwrap_or(0);
        self.route_picker = Some(RoutePicker { routes, selected });
    }

    fn handle_route_picker_key(&mut self, key: KeyEvent) -> PaletteEvent {
        let mut picker = match self.route_picker.take() {
            Some(p) => p,
            None => return PaletteEvent::Consumed,
        };
        match key.code {
            KeyCode::Esc => PaletteEvent::Consumed,
            KeyCode::Enter | KeyCode::Right => {
                let route = picker.routes[picker.selected].clone();
                PaletteEvent::SelectRoute(route)
            }
            KeyCode::Up => {
                picker.selected = picker.selected.saturating_sub(1);
                self.route_picker = Some(picker);
                PaletteEvent::Consumed
            }
            KeyCode::Down => {
                let last = picker.routes.len().saturating_sub(1);
                picker.selected = (picker.selected + 1).min(last);
                self.route_picker = Some(picker);
                PaletteEvent::Consumed
            }
            _ => {
                self.route_picker = Some(picker);
                PaletteEvent::Consumed
            }
        }
    }

    /// Render the palette into `area`. Caller is responsible for reserving
    /// enough vertical space (see [`suggestion_rows`]).
    pub fn render(
        &self,
        f: &mut Frame,
        area: Rect,
        route: &DeviceRoute,
        registry: Option<&RpcRegistry>,
        registry_ready: bool,
        blink: bool,
    ) {
        let footer_height = area.height;
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Max(self.suggestion_rows() + 2),
                Constraint::Length(std::cmp::min(1, footer_height.saturating_sub(1))),
                Constraint::Length(if footer_height > 2 { 2 } else { 1 }),
            ])
            .split(area);

        if footer_height > 3 {
            let (rows, title_left, title_right): (Vec<Line>, Line, Line) =
                if let Some(picker) = &self.picker {
                    let visible_rows = self.visible_rows(footer_height);
                    let end = visible_rows.min(picker.filtered.len());
                    let items = picker.filtered[..end]
                        .iter()
                        .enumerate()
                        .map(|(i, &hist_idx)| {
                            let text = self.history.get(hist_idx).map(|s| s.as_str()).unwrap_or("");
                            let line = Line::from(Span::raw(text.to_string()));
                            if i == picker.selected {
                                line.bold()
                            } else {
                                line.dim()
                            }
                        })
                        .collect::<Vec<_>>();
                    let rows = if items.is_empty() {
                        let msg = if self.history.is_empty() {
                            "(no history yet)"
                        } else {
                            "(no matches)"
                        };
                        vec![Line::from(Span::styled(
                            msg,
                            Style::default().fg(Color::DarkGray),
                        ))]
                    } else {
                        items
                    };
                    let title = format!(" History: {}▏", picker.query);
                    (
                        rows,
                        Line::from(title).left_aligned(),
                        Line::from(" ↑ | ↓ | Esc ").right_aligned(),
                    )
                } else if let Some(rp) = &self.route_picker {
                    let visible_rows = self.visible_rows(footer_height);
                    let end = visible_rows.min(rp.routes.len());
                    let items = rp.routes[..end]
                        .iter()
                        .enumerate()
                        .map(|(i, r)| {
                            let text = format!("{}", r);
                            let line = Line::from(Span::raw(text));
                            if i == rp.selected {
                                line.bold()
                            } else {
                                line.dim()
                            }
                        })
                        .collect::<Vec<_>>();
                    let rows = if items.is_empty() {
                        vec![Line::from(Span::raw("(no routes)"))]
                    } else {
                        items
                    };
                    (
                        rows,
                        Line::from(" Routes ").left_aligned(),
                        Line::from(" ↑ | ↓ | Enter ").right_aligned(),
                    )
                } else if self.current_zone() == Zone::Arg {
                    let rpc_name = self.rpc_name_part().to_string();
                    let sig = registry
                        .and_then(|r| r.find(&rpc_name))
                        .map(rpc_signature)
                        .filter(|s| !s.is_empty())
                        .unwrap_or_default();
                    let mut spans = vec![Span::styled(
                        rpc_name,
                        Style::default().add_modifier(Modifier::BOLD),
                    )];
                    if !sig.is_empty() {
                        spans.push(Span::raw("  "));
                        spans.push(Span::styled(
                            sig,
                            Style::default()
                                .fg(Color::DarkGray)
                                .add_modifier(Modifier::DIM),
                        ));
                    }
                    (
                        vec![Line::from(spans)],
                        Line::from(" Argument ").left_aligned(),
                        Line::from(" Enter | ← clear ").right_aligned(),
                    )
                } else {
                    let items: Vec<Line> = if registry_ready {
                        let visible_rows = self.visible_rows(footer_height);
                        let start = self.effective_scroll(visible_rows);
                        let end = if visible_rows == 0 {
                            start
                        } else {
                            (start + visible_rows).min(self.suggestions.len())
                        };
                        self.suggestions[start..end]
                            .iter()
                            .enumerate()
                            .map(|(i, sugg)| {
                                let is_sel = Some(start + i) == self.selected;
                                self.render_suggestion_line(sugg, registry, is_sel)
                            })
                            .collect()
                    } else {
                        vec![Line::from(vec![
                            Span::styled(spinner_frame(blink), Style::default().fg(Color::Cyan)),
                            Span::raw(" Generating RPC list..."),
                        ])]
                    };
                    let rows = if items.is_empty() {
                        vec![Line::from(Span::raw(""))]
                    } else {
                        items
                    };
                    (
                        rows,
                        Line::from(" RPCs ").left_aligned(),
                        Line::from(" ↑ | ↓ | ^R ").right_aligned(),
                    )
                };

            let rpc_block = Block::default()
                .borders(Borders::ALL)
                .title(title_left)
                .title(title_right);
            f.render_widget(List::new(rows).block(rpc_block), chunks[0]);
        }

        if footer_height > 1 {
            if let Some((msg, color)) = &self.last_rpc_result {
                let line = if self.in_flight {
                    Line::from(vec![
                        Span::styled(spinner_frame(blink), Style::default().fg(*color)),
                        Span::raw(" "),
                        Span::styled(
                            msg.clone(),
                            Style::default().fg(*color).add_modifier(Modifier::BOLD),
                        ),
                    ])
                } else {
                    Line::from(Span::styled(
                        msg.clone(),
                        Style::default().fg(*color).add_modifier(Modifier::BOLD),
                    ))
                };
                f.render_widget(Paragraph::new(line), chunks[1]);
            }
        }

        let user_input = self.input_state.value();
        let cursor_idx = self.input_state.position().min(user_input.len());
        let zone = self.current_zone();

        // The first space acts as the rpc/arg boundary; render it as a visible
        // separator and style anything after it distinctly.
        let split_at = user_input.find(' ');
        let name_style = Style::default();
        let arg_style = Style::default().fg(Color::Green);
        let sep_style = Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD);

        let mut spans: Vec<Span> = Vec::new();
        spans.push(Span::styled(
            format!("[{}] ", route),
            Style::default().fg(Color::Blue),
        ));

        // Cursor is REVERSED on, plain off — so it actually blinks.
        let cursor_style = Style::default().add_modifier(Modifier::REVERSED);

        for (i, ch) in user_input.char_indices() {
            let is_cursor = i == cursor_idx;
            let is_sep = Some(i) == split_at && ch == ' ';
            let (text, base) = if is_sep {
                ("│".to_string(), sep_style)
            } else {
                let s = match split_at {
                    Some(sp) if i > sp => arg_style,
                    _ => name_style,
                };
                (ch.to_string(), s)
            };
            let style = if is_cursor && blink {
                base.patch(cursor_style)
            } else {
                base
            };
            spans.push(Span::styled(text, style));
        }
        if cursor_idx >= user_input.len() {
            let style = if blink {
                cursor_style
            } else {
                Style::default()
            };
            spans.push(Span::styled(" ", style));
        }

        let title_left = match zone {
            Zone::Arg => {
                let sig = registry
                    .and_then(|r| r.find(self.rpc_name_part()))
                    .map(rpc_signature)
                    .filter(|s| !s.is_empty());
                match sig {
                    Some(s) => format!(" Argument {} ", s),
                    None => " Argument ".to_string(),
                }
            }
            _ => " RPC ".to_string(),
        };

        let block = if footer_height < 3 {
            Block::default()
        } else {
            Block::default()
                .borders(Borders::TOP)
                .title(Line::from(title_left).left_aligned())
                .title(Line::from(" <Esc/Ctrl+C> ").right_aligned())
        };

        f.render_widget(Paragraph::new(Line::from(spans)).block(block), chunks[2]);
    }

    /// Render one suggestion row with fuzzy-match char highlights and (when
    /// known) the registry's perm+type signature trailing in dim.
    fn render_suggestion_line<'a>(
        &self,
        sugg: &'a Suggestion,
        registry: Option<&RpcRegistry>,
        is_selected: bool,
    ) -> Line<'a> {
        let base_style = if is_selected {
            Style::default().add_modifier(Modifier::BOLD)
        } else {
            Style::default().add_modifier(Modifier::DIM)
        };
        let hl_style = Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD);

        let mut spans: Vec<Span> = Vec::new();
        for (i, ch) in sugg.name.chars().enumerate() {
            let matched = sugg.positions.binary_search(&(i as u32)).is_ok();
            let style = if matched { hl_style } else { base_style };
            spans.push(Span::styled(ch.to_string(), style));
        }

        if let Some(desc) = registry.and_then(|r| r.find(&sugg.name)) {
            let sig = rpc_signature(desc);
            if !sig.is_empty() {
                spans.push(Span::raw("  "));
                spans.push(Span::styled(
                    sig,
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::DIM),
                ));
            }
        }

        Line::from(spans)
    }

    fn visible_rows(&self, footer_height: u16) -> usize {
        min(RPCLIST_MAX_LEN, footer_height.saturating_sub(5) as usize)
    }

    fn effective_scroll(&self, visible_rows: usize) -> usize {
        if visible_rows == 0 || self.suggestions.len() <= visible_rows {
            return 0;
        }
        let max_scroll = self.suggestions.len() - visible_rows;
        let mut scroll = self.scroll.min(max_scroll);
        if let Some(sel) = self.selected {
            if sel < scroll {
                scroll = sel;
            } else if sel >= scroll + visible_rows {
                scroll = sel + 1 - visible_rows;
            }
        }
        scroll
    }

    fn ensure_selection_visible(&mut self, footer_height: u16) {
        let Some(selected) = self.selected else {
            self.scroll = 0;
            return;
        };
        let visible_rows = self.visible_rows(footer_height);
        if visible_rows == 0 || self.suggestions.len() <= visible_rows {
            self.scroll = 0;
            return;
        }
        if selected < self.scroll {
            self.scroll = selected;
        } else if selected >= self.scroll + visible_rows {
            self.scroll = selected + 1 - visible_rows;
        }
    }

    fn select_next(&mut self, footer_height: u16) {
        if self.suggestions.is_empty() {
            return;
        }
        let next = match self.selected {
            Some(idx) => (idx + 1) % self.suggestions.len(),
            None => 0,
        };
        self.selected = Some(next);
        self.ensure_selection_visible(footer_height);
    }

    fn select_prev(&mut self, footer_height: u16) {
        if self.suggestions.is_empty() {
            return;
        }
        let next = match self.selected {
            Some(0) | None => self.suggestions.len() - 1,
            Some(idx) => idx - 1,
        };
        self.selected = Some(next);
        self.ensure_selection_visible(footer_height);
    }

    pub fn update_suggestions(&mut self, registry: Option<&RpcRegistry>) {
        let query = self.rpc_name_part().to_string();
        let prev_name = self
            .selected
            .and_then(|i| self.suggestions.get(i))
            .map(|s| s.name.clone());
        let Some(registry) = registry else {
            self.suggestions = Vec::new();
            self.selected = None;
            self.scroll = 0;
            return;
        };

        let names = registry.names();
        self.suggestions = if query.is_empty() {
            let mut all: Vec<Suggestion> = names
                .iter()
                .map(|n| Suggestion {
                    name: n.clone(),
                    positions: Vec::new(),
                })
                .collect();
            all.sort_by(|a, b| a.name.cmp(&b.name));
            all
        } else {
            let mut needle_buf = Vec::new();
            let needle = Utf32Str::new(&query, &mut needle_buf);
            let mut scored: Vec<(u16, Suggestion)> = Vec::new();
            for name in names {
                let mut haystack_buf = Vec::new();
                let haystack = Utf32Str::new(name, &mut haystack_buf);
                let mut positions = Vec::new();
                if let Some(score) = self.matcher.fuzzy_indices(haystack, needle, &mut positions) {
                    scored.push((
                        score,
                        Suggestion {
                            name: name.clone(),
                            positions,
                        },
                    ));
                }
            }
            scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.name.cmp(&b.1.name)));
            scored.into_iter().map(|(_, s)| s).collect()
        };
        self.selected = prev_name
            .and_then(|n| self.suggestions.iter().position(|s| s.name == n))
            .or_else(|| (!self.suggestions.is_empty()).then_some(0));
        self.scroll = 0;
    }

    fn commit_to_arg(&mut self, registry: Option<&RpcRegistry>) {
        let existing = self.input_state.value().to_string();
        let name = self
            .selected
            .and_then(|i| self.suggestions.get(i))
            .map(|s| s.name.clone())
            .unwrap_or_else(|| existing.trim().to_string());
        if name.is_empty() {
            return;
        }
        self.undo_stack.push(UndoEntry {
            input: existing,
            accepted_name: name.clone(),
        });
        let new_val = format!("{} ", name);
        self.input_state = TextState::new().with_value(new_val);
        self.input_state.focus();
        self.input_state.move_end();
        self.update_suggestions(registry);
    }

    /// True iff there is non-whitespace text after the first space.
    fn has_arg_content(&self) -> bool {
        self.input_state
            .value()
            .split_once(' ')
            .map(|(_, arg)| !arg.trim().is_empty())
            .unwrap_or(false)
    }

    fn clear_arg(&mut self, registry: Option<&RpcRegistry>) {
        let input = self.input_state.value();
        if let Some(sp) = input.find(' ') {
            let kept = input[..sp].to_string();
            self.input_state = TextState::new().with_value(kept);
            self.input_state.focus();
            self.input_state.move_end();
            self.update_suggestions(registry);
        }
    }

    /// Restore the most recent pre-accept input and re-select the suggestion
    /// that was accepted. Returns true if something was popped.
    fn pop_undo(&mut self, registry: Option<&RpcRegistry>) -> bool {
        let Some(entry) = self.undo_stack.pop() else {
            return false;
        };
        self.input_state = TextState::new().with_value(entry.input);
        self.input_state.focus();
        self.input_state.move_end();
        self.update_suggestions(registry);
        if let Some(idx) = self
            .suggestions
            .iter()
            .position(|s| s.name == entry.accepted_name)
        {
            self.selected = Some(idx);
        }
        true
    }

    fn submit_command(
        &mut self,
        route: &DeviceRoute,
        registry: Option<&RpcRegistry>,
    ) -> Option<RpcReq> {
        let line = self.input_state.value().to_string();
        if line.trim().is_empty() {
            return None;
        }

        let mut parts = line.split_whitespace();
        let method = parts.next()?;
        self.last_rpc_command = method.to_string();
        let remainder: Vec<&str> = parts.collect();
        let arg = if remainder.is_empty() {
            None
        } else {
            Some(remainder.join(" "))
        };
        let meta = registry.and_then(|r| r.find(method)).map(|d| d.meta_raw);
        self.last_rpc_result = Some((format!("Sent to {}", route), Color::Yellow));
        self.in_flight = true;

        let req = RpcReq {
            route: route.clone(),
            meta,
            method: method.to_string(),
            arg,
        };
        if self.history.last() != Some(&line) {
            self.history.push(line);
        }
        Some(req)
    }

    fn clear_input(&mut self, registry: Option<&RpcRegistry>) {
        self.input_state = TextState::default();
        self.input_state.focus();
        self.undo_stack.clear();
        self.update_suggestions(registry);
    }
}

fn spinner_frame(blink: bool) -> &'static str {
    if blink {
        "◐"
    } else {
        "◑"
    }
}

fn rpc_signature(desc: &RpcDescriptor) -> String {
    if desc.is_unknown() {
        return String::new();
    }
    let t = desc.type_str();
    let perm = desc.perm_str();
    if t.is_empty() {
        format!("[{}]", perm)
    } else {
        format!("[{} {}]", perm, t)
    }
}
