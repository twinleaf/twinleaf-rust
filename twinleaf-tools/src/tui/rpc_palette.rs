//! Interactive RPC palette widget for embedding in ratatui TUIs.
//!
//! Hosts (e.g. `tio monitor`, `tio health`) own an [`RpcPalette`], route key
//! events to [`RpcPalette::handle_key`], and render it with
//! [`RpcPalette::render`]. The palette emits [`PaletteEvent`]s for submit /
//! exit; the host owns the RPC worker plumbing and mode transitions.

use std::cmp::min;

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
use twinleaf::{device::RpcRegistry, tio};

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
/// whether to dispatch an RPC, exit the palette mode, or do nothing.
pub enum PaletteEvent {
    /// Key was consumed; no host-level action.
    Consumed,
    /// User asked to execute this RPC.
    Submit(RpcReq),
    /// User asked to exit the palette (Esc on empty input, or Ctrl+C).
    Exit,
}

#[derive(Debug, Default)]
pub struct RpcPalette {
    pub input_state: TextState<'static>,
    pub suggestions: Vec<String>,
    pub selected: Option<usize>,
    pub scroll: usize,
    pub current_completion: String,
    pub last_rpc_result: Option<(String, Color)>,
    pub last_rpc_command: String,
    history: Vec<String>,
    picker: Option<HistoryPicker>,
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
    }

    /// Number of suggestion rows to display (without borders). When the history
    /// picker is open, reflects its filtered-row count instead.
    pub fn suggestion_rows(&self) -> u16 {
        let len = if let Some(picker) = &self.picker {
            picker.filtered.len().max(1).min(RPCLIST_MAX_LEN)
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
    }

    /// Dispatch a key event. The host should call this for every key while the
    /// palette is active; unhandled readline-style edits are routed through to
    /// the underlying [`TextState`].
    pub fn handle_key(
        &mut self,
        key: KeyEvent,
        registry: Option<&RpcRegistry>,
        route: &DeviceRoute,
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
        match key.code {
            KeyCode::Char('r') if key.modifiers == KeyModifiers::CONTROL => {
                self.open_picker();
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
                self.select_prev(footer_height);
                PaletteEvent::Consumed
            }
            KeyCode::Down => {
                self.select_next(footer_height);
                PaletteEvent::Consumed
            }
            KeyCode::Tab => {
                self.accept_completion(registry);
                PaletteEvent::Consumed
            }
            KeyCode::Right if !self.current_completion.is_empty() => {
                self.accept_completion(registry);
                PaletteEvent::Consumed
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Home => {
                self.current_completion.clear();
                self.input_state.handle_key_event(key);
                PaletteEvent::Consumed
            }
            KeyCode::Char('a') if key.modifiers == KeyModifiers::CONTROL => {
                self.current_completion.clear();
                self.input_state.handle_key_event(key);
                PaletteEvent::Consumed
            }
            KeyCode::Enter => match self.submit_command(route, registry) {
                Some(req) => PaletteEvent::Submit(req),
                None => PaletteEvent::Consumed,
            },
            _ => {
                self.input_state.handle_key_event(key);
                self.update_suggestions(registry);
                PaletteEvent::Consumed
            }
        }
    }

    fn open_picker(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let mut picker = HistoryPicker::default();
        picker.refilter(&self.history);
        self.picker = Some(picker);
    }

    fn handle_picker_key(&mut self, key: KeyEvent, registry: Option<&RpcRegistry>) {
        // Take the picker out so we can freely mutate other fields of self. If
        // the key should keep the picker open, we re-install it at the end.
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
                    // user committed to a recall — drop picker
                } else {
                    // no matches; keep picker open so user can adjust query
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
            KeyCode::Char(c) if key.modifiers == KeyModifiers::NONE
                || key.modifiers == KeyModifiers::SHIFT =>
            {
                picker.query.push(c);
                picker.refilter(&self.history);
                self.picker = Some(picker);
            }
            _ => {
                // unknown key; keep picker open
                self.picker = Some(picker);
            }
        }
    }

    /// Render the palette into `area`. Caller is responsible for reserving
    /// enough vertical space (see [`suggestion_block_height`]).
    pub fn render(
        &self,
        f: &mut Frame,
        area: Rect,
        route: &DeviceRoute,
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
                    let start = 0usize;
                    let end = visible_rows.min(picker.filtered.len());
                    let items = picker.filtered[start..end]
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
                        vec![Line::from(Span::raw("(no matches)"))]
                    } else {
                        items
                    };
                    let title = format!(" History: {}▏", picker.query);
                    (
                        rows,
                        Line::from(title).left_aligned(),
                        Line::from(" ↑ | ↓ | Esc ").right_aligned(),
                    )
                } else {
                    let items: Vec<Line> = if registry_ready {
                        let visible_rows = self.visible_rows(footer_height);
                        let start = self.scroll.min(self.suggestions.len());
                        let end = if visible_rows == 0 {
                            start
                        } else {
                            (start + visible_rows).min(self.suggestions.len())
                        };
                        self.suggestions[start..end]
                            .iter()
                            .enumerate()
                            .map(|(i, v)| {
                                let line = Line::from(Span::raw(v.clone()));
                                if Some(start + i) == self.selected {
                                    line.bold()
                                } else {
                                    line.dim()
                                }
                            })
                            .collect()
                    } else {
                        vec![Line::from(Span::from("Generating RPC list..."))]
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
                f.render_widget(
                    Paragraph::new(msg.as_str())
                        .style(Style::default().fg(*color).add_modifier(Modifier::BOLD)),
                    chunks[1],
                );
            }
        }

        let user_input = self.input_state.value();
        let cursor_idx = self.input_state.position().min(user_input.len());

        let mut spans = vec![
            Span::styled(format!("[{}] ", route), Style::default().fg(Color::Blue)),
            Span::raw(&user_input[0..cursor_idx]),
        ];

        if cursor_idx < user_input.len() {
            spans.push(Span::styled(
                &user_input[cursor_idx..cursor_idx + 1],
                if blink {
                    Style::default().bg(Color::White).fg(Color::Black)
                } else {
                    Style::default()
                },
            ));
            spans.push(Span::raw(&user_input[cursor_idx + 1..]));
        } else if blink {
            spans.push(Span::styled(" ", Style::default().bg(Color::White)));
            if !self.current_completion.is_empty() {
                spans.push(Span::styled(
                    &self.current_completion[1..],
                    Style::default().fg(Color::Gray),
                ));
            }
        } else {
            spans.push(Span::styled(
                &self.current_completion,
                Style::default().fg(Color::Gray),
            ));
        }

        let block = if footer_height < 3 {
            Block::default()
        } else {
            Block::default()
                .borders(Borders::TOP)
                .title(Line::from(" Command Mode ").left_aligned())
                .title(Line::from(" <Esc/Ctrl+C> ").right_aligned())
        };

        f.render_widget(Paragraph::new(Line::from(spans)).block(block), chunks[2]);
    }

    fn visible_rows(&self, footer_height: u16) -> usize {
        min(RPCLIST_MAX_LEN, footer_height.saturating_sub(5) as usize)
    }

    fn selected_suggestion(&self) -> Option<&str> {
        self.selected
            .and_then(|idx| self.suggestions.get(idx))
            .map(|s| s.as_str())
    }

    fn sync_completion(&mut self) {
        let input = self.input_state.value();
        self.current_completion = self
            .selected_suggestion()
            .filter(|rpc| input.is_empty() || rpc.starts_with(input))
            .and_then(|rpc| rpc.get(input.len()..))
            .map(|s| s.to_string())
            .unwrap_or_default();
        self.input_state.focus();
        self.input_state.move_end();
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
            self.sync_completion();
            return;
        }
        let next = match self.selected {
            Some(idx) => (idx + 1) % self.suggestions.len(),
            None => 0,
        };
        self.selected = Some(next);
        self.ensure_selection_visible(footer_height);
        self.sync_completion();
    }

    fn select_prev(&mut self, footer_height: u16) {
        if self.suggestions.is_empty() {
            self.sync_completion();
            return;
        }
        let next = match self.selected {
            Some(0) | None => self.suggestions.len() - 1,
            Some(idx) => idx - 1,
        };
        self.selected = Some(next);
        self.ensure_selection_visible(footer_height);
        self.sync_completion();
    }

    pub fn update_suggestions(&mut self, registry: Option<&RpcRegistry>) {
        let line = self.input_state.value().to_string();
        let query = line.split_whitespace().next().unwrap_or("");

        self.suggestions = if let Some(registry) = registry {
            if query.is_empty() {
                registry
                    .children_of("")
                    .into_iter()
                    .map(|s| s + "...")
                    .collect()
            } else {
                registry.search(query)
            }
        } else {
            Vec::new()
        };
        self.selected = (!self.suggestions.is_empty()).then_some(0);
        self.scroll = 0;
        self.sync_completion();
    }

    fn accept_completion(&mut self, registry: Option<&RpcRegistry>) {
        let mut complete_command = if self.current_completion.is_empty() {
            self.selected_suggestion().unwrap_or_default().to_string()
        } else {
            format!("{}{}", self.input_state.value(), self.current_completion)
        };
        complete_command = complete_command.replace("...", ".");
        self.input_state = TextState::new().with_value(complete_command);
        self.input_state.focus();
        self.input_state.move_end();
        self.update_suggestions(registry);
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
        self.last_rpc_result = Some((format!("Sent to {}...", route), Color::Yellow));

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
        self.update_suggestions(registry);
    }
}
