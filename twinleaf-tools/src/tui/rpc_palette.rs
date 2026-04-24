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

    /// Number of suggestion rows to display (without borders).
    pub fn suggestion_rows(&self) -> u16 {
        let len = if self.suggestions.is_empty() {
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
        match key.code {
            KeyCode::Char('c') if key.modifiers == KeyModifiers::CONTROL => PaletteEvent::Exit,
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
            let rpcs: Vec<Span> = if registry_ready {
                let visible_rows = self.visible_rows(footer_height);
                let start = self.scroll.min(self.suggestions.len());
                let end = if visible_rows == 0 {
                    start
                } else {
                    (start + visible_rows).min(self.suggestions.len())
                };
                self.suggestions[start..end]
                    .iter()
                    .map(|v| Span::raw(v.clone()))
                    .enumerate()
                    .map(|(i, v)| {
                        if Some(start + i) == self.selected {
                            v.bold()
                        } else {
                            v.dim()
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![Span::from("Generating RPC list...")]
            };
            let rpcs = if rpcs.is_empty() {
                vec![Span::raw("")]
            } else {
                rpcs
            };

            let rpc_block = Block::default()
                .borders(Borders::ALL)
                .title(Line::from(" RPCs ").left_aligned())
                .title(Line::from(" ↑ | ↓ ").right_aligned());
            f.render_widget(List::new(rpcs).block(rpc_block), chunks[0]);
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

        Some(RpcReq {
            route: route.clone(),
            meta,
            method: method.to_string(),
            arg,
        })
    }

    fn clear_input(&mut self, registry: Option<&RpcRegistry>) {
        self.input_state = TextState::default();
        self.input_state.focus();
        self.update_suggestions(registry);
    }
}
