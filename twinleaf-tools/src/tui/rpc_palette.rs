//! Interactive RPC palette widget for embedding in ratatui TUIs.
//!
//! Hosts (e.g. `tio monitor`, `tio health`) own an [`RpcPalette`], route key
//! events to [`RpcPalette::handle_key`], and render it with
//! [`RpcPalette::render`]. The palette emits [`PaletteEvent`]s for submit /
//! exit / route-switch; the host owns the RPC worker plumbing and mode
//! transitions.

use std::cmp::min;
use std::time::{Duration, Instant};

use crate::cli::parse_rpc_type;
use crate::tui::scroll::{follow_scroll, wheel_scroll, NAV_MARGIN};
use nucleo_matcher::{Config, Matcher, Utf32Str};
use ratatui::{
    crossterm::event::{
        KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
    },
    layout::{Constraint, Direction, Layout, Margin, Position, Rect},
    prelude::Stylize,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
    Frame,
};
use tui_prompts::{State, TextState};
use twinleaf::device::{DeviceRoute, RpcDescriptor, RpcRegistry};
use twinleaf::tio::proto::RpcValueType;

const RPCLIST_MAX_LEN: usize = 12;
/// Close-affordance title on the input block; also hit-tested for clicks.
const CLOSE_HINT: &str = " <Esc/Ctrl+C> ";

#[derive(Debug, Clone)]
pub struct RpcReq {
    pub route: DeviceRoute,
    pub meta: Option<u16>,
    pub method: String,
    pub arg: Option<String>,
    pub req_type: Option<RpcValueType>,
    pub rep_type: Option<RpcValueType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokRole {
    Method,
    Flag,
    FlagVal,
    Arg,
}

struct ParsedLine {
    method: Option<String>,
    arg: Option<String>,
    req_type: Option<RpcValueType>,
    rep_type: Option<RpcValueType>,
    bad_type: Option<String>,
    roles: Vec<(std::ops::Range<usize>, TokRole)>,
}

/// Split `line` into (byte-range, text) tokens on whitespace.
fn tokenize(line: &str) -> Vec<(std::ops::Range<usize>, &str)> {
    let mut out = Vec::new();
    let mut start: Option<usize> = None;
    for (i, ch) in line.char_indices() {
        if ch.is_whitespace() {
            if let Some(s) = start.take() {
                out.push((s..i, &line[s..i]));
            }
        } else if start.is_none() {
            start = Some(i);
        }
    }
    if let Some(s) = start {
        out.push((s..line.len(), &line[s..]));
    }
    out
}

fn parse_palette_line(line: &str) -> ParsedLine {
    let toks = tokenize(line);
    let mut method = None;
    let mut arg_parts: Vec<&str> = Vec::new();
    let mut req_type = None;
    let mut rep_type = None;
    let mut bad_type = None;
    let mut roles = Vec::with_capacity(toks.len());

    let mut i = 0;
    while i < toks.len() {
        let (range, tok) = (toks[i].0.clone(), toks[i].1);
        if method.is_none() {
            method = Some(tok.to_string());
            roles.push((range, TokRole::Method));
            i += 1;
            continue;
        }
        match tok {
            "-t" | "--req-type" | "-T" | "--rep-type" => {
                roles.push((range, TokRole::Flag));
                let is_req = matches!(tok, "-t" | "--req-type");
                match toks.get(i + 1) {
                    Some((vrange, vtok)) => {
                        roles.push((vrange.clone(), TokRole::FlagVal));
                        match parse_rpc_type(vtok) {
                            Some(t) if is_req => req_type = Some(t),
                            Some(t) => rep_type = Some(t),
                            None => bad_type = Some((*vtok).to_string()),
                        }
                        i += 2;
                    }
                    None => {
                        bad_type = Some(format!("{} needs a type", tok));
                        i += 1;
                    }
                }
            }
            _ => {
                roles.push((range, TokRole::Arg));
                arg_parts.push(tok);
                i += 1;
            }
        }
    }

    ParsedLine {
        method,
        arg: (!arg_parts.is_empty()).then(|| arg_parts.join(" ")),
        req_type,
        rep_type,
        bad_type,
        roles,
    }
}

#[derive(Debug)]
pub struct RpcResp {
    pub result: Result<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcPaletteStatus {
    Ready,
    Fetching,
    Refreshing,
    Disconnected,
    WaitingForRpc,
    Failed,
    FailedUsingPrevious,
}

/// Outcome of [`RpcPalette::handle_key`]. The host interprets this to decide
/// whether to dispatch an RPC, switch routes, exit palette mode, or do nothing.
pub enum PaletteEvent {
    Consumed,
    Submit(RpcReq),
    SelectRoute(DeviceRoute),
    Exit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Zone {
    Empty,
    RpcName,
    Arg,
}

/// Which list the captured [`HitZones`] geometry describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ListKind {
    #[default]
    None,
    Suggestions,
    RoutePicker,
}

/// Screen geometry captured during `render` for mouse hit-testing.
/// Zero-sized rects mean the element isn't currently shown.
#[derive(Debug, Default)]
struct HitZones {
    list: Rect,
    list_kind: ListKind,
    /// First visible row of the rendered list.
    list_start: usize,
    /// The `[route]` prefix in the input line.
    route: Rect,
    /// The `CLOSE_HINT` title.
    close: Rect,
    visible_rows: usize,
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
    undo_stack: Vec<UndoEntry>,
    matcher: Matcher,
    zones: HitZones,
    last_click: Option<(Instant, usize)>,
    hovered: Option<usize>,
    hover_route: bool,
    hover_close: bool,
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
            zones: HitZones::default(),
            last_click: None,
            hovered: None,
            hover_route: false,
            hover_close: false,
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
    /// Index of the first visible row.
    scroll: usize,
}

impl RoutePicker {
    fn commit(&self) -> PaletteEvent {
        PaletteEvent::SelectRoute(self.routes[self.selected])
    }

    fn step(&mut self, forward: bool) {
        if forward {
            let last = self.routes.len().saturating_sub(1);
            self.selected = (self.selected + 1).min(last);
        } else {
            self.selected = self.selected.saturating_sub(1);
        }
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
        self.route_picker = None;
        self.undo_stack.clear();
        self.clear_hover();
    }

    /// Drop the hover highlights. Call when the mouse leaves the palette.
    pub fn clear_hover(&mut self) {
        self.hovered = None;
        self.hover_route = false;
        self.hover_close = false;
    }

    /// Number of suggestion rows to display (without borders). Reflects the
    /// active sub-picker if one is open.
    pub fn suggestion_rows(&self) -> u16 {
        let len = if let Some(picker) = &self.picker {
            picker.filtered.len().clamp(1, RPCLIST_MAX_LEN)
        } else if let Some(rp) = &self.route_picker {
            rp.routes.len().clamp(1, RPCLIST_MAX_LEN)
        } else if self.current_zone() == Zone::Arg || self.suggestions.is_empty() {
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
            KeyCode::PageUp => {
                if zone != Zone::Arg {
                    self.select_page(footer_height, false);
                }
                PaletteEvent::Consumed
            }
            KeyCode::PageDown => {
                if zone != Zone::Arg {
                    self.select_page(footer_height, true);
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
                    let restored = !self.has_arg_content() && self.pop_undo(registry);
                    if !restored {
                        self.clear_arg(registry);
                    }
                    PaletteEvent::Consumed
                }
            },
            KeyCode::Right | KeyCode::Tab | KeyCode::Char(' ') => match zone {
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

    /// Dispatch a mouse event. The host should call this for mouse events
    /// landing within the palette's rendered area while the palette is active.
    pub fn handle_mouse(
        &mut self,
        mouse: MouseEvent,
        registry: Option<&RpcRegistry>,
        route: &DeviceRoute,
        available_routes: &[DeviceRoute],
        footer_height: u16,
    ) -> PaletteEvent {
        self.hover_route = hit(self.zones.route, mouse);
        self.hover_close = hit(self.zones.close, mouse);
        self.hovered = self.hovered_row(mouse);
        // The history picker (Ctrl+R) is out of scope for mouse support.
        if self.picker.is_some() {
            return PaletteEvent::Consumed;
        }
        let left_click = matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left));
        if left_click && self.hover_close {
            return PaletteEvent::Exit;
        }
        if left_click && self.hover_route {
            // A second click on the route button cancels the picker, like Esc.
            if self.route_picker.is_none() {
                self.open_route_picker(available_routes, route);
            } else {
                self.route_picker = None;
            }
            return PaletteEvent::Consumed;
        }
        if self.route_picker.is_some() {
            return match mouse.kind {
                MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
                    let down = matches!(mouse.kind, MouseEventKind::ScrollDown);
                    let visible = self.visible_rows(footer_height);
                    if let Some(picker) = self.route_picker.as_mut() {
                        picker.scroll =
                            wheel_scroll(picker.scroll, down, visible, picker.routes.len());
                        self.zones.list_start = picker.scroll;
                    }
                    self.hovered = self.hovered_row(mouse);
                    PaletteEvent::Consumed
                }
                MouseEventKind::Down(MouseButton::Left) => self.click_route_picker_row(mouse),
                _ => PaletteEvent::Consumed,
            };
        }
        if registry.is_none() || self.current_zone() == Zone::Arg {
            return PaletteEvent::Consumed;
        }
        match mouse.kind {
            MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
                let down = matches!(mouse.kind, MouseEventKind::ScrollDown);
                let visible = self.visible_rows(footer_height);
                self.scroll = wheel_scroll(self.scroll, down, visible, self.suggestions.len());
                self.zones.list_start = self.scroll;
                self.hovered = self.hovered_row(mouse);
            }
            MouseEventKind::Down(MouseButton::Left) => {
                return self.click_suggestion(mouse, registry, route)
            }
            _ => {}
        }
        PaletteEvent::Consumed
    }

    /// Row of the active list under the cursor, or None if the geometry
    /// captured at the last render no longer matches the open list.
    fn hovered_row(&self, mouse: MouseEvent) -> Option<usize> {
        let len = match self.zones.list_kind {
            ListKind::None => return None,
            ListKind::Suggestions => self.suggestions.len(),
            ListKind::RoutePicker => self.route_picker.as_ref()?.routes.len(),
        };
        self.row_at(mouse, self.zones.list_kind)
            .filter(|&i| i < len)
    }

    /// Absolute row index at `mouse`, provided the last render drew a `kind`
    /// list there (guards against stale geometry from another sub-mode).
    fn row_at(&self, mouse: MouseEvent, kind: ListKind) -> Option<usize> {
        if self.zones.list_kind != kind || !hit(self.zones.list, mouse) {
            return None;
        }
        Some(self.zones.list_start + (mouse.row - self.zones.list.y) as usize)
    }

    /// Select the clicked suggestion row without touching the input text (so
    /// the list doesn't reflow); a second click on the same row within 400ms
    /// submits it as an empty-arg query.
    fn click_suggestion(
        &mut self,
        mouse: MouseEvent,
        registry: Option<&RpcRegistry>,
        route: &DeviceRoute,
    ) -> PaletteEvent {
        let Some(idx) = self.row_at(mouse, ListKind::Suggestions) else {
            return PaletteEvent::Consumed;
        };
        if idx >= self.suggestions.len() {
            return PaletteEvent::Consumed;
        }
        self.selected = Some(idx);

        let now = Instant::now();
        let is_double = matches!(self.last_click.take(),
            Some((t, prev)) if prev == idx && now.duration_since(t) < Duration::from_millis(400));
        if is_double {
            let name = self.suggestions[idx].name.clone();
            let req = RpcReq {
                route: *route,
                meta: None,
                method: name.clone(),
                arg: None,
                req_type: None,
                rep_type: None,
            };
            return PaletteEvent::Submit(self.dispatch(registry, req, name));
        }
        self.last_click = Some((now, idx));
        PaletteEvent::Consumed
    }

    /// A click on a route picker row selects and commits it, like Enter.
    fn click_route_picker_row(&mut self, mouse: MouseEvent) -> PaletteEvent {
        let Some(idx) = self.row_at(mouse, ListKind::RoutePicker) else {
            return PaletteEvent::Consumed;
        };
        let Some(mut picker) = self.route_picker.take() else {
            return PaletteEvent::Consumed;
        };
        if idx >= picker.routes.len() {
            self.route_picker = Some(picker);
            return PaletteEvent::Consumed;
        }
        picker.selected = idx;
        picker.commit()
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
        let scroll = follow_scroll(
            0,
            selected,
            self.zones.visible_rows,
            routes.len(),
            NAV_MARGIN,
        );
        self.route_picker = Some(RoutePicker {
            routes,
            selected,
            scroll,
        });
    }

    fn handle_route_picker_key(&mut self, key: KeyEvent) -> PaletteEvent {
        let mut picker = match self.route_picker.take() {
            Some(p) => p,
            None => return PaletteEvent::Consumed,
        };
        match key.code {
            KeyCode::Esc => PaletteEvent::Consumed,
            KeyCode::Enter | KeyCode::Right => picker.commit(),
            KeyCode::Up | KeyCode::Down => {
                picker.step(key.code == KeyCode::Down);
                picker.scroll = follow_scroll(
                    picker.scroll,
                    picker.selected,
                    self.zones.visible_rows,
                    picker.routes.len(),
                    NAV_MARGIN,
                );
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
    /// enough vertical space (see [`Self::suggestion_rows`]).
    pub fn render(
        &mut self,
        f: &mut Frame,
        area: Rect,
        route: &DeviceRoute,
        registry: Option<&RpcRegistry>,
        status: RpcPaletteStatus,
        blink: bool,
    ) {
        let footer_height = area.height;
        self.zones = HitZones {
            visible_rows: self.visible_rows(footer_height),
            ..HitZones::default()
        };
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
                    let start = rp.scroll.min(rp.routes.len().saturating_sub(visible_rows));
                    let end = (start + visible_rows).min(rp.routes.len());
                    let items = rp.routes[start..end]
                        .iter()
                        .enumerate()
                        .map(|(i, r)| {
                            let idx = start + i;
                            let text = format!("{}", r);
                            let mut line = Line::from(Span::raw(text));
                            if idx == rp.selected {
                                line = line.bold();
                            } else if self.hovered != Some(idx) {
                                line = line.dim();
                            }
                            if self.hovered == Some(idx) {
                                line = line.bg(Color::DarkGray);
                            }
                            line
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
                    let items: Vec<Line> = if registry.is_some() {
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
                                let is_hover = Some(start + i) == self.hovered;
                                self.render_suggestion_line(sugg, registry, is_sel, is_hover)
                            })
                            .collect()
                    } else {
                        vec![Line::from(vec![
                            Span::styled(spinner_frame(blink), Style::default().fg(Color::Cyan)),
                            Span::raw(format!(" {}", status.loading_message())),
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
                        Line::from(status.title_right()).right_aligned(),
                    )
                };

            let rpc_block = Block::default()
                .borders(Borders::ALL)
                .title(title_left)
                .title(title_right);
            let inner = rpc_block.inner(chunks[0]);
            f.render_widget(List::new(rows).block(rpc_block), chunks[0]);

            let showing_suggestions = self.picker.is_none()
                && self.route_picker.is_none()
                && self.current_zone() != Zone::Arg
                && registry.is_some();
            if showing_suggestions {
                let visible_rows = self.visible_rows(footer_height);
                let start = self.effective_scroll(visible_rows);
                self.zones.list = inner;
                self.zones.list_kind = ListKind::Suggestions;
                self.zones.list_start = start;
                if self.suggestions.len() > visible_rows {
                    let mut sb_state =
                        ScrollbarState::new(self.suggestions.len() - visible_rows + 1)
                            .viewport_content_length(visible_rows)
                            .position(start);
                    f.render_stateful_widget(
                        Scrollbar::new(ScrollbarOrientation::VerticalRight)
                            .begin_symbol(None)
                            .end_symbol(None)
                            .thumb_style(Style::default().fg(Color::DarkGray))
                            .track_style(Style::default().fg(Color::DarkGray)),
                        chunks[0].inner(Margin {
                            vertical: 1,
                            horizontal: 0,
                        }),
                        &mut sb_state,
                    );
                }
            } else if let Some(rp) = &self.route_picker {
                let visible_rows = self.visible_rows(footer_height);
                let start = rp.scroll.min(rp.routes.len().saturating_sub(visible_rows));
                self.zones.list = inner;
                self.zones.list_kind = ListKind::RoutePicker;
                self.zones.list_start = start;
            }
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
        let picking_route = self.route_picker.is_some();

        // The first space acts as the rpc/arg boundary; render it as a visible
        // separator. Each token is then styled by its parsed role so the colors
        // match exactly what `submit_command` will send.
        let split_at = user_input.find(' ');
        let name_style = Style::default();
        let arg_style = Style::default().fg(Color::Green);
        let flag_style = Style::default()
            .fg(Color::Magenta)
            .add_modifier(Modifier::DIM);
        let flagval_style = Style::default().fg(Color::Magenta);
        let sep_style = Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD);

        let roles = parse_palette_line(user_input).roles;
        let role_at = |idx: usize| {
            roles
                .iter()
                .find(|(r, _)| r.contains(&idx))
                .map(|(_, t)| *t)
        };

        let mut spans: Vec<Span> = Vec::new();
        let display_route = self
            .route_picker
            .as_ref()
            .and_then(|picker| picker.routes.get(picker.selected))
            .unwrap_or(route);
        let route_style = if picking_route {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else if self.hover_route {
            Style::default()
                .fg(Color::Blue)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Blue)
        };
        let route_text = format!("[{}] ", display_route);
        let route_width = route_text.chars().count() as u16;
        spans.push(Span::styled(route_text, route_style));

        // Cursor is REVERSED on, plain off — so it actually blinks.
        let cursor_style = Style::default().add_modifier(Modifier::REVERSED);

        for (i, ch) in user_input.char_indices() {
            let is_cursor = i == cursor_idx;
            let is_sep = Some(i) == split_at && ch == ' ';
            let (text, base) = if is_sep {
                ("│".to_string(), sep_style)
            } else {
                let s = match role_at(i) {
                    Some(TokRole::Arg) => arg_style,
                    Some(TokRole::Flag) => flag_style,
                    Some(TokRole::FlagVal) => flagval_style,
                    Some(TokRole::Method) | None => name_style,
                };
                (ch.to_string(), s)
            };
            let style = if is_cursor && blink && !picking_route {
                base.patch(cursor_style)
            } else {
                base
            };
            spans.push(Span::styled(text, style));
        }
        if cursor_idx >= user_input.len() && !picking_route {
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
            _ if picking_route => " Route ".to_string(),
            _ => " RPC ".to_string(),
        };

        let block = if footer_height < 3 {
            Block::default()
        } else {
            Block::default()
                .borders(Borders::TOP)
                .title(Line::from(title_left).left_aligned())
                .title(if self.hover_close {
                    Line::from(CLOSE_HINT).bold().right_aligned()
                } else {
                    Line::from(CLOSE_HINT).right_aligned()
                })
        };

        let input_inner = block.inner(chunks[2]);
        if input_inner.width > 0 && input_inner.height > 0 {
            self.zones.route = Rect {
                x: input_inner.x,
                y: input_inner.y,
                width: route_width.min(input_inner.width),
                height: 1,
            };
        }
        if footer_height >= 3 {
            let hint_width = CLOSE_HINT.chars().count() as u16;
            let x = chunks[2]
                .right()
                .saturating_sub(hint_width)
                .max(chunks[2].left());
            self.zones.close = Rect {
                x,
                y: chunks[2].y,
                width: hint_width.min(chunks[2].width),
                height: 1,
            };
        }

        f.render_widget(Paragraph::new(Line::from(spans)).block(block), chunks[2]);
    }

    /// Render one suggestion row with fuzzy-match char highlights and (when
    /// known) the registry's perm+type signature trailing in dim.
    fn render_suggestion_line<'a>(
        &self,
        sugg: &'a Suggestion,
        registry: Option<&RpcRegistry>,
        is_selected: bool,
        is_hovered: bool,
    ) -> Line<'a> {
        let base_style = if is_selected {
            Style::default().add_modifier(Modifier::BOLD)
        } else if is_hovered {
            Style::default()
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
                // DarkGray fg would vanish on the DarkGray hover tint.
                let sig_style = if is_hovered {
                    Style::default().add_modifier(Modifier::DIM)
                } else {
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::DIM)
                };
                spans.push(Span::raw("  "));
                spans.push(Span::styled(sig, sig_style));
            }
        }

        if is_hovered {
            Line::from(spans).bg(Color::DarkGray)
        } else {
            Line::from(spans)
        }
    }

    fn visible_rows(&self, footer_height: u16) -> usize {
        min(RPCLIST_MAX_LEN, footer_height.saturating_sub(5) as usize)
    }

    /// Clamp the stored offset to the content — deliberately without
    /// following the selection, which would snap the view back mid wheel-peek.
    fn effective_scroll(&self, visible_rows: usize) -> usize {
        if visible_rows == 0 || self.suggestions.len() <= visible_rows {
            return 0;
        }
        self.scroll.min(self.suggestions.len() - visible_rows)
    }

    fn ensure_selection_visible(&mut self, footer_height: u16) {
        let Some(selected) = self.selected else {
            self.scroll = 0;
            return;
        };
        let visible_rows = self.visible_rows(footer_height);
        self.scroll = follow_scroll(
            self.scroll,
            selected,
            visible_rows,
            self.suggestions.len(),
            NAV_MARGIN,
        );
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

    fn select_page(&mut self, footer_height: u16, forward: bool) {
        if self.suggestions.is_empty() {
            return;
        }
        let page = self.visible_rows(footer_height).max(1);
        let last = self.suggestions.len() - 1;
        let cur = self.selected.unwrap_or(0);
        let next = if forward {
            (cur + page).min(last)
        } else {
            cur.saturating_sub(page)
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
            for name in &names {
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
        // Reveal a preserved-by-name selection deep in the refiltered list.
        self.scroll = match self.selected {
            Some(sel) => follow_scroll(
                0,
                sel,
                self.zones.visible_rows,
                self.suggestions.len(),
                NAV_MARGIN,
            ),
            None => 0,
        };
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

        let parsed = parse_palette_line(&line);
        if let Some(bad) = parsed.bad_type {
            self.last_rpc_result = Some((format!("bad type: {}", bad), Color::Red));
            return None;
        }
        let method = parsed.method?;
        let req = RpcReq {
            route: *route,
            meta: None,
            method,
            arg: parsed.arg,
            req_type: parsed.req_type,
            rep_type: parsed.rep_type,
        };
        Some(self.dispatch(registry, req, line))
    }

    /// Shared submit tail (keyboard Enter and mouse double-click): fill in
    /// registry meta, record status, push history, return the request.
    fn dispatch(
        &mut self,
        registry: Option<&RpcRegistry>,
        mut req: RpcReq,
        history_line: String,
    ) -> RpcReq {
        self.last_rpc_command = req.method.clone();
        req.meta = registry
            .and_then(|r| r.find(&req.method))
            .map(|d| d.meta.bits());
        self.last_rpc_result = Some((format!("Sent to {}", req.route), Color::Yellow));
        self.in_flight = true;

        if self.history.last() != Some(&history_line) {
            self.history.push(history_line);
        }
        req
    }

    fn clear_input(&mut self, registry: Option<&RpcRegistry>) {
        self.input_state = TextState::default();
        self.input_state.focus();
        self.undo_stack.clear();
        self.update_suggestions(registry);
    }
}

impl RpcPaletteStatus {
    fn loading_message(self) -> &'static str {
        match self {
            RpcPaletteStatus::Ready => "",
            RpcPaletteStatus::Fetching => "Generating RPC list...",
            RpcPaletteStatus::Refreshing => "Refreshing RPC list...",
            RpcPaletteStatus::Disconnected => "Device disconnected",
            RpcPaletteStatus::WaitingForRpc => "Waiting for RPCs...",
            RpcPaletteStatus::Failed => "RPC list unavailable",
            RpcPaletteStatus::FailedUsingPrevious => "RPC list failed; using previous list",
        }
    }

    fn title_right(self) -> String {
        let status = match self {
            RpcPaletteStatus::Ready => "",
            RpcPaletteStatus::Fetching => "Generating",
            RpcPaletteStatus::Refreshing => "Refreshing",
            RpcPaletteStatus::Disconnected => "Disconnected",
            RpcPaletteStatus::WaitingForRpc => "Waiting",
            RpcPaletteStatus::Failed => "Unavailable",
            RpcPaletteStatus::FailedUsingPrevious => "Using previous",
        };

        if status.is_empty() {
            " ↑ | ↓ | ^R ".to_string()
        } else {
            format!(" {} | ↑ | ↓ | ^R ", status)
        }
    }
}

fn hit(rect: Rect, mouse: MouseEvent) -> bool {
    rect.contains(Position::new(mouse.column, mouse.row))
}

fn spinner_frame(blink: bool) -> &'static str {
    if blink {
        "◐"
    } else {
        "◑"
    }
}

fn rpc_signature(desc: &RpcDescriptor) -> String {
    if desc.meta.is_unknown() {
        return String::new();
    }
    let t = desc.meta.type_str();
    let perm = desc.meta.perm_str();
    if t.is_empty() {
        format!("[{}]", perm)
    } else {
        format!("[{} {}]", perm, t)
    }
}
