//! The `tio health` subcommand, a live table of timing and rate diagnostics
//! for each stream, by device route. [`run_health`] runs it.

use crate::tui::{self, RpcConsole, Screen};
use crate::HealthCli;
use chrono::{DateTime, Local};
use ratatui::{
    crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers, MouseButton, MouseEventKind},
    layout::{Constraint, Direction, Layout, Margin, Position, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Cell, Paragraph, Row, Scrollbar, ScrollbarOrientation, ScrollbarState,
        Table, TableState,
    },
    DefaultTerminal, Frame,
};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io,
    time::{Duration, Instant, SystemTime},
};
use twinleaf::{
    data::{BoundaryClass, BoundaryReason, SampleBatch, StreamKey, StreamMetadataSnapshot},
    device::{DeviceEvent, Event as StreamEvent, LinkEvent, RecvError},
    tio, Connection, DeviceRoute, SampleNumber, StreamId,
};

#[derive(Default)]
struct DeviceState {
    last_heartbeat: Option<Instant>,
    heartbeat_toggle: bool,
}

impl DeviceState {
    fn heartbeat_char(&self, now: Instant) -> char {
        let fresh = self
            .last_heartbeat
            .map(|t| now.duration_since(t) < Duration::from_millis(500))
            .unwrap_or(false);

        if !fresh {
            '♡' // No recent heartbeat
        } else if self.heartbeat_toggle {
            '♥' // Filled
        } else {
            '♡' // Empty
        }
    }

    fn on_heartbeat(&mut self, now: Instant) {
        self.last_heartbeat = Some(now);
        self.heartbeat_toggle = !self.heartbeat_toggle;
    }
}

struct TimeWindow {
    buf: Vec<f64>,
    cap: usize,
    idx: usize,
    filled: bool,
}

/// The sample rate a jitter window is sized for.
const JITTER_WINDOW_HZ: f64 = 100.0;

impl TimeWindow {
    fn new(seconds: u64) -> Self {
        let cap = ((seconds as f64 * JITTER_WINDOW_HZ).round() as usize).max(16);
        Self {
            buf: vec![0.0; cap],
            cap,
            idx: 0,
            filled: false,
        }
    }

    fn push(&mut self, v: f64) {
        self.buf[self.idx] = v;
        self.idx = (self.idx + 1) % self.cap;
        if self.idx == 0 {
            self.filled = true;
        }
    }

    fn std_ms(&self) -> f64 {
        let n = if self.filled { self.cap } else { self.idx };
        if n == 0 {
            return 0.0;
        }
        let mean: f64 = self.buf[..n].iter().sum::<f64>() / (n as f64);
        let var: f64 = self.buf[..n]
            .iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>()
            / (n as f64);
        var.sqrt()
    }
}

struct OnlineSlope {
    n: u64,
    sum_x: f64,
    sum_y: f64,
    sum_xx: f64,
    sum_xy: f64,
    x0: f64,
    y0: f64,
}

impl Default for OnlineSlope {
    fn default() -> Self {
        Self {
            n: 0,
            sum_x: 0.0,
            sum_y: 0.0,
            sum_xx: 0.0,
            sum_xy: 0.0,
            x0: 0.0,
            y0: 0.0,
        }
    }
}

impl OnlineSlope {
    fn push(&mut self, x: f64, y: f64) {
        if self.n == 0 {
            self.x0 = x;
            self.y0 = y;
        }
        let dx = x - self.x0;
        let dy = y - self.y0;
        self.n += 1;
        self.sum_x += dx;
        self.sum_y += dy;
        self.sum_xx += dx * dx;
        self.sum_xy += dx * dy;
    }

    fn slope(&self) -> Option<f64> {
        if self.n < 2 {
            return None;
        }
        let denom = self.n as f64 * self.sum_xx - self.sum_x * self.sum_x;
        if denom.abs() < f64::EPSILON {
            return None;
        }
        Some((self.n as f64 * self.sum_xy - self.sum_x * self.sum_y) / denom)
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

const MIN_DRIFT_SAMPLES: u64 = 50;
const RECENT_BOUNDARIES_CAP: usize = 20;

#[derive(Default)]
struct StreamStats {
    host_epoch: Option<Instant>,

    drift_slope: OnlineSlope,
    drift_s: f64,
    ppm: f64,

    last_host: Option<Instant>,
    last_data: Option<f64>,
    jitter_ms: f64,
    jitter_window: Option<TimeWindow>,

    last_n: Option<SampleNumber>,
    samples_dropped: u64,

    rate_slope: OnlineSlope,
    received_count: u64,
    rate_smps: f64,

    name: String,
    last_seen: Option<Instant>,

    // Last-known metadata; deliberately survives the reset methods.
    metadata: Option<StreamMetadataSnapshot>,
    recent_boundaries: VecDeque<(SystemTime, BoundaryReason)>,
}

impl StreamStats {
    fn on_sample(
        &mut self,
        sample_n: SampleNumber,
        t_data: f64,
        now: Instant,
        jitter_window_s: u64,
    ) {
        if self.host_epoch.is_none() {
            self.host_epoch = Some(now);
        }
        let host_time = now.duration_since(self.host_epoch.unwrap()).as_secs_f64();

        // Jitter (unchanged)
        if self.jitter_window.is_none() {
            self.jitter_window = Some(TimeWindow::new(jitter_window_s));
        }
        if let (Some(lh), Some(ld)) = (self.last_host, self.last_data) {
            let dh = now.duration_since(lh).as_secs_f64();
            let dd = t_data - ld;
            if let Some(w) = &mut self.jitter_window {
                w.push((dd - dh) * 1000.0);
                self.jitter_ms = w.std_ms();
            }
        }
        self.last_host = Some(now);
        self.last_data = Some(t_data);

        // Drift / PPM via incremental OLS
        self.drift_slope.push(host_time, t_data);
        if self.drift_slope.n >= MIN_DRIFT_SAMPLES {
            if let Some(beta) = self.drift_slope.slope() {
                let host_elapsed = host_time - self.drift_slope.x0;
                self.drift_s = (beta - 1.0) * host_elapsed;
                self.ppm = (beta - 1.0) * 1e6;
            }
        }

        // Rate via incremental OLS
        self.received_count += 1;
        self.rate_slope.push(host_time, self.received_count as f64);
        if let Some(slope) = self.rate_slope.slope() {
            self.rate_smps = slope;
        }

        self.last_n = Some(sample_n);
    }

    fn reset_timing(&mut self) {
        self.drift_slope.reset();
        self.drift_s = 0.0;
        self.ppm = 0.0;
        self.last_host = None;
        self.last_data = None;
        self.jitter_ms = 0.0;
        self.jitter_window = None;
        self.last_n = None;
    }

    /// Starts the rate fit over.
    fn reset_rate(&mut self) {
        self.rate_slope.reset();
        self.received_count = 0;
        self.rate_smps = 0.0;
        self.host_epoch = None;
    }

    fn reset_all(&mut self) {
        self.reset_timing();
        self.reset_rate();
        self.samples_dropped = 0;
    }

    // floor of stale_dur
    fn stale_threshold(&self, floor: Duration) -> Duration {
        if self.rate_slope.n >= 2 && self.rate_smps > 0.0 {
            let period = Duration::from_secs_f64(2.0 / self.rate_smps);
            std::cmp::max(floor, period)
        } else {
            floor
        }
    }

    fn is_stale(&self, now: Instant, floor: Duration) -> bool {
        let threshold = self.stale_threshold(floor);
        self.last_seen
            .map(|t| now.duration_since(t) > threshold)
            .unwrap_or(true)
    }
}

/// How much an event matters, which sets the colour it is drawn in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Severity {
    Info,
    Warning,
    Error,
}

impl Severity {
    fn color(self) -> Color {
        match self {
            Severity::Info => Color::Green,
            Severity::Warning => Color::Yellow,
            Severity::Error => Color::Red,
        }
    }
}

#[derive(Clone)]
struct LoggedEvent {
    timestamp: SystemTime,
    event: String,
    severity: Severity,
}

/// Returns how much a boundary matters, by the class the library gives it.
fn boundary_severity(reason: &BoundaryReason) -> Severity {
    match reason.class() {
        BoundaryClass::Startup | BoundaryClass::Seamless => Severity::Info,
        BoundaryClass::Reconfig => Severity::Warning,
        BoundaryClass::DataLoss | BoundaryClass::Anomaly => Severity::Error,
    }
}

/// Which panel the keyboard scroll keys drive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Focus {
    Table,
    Events,
}

enum Action {
    Quit,
    ToggleHeartbeat,
    TogglePpm,
    ToggleSampleTime,
    ToggleAge,
    ToggleEventLog,
    CycleFocus,
    // Event log
    EventScrollUp,
    EventScrollDown,
    EventPageUp,
    EventPageDown,
    EventHome,
    EventEnd,
    // Main stream table
    TableUp,
    TableDown,
    TablePageUp,
    TablePageDown,
    TableHome,
    TableEnd,
    TableScrollUp,
    TableScrollDown,
    SelectRow(usize),
    ClearSelection,
    // Detail pane (boundary history)
    DetailScrollUp,
    DetailScrollDown,
    OpenConsole,
    ResetStats,
    ClearLog,
}

struct HealthState {
    /// The options `tio health` was run with.
    cli: HealthCli,
    stats: BTreeMap<StreamKey, StreamStats>,
    device_states: HashMap<DeviceRoute, DeviceState>,
    event_log: VecDeque<LoggedEvent>,
    event_scroll_offset: usize,
    show_event_log: bool,
    focus: Focus,
    focus_user_set: bool,
    table_scroll: usize,
    table_total_rows: usize,
    table_view_rows: usize,
    table_area: Rect,
    table_state: TableState,
    table_keys: Vec<StreamKey>,
    selected: Option<StreamKey>,
    detail_scroll: usize,
    show_heartbeat: bool,
    show_ppm: bool,
    show_sample_time: bool,
    show_age: bool,
    console: RpcConsole,
    session_start: Instant,
    events_area: Rect,
}

impl HealthState {
    fn new(cli: HealthCli, console: RpcConsole) -> Self {
        Self {
            cli,
            stats: BTreeMap::new(),
            device_states: HashMap::new(),
            event_log: VecDeque::new(),
            event_scroll_offset: 0,
            show_event_log: true,
            focus: Focus::Table,
            focus_user_set: false,
            table_scroll: 0,
            table_total_rows: 0,
            table_view_rows: 0,
            table_area: Rect::default(),
            table_state: TableState::default(),
            table_keys: Vec::new(),
            selected: None,
            detail_scroll: 0,
            show_heartbeat: false,
            show_ppm: true,
            show_sample_time: true,
            show_age: false,
            console,
            session_start: Instant::now(),
            events_area: Rect::default(),
        }
    }

    fn available_routes(&self) -> Vec<DeviceRoute> {
        let mut routes: Vec<DeviceRoute> = self.device_states.keys().cloned().collect();
        if !routes.contains(&self.cli.tio.route) {
            routes.push(self.cli.tio.route);
        }
        routes.sort();
        routes
    }

    fn log_event(&mut self, msg: String, severity: Severity) {
        self.event_log.push_front(LoggedEvent {
            timestamp: SystemTime::now(),
            event: msg,
            severity,
        });
        if self.event_log.len() > self.cli.event_log_size as usize {
            self.event_log.pop_back();
        }
        // A scrolled-back view stays anchored as new events push in at the front.
        if self.event_scroll_offset > 0 {
            self.event_scroll_offset += 1;
        }
    }

    fn table_scroll_max(&self) -> usize {
        self.table_total_rows.saturating_sub(self.table_view_rows)
    }

    fn table_select(&mut self, index: usize) {
        let Some(key) = self.table_keys.get(index).cloned() else {
            return;
        };
        if self.selected.as_ref() != Some(&key) {
            self.detail_scroll = 0;
        }
        // Follow the selected stream: point the RPC palette at its device.
        self.console.pick(key.route);
        self.selected = Some(key);
        self.table_state.select(Some(index));
    }

    fn table_cursor_up(&mut self, step: usize) {
        match self.table_state.selected() {
            Some(i) => self.table_select(i.saturating_sub(step)),
            None => {
                let last_visible = (self.table_scroll + self.table_view_rows.max(1) - 1)
                    .min(self.table_keys.len().saturating_sub(1));
                self.table_select(last_visible);
            }
        }
    }

    fn table_cursor_down(&mut self, step: usize) {
        let last = self.table_keys.len().saturating_sub(1);
        match self.table_state.selected() {
            Some(i) => self.table_select((i + step).min(last)),
            None => self.table_select(self.table_scroll.min(last)),
        }
    }

    fn clear_selection(&mut self) {
        self.selected = None;
        self.detail_scroll = 0;
        // select(None) would also reset the offset and yank the viewport.
        *self.table_state.selected_mut() = None;
    }

    /// Returns `true` if the event log panel is on screen, which it is when
    /// enabled, non-empty, and not replaced by the detail pane.
    fn events_visible(&self) -> bool {
        self.show_event_log && !self.event_log.is_empty() && self.selected.is_none()
    }

    /// Returns the events the log shows. `--warnings-only` limits them to
    /// warnings and errors.
    fn shown_events(&self) -> impl Iterator<Item = &LoggedEvent> {
        let least = match self.cli.warnings_only {
            true => Severity::Warning,
            false => Severity::Info,
        };
        self.event_log.iter().filter(move |e| e.severity >= least)
    }

    fn handle_batch(&mut self, batch: SampleBatch, now: Instant) {
        let route = batch.route();
        let sid = batch.stream().stream_id;

        if let Some(filter) = &self.cli.streams {
            if !filter.contains(&sid.value()) {
                return;
            }
        }

        let key = StreamKey::new(route, sid);
        let st = self.stats.entry(key).or_insert_with(|| StreamStats {
            name: batch.stream().name.to_string(),
            ..Default::default()
        });

        st.name = batch.stream().name.to_string();
        if st.metadata.is_none() || batch.boundary().is_some() {
            st.metadata = Some(batch.metadata());
        }

        if let Some(boundary) = batch.boundary() {
            self.handle_boundary(boundary, &route, batch.stream().name, sid);
        }

        let st = self.stats.get_mut(&StreamKey::new(route, sid)).unwrap();
        for row in batch.iter() {
            if st.last_n.map(|n| row.n() != n).unwrap_or(true) {
                st.last_seen = Some(now);
            }
            st.on_sample(row.n(), row.timestamp_end(), now, self.cli.jitter_window);
        }
    }

    fn handle_boundary(
        &mut self,
        reason: &BoundaryReason,
        route: &DeviceRoute,
        stream_name: &str,
        stream_id: StreamId,
    ) {
        let key = StreamKey::new(*route, stream_id);
        if let Some(st) = self.stats.get_mut(&key) {
            st.recent_boundaries
                .push_front((SystemTime::now(), reason.clone()));
            if st.recent_boundaries.len() > RECENT_BOUNDARIES_CAP {
                st.recent_boundaries.pop_back();
            }
        }
        let severity = boundary_severity(reason);
        match reason {
            BoundaryReason::Initial => {
                self.log_event(
                    format!("[{}/{}] STREAM STARTED", route, stream_name),
                    severity,
                );
            }
            BoundaryReason::SessionChanged { old, new } => {
                self.log_event(
                    format!("[{}/{}] SESSION: {} → {}", route, stream_name, old, new),
                    severity,
                );
                if let Some(st) = self.stats.get_mut(&key) {
                    st.reset_all();
                }
            }
            BoundaryReason::SamplesLost { expected, received } => {
                let count = received.wrapping_sub(*expected);
                self.log_event(
                    format!("[{}/{}] DROPPED: {} samples", route, stream_name, count),
                    severity,
                );
                if let Some(st) = self.stats.get_mut(&key) {
                    st.samples_dropped += count as u64;
                }
            }
            BoundaryReason::TimeBackward { gap_seconds } => {
                self.log_event(
                    format!(
                        "[{}/{}] TIME BACKWARD: {:.3}s",
                        route, stream_name, gap_seconds
                    ),
                    severity,
                );
            }
            BoundaryReason::TimeForward { gap_seconds } => {
                self.log_event(
                    format!(
                        "[{}/{}] TIME FORWARD: {:.3}s",
                        route, stream_name, gap_seconds
                    ),
                    severity,
                );
            }
            BoundaryReason::RateChanged { old_rate, new_rate } => {
                self.log_event(
                    format!(
                        "[{}/{}] RATE: {:.1} → {:.1} Hz",
                        route, stream_name, old_rate, new_rate
                    ),
                    severity,
                );
                if let Some(st) = self.stats.get_mut(&key) {
                    st.reset_timing();
                    st.reset_rate();
                }
            }
            BoundaryReason::TimeRefSessionChanged { old, new } => {
                self.log_event(
                    format!("[{}/{}] TIME REF: {} → {}", route, stream_name, old, new),
                    severity,
                );
                if let Some(st) = self.stats.get_mut(&key) {
                    st.reset_timing();
                    st.reset_rate();
                }
            }
            BoundaryReason::SegmentRollover { old_id, new_id } => {
                self.log_event(
                    format!(
                        "[{}/{}] SEGMENT: {} → {}",
                        route, stream_name, old_id, new_id
                    ),
                    severity,
                );
            }
            BoundaryReason::SegmentChanged { old_id, new_id } => {
                self.log_event(
                    format!(
                        "[{}/{}] SEGMENT CHANGED: {} → {}",
                        route, stream_name, old_id, new_id
                    ),
                    severity,
                );
                if let Some(st) = self.stats.get_mut(&key) {
                    st.reset_timing();
                }
            }
            _ => {
                self.log_event(
                    format!("[{}/{}] BOUNDARY: {:?}", route, stream_name, reason),
                    severity,
                );
            }
        }
    }

    fn handle_event(&mut self, event: StreamEvent, now: Instant) {
        match event {
            StreamEvent::Link {
                subtree,
                event: LinkEvent::Status(status),
            } => {
                self.log_event(
                    format!("[{}] STATUS: {:?}", subtree, status),
                    Severity::Warning,
                );
                if matches!(
                    status,
                    tio::packet::LinkStatus::SensorDisconnected | tio::packet::LinkStatus::Stopped
                ) {
                    for (key, st) in &mut self.stats {
                        if key.route.starts_with(&subtree) {
                            st.reset_timing();
                            st.reset_rate();
                        }
                    }
                }
            }
            StreamEvent::Link {
                event: LinkEvent::InputOverrun,
                ..
            } => {
                self.log_event("INPUT OVERRUN: samples were lost".into(), Severity::Error);
            }
            StreamEvent::RouteDiscovered { route } => {
                self.device_states.entry(route).or_default();
                self.log_event(format!("[{}] ROUTE DISCOVERED", route), Severity::Info);
            }
            StreamEvent::Device { route, event } => match event {
                DeviceEvent::Heartbeat { .. } => {
                    self.device_states
                        .entry(route)
                        .or_default()
                        .on_heartbeat(now);
                }
                DeviceEvent::Metadata(snapshot) => {
                    self.log_event(
                        format!("[{}] METADATA: {}", route, snapshot.device().name),
                        Severity::Info,
                    );
                }
                DeviceEvent::MetadataUnavailable => {
                    self.log_event(format!("[{}] NO METADATA SUPPORT", route), Severity::Error);
                }
                DeviceEvent::NewHash(hash) => {
                    self.log_event(format!("[{}] NEW HASH: {:?}", route, hash), Severity::Info);
                }
                DeviceEvent::Setting { .. } => {}
                _ => {}
            },
            _ => {}
        }
    }

    fn tick(&mut self, now: Instant) {
        for st in self.stats.values_mut() {
            if st.is_stale(now, self.cli.stale_dur()) && st.rate_slope.n >= 2 {
                st.reset_timing();
                st.reset_rate();
            }
        }

        // Default focus to the event log whenever it's on screen, until the
        // user takes over with Tab. Fall back to the table when it's not.
        if self.events_visible() {
            if !self.focus_user_set {
                self.focus = Focus::Events;
            }
        } else if self.focus == Focus::Events {
            self.focus = Focus::Table;
        }
    }

    fn update(&mut self, action: Action) -> bool {
        let total = self.shown_events().count();
        let display_count = usize::from(self.cli.event_display_lines);
        match action {
            Action::Quit => return true,
            Action::ToggleHeartbeat => self.show_heartbeat = !self.show_heartbeat,
            Action::TogglePpm => self.show_ppm = !self.show_ppm,
            Action::ToggleSampleTime => self.show_sample_time = !self.show_sample_time,
            Action::ToggleAge => self.show_age = !self.show_age,
            Action::ToggleEventLog => {
                self.show_event_log = !self.show_event_log;
                // Don't leave focus stranded on a hidden panel.
                if !self.events_visible() {
                    self.focus = Focus::Table;
                }
            }
            Action::CycleFocus => {
                self.focus = match self.focus {
                    Focus::Table if self.events_visible() => Focus::Events,
                    _ => Focus::Table,
                };
                // Once the user picks a panel, stop auto-following the log.
                self.focus_user_set = true;
            }
            Action::EventScrollUp => {
                self.event_scroll_offset = self.event_scroll_offset.saturating_sub(1);
            }
            Action::EventScrollDown => {
                self.event_scroll_offset =
                    (self.event_scroll_offset + 1).min(total.saturating_sub(display_count));
            }
            Action::EventPageUp => {
                self.event_scroll_offset = self.event_scroll_offset.saturating_sub(display_count);
            }
            Action::EventPageDown => {
                self.event_scroll_offset = (self.event_scroll_offset + display_count)
                    .min(total.saturating_sub(display_count));
            }
            Action::EventHome => {
                self.event_scroll_offset = 0;
            }
            Action::EventEnd => {
                self.event_scroll_offset = total.saturating_sub(display_count);
            }
            Action::TableUp => self.table_cursor_up(1),
            Action::TableDown => self.table_cursor_down(1),
            Action::TablePageUp => self.table_cursor_up(self.table_view_rows.max(1)),
            Action::TablePageDown => self.table_cursor_down(self.table_view_rows.max(1)),
            Action::TableHome => self.table_select(0),
            Action::TableEnd => self.table_select(self.table_keys.len().saturating_sub(1)),
            Action::TableScrollUp => {
                *self.table_state.offset_mut() = self.table_state.offset().saturating_sub(1);
            }
            Action::TableScrollDown => {
                *self.table_state.offset_mut() =
                    (self.table_state.offset() + 1).min(self.table_scroll_max());
            }
            Action::SelectRow(index) => self.table_select(index),
            Action::ClearSelection => self.clear_selection(),
            Action::DetailScrollUp => {
                self.detail_scroll = self.detail_scroll.saturating_sub(1);
            }
            Action::DetailScrollDown => {
                self.detail_scroll = self.detail_scroll.saturating_add(1);
            }
            Action::OpenConsole => self.console.open(self.cli.tio.route),
            Action::ResetStats => {
                for st in self.stats.values_mut() {
                    st.reset_all();
                }
            }
            Action::ClearLog => {
                self.event_log.clear();
                self.event_scroll_offset = 0;
            }
        }
        false
    }
}

struct DisplayRow {
    key: StreamKey,
    route: String,
    stream_id: StreamId,
    name: String,
    rate_smps: f64,
    drift_s: f64,
    ppm: f64,
    jitter_ms: f64,
    samples_dropped: u64,
    last_n: Option<SampleNumber>,
    last_data: Option<f64>,
    elapsed_time: Option<f64>,
    age_s: Option<f64>,
    status: &'static str,
    color: Color,
}

impl DisplayRow {
    fn from_stats(
        key: StreamKey,
        st: &StreamStats,
        now: Instant,
        stale_dur: Duration,
        ppm_warn: f64,
        ppm_err: f64,
    ) -> Self {
        let stale = st.is_stale(now, stale_dur);
        let (color, status) = if stale {
            (Color::DarkGray, "STALLED")
        } else if st.ppm.abs() >= ppm_err {
            (Color::Red, "ERROR")
        } else if st.ppm.abs() >= ppm_warn {
            (Color::Yellow, "WARN")
        } else {
            (Color::Green, "OK")
        };

        let elapsed_time = st
            .host_epoch
            .map(|epoch| now.duration_since(epoch).as_secs_f64());

        let age_s = st.last_seen.map(|t| now.duration_since(t).as_secs_f64());

        DisplayRow {
            route: key.route.to_string(),
            stream_id: key.stream_id,
            key,
            name: st.name.clone(),
            rate_smps: st.rate_smps,
            drift_s: st.drift_s,
            ppm: st.ppm,
            jitter_ms: st.jitter_ms,
            samples_dropped: st.samples_dropped,
            last_n: st.last_n,
            last_data: st.last_data,
            elapsed_time,
            age_s,
            status,
            color,
        }
    }

    fn to_table_row(
        &self,
        route_cell: &str,
        show_ppm: bool,
        show_sample_time: bool,
        show_age: bool,
    ) -> Row<'static> {
        let style = Style::default().fg(self.color);
        let drift_cell = if show_ppm {
            Cell::from(format!("{:.2}", self.ppm))
        } else {
            Cell::from(format!("{:.4}", self.drift_s))
        };
        let time_cell = if show_sample_time {
            Cell::from(format!("{:.3}", self.last_data.unwrap_or(0.0)))
        } else {
            Cell::from(format!("{:.1}", self.elapsed_time.unwrap_or(0.0)))
        };
        let last_n_cell = if show_age {
            match self.age_s {
                Some(age) => Cell::from(format!("{:.3}", age)),
                None => Cell::from("-"),
            }
        } else {
            Cell::from(format!("{}", self.last_n.unwrap_or(SampleNumber::new(0))))
        };
        Row::new(vec![
            Cell::from(route_cell.to_string()).style(style),
            Cell::from(format!("{}", self.stream_id)).style(style),
            Cell::from(self.name.clone()).style(style),
            Cell::from(format!("{:.1}", self.rate_smps)).style(style),
            drift_cell.style(style),
            Cell::from(format!("{:.2}", self.jitter_ms)).style(style),
            Cell::from(format!("{}", self.samples_dropped)).style(style),
            last_n_cell.style(style),
            time_cell.style(style),
            Cell::from(self.status).style(style),
        ])
    }
}

fn draw_detail_pane(
    f: &mut Frame,
    area: Rect,
    key: &StreamKey,
    st: &StreamStats,
    detail_scroll: &mut usize,
) {
    // Each column is at least its floor wide (steady layout across typical
    // devices) and grows to fit long content plus a two-space gap.
    const NAME_FLOOR: usize = 9;
    const PAIR_FLOORS: [usize; 4] = [17, 20, 14, 20];

    /// Flows cells after a label and an optional bold name column. Each cell
    /// is a tuple of spans, natural width and grid width. A cell pads out to
    /// its grid width while the line has room, and wraps whole to a
    /// continuation line, indented past the label, when it does not. A row
    /// with no name and no cells renders `-`.
    fn flow(
        label: &str,
        name: (Option<&str>, usize),
        cells: Vec<(Vec<Span<'static>>, usize, usize)>,
        inner_w: usize,
    ) -> Vec<Line<'static>> {
        let mut spans = vec![Span::styled(
            format!("{:<9}", label),
            Style::default().fg(Color::Cyan),
        )];
        let mut x = 9;
        let (name_text, name_w) = name;
        if name_w > 0 {
            match name_text {
                Some(n) => spans.push(Span::styled(
                    format!("{:<name_w$}", n),
                    Style::default().add_modifier(Modifier::BOLD),
                )),
                None => spans.push(Span::raw(" ".repeat(name_w))),
            }
            x += name_w;
        }
        if name_text.is_none() && cells.is_empty() {
            spans.push(Span::raw("-"));
        }
        let mut lines = Vec::new();
        for (cell, nat, grid_w) in cells {
            if x + nat > inner_w && x > 9 {
                lines.push(Line::from(std::mem::take(&mut spans)));
                spans.push(Span::raw(" ".repeat(9)));
                x = 9;
            }
            spans.extend(cell);
            if grid_w > nat && x + grid_w <= inner_w {
                spans.push(Span::raw(" ".repeat(grid_w - nat)));
                x += grid_w;
            } else if grid_w > 0 {
                spans.push(Span::raw("  "));
                x += nat + 2;
            } else {
                x += nat;
            }
        }
        lines.push(Line::from(spans));
        lines
    }

    fn kv_cells(
        pairs: &[(&str, String)],
        widths: &[usize],
    ) -> Vec<(Vec<Span<'static>>, usize, usize)> {
        pairs
            .iter()
            .enumerate()
            .map(|(i, (subkey, value))| {
                (
                    vec![
                        Span::styled(format!("{} ", subkey), Style::default().fg(Color::Cyan)),
                        Span::raw(value.clone()),
                    ],
                    subkey.len() + 1 + value.len(),
                    widths.get(i).copied().unwrap_or(0),
                )
            })
            .collect()
    }

    fn column_cells<'a>(
        schema: impl ExactSizeIterator<Item = twinleaf::proto::data::Column<'a>>,
    ) -> Vec<(Vec<Span<'static>>, usize, usize)> {
        let len = schema.len();
        schema
            .enumerate()
            .map(|(i, column)| {
                let mut spans = vec![Span::raw(column.name.to_string())];
                let mut nat = column.name.len();
                if !column.units.is_empty() {
                    spans.push(Span::styled(
                        format!(" ({})", column.units),
                        Style::default().fg(Color::DarkGray),
                    ));
                    nat += column.units.len() + 3;
                }
                if i + 1 < len {
                    spans.push(Span::raw(", "));
                    nat += 2;
                }
                (spans, nat, 0)
            })
            .collect()
    }

    let device_row = st.metadata.as_ref().map(|metadata| {
        let device = metadata.device();
        (
            device.name.to_string(),
            vec![
                ("serial", device.serial.to_string()),
                ("session", format!("{:#010x}", device.session.value())),
                ("firmware", device.firmware.to_string()),
            ],
        )
    });
    let stream_row = st.metadata.as_ref().map(|metadata| {
        let stream = metadata.stream();
        (
            stream.name.to_string(),
            vec![
                ("columns", stream.n_columns.to_string()),
                ("segments", stream.n_segments.to_string()),
            ],
        )
    });
    let segment_row = st.metadata.as_ref().map(|metadata| {
        let segment = metadata.segment();
        let mut pairs = vec![
            ("id", segment.segment_id.to_string()),
            ("rate", format!("{} Hz", segment.sampling_rate)),
            ("decimation", segment.decimation.to_string()),
            ("epoch", segment.epoch.to_string()),
        ];
        if segment.filter_type != twinleaf::proto::data::FilterType::NONE {
            pairs.push((
                "filter",
                format!("{} @ {} Hz", segment.filter_type, segment.filter_cutoff),
            ));
        }
        pairs
    });

    let name_w = [&device_row, &stream_row]
        .into_iter()
        .flatten()
        .fold(NAME_FLOOR, |w, (name, _)| w.max(name.len() + 2));
    let mut widths = PAIR_FLOORS.to_vec();
    for pairs in [
        device_row.as_ref().map(|(_, p)| p),
        stream_row.as_ref().map(|(_, p)| p),
        segment_row.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        // A row's last cell renders at natural width, so it doesn't get a
        // vote on its column's width.
        for (i, (subkey, value)) in pairs.iter().enumerate().rev().skip(1) {
            if i >= widths.len() {
                widths.push(0);
            }
            widths[i] = widths[i].max(subkey.len() + 1 + value.len() + 2);
        }
    }

    let inner_w = area.width.saturating_sub(2) as usize;
    let mut meta_lines: Vec<Line> = Vec::new();
    match &device_row {
        Some((name, pairs)) => meta_lines.extend(flow(
            "device",
            (Some(name), name_w),
            kv_cells(pairs, &widths),
            inner_w,
        )),
        None => meta_lines.extend(flow("device", (None, 0), Vec::new(), inner_w)),
    }
    match &stream_row {
        Some((name, pairs)) => meta_lines.extend(flow(
            "stream",
            (Some(name), name_w),
            kv_cells(pairs, &widths),
            inner_w,
        )),
        None => meta_lines.extend(flow("stream", (None, 0), Vec::new(), inner_w)),
    }
    match &segment_row {
        Some(pairs) => meta_lines.extend(flow(
            "segment",
            (None, name_w),
            kv_cells(pairs, &widths),
            inner_w,
        )),
        None => meta_lines.extend(flow("segment", (None, 0), Vec::new(), inner_w)),
    }
    meta_lines.extend(flow(
        "columns",
        (None, 0),
        st.metadata
            .as_ref()
            .map_or_else(Vec::new, |metadata| column_cells(metadata.columns())),
        inner_w,
    ));
    meta_lines.push(Line::default());

    let visible_lines = (area.height.saturating_sub(2) as usize).saturating_sub(meta_lines.len());
    let total = st.recent_boundaries.len();
    let max_scroll = total.saturating_sub(visible_lines);
    *detail_scroll = (*detail_scroll).min(max_scroll);
    let offset = *detail_scroll;
    let end = (offset + visible_lines).min(total);

    let mut lines = meta_lines;
    for (ts, reason) in st.recent_boundaries.iter().skip(offset).take(visible_lines) {
        let dt: DateTime<Local> = (*ts).into();
        let color = boundary_severity(reason).color();
        lines.push(Line::from(vec![
            Span::styled(
                format!("[{}] ", dt.format("%H:%M:%S%.3f")),
                Style::default().fg(color),
            ),
            Span::styled(format!("{:?}", reason), Style::default().fg(color)),
        ]));
    }

    let title = if total > visible_lines {
        format!(
            " {}/{} (sid {}) [{}-{}/{}] ",
            key.route,
            st.name,
            key.stream_id,
            offset + 1,
            end,
            total
        )
    } else {
        format!(" {}/{} (sid {}) ", key.route, st.name, key.stream_id)
    };

    f.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .title(Span::styled(
                    title,
                    Style::default().add_modifier(Modifier::BOLD),
                ))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        ),
        area,
    );

    if total > visible_lines {
        let mut sb_state = ScrollbarState::new(total - visible_lines + 1)
            .viewport_content_length(visible_lines)
            .position(offset);
        f.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None)
                .thumb_style(Style::default().fg(Color::DarkGray))
                .track_style(Style::default().fg(Color::DarkGray)),
            area.inner(Margin {
                vertical: 1,
                horizontal: 0,
            }),
            &mut sb_state,
        );
    }
}

fn draw_ui(terminal: &mut DefaultTerminal, app: &mut HealthState) -> io::Result<()> {
    let now = Instant::now();

    let cli = &app.cli;
    let mut rows: Vec<DisplayRow> = app
        .stats
        .iter()
        .map(|(key, st)| {
            DisplayRow::from_stats(*key, st, now, cli.stale_dur(), cli.ppm_warn, cli.ppm_err)
        })
        .collect();

    rows.sort_by(|a, b| a.route.cmp(&b.route).then(a.stream_id.cmp(&b.stream_id)));

    app.table_keys = rows.iter().map(|r| r.key).collect();
    let selected_idx = app
        .selected
        .as_ref()
        .and_then(|sel| app.table_keys.iter().position(|k| k == sel));
    if selected_idx.is_none() {
        app.selected = None;
    }
    // selected_mut() rather than select(): select(None) would reset the offset.
    *app.table_state.selected_mut() = selected_idx;
    let detail_active = selected_idx.is_some();

    let route_cells: Vec<String> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let is_first = i == 0 || rows[i - 1].route != r.route;
            let is_last = i + 1 == rows.len() || rows[i + 1].route != r.route;
            if is_first {
                r.route.clone()
            } else if is_last {
                "└─".to_string()
            } else {
                "├─".to_string()
            }
        })
        .collect();

    let mut heartbeat_entries: Vec<_> = app
        .device_states
        .iter()
        .map(|(route, state)| (route.to_string(), state.heartbeat_char(now)))
        .collect();
    heartbeat_entries.sort_by(|a, b| a.0.cmp(&b.0));
    let heartbeat_display: String = heartbeat_entries
        .iter()
        .map(|(route, ch)| format!("{}: {}", route, ch))
        .collect::<Vec<_>>()
        .join("  ");

    let show_heartbeat = app.show_heartbeat;
    let show_event_log = app.show_event_log;
    let focus = app.focus;
    let show_ppm = app.show_ppm;
    let show_sample_time = app.show_sample_time;
    let show_age = app.show_age;
    let event_scroll_offset = app.event_scroll_offset;
    let event_display_lines = app.cli.event_display_lines;
    let quiet = app.cli.quiet;

    let in_command = app.console.is_open();
    let console_height = app.console.height();
    let session_str = indicatif::FormattedDuration(app.session_start.elapsed()).to_string();
    let latest_log = crate::logging::latest();

    terminal.draw(|f| {
        let size = f.area();
        // The detail pane needs at least 8 inner lines for the metadata block.
        let event_block_height = if detail_active {
            (event_display_lines + 2).max(10)
        } else if app.event_log.is_empty() || !show_event_log {
            0
        } else {
            event_display_lines + 2
        };
        let footer_height = if in_command {
            console_height
        } else if quiet {
            0
        } else {
            1
        };
        let heartbeat_height = if show_heartbeat { 1 } else { 0 };
        let log_height = if latest_log.is_some() { 1 } else { 0 };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Length(heartbeat_height),
                Constraint::Min(10),
                Constraint::Length(event_block_height),
                Constraint::Length(log_height),
                Constraint::Length(footer_height),
            ])
            .split(size);

        app.events_area = chunks[3];

        // Header
        let header_text = format!(
            "tio health ({}) — jitter={}s  warn/err={}/{}ppm  fps={}  stale={}ms",
            session_str,
            app.cli.jitter_window,
            app.cli.ppm_warn,
            app.cli.ppm_err,
            app.cli.fps,
            app.cli.stale_ms
        );
        f.render_widget(
            Paragraph::new(header_text).style(Style::default().add_modifier(Modifier::BOLD)),
            chunks[0],
        );

        // Heartbeat (conditional)
        if show_heartbeat {
            f.render_widget(
                Paragraph::new(heartbeat_display.clone()).style(Style::default().fg(Color::Cyan)),
                chunks[1],
            );
        }

        // Table
        let drift_header = if show_ppm { "ppm" } else { "drift(s)" };
        let time_header = if show_sample_time {
            "sample_time"
        } else {
            "elapsed(s)"
        };
        let last_n_header = if show_age { "age(s)" } else { "last_n" };

        let header_cells = [
            "route",
            "sid",
            "stream",
            "smps/s",
            drift_header,
            "jitter(ms)",
            "dropped",
            last_n_header,
            time_header,
            "status",
        ];
        let header_style = if focus == Focus::Table {
            Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan)
        } else {
            Style::default().add_modifier(Modifier::BOLD)
        };
        let header_cells = header_cells
            .into_iter()
            .map(|h| Cell::from(h).style(header_style));

        let widths = [
            Constraint::Length(10),
            Constraint::Length(4),
            Constraint::Length(20),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(11),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(8),
        ];

        // Stream table geometry: one header row, the rest is body viewport.
        let table_full = chunks[2];
        app.table_area = table_full;
        app.table_total_rows = rows.len();
        let view_rows = table_full.height.saturating_sub(1) as usize;
        app.table_view_rows = view_rows;

        // Clamp the scroll offset in case rows shrank since the last frame.
        let max_scroll = app.table_total_rows.saturating_sub(view_rows);
        *app.table_state.offset_mut() = app.table_state.offset().min(max_scroll);
        let needs_scroll = app.table_total_rows > view_rows;

        // Reserve the last column for the scrollbar when scrolling is possible.
        let table_area = if needs_scroll {
            Rect {
                width: table_full.width.saturating_sub(1),
                ..table_full
            }
        } else {
            table_full
        };

        let table = Table::new(
            rows.iter()
                .zip(route_cells.iter())
                .map(|(r, rc)| r.to_table_row(rc, show_ppm, show_sample_time, show_age))
                .collect::<Vec<_>>(),
            widths,
        )
        .header(Row::new(header_cells).height(1))
        .column_spacing(2)
        .row_highlight_style(Style::default().bg(Color::Indexed(238)));

        f.render_stateful_widget(table, table_area, &mut app.table_state);
        app.table_scroll = app.table_state.offset();

        if needs_scroll {
            // Track spans the body rows only (skip the header row).
            let sb_area = Rect {
                x: table_full.x,
                y: table_full.y + 1,
                width: table_full.width,
                height: table_full.height.saturating_sub(1),
            };
            let mut sb_state = ScrollbarState::new(app.table_total_rows - view_rows + 1)
                .viewport_content_length(view_rows)
                .position(app.table_scroll);
            f.render_stateful_widget(
                Scrollbar::new(ScrollbarOrientation::VerticalRight)
                    .begin_symbol(None)
                    .end_symbol(None)
                    .thumb_style(Style::default().fg(Color::DarkGray))
                    .track_style(Style::default().fg(Color::DarkGray)),
                sb_area,
                &mut sb_state,
            );
        }

        // Bottom slot: detail pane for the selected stream, event log otherwise
        if detail_active {
            if let Some(key) = &app.selected {
                if let Some(st) = app.stats.get(key) {
                    draw_detail_pane(f, chunks[3], key, st, &mut app.detail_scroll);
                }
            }
        } else if show_event_log && !app.event_log.is_empty() {
            let events_to_show: Vec<&LoggedEvent> = app.shown_events().collect();

            let total = events_to_show.len();
            let display_count = event_display_lines as usize;
            let start = event_scroll_offset.min(total.saturating_sub(display_count));
            let end = (start + display_count).min(total);

            let visible: Vec<Line> = events_to_show[start..end]
                .iter()
                .map(|e| {
                    let dt: DateTime<Local> = e.timestamp.into();
                    let color = e.severity.color();
                    Line::from(vec![
                        Span::styled(
                            format!("[{}] ", dt.format("%H:%M:%S%.3f")),
                            Style::default().fg(color),
                        ),
                        Span::styled(e.event.clone(), Style::default().fg(color)),
                    ])
                })
                .collect();

            let title = if total > display_count {
                format!(" Events [{}-{}/{}] (↑/↓) ", start + 1, end, total)
            } else {
                " Events ".to_string()
            };
            let (title_style, border_color) = if focus == Focus::Events {
                (
                    Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan),
                    Color::Cyan,
                )
            } else {
                (
                    Style::default().add_modifier(Modifier::BOLD),
                    Color::DarkGray,
                )
            };

            f.render_widget(
                Paragraph::new(visible).block(
                    Block::default()
                        .title(Span::styled(title, title_style))
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(border_color)),
                ),
                chunks[3],
            );

            if total > display_count {
                let mut sb_state = ScrollbarState::new(total - display_count + 1)
                    .viewport_content_length(display_count)
                    .position(start);
                f.render_stateful_widget(
                    Scrollbar::new(ScrollbarOrientation::VerticalRight)
                        .begin_symbol(None)
                        .end_symbol(None)
                        .thumb_style(Style::default().fg(Color::DarkGray))
                        .track_style(Style::default().fg(Color::DarkGray)),
                    chunks[3].inner(Margin {
                        vertical: 1,
                        horizontal: 0,
                    }),
                    &mut sb_state,
                );
            }
        }

        // Footer: RPC palette when in Command mode, keybind hints otherwise
        if in_command {
            app.console.render(f, chunks[5], app.cli.tio.route);
        } else if !quiet {
            let heartbeat_hint = if show_heartbeat {
                "h:hide heartbeat"
            } else {
                "h:show heartbeat"
            };
            let drift_hint = if show_ppm { "p:drift" } else { "p:ppm" };
            let time_hint = if show_sample_time {
                "s:elapsed"
            } else {
                "s:sample"
            };
            let age_hint = if show_age { "a:last_n" } else { "a:age" };
            let log_hint = if show_event_log {
                "l:hide log"
            } else {
                "l:show log"
            };
            let esc_hint = if detail_active { "  Esc:back" } else { "" };
            f.render_widget(
                Paragraph::new(format!(
                    "q to quit  |  : RPC  |  {}  {}  {}  {}  |  r:reset  c:clear  {}  |  Tab:focus  ↑/↓ scroll{}",
                    heartbeat_hint, drift_hint, time_hint, age_hint, log_hint, esc_hint
                ))
                .style(Style::default().fg(Color::Gray)),
                chunks[5],
            );
        }

        // The latest log record, which would corrupt the screen on stderr.
        if let Some((level, line)) = &latest_log {
            let color = match level {
                log::Level::Error => Color::Red,
                log::Level::Warn => Color::Yellow,
                _ => Color::Gray,
            };
            f.render_widget(
                Paragraph::new(line.as_str()).style(Style::default().fg(color)),
                chunks[4],
            );
        }
    })?;
    Ok(())
}

fn get_action(ev: Event, app: &mut HealthState) -> Option<Action> {
    if let Event::Mouse(m) = ev {
        // The open RPC palette claims mouse events over it.
        let routes = app.available_routes();
        if app.console.handle_mouse(m, app.cli.tio.route, &routes) {
            return None;
        }
        // Scroll whichever panel the cursor is over (works in any mode; footer wins above).
        let over_events =
            app.events_visible() && app.events_area.contains(Position::new(m.column, m.row));
        let over_detail =
            app.selected.is_some() && app.events_area.contains(Position::new(m.column, m.row));
        let over_table = app.table_area.contains(Position::new(m.column, m.row));
        return match m.kind {
            MouseEventKind::ScrollDown if over_detail => Some(Action::DetailScrollDown),
            MouseEventKind::ScrollUp if over_detail => Some(Action::DetailScrollUp),
            MouseEventKind::ScrollDown if over_events => Some(Action::EventScrollDown),
            MouseEventKind::ScrollUp if over_events => Some(Action::EventScrollUp),
            MouseEventKind::ScrollDown if over_table => Some(Action::TableScrollDown),
            MouseEventKind::ScrollUp if over_table => Some(Action::TableScrollUp),
            MouseEventKind::Down(MouseButton::Left) if over_table => {
                match ((m.row - app.table_area.y) as usize)
                    .checked_sub(1)
                    .map(|r| r + app.table_state.offset())
                {
                    Some(idx) if idx < app.table_keys.len() => {
                        if app.table_state.selected() == Some(idx) {
                            Some(Action::ClearSelection)
                        } else {
                            Some(Action::SelectRow(idx))
                        }
                    }
                    _ => Some(Action::ClearSelection),
                }
            }
            _ => None,
        };
    }
    let Event::Key(k) = ev else { return None };
    if k.kind != KeyEventKind::Press {
        return None;
    }
    if app.console.is_open() {
        let routes = app.available_routes();
        app.console.handle_key(k, app.cli.tio.route, &routes);
        return None;
    }
    if k.code == KeyCode::Char('c') && k.modifiers == KeyModifiers::CONTROL {
        return Some(Action::Quit);
    }
    match k.code {
        KeyCode::Char(':') => Some(Action::OpenConsole),
        KeyCode::Char('q') => Some(Action::Quit),
        KeyCode::Char('h') => Some(Action::ToggleHeartbeat),
        KeyCode::Char('p') => Some(Action::TogglePpm),
        KeyCode::Char('s') => Some(Action::ToggleSampleTime),
        KeyCode::Char('a') => Some(Action::ToggleAge),
        KeyCode::Char('r') => Some(Action::ResetStats),
        KeyCode::Char('c') => Some(Action::ClearLog),
        KeyCode::Char('l') => Some(Action::ToggleEventLog),
        KeyCode::Esc => Some(Action::ClearSelection),
        KeyCode::Tab | KeyCode::BackTab => Some(Action::CycleFocus),
        KeyCode::Up => Some(match app.focus {
            Focus::Table => Action::TableUp,
            Focus::Events => Action::EventScrollUp,
        }),
        KeyCode::Down => Some(match app.focus {
            Focus::Table => Action::TableDown,
            Focus::Events => Action::EventScrollDown,
        }),
        KeyCode::PageUp => Some(match app.focus {
            Focus::Table => Action::TablePageUp,
            Focus::Events => Action::EventPageUp,
        }),
        KeyCode::PageDown => Some(match app.focus {
            Focus::Table => Action::TablePageDown,
            Focus::Events => Action::EventPageDown,
        }),
        KeyCode::Home => Some(match app.focus {
            Focus::Table => Action::TableHome,
            Focus::Events => Action::EventHome,
        }),
        KeyCode::End => Some(match app.focus {
            Focus::Table => Action::TableEnd,
            Focus::Events => Action::EventEnd,
        }),
        _ => None,
    }
}

impl Screen for HealthState {
    fn console(&mut self) -> &mut RpcConsole {
        &mut self.console
    }

    fn rpc_default(&self) -> DeviceRoute {
        self.cli.tio.route
    }

    fn on_batch(&mut self, batch: SampleBatch) {
        self.handle_batch(batch, Instant::now());
    }

    fn on_event(&mut self, event: StreamEvent) {
        self.handle_event(event, Instant::now());
    }

    fn on_lagged(&mut self, error: RecvError) {
        self.log_event(error.to_string(), Severity::Error);
    }

    fn on_input(&mut self, event: Event) -> bool {
        get_action(event, self).is_some_and(|action| self.update(action))
    }

    fn draw(&mut self, terminal: &mut DefaultTerminal) -> io::Result<()> {
        self.tick(Instant::now());
        draw_ui(terminal, self)
    }
}

pub fn run_health(cli: HealthCli) -> eyre::Result<()> {
    let connection = Connection::open(&cli.tio.root)?;
    // One connection: the library pumps samples, cloned trees call.
    let tree = connection.tree(cli.tio.route);
    let fps = cli.fps;
    let mut app = HealthState::new(cli, RpcConsole::new(tree.clone()));
    tui::run(&tree, fps, &mut app)
}
