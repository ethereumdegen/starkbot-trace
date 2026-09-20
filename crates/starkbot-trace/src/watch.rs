//! The dashboard you leave open while the agent works.
//!
//! `tail --follow` answers "what is happening right now" one line at a time,
//! which is the wrong shape for watching: by the time an interesting jev step
//! scrolls past, the turn it belonged to is gone. This surface keeps the
//! recent turns, the live record stream and the last fifteen minutes of
//! latency on screen at once, and lets a turn be pinned so the stream shows
//! only that turn's records.
//!
//! It owns the terminal while it runs, so every exit path — quit, error, or
//! panic — goes through a guard that hands raw mode back.

use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph, Wrap};
use ratatui::{DefaultTerminal, Frame};
use time::OffsetDateTime;

use crate::report::{clip, clock, fmt_ms, short_id, stamp};
use crate::store::{Filter, Row, Stats, Trace, TurnSummary};

/// How long a frame waits for a key before the store is polled again. It is
/// also the refresh cadence: fast enough to read as live, slow enough that an
/// idle dashboard costs nothing.
const TICK: Duration = Duration::from_millis(250);
/// The window the side panel summarises. Long enough to cover the turn that
/// just ran, short enough that yesterday's numbers do not hide today's.
const WINDOW_MS: i64 = 15 * 60 * 1_000;
/// Records kept in memory for the stream pane, and the most fetched per poll.
const STREAM_CAP: usize = 500;
const STREAM_BATCH: usize = 200;
/// Turns offered in the list.
const TURN_CAP: usize = 200;

const MIN_WIDTH: u16 = 72;
const MIN_HEIGHT: u16 = 16;
const SIDE_WIDTH: u16 = 32;
const GREY: Color = Color::DarkGray;

/// Take over the terminal, run the dashboard, and give the terminal back.
pub fn watch(trace: &Trace) -> Result<()> {
    let mut guard = TerminalGuard::new()?;
    event_loop(trace, &mut guard.terminal)
}

/// Restores the terminal on drop. `ratatui::try_init` additionally installs a
/// panic hook that restores before the message is printed, so neither a panic
/// nor a `?` on a SQLite error can leave the user in raw mode on the
/// alternate screen.
struct TerminalGuard {
    terminal: DefaultTerminal,
}

impl TerminalGuard {
    fn new() -> io::Result<Self> {
        Ok(Self {
            terminal: ratatui::try_init()?,
        })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        // There is nothing useful to say about a failure here: the screen the
        // complaint would land on is the one that could not be restored.
        let _ = ratatui::try_restore();
    }
}

fn event_loop(trace: &Trace, terminal: &mut DefaultTerminal) -> Result<()> {
    let mut dashboard = Dashboard::new(trace)?;
    let mut last_refresh = Instant::now();
    loop {
        terminal.draw(|frame| draw(frame, &dashboard))?;

        if event::poll(TICK)? {
            match event::read()? {
                // A key press and its release both arrive on the terminals
                // that report releases, and a dashboard that acts twice per
                // keystroke skips a turn every time the selection moves.
                Event::Key(key) if key.kind == KeyEventKind::Press => {
                    if dashboard.on_key(key, trace)? == Flow::Quit {
                        return Ok(());
                    }
                }
                _ => {}
            }
        }

        if last_refresh.elapsed() >= TICK {
            dashboard.refresh(trace)?;
            last_refresh = Instant::now();
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Flow {
    Continue,
    Quit,
}

/// Everything on screen. The store is the only source of truth; this is the
/// slice of it the current frame draws.
struct Dashboard {
    turns: Vec<TurnSummary>,
    selected: usize,
    stream: VecDeque<Row>,
    last_id: i64,
    filter: Filter,
    stats: Stats,
    paused: bool,
}

impl Dashboard {
    fn new(trace: &Trace) -> Result<Self> {
        let mut dashboard = Self {
            turns: Vec::new(),
            selected: 0,
            stream: VecDeque::new(),
            last_id: 0,
            filter: Filter::default(),
            stats: trace.stats(Some(window_start()))?,
            paused: false,
        };
        dashboard.reload_stream(trace)?;
        dashboard.refresh(trace)?;
        Ok(dashboard)
    }

    /// Start the stream from the newest records. `rows_after(0, …)` would
    /// replay the database from its very first record instead, which on a
    /// week-old collector means the pane shows last Tuesday.
    fn reload_stream(&mut self, trace: &Trace) -> Result<()> {
        let rows = trace.rows(&self.filter, STREAM_CAP)?;
        self.last_id = rows.last().map_or(0, |row| row.id);
        self.stream = rows.into_iter().collect();
        Ok(())
    }

    fn refresh(&mut self, trace: &Trace) -> Result<()> {
        let anchor = self.turns.get(self.selected).map(|turn| turn.turn.clone());
        self.turns = trace.turns(TURN_CAP)?;
        // New turns arrive at the top of the list, so the cursor follows the
        // turn the user picked rather than sliding down under them.
        self.selected = anchor
            .and_then(|id| self.turns.iter().position(|turn| turn.turn == id))
            .unwrap_or(self.selected)
            .min(self.turns.len().saturating_sub(1));
        self.stats = trace.stats(Some(window_start()))?;

        if self.paused {
            return Ok(());
        }
        let rows = trace.rows_after(self.last_id, &self.filter, STREAM_BATCH)?;
        if let Some(row) = rows.last() {
            self.last_id = row.id;
        }
        for row in rows {
            while self.stream.len() >= STREAM_CAP {
                self.stream.pop_front();
            }
            self.stream.push_back(row);
        }
        Ok(())
    }

    fn on_key(&mut self, key: KeyEvent, trace: &Trace) -> Result<Flow> {
        match key.code {
            // Raw mode swallows SIGINT, so Ctrl-C arrives as a key: without
            // this arm the habitual way out of a terminal program does
            // nothing at all.
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Ok(Flow::Quit);
            }
            KeyCode::Char('q') => return Ok(Flow::Quit),
            // Esc drops the pin before it drops the dashboard: closing the
            // whole view because you wanted to stop filtering is the annoying
            // version of this.
            KeyCode::Esc => {
                if self.filter.turn.is_none() {
                    return Ok(Flow::Quit);
                }
                self.filter.turn = None;
                self.reload_stream(trace)?;
            }
            KeyCode::Char('j') | KeyCode::Down => self.move_selection(true),
            KeyCode::Char('k') | KeyCode::Up => self.move_selection(false),
            KeyCode::Enter => {
                if let Some(turn) = self.turns.get(self.selected) {
                    self.filter.turn = Some(turn.turn.clone());
                    self.reload_stream(trace)?;
                }
            }
            KeyCode::Char('p') => self.paused = !self.paused,
            _ => {}
        }
        Ok(Flow::Continue)
    }

    fn move_selection(&mut self, down: bool) {
        if self.turns.is_empty() {
            self.selected = 0;
            return;
        }
        let last = self.turns.len().saturating_sub(1);
        self.selected = if down {
            self.selected.saturating_add(1).min(last)
        } else {
            self.selected.saturating_sub(1)
        };
    }
}

// ------------------------------------------------------------------ drawing

fn draw(frame: &mut Frame, dashboard: &Dashboard) {
    let area = frame.area();
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        frame.render_widget(
            Paragraph::new(Line::styled(
                format!("terminal too small — {MIN_WIDTH}×{MIN_HEIGHT} minimum"),
                Style::new().fg(Color::Yellow),
            )),
            area,
        );
        return;
    }

    let [header, body, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .areas(area);

    // The side panel only earns its columns on a wide terminal; below that the
    // stream is worth more than the numbers.
    let side_width = if area.width >= MIN_WIDTH + SIDE_WIDTH {
        SIDE_WIDTH
    } else {
        0
    };
    let [left, side] =
        Layout::horizontal([Constraint::Min(40), Constraint::Length(side_width)]).areas(body);
    let [turns, stream] =
        Layout::vertical([Constraint::Percentage(40), Constraint::Min(3)]).areas(left);

    frame.render_widget(header_line(dashboard), header);
    render_turns(frame, turns, dashboard);
    render_stream(frame, stream, dashboard);
    if side_width > 0 {
        render_stats(frame, side, dashboard);
    }
    frame.render_widget(footer_line(dashboard), footer);
}

fn header_line(dashboard: &Dashboard) -> Paragraph<'_> {
    let mut spans = vec![
        Span::styled("starkbot-trace", Style::new().add_modifier(Modifier::BOLD)),
        Span::styled("  last 15 min  ", Style::new().fg(GREY)),
        Span::raw(format!(
            "{} records · {} turns · {} inferences",
            dashboard.stats.records, dashboard.stats.turns, dashboard.stats.inferences
        )),
    ];
    if let Some(turn) = dashboard.filter.turn.as_deref() {
        spans.push(Span::styled(
            format!("   pinned to turn {}", short_id(turn)),
            Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        ));
    }
    if dashboard.paused {
        spans.push(Span::styled(
            "   PAUSED",
            Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ));
    }
    Paragraph::new(Line::from(spans))
}

fn footer_line(dashboard: &Dashboard) -> Paragraph<'_> {
    let escape = if dashboard.filter.turn.is_some() {
        "Esc clears the pin"
    } else {
        "Esc quits"
    };
    Paragraph::new(Line::styled(
        format!(
            "q quit · j/k select · Enter pins the selected turn · {escape} · p pauses the stream"
        ),
        Style::new().fg(GREY),
    ))
}

fn render_turns(frame: &mut Frame, area: Rect, dashboard: &Dashboard) {
    let block = Block::bordered()
        .border_style(Style::new().fg(GREY))
        .title(format!(" Turns ({}) ", dashboard.turns.len()));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 || inner.width == 0 {
        return;
    }

    if dashboard.turns.is_empty() {
        frame.render_widget(
            Paragraph::new(Line::styled(
                "no turns yet — one appears here as soon as the agent starts working",
                Style::new().fg(GREY),
            ))
            .wrap(Wrap { trim: false }),
            inner,
        );
        return;
    }

    let width = usize::from(inner.width);
    let lines: Vec<Line<'_>> = dashboard
        .turns
        .iter()
        .enumerate()
        .map(|(index, turn)| turn_line(turn, index == dashboard.selected, width))
        .collect();
    // Keep the cursor on screen without a scrollbar: the window ends at the
    // selected row, exactly as the settings list does in neo-tui.
    let height = usize::from(inner.height);
    let offset = dashboard.selected.saturating_sub(height.saturating_sub(1));
    frame.render_widget(
        Paragraph::new(lines).scroll((u16::try_from(offset).unwrap_or(0), 0)),
        inner,
    );
}

fn turn_line(turn: &TurnSummary, selected: bool, width: usize) -> Line<'static> {
    let marker = if selected { "▸ " } else { "  " };
    let head = format!(
        "{marker}{} {:<8} {:<11} {:>3}s {:>3}i {:>4}j {:>6}/{:<6} {}",
        stamp(turn.started_ms),
        short_id(&turn.turn),
        clip(&turn.source, 11),
        turn.steps,
        turn.inferences,
        turn.jev_steps,
        turn.input_tokens,
        turn.output_tokens,
        if turn.failed { "failed" } else { "ok" }
    );
    let text = format!("{head}  {}", turn.text);
    let style = match (selected, turn.failed) {
        (true, _) => Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        (false, true) => Style::new().fg(Color::Red),
        (false, false) => Style::new(),
    };
    Line::styled(clip(&text, width), style)
}

fn render_stream(frame: &mut Frame, area: Rect, dashboard: &Dashboard) {
    let title = match dashboard.filter.turn.as_deref() {
        Some(turn) => format!(" Stream · turn {} ", short_id(turn)),
        None => " Stream ".to_owned(),
    };
    let border = if dashboard.filter.turn.is_some() {
        Style::new().fg(Color::Cyan)
    } else {
        Style::new().fg(GREY)
    };
    let block = Block::bordered().border_style(border).title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 || inner.width == 0 {
        return;
    }

    if dashboard.stream.is_empty() {
        let empty = if dashboard.filter.turn.is_some() {
            "no records for this turn yet"
        } else {
            "nothing has arrived yet — records appear here as soon as an agent connects"
        };
        frame.render_widget(
            Paragraph::new(Line::styled(empty, Style::new().fg(GREY))).wrap(Wrap { trim: false }),
            inner,
        );
        return;
    }

    // The newest record is the one being read, so the pane shows the tail of
    // the ring and never scrolls away from it.
    let height = usize::from(inner.height);
    let width = usize::from(inner.width);
    let skip = dashboard.stream.len().saturating_sub(height);
    let lines: Vec<Line<'_>> = dashboard
        .stream
        .iter()
        .skip(skip)
        .map(|row| stream_line(row, width))
        .collect();
    frame.render_widget(Paragraph::new(lines), inner);
}

fn stream_line(row: &Row, width: usize) -> Line<'static> {
    let duration = row.duration_ms.map(fmt_ms).unwrap_or_default();
    let text = format!(
        "{} {:<18} {:>8}  {}",
        clock(row.ts_ms),
        clip(&row.kind, 18),
        duration,
        row.label
    );
    let style = match row.ok {
        Some(false) => Style::new().fg(Color::Red),
        _ => kind_style(&row.kind),
    };
    Line::styled(clip(&text, width), style)
}

/// Colour carries no information on its own here — the kind is spelled out in
/// its own column — it only makes the shape of a turn visible at a glance.
fn kind_style(kind: &str) -> Style {
    match kind {
        "turn_started" | "turn_finished" => Style::new().fg(Color::Cyan),
        "turn_failed" => Style::new().fg(Color::Red),
        "inference" => Style::new().fg(Color::Blue),
        "jev_step" => Style::new().fg(GREY),
        _ => Style::new(),
    }
}

fn render_stats(frame: &mut Frame, area: Rect, dashboard: &Dashboard) {
    let block = Block::bordered()
        .border_style(Style::new().fg(GREY))
        .title(" Last 15 min ");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 || inner.width == 0 {
        return;
    }

    let stats = &dashboard.stats;
    if stats.records == 0 {
        frame.render_widget(
            Paragraph::new(Line::styled(
                "quiet — nothing in the last fifteen minutes",
                Style::new().fg(GREY),
            ))
            .wrap(Wrap { trim: false }),
            inner,
        );
        return;
    }

    let mut lines = vec![
        count_line("records", stats.records),
        count_line("runs", stats.runs),
        count_line("turns", stats.turns),
        count_line("steps", stats.steps),
        count_line("jev steps", stats.jev_steps),
        count_line("inferences", stats.inferences),
    ];
    if stats.failures > 0 {
        lines.push(Line::styled(
            format!("{:<12}{:>8}", "failures", stats.failures),
            Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
        ));
    } else {
        lines.push(count_line("failures", 0));
    }

    lines.push(Line::raw(""));
    lines.push(Line::styled(
        format!("{:<10}{:>8}{:>9}", "", "p50", "p95"),
        Style::new().fg(GREY),
    ));
    lines.push(latency_line("inference", &stats.inference_ms));
    lines.push(latency_line("jev", &stats.jev_ms));
    lines.push(latency_line("step", &stats.step_ms));

    lines.push(Line::raw(""));
    lines.push(count_line("tokens in", stats.input_tokens));
    lines.push(count_line("tokens out", stats.output_tokens));

    if let Some((operation, count)) = stats.by_operation.first() {
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            format!("top action  {operation} ({count})"),
            Style::new().fg(GREY),
        ));
    }

    frame.render_widget(Paragraph::new(lines), inner);
}

fn count_line(label: &str, count: u64) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label:<12}"), Style::new().fg(GREY)),
        Span::raw(format!("{count:>8}")),
    ])
}

fn latency_line(label: &str, percentiles: &crate::store::Percentiles) -> Line<'static> {
    if percentiles.count == 0 {
        return Line::styled(
            format!("{label:<10}{:>8}{:>9}", "-", "-"),
            Style::new().fg(GREY),
        );
    }
    Line::from(vec![
        Span::styled(format!("{label:<10}"), Style::new().fg(GREY)),
        Span::raw(format!(
            "{:>8}{:>9}",
            fmt_ms(percentiles.p50),
            fmt_ms(percentiles.p95)
        )),
    ])
}

/// The start of the summary window, in unix milliseconds.
fn window_start() -> i64 {
    now_ms().saturating_sub(WINDOW_MS)
}

fn now_ms() -> i64 {
    i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000).unwrap_or(0)
}
