//! Shared scroll math for list views.

/// Lines of context kept visible past the selection during keyboard
/// navigation (vim `scrolloff`). Mouse paths pass 0.
pub const NAV_MARGIN: usize = 3;

/// Scroll `sel` into view with minimal movement, keeping `margin` lines of
/// context past it. The margin collapses at the list ends and in viewports
/// too short to honor it; a selection already inside the viewport (plus
/// margin) never moves the view.
pub fn follow_scroll(
    scroll: usize,
    sel: usize,
    view_h: usize,
    total: usize,
    margin: usize,
) -> usize {
    if view_h == 0 || total <= view_h {
        return 0;
    }
    let max_scroll = total - view_h;
    let scroll = scroll.min(max_scroll);
    let margin = margin.min((view_h - 1) / 2);
    if sel < scroll + margin {
        sel.saturating_sub(margin)
    } else if sel + margin >= scroll + view_h {
        (sel + margin + 1 - view_h).min(max_scroll)
    } else {
        scroll
    }
}

/// Wheel-scroll one row, clamped to the content; never moves the selection.
pub fn wheel_scroll(scroll: usize, down: bool, view_h: usize, total: usize) -> usize {
    let max = total.saturating_sub(view_h);
    if down {
        (scroll + 1).min(max)
    } else {
        scroll.min(max).saturating_sub(1)
    }
}
