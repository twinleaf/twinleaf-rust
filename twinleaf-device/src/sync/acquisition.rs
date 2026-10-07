//! Acquisition control and edge scheduling, free of any hardware.
//!
//! A start waits for a second edge, stages there, and schedules sample zero on
//! the following edge, so the board has nearly a second to program hardware.

use super::{Actions, CounterDomain, Reference};

/// How far past the staging edge a plan may still be programmed: an eighth of
/// a second, as `tlfw_acq.c` allows.
const ARM_WINDOW_DIVISOR: u32 = 8;

/// How long after the staging second event a plan may still be programmed.
const ARM_WINDOW_NS: u64 = 125_000_000;

/// A named second edge in both the reference and hardware counter domains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduledEdge {
    /// The second this edge carries.
    pub reference: Reference,
    /// Counter value of the edge.
    pub counter_edge: u32,
}

impl ScheduledEdge {
    /// The following second has the same counter value on a one-second timer.
    pub const fn next_second(self) -> Self {
        Self {
            reference: self.reference.advance(1),
            counter_edge: self.counter_edge,
        }
    }
}

/// What a board programs for a legacy-compatible start: the compare one
/// second after the edge the plan was made at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartPlan {
    /// Identifies this plan in the acknowledgements it draws.
    pub id: u16,
    /// Counter value of the staging edge, and of the target edge a second on.
    pub counter_edge: u32,
    /// Monotonic nanosecond of the staging edge.
    pub staging_ns: u64,
}

impl StartPlan {
    /// Whether the board is still early enough in the staging second to
    /// program the target edge. The counter alone cannot tell a board an
    /// eighth of a second late from one a whole second late.
    pub fn armable(&self, counter_now: u32, now_ns: u64, domain: CounterDomain) -> bool {
        let since = domain.signed_delta(counter_now, self.counter_edge);
        (0..(domain.period() / ARM_WINDOW_DIVISOR) as i64).contains(&since)
            && now_ns.saturating_sub(self.staging_ns) < ARM_WINDOW_NS
    }
}

/// Acquisition lifecycle state.
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquisitionState {
    /// Boot-time delay before an automatic start or transition to idle.
    Autostart,
    /// No acquisition is requested.
    Idle,
    /// A start is requested; the next second edge will be the staging edge.
    WaitingForEdge,
    /// Hardware has a future start edge programmed.
    Armed,
    /// The target edge occurred, but the sample source has not confirmed data.
    Starting,
    /// Samples are being acquired.
    Running,
    /// Hardware is draining or otherwise completing a stop.
    Stopping,
}

/// Work for the board-specific acquisition adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquisitionAction {
    /// Nothing for the board this pass.
    None,
    /// Program hardware compares for this future start.
    Arm(StartPlan),
    /// The staged target never arrived and this edge replaces it. Boards treat
    /// it like [`Self::Arm`]; the variant makes repeated re-arms visible.
    Rearm(StartPlan),
    /// Cancel the compares an [`Self::Arm`] programmed; no start is coming.
    Disarm,
    /// The target edge occurred. Enable any software-side acquisition path.
    Start(ScheduledEdge),
    /// Stop or drain the hardware acquisition path.
    Stop,
    /// A new reference names the same edges; streams relabel from their next
    /// segment without stopping.
    Relabel {
        /// The second the replaced reference gave the edge.
        from: u32,
        /// The new reference of that edge.
        to: Reference,
    },
}

/// The requested transition is not valid in the reported state. It is the only
/// runtime error here, and maps to the `State` RPC error.
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcquisitionError(pub AcquisitionState);

/// Pure state machine for `dev.start`, `dev.stop`, `dev.restart`, and
/// `dev.autostart`. It owns no timer: [`Synchronizer`](super::Synchronizer)
/// feeds it qualified second edges and the board confirms each transition.
pub struct AcquisitionMachine {
    state: AcquisitionState,
    autostart_seconds: u8,
    /// Only the reference: the counter anchor can move by a tick between
    /// staging and target without invalidating the plan.
    target: Option<Reference>,
    plan: u16,
    restart_after_stop: bool,
    stale_acks: u32,
    superseded_plans: u32,
}

impl AcquisitionMachine {
    /// A machine in its boot-time autostart delay.
    pub const fn new(autostart_seconds: u8) -> Self {
        Self {
            state: AcquisitionState::Autostart,
            autostart_seconds,
            target: None,
            plan: 0,
            restart_after_stop: false,
            stale_acks: 0,
            superseded_plans: 0,
        }
    }

    /// Where the acquisition stands.
    pub const fn state(&self) -> AcquisitionState {
        self.state
    }

    /// `dev.autostart`.
    pub const fn autostart_seconds(&self) -> u8 {
        self.autostart_seconds
    }

    /// Acknowledgements that named a plan the machine had replaced.
    pub const fn stale_acks(&self) -> u32 {
        self.stale_acks
    }

    /// Staged starts whose target second never arrived.
    pub const fn superseded_plans(&self) -> u32 {
        self.superseded_plans
    }

    /// Update the persistent autostart value. Changing it after the boot-time
    /// [`AcquisitionState::Autostart`] phase does not itself start a run.
    pub fn set_autostart_seconds(&mut self, seconds: u8) {
        self.autostart_seconds = seconds;
    }

    /// Advance the boot-time autostart from a count of elapsed seconds. The
    /// comparison is strict: a value of four starts after four whole seconds.
    pub fn advance_autostart(&mut self, seconds: u32) {
        if self.state != AcquisitionState::Autostart {
            return;
        }
        if self.autostart_seconds == 0 {
            self.state = AcquisitionState::Idle;
        } else if seconds > self.autostart_seconds as u32 {
            self.state = AcquisitionState::WaitingForEdge;
        }
    }

    /// Handle `dev.start`.
    pub fn request_start(&mut self) -> Result<Actions, AcquisitionError> {
        match self.state {
            AcquisitionState::Autostart | AcquisitionState::Idle => {
                self.target = None;
                self.restart_after_stop = false;
                self.state = AcquisitionState::WaitingForEdge;
                Ok(Actions::NONE)
            }
            AcquisitionState::WaitingForEdge
            | AcquisitionState::Armed
            | AcquisitionState::Starting
            | AcquisitionState::Running
            | AcquisitionState::Stopping => Err(AcquisitionError(self.state)),
        }
    }

    /// Feed one qualified second edge to the scheduler.
    pub fn on_edge(&mut self, edge: ScheduledEdge, staging_ns: u64) -> Actions {
        match self.state {
            AcquisitionState::WaitingForEdge => {
                Actions::local(self.arm_from(edge, staging_ns, false))
            }
            AcquisitionState::Armed if self.target == Some(edge.reference) => {
                self.state = AcquisitionState::Starting;
                Actions::local(AcquisitionAction::Start(edge))
            }
            AcquisitionState::Armed => {
                self.superseded_plans = self.superseded_plans.saturating_add(1);
                Actions::local(self.arm_from(edge, staging_ns, true))
            }
            AcquisitionState::Autostart
            | AcquisitionState::Idle
            | AcquisitionState::Starting
            | AcquisitionState::Running
            | AcquisitionState::Stopping => Actions::NONE,
        }
    }

    /// Rewind a start to [`AcquisitionState::Armed`]. Only valid immediately
    /// after [`Self::on_edge`] returned a start the board has not been given.
    pub fn rearm_from(&mut self, staging: ScheduledEdge, staging_ns: u64) -> AcquisitionAction {
        self.arm_from(staging, staging_ns, true)
    }

    /// Confirm that the peripheral has produced the first sample of `plan`.
    pub fn mark_running(&mut self, plan: u16) -> Result<(), AcquisitionError> {
        if plan != self.plan {
            self.stale_acks = self.stale_acks.saturating_add(1);
            return Ok(());
        }
        if self.state != AcquisitionState::Starting {
            return Err(AcquisitionError(self.state));
        }
        self.state = AcquisitionState::Running;
        Ok(())
    }

    /// Report that the board found `plan`'s staging window already past. The
    /// next qualified edge stages a new one.
    pub fn missed(&mut self, plan: u16) -> Result<(), AcquisitionError> {
        if plan != self.plan {
            self.stale_acks = self.stale_acks.saturating_add(1);
            return Ok(());
        }
        if self.state != AcquisitionState::Armed {
            return Err(AcquisitionError(self.state));
        }
        self.target = None;
        self.state = AcquisitionState::WaitingForEdge;
        Ok(())
    }

    /// Handle `dev.stop`.
    pub fn request_stop(&mut self) -> Result<Actions, AcquisitionError> {
        self.restart_after_stop = false;
        self.target = None;
        match self.state {
            AcquisitionState::Autostart
            | AcquisitionState::Idle
            | AcquisitionState::WaitingForEdge => {
                self.state = AcquisitionState::Idle;
                Ok(Actions::NONE)
            }
            AcquisitionState::Armed => {
                self.state = AcquisitionState::Idle;
                Ok(Actions::local(AcquisitionAction::Disarm))
            }
            AcquisitionState::Starting | AcquisitionState::Running => {
                self.state = AcquisitionState::Stopping;
                Ok(Actions::local(AcquisitionAction::Stop))
            }
            AcquisitionState::Stopping => Err(AcquisitionError(self.state)),
        }
    }

    /// Handle `dev.restart`.
    pub fn request_restart(&mut self) -> Result<Actions, AcquisitionError> {
        self.target = None;
        match self.state {
            AcquisitionState::Starting | AcquisitionState::Running => {
                self.restart_after_stop = true;
                self.state = AcquisitionState::Stopping;
                Ok(Actions::local(AcquisitionAction::Stop))
            }
            AcquisitionState::Stopping => {
                self.restart_after_stop = true;
                Ok(Actions::NONE)
            }
            AcquisitionState::Armed => {
                self.restart_after_stop = false;
                self.state = AcquisitionState::WaitingForEdge;
                Ok(Actions::local(AcquisitionAction::Disarm))
            }
            AcquisitionState::Autostart
            | AcquisitionState::Idle
            | AcquisitionState::WaitingForEdge => {
                self.restart_after_stop = false;
                self.state = AcquisitionState::WaitingForEdge;
                Ok(Actions::NONE)
            }
        }
    }

    /// React to a phase-anchor change: an armed start is re-planned, and a
    /// running acquisition stops and stages a new segment.
    pub fn synchronization_changed(&mut self) -> Actions {
        self.target = None;
        match self.state {
            AcquisitionState::Armed => {
                self.state = AcquisitionState::WaitingForEdge;
                Actions::local(AcquisitionAction::Disarm)
            }
            AcquisitionState::WaitingForEdge => Actions::NONE,
            AcquisitionState::Starting | AcquisitionState::Running => {
                self.restart_after_stop = true;
                self.state = AcquisitionState::Stopping;
                Actions::local(AcquisitionAction::Stop)
            }
            AcquisitionState::Stopping => {
                self.restart_after_stop = true;
                Actions::NONE
            }
            AcquisitionState::Autostart | AcquisitionState::Idle => Actions::NONE,
        }
    }

    /// React to a new reference naming the same edges, `from` becoming `to`:
    /// an armed start is re-planned, and a running acquisition relabels.
    pub fn reference_changed(&mut self, from: u32, to: Reference) -> Actions {
        match self.state {
            AcquisitionState::Armed => self.synchronization_changed(),
            AcquisitionState::Starting | AcquisitionState::Running => {
                Actions::local(AcquisitionAction::Relabel { from, to })
            }
            AcquisitionState::Autostart
            | AcquisitionState::Idle
            | AcquisitionState::WaitingForEdge
            | AcquisitionState::Stopping => Actions::NONE,
        }
    }

    /// Confirm that the board-specific stop operation has completed.
    pub fn finish_stop(&mut self) -> Result<(), AcquisitionError> {
        if self.state != AcquisitionState::Stopping {
            return Err(AcquisitionError(self.state));
        }
        self.state = if self.restart_after_stop {
            AcquisitionState::WaitingForEdge
        } else {
            AcquisitionState::Idle
        };
        self.restart_after_stop = false;
        Ok(())
    }

    fn arm_from(
        &mut self,
        staging: ScheduledEdge,
        staging_ns: u64,
        replacing: bool,
    ) -> AcquisitionAction {
        let target = staging.next_second();
        self.plan = self.plan.wrapping_add(1);
        let plan = StartPlan {
            id: self.plan,
            counter_edge: staging.counter_edge,
            staging_ns,
        };
        self.target = Some(target.reference);
        self.state = AcquisitionState::Armed;
        if replacing {
            AcquisitionAction::Rearm(plan)
        } else {
            AcquisitionAction::Arm(plan)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::ReferenceIdentity;
    use twinleaf_proto::sync::Epoch;
    use twinleaf_proto::SessionId;

    const PERIOD: u32 = 1_000_000;

    fn at(second: u32, counter_edge: u32) -> ScheduledEdge {
        ScheduledEdge {
            reference: Reference {
                identity: ReferenceIdentity::new(Epoch::UNIX, SessionId::new(42), b"parent"),
                second,
            },
            counter_edge,
        }
    }

    fn edge(second: u32) -> ScheduledEdge {
        at(second, 1234)
    }

    fn armed(machine: &mut AcquisitionMachine, second: u32) -> StartPlan {
        match machine.on_edge(edge(second), 0).local {
            AcquisitionAction::Arm(plan) | AcquisitionAction::Rearm(plan) => plan,
            action => panic!("unexpected action: {action:?}"),
        }
    }

    #[test]
    fn legacy_start_stages_one_edge_ahead() {
        let mut machine = AcquisitionMachine::new(0);
        machine.request_start().unwrap();

        let staging = edge(100);
        let plan = match machine.on_edge(staging, 7_000_000_000).local {
            AcquisitionAction::Arm(plan) => plan,
            action => panic!("unexpected action: {action:?}"),
        };
        assert_eq!(plan.counter_edge, staging.counter_edge);
        assert_eq!(plan.staging_ns, 7_000_000_000);
        assert_eq!(machine.state(), AcquisitionState::Armed);

        assert_eq!(
            machine.on_edge(edge(101), 8_000_000_000).local,
            AcquisitionAction::Start(edge(101))
        );
        assert_eq!(machine.state(), AcquisitionState::Starting);
        machine.mark_running(plan.id).unwrap();
        assert_eq!(machine.state(), AcquisitionState::Running);
    }

    #[test]
    fn an_anchor_moving_by_a_tick_still_starts_on_time() {
        let mut machine = AcquisitionMachine::new(0);
        machine.request_start().unwrap();
        machine.on_edge(at(100, 1234), 0);
        assert_eq!(
            machine.on_edge(at(101, 1235), 0).local,
            AcquisitionAction::Start(at(101, 1235))
        );
    }

    #[test]
    fn stop_before_target_cancels_start() {
        let mut machine = AcquisitionMachine::new(0);
        machine.request_start().unwrap();
        machine.on_edge(edge(10), 0);
        assert_eq!(
            machine.request_stop().unwrap(),
            Actions::local(AcquisitionAction::Disarm)
        );
        assert_eq!(machine.state(), AcquisitionState::Idle);
        assert_eq!(machine.target, None);
    }

    #[test]
    fn restart_during_run_stops_then_stages_again() {
        let mut machine = AcquisitionMachine::new(0);
        machine.request_start().unwrap();
        let plan = armed(&mut machine, 10);
        machine.on_edge(edge(11), 0);
        machine.mark_running(plan.id).unwrap();

        assert_eq!(
            machine.request_restart().unwrap(),
            Actions::local(AcquisitionAction::Stop)
        );
        assert_eq!(machine.state(), AcquisitionState::Stopping);
        machine.finish_stop().unwrap();
        assert_eq!(machine.state(), AcquisitionState::WaitingForEdge);
    }

    #[test]
    fn synchronization_change_restarts_an_active_segment() {
        let mut machine = AcquisitionMachine::new(0);
        machine.request_start().unwrap();
        let plan = armed(&mut machine, 10);
        machine.on_edge(edge(11), 0);
        machine.mark_running(plan.id).unwrap();

        assert_eq!(
            machine.synchronization_changed(),
            Actions::local(AcquisitionAction::Stop)
        );
        machine.finish_stop().unwrap();
        assert_eq!(machine.state(), AcquisitionState::WaitingForEdge);
    }

    #[test]
    fn a_reference_change_relabels_an_active_segment_and_nothing_else() {
        let mut machine = AcquisitionMachine::new(0);
        machine.request_start().unwrap();
        let plan = armed(&mut machine, 10);
        machine.on_edge(edge(11), 0);
        let to = edge(500).reference;
        let relabel = Actions::local(AcquisitionAction::Relabel { from: 12, to });
        assert_eq!(machine.reference_changed(12, to), relabel);
        machine.mark_running(plan.id).unwrap();
        assert_eq!(machine.reference_changed(12, to), relabel);
        assert_eq!(machine.state(), AcquisitionState::Running);

        machine.request_stop().unwrap();
        assert_eq!(machine.reference_changed(12, to), Actions::NONE);
        machine.finish_stop().unwrap();
        assert_eq!(machine.state(), AcquisitionState::Idle);
    }

    #[test]
    fn a_skipped_target_second_is_reported_as_a_rearm() {
        let mut machine = AcquisitionMachine::new(0);
        machine.request_start().unwrap();
        let plan = armed(&mut machine, 10);

        let replacement = match machine.on_edge(edge(50), 0).local {
            AcquisitionAction::Rearm(plan) => plan,
            action => panic!("unexpected action: {action:?}"),
        };
        assert_ne!(replacement.id, plan.id);
        assert_eq!(machine.superseded_plans(), 1);
    }

    #[test]
    fn a_wrong_state_transition_reports_the_state() {
        let mut machine = AcquisitionMachine::new(0);
        machine.request_start().unwrap();
        assert_eq!(
            machine.request_start(),
            Err(AcquisitionError(AcquisitionState::WaitingForEdge))
        );
        assert_eq!(
            machine.mark_running(0),
            Err(AcquisitionError(AcquisitionState::WaitingForEdge))
        );
    }

    /// tl-chibi's rows: a stop from idle succeeds, a restart from idle starts,
    /// and a restart while stopping is remembered.
    #[test]
    fn stop_and_restart_answer_from_every_resting_state() {
        let mut machine = AcquisitionMachine::new(0);
        machine.advance_autostart(1);
        assert_eq!(machine.state(), AcquisitionState::Idle);
        assert_eq!(machine.request_stop(), Ok(Actions::NONE));
        assert_eq!(machine.state(), AcquisitionState::Idle);

        assert_eq!(machine.request_restart(), Ok(Actions::NONE));
        assert_eq!(machine.state(), AcquisitionState::WaitingForEdge);
        let plan = armed(&mut machine, 10);
        machine.on_edge(edge(11), 0);
        machine.mark_running(plan.id).unwrap();

        machine.request_stop().unwrap();
        assert_eq!(machine.state(), AcquisitionState::Stopping);
        assert_eq!(
            machine.request_stop(),
            Err(AcquisitionError(AcquisitionState::Stopping))
        );
        assert_eq!(machine.request_restart(), Ok(Actions::NONE));
        machine.finish_stop().unwrap();
        assert_eq!(machine.state(), AcquisitionState::WaitingForEdge);
    }

    /// R1: a board that programmed a compare at `Arm` is told to cancel it.
    #[test]
    fn every_way_out_of_armed_disarms_the_board() {
        let disarming: [fn(&mut AcquisitionMachine) -> Actions; 4] = [
            |machine| machine.request_stop().unwrap(),
            |machine| machine.request_restart().unwrap(),
            AcquisitionMachine::synchronization_changed,
            |machine| machine.reference_changed(10, edge(500).reference),
        ];
        for leave in disarming {
            let mut machine = AcquisitionMachine::new(0);
            machine.request_start().unwrap();
            armed(&mut machine, 10);
            assert_eq!(leave(&mut machine).local, AcquisitionAction::Disarm);
        }
    }

    /// R1: a plan the board could not program is staged again from scratch.
    #[test]
    fn a_missed_plan_waits_for_the_next_edge_and_arms_it() {
        let mut machine = AcquisitionMachine::new(0);
        machine.request_start().unwrap();
        let plan = armed(&mut machine, 10);

        machine.missed(plan.id).unwrap();
        assert_eq!(machine.state(), AcquisitionState::WaitingForEdge);
        let next = match machine.on_edge(edge(11), 0).local {
            AcquisitionAction::Arm(plan) => plan,
            action => panic!("unexpected action: {action:?}"),
        };
        assert_ne!(next.id, plan.id);
        assert_eq!(machine.superseded_plans(), 0);
    }

    /// D10: an acknowledgement that names a replaced plan changes nothing.
    #[test]
    fn a_stale_plan_id_is_ignored_and_counted() {
        let mut machine = AcquisitionMachine::new(0);
        machine.request_start().unwrap();
        let first = armed(&mut machine, 10);
        machine.missed(first.id).unwrap();
        let second = armed(&mut machine, 11);

        assert_eq!(machine.missed(first.id), Ok(()));
        assert_eq!(machine.mark_running(first.id), Ok(()));
        assert_eq!(machine.state(), AcquisitionState::Armed);
        assert_eq!(machine.stale_acks(), 2);

        machine.on_edge(edge(12), 0);
        assert_eq!(machine.mark_running(second.id), Ok(()));
        assert_eq!(machine.state(), AcquisitionState::Running);
    }

    /// R9: the counter alone cannot tell an eighth of a second late from a
    /// whole second late, so the plan carries its wall time too.
    #[test]
    fn a_plan_is_armable_only_just_after_its_staging_edge() {
        let domain = CounterDomain::new(PERIOD);
        let plan = StartPlan {
            id: 1,
            counter_edge: PERIOD - 10,
            staging_ns: 5_000_000_000,
        };
        assert!(plan.armable(PERIOD - 10, 5_000_000_000, domain));
        assert!(plan.armable(PERIOD / 16, 5_060_000_000, domain));
        assert!(!plan.armable(PERIOD / 4, 5_060_000_000, domain));
        assert!(!plan.armable(PERIOD - 11, 5_000_000_000, domain));
        assert!(!plan.armable(PERIOD - 10, 6_000_000_000, domain));
    }
}
