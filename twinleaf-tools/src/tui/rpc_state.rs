use twinleaf::{
    device::{RpcList, RpcRegistry},
    tio::proto::{identifiers::SessionId, ProxyStatus},
};

use crate::tui::rpc_palette::RpcPaletteStatus;

enum RouteRpcPhase {
    Empty,
    Fetching,
    Ready {
        registry: RpcRegistry,
    },
    Refreshing {
        registry: RpcRegistry,
    },
    Disconnected {
        registry: Option<RpcRegistry>,
    },
    Failed {
        registry: Option<RpcRegistry>,
        error: String,
    },
}

pub struct RouteRpcState {
    phase: RouteRpcPhase,
    last_session_id: Option<SessionId>,
}

impl Default for RouteRpcState {
    fn default() -> Self {
        Self {
            phase: RouteRpcPhase::Empty,
            last_session_id: None,
        }
    }
}

impl RouteRpcState {
    pub fn registry(&self) -> Option<&RpcRegistry> {
        match &self.phase {
            RouteRpcPhase::Ready { registry } | RouteRpcPhase::Refreshing { registry, .. } => {
                Some(registry)
            }
            RouteRpcPhase::Disconnected {
                registry: Some(registry),
            }
            | RouteRpcPhase::Failed {
                registry: Some(registry),
                ..
            } => Some(registry),
            _ => None,
        }
    }

    pub fn palette_status(&self) -> RpcPaletteStatus {
        match &self.phase {
            RouteRpcPhase::Empty => RpcPaletteStatus::WaitingForRpc,
            RouteRpcPhase::Fetching => RpcPaletteStatus::Fetching,
            RouteRpcPhase::Ready { .. } => RpcPaletteStatus::Ready,
            RouteRpcPhase::Refreshing { .. } => RpcPaletteStatus::Refreshing,
            RouteRpcPhase::Disconnected { .. } => RpcPaletteStatus::Disconnected,
            RouteRpcPhase::Failed { registry, .. } => {
                if registry.is_some() {
                    RpcPaletteStatus::FailedUsingPrevious
                } else {
                    RpcPaletteStatus::Failed
                }
            }
        }
    }

    pub fn last_error(&self) -> Option<&str> {
        match &self.phase {
            RouteRpcPhase::Failed { error, .. } => Some(error.as_str()),
            _ => None,
        }
    }

    pub fn on_route_discovered(&mut self) -> bool {
        match &self.phase {
            RouteRpcPhase::Empty
            | RouteRpcPhase::Disconnected { registry: None }
            | RouteRpcPhase::Failed { registry: None, .. } => {
                self.phase = RouteRpcPhase::Fetching;
                true
            }
            _ => false,
        }
    }

    pub fn on_status(&mut self, status: ProxyStatus) -> bool {
        match status {
            ProxyStatus::SensorDisconnected
            | ProxyStatus::FailedToConnect
            | ProxyStatus::FailedToReconnect => {
                let registry = self.take_registry();
                self.phase = RouteRpcPhase::Disconnected { registry };
                false
            }
            ProxyStatus::SensorReconnected => self.begin_fetch(),
            ProxyStatus::Unknown(_) => false,
        }
    }

    pub fn on_heartbeat(&mut self, session_id: Option<SessionId>) -> bool {
        let changed = match (self.last_session_id, session_id) {
            (Some(old), Some(new)) => old != new,
            _ => false,
        };
        if let Some(new) = session_id {
            self.last_session_id = Some(new);
        }

        if changed {
            return self.begin_fetch();
        }

        match &self.phase {
            RouteRpcPhase::Empty | RouteRpcPhase::Failed { registry: None, .. } => {
                self.begin_fetch()
            }
            _ => false,
        }
    }

    pub fn on_new_hash(&mut self, hash: Option<u32>) -> bool {
        match hash {
            Some(hash) => {
                if self.registry().and_then(|reg| reg.hash) == Some(hash) {
                    if let Some(registry) = self.take_registry() {
                        self.phase = RouteRpcPhase::Ready { registry };
                    }
                    false
                } else {
                    self.begin_fetch()
                }
            }
            None => self.begin_fetch(),
        }
    }

    pub fn on_fetch_success(&mut self, list: &RpcList) {
        let registry = RpcRegistry::from(list);
        self.phase = RouteRpcPhase::Ready { registry };
    }

    pub fn on_fetch_error(&mut self, error: String) {
        let registry = self.take_registry();
        self.phase = RouteRpcPhase::Failed { registry, error };
    }

    fn begin_fetch(&mut self) -> bool {
        match &self.phase {
            RouteRpcPhase::Fetching | RouteRpcPhase::Refreshing { .. } => false,
            _ => {
                if let Some(registry) = self.take_registry() {
                    self.phase = RouteRpcPhase::Refreshing { registry };
                } else {
                    self.phase = RouteRpcPhase::Fetching;
                }
                true
            }
        }
    }

    fn take_registry(&mut self) -> Option<RpcRegistry> {
        let phase = std::mem::replace(&mut self.phase, RouteRpcPhase::Empty);
        match phase {
            RouteRpcPhase::Ready { registry } | RouteRpcPhase::Refreshing { registry, .. } => {
                Some(registry)
            }
            RouteRpcPhase::Disconnected { registry } | RouteRpcPhase::Failed { registry, .. } => {
                registry
            }
            RouteRpcPhase::Empty | RouteRpcPhase::Fetching => None,
        }
    }
}
