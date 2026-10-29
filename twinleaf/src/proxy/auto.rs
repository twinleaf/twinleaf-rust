//! The resolution of the `auto` target. [`resolve`] picks the host's default
//! layout, the one device in use, or the one device attached.

use super::link::Link;
use super::Layout;
use crate::tio::transport::{self, PortInterface};

/// The target the proxy resolves for the client.
pub(super) const AUTO: &str = "auto";
/// The target that opens the host's default layout.
pub(super) const DEFAULT: &str = "default";

/// What `auto` resolved to.
pub(super) enum Choice<'a> {
    /// The host's default layout.
    Default(&'a Layout),
    /// An open link. Its URL may no longer find it, for example after a replug
    /// moved its device to another port.
    Open(Link),
    /// The one device attached, by URL.
    Url(String),
}

/// A link the proxy has open whose device has answered, as `auto` weighs
/// it.
#[derive(Clone)]
pub(super) struct Candidate {
    /// The link.
    pub(super) link: Link,
    /// Whether a `proxy.open` has bound it. It is `false` while only probes
    /// have.
    pub(super) chosen: bool,
}

/// Resolves `auto` to the host's `default` layout, else the one link in
/// `open` that a client chose, idle or not, else the one device `attached`
/// lists. `open` holds only the links whose device has answered. `attached`
/// runs only if it is needed.
///
/// Returns an error listing the candidates if none of these is single.
pub(super) fn resolve<'a>(
    default: Option<&'a Layout>,
    open: &[Candidate],
    attached: impl FnOnce() -> Vec<String>,
) -> Result<Choice<'a>, String> {
    if let Some(layout) = default {
        return Ok(Choice::Default(layout));
    }
    let open: Vec<&Link> = open
        .iter()
        .filter(|candidate| candidate.chosen)
        .map(|candidate| &candidate.link)
        .collect();
    if let [link] = open.as_slice() {
        return Ok(Choice::Open(Link::clone(link)));
    }
    let attached = attached();
    if let [url] = attached.as_slice() {
        return Ok(Choice::Url(url.clone()));
    }
    Err(match (open.len(), attached.len()) {
        (0, 0) => "no Twinleaf device is attached or in use, so auto finds none; \
                   pass -r <url> to name one, or find one with tio list"
            .to_string(),
        (0, n) => format!(
            "{n} Twinleaf serial devices are attached ({}) and none is in use, \
             so auto cannot choose; choose one with tio list, or pass -r <url>",
            attached.join(", ")
        ),
        (n, _) => format!(
            "{n} devices are in use ({}), so auto cannot choose; pass -r <url> to name one",
            open.iter()
                .map(|link| link.url())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    })
}

/// Returns the URLs of the Twinleaf serial devices attached to this machine.
pub(super) fn attached() -> Vec<String> {
    transport::enumerate_serial(false)
        .into_iter()
        .filter(|device| matches!(device.interface, PortInterface::STM32 | PortInterface::FTDI))
        .map(|device| device.url)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn urls(urls: &[&str]) -> Vec<String> {
        urls.iter().map(|url| url.to_string()).collect()
    }

    /// Links that a `proxy.open` bound when `chosen`, else only probes.
    fn links(urls: &[&str], chosen: bool) -> Vec<Candidate> {
        urls.iter()
            .map(|url| Candidate {
                link: Link::idle(url).0,
                chosen,
            })
            .collect()
    }

    /// What `resolve` chose, in words: `default`, `open` and the URL of a
    /// link open, or `attached` and the URL of a device attached.
    fn chose(choice: Result<Choice, String>) -> Result<String, String> {
        choice.map(|choice| match choice {
            Choice::Default(_) => "default".to_string(),
            Choice::Open(link) => format!("open {}", link.url()),
            Choice::Url(url) => format!("attached {url}"),
        })
    }

    fn unused() -> Vec<String> {
        panic!("attached devices were enumerated when they could not matter")
    }

    #[test]
    fn the_hosts_default_comes_first() {
        let layout = Layout::single("tcp://127.0.0.1:7856");
        let open = links(&["serial:///dev/ttyACM0"], true);

        assert!(matches!(
            resolve(Some(&layout), &open, unused),
            Ok(Choice::Default(chosen)) if std::ptr::eq(chosen, &layout)
        ));
    }

    /// The link open is handed back as it is, since its URL may no longer
    /// find it.
    #[test]
    fn the_one_link_open_comes_before_what_is_attached() {
        let open = links(&["serial:///dev/ttyACM0"], true);

        assert!(matches!(
            resolve(None, &open, unused),
            Ok(Choice::Open(link)) if link.id() == open[0].link.id()
        ));
    }

    #[test]
    fn the_one_device_attached_comes_last() {
        let attached = || urls(&["serial:///dev/ttyACM1"]);

        assert_eq!(
            chose(resolve(None, &[], attached)),
            Ok("attached serial:///dev/ttyACM1".to_string())
        );
    }

    /// A link only probes have bound is nobody's choice, so it neither wins
    /// nor makes the choice ambiguous.
    #[test]
    fn a_link_only_probed_is_no_choice() {
        let mut open = links(&["tcp://192.0.2.1"], false);
        let attached = || urls(&["serial:///dev/ttyACM1"]);

        assert_eq!(
            chose(resolve(None, &open, attached)),
            Ok("attached serial:///dev/ttyACM1".to_string())
        );
        assert_eq!(
            chose(resolve(None, &open, Vec::new)),
            Err(
                "no Twinleaf device is attached or in use, so auto finds none; \
                 pass -r <url> to name one, or find one with tio list"
                    .to_string()
            )
        );

        open.extend(links(&["serial:///dev/ttyACM0"], true));
        assert_eq!(
            chose(resolve(None, &open, unused)),
            Ok("open serial:///dev/ttyACM0".to_string())
        );
    }

    #[test]
    fn several_devices_attached_are_ambiguous_and_listed() {
        let attached = || urls(&["serial:///dev/ttyACM0", "serial:///dev/ttyACM1"]);

        assert_eq!(
            chose(resolve(None, &[], attached)),
            Err("2 Twinleaf serial devices are attached \
                 (serial:///dev/ttyACM0, serial:///dev/ttyACM1) and none is in use, \
                 so auto cannot choose; choose one with tio list, or pass -r <url>"
                .to_string())
        );
    }

    /// Several devices in use are the candidates, whatever is attached.
    #[test]
    fn several_devices_in_use_are_ambiguous_and_listed() {
        let open = links(&["tcp://192.0.2.1", "tcp://192.0.2.2"], true);
        let message = "2 devices are in use (tcp://192.0.2.1, tcp://192.0.2.2), \
                       so auto cannot choose; pass -r <url> to name one";

        assert_eq!(
            chose(resolve(None, &open, Vec::new)),
            Err(message.to_string())
        );
        let attached = || urls(&["serial:///dev/ttyACM0", "serial:///dev/ttyACM1"]);
        assert_eq!(
            chose(resolve(None, &open, attached)),
            Err(message.to_string())
        );
    }
}
