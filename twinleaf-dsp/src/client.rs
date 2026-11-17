//! The other end of `dsp.spec`. It reads and writes the spec of a graph
//! that is running somewhere, over its RPCs.
//!
//! The text does not fit one packet, so it moves in pieces, as firmware
//! does. A caller gives a way to make one RPC call and gets the whole text
//! back, or has the whole text taken. It works the same against a graph in
//! this process and one across the lab.

use crate::serve::SPEC_CHUNK;

/// Why a spec was not read or written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransferError<E> {
    /// A call failed. For `dsp.spec.apply`, this is the device saying the
    /// text does not check, and what is wrong with it is in the device's
    /// log.
    #[error("{0}")]
    Call(E),
    #[error("the reply to `{0}` is not a count")]
    Reply(&'static str),
    #[error("the spec kept changing while it was being read")]
    Changing,
    #[error("the spec is not text")]
    NotText,
    #[error("the device holds {held} bytes of the {sent} written")]
    Short { held: usize, sent: usize },
}

/// How many times to start over when the spec changes under a read, as it
/// does when someone is dragging a slider.
const ATTEMPTS: usize = 4;

fn count<E>(reply: Vec<u8>, name: &'static str) -> Result<usize, TransferError<E>> {
    let bytes: [u8; 4] = reply
        .as_slice()
        .try_into()
        .map_err(|_| TransferError::Reply(name))?;
    Ok(u32::from_le_bytes(bytes) as usize)
}

/// How long the spec is, as the graph tells it.
fn size<E>(
    call: &mut impl FnMut(&str, &[u8]) -> Result<Vec<u8>, E>,
) -> Result<usize, TransferError<E>> {
    count(
        call("dsp.spec.size", &[]).map_err(TransferError::Call)?,
        "dsp.spec.size",
    )
}

/// Reads the spec a graph is running. `call` makes one RPC call by name.
pub fn read_spec<E>(
    mut call: impl FnMut(&str, &[u8]) -> Result<Vec<u8>, E>,
) -> Result<String, TransferError<E>> {
    for _ in 0..ATTEMPTS {
        let expected = size(&mut call)?;
        let mut text = Vec::with_capacity(expected);
        while text.len() < expected {
            let at = (text.len() as u32).to_le_bytes();
            match call("dsp.spec.read", &at) {
                Ok(piece) if piece.is_empty() => break,
                Ok(piece) => text.extend(piece),
                // The text grew shorter than where this read began.
                Err(_) if !text.is_empty() => break,
                Err(error) => return Err(TransferError::Call(error)),
            }
        }
        // A text of another length was set meanwhile: what was gathered is
        // part of one and part of the other.
        if text.len() == expected && size(&mut call)? == expected {
            return String::from_utf8(text).map_err(|_| TransferError::NotText);
        }
    }
    Err(TransferError::Changing)
}

/// Writes `text` as the spec and has the graph run it. If the text does not
/// check, the apply fails, the graph carries on with the text it had, and
/// what was wrong is in the device's log.
pub fn write_spec<E>(
    mut call: impl FnMut(&str, &[u8]) -> Result<Vec<u8>, E>,
    text: &str,
) -> Result<(), TransferError<E>> {
    let bytes = text.as_bytes();
    let mut at = 0;
    // The first write is at offset zero even when there is nothing to
    // send, because that is what starts the text over.
    loop {
        let piece = &bytes[at..(at + SPEC_CHUNK).min(bytes.len())];
        let mut request = (at as u32).to_le_bytes().to_vec();
        request.extend_from_slice(piece);
        let reply = call("dsp.spec.write", &request).map_err(TransferError::Call)?;
        at += piece.len();
        let held = count(reply, "dsp.spec.write")?;
        if held != at {
            return Err(TransferError::Short { held, sent: at });
        }
        if at == bytes.len() {
            break;
        }
    }
    call("dsp.spec.apply", &[]).map_err(TransferError::Call)?;
    Ok(())
}
