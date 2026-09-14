//! The configuration image: what `dev.conf.save` hands the platform to write,
//! and what `dev.conf.load` or a boot hands back.
//!
//! An image is a run of entries, each the name of a setting and the bytes its
//! RPC replies with, so a firmware may add or drop a setting without migrating
//! what is already stored. The flash itself is the platform's.

use twinleaf_proto::rpc::RpcError;

use crate::rpc::Reply;
use crate::settings::Persisted;

/// The bytes a configuration is stored as, in flash the platform sizes.
pub type Image<const N: usize> = heapless::Vec<u8, N>;

/// One stored setting: the name it was saved under and its value bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry<'a> {
    /// The name of the setting the value belongs to.
    pub name: &'a [u8],
    /// The value, as that setting's RPC replies it.
    pub value: &'a [u8],
}

/// Append every setting to `image` as its name and the bytes its RPC replies,
/// taking each as saved.
pub fn encode<const N: usize>(
    settings: &mut [&mut dyn Persisted],
    image: &mut Image<N>,
) -> Result<(), RpcError> {
    for setting in settings.iter_mut() {
        let mut value = Reply::new();
        setting.save(&mut value)?;
        let name = setting.name().as_bytes();
        let name_len = u8::try_from(name.len()).map_err(|_| RpcError::Save)?;
        let value_len = u8::try_from(value.len()).map_err(|_| RpcError::Save)?;
        for bytes in [&[name_len, value_len][..], name, &value] {
            image
                .extend_from_slice(bytes)
                .map_err(|_| RpcError::NoBufs)?;
        }
    }
    Ok(())
}

/// The entries of an image, ending with [`RpcError::Load`] at the first one
/// that does not fit in what is left.
pub fn entries(bytes: &[u8]) -> Entries<'_> {
    Entries { rest: bytes }
}

/// The iterator [`entries`] returns.
#[derive(Clone, Debug)]
pub struct Entries<'a> {
    rest: &'a [u8],
}

impl<'a> Iterator for Entries<'a> {
    type Item = Result<Entry<'a>, RpcError>;

    fn next(&mut self) -> Option<Self::Item> {
        let split = match self.rest {
            [] => return None,
            [name_len, value_len, fields @ ..] => fields
                .split_at_checked(usize::from(*name_len))
                .and_then(|(name, rest)| {
                    let (value, rest) = rest.split_at_checked(usize::from(*value_len))?;
                    Some((Entry { name, value }, rest))
                }),
            [_] => None,
        };
        match split {
            Some((entry, rest)) => {
                self.rest = rest;
                Some(Ok(entry))
            }
            None => {
                self.rest = &[];
                Some(Err(RpcError::Load))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::Setting;

    /// As much flash as the tests give a configuration.
    const FLASH: usize = 1024;

    fn image(settings: &mut [&mut dyn Persisted]) -> Image<FLASH> {
        let mut image = Image::new();
        encode(settings, &mut image).unwrap();
        image
    }

    /// An entry as the tests read it back.
    type Decoded = Result<(Vec<u8>, Vec<u8>), RpcError>;

    fn decoded(bytes: &[u8]) -> Vec<Decoded> {
        entries(bytes)
            .map(|entry| entry.map(|entry| (entry.name.to_vec(), entry.value.to_vec())))
            .collect()
    }

    #[test]
    fn an_image_carries_each_name_and_the_bytes_its_rpc_replies() {
        let mut gain = Setting::new("app.gain", 7u16).persistent();
        let mut enable = Setting::new("app.enable", true).persistent();
        let image = image(&mut [&mut gain, &mut enable]);

        assert_eq!(
            image.as_slice(),
            [
                &[8, 2][..],
                b"app.gain",
                &7u16.to_le_bytes(),
                &[10, 1],
                b"app.enable",
                &[1]
            ]
            .concat()
        );
        assert_eq!(
            decoded(&image),
            [
                Ok((b"app.gain".to_vec(), 7u16.to_le_bytes().to_vec())),
                Ok((b"app.enable".to_vec(), vec![1])),
            ]
        );
    }

    #[test]
    fn an_empty_image_has_no_entries() {
        assert_eq!(decoded(&[]), []);
        assert_eq!(image(&mut []), Image::<FLASH>::new());
    }

    #[test]
    fn an_image_that_does_not_add_up_ends_in_a_load_error() {
        let mut gain = Setting::new("app.gain", 7u16).persistent();
        let image = image(&mut [&mut gain]);

        for truncated in 1..image.len() {
            assert_eq!(
                decoded(&image[..truncated]).last(),
                Some(&Err(RpcError::Load)),
                "{truncated} bytes"
            );
        }
        assert_eq!(decoded(&[1]), [Err(RpcError::Load)]);
    }

    #[test]
    fn an_image_too_large_for_the_flash_is_refused() {
        let mut settings: Vec<Setting<u32>> = (0..FLASH / 8)
            .map(|_| Setting::new("app.wide", 0u32).persistent())
            .collect();
        let mut listed: Vec<&mut dyn Persisted> = settings
            .iter_mut()
            .map(|setting| setting as &mut dyn Persisted)
            .collect();
        let mut image = Image::<FLASH>::new();
        assert_eq!(encode(&mut listed, &mut image), Err(RpcError::NoBufs));
    }
}
