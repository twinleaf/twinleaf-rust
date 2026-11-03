//! Wire bytes to native values.
//!
//! A stream's samples sit back to back, each packing its columns
//! little-endian at the offsets their types add up to. [`decode_samples`]
//! reads a packet's samples column by column, each in one strided pass, and
//! [`ColumnData::from_le_bytes`] reads one value.

use super::metadata::StreamSchema;
use super::sample::{native_types, ColumnBuilder, ColumnData};
use crate::proto::data as wire;

macro_rules! define_decoders {
    ($($variant:ident $native:ty),* $(,)?) => {
        /// Returns `true` if this build decodes columns of `data_type`.
        pub(crate) fn decodes(data_type: wire::DataType) -> bool {
            matches!(data_type, $(wire::DataType::$variant)|*)
        }

        impl ColumnData {
            /// Decodes the value of `data_type` at the front of `data`, or
            /// returns [`ColumnData::Unknown`] for a type this build cannot
            /// decode.
            ///
            /// # Panics
            ///
            /// Panics if `data` is shorter than the type's size.
            pub(crate) fn from_le_bytes(data: &[u8], data_type: wire::DataType) -> ColumnData {
                match data_type {
                    $(
                        wire::DataType::$variant => Self::$variant(<$native>::from_le_bytes(
                            data[..size_of::<$native>()]
                                .try_into()
                                .expect("a value of its own size"),
                        )),
                    )*
                    _ => Self::Unknown,
                }
            }
        }

        /// Appends the value `offset` bytes into each sample of `samples`,
        /// which packs samples `stride` bytes apart, to `values`. The column's
        /// type decides the loop once, so each value is a plain load.
        fn decode_column(values: &mut ColumnBuilder, samples: &[u8], stride: usize, offset: usize) {
            match values {
                $(
                    ColumnBuilder::$variant(values) => {
                        values.extend(samples.chunks_exact(stride).map(|sample| {
                            <$native>::from_le_bytes(
                                sample[offset..offset + size_of::<$native>()]
                                    .try_into()
                                    .expect("a value of its own size"),
                            )
                        }))
                    }
                )*
            }
        }
    };
}

native_types!(define_decoders);

/// Appends the first `rows` samples of `encoded`, laid out as `schema`
/// describes, to `columns`, one builder per schema column in index order.
///
/// # Panics
///
/// Panics if `encoded` holds fewer than `rows` samples.
pub(super) fn decode_samples<'a>(
    schema: &StreamSchema,
    encoded: &[u8],
    rows: usize,
    columns: impl IntoIterator<Item = &'a mut ColumnBuilder>,
) {
    let stride = usize::from(schema.stream().sample_size());
    let samples = &encoded[..rows * stride];
    let mut offset = 0;
    for (column, values) in schema.columns().zip(columns) {
        decode_column(values, samples, stride, offset);
        offset += column.data_type().size();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::data::DataType;

    /// No device sends 24-bit columns, so they are not decoded. Their type
    /// codes still parse, and the parser steps over their bytes.
    #[test]
    fn int24_values_are_not_decoded() {
        for data_type in [DataType::I24, DataType::U24] {
            assert!(!decodes(data_type));
            assert!(matches!(
                ColumnData::from_le_bytes(&[0xff, 0xff, 0xff], data_type),
                ColumnData::Unknown
            ));
        }
    }
}
