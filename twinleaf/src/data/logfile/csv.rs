//! CSV tables of sample batches.
//!
//! [`StreamTable`] writes one stream, a row per sample with its time and
//! every column. [`write_merged`] writes chosen columns of any streams onto
//! one time column, a row per distinct time, with a cell left empty where a
//! column has no sample at that time. Both write each row as they read it.
//!
//! The two tables write numbers differently. A stream table writes a time
//! with six decimals and a value as its own type writes it. A merged table
//! writes a time with the fewest digits that read back as the same `f64`,
//! and a value as an `f64` with nine significant digits, which read back a
//! 32 bit float exactly.

use super::super::{ColumnArray, ColumnData, SampleBatch, StreamSchema};
use crate::proto::ColumnId;
use std::fmt::Write as _;
use std::io::{self, Write};
use std::sync::Arc;

/// How a table writes its times and values.
#[derive(Clone, Copy, Debug)]
enum Digits {
    /// A time with six decimals, and a value as its own type writes it.
    Fixed,
    /// A time with the fewest digits that read back as the same `f64`, and
    /// a value as an `f64` with nine significant digits.
    Significant,
}

impl Digits {
    fn time(self, line: &mut String, time: f64) {
        let written = match self {
            Digits::Fixed => write!(line, "{time:.6}"),
            Digits::Significant => write!(line, "{time}"),
        };
        written.expect("writing to a string cannot fail");
    }

    fn value(self, line: &mut String, value: ColumnData) {
        let written = match (self, value.try_as_f64()) {
            (Digits::Significant, Some(value)) => write!(line, "{}", significant(value)),
            _ => write!(line, "{value}"),
        };
        written.expect("writing to a string cannot fail");
    }
}

/// Returns `value` with nine significant digits, which give back a 32 bit
/// float exactly, without the digits that widening it to 64 bits invents.
fn significant(value: f64) -> f64 {
    format!("{value:.8e}").parse().unwrap_or(value)
}

/// Returns `fields` as one CSV line without its line end. A field with a
/// comma, a quote or a line break is quoted, its quotes doubled.
fn header_line(fields: &[String]) -> String {
    fields
        .iter()
        .map(|field| {
            if field.contains([',', '"', '\n', '\r']) {
                format!("\"{}\"", field.replace('"', "\"\""))
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Why a [`StreamTable`] did not write a batch.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WriteError {
    /// The batch is of another stream or schema than the table, so its rows
    /// do not fit the header.
    #[error("the batch is of another stream or schema than the table's header")]
    Schema,
    /// Writing to the output failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// A CSV table of one stream, written to `W` as batches arrive.
///
/// The header is `time` and the name of each column of the stream, in
/// schema order. Each row is a sample's end time and its value in every
/// column.
#[derive(Debug)]
pub struct StreamTable<W: Write> {
    out: W,
    schema: Arc<StreamSchema>,
    header: Vec<String>,
    line: String,
    rows: u64,
}

impl<W: Write> StreamTable<W> {
    /// Starts the table of the stream `schema` describes, and writes its
    /// header to `out`.
    ///
    /// # Errors
    ///
    /// Returns the error writing the header to `out` gives.
    pub fn new(schema: &Arc<StreamSchema>, mut out: W) -> io::Result<StreamTable<W>> {
        let mut header = vec!["time".to_string()];
        header.extend(schema.columns().map(|column| column.name().to_string()));
        writeln!(out, "{}", header_line(&header))?;
        Ok(StreamTable {
            out,
            schema: Arc::clone(schema),
            header,
            line: String::new(),
            rows: 0,
        })
    }

    /// Returns the names the header gives the table's columns, `time` first.
    pub fn header(&self) -> &[String] {
        &self.header
    }

    /// Returns how many rows the table has written.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Writes a row for each sample of `batch`.
    ///
    /// # Errors
    ///
    /// Returns [`WriteError::Schema`] and writes nothing if `batch` is not
    /// of the table's schema, as its rows do not fit the header. Otherwise
    /// returns [`WriteError::Io`] with the error writing to the output gives.
    pub fn write_batch(&mut self, batch: &SampleBatch) -> Result<(), WriteError> {
        if batch.schema() != &self.schema {
            return Err(WriteError::Schema);
        }
        for sample in batch.iter() {
            self.line.clear();
            Digits::Fixed.time(&mut self.line, sample.timestamp_end());
            for value in sample.values() {
                self.line.push(',');
                Digits::Fixed.value(&mut self.line, value);
            }
            self.line.push('\n');
            self.out.write_all(self.line.as_bytes())?;
            self.rows += 1;
        }
        Ok(())
    }

    /// Flushes the output.
    ///
    /// # Errors
    ///
    /// Returns the error flushing the output gives.
    pub fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }
}

/// One column of a merged table, its rows read from its batches.
#[derive(Debug, Clone)]
pub struct MergedColumn {
    /// What the header names the column.
    pub name: String,
    /// The column's units, empty for none.
    pub units: String,
    /// Whether the batches' times are Unix seconds.
    pub unix: bool,
    /// The column of the batches to read.
    pub column: ColumnId,
    /// The batches, in time order. A batch without the column is passed
    /// over.
    pub batches: Vec<SampleBatch>,
}

impl MergedColumn {
    /// Returns the rows of the column, those of each batch that holds it.
    pub fn rows(&self) -> usize {
        (self.batches.iter())
            .filter(|batch| batch.column(self.column).is_some())
            .map(SampleBatch::len)
            .sum()
    }
}

/// One column's rows, read a row at a time.
struct Cursor<'a> {
    batches: std::slice::Iter<'a, SampleBatch>,
    column: ColumnId,
    /// The times and values of the batch read now.
    current: Option<(&'a [f64], &'a ColumnArray)>,
    row: usize,
}

impl<'a> Cursor<'a> {
    fn of(column: &'a MergedColumn) -> Cursor<'a> {
        let mut cursor = Cursor {
            batches: column.batches.iter(),
            column: column.column,
            current: None,
            row: 0,
        };
        cursor.settle();
        cursor
    }

    /// Moves on to the next batch with a row left, if the batch read now
    /// has none.
    fn settle(&mut self) {
        while self
            .current
            .is_none_or(|(times, _)| self.row >= times.len())
        {
            let Some(batch) = self.batches.next() else {
                self.current = None;
                return;
            };
            self.current = batch.column(self.column).map(|v| (batch.timestamps(), v));
            self.row = 0;
        }
    }

    fn time(&self) -> Option<f64> {
        self.current.map(|(times, _)| times[self.row])
    }

    /// Returns the value of the row read now, and moves to the next.
    fn take(&mut self) -> ColumnData {
        let value = self
            .current
            .map_or(ColumnData::Unknown, |(_, v)| v.get(self.row));
        self.row += 1;
        self.settle();
        value
    }
}

/// Writes `columns` as one CSV table to `out`, and flushes it.
///
/// The header is the time, as `time (Unix s)` when every column's times
/// are Unix seconds and `time (s)` otherwise, then each column's name with
/// its units in parentheses. Each row is one distinct time, oldest first,
/// with the value each column has at that time, or nothing for a column
/// with no sample then. A header field with a comma, a quote or a line
/// break is quoted, as a stream table quotes its own.
///
/// # Errors
///
/// Returns the error writing to `out` gives.
pub fn write_merged(columns: &[MergedColumn], out: &mut impl Write) -> io::Result<()> {
    let time = if columns.iter().all(|c| c.unix) {
        "time (Unix s)"
    } else {
        "time (s)"
    };
    let mut header = vec![time.to_string()];
    header.extend(columns.iter().map(|c| match c.units.as_str() {
        "" => c.name.clone(),
        units => format!("{} ({units})", c.name),
    }));
    writeln!(out, "{}", header_line(&header))?;
    let mut cursors: Vec<Cursor> = columns.iter().map(Cursor::of).collect();
    let mut line = String::new();
    while let Some(t) = cursors.iter().filter_map(Cursor::time).reduce(f64::min) {
        line.clear();
        Digits::Significant.time(&mut line, t);
        for cursor in &mut cursors {
            line.push(',');
            if cursor.time() == Some(t) {
                Digits::Significant.value(&mut line, cursor.take());
            }
        }
        line.push('\n');
        out.write_all(line.as_bytes())?;
    }
    out.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::fixtures;
    use crate::data::model::continuity::Generations;
    use crate::data::model::sample::{BatchContext, SampleBatchBuilder};
    use crate::proto::data as wire;
    use crate::proto::data::DataType;
    use crate::proto::{DeviceRoute, SampleNumber};

    /// A batch of stream `stream_id` sampled at 4 Hz, with a column of
    /// each of `columns`, its name and type, and a row of `rows` for each
    /// sample number from `first`.
    fn batch(
        stream_id: u8,
        columns: &[(&str, DataType)],
        first: u32,
        rows: &[Vec<ColumnData>],
    ) -> SampleBatch {
        let columns: Vec<wire::Column> = columns
            .iter()
            .enumerate()
            .map(|(index, (name, data_type))| wire::Column {
                name,
                ..fixtures::column(stream_id, index as u8, *data_type)
            })
            .collect();
        let metadata = fixtures::metadata(
            DeviceRoute::root(),
            fixtures::device(),
            fixtures::stream(stream_id),
            wire::Segment {
                sampling_rate: 4,
                ..fixtures::segment(stream_id)
            },
            &columns,
        );
        let context = BatchContext {
            metadata,
            boundary: None,
            generations: Generations {
                stream: 1,
                device: 0,
                global: 0,
            },
        };
        let mut builder = SampleBatchBuilder::new(context, rows.len());
        for (n, row) in rows.iter().enumerate() {
            builder.push_row(SampleNumber::new(first + n as u32), row.iter().copied());
        }
        builder.finish()
    }

    #[test]
    fn a_stream_table_writes_six_decimal_times_and_native_values() {
        let columns = [("x,\"q\"", DataType::F32), ("count", DataType::U16)];
        let rows = [
            vec![ColumnData::F32(0.1), ColumnData::U16(7)],
            vec![ColumnData::F32(-2.5), ColumnData::U16(65535)],
        ];
        let batch = batch(1, &columns, 0, &rows);
        let mut table = StreamTable::new(batch.schema(), Vec::new()).unwrap();
        table.write_batch(&batch).unwrap();
        assert_eq!(table.rows(), 2);
        assert_eq!(table.header(), ["time", "x,\"q\"", "count"]);
        let written = String::from_utf8(table.out).unwrap();
        let expected = "time,\"x,\"\"q\"\"\",count\n0.250000,0.1,7\n0.500000,-2.5,65535\n";
        assert_eq!(written, expected);
    }

    #[test]
    fn a_stream_table_refuses_a_batch_of_another_schema() {
        let one = batch(1, &[("a", DataType::F32)], 0, &[vec![ColumnData::F32(1.0)]]);
        let other = batch(2, &[("a", DataType::F32)], 0, &[vec![ColumnData::F32(1.0)]]);
        let mut table = StreamTable::new(one.schema(), Vec::new()).unwrap();
        let error = table.write_batch(&other).unwrap_err();
        assert!(matches!(error, WriteError::Schema), "{error:?}");
        assert_eq!(table.rows(), 0);
        assert_eq!(table.out, b"time,a\n");
    }

    #[test]
    fn a_header_quotes_special_column_names() {
        assert_eq!(
            header_line(&["time".to_string(), "x,\"quoted\"".to_string()]),
            "time,\"x,\"\"quoted\"\"\""
        );
        let broken = ["a\nb".to_string(), "c\rd".to_string(), "e".to_string()];
        assert_eq!(header_line(&broken), "\"a\nb\",\"c\rd\",e");
    }

    /// An output that takes `left` bytes and then fails.
    struct Fails {
        left: usize,
    }

    impl Write for Fails {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.left == 0 {
                return Err(io::Error::other("the disk is full"));
            }
            let taken = bytes.len().min(self.left);
            self.left -= taken;
            Ok(taken)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// An output that fails is a write error of its own, which says the
    /// output's error.
    #[test]
    fn a_failed_output_is_an_io_error() {
        let one = batch(1, &[("a", DataType::F32)], 0, &[vec![ColumnData::F32(1.0)]]);
        let header = "time,a\n".len();
        let mut table = StreamTable::new(one.schema(), Fails { left: header }).unwrap();
        let error = table.write_batch(&one).unwrap_err();
        assert!(matches!(&error, WriteError::Io(_)), "{error:?}");
        assert_eq!(error.to_string(), "the disk is full");
    }

    #[test]
    fn a_value_keeps_nine_significant_digits() {
        assert_eq!(significant(f64::from(0.1f32)).to_string(), "0.100000001");
        assert_eq!(significant(2.0).to_string(), "2");
        assert_eq!(significant(-1234.5678).to_string(), "-1234.5678");
    }

    /// The merged table puts a row for each distinct time, and leaves a
    /// column empty at a time it has no sample.
    #[test]
    fn a_merged_table_writes_each_distinct_time_once() {
        let f32s = |values: &[f32]| values.iter().map(|&v| vec![ColumnData::F32(v)]).collect();
        let a: Vec<Vec<ColumnData>> = f32s(&[0.1, 0.2]);
        let b: Vec<Vec<ColumnData>> = f32s(&[5.0, 6.0]);
        let columns = [
            MergedColumn {
                name: "a.x".into(),
                units: "V".into(),
                unix: true,
                column: ColumnId::new(0),
                batches: vec![batch(1, &[("x", DataType::F32)], 0, &a)],
            },
            MergedColumn {
                name: "b.y".into(),
                units: String::new(),
                unix: false,
                column: ColumnId::new(0),
                batches: vec![
                    batch(2, &[("y", DataType::F32)], 1, &b[..1]),
                    batch(2, &[("y", DataType::F32)], 3, &b[1..]),
                ],
            },
        ];
        assert_eq!((columns[0].rows(), columns[1].rows()), (2, 2));
        let absent = MergedColumn {
            column: ColumnId::new(5),
            ..columns[0].clone()
        };
        assert_eq!(
            absent.rows(),
            0,
            "a batch without the column holds none of it"
        );
        let mut out = Vec::new();
        write_merged(&columns, &mut out).unwrap();
        let expected = "time (s),a.x (V),b.y\n0.25,0.100000001,\n0.5,0.200000003,5\n1,,6\n";
        assert_eq!(String::from_utf8(out).unwrap(), expected);
    }

    /// A merged table quotes a header field with a comma or a quote, so
    /// the table keeps its columns, and leaves the others as they are.
    #[test]
    fn a_merged_header_quotes_a_name_that_needs_it() {
        let rows = vec![vec![ColumnData::F32(1.0)]];
        let column = |name: &str, units: &str| MergedColumn {
            name: name.into(),
            units: units.into(),
            unix: false,
            column: ColumnId::new(0),
            batches: vec![batch(1, &[("x", DataType::F32)], 0, &rows)],
        };
        let columns = [
            column("a,b", "V"),
            column("say \"x\"", ""),
            column("c.d", "nT"),
        ];
        let mut out = Vec::new();
        write_merged(&columns, &mut out).unwrap();
        let header = "time (s),\"a,b (V)\",\"say \"\"x\"\"\",c.d (nT)\n";
        assert_eq!(
            String::from_utf8(out).unwrap(),
            header.to_string() + "0.25,1,1,1\n"
        );
    }
}
