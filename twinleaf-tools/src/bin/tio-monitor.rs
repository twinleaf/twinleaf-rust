use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::time::{Duration, Instant};

use toml_edit::{DocumentMut, InlineTable, Value};

use twinleaf::{data, tio};
use twinleaf_tools::{tio_opts, tio_parseopts};

use crossterm;
use crossterm::QueueableCommand;
use crossterm::{cursor, style, terminal};

struct ColumnFormatter {
    bounds: HashMap<String, (std::ops::RangeInclusive<f64>, bool)>,
}

impl ColumnFormatter {
    fn new() -> ColumnFormatter {
        ColumnFormatter {
            bounds: HashMap::new(),
        }
    }

    fn from_conf(conf_file_path: &String) -> ColumnFormatter {
        use std::{fs::File, io::Read, str::FromStr};

        let file_contents = match File::open(&conf_file_path) {
            Ok(mut file) => {
                let mut contents = String::new();
                if let Err(e) = file.read_to_string(&mut contents) {
                    panic!("Unable to read {}: {:?}", conf_file_path, e);
                }
                contents
            }
            Err(e) => {
                panic!("Failed to open {}: {:?}", conf_file_path, e);
            }
        };

        let doc = DocumentMut::from_str(&file_contents).unwrap();
        let mut bounds = HashMap::new();

        for (keys, value) in doc.get_values() {
            let column_name = keys.iter().map(|k| k.get()).collect::<Vec<_>>().join(".");
            if let Value::InlineTable(it) = value {
                let (is_temperature, lower_bound) =
                    if let Some(min) = Self::get_value(it, "cold", &column_name) {
                        (true, min)
                    } else if let Some(min) = Self::get_value(it, "min", &column_name) {
                        (false, min)
                    } else {
                        (false, f64::NEG_INFINITY)
                    };
                let (is_temperature, upper_bound) =
                    if let Some(max) = Self::get_value(it, "hot", &column_name) {
                        (true, max)
                    } else if let Some(max) = Self::get_value(it, "max", &column_name) {
                        (is_temperature, max)
                    } else {
                        (is_temperature, f64::INFINITY)
                    };
                if let Some(_) = bounds.insert(
                    column_name,
                    (
                        std::ops::RangeInclusive::new(lower_bound, upper_bound),
                        is_temperature,
                    ),
                ) {
                    // Should not happen (caught upon parsing toml)
                    panic!("Duplicate stream column");
                }
            } else {
                panic!("Unexpected type for {}", column_name);
            }
        }
        ColumnFormatter { bounds }
    }

    fn get_value(it: &InlineTable, key: &str, column_name: &str) -> Option<f64> {
        let value = it.get(key)?;
        match value {
            Value::Float(x) => {
                let fvalue = x.clone().into_value();
                if fvalue.is_nan() {
                    None
                } else {
                    Some(fvalue)
                }
            }
            Value::Integer(x) => Some(x.clone().into_value() as f64),
            _ => {
                panic!("Cannot parse {}::{} as a number", column_name, key);
            }
        }
    }

    fn render_header(
        &self,
        mut stdout: &std::io::Stdout,
        dev: Option<&tio::proto::meta::DeviceMetadata>,
    ) -> std::io::Result<()> {
        stdout.queue(cursor::MoveTo(0, 0))?;
        stdout.queue(terminal::Clear(terminal::ClearType::FromCursorDown))?;
        if let Some(&ref device) = dev {
            stdout.queue(style::SetAttribute(style::Attribute::Bold))?;
            stdout.queue(style::Print(&device.name))?;
            stdout.queue(style::SetAttribute(style::Attribute::Reset))?;
            stdout.queue(style::Print(format!("  Serial: {}", device.serial_number)))?;
        } else {
            stdout.queue(style::Print("Waiting for data..."))?;
        }
        stdout.queue(cursor::MoveToNextLine(1))?;
        Ok(())
    }

    fn render_column(
        &self,
        mut stdout: &std::io::Stdout,
        col: &data::Column,
        stream: &tio::proto::meta::StreamMetadata,
        age: Duration,
        desc_width: usize,
    ) -> std::io::Result<()> {
        use data::ColumnData;

        let fval = col.value.try_as_f64().unwrap_or(f64::NAN);
        let fmtval = match col.value {
            ColumnData::Int(x) => format!("{:10}      ", x),
            ColumnData::UInt(x) => format!("{:10}      ", x),
            ColumnData::Float(x) => {
                if x.is_nan() {
                    format!("{:10}      ", x)
                } else {
                    format!("{:15.4} ", x)
                }
            }
            ColumnData::Unknown => format!("{:>15} ", "unsupported"),
        };

        stdout.queue(cursor::MoveToNextLine(1))?;

        let name = format!("{}.{}", stream.name, col.desc.name);
        let old = age > Duration::from_millis(1200);

        stdout.queue(style::SetAttribute(if old {
            style::Attribute::Dim
        } else {
            style::Attribute::Bold
        }))?;
        stdout.queue(style::Print(&col.desc.description))?;
        stdout.queue(cursor::MoveToColumn((desc_width + 2) as u16))?;

        let color = if fval.is_nan() {
            style::Color::Yellow
        } else if let Some((range, temperature)) = self.bounds.get(&name) {
            if fval < *range.start() {
                if *temperature {
                    style::Color::Blue
                } else {
                    style::Color::Red
                }
            } else if fval > *range.end() {
                style::Color::Red
            } else {
                style::Color::Green
            }
        } else {
            style::Color::Reset
        };
        if color != style::Color::Reset {
            stdout.queue(style::SetForegroundColor(color))?;
        }
        stdout.queue(style::Print(fmtval))?;
        if color != style::Color::Reset {
            stdout.queue(style::SetForegroundColor(style::Color::Reset))?;
        }

        stdout.queue(style::Print(&col.desc.units))?;
        stdout.queue(style::SetAttribute(style::Attribute::Reset))?;

        Ok(())
    }
}

fn terminal_setup(mut stdout: &std::io::Stdout) -> std::io::Result<()> {
    terminal::enable_raw_mode()?;
    //stdout.queue(terminal::EnterAlternateScreen)?;
    stdout.queue(cursor::Hide)?;
    stdout.flush()
}

fn terminal_teardown(mut stdout: &std::io::Stdout) {
    _ = stdout.queue(cursor::MoveToNextLine(1));
    _ = stdout.queue(cursor::Show);
    //_ = stdout.execute(terminal::LeaveAlternateScreen);
    _ = stdout.flush();
    _ = terminal::disable_raw_mode();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut opts = tio_opts();
    opts.optopt(
        "c",
        "colors",
        "Configuration file for colorizing stream column values",
        "conf.toml",
    );
    let (matches, root, route) = tio_parseopts(&opts, &args);

    let formatter = if let Some(path) = matches.opt_str("colors") {
        ColumnFormatter::from_conf(&path)
    } else {
        ColumnFormatter::new()
    };

    let proxy = tio::proxy::Interface::new(&root);
    let mut device = twinleaf::Device::open(&proxy, route)
        .expect("Failed to open device. Check connection and permissions.");

    let mut stdout = std::io::stdout();
    terminal_setup(&stdout).expect("Failed to setup terminal");
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        terminal_teardown(&std::io::stdout());
        original_hook(panic_info);
    }));

    let mut wait_until = Instant::now();
    let mut width: usize = 0;
    let mut last_sample: BTreeMap<u8, (data::Sample, Instant)> = BTreeMap::new();

    'drawing: loop {
        let now = Instant::now();
        let sleep_time = wait_until.saturating_duration_since(now);
        std::thread::sleep(sleep_time);
        wait_until = now + sleep_time + Duration::from_millis(50);

        if crossterm::event::poll(Duration::from_secs(0)).unwrap_or(false) {
            match crossterm::event::read() {
                Ok(crossterm::event::Event::Key(key)) => {
                    use crossterm::event::{KeyCode, KeyModifiers};
                    if key.code == KeyCode::Char('q')
                        || key.code == KeyCode::Esc
                        || (key.code == KeyCode::Char('c') && key.modifiers == KeyModifiers::CONTROL)
                    {
                        break 'drawing;
                    }
                }
                Err(_) => break 'drawing,
                _ => {}
            }
        }

        let mut recompute_width = false;
        loop {
            match device.try_next() {
                Ok(Some(sample)) => {
                    recompute_width = recompute_width || sample.meta_changed;
                    last_sample.insert(sample.stream.as_ref().stream_id, (sample, now));
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    eprintln!("\nError receiving data from device: {:?}. Exiting.", e);
                    break 'drawing;
                }
            }
        }
        if recompute_width {
            for (_, (sample, _)) in &last_sample {
                for col in &sample.columns {
                    width = std::cmp::max(width, col.desc.description.len());
                }
            }
        }

        let dev_meta = if let Some((_, (sample, _))) = last_sample.first_key_value() {
            Some(sample.device.as_ref())
        } else {
            None
        };
        if let Err(_) = formatter.render_header(&stdout, dev_meta) {
            break 'drawing;
        }
        for (_, (sample, time)) in &last_sample {
            let age = now - *time;
            for col in &sample.columns {
                if let Err(_) = formatter.render_column(&stdout, &col, &sample.stream, age, width) {
                    break 'drawing;
                }
            }
        }
        if let Err(_) = stdout.flush() {
            break 'drawing;
        }
    }

    terminal_teardown(&stdout);
}
