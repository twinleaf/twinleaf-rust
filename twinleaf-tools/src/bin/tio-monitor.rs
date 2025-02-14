use tio::{proto::DeviceRoute, proxy, util};
use twinleaf::{
    data::{ColumnData, Device},
    tio,
};

use getopts::Options;
use std::collections::HashMap;
use std::{env, io::stdout, time::Duration, io::Read, fs};
use serde::{ser::StdError, Deserialize};

use futures::{future::FutureExt, select, StreamExt};
use futures_timer::Delay;

use crossterm::ExecutableCommand;
use crossterm::{
    cursor::*,
    event::{Event, EventStream, KeyCode, KeyModifiers},
    terminal::*,
    style::*
};

fn tio_opts() -> Options {
    let mut opts = Options::new();
    opts.optopt(
        "r",
        "",
        &format!("sensor root (default {})", util::default_proxy_url()),
        "address",
    );
    opts.optopt(
        "s",
        "",
        "sensor path in the sensor tree (default /)",
        "path",
    );
    opts
}

fn tio_parseopts(opts: Options, args: &[String]) -> (getopts::Matches, String, DeviceRoute) {
    let matches = match opts.parse(args) {
        std::result::Result::Ok(m) => m,
        Err(f) => {
            panic!("{}", f.to_string())
        }
    };
    let root = if let Some(url) = matches.opt_str("r") {
        url
    } else {
        "tcp://localhost".to_string()
    };
    let route = if let Some(path) = matches.opt_str("s") {
        DeviceRoute::from_str(&path).unwrap()
    } else {
        DeviceRoute::root()
    };
    (matches, root, route)
}

#[derive(Deserialize, Debug)]
struct Threshold {
    min: f32,
    max: f32,
}

impl Threshold {
    fn within_range(&self, value: f32) -> bool {
        value >= self.min && value <= self.max
    }

    fn no_value(&self) -> bool {
        self.min == self.max
    }
}

fn read_file(file_path: String) -> std::result::Result<String, Box<dyn StdError >>{
    //read file contents to string
    let mut file = fs::File::open(&file_path)?;
    let mut file_content = String::new();
    _ = file.read_to_string(&mut file_content);

    Ok(file_content)
}

fn test_range(column: String, value: f32, file_path: Option<String>) -> u32 {
    let path = file_path.unwrap_or(String::from("default.yaml"));
    let clean_name = str::replace(&column, ".", "_");

    match read_file(path) {
        Ok(result) => {
            let read_yaml: HashMap<String, Threshold> = serde_yaml::from_str(&result).expect("FAILED TO READ YAML");

            if let Some(range) = read_yaml.get(&clean_name) {
                if range.no_value(){
                    return 1; 

                } else if range.within_range(value) {
                    return 2;
                
                } else{
                    return 3;
                }
        }
        }
        Err(_err) => {
            return 1;
        }
    }
    1
}

fn set_text_color(match_color: u32){
    let mut stdout = stdout();

    match match_color {
        1 => {_ = stdout.execute(SetForegroundColor(Color::Reset));}
        2 => {_ = stdout.execute(SetForegroundColor(Color::Green));}
        3 => {_ = stdout.execute(SetForegroundColor(Color::Red));}
        _ => {}
    };
}

async fn run_monitor() {
    let mut reader = EventStream::new();
    let mut stdout = stdout();

    let args: Vec<String> = env::args().collect();
    let default_path = "default.yaml".to_string();
    let path = args.get(2).unwrap_or(&default_path);

    let opts = tio_opts();
    let (_matches, root, route) = tio_parseopts(opts, &args);

    let proxy = proxy::Interface::new(&root);
    let device = proxy.device_full(route).unwrap();

    let mut device = Device::new(device);
    let meta = device.get_metadata();
    let mut positions: HashMap<u8, usize> = HashMap::new();
    for stream in meta.streams.values() {
        positions.insert(stream.stream.stream_id, stream.stream.n_columns);
    }

    'drawing: loop {
        let mut delay = Delay::new(Duration::from_nanos(1)).fuse();
        let mut event = reader.next().fuse();
        let sample = device.next();

        //write in device info
        let name = format!(
            "{}  Serial: {}",
            sample.device.name, sample.device.serial_number
        );
        _ = stdout.execute(SetForegroundColor(Color::White));
        _ = stdout.execute(MoveToRow(0));
        println!("\r{}", name);

        select! {
            _= delay => {
                for col in &sample.columns{
                    let color_pair = test_range(col.desc.name.clone(), 
                        match col.value {
                        ColumnData::Int(x) => x as f32,
                        ColumnData::UInt(x) => x as f32,
                        ColumnData::Float(x) => x as f32,
                        ColumnData::Unknown => 0.0,
                        }, Some(path.to_string()));

                    set_text_color(color_pair);

                    let width = sample.columns.iter().map(|col| col.desc.description.len().clone()).max().unwrap();
                    let string = format!(
                        " {:<width$} {} {}",
                        col.desc.description,
                        match col.value {
                            ColumnData::Int(x) => format!("{}", x),
                            ColumnData::UInt(x) => format!("{:.3}", x),
                            ColumnData::Float(x) => format!("{:.4}", x),
                            ColumnData::Unknown => "?".to_string(),
                        },
                        col.desc.units
                    );

                    if col.desc.name == sample.columns[0].desc.name.clone(){
                        match positions.get(&sample.stream.stream_id) {
                            Some(_row) => {
                                let mut row_position = 0;
                                for pos in positions.keys() {
                                    if &sample.stream.stream_id > pos {
                                        _ = stdout.execute(MoveToNextLine(1));
                                        row_position += positions[pos];
                                    }
                                }
                                _ = stdout.execute(MoveDown((row_position + 1).try_into().unwrap()));
                            },
                            None => println!("\rError, stream not found")
                        };
                    }

                    _ = stdout.execute(Clear(ClearType::CurrentLine));
                    println!("\r{}", string);
                }
            },
            some_event = event => {
                match some_event {
                    Some(Ok(event)) => {
                        if let Event::Resize(_x, _y) = event{
                            _ = stdout.execute(Clear(ClearType::All));
                        } else if event == Event::Key(KeyCode::Char('q').into()) {
                            break 'drawing;
                        } else{
                            if let Event::Key(key_event) = event{
                                if key_event.code == KeyCode::Esc|| (key_event.code == KeyCode::Char('c') && key_event.modifiers == KeyModifiers::CONTROL){
                                    break 'drawing;
                                }
                            }
                        }
                    },
                    Some(Err(e)) => println!("Error{}\r", e),
                    None => continue,
                }
            }
        }
    }
}

fn main() -> std::io::Result<()> {
    let mut args: Vec<String> = std::env::args().collect();
    if args.len() < 2{
        args.push("run".to_string())
    } 
    match args[1].as_str() {
        "run" => {
            let mut stdout = stdout();

            enable_raw_mode()?;
            stdout.execute(EnterAlternateScreen)?;
            stdout.execute(Clear(ClearType::All))?;
            stdout.execute(SavePosition)?;
            stdout.execute(Hide)?;

            async_std::task::block_on(run_monitor());
            stdout.execute(LeaveAlternateScreen)?;
            stdout.execute(Show)?;
            disable_raw_mode()?;
        }
        _ => {
            println!("Usage:");
            println!(" tio-monitor help");
            println!(" tio-monitor run [yaml_file_path]"); 
        }
    }

    Ok(())
}
