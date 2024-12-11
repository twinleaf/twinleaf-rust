use tio::{proto::DeviceRoute, proxy, util};
use twinleaf::{
    data::{ColumnData, Device},
    tio,
};

use getopts::Options;
use std::{env, io::stdout, time::Duration};

use futures::{future::FutureExt, select, StreamExt};
use futures_timer::Delay;

use crossterm::ExecutableCommand;
use crossterm::{
    cursor::*,
    event::{Event, EventStream, KeyCode},
    style::*,
    terminal::*,
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

async fn run_monitor() {
    let mut reader = EventStream::new();

    let args: Vec<String> = env::args().collect();

    let opts = tio_opts();
    let (_matches, root, route) = tio_parseopts(opts, &args);

    let proxy = proxy::Interface::new(&root);
    let device = proxy.device_full(route).unwrap();
    let mut device = Device::new(device);

    let mut stdout = stdout();
    let row: usize = 2;

    'drawing: loop {
        let mut delay = Delay::new(Duration::from_nanos(1)).fuse();
        let mut event = reader.next().fuse();

        let sample = device.next();
        
        //write in title
        let name = format!(
            "Device Name: {}  Serial: {}   Session ID: {}",
            sample.device.name, sample.device.serial_number, sample.device.session_id
        );
        _ = stdout.execute(MoveToRow(0));
        println!("\r{}", name);
        
        select! {
            _= delay => {
                for col in &sample.columns{
                    let width = sample.columns.iter().map(|col| col.desc.name.len().clone()).max().unwrap();
                    let unit_width = sample.columns.iter().map(|col| col.desc.units.len().clone()).max().unwrap();
                    let string = format!(
                        " {:<width$}({:<unit_width$}) {}",
                        col.desc.name, 
                        col.desc.units,
                        match col.value {
                            ColumnData::Int(x) => format!("{}", x),
                            ColumnData::UInt(x) => format!("{}", x),
                            ColumnData::Float(x) => format!("{:.3}", x),
                            ColumnData::Unknown => "?".to_string(),
                        }
                    );
                    //if on first column and first stream move to default position
                    if col.desc.name == sample.columns[0].desc.name.clone() {
                        if sample.stream.stream_id == 1 {
                            _ = stdout.execute(MoveToRow(row.try_into().unwrap())); 
                        } else{
                            _ = stdout.execute(RestorePosition);
                        }
                    }
                    
                    //first two streams overwrite on each position line
                    if sample.stream.stream_id < 3{
                        _ = stdout.execute(Clear(ClearType::CurrentLine));
                        println!("\r{}", string);
                    } else{ 
                        //TODO: Get third stream to dynamically display under stream 2
                        //third stream dynamically is moved to row 31
                        _ = stdout.execute(MoveToRow(31));
                        _ = stdout.execute(Clear(ClearType::CurrentLine));
                        println!("\r{}", string);
                        _ = stdout.execute(RestorePosition);
                    }   
                    
                    //if the value is the last in stream 1, move down the length of rows
                    if col.desc.name == sample.columns[&sample.columns.len() -1].desc.name.clone() && sample.stream.stream_id == 1 {
                        _ = stdout.execute(MoveToRow((sample.columns.len().clone() + row + 1).try_into().unwrap()));
                    }
                        
                    _ = stdout.execute(SavePosition);    
                }
                
            },
            some_event = event => {
                match some_event {
                    Some(Ok(event)) => {
                        if event == Event::Key(KeyCode::Char('q').into()) {
                            break 'drawing;
                        } else if event == Event::Key(KeyCode::Esc.into()) {
                            break 'drawing;
                        //TODO: Fix Ctrl c bug
                        } else if event == Event::Key(KeyCode::Char('c').into()){
                            break 'drawing;
                        }
                    }
                    Some(Err(e)) => println!("Error{}\r", e),
                    None => continue,
                }
            }
        }
    }
}

fn main() -> std::io::Result<()> {
    let mut stdout = stdout();

    //setup terminal
    enable_raw_mode()?;
    stdout.execute(EnterAlternateScreen)?;
    stdout.execute(SetBackgroundColor(Color::Black))?;
    stdout.execute(SetForegroundColor(Color::White))?;
    stdout.execute(Clear(ClearType::All))?;
    stdout.execute(SavePosition)?;
    stdout.execute(Hide)?;

    async_std::task::block_on(run_monitor());

    //clean up terminal on end
    stdout.execute(LeaveAlternateScreen)?;
    stdout.execute(Show)?;
    disable_raw_mode()?;

    Ok(())
}
