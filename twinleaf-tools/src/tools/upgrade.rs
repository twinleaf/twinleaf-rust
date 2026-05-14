use crate::{ProxyHelp, TioOpts, UpgradeCli};
use twinleaf::device::DeviceRoute;
use twinleaf::tio::{self, proxy, util};

pub fn run_upgrade(upgrade_cli: UpgradeCli) -> eyre::Result<()> {
    firmware_upgrade(&upgrade_cli.tio, upgrade_cli.firmware_path, upgrade_cli.yes)
}

pub fn firmware_upgrade(
    tio: &TioOpts,
    firmware_path: std::path::PathBuf,
    skip_confirm: bool,
) -> eyre::Result<()> {
    use color_eyre::Help;
    use eyre::WrapErr;
    use indicatif::{ProgressBar, ProgressStyle};

    let firmware_data = std::fs::read(&firmware_path)
        .wrap_err_with(|| format!("could not read firmware file {:?}", firmware_path))?;

    log::info!("loaded {} bytes firmware", firmware_data.len());

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.route.clone();
    let device = proxy
        .device_rpc(route)
        .wrap_err_with(|| format!("could not open device at {}", tio.root))
        .with_proxy_help()?;

    let dev_name: String = device
        .rpc("dev.name", ())
        .wrap_err("failed to query device name")?;

    if !skip_confirm {
        let confirmed = dialoguer::Confirm::new()
            .with_prompt(format!("Upgrade firmware on '{}'?", dev_name))
            .default(false)
            .interact()
            .wrap_err("failed to read confirmation")?;
        if !confirmed {
            log::info!("aborted");
            return Ok(());
        }
    }

    match device.action("dev.stop") {
        Ok(()) => {}
        Err(proxy::RpcError::ExecError(ref e))
            if matches!(
                e.error,
                tio::proto::RpcErrorCode::NotFound | tio::proto::RpcErrorCode::WrongDeviceState
            ) => {}
        Err(e) => {
            return Err(
                eyre::Report::new(e).wrap_err("failed to stop device before firmware upgrade")
            );
        }
    }

    let total_chunks = firmware_data.len().div_ceil(288);
    let pb = crate::multi_progress().add(ProgressBar::new(total_chunks as u64));
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {percent}%")
            .unwrap()
            .progress_chars("#>-"),
    );

    let power_cycle_hint =
        "power cycle the device before retrying and check if dev.stop exists as an rpc";

    let mut next_send_chunk: u16 = 0;
    let mut next_ack_chunk: u16 = 0;
    let mut more_to_send = true;
    const MAX_CHUNKS_IN_FLIGHT: u16 = 2;

    while more_to_send || (next_ack_chunk != next_send_chunk) {
        if more_to_send && ((next_send_chunk - next_ack_chunk) < MAX_CHUNKS_IN_FLIGHT) {
            let offset = usize::from(next_send_chunk) * 288;
            let chunk_end = if (offset + 288) > firmware_data.len() {
                firmware_data.len()
            } else {
                offset + 288
            };

            device
                .send(util::PacketBuilder::make_rpc_request(
                    "dev.firmware.upload",
                    &firmware_data[offset..chunk_end],
                    next_send_chunk,
                    DeviceRoute::root(),
                ))
                .wrap_err_with(|| {
                    format!(
                        "failed to send firmware chunk {}/{}",
                        next_send_chunk + 1,
                        total_chunks
                    )
                })
                .suggestion(power_cycle_hint)?;
            next_send_chunk += 1;
            more_to_send = chunk_end < firmware_data.len();
        }

        let pkt = if more_to_send && ((next_send_chunk - next_ack_chunk) < MAX_CHUNKS_IN_FLIGHT) {
            match device.try_recv() {
                Ok(pkt) => pkt,
                Err(proxy::RecvError::WouldBlock) => continue,
                Err(e) => {
                    return Err(eyre::Report::new(e)
                        .wrap_err("failed to receive firmware upload ack")
                        .suggestion(power_cycle_hint));
                }
            }
        } else {
            device
                .recv()
                .wrap_err("failed to receive firmware upload ack")
                .suggestion(power_cycle_hint)?
        };

        match pkt.payload {
            tio::proto::Payload::RpcReply(rep) => {
                if rep.id != next_ack_chunk {
                    return Err(eyre::eyre!(
                        "firmware chunk ack out of order (expected {}, got {})",
                        next_ack_chunk,
                        rep.id
                    )
                    .suggestion(power_cycle_hint));
                }
                next_ack_chunk += 1;
                pb.set_position(next_ack_chunk as u64);
            }
            tio::proto::Payload::RpcError(err) => {
                return Err(eyre::eyre!(
                    "device rejected firmware chunk {}/{}: {}",
                    next_ack_chunk + 1,
                    total_chunks,
                    err.error
                )
                .suggestion(power_cycle_hint));
            }
            _ => continue,
        }
    }

    pb.finish_and_clear();

    device
        .action("dev.firmware.upgrade")
        .wrap_err("device rejected firmware commit")
        .suggestion(power_cycle_hint)?;

    // Wait 5 seconds before returning to ensure the device is not
    // power-cycled while the firmware upgrade is being committed to flash.
    let spinner = crate::multi_progress().add(ProgressBar::new_spinner());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    spinner.set_message("Finalizing upgrade...");
    for _ in 0..50 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        spinner.tick();
    }
    spinner.finish_with_message("Firmware upgrade complete.");

    Ok(())
}
