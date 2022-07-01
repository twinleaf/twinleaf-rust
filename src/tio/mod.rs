mod port;
pub mod proto;

pub use port::{Port, RecvError, SendError};
pub use proto::Packet;

use std::io;
use std::thread;
use std::time::{Duration, Instant};

pub struct TioProxyPort {
    new_queue: crossbeam::channel::Sender<(
        crossbeam::channel::Sender<Packet>,
        crossbeam::channel::Receiver<Packet>,
    )>,
}

impl TioProxyPort {
    pub fn new(url: &str) -> TioProxyPort {
        let (sender, receiver) = crossbeam::channel::bounded::<(
            crossbeam::channel::Sender<Packet>,
            crossbeam::channel::Receiver<Packet>,
        )>(5);
        let url_string = url.to_string();
        thread::spawn(move || {
            TioProxyPort::internal_thread(url_string, receiver);
        });
        TioProxyPort { new_queue: sender }
    }

    pub fn new_proxy(
        &self,
    ) -> (
        crossbeam::channel::Sender<Packet>,
        crossbeam::channel::Receiver<Packet>,
    ) {
        let (proxy_sender, proxy_sender_receiver) = crossbeam::channel::bounded::<Packet>(32);
        let (proxy_receiver_sender, proxy_receiver) = crossbeam::channel::bounded::<Packet>(32);
        self.new_queue
            .send((proxy_receiver_sender, proxy_sender_receiver));
        (proxy_sender, proxy_receiver)
    }

    fn internal_thread(
        url: String,
        new_queue: crossbeam::channel::Receiver<(
            crossbeam::channel::Sender<Packet>,
            crossbeam::channel::Receiver<Packet>,
        )>,
    ) {
        use crossbeam::channel::TryRecvError;
        use std::collections::HashMap;

        let mut rpc_id: u16 = 0;
        struct RpcRemap {
            orig_id: u16,
            client: u64,
        };
        let mut rpc_map: HashMap<u16, RpcRemap> = HashMap::new();
        let mut client_id: u64 = 0;
        let mut clients: HashMap<
            u64,
            (
                crossbeam::channel::Sender<Packet>,
                crossbeam::channel::Receiver<Packet>,
            ),
        > = HashMap::new();
        let (port_rx_send, port_rx) = Port::rx_channel();
        let port = Port::from_url(&url, Port::rx_to_channel(port_rx_send)).unwrap();
        if let Some(rates) = port.rate_info() {
            println!(
                "RATES: {} {} {}",
                rates.default_bps, rates.min_bps, rates.max_bps
            );
        }
        let mut drop_client: Option<u64> = None;

        'mainloop: loop {
            if let Some(client_id) = drop_client {
                // TODO: check that if let leaves drop_client to None
                drop(clients.remove(&client_id));
            }
            let mut sel = crossbeam::channel::Select::new();
            let mut ids: Vec<u64> = Vec::new();
            for (id, client) in clients.iter() {
                sel.recv(&client.1);
                ids.push(*id);
            }
            sel.recv(&port_rx);
            sel.recv(&new_queue);

            let index = sel.ready();
            //            println!("PP: READY {}", index);

            // TODO: for all these should loop
            if index < ids.len() {
                // data from a client to send to the port
                loop {
                    match clients.get(&ids[index]).unwrap().1.try_recv() {
                        Ok(pkt) => {
                            let send_pkt = if let proto::Payload::RpcRequest(req) = pkt.payload {
                                if rpc_map.contains_key(&rpc_id) {
                                    // TODO: handle gracefully. could also key by routing, but unnecessarily complicated
                                    panic!("REMAP FAIL");
                                }
                                rpc_map.insert(
                                    rpc_id,
                                    RpcRemap {
                                        orig_id: req.id,
                                        client: ids[index],
                                    },
                                );
                                let new_req = proto::RpcRequestPayload {
                                    id: rpc_id,
                                    method: req.method,
                                    arg: req.arg,
                                };
                                println!("REMAPPED: {} -> {}", req.id, rpc_id);
                                rpc_id += 1;
                                proto::Packet {
                                    payload: proto::Payload::RpcRequest(new_req),
                                    routing: pkt.routing,
                                    ttl: pkt.ttl,
                                }
                            } else {
                                pkt
                            };
                            // TODO: check for failure
                            port.send(send_pkt);
                        }
                        Err(TryRecvError::Empty) => {
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            drop_client = Some(ids[index]);
                            break;
                        }
                    }
                }
            } else if index == ids.len() {
                // data from the device
                loop {
                    //println!("PP: devdata");
                    match port_rx.try_recv() {
                        Ok(Ok(pkt)) => {
                            match pkt.payload {
                                proto::Payload::RpcReply(rep) => {
                                    let remap = if let Some(r) = rpc_map.remove(&rep.id) {
                                        r
                                    } else {
                                        // TODO: say something
                                        continue;
                                    };
                                    let client = if let Some(c) = clients.get(&remap.client) {
                                        c
                                    } else {
                                        // TODO: say something
                                        continue;
                                    };
                                    client.0.send(proto::Packet {
                                        payload: proto::Payload::RpcReply(proto::RpcReplyPayload {
                                            id: remap.orig_id,
                                            reply: rep.reply,
                                        }),
                                        routing: pkt.routing,
                                        ttl: pkt.ttl,
                                    });
                                }
                                // TODO: find a good way to avoid duplication
                                proto::Payload::RpcError(err) => {
                                    let remap = if let Some(r) = rpc_map.remove(&err.id) {
                                        r
                                    } else {
                                        // TODO: say something
                                        continue;
                                    };
                                    let client = if let Some(c) = clients.get(&remap.client) {
                                        c
                                    } else {
                                        // TODO: say something
                                        continue;
                                    };
                                    client.0.send(proto::Packet {
                                        payload: proto::Payload::RpcError(proto::RpcErrorPayload {
                                            id: remap.orig_id,
                                            error: err.error,
                                            extra: err.extra,
                                        }),
                                        routing: pkt.routing,
                                        ttl: pkt.ttl,
                                    });
                                }
                                _ => {
                                    for (_, client) in clients.iter() {
                                        // TODO: check for failure
                                        client.0.send(pkt.clone());
                                    }
                                }
                            }
                        }
                        Err(TryRecvError::Empty) => {
                            break;
                        }
                        Ok(Err(_)) => {
                            // TODO: not all error should be fatal
                            println!("PP: porterror");
                            break 'mainloop;
                        }
                        Err(_) => {
                            // TODO: not all error should be fatal
                            println!("PP: porterror");
                            break 'mainloop;
                        }
                    }
                }
            } else {
                // new proxy client
                loop {
                    println!("PP: newclient");
                    match new_queue.try_recv() {
                        Ok((sender, receiver)) => {
                            println!("PP: new client {}", client_id);
                            clients.insert(client_id, (sender, receiver));
                            client_id += 1;
                        }
                        Err(TryRecvError::Empty) => {
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            // channel closed.
                            println!("PP: channelclosed");
                            break 'mainloop;
                        }
                    }
                }
            }
        }
    }
}
