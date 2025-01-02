use super::tio;
use proto::DeviceRoute;
use tio::{proto, proxy, util};

use std::collections::HashMap;
use tio::proto::meta::MetadataType;

static TL_STREAMRPC_MAX_META: usize = 16;

#[derive(Debug, Clone)]
struct StreamRpcMetaReq {
    mtype: MetadataType,
    stream_id: u8,
    index: u8,
}

impl StreamRpcMetaReq {
    pub fn device() -> StreamRpcMetaReq {
        StreamRpcMetaReq {
            mtype: MetadataType::Device,
            stream_id: 0,
            index: 0,
        }
    }
    pub fn stream(id: u8) -> StreamRpcMetaReq {
        StreamRpcMetaReq {
            mtype: MetadataType::Stream,
            stream_id: id,
            index: 0,
        }
    }
    pub fn segment(index: u8, stream_id: u8) -> StreamRpcMetaReq {
        StreamRpcMetaReq {
            mtype: MetadataType::Segment,
            stream_id: stream_id,
            index: index,
        }
    }
    pub fn column(index: u8, stream_id: u8) -> StreamRpcMetaReq {
        StreamRpcMetaReq {
            mtype: MetadataType::Column,
            stream_id: stream_id,
            index: index,
        }
    }
}

fn make_metareq(reqs: Vec<StreamRpcMetaReq>) -> Vec<u8> {
    let mut ret = vec![];
    if reqs.len() > TL_STREAMRPC_MAX_META {
        panic!("too many requests")
    }
    for req in reqs {
        ret.push(req.mtype.clone().into());
        ret.push(req.stream_id);
        ret.push(req.index);
    }
    ret
}

// Convert a number of metadata requests to RPC request packets.
fn metareqs_to_rpcs(metareqs: &Vec<StreamRpcMetaReq>) -> Vec<tio::Packet> {
    let mut ret = vec![];
    let mut reqs = &metareqs[..];
    loop {
        if reqs.len() == 0 {
            break;
        }
        let n_reqs = if reqs.len() > TL_STREAMRPC_MAX_META {
            TL_STREAMRPC_MAX_META
        } else if reqs.len() == 1 {
            // If we don't know anything about the device, send zero
            // arguments to get an automatic reply fitting as many things
            // as possible at the beginning, to bootstrap the process
            // more efficiently
            if let MetadataType::Device = reqs[0].mtype {
                reqs = &reqs[1..];
                0
            } else {
                1
            }
        } else {
            reqs.len()
        };
        ret.push(util::PacketBuilder::make_rpc_request(
            "dev.metadata",
            &make_metareq((&reqs[0..n_reqs]).to_vec()),
            7855,
            DeviceRoute::root(),
        ));
        reqs = &reqs[n_reqs..];
    }
    ret
}

fn parse_metarep(rep: Vec<u8>) -> Vec<tio::proto::meta::MetadataContent> {
    use tio::proto::meta;
    let mut ret = vec![];
    let mut offset: usize = 0;
    while (offset + 2) <= rep.len() {
        let mtype = MetadataType::from(rep[offset]);
        let varlen = usize::from(rep[offset + 1]);
        let metadata = &rep[offset + 2..offset + 2 + varlen];
        offset += varlen + 2;
        match mtype {
            MetadataType::Device => {
                let (dm, _, _) = meta::DeviceMetadata::deserialize(metadata, &[]).unwrap();
                ret.push(meta::MetadataContent::Device(dm));
            }
            MetadataType::Stream => {
                let (sm, _, _) = meta::StreamMetadata::deserialize(metadata, &[]).unwrap();
                ret.push(meta::MetadataContent::Stream(sm));
            }
            MetadataType::Segment => {
                let (sm, _, _) = meta::SegmentMetadata::deserialize(metadata, &[]).unwrap();
                ret.push(meta::MetadataContent::Segment(sm));
            }
            MetadataType::Column => {
                let (cm, _, _) = meta::ColumnMetadata::deserialize(metadata, &[]).unwrap();
                ret.push(meta::MetadataContent::Column(cm));
            }
            _ => {}
        }
    }
    ret
}

use std::sync::Arc;
use tio::proto::meta::{
    ColumnMetadata, DeviceMetadata, MetadataContent, SegmentMetadata, StreamMetadata,
};

#[derive(Debug, Clone)]
pub enum ColumnData {
    Int(i64),
    UInt(u64),
    Float(f64),
    Unknown,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub value: ColumnData,
    pub desc: Arc<ColumnMetadata>,
}

impl Column {
    fn from_le_bytes(data: &[u8], md: Arc<ColumnMetadata>) -> Column {
        use tio::proto::DataType;
        Column {
            value: match md.data_type {
                DataType::Int8 => ColumnData::Int(i8::from_le_bytes([data[0]]).into()),
                DataType::UInt8 => ColumnData::UInt(data[0].into()),
                DataType::Int16 => ColumnData::Int(i16::from_le_bytes([data[0], data[1]]).into()),
                DataType::UInt16 => ColumnData::UInt(u16::from_le_bytes([data[0], data[1]]).into()),
                DataType::Int24 => {
                    ColumnData::Int(i32::from_le_bytes([data[0], data[1], data[2], 0]).into())
                }
                DataType::UInt24 => {
                    ColumnData::UInt(u32::from_le_bytes([data[0], data[1], data[2], 0]).into())
                }
                DataType::Int32 => {
                    ColumnData::Int(i32::from_le_bytes([data[0], data[1], data[2], data[3]]).into())
                }
                DataType::UInt32 => ColumnData::UInt(
                    u32::from_le_bytes([data[0], data[1], data[2], data[3]]).into(),
                ),
                DataType::Int64 => ColumnData::Int(
                    i64::from_le_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ])
                    .into(),
                ),
                DataType::UInt64 => ColumnData::UInt(
                    u64::from_le_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ])
                    .into(),
                ),
                DataType::Float32 => ColumnData::Float(
                    f32::from_le_bytes([data[0], data[1], data[2], data[3]]).into(),
                ),
                DataType::Float64 => ColumnData::Float(
                    f64::from_le_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ])
                    .into(),
                ),
                DataType::Unknown(_) => ColumnData::Unknown,
            },
            desc: md,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Sample {
    pub n: u32,
    pub columns: Vec<Column>,
    pub segment: Arc<SegmentMetadata>,
    pub stream: Arc<StreamMetadata>,
    pub device: Arc<DeviceMetadata>,
    pub segment_changed: bool,
    pub meta_changed: bool,
}

impl Sample {
    pub fn timestamp_begin(&self) -> f64 {
        let period =
            1.0 / f64::from(self.segment.sampling_rate) * f64::from(self.segment.decimation);
        f64::from(self.segment.start_time) + period * f64::from(self.n)
    }
    pub fn timestamp_end(&self) -> f64 {
        let period =
            1.0 / f64::from(self.segment.sampling_rate) * f64::from(self.segment.decimation);
        f64::from(self.segment.start_time) + period * f64::from(self.n + 1)
    }
}

#[derive(Debug)]
pub struct DeviceStreamMetadata {
    pub stream: Arc<StreamMetadata>,
    pub segment: Arc<SegmentMetadata>,
    pub columns: Vec<Arc<ColumnMetadata>>,
}

#[derive(Debug)]
struct DeviceColumn {
    metadata: Arc<ColumnMetadata>,
    offset: usize,
}

#[derive(Debug)]
struct DeviceStream {
    stream: Option<Arc<StreamMetadata>>,
    segment: Option<Arc<SegmentMetadata>>,
    columns: Vec<DeviceColumn>,

    id: u8,
    last_seg: u8,
    last_sample_number: u32,
    segment_changed: bool,
    meta_changed: bool,
}

impl DeviceStream {
    fn requests(&self) -> Vec<StreamRpcMetaReq> {
        let mut ret = vec![];
        match self.stream.as_ref() {
            Some(stream) => {
                let n_cols = stream.n_columns;
                for i in self.columns.len()..n_cols {
                    ret.push(StreamRpcMetaReq::column(i as u8, self.id))
                }
            }
            None => {
                ret.push(StreamRpcMetaReq::stream(self.id));
            }
        }
        if match self.segment.as_ref() {
            Some(seg) => seg.segment_id != self.last_seg,
            None => true,
        } {
            ret.push(StreamRpcMetaReq::segment(self.last_seg, self.id));
        }
        ret
    }

    fn parse_sample(&self, data: &[u8]) -> Vec<Column> {
        let mut ret = vec![];
        for col in &self.columns {
            ret.push(Column::from_le_bytes(
                &data[col.offset..],
                col.metadata.clone(),
            ));
        }
        ret
    }

    fn process_samples(
        &mut self,
        data: &tio::proto::StreamDataPayload,
        dev: Arc<DeviceMetadata>,
    ) -> Vec<Sample> {
        // Update this first, so even if we can't parse the sample, the right
        // request will be sent out next
        self.last_seg = data.segment_id;

        if self.stream.is_none() || self.segment.is_none() {
            return vec![];
        }

        let stream = self.stream.as_ref().unwrap().clone();
        if stream.n_columns != self.columns.len() {
            return vec![];
        }

        let segment = self.segment.as_ref().unwrap().clone();
        if segment.segment_id != data.segment_id {
            // Here, generate proactively a new segment if this looks like
            // a sample number rollover
            let next_sample = self.last_sample_number + 1;
            let next_segment = if usize::from(segment.segment_id) == stream.n_segments {
                0
            } else {
                segment.segment_id + 1
            };
            let rate = segment.sampling_rate / segment.decimation;
            if (data.first_sample_n == 0)
                && ((next_sample % rate) == 0)
                && (data.segment_id == next_segment)
            {
                let mut new_seg = (*segment).clone();
                new_seg.start_time += next_sample / rate;
                self.segment = Some(Arc::new(new_seg));
            } else {
                return vec![];
            }
        }

        let segment = self.segment.as_ref().unwrap().clone();

        let mut ret = vec![];
        let mut sample_n = data.first_sample_n;
        let mut offset = 0;

        // TODO: validate size
        while offset < data.data.len() {
            let raw_sample = &data.data[offset..(offset + stream.sample_size)];
            ret.push(Sample {
                n: sample_n,
                columns: self.parse_sample(raw_sample),
                segment: segment.clone(),
                stream: stream.clone(),
                device: dev.clone(),
                segment_changed: self.segment_changed,
                meta_changed: self.meta_changed,
            });
            self.segment_changed = false;
            self.meta_changed = false;
            offset += stream.sample_size;
            sample_n += 1;
        }

        ret
    }

    fn get_metadata(&self) -> Result<DeviceStreamMetadata, Vec<StreamRpcMetaReq>> {
        let reqs = self.requests();
        if reqs.is_empty() {
            Ok(DeviceStreamMetadata {
                stream: self.stream.as_ref().unwrap().clone(),
                segment: self.segment.as_ref().unwrap().clone(),
                columns: self.columns.iter().map(|x| x.metadata.clone()).collect(),
            })
        } else {
            Err(reqs)
        }
    }
}

#[derive(Debug)]
pub struct DeviceFullMetadata {
    pub device: Arc<DeviceMetadata>,
    pub streams: HashMap<u8, DeviceStreamMetadata>,
}

pub struct DeviceDataParser {
    device: Option<Arc<DeviceMetadata>>,
    streams: HashMap<u8, DeviceStream>,
    ignore_session: bool,
}

impl DeviceDataParser {
    pub fn new(ignore_session: bool) -> DeviceDataParser {
        DeviceDataParser {
            device: None,
            streams: HashMap::new(),
            ignore_session: ignore_session,
        }
    }

    fn get_stream<'a>(&'a mut self, stream_id: u8) -> &'a mut DeviceStream {
        if !self.streams.contains_key(&stream_id) {
            self.streams.insert(
                stream_id,
                DeviceStream {
                    stream: None,
                    segment: None,
                    columns: vec![],
                    id: stream_id,
                    last_seg: 0,
                    last_sample_number: 0,
                    segment_changed: true,
                    meta_changed: true,
                },
            );
        }
        self.streams.get_mut(&stream_id).unwrap()
    }

    fn process_metadata(&mut self, metadata: &MetadataContent, from_update: bool) {
        match metadata {
            MetadataContent::Device(dm) => {
                if let Some(cur) = &self.device {
                    if (cur.serial_number != dm.serial_number)
                        || (cur.session_id != dm.session_id)
                        || (cur.firmware_hash != dm.firmware_hash)
                    {
                        self.device.replace(Arc::new(dm.clone()));
                        self.streams.clear();
                    } else if cur.n_streams != dm.n_streams {
                        self.device.replace(Arc::new(dm.clone()));
                    }
                } else {
                    self.device.replace(Arc::new(dm.clone()));
                }
            }
            MetadataContent::Stream(sm) => {
                if let Some(dev) = &self.device {
                    if usize::from(sm.stream_id) > dev.n_streams {
                        // Should never happen, but force a reload.
                        self.device.take();
                        self.streams.clear();
                    }
                }
                let dstream = self.get_stream(sm.stream_id);
                if let Some(stream) = &dstream.stream {
                    if stream.as_ref() != sm {
                        // This should never happen: stream metadata is constant.
                        // To be safe, reload the whole thing:
                        self.device.take();
                        self.streams.clear();
                    }
                } else {
                    dstream.stream.replace(Arc::new(sm.clone()));
                    dstream.meta_changed = true;
                }
            }
            MetadataContent::Segment(sm) => {
                if let Some(dev) = &self.device {
                    if usize::from(sm.stream_id) > dev.n_streams {
                        // Should never happen, but force a reload.
                        self.device.take();
                        self.streams.clear();
                    }
                }
                let dstream = self.get_stream(sm.stream_id);
                if let Some(segment) = &dstream.segment {
                    if segment.as_ref() != sm {
                        dstream.segment.replace(Arc::new(sm.clone()));
                        dstream.segment_changed = true;
                        if from_update {
                            dstream.last_seg = sm.segment_id;
                        }
                    }
                } else {
                    let dstream = self.get_stream(sm.stream_id);
                    dstream.segment.replace(Arc::new(sm.clone()));
                    dstream.segment_changed = true;
                    if from_update {
                        dstream.last_seg = sm.segment_id;
                    }
                }
            }
            MetadataContent::Column(cm) => {
                if let Some(dev) = &self.device {
                    if usize::from(cm.stream_id) > dev.n_streams {
                        // Should never happen, but force a reload.
                        self.device.take();
                        self.streams.clear();
                    }
                }
                let dstream = self.get_stream(cm.stream_id);
                if usize::from(cm.index) < dstream.columns.len() {
                    if dstream.columns[cm.index].metadata.as_ref() != cm {
                        // This should never happen: columns are constant.
                        // To be safe, reload everything.
                        self.device.take();
                        self.streams.clear();
                    }
                } else if usize::from(cm.index) == dstream.columns.len() {
                    // Next column to append
                    // TODO: make sure it agrees with the rest of the metadata
                    let offset = if cm.index == 0 {
                        0
                    } else {
                        let prev_col = &dstream.columns[cm.index - 1];
                        prev_col.offset + prev_col.metadata.data_type.size()
                    };
                    dstream.columns.push(DeviceColumn {
                        metadata: Arc::new(cm.clone()),
                        offset: offset,
                    })
                }
            }
            _ => {}
        }
    }

    pub fn process_packet(&mut self, pkt: &tio::Packet) -> Vec<Sample> {
        match &pkt.payload {
            tio::proto::Payload::RpcReply(rep) => {
                for metadata in parse_metarep(rep.reply.clone()) {
                    self.process_metadata(&metadata, false)
                }
            }
            tio::proto::Payload::Metadata(mp) => self.process_metadata(&mp.content, true),
            tio::proto::Payload::Heartbeat(hb) => {
                if let tio::proto::HeartbeatPayload::Session(session_id) = hb {
                    if let Some(dev) = &self.device {
                        if (dev.session_id != *session_id) && !self.ignore_session {
                            self.device.take();
                            self.streams.clear();
                        }
                    }
                }
            }
            tio::proto::Payload::StreamData(data) => {
                // Attempt to parse samples
                if let Some(dev) = &self.device {
                    if usize::from(data.stream_id) > dev.n_streams {
                        // Should never happen, but force a reload.
                        self.device.take();
                        self.streams.clear();
                    } else {
                        let ndev = dev.clone();
                        let dstream = self.get_stream(data.stream_id);
                        return dstream.process_samples(data, ndev);
                    }
                }
            }
            _ => {
                // TODO: something about rpc errors? at least hold off to not
                // issue too many requests.
            }
        }
        return vec![];
    }

    pub fn requests(&self) -> Vec<tio::Packet> {
        // Determine all the metadata requests to issue.
        let mut reqs = vec![];
        match self.device.as_ref() {
            Some(device) => {
                for i in 0..device.n_streams {
                    let stream_id = (i + 1) as u8;
                    if let Some(stream) = self.streams.get(&stream_id) {
                        reqs.extend(stream.requests())
                    } else {
                        reqs.push(StreamRpcMetaReq::stream(stream_id));
                    }
                }
            }
            None => {
                reqs.push(StreamRpcMetaReq::device());
            }
        }
        metareqs_to_rpcs(&reqs)
    }

    fn get_metadata(&self) -> Result<DeviceFullMetadata, Vec<tio::Packet>> {
        let reqs = self.requests();
        if !reqs.is_empty() {
            return Err(reqs);
        }
        let mut streams = HashMap::new();
        for (id, stream) in &self.streams {
            streams.insert(*id, stream.get_metadata().unwrap());
        }
        Ok(DeviceFullMetadata {
            device: self.device.as_ref().unwrap().clone(),
            streams: streams,
        })
    }
}

use std::collections::VecDeque;

//#[deprecated(note = "this API is in active development and may change")]
pub struct Device {
    dev_port: proxy::Port,
    parser: DeviceDataParser,
    n_reqs: usize,
    sample_queue: VecDeque<Sample>,
}

impl Device {
    pub fn new(dev_port: proxy::Port) -> Device {
        Device {
            dev_port: dev_port,
            parser: DeviceDataParser::new(false),
            n_reqs: 0,
            sample_queue: VecDeque::new(),
        }
    }

    fn internal_rpcs(&mut self) {
        if self.n_reqs == 0 {
            let reqs = self.parser.requests();
            for req in reqs {
                self.dev_port.send(req).unwrap();
                self.n_reqs += 1;
            }
        }
    }

    fn process_packet(&mut self, pkt: tio::Packet) -> Option<tio::Packet> {
        match &pkt.payload {
            tio::proto::Payload::RpcReply(rep) => {
                if rep.id == 7855 {
                    self.n_reqs -= 1
                } else {
                    return Some(pkt);
                }
            }
            tio::proto::Payload::RpcError(err) => {
                if err.id == 7855 {
                    self.n_reqs -= 1
                } else {
                    return Some(pkt);
                }
            }
            _ => {}
        }

        self.sample_queue
            .append(&mut VecDeque::from(self.parser.process_packet(&pkt)));
        None
    }

    pub fn get_metadata(&mut self) -> DeviceFullMetadata {
        loop {
            if self.n_reqs == 0 {
                match self.parser.get_metadata() {
                    Ok(full_meta) => return full_meta,
                    Err(reqs) => {
                        for req in reqs {
                            self.dev_port.send(req).unwrap();
                            self.n_reqs += 1;
                        }
                    }
                }
            }
            let pkt = self.dev_port.recv().unwrap();
            self.process_packet(pkt);
        }
    }

    pub fn next(&mut self) -> Sample {
        loop {
            if !self.sample_queue.is_empty() {
                return self.sample_queue.pop_front().unwrap();
            }

            self.internal_rpcs();
            let pkt = self.dev_port.recv().expect("no packet in blocking recv");
            self.process_packet(pkt);
        }
    }

    pub fn try_next(&mut self) -> Option<Sample> {
        loop {
            if !self.sample_queue.is_empty() {
                return self.sample_queue.pop_front();
            }

            self.internal_rpcs();
            self.process_packet(match self.dev_port.try_recv() {
                Ok(pkt) => pkt,
                Err(proxy::RecvError::WouldBlock) => {
                    return None;
                }
                _ => {
                    panic!("receive error");
                }
            });
        }
    }

    pub fn drain(&mut self) -> Vec<Sample> {
        loop {
            self.internal_rpcs();
            self.process_packet(match self.dev_port.try_recv() {
                Ok(pkt) => pkt,
                Err(proxy::RecvError::WouldBlock) => {
                    break;
                }
                _ => {
                    panic!("receive error");
                }
            });
        }

        self.sample_queue.drain(0..).collect()
    }

    pub fn raw_rpc(&mut self, name: &str, arg: &[u8]) -> Result<Vec<u8>, tio::proxy::RpcError> {
        if let Err(err) = self.dev_port.send(util::PacketBuilder::make_rpc_request(
            name,
            arg,
            0,
            DeviceRoute::root(),
        )) {
            return Err(tio::proxy::RpcError::SendFailed(err));
        }
        loop {
            self.internal_rpcs();
            let pkt = self.dev_port.recv().expect("no packet in blocking recv");
            if let Some(pkt) = self.process_packet(pkt) {
                match pkt.payload {
                    tio::proto::Payload::RpcReply(rep) => return Ok(rep.reply),
                    tio::proto::Payload::RpcError(err) => {
                        return Err(tio::proxy::RpcError::ExecError(err))
                    }
                    _ => panic!("unexpected"),
                }
            }
        }
    }

    pub fn rpc<ReqT: tio::util::TioRpcRequestable<ReqT>, RepT: tio::util::TioRpcReplyable<RepT>>(
        &mut self,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, tio::proxy::RpcError> {
        let ret = self.raw_rpc(name, &arg.to_request())?;
        if let Ok(val) = RepT::from_reply(&ret) {
            Ok(val)
        } else {
            Err(tio::proxy::RpcError::TypeError)
        }
    }

    /// Action: rpc with no argument which returns nothing
    pub fn action(&mut self, name: &str) -> Result<(), tio::proxy::RpcError> {
        self.rpc(name, ())
    }

    pub fn get<T: tio::util::TioRpcReplyable<T>>(
        &mut self,
        name: &str,
    ) -> Result<T, tio::proxy::RpcError> {
        self.rpc(name, ())
    }

    pub fn get_multi(&mut self, name: &str) -> Vec<u8> {
        let mut full_reply = vec![];

        for i in 0u16..=65535u16 {
            match self.raw_rpc(&name, &i.to_le_bytes().to_vec()) {
                Ok(mut rep) => full_reply.append(&mut rep),
                Err(proxy::RpcError::ExecError(err)) => {
                    if let tio::proto::RpcErrorCode::InvalidArgs = err.error {
                        break;
                    } else {
                        panic!("RPC error");
                    }
                }
                _ => {
                    panic!("RPC error")
                }
            }
        }

        full_reply
    }

    pub fn get_multi_str(&mut self, name: &str) -> String {
        String::from_utf8_lossy(&self.get_multi(name)).to_string()
    }
}
