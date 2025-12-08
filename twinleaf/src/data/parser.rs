use super::sample::{Column, Sample};
use crate::tio;
use proto::meta::MetadataType;
use proto::DeviceRoute;
use std::collections::HashMap;
use std::sync::Arc;
use tio::proto::meta::{
    ColumnMetadata, DeviceMetadata, MetadataContent, SegmentMetadata, StreamMetadata,
};
use tio::{proto, util};

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
            let next_segment = (segment.segment_id + 1).rem_euclid(stream.n_segments as u8);
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
                source: data.clone(),
            });
            self.segment_changed = false;
            self.meta_changed = false;
            offset += stream.sample_size;
            self.last_sample_number = sample_n;
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

    pub fn get_metadata(&self) -> Result<DeviceFullMetadata, Vec<tio::Packet>> {
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
