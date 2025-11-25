use crate::data::buffer::{DataSlice, ColumnBatch};
use crate::data::ColumnFilter;
use hdf5::{File, Result, H5Type, Dataset, SimpleExtents, Group, Location};
use hdf5::filters::{Blosc, BloscShuffle};
use hdf5::types::VarLenUnicode; 
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct ExportStats {
    pub total_samples: u64,
    pub start_time: Option<f64>,
    pub end_time: Option<f64>,
    pub streams_written: HashSet<String>,
}

impl Default for ExportStats {
    fn default() -> Self {
        Self {
            total_samples: 0,
            start_time: None,
            end_time: None,
            streams_written: HashSet::new(),
        }
    }
}

pub struct Hdf5Appender {
    file: File,
    datasets: HashMap<String, Dataset>,
    compress: bool,
    debug: bool,
    seen_debug: HashSet<String>,
    pub stats: ExportStats,
}

impl Hdf5Appender {
    pub fn new(path: &Path, compress: bool, debug: bool) -> Result<Self> {
        if path.exists() {
            panic!("Refusing to overwrite existing file: {:?}", path);
        }

        let file = File::create(path)?;
        
        Ok(Self {
            file,
            datasets: HashMap::new(),
            compress,
            debug,
            seen_debug: HashSet::new(),
            stats: ExportStats::default(),
        })
    }

    pub fn write_slice(&mut self, slice: &DataSlice, filter: &Option<ColumnFilter>) -> Result<()> {
        if slice.is_empty() {
            return Ok(());
        }

        let route_str = slice.stream_key.route.to_string().trim_start_matches('/').to_string();
        let stream_name = &slice.stream_metadata.name; 

        // 1. Identify valid columns BEFORE writing anything to disk.
        //    This prevents empty groups or timestamps for filtered-out streams (like IMUs).
        let mut valid_columns = Vec::new();

        for (col_id, batch) in &slice.columns {
            // Extract metadata if available
            let (name, units, desc) = if let Some(meta) = slice.column_metadata.get(col_id) {
                (meta.name.clone(), Some(meta.units.clone()), Some(meta.description.clone()))
            } else {
                (format!("col_{}", col_id), None, None)
            };

            // Apply Filter
            if let Some(f) = filter {
                let path_check = f.get_path_string(&slice.stream_key.route, stream_name, &name);
                
                if self.debug && !self.seen_debug.contains(&path_check) {
                    let is_match = f.matches(&slice.stream_key.route, stream_name, &name);
                    println!("[DEBUG] Filter Check: '{}' -> {}", path_check, is_match);
                    self.seen_debug.insert(path_check);
                }

                if !f.matches(&slice.stream_key.route, stream_name, &name) {
                    continue; 
                }
            }
            valid_columns.push((col_id, batch, name, units, desc));
        }

        // 2. If no columns matched the filter, abort immediately.
        if valid_columns.is_empty() {
            return Ok(());
        }

        // 3. Create Group and Write Metadata (Only runs if we have data)
        let group_path = format!("/{}/{}_{}/{}", 
            route_str, 
            slice.session_id, 
            slice.segment_id, 
            stream_name
        );

        self.ensure_group(&group_path)?;

        let group = self.file.group(&group_path)?;
        
        // Write segment attributes once per group creation
        if group.attr("sampling_rate").is_err() {
            self.write_metadata_attributes(&group, slice)?;
        }

        // 4. Write Time and Data
        self.append_dataset(&group_path, "time", &slice.timestamps, None, Some("Time in seconds"))?;
        
        for (_col_id, batch, name, units, desc) in valid_columns {
            let units_opt = units.as_ref().filter(|u| !u.is_empty());
            let desc_opt = desc.as_deref();

            match batch {
                ColumnBatch::F64(data) => self.append_dataset(&group_path, &name, data, units_opt, desc_opt)?,
                ColumnBatch::I64(data) => self.append_dataset(&group_path, &name, data, units_opt, desc_opt)?,
                ColumnBatch::U64(data) => self.append_dataset(&group_path, &name, data, units_opt, desc_opt)?,
            }
        }

        // 5. Update Statistics
        self.update_stats(slice, &route_str, stream_name);

        Ok(())
    }

    fn update_stats(&mut self, slice: &DataSlice, route_str: &str, stream_name: &str) {
        let count = slice.len() as u64;
        self.stats.total_samples += count;
        
        let t_start = slice.timestamps.first().copied().unwrap_or(0.0);
        let t_end = slice.timestamps.last().copied().unwrap_or(0.0);

        self.stats.start_time = Some(self.stats.start_time.map_or(t_start, |t| t.min(t_start)));
        self.stats.end_time = Some(self.stats.end_time.map_or(t_end, |t| t.max(t_end)));
        
        self.stats.streams_written.insert(format!("/{}/{}", route_str, stream_name));
    }

    fn write_metadata_attributes(&self, group: &Group, slice: &DataSlice) -> Result<()> {
        let meta = &slice.segment_metadata;

        self.write_attr_scalar(group, "sampling_rate", &meta.sampling_rate)?;
        self.write_attr_scalar(group, "decimation", &meta.decimation)?;
        self.write_attr_scalar(group, "start_time", &meta.start_time)?;
        self.write_attr_scalar(group, "filter_cutoff", &meta.filter_cutoff)?;
        self.write_attr_scalar(group, "session_id", &slice.session_id)?;

        let epoch_u8: u8 = meta.time_ref_epoch.clone().into();
        self.write_attr_scalar(group, "time_ref_epoch", &epoch_u8)?;
        
        let filter_type_u8: u8 = meta.filter_type.clone().into();
        self.write_attr_scalar(group, "filter_type", &filter_type_u8)?;

        if !meta.time_ref_serial.is_empty() {
            self.write_attr_string(group, "time_ref_serial", &meta.time_ref_serial)?;
        }
        
        Ok(())
    }

    fn append_dataset<T: H5Type + Clone>(
        &mut self, 
        group_path: &str, 
        name: &str, 
        data: &[T],
        units: Option<&String>,
        description: Option<&str>
    ) -> Result<()> {
        let full_path = format!("{}/{}", group_path, name);
        
        if !self.datasets.contains_key(&full_path) {
            let group = self.file.group(group_path)?;
            
            let ds = if let Ok(existing) = group.dataset(name) {
                existing
            } else {
                let builder = group.new_dataset::<T>()
                    .chunk((65_536,))
                    .shape(SimpleExtents::resizable([0usize]));
                
                let builder = if self.compress {
                    builder.blosc(Blosc::BloscLZ, 5, BloscShuffle::Byte)
                } else {
                    builder
                };
                
                let ds = builder.create(name)?;

                if let Some(u_str) = units {
                    self.write_attr_string(&ds, "units", u_str)?;
                }
                if let Some(d_str) = description {
                    if !d_str.is_empty() {
                        self.write_attr_string(&ds, "description", d_str)?;
                    }
                }

                ds
            };
            self.datasets.insert(full_path.clone(), ds);
        }
        
        let ds = self.datasets.get(&full_path).unwrap();
        let current_size = ds.shape()[0];
        let new_data_len = data.len();
        
        ds.resize((current_size + new_data_len,))?;
        ds.write_slice(data, current_size..)?;
        Ok(())
    }

    fn ensure_group(&self, path: &str) -> Result<()> {
        if self.file.group(path).is_ok() {
            return Ok(());
        }
        
        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let mut current_path = String::new();
        
        for part in parts {
            current_path.push('/');
            current_path.push_str(part);
            
            if self.file.group(&current_path).is_err() {
                self.file.create_group(&current_path)?;
            }
        }
        Ok(())
    }
    
    fn write_attr_scalar<T: H5Type>(&self, loc: &Location, name: &str, val: &T) -> Result<()> {
        let attr = loc.new_attr::<T>().create(name)?;
        attr.write_scalar(val)
    }

    fn write_attr_string(&self, loc: &Location, name: &str, val: &str) -> Result<()> {
        let attr = loc.new_attr::<VarLenUnicode>().create(name)?;
        let vlu = val.parse::<VarLenUnicode>().unwrap();
        attr.write_scalar(&vlu)
    }
}