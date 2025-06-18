// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`ParquetFileReaderFactory`] and [`DefaultParquetFileReaderFactory`] for
//! low level control of parquet file readers

use crate::ParquetFileMetrics;
use bytes::Bytes;
use datafusion_common::DataFusionError;
use datafusion_datasource::file_meta::FileMeta;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use object_store::DynObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::Length;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Error, Read, Seek, SeekFrom};
use std::ops::Range;
use std::os::fd::AsFd;
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex};

/// Interface for reading parquet files.
///
/// The combined implementations of [`ParquetFileReaderFactory`] and
/// [`AsyncFileReader`] can be used to provide custom data access operations
/// such as pre-cached metadata, I/O coalescing, etc.
///
/// See [`DefaultParquetFileReaderFactory`] for a simple implementation.
pub trait ParquetFileReaderFactory: Debug + Send + Sync + 'static {
    /// Provides an `AsyncFileReader` for reading data from a parquet file specified
    ///
    /// # Notes
    ///
    /// If the resulting [`AsyncFileReader`]  returns `ParquetMetaData` without
    /// page index information, the reader will load it on demand. Thus it is important
    /// to ensure that the returned `ParquetMetaData` has the necessary information
    /// if you wish to avoid a subsequent I/O
    ///
    /// # Arguments
    /// * partition_index - Index of the partition (for reporting metrics)
    /// * file_meta - The file to be read
    /// * metadata_size_hint - If specified, the first IO reads this many bytes from the footer
    /// * metrics - Execution metrics
    fn create_reader(
        &self,
        store: Arc<DynObjectStore>,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>>;

    fn create_file_reader(
        &self,
        _: File,
        store: Arc<DynObjectStore>,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        self.create_reader(
            store,
            partition_index,
            file_meta,
            metadata_size_hint,
            metrics,
        )
    }
}

/// Default implementation of [`ParquetFileReaderFactory`]
///
/// This implementation:
/// 1. Reads parquet directly from an underlying [`ObjectStore`] instance.
/// 2. Reads the footer and page metadata on demand.
/// 3. Does not cache metadata or coalesce I/O operations.
#[derive(Debug)]
pub struct DefaultParquetFileReaderFactory {}

impl DefaultParquetFileReaderFactory {
    /// Create a new `DefaultParquetFileReaderFactory`.
    pub fn new() -> Self {
        Self {}
    }
}

/// Implements [`AsyncFileReader`] for a parquet file in object storage.
///
/// This implementation uses the [`ParquetObjectReader`] to read data from the
/// object store on demand, as required, tracking the number of bytes read.
///
/// This implementation does not coalesce I/O operations or cache bytes. Such
/// optimizations can be done either at the object store level or by providing a
/// custom implementation of [`ParquetFileReaderFactory`].
pub(crate) struct ParquetFileReader<T>
where
    T: AsyncFileReader,
{
    pub file_metrics: ParquetFileMetrics,
    pub inner: T,
}

impl<T> AsyncFileReader for ParquetFileReader<T>
where
    T: AsyncFileReader,
{
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let bytes_scanned = range.end - range.start;
        self.file_metrics.bytes_scanned.add(bytes_scanned as usize);
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>>
    where
        Self: Send,
    {
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total as usize);
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        self.inner.get_metadata(options)
    }
}

struct StdFileReader {
    file: Arc<File>,
    file_size: u64,
    metadata_size_hint: Option<usize>,
}

fn read_range(
    file: &File,
    file_size: u64,
    range: Range<u64>,
) -> parquet::errors::Result<Bytes> {
    if range.start >= file_size {
        return Err(ParquetError::EOF("".to_string()));
    }

    // Don't read past end of file
    let to_read = (range.end.min(file_size) - range.start) as usize;

    let mut buf = Vec::with_capacity(to_read);
    unsafe { buf.set_len(to_read); }
    file.read_exact_at(&mut buf, range.start)?;
    Ok(buf.into())
}

impl AsyncFileReader for StdFileReader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                let file = Arc::clone(&self.file);
                let file_size = self.file_size;
                let res = runtime
                    .spawn_blocking(move || {
                        read_range(&file, file_size, range)
                    })
                    .unwrap_or_else(|err| Err(ParquetError::External(Box::new(err))))
                    .boxed();
                res
            }
            Err(err) => {
                futures::future::ready(Err(ParquetError::External(Box::new(err)))).boxed()
            }
        }
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                let file = Arc::clone(&self.file);
                let file_size = self.file_size;
                let res = runtime
                    .spawn_blocking(move || {
                        let mut byte_ranges = vec![];
                        for range in ranges {
                            byte_ranges.push(read_range(&file, file_size, range)?)
                        }
                        Ok(byte_ranges)
                    })
                    .unwrap_or_else(|err| Err(ParquetError::External(Box::new(err))))
                    .boxed();
                res
            }
            Err(err) => {
                futures::future::ready(Err(ParquetError::External(Box::new(err)))).boxed()
            }
        }
    }

    fn get_metadata(
        &mut self,
        _options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let file_size = self.file_size;

            let metadata = ParquetMetaDataReader::new()
                .with_prefetch_hint(self.metadata_size_hint)
                .load_and_finish(self, file_size)
                .await
                .map_err(DataFusionError::from)
                .map_err(|e| {
                    ParquetError::General(format!(
                        "AsyncChunkReader::get_metadata error: {e}"
                    ))
                })?;

            Ok(Arc::new(metadata))
        })
    }
}

impl ParquetFileReaderFactory for DefaultParquetFileReaderFactory {
    fn create_reader(
        &self,
        store: Arc<DynObjectStore>,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            file_meta.location().as_ref(),
            metrics,
        );

        let mut inner =
            ParquetObjectReader::new(store, file_meta.object_meta.location.clone())
                .with_file_size(file_meta.object_meta.size);

        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint)
        };

        Ok(Box::new(ParquetFileReader {
            inner,
            file_metrics,
        }))
    }

    fn create_file_reader(
        &self,
        file: File,
        _: Arc<DynObjectStore>,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            file_meta.location().as_ref(),
            metrics,
        );

        let file_size = file.len();

        let reader = StdFileReader {
            file: Arc::new(file),
            file_size,
            metadata_size_hint,
        };

        Ok(Box::new(ParquetFileReader {
            inner: reader,
            file_metrics,
        }))
    }
}
