use crate::error::{Error, Result};
use crate::model::{FsOp, Key};

pub fn metadata_workload(batch_count: usize, batch_size: usize) -> Result<Vec<Vec<FsOp>>> {
    if batch_count == 0 {
        return Ok(Vec::new());
    }
    if batch_size == 0 {
        return Err(Error::InvalidBatch(
            "batch_size must be greater than zero".to_string(),
        ));
    }

    let dir_count = if batch_size == 1 {
        0
    } else {
        (batch_size - 1).min(256)
    };

    let mut batches = Vec::with_capacity(batch_count);
    batches.push(first_batch(batch_size, dir_count)?);
    for batch_index in 1..batch_count {
        batches.push(create_file_batch(batch_index, batch_size, dir_count)?);
    }
    Ok(batches)
}

fn first_batch(batch_size: usize, dir_count: usize) -> Result<Vec<FsOp>> {
    let mut ops = Vec::with_capacity(batch_size);
    ops.push(FsOp::Mkdir { path: Key::root() });
    for dir_index in 0..dir_count {
        ops.push(FsOp::Mkdir {
            path: Key::new(format!("/dir_{dir_index}"))?,
        });
    }
    let mut file_index = 0usize;
    while ops.len() < batch_size {
        let dir_index = file_index % dir_count.max(1);
        ops.push(FsOp::Create {
            path: Key::new(format!("/dir_{dir_index}/b0_file_{file_index}"))?,
        });
        file_index += 1;
    }
    Ok(ops)
}

fn create_file_batch(batch_index: usize, batch_size: usize, dir_count: usize) -> Result<Vec<FsOp>> {
    let mut ops = Vec::with_capacity(batch_size);
    for file_index in 0..batch_size {
        let dir_index = file_index % dir_count.max(1);
        ops.push(FsOp::Create {
            path: Key::new(format!("/dir_{dir_index}/b{batch_index}_file_{file_index}"))?,
        });
    }
    Ok(ops)
}
