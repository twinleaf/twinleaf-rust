use super::{too_small, Error};

// Split a varlen message into fixed and variable length parts
pub fn split<'a>(raw: &'a [u8], full_data: &[u8]) -> Result<(&'a [u8], &'a [u8]), Error> {
    if raw.len() < 1 {
        return Err(too_small(full_data));
    }
    let fixed_len = raw[0] as usize;
    if (fixed_len < 2) || (fixed_len > raw.len()) {
        Err(Error::InvalidPayload(full_data.to_vec()))
    } else {
        Ok((&raw[0..fixed_len], &raw[fixed_len..]))
    }
}

pub fn peel<'a>(
    varlen: &'a [u8],
    len: u8,
    full_data: &[u8],
) -> Result<(&'a [u8], &'a [u8]), Error> {
    let len = usize::from(len);
    if len <= varlen.len() {
        Ok((&varlen[0..len], &varlen[len..]))
    } else {
        Err(Error::InvalidPayload(full_data.to_vec()))
    }
}

pub fn peel_string<'a>(
    varlen: &'a [u8],
    len: u8,
    full_data: &[u8],
) -> Result<(String, &'a [u8]), Error> {
    let (arg, rest) = peel(varlen, len, full_data)?;
    Ok((String::from_utf8_lossy(&arg).to_string(), rest))
}

pub fn checked_u8_size(size: usize) -> Result<u8, ()> {
    if size <= u8::MAX.into() {
        Ok(size as u8)
    } else {
        Err(())
    }
}

pub fn checked_u16_size(size: usize) -> Result<u16, ()> {
    if size <= u16::MAX.into() {
        Ok(size as u16)
    } else {
        Err(())
    }
}

pub fn append_string(varlen: &mut Vec<u8>, value: &str) -> Result<u8, ()> {
    let orig_len = varlen.len();
    varlen.extend(value.as_bytes());
    let len = varlen.len() - orig_len;
    checked_u8_size(len)
}

pub fn extend(
    mut fixed: Vec<u8>,
    mut varlen: Vec<u8>,
    extra_fixed: &[u8],
    extra_varlen: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), ()> {
    if (extra_varlen.len() > 0) && (extra_fixed.len() == 0) {
        return Err(());
    }
    if (fixed.len() < 1) || (usize::from(fixed[0]) != fixed.len()) {
        return Err(());
    }
    fixed[0] = checked_u8_size(fixed.len() + extra_fixed.len())?;
    fixed.extend(extra_fixed);
    varlen.extend(extra_varlen);
    Ok((fixed, varlen))
}
