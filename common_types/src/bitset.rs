// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! BitSet supports counting set/unset bits.

#[derive(Debug, Default, Clone)]
pub struct BitSet {
    /// The bits are stored as bytes in the least significant bit order.
    buffer: Vec<u8>,
    /// The number of real bits in the `buffer`
    num_bits: usize,
}

impl BitSet {
    /// Initialize a unset [`BitSet`].
    pub fn new(num_bits: usize) -> Self {
        Self {
            buffer: vec![0; Self::num_bytes(num_bits)],
            num_bits,
        }
    }

    #[inline]
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    #[inline]
    pub fn num_bytes(num_bits: usize) -> usize {
        (num_bits + 7) >> 3
    }

    /// Initialize directly from a buffer.
    ///
    /// None will be returned if the buffer's length is not enough to cover the
    /// bits of `num_bits`.
    pub fn try_from_raw(buffer: Vec<u8>, num_bits: usize) -> Option<Self> {
        if buffer.len() < Self::num_bytes(num_bits) {
            None
        } else {
            Some(Self { buffer, num_bits })
        }
    }

    /// Set the bit at the `index`.
    ///
    /// Return false if the index is outside the range.
    pub fn set(&mut self, index: usize) -> bool {
        if index >= self.num_bits {
            return false;
        }
        let (byte_index, bit_index) = Self::compute_byte_bit_index(index);
        self.buffer[byte_index] |= 1 << bit_index;
        true
    }

    /// Tells whether the bit at the `index` is set.
    pub fn is_set(&self, index: usize) -> Option<bool> {
        if index >= self.num_bits {
            return None;
        }
        let (byte_index, bit_index) = Self::compute_byte_bit_index(index);
        let set = (self.buffer[byte_index] & (1 << bit_index)) != 0;
        Some(set)
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buffer
    }

    #[inline]
    fn compute_byte_bit_index(index: usize) -> (usize, usize) {
        (index >> 3, index & 7)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_eq;

    use super::BitSet;

    #[test]
    fn test_set_op() {
        let mut bit_set = BitSet::new(50);

        assert!(bit_set.set(1));
        assert!(bit_set.is_set(1).unwrap());

        assert!(bit_set.set(20));
        assert!(bit_set.is_set(20).unwrap());
        assert!(bit_set.set(49));
        assert!(bit_set.is_set(49).unwrap());

        assert!(!bit_set.set(100));
        assert!(bit_set.is_set(100).is_none());

        assert_eq!(
            bit_set.into_bytes(),
            vec![
                0b00000010,
                0b00000000,
                0b00010000,
                0b000000000,
                0b00000000,
                0b00000000,
                0b00000010
            ]
        );
    }

    #[test]
    fn test_try_from_raw() {
        let raw_bytes: Vec<u8> = vec![0b11111111, 0b11110000, 0b00001111, 0b00001100, 0b00001001];
        assert!(BitSet::try_from_raw(raw_bytes.clone(), 50).is_none());
        assert!(BitSet::try_from_raw(raw_bytes.clone(), 40).is_some());
        assert!(BitSet::try_from_raw(raw_bytes, 1).is_some());
    }
}
