// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::boxed::Box;

use crate::bits::{Bit, BIT_MASKS};

/// BufferedWriter
/// BufferedWriter writes bytes to a buffer.
#[derive(Debug, Default, Clone)]
pub struct BufferedWriter {
    buf: Vec<u8>,
    pos: u32, // position in the last byte in the buffer
}

impl BufferedWriter {
    pub fn with_capacity(capacity: usize) -> Self {
        BufferedWriter {
            buf: Vec::with_capacity(capacity),
            // set pos to 8 to indicate the buffer has no space presently since it is empty
            pos: 8,
        }
    }

    #[allow(dead_code)]
    pub fn with_buf(buf: Vec<u8>) -> Self {
        BufferedWriter {
            buf,
            // set pos to 8 to indicate the buffer has no space presently since it is empty
            pos: 8,
        }
    }

    fn grow(&mut self) {
        self.buf.push(0);
    }

    fn last_index(&self) -> usize {
        if self.buf.is_empty() {
            return 0;
        }
        self.buf.len() - 1
    }
}

impl BufferedWriter {
    pub fn write_bit(&mut self, bit: Bit) {
        if self.pos == 8 {
            self.grow();
            self.pos = 0;
        }

        let i = self.last_index();

        if bit != Bit(0) {
            self.buf[i] |= BIT_MASKS[self.pos as usize];
        }

        self.pos += 1;
    }

    pub fn write_byte(&mut self, byte: u8) {
        if self.pos == 8 {
            self.grow();

            let i = self.last_index();
            self.buf[i] = byte;
            return;
        }

        let i = self.last_index();
        let mut b = byte >> self.pos;
        self.buf[i] |= b;

        self.grow();

        b = byte << (8 - self.pos);
        self.buf[i + 1] |= b;
    }

    // example: wtire_bits(4): data(u64 0000 0000 0000 00ff), write data 1111
    pub fn write_bits(&mut self, mut bits: u64, mut num: u32) {
        // we should never write more than 64 bits for a u64
        if num > 64 {
            num = 64;
        }

        bits = bits << (64 - num);
        while num >= 8 {
            let byte = bits >> 56;
            self.write_byte(byte as u8);

            bits <<= 8;
            num -= 8;
        }

        while num > 0 {
            let byte = bits >> 63;
            if byte == 1 {
                self.write_bit(Bit(1));
            } else {
                self.write_bit(Bit(0));
            }

            bits <<= 1;
            num -= 1;
        }
    }

    pub fn close(self) -> Box<[u8]> {
        self.buf.into_boxed_slice()
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.buf.len()
    }
}

#[cfg(test)]
mod tests {
    use super::BufferedWriter;
    use crate::bits::Bit;

    #[test]
    fn write_bit() {
        let mut b = BufferedWriter::with_capacity(0);

        // 170 = 0b10101010
        for i in 0..8 {
            if i % 2 == 0 {
                b.write_bit(Bit(1));
                continue;
            }

            b.write_bit(Bit(0));
        }

        // 146 = 0b10010010
        for i in 0..8 {
            if i % 3 == 0 {
                b.write_bit(Bit(1));
                continue;
            }

            b.write_bit(Bit(0));
        }

        // 136 = 010001000
        for i in 0..8 {
            if i % 4 == 0 {
                b.write_bit(Bit(1));
                continue;
            }

            b.write_bit(Bit(0));
        }

        assert_eq!(b.buf.len(), 3);

        assert_eq!(b.buf[0], 170);
        assert_eq!(b.buf[1], 146);
        assert_eq!(b.buf[2], 136);
    }

    #[test]
    fn write_byte() {
        let mut b = BufferedWriter::with_capacity(0);

        b.write_byte(234);
        b.write_byte(188);
        b.write_byte(77);

        assert_eq!(b.buf.len(), 3);

        assert_eq!(b.buf[0], 234);
        assert_eq!(b.buf[1], 188);
        assert_eq!(b.buf[2], 77);

        // write some bits so we can test `write_byte` when the last byte is partially
        // filled
        b.write_bit(Bit(1));
        b.write_bit(Bit(1));
        b.write_bit(Bit(1));
        b.write_bit(Bit(1));
        b.write_byte(0b11110000); // 1111 1111 0000
        b.write_byte(0b00001111); // 1111 1111 0000 0000 1111
        b.write_byte(0b00001111); // 1111 1111 0000 0000 1111 0000 1111

        assert_eq!(b.buf.len(), 7);
        assert_eq!(b.buf[3], 255); // 0b11111111 = 255
        assert_eq!(b.buf[4], 0); // 0b00000000 = 0
        assert_eq!(b.buf[5], 240); // 0b11110000 = 240
    }

    #[test]
    fn write_bits() {
        let mut b = BufferedWriter::with_capacity(0);

        // 101011
        b.write_bits(43, 6);

        // 010
        b.write_bits(2, 3);

        // 1
        b.write_bits(1, 1);

        // 1010 1100 1110 0011 1101
        b.write_bits(708157, 20);

        // 11
        b.write_bits(3, 2);

        assert_eq!(b.buf.len(), 4);

        assert_eq!(b.buf[0], 173); // 0b10101101 = 173
        assert_eq!(b.buf[1], 107); // 0b01101011 = 107
        assert_eq!(b.buf[2], 56); // 0b00111000 = 56
        assert_eq!(b.buf[3], 247); // 0b11110111 = 247
    }

    #[test]
    fn write_mixed() {
        let mut b = BufferedWriter::with_capacity(0);

        // 1010 1010
        for i in 0..8 {
            if i % 2 == 0 {
                b.write_bit(Bit(1));
                continue;
            }

            b.write_bit(Bit(0));
        }

        // 0000 1001
        b.write_byte(9);

        // 1001 1100 1100
        b.write_bits(2508, 12);

        println!("{:?}", b.buf);

        // 1111
        for _ in 0..4 {
            b.write_bit(Bit(1));
        }

        assert_eq!(b.buf.len(), 4);

        println!("{:?}", b.buf);

        assert_eq!(b.buf[0], 170); // 0b10101010 = 170
        assert_eq!(b.buf[1], 9); // 0b00001001 = 9
        assert_eq!(b.buf[2], 156); // 0b10011100 = 156
        assert_eq!(b.buf[3], 207); // 0b11001111 = 207
    }
}
