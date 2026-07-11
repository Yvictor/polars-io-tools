use crate::TypeInfo;
use bytes::buf::UninitSlice;
use bytes::{BufMut, BytesMut};
use std::borrow::{Borrow, BorrowMut};
use std::ops::{Deref, DerefMut};

pub(crate) struct BytesMutWithTypeInfo<'bytes, 'info> {
    bytes: &'bytes mut BytesMut,
    type_info: Option<&'info TypeInfo>,
}

impl<'bytes, 'info> BytesMutWithTypeInfo<'bytes, 'info> {
    pub fn new(bytes: &'bytes mut BytesMut) -> Self {
        BytesMutWithTypeInfo {
            bytes,
            type_info: None,
        }
    }

    pub fn with_type_info(mut self, type_info: &'info TypeInfo) -> Self {
        self.type_info = Some(type_info);
        self
    }

    pub fn type_info(&self) -> Option<&'info TypeInfo> {
        self.type_info
    }
}

unsafe impl BufMut for BytesMutWithTypeInfo<'_, '_> {
    fn remaining_mut(&self) -> usize {
        self.bytes.remaining_mut()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.bytes.advance_mut(cnt)
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.bytes.chunk_mut()
    }
}

impl Borrow<[u8]> for BytesMutWithTypeInfo<'_, '_> {
    fn borrow(&self) -> &[u8] {
        self.bytes.deref()
    }
}

impl BorrowMut<[u8]> for BytesMutWithTypeInfo<'_, '_> {
    fn borrow_mut(&mut self) -> &mut [u8] {
        self.bytes.borrow_mut()
    }
}

impl Deref for BytesMutWithTypeInfo<'_, '_> {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        self.bytes
    }
}

impl DerefMut for BytesMutWithTypeInfo<'_, '_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.bytes
    }
}
