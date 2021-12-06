use super::*;

pub struct OutProtocol<'a> {
    store: &'a mut Vec<u8>,
}

impl<'a> OutProtocol<'a> {
    pub fn new(store: &'a mut Vec<u8>) -> Self {
        Self { store }
    }
}

impl OutProtocol<'_> {
    pub fn write_u32(&mut self, val: u32) {
        self.store.extend_from_slice(&val.to_be_bytes()[..]);
    }

    pub fn write_i64(&mut self, val: i64) {
        self.store.extend_from_slice(&val.to_be_bytes()[..]);
    }

    pub fn write_f64(&mut self, val: f64) {
        self.store.extend_from_slice(&val.to_be_bytes()[..]);
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.write_u32(bytes.len() as u32);
        self.store.extend_from_slice(bytes);
    }

    /// write field type and index, each took 1 byte
    pub fn write_field_header(&mut self, ty: Ty, index: u8) {
        self.store.push(ty_to_u8(ty));
        self.store.push(index);
    }

    pub fn write_list_header(&mut self, item_ty: Ty, item_size: u32) {
        self.store.push(ty_to_u8(item_ty));
        self.write_u32(item_size);
    }

    pub fn write_stop(&mut self) {
        self.write_field_header(Ty::Stop, 255);
    }
}
