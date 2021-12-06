use super::*;

pub struct InputProtocol<'a> {
    store: &'a [u8],
}

impl<'a> InputProtocol<'a> {
    pub fn new(store: &'a [u8]) -> Self {
        Self { store }
    }
}

impl InputProtocol<'_> {
    pub fn read_i64(&mut self) -> Result<i64, Error> {
        Ok(i64::from_be_bytes(self.read_n_bytes::<8>()?))
    }

    pub fn read_f64(&mut self) -> Result<f64, Error> {
        Ok(f64::from_be_bytes(self.read_n_bytes::<8>()?))
    }

    pub fn read_bytes(&mut self) -> Result<&[u8], Error> {
        let size = self.raw_read_u32()?;
        self.raw_read_bytes(size)
    }

    pub fn raw_read_u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(self.read_n_bytes::<4>()?))
    }

    pub fn raw_read_bytes(&mut self, n: u32) -> Result<&[u8], Error> {
        if self.store.len() < n as usize {
            return Err(Error::DecodePrematureEnd);
        }

        let (left, right) = self.store.split_at(n as usize);
        self.store = right;
        Ok(left)
    }

    pub fn read_n_bytes<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        if self.store.len() < N {
            return Err(Error::DecodePrematureEnd);
        }

        let mut buffer = [0u8; N];
        let (left, right) = self.store.split_at(N);
        buffer.copy_from_slice(left);
        self.store = right;
        Ok(buffer)
    }

    pub fn read_field_header(&mut self) -> Result<(Ty, u8), Error> {
        let [ty, index] = self.read_n_bytes::<2>()?;
        let ty = try_u8_to_ty(ty)?;
        Ok((ty, index))
    }

    pub fn read_list_header(&mut self) -> Result<(Ty, u32), Error> {
        let [ty] = self.read_n_bytes::<1>()?;
        let ty = try_u8_to_ty(ty)?;
        Ok((ty, self.raw_read_u32()?))
    }

    pub fn read_non_stop_field(&mut self) -> Result<Option<(Ty, u8)>, Error> {
        let (ty, index) = self.read_field_header()?;
        if ty == Ty::Stop {
            return Ok(None);
        }

        Ok(Some((ty, index)))
    }

    /// skip field with type ty
    pub fn skip_field(&mut self, ty: Ty) -> Result<(), Error> {
        match ty {
            Ty::Any | Ty::Stop => {
                // do nothing, Any only valid for zero sized List,
                // Stop already processed by read_non_stop_field
            }
            Ty::I64 | Ty::F64 => {
                self.read_n_bytes::<8>()?;
            }
            Ty::Bytes => {
                self.read_bytes()?;
            }
            Ty::List => {
                let (item_ty, size) = self.read_list_header()?;
                for _i in 0..size {
                    self.skip_field(item_ty)?;
                }
            }
            Ty::Struct => {
                while let Some((ty, _index)) = self.read_non_stop_field()? {
                    self.skip_field(ty)?;
                }
            }
        };

        Ok(())
    }
}
