mod error;
pub use error::Error;

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
#[repr(u8)]
pub enum Ty {
    /// Any type is only allowed for empty sized list.
    /// it doesn't make any difference for item_type.
    /// introduce Any type enables dynamic value bag
    Any = 0,
    I64 = 1,
    F64 = 2,
    Bytes = 3,
    List = 4,
    Struct = 5,
    // indicate Struct finish
    Stop = 255,
}

pub trait Value: Default + Sized {
    /// the wire type for this value
    fn ty(&self) -> Ty;

    fn from_input(&mut self, input: &mut InputProtocol<'_>) -> Result<(), Error>;

    fn to_output(&self, output: &mut OutProtocol<'_>);

    /// load from slice
    fn from_slice(slice: &[u8]) -> Result<Self, Error> {
        let mut input = InputProtocol::new(slice);
        let mut value = Self::default();
        value.from_input(&mut input)?;
        Ok(value)
    }

    /// serialize to vec
    fn to_vec(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(128);
        let mut output = OutProtocol::new(&mut buffer);
        self.to_output(&mut output);
        buffer
    }
}

mod output;
pub use output::*;

mod input;
pub use input::*;

mod value;
pub use value::*;

#[cfg(test)]
mod tests;

/// convert ty to u8
pub(crate) fn ty_to_u8(ty: Ty) -> u8 {
    ty as u8
}

/// convert u8 to ty
pub(crate) fn try_u8_to_ty(val: u8) -> Result<Ty, Error> {
    Ok(match val {
        0 => Ty::Any,
        1 => Ty::I64,
        2 => Ty::F64,
        3 => Ty::Bytes,
        4 => Ty::List,
        5 => Ty::Struct,
        255 => Ty::Stop,
        _ => return Err(Error::DecodeInvalidType),
    })
}

// re-export Value derived macro
pub use vdb_derive::Value;

impl Value for i64 {
    fn ty(&self) -> Ty {
        Ty::I64
    }

    fn from_input(&mut self, input: &mut InputProtocol<'_>) -> Result<(), Error> {
        *self = input.read_i64()?;
        Ok(())
    }

    fn to_output(&self, output: &mut OutProtocol<'_>) {
        output.write_i64(*self);
    }
}

impl Value for f64 {
    fn ty(&self) -> Ty {
        Ty::F64
    }

    fn from_input(&mut self, input: &mut InputProtocol<'_>) -> Result<(), Error> {
        *self = input.read_f64()?;
        Ok(())
    }

    fn to_output(&self, output: &mut OutProtocol<'_>) {
        output.write_f64(*self);
    }
}

impl Value for Vec<u8> {
    fn ty(&self) -> Ty {
        Ty::Bytes
    }

    fn from_input(&mut self, input: &mut InputProtocol<'_>) -> Result<(), Error> {
        *self = input.read_bytes()?.to_vec();
        Ok(())
    }

    fn to_output(&self, output: &mut OutProtocol<'_>) {
        output.write_bytes(self.as_slice())
    }
}

impl Value for String {
    fn ty(&self) -> Ty {
        Ty::Bytes
    }

    fn from_input(&mut self, input: &mut InputProtocol<'_>) -> Result<(), Error> {
        *self = String::from_utf8(input.read_bytes()?.to_vec()).map_err(|_| Error::String)?;
        Ok(())
    }

    fn to_output(&self, output: &mut OutProtocol<'_>) {
        output.write_bytes(self.as_bytes())
    }
}

impl<T: Value> Value for Vec<T> {
    fn ty(&self) -> Ty {
        Ty::List
    }

    fn from_input(&mut self, input: &mut InputProtocol<'_>) -> Result<(), Error> {
        let (_item_ty, size) = input.read_list_header()?;
        let mut result = Vec::with_capacity(size as usize);
        for _i in 0..size {
            let mut item_val = T::default();
            item_val.from_input(input)?;
            result.push(item_val);
        }
        *self = result;
        Ok(())
    }

    fn to_output(&self, output: &mut OutProtocol<'_>) {
        let item_ty = T::default().ty();
        output.write_list_header(item_ty, self.len() as u32);
        for item in self.iter() {
            item.to_output(output);
        }
    }
}

impl<T: Value> Value for Option<T> {
    fn ty(&self) -> Ty {
        T::default().ty()
    }

    fn from_input(&mut self, input: &mut InputProtocol<'_>) -> Result<(), Error> {
        let mut value = T::default();
        value.from_input(input)?;
        *self = Some(value);
        Ok(())
    }

    fn to_output(&self, output: &mut OutProtocol<'_>) {
        if let Some(ref val) = self {
            val.to_output(output);
        } else {
            let val = T::default();
            val.to_output(output);
        }
    }
}
