use super::*;
use std::collections::BTreeMap;
use std::ops::DerefMut;

/// Dynamic value, type erased value container
#[derive(Debug)]
pub enum DynamicValue {
    I64(i64),
    F64(f64),
    Bytes(Box<Vec<u8>>),
    Struct(Box<DynamicStruct>),
    List {
        item_ty: Ty,
        items: Box<Vec<DynamicValue>>,
    },
    Stop,
}

impl Default for DynamicValue {
    fn default() -> Self {
        panic!("call default_for_ty");
    }
}

impl DynamicValue {
    pub fn default_for_ty(ty: Ty) -> Self {
        match ty {
            Ty::I64 => Self::I64(0),
            Ty::F64 => Self::F64(0.),
            Ty::Bytes => Self::Bytes(Box::new(vec![])),
            Ty::List => Self::List {
                item_ty: Ty::Any,
                items: Box::new(vec![]),
            },
            Ty::Struct => Self::Struct(Box::new(DynamicStruct::default())),
            Ty::Stop => Self::Stop,
            Ty::Any => {
                panic!("No default for Any ty allowed");
            }
        }
    }
}

impl Value for DynamicValue {
    fn ty(&self) -> Ty {
        match self {
            DynamicValue::I64(_) => Ty::I64,
            DynamicValue::F64(_) => Ty::F64,
            DynamicValue::Bytes(_) => Ty::Bytes,
            DynamicValue::Struct(_) => Ty::Struct,
            DynamicValue::List { .. } => Ty::List,
            DynamicValue::Stop => Ty::Stop,
        }
    }

    fn from_input(&mut self, input: &mut InputProtocol<'_>) -> Result<(), Error> {
        match self {
            DynamicValue::I64(ref mut v) => *v = input.read_i64()?,
            DynamicValue::F64(ref mut v) => *v = input.read_f64()?,
            DynamicValue::Bytes(ref mut v) => *v.deref_mut() = input.read_bytes()?.to_vec(),
            DynamicValue::Struct(ref mut s) => s.from_input(input)?,
            DynamicValue::List {
                ref mut item_ty,
                ref mut items,
            } => {
                let (item_ty_input, item_len) = input.read_list_header()?;

                let mut items_input = Vec::with_capacity(item_len as usize);
                for _i in 0..item_len {
                    let mut v = DynamicValue::default_for_ty(item_ty_input);
                    v.from_input(input)?;
                    items_input.push(v);
                }
                *items = Box::new(items_input);
                *item_ty = item_ty_input;
            }
            DynamicValue::Stop => {
                // do nothing
            }
        };
        Ok(())
    }

    fn to_output(&self, output: &mut OutProtocol<'_>) {
        match self {
            DynamicValue::I64(v) => output.write_i64(*v),
            DynamicValue::F64(v) => output.write_f64(*v),
            DynamicValue::Bytes(v) => output.write_bytes(v.as_slice()),
            DynamicValue::Struct(s) => s.to_output(output),
            DynamicValue::List { item_ty, items } => {
                output.write_list_header(*item_ty, items.len() as u32);
                for item in items.iter() {
                    item.to_output(output);
                }
            }
            DynamicValue::Stop => {
                // do nothing
            }
        }
    }
}

/// A dynamic value bag to hold dynamic values
#[derive(Debug)]
pub struct DynamicStruct {
    fields: BTreeMap<u8, DynamicValue>,
}

impl Default for DynamicStruct {
    fn default() -> Self {
        Self {
            fields: Default::default(),
        }
    }
}

impl DynamicStruct {
    pub fn insert(&mut self, index: u8, value: DynamicValue) -> Option<DynamicValue> {
        self.fields.insert(index, value)
    }
}

impl Value for DynamicStruct {
    fn ty(&self) -> Ty {
        Ty::Struct
    }

    fn from_input(&mut self, input: &mut InputProtocol<'_>) -> Result<(), Error> {
        let mut fields = BTreeMap::<u8, DynamicValue>::default();

        while let Some((ty, index)) = input.read_non_stop_field()? {
            let mut value = DynamicValue::default_for_ty(ty);
            value.from_input(input)?;
            fields.insert(index, value);
        }

        self.fields = fields;
        Ok(())
    }

    fn to_output(&self, output: &mut OutProtocol<'_>) {
        for (index, value) in self.fields.iter() {
            output.write_field_header(value.ty(), *index);
            value.to_output(output);
        }
        output.write_stop();
    }
}
