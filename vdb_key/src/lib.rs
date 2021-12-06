use std::cmp::Ordering;
use std::f64;
use std::fmt::{Debug, Formatter};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("[vdb_key] type mismatch")]
    TypeMismatch,
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
#[repr(u8)]
pub enum Ty {
    I64 = 1,
    F64 = 2,
    Bytes = 3,
}

#[derive(Debug, Clone)]
pub enum Component {
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
}

impl From<i8> for Component {
    fn from(v: i8) -> Self {
        Self::I64(v as i64)
    }
}

impl From<i16> for Component {
    fn from(v: i16) -> Self {
        Self::I64(v as i64)
    }
}

impl From<i32> for Component {
    fn from(v: i32) -> Self {
        Self::I64(v as i64)
    }
}

impl From<i64> for Component {
    fn from(v: i64) -> Self {
        Self::I64(v)
    }
}

impl From<f32> for Component {
    fn from(v: f32) -> Self {
        Self::F64(v as f64)
    }
}

impl From<f64> for Component {
    fn from(v: f64) -> Self {
        Self::F64(v)
    }
}

impl From<Vec<u8>> for Component {
    fn from(v: Vec<u8>) -> Self {
        Self::Bytes(v)
    }
}

impl Component {
    fn ty(&self) -> Ty {
        match self {
            Component::I64(_) => Ty::I64,
            Component::F64(_) => Ty::F64,
            Component::Bytes(_) => Ty::Bytes,
        }
    }

    fn byte_len_hint(&self) -> usize {
        match self {
            Component::I64(_) => 9,
            Component::F64(_) => 9,
            Component::Bytes(ref bytes) => {
                // bytes maybe encoded, buffer also allocated
                // with extra space, so just use naive number
                1 + bytes.len() + 1
            }
        }
    }
}

impl PartialEq<Self> for Component {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Component::I64(l), Component::I64(r)) => l.eq(r),
            (Component::F64(l), Component::F64(r)) => l.eq(r),
            (Component::Bytes(ref l), Component::Bytes(ref r)) => l.eq(r),
            _ => false,
        }
    }
}

impl Eq for Component {}

impl PartialOrd for Component {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Component {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Component::I64(l), Component::I64(r)) => l.cmp(r),
            (Component::F64(l), Component::F64(r)) => {
                // we treat Nan same here
                l.partial_cmp(r).unwrap_or(Ordering::Equal)
            }
            (Component::Bytes(ref l), Component::Bytes(ref r)) => l.cmp(r),
            (l, r) => l.ty().cmp(&r.ty()),
        }
    }
}

/// A maximum three parts component key, each part is a byte array.
/// the serialized bytes should hold same compare order as the struct
/// itself.
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Key {
    storage: Vec<u8>,
}

impl Debug for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.as_components())
    }
}

impl From<Component> for Key {
    fn from(c: Component) -> Self {
        Key::from(&[c][..])
    }
}

impl From<i64> for Key {
    fn from(val: i64) -> Self {
        Self::from(Component::from(val))
    }
}

impl TryFrom<Key> for i64 {
    type Error = Error;

    fn try_from(value: Key) -> Result<Self, Self::Error> {
        let mut it = value.as_components().into_iter();
        match (it.next(), it.next()) {
            (Some(Component::I64(val)), None) => Ok(val),
            _ => Err(Error::TypeMismatch),
        }
    }
}

impl From<&[Component]> for Key {
    fn from(components: &[Component]) -> Self {
        let cap = components
            .iter()
            .fold(0, |accu, c| accu + c.byte_len_hint());
        let mut key = Key::with_capacity(cap);
        for c in components {
            key.append_component(c);
        }
        key
    }
}

impl Key {
    pub fn new() -> Self {
        Self {
            storage: Default::default(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            storage: Vec::with_capacity(cap),
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.storage
    }

    pub fn load_from_bytes_unchecked(bytes: Vec<u8>) -> Self {
        Self { storage: bytes }
    }

    pub fn append_i64(&mut self, val: i64) {
        self.storage.push(Ty::I64 as u8);
        self.storage.extend_from_slice(&val.to_be_bytes());
    }

    pub fn append_f64(&mut self, val: f64) {
        self.storage.push(Ty::F64 as u8);
        self.storage.extend_from_slice(&val.to_be_bytes());
    }

    pub fn append_bytes(&mut self, val: &[u8]) {
        self.storage.push(Ty::Bytes as u8);
        let escaped = escape_bytes(val);
        self.storage.extend_from_slice(escaped.as_slice());
    }

    pub fn append_component(&mut self, component: &Component) {
        match component {
            Component::I64(v) => {
                self.append_i64(*v);
            }
            Component::F64(v) => {
                self.append_f64(*v);
            }
            Component::Bytes(v) => {
                self.append_bytes(v);
            }
        }
    }

    /// parse to components, if bytes exists, then it will be
    /// cloned.
    pub fn as_components(&self) -> Vec<Component> {
        let mut current = self.storage.as_slice();
        let mut components: Vec<Component> = Vec::new();

        // each component is at least 8 bytes, plus one type byte
        while !current.is_empty() {
            let ty = match current[0] {
                1 => Ty::I64,
                2 => Ty::F64,
                3 => Ty::Bytes,
                _ => panic!("invalid"),
            };

            current = &current[1..];

            match ty {
                Ty::I64 => {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&current[..8]);
                    let v = i64::from_be_bytes(buf);
                    components.push(Component::I64(v));
                    current = &current[8..];
                }
                Ty::F64 => {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&current[..8]);
                    let v = f64::from_be_bytes(buf);
                    components.push(Component::F64(v));
                    current = &current[8..];
                }
                Ty::Bytes => {
                    let (new_current, bytes) = parse_bytes(current);
                    components.push(Component::Bytes(bytes));
                    current = new_current;
                }
            }
        }

        components
    }
}

const BYTE_SEPARATOR: u8 = 0u8;

fn escape_bytes(val: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(val.len() + 1);

    for b in val {
        if *b == BYTE_SEPARATOR {
            result.push(BYTE_SEPARATOR);
        }
        result.push(*b);
    }

    result.push(BYTE_SEPARATOR);

    result
}

fn parse_bytes(val: &[u8]) -> (&[u8], Vec<u8>) {
    let mut result = Vec::with_capacity(val.len());
    let mut escaping = false;

    for (idx, b) in val.iter().enumerate() {
        if *b == BYTE_SEPARATOR {
            if escaping {
                result.push(BYTE_SEPARATOR);
                escaping = false;
            } else {
                escaping = true;
            }
            continue;
        }

        if escaping {
            // escaping and met non separator, should skip
            // the filled zeros if len less than 8
            return (&val[idx..], result);
        }

        // then just push current char
        result.push(*b);
    }

    if escaping {
        return (&val[val.len()..], result);
    } else {
        // didn't finish, it means the bytes is a valid key bytes
        // panic now
        panic!("panic now");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_round_trip() {
        for v in [
            &b"part1|hello".to_vec(),
            &b"".to_vec(),
            &b"1".to_vec(),
            &b"|".to_vec(),
            &b"||".to_vec(),
            &b"|||".to_vec(),
            &b"||||||".to_vec(),
        ] {
            let escaped = escape_bytes(v);
            let (_x, y) = parse_bytes(escaped.as_slice());
            assert_eq!(v, y.as_slice());
        }
    }

    #[test]
    fn test_key_components_round_trip() {
        for components in [
            vec![Component::F64(0f64)],
            vec![],
            vec![Component::I64(0)],
            vec![Component::F64(12.)],
            vec![Component::Bytes(b"h".to_vec())],
            vec![
                Component::Bytes(b"h".to_vec()),
                Component::Bytes(b"e".to_vec()),
                Component::Bytes(b"l".to_vec()),
                Component::Bytes(b"l".to_vec()),
                Component::Bytes(b"o".to_vec()),
                Component::Bytes(b" ".to_vec()),
                Component::Bytes(b"w".to_vec()),
                Component::Bytes(b"o".to_vec()),
                Component::Bytes(b"u".to_vec()),
                Component::Bytes(b"|".to_vec()),
                Component::Bytes(b"d".to_vec()),
                Component::I64(12231232131231232),
            ],
        ]
        .into_iter()
        {
            let key = Key::from(components.as_slice());
            assert_eq!(key.as_components(), components);
        }
    }

    #[test]
    fn test_key_order() {
        for (l, r, order) in vec![
            (
                vec![Component::F64(0f64)],
                vec![Component::F64(1f64)],
                Ordering::Less,
            ),
            (
                vec![Component::I64(123)],
                vec![Component::I64(567)],
                Ordering::Less,
            ),
            (vec![], vec![], Ordering::Equal),
            (vec![], vec![Component::I64(567)], Ordering::Less),
            (vec![Component::I64(567)], vec![], Ordering::Greater),
            (
                vec![Component::I64(567)],
                vec![Component::F64(0.)],
                Ordering::Less,
            ),
            (
                vec![Component::I64(567), Component::I64(1)],
                vec![Component::I64(567)],
                Ordering::Greater,
            ),
            (
                vec![Component::I64(567), Component::Bytes(b"".to_vec())],
                vec![Component::I64(567)],
                Ordering::Greater,
            ),
            (
                vec![Component::I64(567), Component::Bytes(b"|".to_vec())],
                vec![Component::I64(567), Component::Bytes(b"|".to_vec())],
                Ordering::Equal,
            ),
            (
                vec![Component::I64(567), Component::Bytes(b"".to_vec())],
                vec![Component::I64(567), Component::Bytes(b"|".to_vec())],
                Ordering::Less,
            ),
            (
                vec![567.into(), b"hello".to_vec().into()],
                vec![Component::I64(567), Component::Bytes(b"iello".to_vec())],
                Ordering::Less,
            ),
            (
                vec![Component::Bytes(b"ab".to_vec())],
                vec![Component::Bytes(b"a|".to_vec())],
                Ordering::Less,
            ),
            (
                vec![Component::Bytes(b"a".to_vec())],
                vec![Component::Bytes(b"a\0".to_vec())],
                Ordering::Less,
            ),
        ]
        .into_iter()
        {
            let l_str = format!("{:?}", l);
            let r_str = format!("{:?}", r);
            assert_eq!(order, l.cmp(&r));

            let l = Key::from(l.as_slice());
            let r = Key::from(r.as_slice());

            let l_bytes = dbg!(l.into_bytes());
            let r_bytes = dbg!(r.into_bytes());

            assert_eq!(order, l_bytes.cmp(&r_bytes), "{} {}", l_str, r_str);
        }
    }
}
