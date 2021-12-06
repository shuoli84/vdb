use super::{InputProtocol, OutProtocol, Ty};
use crate::{try_u8_to_ty, DynamicStruct, DynamicValue, Error, Value};
use rand::{thread_rng, Rng};

#[test]
fn test_input_output() {
    let mut buffer = Vec::new();
    {
        let mut out = OutProtocol::new(&mut buffer);
        out.write_field_header(Ty::I64, 1);
        out.write_i64(1234);

        out.write_field_header(Ty::List, 2);
        out.write_list_header(Ty::I64, 2);
        out.write_i64(333);
        out.write_i64(444);

        out.write_stop();
    }

    {
        let mut input = InputProtocol::new(&buffer);
        let (ty, index) = input.read_field_header().unwrap();
        assert_eq!(ty, Ty::I64);
        assert_eq!(index, 1);
        let value = input.read_i64().unwrap();
        assert_eq!(value, 1234);

        let (ty, index) = input.read_field_header().unwrap();
        assert_eq!(ty, Ty::List);
        assert_eq!(index, 2);
        let (item_ty, size) = input.read_list_header().unwrap();
        assert_eq!(item_ty, Ty::I64);
        assert_eq!(size, 2);
        assert_eq!(input.read_i64().unwrap(), 333);
        assert_eq!(input.read_i64().unwrap(), 444);

        assert!(input.read_non_stop_field().unwrap().is_none());
    }
}

fn rand_ty(allow_container: bool) -> Ty {
    loop {
        let ty = try_u8_to_ty(rand::thread_rng().gen_range(1..6u8)).unwrap();
        match ty {
            Ty::Any | Ty::Stop => {
                unreachable!()
            }
            Ty::List | Ty::Struct => {
                if allow_container {
                    return ty;
                }
                continue;
            }
            _ => {
                return ty;
            }
        }
    }
}

fn rand_struct(depth: u8) -> DynamicStruct {
    let mut st = DynamicStruct::default();

    let field_num = thread_rng().gen_range(0..2);
    (0..field_num).for_each(|field_idx| {
        let field_ty = rand_ty(depth < 10);
        let value = rand_value_for_ty(field_ty, depth + 1);
        st.insert(field_idx, value);
    });

    st
}

fn rand_value_for_ty(ty: Ty, depth: u8) -> DynamicValue {
    // let (ty, depth) = dbg!((ty, depth));
    match ty {
        Ty::Any => {
            unreachable!()
        }
        Ty::I64 => DynamicValue::I64(thread_rng().gen_range(0..i64::MAX)),
        Ty::F64 => DynamicValue::F64(thread_rng().gen_range(0f64..f64::MAX)),
        Ty::Bytes => {
            let len = thread_rng().gen_range(0..10usize);
            let random_bytes: Vec<u8> = (0..len).map(|_| rand::random::<u8>()).collect();
            DynamicValue::Bytes(Box::new(random_bytes))
        }
        Ty::List => {
            let item_ty = rand_ty(depth < 10);
            let item_size = thread_rng().gen_range(0..32);
            let items = (0..item_size)
                .map(|_| rand_value_for_ty(item_ty, depth + 1))
                .collect::<Vec<_>>();
            DynamicValue::List {
                item_ty,
                items: Box::new(items),
            }
        }
        Ty::Struct => DynamicValue::Struct(Box::new(rand_struct(depth + 1))),
        Ty::Stop => {
            unreachable!()
        }
    }
}

#[test]
fn fuzz_dynamic_value() {
    use super::DynamicStruct;

    for i in 0..30 {
        println!("processing {}", i);
        let st = rand_struct(0);

        let buffer = st.to_vec();
        let st_back = DynamicStruct::from_slice(&buffer).unwrap();
        let buffer_again = st_back.to_vec();
        assert_eq!(buffer_again, buffer);

        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

#[test]
fn manual_impl_value() {
    #[derive(Default, Debug, Ord, PartialOrd, Eq, PartialEq)]
    struct TestModel {
        val_1: i64,
        val_s: Vec<u8>,
    }

    impl Value for TestModel {
        fn ty(&self) -> Ty {
            Ty::Struct
        }

        fn from_input(&mut self, input: &mut InputProtocol<'_>) -> Result<(), Error> {
            while let Some((ty, index)) = dbg!(input.read_non_stop_field()?) {
                match index {
                    0 => {
                        self.val_1.from_input(input)?;
                    }
                    1 => {
                        self.val_s.from_input(input)?;
                    }
                    _ => {
                        input.skip_field(ty)?;
                    }
                }
            }
            Ok(())
        }

        fn to_output(&self, output: &mut OutProtocol<'_>) {
            output.write_field_header(Ty::I64, 0);
            self.val_1.to_output(output);

            output.write_field_header(Ty::Bytes, 1);
            self.val_s.to_output(output);

            output.write_stop();
        }
    }

    let mut model = TestModel {
        val_1: 12345,
        val_s: b"foo bar".to_vec(),
    };

    let buf = dbg!(model.to_vec());
    let back_model = TestModel::from_slice(&buf).unwrap();

    assert_eq!(back_model, model);
}
