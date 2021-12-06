use super::*;
use vdb_key::{Component, Key};
use vdb_value::Value;

#[test]
fn test_table_create_and_delete() {
    let mut table = Table::new("test_table".to_string());
    table.append_observer(Box::new(|event: TableEvent<'_>| {
        println!("{:?}", event);
    }));

    let mut conn = rusqlite::Connection::open_in_memory().unwrap();
    table.create_table(&conn).unwrap();

    let v = table
        .insert(&mut conn, b"abc".to_vec(), b"def".to_vec())
        .unwrap();
    assert_eq!(
        table.get(&mut conn, b"abc").unwrap().unwrap(),
        (b"def".to_vec(), v)
    );

    let v2 = table
        .insert(&mut conn, b"abc".to_vec(), b"foo".to_vec())
        .unwrap();
    assert_eq!(
        table.get(&mut conn, b"abc").unwrap().unwrap(),
        (b"foo".to_vec(), v2)
    );

    // try to delete with prev v, should return error
    assert_eq!(
        table
            .delete_with_version(&mut conn, b"abc".to_vec(), dbg!(v))
            .unwrap(),
        0
    );
    assert_eq!(
        table.get(&mut conn, b"abc").unwrap().unwrap(),
        (b"foo".to_vec(), v2)
    );

    assert_eq!(
        table
            .delete_with_version(&mut conn, b"abc".to_vec(), v2)
            .unwrap(),
        3
    );

    assert!(table.get(&mut conn, b"abc").unwrap().is_none());
}

#[test]
fn test_update() {
    let mut table = Table::new("test_table".to_string());
    let mut conn = rusqlite::Connection::open_in_memory().unwrap();

    table.append_observer(Box::new(|event: TableEvent<'_>| {
        println!("{:?}", event);
    }));

    table.create_table(&conn).unwrap();

    let new_v = table
        .update(
            &mut conn,
            b"abc".to_vec(),
            Box::new(|_prev| Ok(UpdateResult::Update(b"foo".to_vec()))),
        )
        .unwrap();
    assert_eq!(new_v.unwrap(), 1);

    let new_v = table
        .update(
            &mut conn,
            b"abc".to_vec(),
            Box::new(|prev| {
                let (prev_body, prev_version) = prev.unwrap();
                assert_eq!(prev_version, 1);
                assert_eq!(prev_body, b"foo".to_vec());
                Ok(UpdateResult::NotChange)
            }),
        )
        .unwrap();
    assert!(new_v.is_none());

    let new_v = table
        .update(
            &mut conn,
            b"abc".to_vec(),
            Box::new(|prev| {
                let (_prev_body, _prev_version) = prev.unwrap();
                Ok(UpdateResult::Update(b"foo new".to_vec()))
            }),
        )
        .unwrap();
    assert!(new_v.is_some());
}

#[test]
fn test_index_create() {
    let mut conn = rusqlite::Connection::open("test_db.sqlite").unwrap();
    // let mut conn = rusqlite::Connection::open_in_memory().unwrap();
    let mut table = create_test_table(&conn);
    for i in 0..4 {
        let new_v = table
            .update(
                &mut conn,
                b"test_key".to_vec(),
                Box::new(|prev| {
                    Ok(match prev {
                        None => UpdateResult::Update(
                            TestModel {
                                val_1: 123,
                                val_2: 321.,
                            }
                            .to_vec(),
                        ),
                        Some((prev_val, _prev_version)) => {
                            let model = TestModel::from_slice(prev_val.as_slice())?;
                            UpdateResult::Update(
                                TestModel {
                                    val_1: model.val_1 + 1,
                                    val_2: model.val_2 + 2.,
                                }
                                .to_vec(),
                            )
                        }
                    })
                }),
            )
            .unwrap();

        assert_eq!(new_v.unwrap(), i + 1);

        dbg!(table
            .get_by_index(
                &conn,
                "test_index",
                Key::from(Component::from(123 * 100))
                    .into_bytes()
                    .as_slice(),
                100,
            )
            .unwrap());
    }

    table.append_index(
        "another_index",
        Box::new(|_key, val| {
            let model = TestModel::from_slice(val)?;
            let mut key = vdb_key::Key::new();
            key.append_i64(model.val_1 * 1000);
            Ok(vec![key.into_bytes()])
        }),
    );
    table.create_table(&conn).unwrap();
    // now assert index also updated
}

#[derive(Value, Default)]
struct TestModel {
    #[vdb_value(index = 1)]
    val_1: i64,

    #[vdb_value(index = 2)]
    val_2: f64,
}

impl TableItem for TestModel {
    type PrimaryKey = i64;

    fn primary_key(&self) -> Self::PrimaryKey {
        self.val_1
    }
}

fn create_test_table(conn: &rusqlite::Connection) -> Table {
    let mut table = Table::new("test_table".to_string());
    table.append_index(
        "test_index",
        Box::new(|_key: &[u8], val: &[u8]| {
            let model = TestModel::from_slice(val)?;
            let mut key = vdb_key::Key::new();
            key.append_i64(model.val_1 * 100);
            Ok(vec![key.into_bytes()])
        }),
    );
    table.create_table(&conn).unwrap();
    table
}

#[test]
fn test_table_typed() {
    // let mut conn = rusqlite::Connection::open("test_db.sqlite").unwrap();
    let mut conn = rusqlite::Connection::open_in_memory().unwrap();
    let mut table = TypedTable::<TestModel>::new("test_table");
    table.append_index("test_index", |pk, _item| vec![pk * 100]);
    table.create_table(&conn).unwrap();

    for i in 0..10i64 {
        table
            .insert(
                &mut conn,
                &TestModel {
                    val_1: i,
                    val_2: i as f64,
                },
            )
            .unwrap();
    }
}

#[test]
fn test_derive() {
    use vdb_value::Value;

    #[derive(Value, Default, Debug, PartialOrd, PartialEq)]
    struct TestModel {
        #[vdb_value(index = 1)]
        val_1: i64,

        #[vdb_value(index = 2)]
        val_2: f64,
    }

    let model = TestModel {
        val_1: 12345,
        val_2: 123.,
    };

    let buf = dbg!(model.to_vec());
    let back_model = TestModel::from_slice(&buf).unwrap();

    assert_eq!(back_model, model);
}
