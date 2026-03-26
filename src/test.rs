//! Testing module.
//!
//! Feel free to add your own tests here! Functions marked `#[test]` will be executed by running
//! `cargo test` in the command line. You may also define normal functions as helpers if you like.

#[allow(unused_imports)]
use crate::{
    col::*,
    storage::*,
    table::{iter::*, select::*, *},
    types::*,
};

#[test]
fn dbref_and_dbmut_roundtrip() {
    let mut v = DbVal::String("hello".to_owned());
    let r = v.as_ref();
    assert_eq!(r.to_owned(), DbVal::String("hello".to_owned()));

    if let DbMut::String(s) = v.as_mut() {
        s.push_str(" world");
    } else {
        panic!("expected string dbmut");
    }
    assert_eq!(v, DbVal::String("hello world".to_owned()));
}

#[test]
fn col_storage_put_get_take() {
    let mut col = Col::<i64>::new(
        ColId {
            idx: 0,
            ty: DbType::Integer,
        },
        "age",
    );
    let row = RowId { idx: 2 };

    assert_eq!(col.get(row), None);
    assert_eq!(col.put(row, 42), None);
    assert_eq!(col.get(row), Some(&42));
    assert_eq!(col.put(row, 99), Some(42));
    assert_eq!(col.take(row), Some(99));
    assert_eq!(col.get(row), None);
}

fn sample_table() -> (Table, ColId, ColId, ColId) {
    let mut table = Table::default();
    let name = table.add_col("name", DbType::String);
    let score = table.add_col("score", DbType::Double);
    let active = table.add_col("active", DbType::Boolean);

    let r0 = table.add_row();
    let r1 = table.add_row();
    let r2 = table.add_row();

    table.put((r0, name), "alice");
    table.put((r1, name), "bob");
    table.put((r2, name), "carol");
    table.put((r0, score), 9.5);
    table.put((r1, score), 7.25);
    table.put((r0, active), true);
    table.put((r1, active), false);

    (table, name, score, active)
}

#[test]
fn table_iterators_cover_rows_and_columns() {
    let (table, _name, _score, _active) = sample_table();
    assert_eq!(table.iter_rows().count(), 3);
    assert_eq!(table.iter_cols().count(), 3);
}

#[test]
fn select_filter_and_order_work() {
    let (table, name, score, _active) = sample_table();

    let view = table.select(
        Select::new(vec![name, score])
            .filter(Filter::ge(score, 7.25))
            .order_by(score, Order::Descending),
    );

    let rows: Vec<RowId> = view.iter_row_ids().collect();
    assert_eq!(rows.len(), 2);

    let top_name = table.get((rows[0], name)).unwrap().to_owned().into_string();
    let second_name = table.get((rows[1], name)).unwrap().to_owned().into_string();
    assert_eq!(top_name, "alice");
    assert_eq!(second_name, "bob");
}

#[test]
fn select_null_and_boolean_logic() {
    let (table, _name, score, active) = sample_table();
    let view = table
        .select(Select::all(&table).filter(Filter::is_null(score).or(Filter::eq(active, true))));
    let rows: Vec<RowId> = view.iter_row_ids().collect();
    assert_eq!(rows.len(), 2);
}

#[test]
fn table_view_mut_iter_cols_mut_updates_data() {
    let (mut table, _name, score, _active) = sample_table();
    let mut view = table.select_mut(Select::new(vec![score]));

    for mut col in view.iter_cols_mut() {
        for cell in col.iter_mut() {
            if let Some(DbMut::Double(v)) = cell {
                *v += 1.0;
            }
        }
    }

    let updated: Vec<f64> = table
        .iter_row_ids()
        .filter_map(|r| table.get((r, score)).map(|v| v.to_owned().into_double()))
        .collect();
    assert_eq!(updated, vec![10.5, 8.25]);
}

fn load_dummy() -> Table {
    Table::from_csv("dummy.csv")
        .expect("dummy.csv should load")
        .0
}

fn collect_strings(table: &Table, view: &impl TableIter, col: ColId) -> Vec<Option<String>> {
    view.iter_row_ids()
        .map(|row| {
            table
                .get((row, col))
                .map(|v| v.to_owned().into_string())
        })
        .collect()
}

#[test]
fn select_single() {
    let table = load_dummy();
    let product_id = table.col_id("product_id").unwrap();
    let view = table.select(Select::new(vec![product_id]));
    assert_eq!(view.iter_col_ids().collect::<Vec<_>>(), vec![product_id]);
    assert_eq!(
        collect_strings(&table, &view, product_id),
        vec![
            Some("P001".to_owned()),
            Some("P002".to_owned()),
            Some("P003".to_owned()),
            Some("P004".to_owned()),
            Some("P005".to_owned()),
            Some("P006".to_owned()),
            Some("P007".to_owned()),
        ]
    );
}

#[test]
fn select_filter_eq_ne_null() {
    let table = load_dummy();
    let category = table.col_id("category").unwrap();
    let eq_view = table.select(Select::all(&table).filter(Filter::eq(category, "Electronics")));
    let ne_view = table.select(Select::all(&table).filter(Filter::ne(category, "Electronics")));
    let null_view = table.select(Select::all(&table).filter(Filter::is_null(category)));
    let non_null_view = table.select(Select::all(&table).filter(Filter::is_non_null(category)));

    assert_eq!(eq_view.iter_row_ids().count(), 2);
    assert_eq!(ne_view.iter_row_ids().count(), 5);
    assert_eq!(null_view.iter_row_ids().count(), 1);
    assert_eq!(non_null_view.iter_row_ids().count(), 6);
}

#[test]
fn select_filter_order_numeric_comparisons() {
    let table = load_dummy();
    let rating = table.col_id("rating").unwrap();
    let product_id = table.col_id("product_id").unwrap();

    let gt = table.select(Select::new(vec![product_id]).filter(Filter::gt(rating, 4.5)));
    let ge = table.select(Select::new(vec![product_id]).filter(Filter::ge(rating, 4.5)));
    let lt = table.select(Select::new(vec![product_id]).filter(Filter::lt(rating, 4.5)));
    let le = table.select(Select::new(vec![product_id]).filter(Filter::le(rating, 4.5)));

    assert_eq!(gt.iter_row_ids().count(), 3);
    assert_eq!(ge.iter_row_ids().count(), 4);
    assert_eq!(lt.iter_row_ids().count(), 3);
    assert_eq!(le.iter_row_ids().count(), 3);
}

#[test]
fn select_filter_many_and_ordering() {
    let table = load_dummy();
    let in_stock = table.col_id("in_stock").unwrap();
    let rating = table.col_id("rating").unwrap();
    let price = table.col_id("price").unwrap();
    let product_id = table.col_id("product_id").unwrap();

    let view = table.select(
        Select::new(vec![product_id, rating, price])
            .filter(Filter::eq(in_stock, true).and(Filter::ge(rating, 4.5)))
            .order_by(price, Order::Ascending),
    );

    assert_eq!(
        collect_strings(&table, &view, product_id),
        vec![
            Some("P005".to_owned()),
            Some("P002".to_owned()),
            Some("P001".to_owned()),
            Some("P004".to_owned()),
        ]
    );
}

#[test]
fn select_multiple_orderings() {
    let mut table = Table::default();
    let s = table.add_col("s", DbType::String);
    let i = table.add_col("i", DbType::Integer);

    let r0 = table.add_row();
    let r1 = table.add_row();
    let r2 = table.add_row();
    let r3 = table.add_row();
    table.put((r0, s), "b");
    table.put((r0, i), 1);
    table.put((r1, s), "a");
    table.put((r1, i), 2);
    table.put((r2, s), "b");
    table.put((r2, i), 2);
    table.put((r3, s), "a");
    table.put((r3, i), 1);

    let view = table.select(
        Select::new(vec![s, i])
            .order_by(s, Order::Ascending)
            .order_by(i, Order::Descending),
    );

    let rows: Vec<(String, i64)> = view
        .iter_row_ids()
        .map(|r| {
            let sv = table.get((r, s)).unwrap().to_owned().into_string();
            let iv = table.get((r, i)).unwrap().to_owned().into_integer();
            (sv, iv)
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            ("a".to_owned(), 2),
            ("a".to_owned(), 1),
            ("b".to_owned(), 2),
            ("b".to_owned(), 1),
        ]
    );
}

#[test]
fn select_order_each_type() {
    let table = load_dummy();
    let in_stock = table.col_id("in_stock").unwrap();
    let category = table.col_id("category").unwrap();
    let rating = table.col_id("rating").unwrap();
    let product_id = table.col_id("product_id").unwrap();

    let bool_asc = table.select(Select::new(vec![product_id]).order_by(in_stock, Order::Ascending));
    let bool_desc =
        table.select(Select::new(vec![product_id]).order_by(in_stock, Order::Descending));
    assert_ne!(
        collect_strings(&table, &bool_asc, product_id),
        collect_strings(&table, &bool_desc, product_id)
    );

    let str_asc = table.select(Select::new(vec![product_id]).order_by(category, Order::Ascending));
    let str_desc = table.select(Select::new(vec![product_id]).order_by(category, Order::Descending));
    assert_ne!(
        collect_strings(&table, &str_asc, product_id),
        collect_strings(&table, &str_desc, product_id)
    );

    let dbl_asc = table.select(Select::new(vec![product_id]).order_by(rating, Order::Ascending));
    let dbl_desc = table.select(Select::new(vec![product_id]).order_by(rating, Order::Descending));
    assert_ne!(
        collect_strings(&table, &dbl_asc, product_id),
        collect_strings(&table, &dbl_desc, product_id)
    );
}
