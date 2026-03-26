//! This file defines the `Table` type, which stores a table of database data. It also implements
//! the `Storage<DbVal>` and `StorageMut<DbVal>` traits for `Table`, allowing us to get and set
//! values within the table through this trait interface. Furthermore, we provide an implementation
//! of `Display`, which makes it convenient to view the table's contents via the `println!()`
//! macro.

pub mod csv;
pub mod iter;
pub mod select;
pub mod view;

use crate::{
    col::Col,
    storage::{Storage, StorageMut},
    types::{ColId, DbMut, DbRef, DbType, DbVal, RowId},
};

use std::{
    collections::HashMap,
    fmt::{self, Display, Formatter},
};

/// A database table, which consists of a series of columns with names, ids, and types. The columns
/// are all of the same height, which lets us define a canonical `RowId` used to access a single
/// row of the table.
///
/// On the implementation side, we store four different `Vec<Col<T>>` for each specific database
/// type (`String`, `i64`, `bool`, and `f64`). This is done to force the memory layouts to be
/// efficient (as opposed to storing a `Vec<Col<DbVal>>`, which is storing a `Vec` of enums,
/// wasting space for the tag byte).
///
/// Each column has a name, so we have a map from column names to ids to make the user-facing
/// interface nicer to use. We also keep track of the "next index" for each data type, which
/// ensures we assign a unique (index, type) pair for each `ColId`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Table {
    id_map: HashMap<String, ColId>,
    next_row_idx: usize,

    next_string_idx: usize,
    next_integer_idx: usize,
    next_boolean_idx: usize,
    next_double_idx: usize,

    strings: Vec<Col<String>>,
    integers: Vec<Col<i64>>,
    booleans: Vec<Col<bool>>,
    doubles: Vec<Col<f64>>,
}

impl Table {
    /// Returns the total number of rows in the table.
    pub fn num_rows(&self) -> usize {
        self.next_row_idx
    }

    /// Adds a row of null values to the table.
    pub fn add_row(&mut self) -> RowId {
        let idx = self.next_row_idx;
        self.next_row_idx += 1;
        RowId { idx }
    }

    /// Gets a row from the table by its index (which is unstable!).
    pub fn get_row_by_index(&self, idx: usize) -> Option<RowId> {
        if idx < self.next_row_idx {
            Some(RowId { idx })
        } else {
            None
        }
    }

    /// Returns the total number of columns in the table.
    pub fn num_cols(&self) -> usize {
        self.id_map.len()
    }

    /// Returns the ID associated with a column name.
    pub fn col_id(&self, name: &str) -> Option<ColId> {
        self.id_map.get(name).copied()
    }

    /// Returns the type associated with a column name.
    pub fn col_type(&self, name: &str) -> Option<DbType> {
        self.id_map.get(name).map(|id| id.ty)
    }

    /// Adds a column to the table with the given name and type.
    pub fn add_col(&mut self, name: &str, ty: DbType) -> ColId {
        if let Some(id) = self.id_map.get(name) {
            if id.ty != ty {
                panic!("type mismatch when getting database col");
            }
            return *id;
        }

        let id = match ty {
            DbType::String => {
                let idx = self.next_string_idx;
                self.next_string_idx += 1;
                let id = ColId { idx, ty };
                self.strings.push(Col::new(id, name.to_owned()));
                id
            }
            DbType::Integer => {
                let idx = self.next_integer_idx;
                self.next_integer_idx += 1;
                let id = ColId { idx, ty };
                self.integers.push(Col::new(id, name.to_owned()));
                id
            }
            DbType::Boolean => {
                let idx = self.next_boolean_idx;
                self.next_boolean_idx += 1;
                let id = ColId { idx, ty };
                self.booleans.push(Col::new(id, name.to_owned()));
                id
            }
            DbType::Double => {
                let idx = self.next_double_idx;
                self.next_double_idx += 1;
                let id = ColId { idx, ty };
                self.doubles.push(Col::new(id, name.to_owned()));
                id
            }
        };

        self.id_map.insert(name.to_owned(), id);
        id
    }
}

// STEP 5: Implement the `Storage<DbVal>` and `StorageMut<DbVal>` traits for `Table`.
//
// We're providing the `Id` and `Ref` types here for you this time.
//
// HINT: These implementations should be relatively straightforward if you implemented the previous
// step correctly.

impl Storage<DbVal> for Table {
    type Id = (RowId, ColId);
    type Ref<'a> = DbRef<'a>;

    fn get<'a>(&'a self, id: (RowId, ColId)) -> Option<DbRef<'a>> {
        unimplemented!()
    }
}

impl StorageMut<DbVal> for Table {
    type RefMut<'a> = DbMut<'a>;

    fn get_mut<'a>(&'a mut self, id: (RowId, ColId)) -> Option<DbMut<'a>> {
        unimplemented!()
    }

    fn put(&mut self, id: (RowId, ColId), val: impl Into<DbVal>) -> Option<DbVal> {
        unimplemented!()
    }

    fn take(&mut self, id: (RowId, ColId)) -> Option<DbVal> {
        unimplemented!()
    }
}

// Below is the implementation of the `Display` trait for `Table`. You can view a table in this way
// with the `print` command in table mode of the REPL.

impl Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.next_row_idx == 0 && self.id_map.is_empty() {
            return writeln!(f, "<empty table>");
        } else if self.next_row_idx == 0 {
            return writeln!(f, "<no rows>");
        } else if self.id_map.is_empty() {
            return writeln!(f, "<no cols>");
        }

        let mut widths = Vec::new();
        for (name, col_id) in &self.id_map {
            let mut max_width = name.len();

            for row_idx in 0..self.next_row_idx {
                let row = RowId { idx: row_idx };

                let cell_str = match self.get((row, *col_id)) {
                    Some(val) => val.to_owned().to_string(),
                    None => "NULL".to_string(),
                };

                max_width = max_width.max(cell_str.len());
            }

            widths.push(max_width);
        }

        write!(f, " | ")?;
        for ((name, _), width) in self.id_map.iter().zip(&widths) {
            write!(f, "{:width$} | ", name, width = width)?;
        }
        writeln!(f)?;

        write!(f, "-+-")?;
        for width in &widths {
            write!(f, "{}-+-", "-".repeat(*width))?;
        }
        writeln!(f)?;

        for row_idx in 0..self.next_row_idx {
            let row = RowId { idx: row_idx };

            write!(f, " | ")?;
            for ((_, col_id), width) in self.id_map.iter().zip(&widths) {
                let cell_str = match self.get((row, *col_id)) {
                    Some(val) => val.to_owned().to_string(),
                    None => "NULL".to_string(),
                };

                write!(f, "{:width$} | ", cell_str, width = width)?;
            }

            writeln!(f)?;
        }

        Ok(())
    }
}
