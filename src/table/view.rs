//! This file defines "view" types to deal with rows and columns of tables in a nice way.
//! Specifically, we provide the `RowRef<'a>` and `RowMut<'a>` for dealing with columns immutably
//! and mutably, respectively, and we provide the `ColRef<'a>` and `ColMut<'a>` for dealing with
//! columns immutably and mutably, respectively.
//!
//! Notably, the `ColMut<'a>` type is constructed in such a way that permits mutable iteration over
//! all columns simultaneously. However, `RowMut<'a>` requires a mutable reference to `Table`,
//! which prevents overlapping instances of `RowMut<'a>` from being constructed. Although it is
//! theoretically possible to do this safely, since tables are stored in column-major format, the
//! Rust compiler rejects such an implementation.

use crate::{
    col::Col,
    storage::{Storage, StorageMut},
    table::Table,
    types::{ColId, DbMut, DbRef, DbType, DbVal, RowId},
};

/// A view of a row within a table of lifetime `'a`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RowRef<'a> {
    pub table: &'a Table,
    pub id: RowId,
}

/// A mutable view of a row within a table of lifetime `'a`.
#[derive(Debug, PartialEq)]
pub struct RowMut<'a> {
    pub table: &'a mut Table,
    pub id: RowId,
}

/// A view of a column within a table of lifetime `'a`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColRef<'a> {
    String(&'a Col<String>),
    Integer(&'a Col<i64>),
    Boolean(&'a Col<bool>),
    Double(&'a Col<f64>),
}

/// A mutable view of a column within a table of lifetime `'a`.
#[derive(Debug, PartialEq)]
pub enum ColMut<'a> {
    String(&'a mut Col<String>),
    Integer(&'a mut Col<i64>),
    Boolean(&'a mut Col<bool>),
    Double(&'a mut Col<f64>),
}

impl Table {
    /// Projects a row of the table with the given ID immutably into a view of lifetime `'a`.
    pub fn row<'a>(&'a self, id: RowId) -> RowRef<'a> {
        RowRef { table: self, id }
    }

    /// Projects a row of the table with the given ID immutably into a mutable view of lifetime `'a`.
    pub fn row_mut<'a>(&'a mut self, id: RowId) -> RowMut<'a> {
        RowMut { table: self, id }
    }

    /// Projects a column of the table with the given ID immutably into a view of lifetime `'a`.
    pub fn col<'a>(&'a self, id: ColId) -> ColRef<'a> {
        match id.ty {
            DbType::String => ColRef::String(&self.strings[id.idx]),
            DbType::Integer => ColRef::Integer(&self.integers[id.idx]),
            DbType::Boolean => ColRef::Boolean(&self.booleans[id.idx]),
            DbType::Double => ColRef::Double(&self.doubles[id.idx]),
        }
    }
    /// Projects a column of the table with the given ID immutably into a mutable view of lifetime `'a`.
    pub fn col_mut<'a>(&'a mut self, id: ColId) -> ColMut<'a> {
        match id.ty {
            DbType::String => ColMut::String(&mut self.strings[id.idx]),
            DbType::Integer => ColMut::Integer(&mut self.integers[id.idx]),
            DbType::Boolean => ColMut::Boolean(&mut self.booleans[id.idx]),
            DbType::Double => ColMut::Double(&mut self.doubles[id.idx]),
        }
    }
}

impl<'a> ColRef<'a> {
    /// Returns the ID associated with this column.
    pub fn id(&self) -> ColId {
        match self {
            ColRef::String(col) => col.id(),
            ColRef::Integer(col) => col.id(),
            ColRef::Boolean(col) => col.id(),
            ColRef::Double(col) => col.id(),
        }
    }

    /// Returns name ID associated with this column.
    pub fn name(&self) -> &str {
        match self {
            ColRef::String(col) => col.name(),
            ColRef::Integer(col) => col.name(),
            ColRef::Boolean(col) => col.name(),
            ColRef::Double(col) => col.name(),
        }
    }
}
impl<'a> ColMut<'a> {
    /// Returns the ID associated with this column.
    pub fn id(&self) -> ColId {
        match self {
            ColMut::String(col) => col.id(),
            ColMut::Integer(col) => col.id(),
            ColMut::Boolean(col) => col.id(),
            ColMut::Double(col) => col.id(),
        }
    }

    /// Returns name ID associated with this column.
    pub fn name(&self) -> &str {
        match self {
            ColMut::String(col) => col.name(),
            ColMut::Integer(col) => col.name(),
            ColMut::Boolean(col) => col.name(),
            ColMut::Double(col) => col.name(),
        }
    }
}

// Below we implement the `Storage<DbVal>` and `StorageMut<DbVal>` for our row and column view
// types. Feel free to look at these implementations.

// STEP 6: Implement the `Storage<DbVal>` and `StorageMut<DbVal>` traits for `RowRef<'a>`,
// `RowMut<'a>`, `ColRef<'a>`, and `ColMut<'a>` types. This step may involve a lot of pattern
// matching!
//
// Hint: you may need to use the syntax `self.table.get((self.id, id))` or similar, since the
// function expects a tuple as a "single" argument in this case.

impl<'a> Storage<DbVal> for RowRef<'a> {
    type Id = ColId;

    type Ref<'b>
        = DbRef<'b>
    where
        Self: 'b;

    fn get<'b>(&'b self, id: Self::Id) -> Option<Self::Ref<'b>> {
        unimplemented!();
    }
}

impl<'a> Storage<DbVal> for RowMut<'a> {
    type Id = ColId;

    type Ref<'b>
        = DbRef<'b>
    where
        Self: 'b;

    fn get<'b>(&'b self, id: Self::Id) -> Option<Self::Ref<'b>> {
        unimplemented!();
    }
}

impl<'a> StorageMut<DbVal> for RowMut<'a> {
    type RefMut<'b>
        = DbMut<'b>
    where
        Self: 'b;

    fn get_mut<'b>(&'b mut self, id: Self::Id) -> Option<Self::RefMut<'b>> {
        unimplemented!();
    }

    fn put(&mut self, id: Self::Id, val: impl Into<DbVal>) -> Option<DbVal> {
        unimplemented!();
    }

    fn take(&mut self, id: Self::Id) -> Option<DbVal> {
        unimplemented!();
    }
}

impl<'a> Storage<DbVal> for ColRef<'a> {
    type Id = RowId;
    type Ref<'b>
        = DbRef<'b>
    where
        Self: 'b;

    fn get<'b>(&'b self, row_id: RowId) -> Option<Self::Ref<'b>> {
        unimplemented!()
    }
}

impl<'a> Storage<DbVal> for ColMut<'a> {
    type Id = RowId;
    type Ref<'b>
        = DbRef<'b>
    where
        Self: 'b;

    fn get<'b>(&'b self, row_id: RowId) -> Option<Self::Ref<'b>> {
        unimplemented!()
    }
}

impl<'a> StorageMut<DbVal> for ColMut<'a> {
    type RefMut<'b>
        = DbMut<'b>
    where
        Self: 'b;

    fn get_mut<'b>(&'b mut self, row_id: RowId) -> Option<Self::RefMut<'b>> {
        unimplemented!()
    }

    fn put(&mut self, row_id: RowId, val: impl Into<DbVal>) -> Option<DbVal> {
        // Note: you will need to do a runtime type check here. If the caller provides the
        // wrong type, simply `panic!()` with an appropriate error message. A useful idiom is to
        // pattern-match on the tuple `(self, val.into())` and use the "fallback" case `_ => {...}`.
        unimplemented!()
    }

    fn take(&mut self, row_id: RowId) -> Option<DbVal> {
        unimplemented!()
    }
}
