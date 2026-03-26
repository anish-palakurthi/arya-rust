//! This file defines iterator implementations and helper functions, including the `TableIter`
//! and `TableIterMut` traits, as well as `iter` functions for table row/column view types.

use crate::{
    storage::Storage,
    table::{
        Table,
        view::{ColMut, ColRef, RowMut, RowRef},
    },
    types::{ColId, DbMut, DbRef, RowId},
};

/// Defines iterator helper functions for a table. This lets us iterate over logical rows and
/// columns within a table. See the `table/view.rs` file for the implementations of the
/// `RowRef<'a>`, `RowMut<'a>`, `ColRef<'a>` and `ColMut<'a>` types.
///
/// Note the use of the `impl Trait` syntax being used in these functions. You may remember from
/// lecture that we talked about this syntax being equivalent to making our function generic over
/// the `impl Trait` type. However in this case we're using the `impl Trait` as a return value
/// ("output type"), so it's more like if we'd defined an associated type which implements the
/// `Iterator` trait.
pub trait TableIter {
    /// Iterates over all rows within this table.
    fn iter_rows<'a>(&'a self) -> impl Iterator<Item = RowRef<'a>>;

    /// Iterates over all columns within this table.
    fn iter_cols<'a>(&'a self) -> impl Iterator<Item = ColRef<'a>>;

    // Note: the below functions are given a "default" implementation, so implementors of
    // `TableIter` don't need to specify them (but may choose to override them if they wish).

    /// Iterate over all row IDs within this table.
    fn iter_row_ids(&self) -> impl Iterator<Item = RowId> {
        self.iter_rows().map(|row| row.id)
    }

    /// Iterate over all column IDs within this table.
    fn iter_col_ids(&self) -> impl Iterator<Item = ColId> {
        self.iter_cols().map(|row| row.id())
    }
}

/// This trait is a subtrait of the `TableIter` trait, but also exposes a mutable iterator over the
/// columns. We don't support mutable iteration over rows due to limitations of Rust's borrow
/// checker.
pub trait TableIterMut: TableIter {
    /// Iterates mutably over all columns within this table.
    fn iter_cols_mut<'a>(&'a mut self) -> impl Iterator<Item = ColMut<'a>>;
}

// STEP 7: Implement the `TableIter` trait for `Table`.
//
// HINT: You may find the following iterator features useful:
//
// - The range syntax `(begin..end)` is itself an iterator.
// - Use `iter.map(|val| ...)` to transform values within an iterator.
// - Use `iter.chain(other_iter)` to produce an iterator that runs one after another.
//
// If you're unsure how to use iterators, you may find chapter 13 of the Rust book useful. Also
// feel free to ask a question on Ed.

impl TableIter for Table {
    fn iter_rows<'a>(&'a self) -> impl Iterator<Item = RowRef<'a>> {
        (0..self.num_rows()).map(|idx| RowRef {
            table: self,
            id: RowId { idx },
        })
    }
    fn iter_cols<'a>(&'a self) -> impl Iterator<Item = ColRef<'a>> {
        let col_ids: Vec<ColId> = self.id_map.values().copied().collect();
        col_ids.into_iter().map(|id| self.col(id))
    }
}

impl TableIterMut for Table {
    fn iter_cols_mut<'a>(&'a mut self) -> impl Iterator<Item = ColMut<'a>> {
        let strings = self.strings.iter_mut().map(ColMut::String);
        let integers = self.integers.iter_mut().map(ColMut::Integer);
        let booleans = self.booleans.iter_mut().map(ColMut::Boolean);
        let doubles = self.doubles.iter_mut().map(ColMut::Double);
        strings.chain(integers).chain(booleans).chain(doubles)
    }
}

// STEP 8: Implement the following functions on the row/column reference types.
//
// Note: we haven't discussed the `dyn` keyword in detail (or the `Box` type). Just know that
// you'll need to construct an iterator like usual, then wrap the return value in `Box::new(..)`

impl<'a> RowRef<'a> {
    pub fn iter(&'a self) -> impl Iterator<Item = (ColRef<'a>, Option<DbRef<'a>>)> {
        self.table.iter_cols().map(|col| {
            let val = self.table.get((self.id, col.id()));
            (col, val)
        })
    }
}
impl<'a> RowMut<'a> {
    pub fn iter(&'a self) -> impl Iterator<Item = (ColRef<'a>, Option<DbRef<'a>>)> {
        self.table.iter_cols().map(|col| {
            let val = self.table.get((self.id, col.id()));
            (col, val)
        })
    }
}

impl<'a> ColRef<'a> {
    pub fn iter(&'a self) -> Box<dyn Iterator<Item = Option<DbRef<'a>>> + 'a> {
        match self {
            ColRef::String(col) => Box::new(col.iter().map(|v| v.map(DbRef::String))),
            ColRef::Integer(col) => Box::new(col.iter().map(|v| v.map(DbRef::Integer))),
            ColRef::Boolean(col) => Box::new(col.iter().map(|v| v.map(DbRef::Boolean))),
            ColRef::Double(col) => Box::new(col.iter().map(|v| v.map(DbRef::Double))),
        }
    }
}
impl<'a> ColMut<'a> {
    pub fn iter(&'a self) -> Box<dyn Iterator<Item = Option<DbRef<'a>>> + 'a> {
        match self {
            ColMut::String(col) => Box::new(col.iter().map(|v| v.map(DbRef::String))),
            ColMut::Integer(col) => Box::new(col.iter().map(|v| v.map(DbRef::Integer))),
            ColMut::Boolean(col) => Box::new(col.iter().map(|v| v.map(DbRef::Boolean))),
            ColMut::Double(col) => Box::new(col.iter().map(|v| v.map(DbRef::Double))),
        }
    }

    pub fn iter_mut(&'a mut self) -> Box<dyn Iterator<Item = Option<DbMut<'a>>> + 'a> {
        match self {
            ColMut::String(col) => Box::new(col.iter_mut().map(|v| v.map(DbMut::String))),
            ColMut::Integer(col) => Box::new(col.iter_mut().map(|v| v.map(DbMut::Integer))),
            ColMut::Boolean(col) => Box::new(col.iter_mut().map(|v| v.map(DbMut::Boolean))),
            ColMut::Double(col) => Box::new(col.iter_mut().map(|v| v.map(DbMut::Double))),
        }
    }
}
