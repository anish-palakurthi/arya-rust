//! This file implements the SQL-like "select" query, which selects some columns from a table,
//! optionally filters the results, and optionally sorts the results.

use std::{
    cmp::Ordering,
    fmt::{self, Display, Formatter},
};

use crate::{
    storage::Storage,
    table::{
        Table,
        iter::{TableIter, TableIterMut},
        view::{self, RowRef},
    },
    types::{ColId, DbRef, DbVal, RowId},
};

/// A complete description of a select query.
pub struct Select {
    cols: Vec<ColId>,
    filter: Option<Filter>,
    order_by: Vec<OrderBy>,
}

/// A filter which can be applied to a select query.
#[derive(Clone, Debug)]
pub enum Filter {
    // Null-comparison filters
    IsNull(ColId),
    IsNonNull(ColId),

    // Comparison filters
    Eq(ColId, DbVal), // ==
    Ne(ColId, DbVal), // !=
    Gt(ColId, DbVal), // >
    Lt(ColId, DbVal), // <
    Ge(ColId, DbVal), // >=
    Le(ColId, DbVal), // <=

    // Note that we need to "box" these values to prevent the type from being infinitely-large. To
    // construct a `Box`, use `Box::new(..)`.
    And(Box<Filter>, Box<Filter>),
    Or(Box<Filter>, Box<Filter>),
    Xor(Box<Filter>, Box<Filter>),
    Not(Box<Filter>),
}

/// An ordering option for a select query, which sorts by the given column.
#[derive(Clone, Debug)]
pub struct OrderBy {
    col: ColId,
    order: Order,
}

/// A direction to sort in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    Ascending,
    Descending,
}

// These are helper methods for building up a select query.
impl Select {
    /// Constructs a new select query over the given columns.
    pub fn new(cols: Vec<ColId>) -> Self {
        Self {
            cols,
            filter: None,
            order_by: Vec::new(),
        }
    }

    /// Constructs a new select query over the all columns in the given table.
    pub fn all(table: &Table) -> Self {
        Self {
            cols: table.iter_col_ids().collect(),
            filter: None,
            order_by: Vec::new(),
        }
    }

    /// Filters out some data from the select query. If multiple filters are provided, takes the
    /// logical AND of the filters.
    pub fn filter(mut self, filter: Filter) -> Self {
        if let Some(old_filter) = self.filter {
            self.filter = Some(Filter::And(Box::new(old_filter), Box::new(filter)));
        } else {
            self.filter = Some(filter);
        }
        self
    }

    /// Orders the results by some column and direction. If multiple orderings are applied, sorting
    /// is applied in reverse order.
    pub fn order_by(mut self, col: ColId, order: Order) -> Self {
        self.order_by.push(OrderBy { col, order });
        self
    }
}

// Filter type builder pattern.
impl Filter {
    pub fn is_null(col: ColId) -> Self {
        Filter::IsNull(col)
    }
    pub fn is_non_null(col: ColId) -> Self {
        Filter::IsNonNull(col)
    }
    pub fn eq(col: ColId, val: impl Into<DbVal>) -> Self {
        Filter::Eq(col, val.into())
    }
    pub fn ne(col: ColId, val: impl Into<DbVal>) -> Self {
        Filter::Ne(col, val.into())
    }
    pub fn gt(col: ColId, val: impl Into<DbVal>) -> Self {
        Filter::Gt(col, val.into())
    }
    pub fn lt(col: ColId, val: impl Into<DbVal>) -> Self {
        Filter::Lt(col, val.into())
    }
    pub fn ge(col: ColId, val: impl Into<DbVal>) -> Self {
        Filter::Ge(col, val.into())
    }
    pub fn le(col: ColId, val: impl Into<DbVal>) -> Self {
        Filter::Le(col, val.into())
    }
    pub fn and(self, filter: Filter) -> Self {
        Filter::And(Box::new(self), Box::new(filter))
    }
    pub fn or(self, filter: Filter) -> Self {
        Filter::Or(Box::new(self), Box::new(filter))
    }
    pub fn xor(self, filter: Filter) -> Self {
        Filter::Xor(Box::new(self), Box::new(filter))
    }
}

// STEP 9: Implement the filter `apply` function below, which determines if a row should be
// filtered out.

impl Filter {
    /// Returns `true` if the data should be kept and `false` if it should be filtered.
    pub fn apply<'a>(&'a self, row: RowRef<'a>) -> bool {
        fn cmp_nullable(lhs: Option<DbVal>, rhs: Option<DbVal>) -> Option<Ordering> {
            match (lhs, rhs) {
                (None, None) => Some(Ordering::Equal),
                (None, Some(_)) => Some(Ordering::Less),
                (Some(_), None) => Some(Ordering::Greater),
                (Some(DbVal::String(a)), Some(DbVal::String(b))) => Some(a.cmp(&b)),
                (Some(DbVal::Integer(a)), Some(DbVal::Integer(b))) => Some(a.cmp(&b)),
                (Some(DbVal::Boolean(a)), Some(DbVal::Boolean(b))) => Some(a.cmp(&b)),
                (Some(DbVal::Double(a)), Some(DbVal::Double(b))) => a.partial_cmp(&b),
                _ => None,
            }
        }

        let get_col = |col_id: ColId| row.get(col_id).map(|v| v.to_owned());

        match self {
            Filter::IsNull(col) => get_col(*col).is_none(),
            Filter::IsNonNull(col) => get_col(*col).is_some(),
            Filter::Eq(col, val) => cmp_nullable(get_col(*col), Some(val.clone()))
                .is_some_and(|ord| ord == Ordering::Equal),
            Filter::Ne(col, val) => cmp_nullable(get_col(*col), Some(val.clone()))
                .is_some_and(|ord| ord != Ordering::Equal),
            Filter::Gt(col, val) => cmp_nullable(get_col(*col), Some(val.clone()))
                .is_some_and(|ord| ord == Ordering::Greater),
            Filter::Lt(col, val) => cmp_nullable(get_col(*col), Some(val.clone()))
                .is_some_and(|ord| ord == Ordering::Less),
            Filter::Ge(col, val) => cmp_nullable(get_col(*col), Some(val.clone()))
                .is_some_and(|ord| ord == Ordering::Greater || ord == Ordering::Equal),
            Filter::Le(col, val) => cmp_nullable(get_col(*col), Some(val.clone()))
                .is_some_and(|ord| ord == Ordering::Less || ord == Ordering::Equal),
            Filter::And(a, b) => a.apply(row) && b.apply(row),
            Filter::Or(a, b) => a.apply(row) || b.apply(row),
            Filter::Xor(a, b) => a.apply(row) ^ b.apply(row),
            Filter::Not(inner) => !inner.apply(row),
        }
    }
}

/// A "view" into a filtered table, which only has the selected rows and columns.
#[derive(Debug)]
pub struct TableView<'a> {
    table: &'a Table,
    col_ids: Vec<ColId>,
    row_ids: Vec<RowId>,
}

/// A mutable "view" into a filtered table, which only has the selected rows and columns.
#[derive(Debug)]
pub struct TableViewMut<'a> {
    table: &'a mut Table,
    col_ids: Vec<ColId>,
    row_ids: Vec<RowId>,
}

// STEP 10: Implement the table `select_helper` function below, which applies a select query to
// produce a list of column and row ids that should be included in the filtered table.
//
// Note: we factored out the "helper" function here to avoid code duplication between the `select`
// and `select_mut` operations.

impl Table {
    pub fn select_helper(&self, select: Select) -> (Vec<ColId>, Vec<RowId>) {
        let mut row_ids: Vec<RowId> = self
            .iter_row_ids()
            .filter(|&row_id| {
                let row = self.row(row_id);
                select
                    .filter
                    .as_ref()
                    .is_none_or(|filter| filter.apply(row))
            })
            .collect();

        for order_by in select.order_by.iter().rev() {
            row_ids.sort_by(|a, b| {
                let av = self.get((*a, order_by.col)).map(|v| v.to_owned());
                let bv = self.get((*b, order_by.col)).map(|v| v.to_owned());
                let ord = match (av, bv) {
                    (None, None) => Ordering::Equal,
                    (None, Some(_)) => Ordering::Less,
                    (Some(_), None) => Ordering::Greater,
                    (Some(DbVal::String(x)), Some(DbVal::String(y))) => x.cmp(&y),
                    (Some(DbVal::Integer(x)), Some(DbVal::Integer(y))) => x.cmp(&y),
                    (Some(DbVal::Boolean(x)), Some(DbVal::Boolean(y))) => x.cmp(&y),
                    (Some(DbVal::Double(x)), Some(DbVal::Double(y))) => {
                        x.partial_cmp(&y).unwrap_or(Ordering::Equal)
                    }
                    _ => panic!("type mismatch when ordering select query"),
                };
                if order_by.order == Order::Ascending {
                    ord
                } else {
                    ord.reverse()
                }
            });
        }

        (select.cols, row_ids)
    }
    pub fn select<'a>(&'a self, select: Select) -> TableView<'a> {
        let (col_ids, row_ids) = self.select_helper(select);
        TableView {
            table: self,
            col_ids,
            row_ids,
        }
    }
    pub fn select_mut<'a>(&'a mut self, select: Select) -> TableViewMut<'a> {
        let (col_ids, row_ids) = self.select_helper(select);
        TableViewMut {
            table: self,
            col_ids,
            row_ids,
        }
    }
}

// STEP 11: Implement the `TableIter` trait for `TableView<'a>` and `TableViewMut<'a>`.

impl<'a> TableIter for TableView<'a> {
    fn iter_rows<'b>(&'b self) -> impl Iterator<Item = RowRef<'b>> {
        self.row_ids.iter().copied().map(|id| self.table.row(id))
    }
    fn iter_cols<'b>(&'b self) -> impl Iterator<Item = view::ColRef<'b>> {
        self.col_ids.iter().copied().map(|id| self.table.col(id))
    }
}
impl<'a> TableIter for TableViewMut<'a> {
    fn iter_rows<'b>(&'b self) -> impl Iterator<Item = RowRef<'b>> {
        self.row_ids.iter().copied().map(|id| self.table.row(id))
    }
    fn iter_cols<'b>(&'b self) -> impl Iterator<Item = view::ColRef<'b>> {
        self.col_ids.iter().copied().map(|id| self.table.col(id))
    }
}

impl<'a> TableIterMut for TableViewMut<'a> {
    fn iter_cols_mut<'b>(&'b mut self) -> impl Iterator<Item = view::ColMut<'b>> {
        self.table
            .iter_cols_mut()
            .filter(|col| self.col_ids.contains(&col.id()))
    }
}

// The display implementation is provided for you.

impl<'a> Display for TableView<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.row_ids.is_empty() {
            return writeln!(f, "<no matching rows>");
        }
        if self.col_ids.is_empty() {
            return writeln!(f, "<no columns selected>");
        }

        let mut widths = Vec::new();
        for &col_id in &self.col_ids {
            let name = self.table.col(col_id).name().to_owned();

            let mut max_width = name.len();

            for &row_id in &self.row_ids {
                let cell_str = match self.table.get((row_id, col_id)) {
                    Some(val) => val.to_owned().to_string(),
                    None => "NULL".to_string(),
                };
                max_width = max_width.max(cell_str.len());
            }

            widths.push(max_width);
        }

        write!(f, " | ")?;
        for (i, &col_id) in self.col_ids.iter().enumerate() {
            let name = self.table.col(col_id).name().to_owned();
            let width = widths[i];
            write!(f, "{:width$} | ", name, width = width)?;
        }
        writeln!(f)?;

        // Separator line
        write!(f, "-+-")?;
        for &width in &widths {
            write!(f, "{}-+-", "-".repeat(width))?;
        }
        writeln!(f)?;

        for &row_id in &self.row_ids {
            write!(f, " | ")?;
            for (i, &col_id) in self.col_ids.iter().enumerate() {
                let cell_str = match self.table.get((row_id, col_id)) {
                    Some(val) => val.to_owned().to_string(),
                    None => "NULL".to_string(),
                };
                let width = widths[i];
                write!(f, "{:width$} | ", cell_str, width = width)?;
            }
            writeln!(f)?;
        }

        Ok(())
    }
}
impl<'a> Display for TableViewMut<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        TableView {
            table: self.table,
            col_ids: self.col_ids.clone(),
            row_ids: self.row_ids.clone(),
        }
        .fmt(f)
    }
}
