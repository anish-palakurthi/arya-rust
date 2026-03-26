//! This file defines basic database-related types. The types are generally capable of representing
//! strings, 64-bit signed integers, booleans, and double-precision floats. We also define
//! custom "reference" types (both immutable and mutable), and helper functions to convert between
//! them and owned database values.

use std::fmt::{self, Display, Formatter};

/// A handle to a table row. See `col.rs` and `table.rs` for more details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowId {
    /// The row's (shared) numerical index into each individual column vector.
    pub(crate) idx: usize,
}

/// A handle to a table column. See `col.rs` and `table.rs` for more details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColId {
    /// The column's numerical index into `Vec` of columns of the given type.
    pub(crate) idx: usize,

    /// The column's type, which specifies which `Vec` to select the column from.
    pub(crate) ty: DbType,
}

/// A type which can be stored within the database. All types are implicitly nullable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DbType {
    String,
    Integer,
    Boolean,
    Double,
}

/// A non-null value which can be stored within the database. We represent nullable values as
/// `Option<DbVal>`, where `Option::None` is the null variant.
#[derive(Debug, Clone, PartialEq)]
pub enum DbVal {
    String(String),
    Integer(i64),
    Boolean(bool),
    Double(f64),
}

// STEP 1: Implement the `DbRef<'a>` and `DbMut<'a>` types.
//
// Implement the following "reference-like" types, which correspond to immutable and mutable
// references to the `DbVal` type, respectively. This step should be relatively straightforward.
//
// HINT: if we just take a reference to `DbVal` directly, we wouldn't be able to store our data
// efficiently within homogenous `Vec`-backed columns. We don't store `DbVal` within our columns,
// so any reference to `DbVal` would be dangling! Think about how you can reference the _original_
// data that lives inside each `Col<T>`.

/// A reference to a database value with lifetime `'a`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DbRef<'a> {
    String(&'a String),
    Integer(&'a i64),
    Boolean(&'a bool),
    Double(&'a f64),
}

/// A reference to a database value with lifetime `'a`.
#[derive(Debug, PartialEq)]
pub enum DbMut<'a> {
    String(&'a mut String),
    Integer(&'a mut i64),
    Boolean(&'a mut bool),
    Double(&'a mut f64),
}

// STEP 2: Implement the `as_ref`, `as_mut`, and `to_owned` functions. The behavior of these
// functions is summarized below:
//
// Reference-conversion functions:
//
// - `as_ref` converts a `&'a DbVal` into a `DbRef<'a>`
// - `as_mut` converts a `&'a mut DbVal` into a `DbMut<'a>`
//
// Ownership-taking functions:
//
// - `to_owned` converts a `&DbRef` or `&DbMut` into an owned `DbVal`
//
// HINT: don't overcomplicate this! The implementation should be relatively straightforward. To
// convert `&String` into `String` you can use `String::to_owned()`. Also be aware that you might
// end up with double references in the `to_owned` implementations, so you might need to
// dereference twice.

impl DbVal {
    /// Converts this database value into a reference.
    pub fn as_ref<'a>(&'a self) -> DbRef<'a> {
        match self {
            DbVal::String(v) => DbRef::String(v),
            DbVal::Integer(v) => DbRef::Integer(v),
            DbVal::Boolean(v) => DbRef::Boolean(v),
            DbVal::Double(v) => DbRef::Double(v),
        }
    }

    /// Converts this database value into a mutable reference.
    pub fn as_mut<'a>(&'a mut self) -> DbMut<'a> {
        match self {
            DbVal::String(v) => DbMut::String(v),
            DbVal::Integer(v) => DbMut::Integer(v),
            DbVal::Boolean(v) => DbMut::Boolean(v),
            DbVal::Double(v) => DbMut::Double(v),
        }
    }
}

impl<'a> DbRef<'a> {
    /// Converts this reference type into a new, owned database value.
    pub fn to_owned(&self) -> DbVal {
        match self {
            DbRef::String(v) => DbVal::String((*v).to_owned()),
            DbRef::Integer(v) => DbVal::Integer(**v),
            DbRef::Boolean(v) => DbVal::Boolean(**v),
            DbRef::Double(v) => DbVal::Double(**v),
        }
    }
}

impl<'a> DbMut<'a> {
    /// Converts this reference type into a new, owned (independent!) database value.
    pub fn to_owned(&self) -> DbVal {
        match self {
            DbMut::String(v) => DbVal::String((**v).to_owned()),
            DbMut::Integer(v) => DbVal::Integer(**v),
            DbMut::Boolean(v) => DbVal::Boolean(**v),
            DbMut::Double(v) => DbVal::Double(**v),
        }
    }
}

// Below we implement various conversion traits to make working with database types easier

impl From<String> for DbVal {
    fn from(value: String) -> Self {
        DbVal::String(value)
    }
}
impl From<&str> for DbVal {
    fn from(value: &str) -> Self {
        DbVal::String(value.to_owned())
    }
}
impl From<i64> for DbVal {
    fn from(value: i64) -> Self {
        DbVal::Integer(value)
    }
}
impl From<bool> for DbVal {
    fn from(value: bool) -> Self {
        DbVal::Boolean(value)
    }
}
impl From<f64> for DbVal {
    fn from(value: f64) -> Self {
        DbVal::Double(value)
    }
}

// STEP 3: Implement the same `From` conversion traits for your `DbRef<'a>` and `DbMut<'a>` types.

impl<'a> From<&'a String> for DbRef<'a> {
    fn from(value: &'a String) -> Self {
        DbRef::String(value)
    }
}
impl<'a> From<&'a i64> for DbRef<'a> {
    fn from(value: &'a i64) -> Self {
        DbRef::Integer(value)
    }
}
impl<'a> From<&'a bool> for DbRef<'a> {
    fn from(value: &'a bool) -> Self {
        DbRef::Boolean(value)
    }
}
impl<'a> From<&'a f64> for DbRef<'a> {
    fn from(value: &'a f64) -> Self {
        DbRef::Double(value)
    }
}

impl<'a> From<&'a mut String> for DbMut<'a> {
    fn from(value: &'a mut String) -> Self {
        DbMut::String(value)
    }
}
impl<'a> From<&'a mut i64> for DbMut<'a> {
    fn from(value: &'a mut i64) -> Self {
        DbMut::Integer(value)
    }
}
impl<'a> From<&'a mut bool> for DbMut<'a> {
    fn from(value: &'a mut bool) -> Self {
        DbMut::Boolean(value)
    }
}
impl<'a> From<&'a mut f64> for DbMut<'a> {
    fn from(value: &'a mut f64) -> Self {
        DbMut::Double(value)
    }
}

impl DbVal {
    /// Tries to "downcast" this value into a string, panicking if the type is mismatched.
    pub fn into_string(self) -> String {
        match self {
            DbVal::String(v) => v,
            _ => panic!("wrong type when calling into_string()"),
        }
    }

    /// Tries to "downcast" this value into an integer, panicking if the type is mismatched.
    pub fn into_integer(self) -> i64 {
        match self {
            DbVal::Integer(v) => v,
            _ => panic!("wrong type when calling into_integer()"),
        }
    }

    /// Tries to "downcast" this value into a boolean, panicking if the type is mismatched.
    pub fn into_boolean(self) -> bool {
        match self {
            DbVal::Boolean(v) => v,
            _ => panic!("wrong type when calling into_boolean()"),
        }
    }

    /// Tries to "downcast" this value into a double, panicking if the type is mismatched.
    pub fn into_double(self) -> f64 {
        match self {
            DbVal::Double(v) => v,
            _ => panic!("wrong type when calling into_double()"),
        }
    }
}

// Below we implement the `Display` trait on our database types so that we can use them with the
// `println!()` macro.

impl Display for DbType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Display for DbVal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DbVal::String(val) => write!(f, "{val}"),
            DbVal::Integer(val) => write!(f, "{val}"),
            DbVal::Boolean(val) => write!(f, "{val}"),
            DbVal::Double(val) => write!(f, "{val:.2}"),
        }
    }
}
