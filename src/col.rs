use crate::{
    storage::{Storage, StorageMut},
    types::{ColId, RowId},
};

// We use the `BitVec` crate for an efficient bit-vector implementation. The standard library
// `Vec<bool>` will use a full 8 bits for each boolean value to preserve memory alignment. This
// crate uses just one bit per boolean, which saves a lot of space at the expense of some runtime
// bit-manipulation.
use bit_vec::BitVec;

/// A column of data of type `T`, stored efficiently in memory as a `Vec<T>` data buffer with a
/// `BitVec` being used to represent "null" values. Using this "struct-of-arrays" representation
/// saves space because we can pack data tightly without any space wasted for padding.
#[derive(Debug, Clone, PartialEq)]
pub struct Col<T> {
    /// The ID of this column, produced by a table.
    id: ColId,

    /// The name of this column, e.g. "Country".
    name: String,

    /// The actual stored data. Some values may be logically null, but the vector is always
    /// guaranteed to have _some_ value (perhaps `T::default()`...).
    data: Vec<T>,

    /// A bit vector where `true` at index i means that `data[i]` holds a real value, and `false`
    /// at index i means that `data[i]` is null.
    occupied: BitVec,
}

impl<T> Col<T> {
    /// Creates a new, empty column.
    pub fn new(id: ColId, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            data: Vec::new(),
            occupied: BitVec::new(),
        }
    }

    /// Returns the ID associated with this column.
    pub fn id(&self) -> ColId {
        self.id
    }

    /// Returns the name associated with this column.
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    // Note: the `zip` function on iterators combines the two iterators into a single iterator over
    // pairs of values.

    /// Iteates over values within this column.
    pub fn iter(&self) -> impl Iterator<Item = Option<&T>> {
        self.data
            .iter()
            .zip(self.occupied.iter())
            .map(|(v, o)| if o { Some(v) } else { None })
    }

    /// Iteates mutably over values within this column.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = Option<&mut T>> {
        self.data
            .iter_mut()
            .zip(self.occupied.iter())
            .map(|(v, o)| if o { Some(v) } else { None })
    }
}

impl<T: Default> Col<T> {
    /// Helper function to make sure we don't index out of bounds for the given row. Extends both
    /// vectors to the requested length by filling them with `T::default()` and `false`,
    /// respectively (i.e. null values).
    fn extend_with_null(&mut self, row: usize) {
        while self.data.len() <= row {
            self.data.push(T::default());
            self.occupied.push(false);
        }
    }
}

// STEP 4: Implement the `Storage<T>` and `StorageMut<T>` traits for `Col<T>`.
//
// Implement the required traits so that we can get and mutate data within a column. Think about
// the generic types and bounds you will need to add to `impl` block (you may need to add some!).
// You should reference the `storage.rs` file to see the corresponding trait definitions.
//
// HINT: The associated type `Id` should be set to `RowId`.
// HINT: Don't overcomplicate the reference type! Remember this is for a generic `T`, not `DbVal`.

impl<T> Storage<T> for Col<T> {
    // TODO: specify associated types
    type Id = ();
    type Ref<'a>
        = ()
    where
        Self: 'a;

    fn get(&self, id: ()) -> Option<()> {
        unimplemented!()
    }
}

impl<T> StorageMut<T> for Col<T> {
    // TODO: specify associated types
    type RefMut<'a>
        = ()
    where
        Self: 'a;

    fn get_mut(&mut self, id: ()) -> Option<()> {
        unimplemented!()
    }

    fn put(&mut self, id: (), val: impl Into<T>) -> Option<T> {
        unimplemented!()
    }

    fn take(&mut self, id: ()) -> Option<T> {
        unimplemented!()
    }
}
