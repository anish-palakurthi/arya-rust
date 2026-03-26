// WARNING: do not modify this file! it will not be considered as part of your submission.

// The `Storage<T>` allows us to use the type `Self` as storage for data of type `T`. This trait
// defines the following associated types:
//
// - `Id` which is the type we use to "index into" this storage. Sort of like a key. We require
//   the bound `Id: 'static` to make sure it doesn't hold references (it should be lightweight).
//
// - `Ref<'a>` which is the type returned from the `get` method. You might think we could just
//   return `&'a T`, but this isn't flexible enough to accomodate our custom reference types. So
//   instead, we let the trait implementor decide how to represent reference types.
//
// The `Storage<T>` trait only implements the `get` method, which is used to query data from the
// storage by its ID, returning a reference to that data.
//
// Note that we need to introduce the bound `where Self: 'a`, which is required by the compiler to
// ensure that the reference type doesn't outlive self. Make sure to include this bound when
// implementing the trait!

pub trait Storage<T> {
    type Id: 'static;
    type Ref<'a>
    where
        Self: 'a;

    /// Gets a value from storage with the given ID, returning an optional reference.
    fn get<'a>(&'a self, id: Self::Id) -> Option<Self::Ref<'a>>;
}

// The `StorageMut<T>` trait is similar to `Storage<T>`, but it supports mutable operations.
// Notably, these operations are:
//
// - `get_mut`, which operates the same as `get` but returns a mutable reference.
// - `put`, which replaces the value in storage with a new value
// - `take`, which takes the value from storage, replacing it with `None`
//
// This trait is a _subtrait_ of `Storage<T>`, meaning we inherit the `Id` and `Ref<'a>` associated
// types as well as the `get` method. Furthermore, we define the `RefMut<'a>` associated type,
// which is the same as `Ref<'a>` but for a mutable reference to a stored value rather than an
// immutable one.

pub trait StorageMut<T>: Storage<T> {
    type RefMut<'a>
    where
        Self: 'a;

    /// Gets a value mutably from storage with the given ID, returning an optional reference.
    fn get_mut<'a>(&'a mut self, id: Self::Id) -> Option<Self::RefMut<'a>>;

    /// Replaces the value in a storage slot with a new value, returning the old value.
    fn put(&mut self, id: Self::Id, val: impl Into<T>) -> Option<T>;

    /// Consumes the value in a storage slot, returning the value and setting the slot to `None`.
    fn take(&mut self, id: Self::Id) -> Option<T>;
}
