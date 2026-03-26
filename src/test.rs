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
fn example_assert() {
    assert_eq!(4, 4);
    assert!(vec!['x'].len() == 1)
}

#[test]
#[should_panic]
fn example_panicking() {
    panic!("should panic");
}
