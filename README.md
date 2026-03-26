# Homework 2: Relational Database

For this homework, you will implement an in-memory relational database.

## Problem Description

You are tasked with completing an implementation of an in-memory database in
Rust. Starter code is provided for you which outlines the basic structure of the
program. The goal of this homework is to get familiar with working with traits,
generics, and lifetimes.

Our database is capable of storing four data types: strings, 64-bit signed
integers, booleans, and double-precision floats, and values are nullable. The
data is stored in a table format. Each column has a name, an ID, and a data
type.

```
 | in_stock | category    | product_id | rating | price  | name           |
-+----------+-------------+------------+--------+--------+----------------+-
 | true     | Electronics | P001       | 4.50   | 24.99  | Wireless Mouse |
 | true     | Kitchen     | P002       | 4.80   | 12.50  | Coffee Mug     |
 | false    | Clothing    | P003       | 3.20   | 19.99  | Blue T-Shirt   |
 | true     | Office      | P004       | 4.70   | 899.00 | Notebook 13"   |
 | true     | Food        | P005       | 5.00   | 2.49   | Chocolate Bar  |
 | false    | Electronics | P006       | 4.10   | 79.99  | Headphones     |
 | false    | NULL        | P007       | NULL   | NULL   | NULL           |
```

> Sample dataset. This column has 6 columns and 7 rows.

To represent columns efficiently, we use two tricks. Firstly, since we know that
columns are of the same data type, we choose to represent each column by the
type `Col<T>`, where `T` is one of `String`, `i64`, `bool`, or `f64`. Secondly,
instead of storing `Option<T>` in the column to represent null values, we store
an additional data structure of type `BitVec`, which operates similarly to
`Vec<bool>`, except each `bool` value is packed into a single bit (rather than a
whole byte).

To represent the columns of the table, we store four fields:

```rust
pub struct Table {
    /* fields omitted */

    strings: Vec<Col<String>>,
    integers: Vec<Col<i64>>,
    booleans: Vec<Col<bool>>,
    doubles: Vec<Col<f64>>,
}
```

Our `ColId` type, which uniquely identifies a column, indexes into the
`strings`, `integers`, `booleans`, or `doubles` vector, and the one to choose is
specified by a type field:

```rust
pub struct ColId {
    pub(crate) idx: usize,
    pub(crate) ty: DbType,
}
```

To interact with database data, we define the following enum (where
`Option<DbVal>` represents a "nullable" value):

```rust
pub enum DbVal {
    String(String),
    Integer(i64),
    Boolean(bool),
    Double(f64),
}
```

This code is all provided for you, and you will not be expected to design any
advanced data structures for this project (unlike homework 1). Instead, the
focus will be on dealing with traits, generics, and lifetimes, particularly with
non-owning "view-like" or "reference-like" types.

### Understanding the Codebase

The starter code is extensively documented. Try running `cargo doc --open` to
see documentation in the browser!

## Suggested Steps

Here is a rough outline of the steps you'll need to take to complete the
database implementation. The assignment is roughly split into two parts:

### 1. Completing the Database Implementation

The database implementation, as it stands, is incomplete. Broadly speaking,
you'll need to implement reference-like types and iterators, making it ergonomic
to work with the database. You are tasked with implementing the following traits
and functions:

1. In `types.rs`, implement the `DbRef<'a>` and `DbMut<'a>` types.
   - Remember to use lifetime references (e.g. `&'a` or `&'a mut`), which are
     required for structs/enums.
   - **Tip**: Copy the pattern from how `DbVal` is defined — it's very similar
     but with references instead of owned values.
2. In `types.rs`, implement the `as_ref`, `as_mut`, and `to_owned` functions.
   - `DbVal::as_ref<'a>(&'a self) -> DbRef<'a>`: return a borrowed view without
     cloning.
   - `DbVal::as_mut<'a>(&'a mut self) -> DbMut<'a>`: return a _mutably_ borrowed
     view without cloning.
   - `DbRef::to_owned(&self) -> DbVal`: clone the inner value if needed (try
     `String::to_owned()`!).
3. In `types.rs`, implement the `From` conversion traits for `DbRef<'a>` and
   `DbMut<'a>`.
   - Goal: make it easy to convert between `String`/`i64`/`bool`/`f64` and
     `DbVal`
   - You may find the following page useful for more info on `From` and `Into`
     traits:
     [Rust by Example](https://doc.rust-lang.org/rust-by-example/conversion/from_into.html)
4. In `col.rs`, implement the `Storage<T>` and `StorageMut<T>` traits for
   `Col<T>`.
   - You should look at the definitions of these traits in `storage.rs`!
   - These traits make use of **associated types** (as discussed in class).
     - These types are used in the `get` and `get_mut` functions.
     - These types should behave like references. Think about what types to use!
   - You also need to implement `put` (which sets a value) and `take` (which
     removes a value).
     - You may find the `std::mem::swap` and `std::mem::take` functions useful
       (see the [docs](https://doc.rust-lang.org/std/mem/index.html)).
5. In `table.rs`, implement the `Storage<T>` and `StorageMut<T>` traits for
   `Table`.
   - Remember that we store the data in four different locations (`strings`,
     `integers`, etc...)
   - You will almost certainly need to pattern-match on a value of type
     `DbType`.
   - You may find the "destructure" syntax `let (row, col) = id` useful.
   - A clean pattern for converting a value of type `String` (or similar) into
     `Option<DbVal>` is via `.map(DbVal::String)`.
6. In `view.rs`, implement the `Storage<DbVal>` and`StorageMut<DbVal>` traits
   for the row/column reference types.
   - This step should be very straightforward. Make use of the functions you
     implemented in step five!
   - Note: we need to introduce a second lifetime, `'b` to satisfy the compiler.
     But we also have the bound `Self: 'b`, guaranteeing that `'b` lives at
     least as long as `'a`.
7. In `table/iter.rs`, implement the `TableIter` trait for `Table`.
   - This is the first time you'll be dealing with iterators. You'll find
     chapter 13 of
     [the book](https://doc.rust-lang.org/book/ch13-02-iterators.html) very
     helpful here.
   - Two tricks will be very useful:
     - For `iter_rows`, you may want to start with the iterator `(0..n)`, which
       iterates over values in the range $[0, n)$, exclusive (i.e. not including
       the value $n$). Think about what $n$ should be!
       - To convert the values from integers to rows, you can use the `.map()`
         function provided by the `Iterator` trait. This function accepts a
         [closure](https://doc.rust-lang.org/book/ch13-01-closures.html) which
         "transforms" values from one type to another. To create a closure, use
         the syntax `|args...| expr` or `|args...| { body }`. Or if it's more
         comfortable, you can also just pass a normal function instead (e.g.
         `.map(my_fn)`.
   - For `iter_cols`, the cleanest approach is to create **four** iterators (one
     for each type). Those iterators will be over `Col<T>`, so you'll need to
     convert them into iterators over `ColRef<T>`.
     - Then, you can _combine_ those iterators into one via the
       [`Iterator::chain()`](https://doc.rust-lang.org/beta/std/iter/struct.Chain.html)
       method.
8. In `table/iter.rs`, implement the `iter` functions for `RowRef<'a>`,
   `RowMut<'a>`, `ColRef<'a>`, and `ColMut<'a>`.
   - For the row types, you'll likely want to start by calling
     `self.table.iter_cols()`.
   - For the column types, you'll likely need to pattern-match and call
     `.iter()` on each column.

### 2. Implementing the Select Query

Now, it is up to you to implement the "select" query operation. These steps
assume you've already completed part 1. Part 2 is about building a simple but
powerful `SELECT`-like query system that filters rows, selects columns, and
optionally sorts the result. You’ll implement three main pieces:

1. In `table/select.rs`, implement the function `Filter::apply`.
   - This will be used to decide whether a row matches the user's filter.
2. In `table/select.rs`, implement the function `Table::select_helper`.
   - This will be used to find the matching rows and columns.
3. In `table/select.rs`, implement `TableIter` and `TableIterMut` for
   `TableView<'a>`.
   - This will let us iterate over the resulting table "view" (filtered down +
     sorted table).

We implement the result using `TableView<'a>` to avoid having to clone the
entire table. This, combined with the `TableIter` trait, makes the
implementation super efficient.

#### `Filter::apply()`

The `Filter` type is written as a (very basic)
[abstract syntax tree](https://en.wikipedia.org/wiki/Abstract_syntax_tree). It's
an enum with variants that either perform _comparisons_ (condition checks), or
combine multiple `Filter`s via some logic operator (AND, OR, XOR, NOT).

You'll notice that we're using `Box<Filter>` in some places instead of `Filter`.
The use of `Box` allows us to put values on the heap, hence avoiding an
infinitely recursive (i.e. infinitely-sized) type.

To make the implementation easier, we have two tips:

1. Pattern matching is strongly encouraged, since it will let you handle each
   case independently. For the logic-operator enum variants, you'll want to
   recursively call the `apply` function and combine those results somehow.
2. To implement all the comparisons, you really only need to implement two (`==`
   and `>`, or similar). Consider making helper functions to abstract out the
   comparison logic!

> Note: you may assume that `null == null`, and that anything else is `!= null`.
> Furthermore, you may assume that `true > false`, and that all values are
> `> null`. Recall that we represent `null` with `Option::None`.

#### `select_helper()`

This is likely the most complex function you will need to implement. Make sure
to take advantage of the functions you've implemented in part 1 to make the
implementation easier! Specifically, you shouldn't need to rely on
implementation details of `Table` or `Col`.

Remember that you can use `for` loops on any iterator (including user-defined
ones)!

The return value of this function is the list of row ids and column ids to be
included in the final table. The row ids should appear in the order requested by
the "order-by" part of the select query. Specifically, sorting should be
_applied_ in **reverse order** to how they appear in the `Vec<OrderBy>` (such
that the sorting goes from "most important" to "least important"). You may find
the `.iter().rev()` function useful (reversing an iterator).

You can easily sort a `Vec` by the `sort_by()` function, which accepts a closure
that specifies the ordering of the sort. The returned ordering is given by the
[`std::cmp::Ordering`](https://doc.rust-lang.org/std/cmp/enum.Ordering.html)
enum (rather than a simple boolean).

#### `TableIter` and `TableIterMut`

This step should be straightforward. Try using the `.filter()` method on
iterators.

## Common Pitfalls and How to Avoid Them

**Multiple mutable borrows**

- Error: "cannot borrow `*self` as mutable more than once"
- Fix: Only borrow mutably when you need to mutate and end borrows quickly (use
  `{}` blocks or `drop`).

**Type mismatch in put/get**

- Error: panic "type mismatch" or wrong variant returned
- Fix: Make sure `ColId` type matches the `Vec<Col<T>>` you're indexing into.

**Iterators returning wrong Option level**

- Error: `Some(Some(value))` instead of `Some(value)`
- Fix: Use `.map(|v| v.map(DbRef::…))` correctly — one `map` for the outer
  `Option`, one for the inner.

**Forgetting to set occupied bit**

- Symptom: `get` returns `None` even after `put` or `Some` even after `take`
- Fix: Always update `occupied` in `put`/`take`.

**Panic when index out of bounds**

- Symptom: panic in `data[idx]`
- Fix: Call `extend_with_null` before accessing `data[idx]` in `put`.

## Testing

To test your code, you can use the REPL by running `cargo run`. This gives you
access to an interactive command line that lets you manage tables. You can also
save and load tables from a CSV file. Use the `help` command to see a list of
available REPL commands. If you have questions about how to use the REPL, please
ask on Ed Discussion.

We've also provided sample data in `dummy.csv`. To load the dummy data, run the
following sequence of commands:

```
table> load dummy.csv dummy
load: successfully loaded 7 row(s)
table> open dummy
dummy> print
 | in_stock | category    | rating | price  | name           | product_id |
-+----------+-------------+--------+--------+----------------+------------+-
 | true     | Electronics | 4.50   | 24.99  | Wireless Mouse | P001       |
 | true     | Kitchen     | 4.80   | 12.50  | Coffee Mug     | P002       |
 | false    | Clothing    | 3.20   | 19.99  | Blue T-Shirt   | P003       |
 | true     | Office      | 4.70   | 899.00 | Notebook 13"   | P004       |
 | true     | Food        | 5.00   | 2.49   | Chocolate Bar  | P005       |
 | false    | Electronics | 4.10   | 79.99  | Headphones     | P006       |
 | false    | NULL        | NULL   | NULL   | NULL           | P007       |
dummy> _
```

Also, you may choose to implement your own tests in `test.rs`. These tests can
be run with `cargo test`.

## Grading

The homework is due on Monday, March 23rd @ 11:59 PM, submitted via Gradescope.

Grading will be broken down as follows:

- 60%---Autograder Tests
- 20%---Function Implementations (weighted equally per function)
- 20%---Manually-Graded Style Points (see below)
- 10%---Clippy and Formatting (make sure to run `cargo clipy` and `cargo fmt`!)

The style portion will mainly focus on avoiding excessive use of `clone()`, or
otherwise doing things that aren't in the spirit of assignment (specifically,
reference-like behavior and avoiding copies). But we won't take off points for
"ugly" code or anything like that.

> For example, if you implement your `DbRef` type without using any references,
> or you call `clone()` on an entire table column, you will lose style points.

The autograder tests aren't provided upfront, but we may release reference tests
on Ed. If your implementation works well with the REPL, it's highly likely that
it will pass the autograder tests (the only reall _logic_ here is part of the
`SELECT` functionality, everything else is pretty much "if it compiles, it
(probably) works").
