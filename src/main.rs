//! Rust in-memory relational database. See the README for detailed homework instructions.
//!
//! The files you will need to change are `types.rs`, `col.rs`, `table.rs`, `view.rs`, `table/iter.rs`, and
//! `table/select.rs`. However, you may find it useful to also look at `storage.rs`.
//!
//! You may also find it helpful to add tests to the `test.rs` file, or alternatively, test your
//! code with the interactive command-line REPL, available via `cargo run`.

// WARNING: do not modify this file! it will not be considered as part of your submission.

// Suppress unused warnings
#![allow(unused)]

/// Commands for the REPL. No need to modify anything here.
pub mod commands;

pub mod types;

/// Defines the `Storage<T>` trait, which is used for getting and setting database values.
pub mod storage;

/// Defines the `Col<T>` trait, which efficiently stores a column of (nullable) data of type `T`.
pub mod col;

/// Defines the `Table` type, which stores a table of database data.
pub mod table;

/// Testing module for use with `cargo test`.
#[cfg(test)]
pub mod test;

use colored::Colorize;
use std::{
    collections::HashMap,
    io::{self, Stdin, Stdout, Write},
};

use crate::table::Table;

/// The currently active REPL mode ("top-level" commands or "table-open" mode).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Mode {
    TopLevel,
    TableOpen,
}

/// A runnable command in the REPL.
pub struct Command {
    names: &'static [&'static str],
    description: &'static str,
    args: &'static [&'static str],
    mode: Option<Mode>,
    execute: fn(&[&str], &mut App) -> Result<(), String>,
}

impl Command {
    fn format_usage(&self) -> String {
        let names_str = self.names.last().unwrap().bold();
        let args_str = self
            .args
            .iter()
            .map(|arg| format!("[{arg}]"))
            .reduce(|acc, s| acc + " " + &s)
            .unwrap_or_default();
        format!("{} {}", names_str, args_str)
    }
}

/// App-level state for the command line REPL.
pub struct App {
    stdin: Stdin,
    stdout: Stdout,
    input: String,
    cur_table: Option<String>,
    tables: HashMap<String, Table>,
    commands: Vec<Command>,
}

impl App {
    fn new() -> Self {
        let commands = vec![
            Command {
                names: &["help"],
                description: "show help menu",
                args: &[],
                mode: None,
                execute: commands::help,
            },
            Command {
                names: &["clear"],
                description: "clear the screen",
                args: &[],
                mode: None,
                execute: commands::clear,
            },
            Command {
                names: &["exit"],
                description: "exit table / application",
                args: &[],
                mode: None,
                execute: commands::exit,
            },
            Command {
                names: &["ls"],
                description: "list tables",
                args: &[],
                mode: Some(Mode::TopLevel),
                execute: commands::ls,
            },
            Command {
                names: &["new"],
                description: "create a new table",
                args: &["TABLE"],
                mode: Some(Mode::TopLevel),
                execute: commands::new,
            },
            Command {
                names: &["rm"],
                description: "delete a table",
                args: &["TABLE"],
                mode: Some(Mode::TopLevel),
                execute: commands::rm,
            },
            Command {
                names: &["mv"],
                description: "rename a table",
                args: &["SRC", "DST"],
                mode: Some(Mode::TopLevel),
                execute: commands::mv,
            },
            Command {
                names: &["cp"],
                description: "copy a table",
                args: &["SRC", "DST"],
                mode: Some(Mode::TopLevel),
                execute: commands::cp,
            },
            Command {
                names: &["o", "open"],
                description: "open or create a table",
                args: &["TABLE"],
                mode: Some(Mode::TopLevel),
                execute: commands::open,
            },
            Command {
                names: &["load"],
                description: "load table from a csv file",
                args: &["FILE", "TABLE"],
                mode: Some(Mode::TopLevel),
                execute: commands::load,
            },
            Command {
                names: &["save"],
                description: "save table to a csv file",
                args: &["TABLE", "FILE"],
                mode: Some(Mode::TopLevel),
                execute: commands::save,
            },
            Command {
                names: &["c", "close"],
                description: "close current table",
                args: &[],
                mode: Some(Mode::TableOpen),
                execute: commands::close,
            },
            Command {
                names: &["p", "print"],
                description: "print the entire table",
                args: &[],
                mode: Some(Mode::TableOpen),
                execute: commands::print,
            },
            Command {
                names: &["ar", "add-row"],
                description: "add one row",
                args: &[],
                mode: Some(Mode::TableOpen),
                execute: commands::add_row,
            },
            Command {
                names: &["ars", "add-rows"],
                description: "add multiple rows",
                args: &["COUNT"],
                mode: Some(Mode::TableOpen),
                execute: commands::add_rows,
            },
            Command {
                names: &["ac", "add-col"],
                description: "create a new column",
                args: &["NAME", "TYPE"],
                mode: Some(Mode::TableOpen),
                execute: commands::add_col,
            },
            Command {
                names: &["s", "select"],
                description: "execute a select query",
                args: &[],
                mode: Some(Mode::TableOpen),
                execute: commands::select,
            },
        ];

        Self {
            stdin: io::stdin(),
            stdout: io::stdout(),
            input: String::new(),
            cur_table: None,
            tables: HashMap::new(),
            commands,
        }
    }

    fn get_mode(&self) -> Mode {
        if self.cur_table.is_some() {
            Mode::TableOpen
        } else {
            Mode::TopLevel
        }
    }

    fn prompt(&mut self) {
        match self.get_mode() {
            Mode::TopLevel => print!("{} ", "table>".blue()),
            Mode::TableOpen => {
                let table = self.cur_table.as_ref().unwrap();
                print!("{} ", format!("{}>", table).purple());
            }
        }
        let _ = self.stdout.flush();
    }

    fn get_input(&mut self) {
        self.input.clear();
        self.stdin.read_line(&mut self.input).unwrap();
    }

    fn table_mut(&mut self) -> &mut Table {
        self.tables
            .get_mut(self.cur_table.as_ref().unwrap())
            .expect("current table should exist")
    }

    fn run(&mut self) {
        loop {
            self.prompt();
            self.get_input();

            if self.input.is_empty() {
                println!();
                if self.cur_table.is_some() {
                    self.cur_table = None;
                    continue;
                } else {
                    break;
                }
            }

            let line = self.input.trim().to_owned();
            if line.is_empty() {
                continue;
            }

            let mut parts = line.split_whitespace();
            let cmd_name = parts.next().unwrap_or("").to_ascii_lowercase();
            let args: Vec<&str> = parts.collect();

            let Some(cmd) = self
                .commands
                .iter()
                .find(|c| c.names.contains(&cmd_name.as_str()))
            else {
                println!("{}", "Unrecognized command. Try \"help\".".red());
                continue;
            };

            if let Some(mode) = cmd.mode
                && mode != self.get_mode()
            {
                let msg = match mode {
                    Mode::TopLevel => "command unavailable in toplevel mode",
                    Mode::TableOpen => "command unavailable in open table mode",
                };
                println!("{}", format!("Error: {msg}.").red());
                continue;
            }

            if args.len() != cmd.args.len() {
                println!("usage: {}", cmd.format_usage());
                continue;
            }

            if let Err(msg) = (cmd.execute)(&args, self) {
                println!("{}", msg.red());
            }
        }
    }
}

fn main() {
    App::new().run();
}
