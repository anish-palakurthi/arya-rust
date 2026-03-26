//! This file implements parsing tables to/from a CSV file, which is available through the `save`
//! and `load` commands in the toplevel REPL. This can be useful for testing purposes.

// WARNING: do not modify this file! it will not be considered as part of your submission.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

use colored::Colorize;

use crate::storage::{Storage, StorageMut};
use crate::table::Table;
use crate::types::{DbType, DbVal, RowId};

impl Table {
    /// Attempts to write this table to a CSV file at the given path.
    pub fn to_csv<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let mut file = File::create(path)?;
        let mut writer = io::BufWriter::new(&mut file);

        if self.num_cols() == 0 || self.num_rows() == 0 {
            if !self.id_map.is_empty() {
                let headers: Vec<_> = self.id_map.keys().cloned().collect();
                write_csv_line(&mut writer, headers.as_slice())?;
            }
            return Ok(());
        }

        let headers: Vec<String> = self.id_map.keys().cloned().collect();
        write_csv_line(&mut writer, &headers)?;

        for row_idx in 0..self.next_row_idx {
            let mut row_values = Vec::with_capacity(self.num_cols());

            for col_id in self.id_map.values() {
                let val = self.get((RowId { idx: row_idx }, *col_id));
                let cell_str = match val.map(|v| v.to_owned()) {
                    Some(DbVal::String(s)) => s,
                    Some(DbVal::Integer(i)) => i.to_string(),
                    Some(DbVal::Boolean(b)) => b.to_string(),
                    Some(DbVal::Double(d)) => format!("{:.8}", d),
                    None => String::new(),
                };
                row_values.push(cell_str);
            }

            write_csv_line(&mut writer, &row_values)?;
        }

        writer.flush()?;
        Ok(())
    }

    /// Attempts to construct a table from a CSV file at the given path.
    pub fn from_csv<P: AsRef<Path>>(path: P) -> io::Result<(Table, usize)> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        let mut table = Table::default();

        let header_line = match lines.next() {
            Some(Ok(line)) => line,
            Some(Err(e)) => return Err(e),
            None => return Ok((table, 0)),
        };

        let headers: Vec<String> = split_csv_line(header_line.trim())?;
        if headers.is_empty() {
            return Ok((table, 0));
        }

        let mut col_types = vec![DbType::String; headers.len()];
        for name in headers.iter() {
            table.add_col(name, DbType::String);
        }

        let mut row_count = 0;

        for line_res in lines {
            let line = line_res?;
            if line.trim().is_empty() {
                continue;
            }

            let values = split_csv_line(line.trim())?;
            if values.len() != headers.len() {
                dbg!(&values);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "row {} has {} fields, expected {}",
                        row_count + 1,
                        values.len(),
                        headers.len()
                    ),
                ));
            }

            if row_count == 0 {
                for (i, val) in values.iter().enumerate() {
                    if let Some(ty) = guess_db_type(val)
                        && ty != DbType::String
                    {
                        table.id_map.remove(&headers[i]);
                        table.add_col(&headers[i], ty);
                        col_types[i] = ty;
                    }
                }
            }

            let row_id = table.add_row();

            for (i, val_str) in values.into_iter().enumerate() {
                if val_str.trim().is_empty() {
                    continue;
                }

                let col_id = *table.id_map.get(&headers[i]).unwrap();
                let val = match col_types[i] {
                    DbType::String => Some(DbVal::String(val_str)),
                    DbType::Integer => {
                        if let Ok(n) = val_str.parse::<i64>() {
                            Some(DbVal::Integer(n))
                        } else {
                            println!(
                                "{}",
                                format!("load: unrecognized integer value: {val_str}").yellow()
                            );
                            None
                        }
                    }
                    DbType::Boolean => {
                        let lower = val_str.to_lowercase();
                        if lower == "true" || lower == "yes" {
                            Some(DbVal::Boolean(true))
                        } else if lower == "false" || lower == "no" {
                            Some(DbVal::Boolean(false))
                        } else {
                            println!(
                                "{}",
                                format!("load: unrecognized boolean value: {val_str}").yellow()
                            );
                            None
                        }
                    }
                    DbType::Double => {
                        if let Ok(f) = val_str.parse::<f64>() {
                            Some(DbVal::Double(f))
                        } else {
                            println!(
                                "{}",
                                format!("load: unrecognized double value: {val_str}").yellow()
                            );
                            None
                        }
                    }
                };

                if let Some(val) = val {
                    table.put((row_id, col_id), val);
                }
            }

            row_count += 1;
        }

        Ok((table, row_count))
    }
}

fn write_csv_line<W: Write>(writer: &mut W, fields: &[String]) -> io::Result<()> {
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            write!(writer, ",")?;
        }

        let needs_quote = field.contains(',') || field.contains('"') || field.contains('\n');
        if needs_quote {
            write!(writer, "\"")?;
            for c in field.chars() {
                if c == '"' {
                    write!(writer, "\"\"")?;
                } else {
                    write!(writer, "{}", c)?;
                }
            }
            write!(writer, "\"")?;
        } else {
            write!(writer, "{}", field)?;
        }
    }
    writeln!(writer)?;
    Ok(())
}

fn split_csv_line(line: &str) -> io::Result<Vec<String>> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '"' if !in_quotes => {
                in_quotes = true;
            }
            '"' if in_quotes => {
                if let Some(&next) = chars.peek() {
                    if next == '"' {
                        current.push('"');
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                } else {
                    in_quotes = false;
                }
            }
            ',' if !in_quotes => {
                fields.push(current);
                current = String::new();
            }
            _ => {
                current.push(c);
            }
        }
    }

    fields.push(current);

    Ok(fields)
}

fn guess_db_type(s: &str) -> Option<DbType> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let lower = s.to_lowercase();
    if lower == "true" || lower == "false" || lower == "yes" || lower == "no" {
        return Some(DbType::Boolean);
    }

    if s.parse::<i64>().is_ok() {
        return Some(DbType::Integer);
    }

    if s.parse::<f64>().is_ok() {
        return Some(DbType::Double);
    }

    None
}
