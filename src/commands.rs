// WARNING: do not modify this file! it will not be considered as part of your submission.

use std::io::Write;

use crate::{
    App,
    table::{
        Table,
        iter::TableIter,
        select::{Filter, Order, Select},
    },
    types::{DbType, DbVal},
};

use colored::Colorize;

pub fn help(_: &[&str], app: &mut App) -> Result<(), String> {
    let show = |name: &str, desc: &str| {
        let buffer_width = 32usize;
        print!("{}", name);
        print!("{}", " ".repeat(buffer_width.saturating_sub(name.len())));
        println!("{desc}");
    };

    for cmd in &app.commands {
        if cmd.mode.is_none() || cmd.mode == Some(app.get_mode()) {
            show(&cmd.format_usage(), cmd.description);
        }
    }
    Ok(())
}

pub fn clear(_: &[&str], _: &mut App) -> Result<(), String> {
    print!("\x1B[2J\x1B[1;1H");
    Ok(())
}

pub fn exit(_: &[&str], app: &mut App) -> Result<(), String> {
    app.cur_table = None;
    Ok(())
}

pub fn ls(_: &[&str], app: &mut App) -> Result<(), String> {
    if app.tables.is_empty() {
        println!("(no tables)");
    } else {
        for (name, table) in &app.tables {
            println!(
                "{} {}",
                name,
                format!("({}x{})", table.num_rows(), table.num_cols()).bright_black()
            );
        }
    }
    Ok(())
}

pub fn new(args: &[&str], app: &mut App) -> Result<(), String> {
    let name = args[0].to_string();
    if app.tables.contains_key(&name) {
        return Err("table already exists".into());
    }
    app.tables.insert(name, Table::default());
    Ok(())
}

pub fn rm(args: &[&str], app: &mut App) -> Result<(), String> {
    let name = args[0];
    if app.tables.remove(name).is_none() {
        return Err("table does not exist".to_string());
    }
    Ok(())
}

pub fn mv(args: &[&str], app: &mut App) -> Result<(), String> {
    let src = args[0];
    let dst = args[1];

    if src == dst {
        return Err("source and destination must be distinct".to_string());
    }

    let Some(table) = app.tables.remove(src) else {
        return Err("source table does not exist".to_string());
    };

    if app.tables.contains_key(dst) {
        app.tables.insert(src.to_string(), table);
        return Err("destination table already exists".to_string());
    }

    app.tables.insert(dst.to_string(), table);
    Ok(())
}

pub fn cp(args: &[&str], app: &mut App) -> Result<(), String> {
    let src = args[0];
    let dst = args[1];

    if src == dst {
        return Err("source and destination must be distinct".to_string());
    }

    let Some(src_table) = app.tables.get(src) else {
        return Err("source table does not exist".to_string());
    };

    if app.tables.contains_key(dst) {
        return Err("destination table already exists".to_string());
    }

    app.tables.insert(dst.to_string(), src_table.clone());
    Ok(())
}

pub fn open(args: &[&str], app: &mut App) -> Result<(), String> {
    let name = args[0].to_string();

    let was_new = !app.tables.contains_key(&name);
    app.tables.entry(name.clone()).or_default();
    app.cur_table = Some(name.clone());

    if was_new {
        println!("open: created new table \"{}\"", name);
    }

    Ok(())
}

pub fn load(args: &[&str], app: &mut App) -> Result<(), String> {
    let file = args[0].to_string();
    let table = args[1].to_string();

    if app.tables.contains_key(&table) {
        return Err("destination table already exists".to_string());
    }
    match Table::from_csv(file) {
        Ok((loaded_table, num_rows)) => {
            app.tables.insert(table, loaded_table);
            println!("load: successfully loaded {num_rows} row(s)");
            Ok(())
        }
        Err(e) => Err(format!("unable to load csv file: {e}")),
    }
}

pub fn save(args: &[&str], app: &mut App) -> Result<(), String> {
    let table = args[0].to_string();
    let file = args[1].to_string();

    if !app.tables.contains_key(&table) {
        return Err("source table does not exist".to_string());
    }
    let table = app.tables.get(&table).unwrap();
    let num_rows = table.num_rows();
    match table.to_csv(file) {
        Ok(()) => {
            println!("load: successfully saved {num_rows} row(s)");
            Ok(())
        }
        Err(e) => Err(format!("unable to save csv file: {e}")),
    }
}

pub fn close(_args: &[&str], app: &mut App) -> Result<(), String> {
    if let Some(table_name) = app.cur_table.take() {
        println!("closed table \"{}\"", table_name);
    }
    Ok(())
}

pub fn print(_args: &[&str], app: &mut App) -> Result<(), String> {
    let table = app.table_mut();
    print!("{}", table);
    let _ = app.stdout.flush();
    Ok(())
}

pub fn add_row(_args: &[&str], app: &mut App) -> Result<(), String> {
    let table = app.table_mut();
    table.add_row();
    println!(
        "add-row: successfully added row, {} total",
        table.num_rows()
    );
    Ok(())
}

pub fn add_rows(args: &[&str], app: &mut App) -> Result<(), String> {
    let count_str = args[0];
    let count: usize = count_str
        .parse()
        .map_err(|_| format!("\"{}\" is not a valid positive integer", count_str))?;

    if count == 0 {
        return Err("count must be at least 1".to_string());
    }

    let table = app.table_mut();
    for _ in 0..count {
        table.add_row();
    }

    println!(
        "add-rows: successfully added {} row(s), now {} row(s)",
        count,
        table.num_rows()
    );

    Ok(())
}

pub fn add_col(args: &[&str], app: &mut App) -> Result<(), String> {
    let col_name = args[0];
    let type_str = args[1].to_lowercase();

    let db_type = match type_str.to_ascii_lowercase().as_str() {
        "s" | "str" | "string" => DbType::String,
        "i" | "int" | "integer" => DbType::Integer,
        "b" | "bool" | "boolean" => DbType::Boolean,
        "d" | "double" => DbType::Double,
        _ => {
            return Err(
                "type must be one of: s / string, i / int, b / bool, d / double".to_string(),
            );
        }
    };

    let table = app.table_mut();

    if table.col_type(col_name).is_some() {
        return Err("column already exists".to_string());
    }

    table.add_col(col_name, db_type);

    println!(
        "add-col: successfully added column \"{}\", now {} column(s)",
        col_name,
        table.num_cols()
    );

    Ok(())
}

pub fn select(_: &[&str], app: &mut App) -> Result<(), String> {
    print!(
        "{}",
        "enter comma-separated list of columns (empty = all): ".cyan()
    );
    app.stdout.flush().unwrap();
    app.get_input();

    let input_cols = app.input.trim();
    let col_names: Vec<String> = if input_cols.is_empty() {
        app.table_mut()
            .iter_cols()
            .map(|c| c.name().to_string())
            .collect()
    } else {
        input_cols
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };

    if col_names.is_empty() {
        return Err("must specify at least one column (or leave empty for all)".to_string());
    }

    let mut col_ids = Vec::new();
    for name in col_names {
        if let Some(id) = app.table_mut().col_id(&name) {
            col_ids.push(id);
        } else {
            println!(
                "{}",
                format!("warning: column '{}' not found, skipping", name).yellow()
            );
        }
    }

    if col_ids.is_empty() {
        return Err("no valid columns selected".to_string());
    }

    let filter = if ask_yes_no(app, "add filter (y/n)? ") {
        build_filter_interactively(app)?
    } else {
        None
    };

    let mut order_by = Vec::new();

    if ask_yes_no(app, "add ordering (y/n)? ") {
        loop {
            print!("{}", "enter column to order by: ".cyan());
            app.stdout.flush().unwrap();
            app.get_input();
            let col_name = app.input.trim().to_string();

            let Some(col_id) = app.table_mut().col_id(&col_name) else {
                println!("{}", format!("no such column '{}'", col_name).yellow());
                continue;
            };

            let direction = loop {
                print!("{}", "order (asc/desc): ".cyan());
                app.stdout.flush().unwrap();
                app.get_input();
                match app.input.trim().to_ascii_lowercase().as_str() {
                    "asc" | "a" | "ascending" => break Order::Ascending,
                    "desc" | "d" | "descending" => break Order::Descending,
                    _ => println!("{}", "must be 'asc' or 'desc'".yellow()),
                }
            };

            order_by.push((col_id, direction));

            if !ask_yes_no(app, "add another order-by column (y/n)? ") {
                break;
            }
        }
    }

    let mut query = Select::new(col_ids);

    if let Some(f) = filter {
        query = query.filter(f);
    }

    for ord in order_by {
        query = query.order_by(ord.0, ord.1);
    }

    let view = app.table_mut().select(query);

    println!("{}", view);

    Ok(())
}

fn ask_yes_no(app: &mut App, prompt: &str) -> bool {
    loop {
        print!("{}", prompt.cyan());
        let _ = app.stdout.flush();
        app.get_input();
        match app.input.trim().to_ascii_lowercase().chars().next() {
            Some('y') | Some('Y') => return true,
            Some('n') | Some('N') => return false,
            _ => println!("{}", "please answer y or n".yellow()),
        }
    }
}

fn build_filter_interactively(app: &mut App) -> Result<Option<Filter>, String> {
    let mut filter: Option<Filter> = None;

    loop {
        print!(
            "{}",
            "enter filter: [COLUMN] [==|!=|>|<|>=|<=] [VALUE|null] ".cyan()
        );
        app.stdout.flush().unwrap();
        app.get_input();

        let line = app.input.trim().to_owned();
        if line.is_empty() {
            break;
        }

        let op_pos = line.find(['=', '!', '>', '<']);
        let Some(pos) = op_pos else {
            println!("{}", "format: COLUMN == VALUE  or  COLUMN > 10".yellow());
            continue;
        };

        let col_part = line[..pos].trim();
        let rest = &line[pos..];

        let (op, val_part) = if rest.starts_with("==") || rest.starts_with("!=") {
            (&rest[..2], rest[2..].trim())
        } else if rest.starts_with('>')
            || rest.starts_with('<')
            || rest.starts_with(">=")
            || rest.starts_with("<=")
        {
            if rest.len() >= 2 && (rest.starts_with(">=") || rest.starts_with("<=")) {
                (&rest[..2], rest[2..].trim())
            } else {
                (&rest[..1], rest[1..].trim())
            }
        } else {
            println!("{}", "unsupported operator".yellow());
            continue;
        };

        let Some(col_id) = app.table_mut().col_id(col_part) else {
            println!("{}", format!("unknown column '{}'", col_part).yellow());
            continue;
        };

        let value = if val_part.eq_ignore_ascii_case("null") {
            None
        } else if let Ok(b) = val_part.parse::<bool>() {
            Some(DbVal::Boolean(b))
        } else if let Ok(i) = val_part.parse::<i64>() {
            Some(DbVal::Integer(i))
        } else if let Ok(f) = val_part.parse::<f64>() {
            Some(DbVal::Double(f))
        } else {
            Some(DbVal::String(val_part.to_string()))
        };

        let new_filter = match (op, value) {
            ("==", None) => Filter::IsNull(col_id),
            ("!=", None) => Filter::IsNonNull(col_id),
            ("==", Some(v)) => Filter::Eq(col_id, v),
            ("!=", Some(v)) => Filter::Ne(col_id, v),
            (">", Some(v)) => Filter::Gt(col_id, v),
            ("<", Some(v)) => Filter::Lt(col_id, v),
            (">=", Some(v)) => Filter::Ge(col_id, v),
            ("<=", Some(v)) => Filter::Le(col_id, v),
            _ => {
                println!("{}", "invalid operator or value".yellow());
                continue;
            }
        };

        filter = if let Some(existing) = filter {
            Some(existing.and(new_filter))
        } else {
            Some(new_filter)
        };

        if !ask_yes_no(app, "add another condition with AND (y/n)? ") {
            break;
        }
    }

    Ok(filter)
}
