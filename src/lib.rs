use calamine::{Reader, Xlsx};
use polars::prelude::*;
use std::fmt;

use std::collections::HashMap;

pub fn load_list<R>(excel: &mut Xlsx<R>, category: &str) -> Result<DataFrame, &'static str>
where
    R: std::io::BufRead,
    R: std::io::Read,
    R: std::io::Seek,
{
    log::info!("Processing category: {}", category);
    let names = excel.sheet_names();
    log::info!("Available categories: {:?}", names);
    let name_sheet = names
        .iter()
        .find(|x| *x == category)
        .ok_or("Error: Category not found")?;

    // Dividend Yield
    // Dividend
    // Share price
    // sector
    let mut df = DataFrame::default();
    if let Some(Ok(r)) = excel.worksheet_range(&name_sheet) {
        let mut rows = r.rows();

        // Rewind to actual categories
        rows.next();
        rows.next();

        let categories = rows
            .next()
            .expect_and_log("Error: unable to get descriptive row");
        //       let mut symbol = 0;

        let mut columns: Vec<&str> = Vec::default();
        let mut sseries: HashMap<usize, Vec<Option<&str>>> = HashMap::new();
        let mut fseries: HashMap<usize, Vec<Option<f64>>> = HashMap::new();
        for c in categories {
            // Find indices of interesting collumns
            if let Some(v) = c.get_string() {
                columns.push(v);
            } else if c.is_empty() {
                columns.push("Blended"); // Blended info got empty name of column
            }
        }
        log::info!("Columns: {:?}", columns);

        // Iterate through rows of actual sold transactions
        for row in rows {

            for (i, cell) in row.iter().enumerate() {
                match cell {
                    calamine::DataType::Float(f) => {
                        if fseries.contains_key(&i) {
                            let vf = fseries
                                .get_mut(&i)
                                .ok_or("Error: accessing invalid category")?;
                            vf.push(Some(*f));
                        } else {
                            fseries.insert(i, vec![Some(*f)]);
                        }
                    }
                    calamine::DataType::String(s) => {
                        if sseries.contains_key(&i) {
                            let vf = sseries
                                .get_mut(&i)
                                .ok_or("Error: accessing invalid category")?;
                            vf.push(Some(s));
                        } else {
                            if s != "" {
                                sseries.insert(i, vec![Some(s)]);
                            } else {
                                // If empty field then it maybe a missing data
                                log::warn!("Missing data at row: {:?}", row);
                                if fseries.contains_key(&i) {
                                    let vf = fseries
                                        .get_mut(&i)
                                        .ok_or("Error: accessing invalid category")?;
                                    vf.push(None);
                                } else {
                                    log::error!("Error: incomplete data. Please update manualy");
                                }
                            }
                        }
                    }
                    calamine::DataType::DateTime(s) => {
                        if fseries.contains_key(&i) {
                            let vf = fseries
                                .get_mut(&i)
                                .ok_or("Error: accessing invalid category")?;
                            vf.push(Some(*s));
                        } else {
                            fseries.insert(i, vec![Some(*s)]);
                        }
                    }
                    calamine::DataType::Empty => {
                        // If empty field then it maybe a missing data
                        log::warn!("Missing data at row: {:?}", row);
                        if fseries.contains_key(&i) {
                            let vf = fseries
                                .get_mut(&i)
                                .ok_or("Error: accessing invalid category")?;
                            vf.push(None);
                        } else if sseries.contains_key(&i) {
                            let vf = sseries
                                .get_mut(&i)
                                .ok_or("Error: accessing invalid category")?;
                            vf.push(None);
                        } else {
                            sseries.insert(i, vec![None]);
                        }
                    }
                    _ => (),
                }
            }
        }

        // Build DataFrame
        let mut df_series: Vec<Series> = vec![];
        fseries.iter().for_each(|(k, v)| {
            let s = Series::new(columns[*k], v.iter());
            df_series.push(s);
        });
        sseries.iter().for_each(|(k, v)| {
            let s = Series::new(columns[*k], v);
            df_series.push(s);
        });
        df = DataFrame::new(df_series).map_err(|msg| { log::error!("DF error: {msg}") ;"Error: Could not create DataFrame"})?;
    }

    Ok(df)
}

// Let's extend Result with logging
pub trait ResultExt<T> {
    fn expect_and_log(self, msg: &str) -> T;
}

impl<T, E: fmt::Debug> ResultExt<T> for Result<T, E> {
    fn expect_and_log(self, err_msg: &str) -> T {
        self.map_err(|e| {
            log::error!("{}", err_msg);
            e
        })
        .expect(err_msg)
    }
}

impl<T> ResultExt<T> for Option<T> {
    fn expect_and_log(self, err_msg: &str) -> T {
        self.or_else(|| {
            log::error!("{}", err_msg);
            None
        })
        .expect(err_msg)
    }
}

#[allow(dead_code)]
pub fn init_logging_infrastructure() {
    // Make a default logging level: error
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "error")
    }
    simple_logger::SimpleLogger::new().env().init().unwrap();
}
