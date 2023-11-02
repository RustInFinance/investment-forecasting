use calamine::{open_workbook, Reader, Xlsx};
use polars::prelude::*;
use std::collections::HashMap;

mod logging;
use logging::ResultExt;

//1) Stopa dywidendy (dywidenda / cena_akcji * 100%)
// - 1.5 - 2 x wzgledem S&P 500 stopy dywidendy
// - wyzsza niz inflacja (srednia inflacja historyczna 3.4% w USA)
// - 10% i wiecej to trzeba mocno sprawdzic stopa wyplaty dywidendy
// - zaleca 4.7%
//2) Stopa wyplat dywidendy (wyplycona dywidenda / zysk netto - licozny od przeplywow)
//  - nie wiecej niz 75% (chyba ze spolki REIT, komandytowo-akcyjne)
//3) stopa wzrostu dywidendy (http://dripinvesting.org)
//  - zaleca 10%

fn analyze<R>(excel: &mut Xlsx<R>, category: &str) -> Result<(), &'static str>
where
    R: std::io::BufRead,
    R: std::io::Read,
    R: std::io::Seek,
{
    log::info!("Processing category: {}", category);
    let names = excel.sheet_names();
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
            //log::info!("{:?}",row);

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
        //        log::info!("Total DF_series {:?}", df_series);
        df = DataFrame::new(df_series).map_err(|_| "Error: Could not create DataFrame")?;
    }
    // Pay-Date and Ex-Date are created , but why?
    log::info!("DATA: {}", df);

    Ok(())
}

fn main() -> Result<(), &'static str> {
    println!("Hello financial analysis world!");
    logging::init_logging_infrastructure();

    let mut excel: Xlsx<_> =
        open_workbook("data/U.S.DividendChampions-LIVE.xlsx").map_err(|_| "Error: opening XLSX")?;

    // Champions
    analyze(&mut excel, "Champions")?;
    // Contenders

    Ok(())
}
