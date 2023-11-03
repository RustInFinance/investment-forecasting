use calamine::{open_workbook, Reader, Xlsx};
use polars::prelude::*;
use std::collections::HashMap;

mod logging;
use logging::ResultExt;

//TODO: CLAP
//TODO: Get ranking from remote or from file
//TODO: 10% Div Y or more is subject to be suspecious

//1) Stopa dywidendy (dywidenda / cena_akcji * 100%)
// - 1.5 - 2 x wzgledem S&P 500 stopy dywidendy
// - wyzsza niz inflacja (srednia inflacja historyczna 3.4% w USA)
// - 10% i wiecej to trzeba mocno sprawdzic stopa wyplaty dywidendy
// - zaleca 4.7%
//2) Stopa wyplat dywidendy (wyplycona dywidenda / zysk netto - licozny od przeplywow)
//  - nie wiecej niz 75% (chyba ze spolki REIT, komandytowo-akcyjne)
//3) stopa wzrostu dywidendy (http://dripinvesting.org)
//  - zaleca 10%

const SP500DIVY: f64 = 1.61; // 2023, make it from CLI
const USINFLATION: f64 = 3.7; // 2023, make it from CLI
const USAVGINFL: f64 = 3.4;
const DIV_PAYOUT_MAX_THRESHOLD: f64 = 0.75;

fn load_list<R>(excel: &mut Xlsx<R>, category: &str) -> Result<DataFrame, &'static str>
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
        df = DataFrame::new(df_series).map_err(|_| "Error: Could not create DataFrame")?;
    }

    Ok(df)
}

fn analyze_div_yield(
    df: &DataFrame,
    sp500_divy: f64,
    inflation: f64,
) -> Result<DataFrame, &'static str> {
    // Dividend Yield should:
    // 1. Be higher than inflation rate
    // 2. be higher than 1.5*S&P500 Div Yield rate
    // 3. More than 10% is suspecious (check their cash flow)
    let min_ref_sp500 = sp500_divy * 1.5;
    let minimal_accepted_divy = if min_ref_sp500 > inflation {
        min_ref_sp500
    } else {
        inflation
    };
    let mask = df
        .column("Div Yield")
        .map_err(|_| "Div Yield column does not exist!")?
        .gt(minimal_accepted_divy)
        .map_err(|_| "Could not apply filtering data based on Div Yield and Inflation Div Yield")?;
    let filtred_df = df.filter(&mask).expect("Error filtering");

    filtred_df
        .sort(["Div Yield"], true, false)
        .map_err(|_| "Could not sort along 'Div Yield'")
}

fn analyze_dividend_payout_rate(
    df: &DataFrame,
    max_threshold: f64,
) -> Result<DataFrame, &'static str> {
    // Dividend Payout rate
    // 1. Is Current Div / Cash flow per share e.g. 0.22 / 1.7  = 0.129412
    // 2. No more than 75%

    let cols = df
        .columns(&["Current Div", "CF/Share"])
        .map_err(|_| "Current Div and/or CF/Share columns do not exist!")?;
    let mask = (cols[0] / cols[1])
        .lt(&Series::new("", &[max_threshold]))
        .unwrap();
    let filtred_df = df.filter(&mask).expect("Error filtering");

    filtred_df
        .sort(["Div Yield"], true, false)
        .map_err(|_| "Could not sort along 'Div Yield'")
}

fn print_summary(df: &DataFrame) -> Result<(), &'static str> {
    let selected_df = df
        .select(&["Symbol", "Company", "Current Div", "Div Yield", "Price"])
        .map_err(|_| "Unable to select mentioned columns!")?;
    println!("{selected_df}");
    Ok(())
}

fn main() -> Result<(), &'static str> {
    println!("Hello financial analysis world!");
    logging::init_logging_infrastructure();

    let mut excel: Xlsx<_> =
        open_workbook("data/U.S.DividendChampions-LIVE.xlsx").map_err(|_| "Error: opening XLSX")?;

    // Champions
    let champions = load_list(&mut excel, "Champions")?;

    // Pay-Date and Ex-Date are created , but why?
    log::info!("Champions: {}", champions);

    let champions_shortlisted_dy = analyze_div_yield(&champions, SP500DIVY, USINFLATION)?;
    log::info!(
        "Champions Shortlisted by DivY: {}",
        champions_shortlisted_dy
    );

    let champions_shortlisted_dy_dp =
        analyze_dividend_payout_rate(&champions_shortlisted_dy, DIV_PAYOUT_MAX_THRESHOLD)?;

    log::info!(
        "Champions Shortlisted by DivY and Div Pay-Out: {}",
        champions_shortlisted_dy_dp
    );

    print_summary(&champions_shortlisted_dy_dp)?;

    // Contenders

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analyze_divy() -> Result<(), String> {
        let inflation = 3.4;
        let sp500_divy = 1.61;

        let s1 = Series::new("Symbol", &["ABM", "INTC", "CAT"]);
        let s2 = Series::new("Div Yield", &[5.54, 1.32, 4.0]);

        let df: DataFrame = DataFrame::new(vec![s1, s2]).unwrap();

        let s1 = Series::new("Symbol", &["ABM", "CAT"]);
        let s2 = Series::new("Div Yield", &[5.54, 4.0]);

        let ref_df: DataFrame = DataFrame::new(vec![s1, s2]).unwrap();

        let result = analyze_div_yield(&df, sp500_divy, inflation).unwrap();
        assert!(result.frame_equal(&ref_df));
        Ok(())
    }

    #[test]
    fn test_analyze_divy_dpy() -> Result<(), String> {
        let max_payout_rate = 0.75;

        let s1 = Series::new("Symbol", &["ABM", "INTC", "CAT"]);
        let s2 = Series::new("Div Yield", &[5.54, 1.32, 4.0]);
        let s3 = Series::new("Current Div", &[0.54, 1.62, 0.14]);
        let s4 = Series::new("CF/Share", &[10.0, 2.0, 20.0]);

        let df: DataFrame = DataFrame::new(vec![s1, s2, s3, s4]).unwrap();

        let s1 = Series::new("Symbol", &["ABM", "CAT"]);
        let s2 = Series::new("Div Yield", &[5.54, 4.0]);
        let s3 = Series::new("Current Div", &[0.54, 0.14]);
        let s4 = Series::new("CF/Share", &[10.0, 20.0]);

        let ref_df: DataFrame = DataFrame::new(vec![s1, s2, s3, s4]).unwrap();
        //print!("Ref DF: {ref_df}");

        let result = analyze_dividend_payout_rate(&df, max_payout_rate).unwrap();
        //print!("result DF: {result}");
        assert!(result.frame_equal(&ref_df));
        Ok(())
    }
}
