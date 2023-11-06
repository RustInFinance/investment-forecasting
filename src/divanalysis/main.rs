use calamine::{open_workbook, Xlsx};
use polars::prelude::*;

//TODO: CLAP
//TODO: Get ranking from remote or from file

const SP500DIVY: f64 = 1.61; // 2023, make it from CLI
const USINFLATION: f64 = 3.7; // 2023, make it from CLI
const USAVGINFL: f64 = 3.4;
const DIV_PAYOUT_MAX_THRESHOLD: f64 = 0.75;
const MIN_DIV_GROWTH: f64 = 10.0;
const MIN_DIV_YIELD: f64 = 4.7;
const MAX_DIV_YIELD: f64 = 10.0;

fn analyze_div_yield(
    df: &DataFrame,
    sp500_divy: f64,
    inflation: f64,
    min_divy: f64,
    max_divy: f64,
) -> Result<DataFrame, &'static str> {
    // Dividend Yield should:
    // 1. Be higher than inflation rate
    // 2. be higher than 1.5*S&P500 Div Yield rate
    // 3. No More than 10% (over 10% is suspecious, check their cash flow)
    let min_ref_sp500 = sp500_divy * 1.5;
    let mut minimal_accepted_divy = if min_ref_sp500 > inflation {
        min_ref_sp500
    } else {
        inflation
    };
    if min_divy > minimal_accepted_divy {
        minimal_accepted_divy = min_divy;
    };

    let divy_col = df
        .column("Div Yield")
        .map_err(|_| "Div Yield column does not exist!")?;

    let mask = divy_col
        .gt(minimal_accepted_divy)
        .map_err(|_| "Could not apply filtering data based on Div Yield and Inflation Div Yield")?;
    let mask2 = divy_col
        .lt_eq(max_divy)
        .map_err(|_| "Error creating filter of min_growth_rate")?;
    let mask = mask & mask2;

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

fn analyze_div_growth(df: &DataFrame, min_growth_rate: f64) -> Result<DataFrame, &'static str> {
    // Dividend growth rate
    // 1. 10% min (more or less) depending on historical growth

    let min_div_growth_5y_to_10y_ratio = 1.0;

    let cols = df
        .columns(&["DGR 1Y", "DGR 3Y", "DGR 5Y", "DGR 10Y"])
        .map_err(|_| "DGR (dividend growth) columns do not exist!")?;
    let mask = (cols[2] / cols[3])
        .gt_eq(&Series::new("", &[min_div_growth_5y_to_10y_ratio]))
        .unwrap();
    let mask2 = cols[0]
        .gt_eq(min_growth_rate)
        .map_err(|_| "Error creating filter of min_growth_rate")?;
    let mask = mask & mask2;

    let filtred_df = df.filter(&mask).expect("Error filtering");

    filtred_df
        .sort(["DGR 1Y"], true, false)
        .map_err(|_| "Could not sort along 'DGR 1Y'")
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
    investments_forecasting::init_logging_infrastructure();

    let mut excel: Xlsx<_> =
        open_workbook("data/U.S.DividendChampions-LIVE.xlsx").map_err(|_| "Error: opening XLSX")?;

    // Champions
    let champions = investments_forecasting::load_list(&mut excel, "Champions")?;

    // Pay-Date and Ex-Date are created , but why?
    log::info!("Champions: {}", champions);

    let champions_shortlisted_dy = analyze_div_yield(
        &champions,
        SP500DIVY,
        USINFLATION,
        MIN_DIV_YIELD,
        MAX_DIV_YIELD,
    )?;
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

    let champions_shortlisted_dy_dp_dg =
        analyze_div_growth(&champions_shortlisted_dy_dp, MIN_DIV_GROWTH)?;

    print_summary(&champions_shortlisted_dy_dp_dg)?;

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
        let max_divy = 10.0;
        let min_divy = 3.9;

        let s1 = Series::new("Symbol", &["ABM", "INTC", "CAT"]);
        let s2 = Series::new("Div Yield", &[5.54, 1.32, 4.0]);

        let df: DataFrame = DataFrame::new(vec![s1, s2]).unwrap();

        let s1 = Series::new("Symbol", &["ABM", "CAT"]);
        let s2 = Series::new("Div Yield", &[5.54, 4.0]);

        let ref_df: DataFrame = DataFrame::new(vec![s1, s2]).unwrap();

        let result = analyze_div_yield(&df, sp500_divy, inflation, min_divy, max_divy).unwrap();
        assert!(result.frame_equal(&ref_df));
        Ok(())
    }

    #[test]
    fn test_analyze_divy_min() -> Result<(), String> {
        let inflation = 3.4;
        let sp500_divy = 1.61;
        let max_divy = 10.0;
        let min_divy = 5.0;

        let s1 = Series::new("Symbol", &["ABM", "INTC", "CAT"]);
        let s2 = Series::new("Div Yield", &[9.0, 1.32, 4.0]);

        let df: DataFrame = DataFrame::new(vec![s1, s2]).unwrap();

        let s1 = Series::new("Symbol", &["ABM"]);
        let s2 = Series::new("Div Yield", &[9.0]);

        let ref_df: DataFrame = DataFrame::new(vec![s1, s2]).unwrap();

        let result = analyze_div_yield(&df, sp500_divy, inflation, min_divy, max_divy).unwrap();
        assert!(result.frame_equal(&ref_df));
        Ok(())
    }

    #[test]
    fn test_analyze_divy_max() -> Result<(), String> {
        let inflation = 3.4;
        let sp500_divy = 1.61;
        let max_divy = 10.0;
        let min_divy = 3.0;

        let s1 = Series::new("Symbol", &["ABM", "INTC", "CAT"]);
        let s2 = Series::new("Div Yield", &[11.0, 1.32, 4.0]);

        let df: DataFrame = DataFrame::new(vec![s1, s2]).unwrap();

        let s1 = Series::new("Symbol", &["CAT"]);
        let s2 = Series::new("Div Yield", &[4.0]);

        let ref_df: DataFrame = DataFrame::new(vec![s1, s2]).unwrap();

        let result = analyze_div_yield(&df, sp500_divy, inflation, min_divy, max_divy).unwrap();
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

    #[test]
    fn test_analyze_div_growth() -> Result<(), String> {
        let min_growth_rate = 7.0;

        let s1 = Series::new("Symbol", &["ABM", "INTC", "CAT"]);
        let s2 = Series::new("Div Yield", &[5.54, 1.32, 4.0]);
        let s3 = Series::new("Current Div", &[0.54, 1.62, 0.14]);
        let s4 = Series::new("CF/Share", &[10.0, 2.0, 20.0]);
        let s5 = Series::new("DGR 1Y", &[7.05, 0.68, 3.94]);
        let s6 = Series::new("DGR 3Y", &[8.51, 0.91, 3.07]);
        let s7 = Series::new("DGR 5Y", &[8.96, 3.36, 5.29]);
        let s8 = Series::new("DGR 10Y", &[8.87, 9.34, 4.97]);

        let df: DataFrame = DataFrame::new(vec![s1, s2, s3, s4, s5, s6, s7, s8]).unwrap();

        let s1 = Series::new("Symbol", &["ABM"]);
        let s2 = Series::new("Div Yield", &[5.54]);
        let s3 = Series::new("Current Div", &[0.54]);
        let s4 = Series::new("CF/Share", &[10.0]);
        let s5 = Series::new("DGR 1Y", &[7.05]);
        let s6 = Series::new("DGR 3Y", &[8.51]);
        let s7 = Series::new("DGR 5Y", &[8.96]);
        let s8 = Series::new("DGR 10Y", &[8.87]);
        let ref_df: DataFrame = DataFrame::new(vec![s1, s2, s3, s4, s5, s6, s7, s8]).unwrap();
        //print!("Ref DF: {ref_df}");

        let result = analyze_div_growth(&df, min_growth_rate).unwrap();
        //        print!("result DF: {result}");
        assert!(result.frame_equal(&ref_df));
        Ok(())
    }
}
