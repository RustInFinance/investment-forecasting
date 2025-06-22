use calamine::{open_workbook, Xlsx};
use clap::Parser;
use polars::prelude::*;

// TODO: convert dividends derived elements into TTM data
// TODO: Add support for Revenue and FCF
// TODO: Make payout ratio based on FCF
// TODO: fix all companies list
// TODO: make downloading all companies data
// TODO: Get polygon companies list (multiple pages) (next_url + api key reqwest has to be done)
// TODO: add ignoring non-complete data
// TODO: Make UK list supported
// TODO: Change to Result fully in get_polygon_data.

/// Program to help to analyze Dividend companies (Fetch XLSX list from: https://moneyzine.com/investments/dividend-champions/)
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Data in XLSX format (Fetch from https://moneyzine.com/investments/dividend-champions/)
    #[arg(long)]
    data: Option<String>,

    /// Name of the list with companies increasing dividends. Possible values: "Champions", "Contenders", "Challengers", "All"
    #[arg(long, default_value = "Champions")]
    list: String,

    /// List all available companies (from database if given or polygon in case of no given
    /// database
    #[arg(long)]
    list_all: bool,

    /// Symbol names of companies from dividend list if "data" is provided and from Polygon.io API
    /// when no "data" is given
    #[arg(long, default_values_t = &[] )]
    company: Vec<String>,

    /// List all available companies (from database if given or polygon in case of no given
    /// database
    #[arg(long = "continue", default_value_t = false, requires = "company")]
    cont: bool,

    /// csv file to read DataFrame from and to write to
    #[arg(long, default_value = None)]
    database: Option<String>,

    /// Average USA inflation during investment time[%]
    #[arg(long, default_value_t = 3.4)]
    inflation: f64,

    /// Minimum accepted Dividend Yield[%]
    #[arg(long, default_value_t = 4.7)]
    min_div_yield: f64,

    /// Maximum accepted Dividend Yield[%]
    #[arg(long, default_value_t = 10.0)]
    max_div_yield: f64,

    /// Minimum accepted Dividend Growth rate[%]
    #[arg(long, default_value_t = 10.0)]
    min_div_growth_rate: f64,

    /// Maximum accepted Dividend Payout rate[%]
    #[arg(long, default_value_t = 75.0)]
    max_div_payout_rate: f64,

    /// Standard and Poor 500 list's average DIV Yield[%]
    #[arg(long, default_value_t = 1.61)]
    sp500_divy: f64,
}

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

fn print_summary(df: &DataFrame, company: Option<&str>) -> Result<(), &'static str> {
    let dfs = match company {
        Some(company) => {
            let mask = df
                .column("Symbol")
                .map_err(|_| "Error: Unable to get Symbol")?
                .equal(company)
                .map_err(|_| "Error: Unable to create mask")?;
            df.filter(&mask)
                .map_err(|_| "Error: Unable to get Symbol")?
        }
        None => df.clone(),
    };
    if dfs.height() == 0 {
        return Err("Company symbol not present in selected List");
    }

    let mut selected_df = dfs
        .select(&["Symbol", "Company", "Current Div", "Div Yield", "Price"])
        .map_err(|_| "Unable to select mentioned columns!")?;
    log::info!("Selected companies: {selected_df}");

    let mut rate = dfs.column("Annualized").expect("No \"Current Div\" column")
        / dfs.column("CF/Share").expect("No \"CF/Share\" column")
        * 100.0;
    let rate = rate.rename("Div Payout Rate[%]");
    selected_df
        .with_column(rate.clone())
        .expect("Unable to add Rate column");
    println!("{selected_df}");
    Ok(())
}

fn configure_dataframes_format() {
    // Make sure to show all columns
    if std::env::var("POLARS_FMT_MAX_COLS").is_err() {
        std::env::set_var("POLARS_FMT_MAX_COLS", "12")
    }
    // Make sure to show all raws
    if std::env::var("POLARS_FMT_MAX_ROWS").is_err() {
        std::env::set_var("POLARS_FMT_MAX_ROWS", "-1")
    }
    // Make sure to show Full device name
    if std::env::var("POLARS_FMT_STR_LEN").is_err() {
        std::env::set_var("POLARS_FMT_STR_LEN", "200")
    }
}

fn get_polygon_companies_data(
    companies: &[String],
    database: Option<String>,
) -> Result<(), &'static str> {
    let mut symbols: Vec<&str> = vec![];
    let mut share_prices: Vec<f64> = vec![];
    let mut curr_divs: Vec<Option<f64>> = vec![];
    let mut divys: Vec<Option<f64>> = vec![];
    let mut freqs: Vec<Option<i64>> = vec![];
    let mut dgrs: Vec<Option<f64>> = vec![];
    let mut dgr5ys: Vec<Option<f64>> = vec![];
    let mut dgr3ys: Vec<Option<f64>> = vec![];
    let mut dgr1ys: Vec<Option<f64>> = vec![];
    let mut years_growth: Vec<Option<i64>> = vec![];
    let mut payout_ratios: Vec<Option<f64>> = vec![];
    let mut sectors: Vec<Option<String>> = vec![];

    let s1 = Series::new("Symbol", &symbols);
    let s2 = Series::new("Share Price", share_prices.clone());
    let s3 = Series::new("Recent Div", curr_divs.clone());
    let s4 = Series::new("Annual Frequency", freqs.clone());
    let s5 = Series::new("Div Yield[%]", divys.clone());
    let s6 = Series::new("DGR 1Y[%]", dgr1ys.clone());
    let s7 = Series::new("DGR 3Y[%]", dgrs.clone());
    let s8 = Series::new("DGR 5Y[%]", dgrs.clone());
    let s9 = Series::new("DGR 10Y[%]", dgrs.clone());
    let s10 = Series::new("Years of consecutive Div growth", years_growth.clone());
    let s11 = Series::new("Payout ratio[%]", payout_ratios.clone());
    let s12 = Series::new("Industry Desc", sectors.clone());
    let df: DataFrame = DataFrame::new(vec![
        s1.clone(),
        s2.clone(),
        s3.clone(),
        s4.clone(),
        s5.clone(),
        s6.clone(),
        s7.clone(),
        s8.clone(),
        s9.clone(),
        s10.clone(),
        s11.clone(),
        s12.clone(),
    ])
    .unwrap();

    let start_df = if let Some(database) = database.clone() {
        let file = std::fs::OpenOptions::new().read(true).open(&database);

        let df = if let Ok(file) = file {
            log::info!("Reading DataFrame from: {database} file");

            let read_df = CsvReader::new(file)
                .has_header(true)
                .finish()
                .map_err(|_| "Unable to read DataFrame from CSV file")?;

            read_df
                .vstack(&df)
                .map_err(|_| "Unable to combine data frames")?
        } else {
            log::info!("Creating a CSV file: {database} file to store DataFrame");
            df
        };
        df
    } else {
        df
    };

    let maybe_success = companies.iter().try_for_each(|symbol| {
        let (
            share_price,
            curr_div,
            divy,
            frequency,
            dgr,
            dgr5y,
            dgr3y,
            dgr1y,
            years_of_growth,
            payout_ratio,
            sector_desc,
        ) = investments_forecasting::get_polygon_data(&symbol)?;

        share_prices.push(share_price);
        curr_divs.push(curr_div);
        divys.push(divy);
        freqs.push(frequency);
        dgr5ys.push(dgr5y);
        dgr3ys.push(dgr3y);
        dgr1ys.push(dgr1y);
        dgrs.push(dgr);
        years_growth.push(years_of_growth);
        payout_ratios.push(payout_ratio);
        symbols.push(&symbol);
        sectors.push(sector_desc);

        if let Some(database) = database.clone() {
            let s1 = Series::new("Symbol", &symbols);
            let s2 = Series::new("Share Price", share_prices.clone());
            let s3 = Series::new("Recent Div", curr_divs.clone());
            let s4 = Series::new("Annual Frequency", freqs.clone());
            let s5 = Series::new("Div Yield[%]", divys.clone());
            let s6 = Series::new("DGR 1Y[%]", dgr1ys.clone());
            let s7 = Series::new("DGR 3Y[%]", dgr3ys.clone());
            let s8 = Series::new("DGR 5Y[%]", dgr5ys.clone());
            let s9 = Series::new("DGR 10Y[%]", dgrs.clone());
            let s10 = Series::new("Years of consecutive Div growth", years_growth.clone());
            let s11 = Series::new("Payout ratio[%]", payout_ratios.clone());
            let s12 = Series::new("Industry Desc", sectors.clone());

            let df: DataFrame = DataFrame::new(vec![
                s1.clone(),
                s2.clone(),
                s3.clone(),
                s4.clone(),
                s5.clone(),
                s6.clone(),
                s7.clone(),
                s8.clone(),
                s9.clone(),
                s10.clone(),
                s11.clone(),
                s12.clone(),
            ])
            .unwrap();

            let df = start_df
                .vstack(&df)
                .map_err(|_| "Unable to combine data frames")?;

            let mut df = df
                .sort(["Years of consecutive Div growth"], true, false)
                .unwrap();

            let mut file =
                std::fs::File::create(&database).map_err(|_| "Unable to create CSV file")?;

            CsvWriter::new(&mut file)
                .has_header(true)
                .finish(&mut df)
                .map_err(|_| "Unable to write DataFrame into CSV file")?;
            log::info!("DataFrame was written to: {database} file");
        }

        Ok::<(), &'static str>(())
    });

    match maybe_success {
        Ok(_) => log::info!("Acquiring of all companies via polygon succeeded!"),
        Err(e) => log::info!("Acquiring of all companies via polygon failed! Error: {e} . Partial results are available"),
    }
    let s1 = Series::new("Symbol", &symbols);
    let s2 = Series::new("Share Price", share_prices.clone());
    let s3 = Series::new("Recent Div", curr_divs.clone());
    let s4 = Series::new("Annual Frequency", freqs.clone());
    let s5 = Series::new("Div Yield[%]", divys.clone());
    let s6 = Series::new("DGR 1Y[%]", dgr1ys.clone());
    let s7 = Series::new("DGR 3Y[%]", dgr3ys.clone());
    let s8 = Series::new("DGR 5Y[%]", dgr5ys.clone());
    let s9 = Series::new("DGR 10Y[%]", dgrs.clone());
    let s10 = Series::new("Years of consecutive Div growth", years_growth.clone());
    let s11 = Series::new("Payout ratio[%]", payout_ratios.clone());
    let s12 = Series::new("Industry Desc", sectors.clone());

    let df: DataFrame = DataFrame::new(vec![
        s1.clone(),
        s2.clone(),
        s3.clone(),
        s4.clone(),
        s5.clone(),
        s6.clone(),
        s7.clone(),
        s8.clone(),
        s9.clone(),
        s10.clone(),
        s11.clone(),
        s12.clone(),
    ])
    .unwrap();

    let df = start_df
        .vstack(&df)
        .map_err(|_| "Unable to combine data frames")?;

    let df = df
        .sort(["Years of consecutive Div growth"], true, false)
        .unwrap();

    println!("{df}");

    Ok(())
}

fn main() -> Result<(), &'static str> {
    investments_forecasting::init_logging_infrastructure();

    configure_dataframes_format();

    let args = Args::parse();

    let data = if let Some(data_file) = args.data {
        let mut excel: Xlsx<_> = open_workbook(data_file).map_err(|_| "Error: opening XLSX")?;
        // Champions
        let data = investments_forecasting::load_list(&mut excel, &args.list)?;
        Some(data)
    } else {
        None
    };

    //let company = <std::string::String as AsRef<str>>::as_ref(&args.company).to_uppercase();
    let companies = args
        .company
        .iter()
        .map(|x| x.to_uppercase())
        .collect::<Vec<String>>();
    // For no handpicked companies just make overall analysis
    if companies.len() == 0 {
        if args.list_all {
            match data {
                Some(database) => print_summary(&database, None)?,
                None => {
                    let companies = investments_forecasting::get_polygon_companies_list()?;

                    let mut symbols: Vec<String> = vec![];
                    let mut names: Vec<Option<String>> = vec![];

                    companies.into_iter().for_each(|(s, n)| {
                        symbols.push(s);
                        names.push(n);
                    });

                    let s1 = Series::new("Symbol", &symbols);
                    let s2 = Series::new("Company", &names);
                    let df: DataFrame = DataFrame::new(vec![s1, s2]).unwrap();
                    println!("{df}");
                }
            }
        } else {
            match data {
                Some(data) => {
                    let data_shortlisted_dy = analyze_div_yield(
                        &data,
                        args.sp500_divy,
                        args.inflation,
                        args.min_div_yield,
                        args.max_div_yield,
                    )?;
                    log::info!("Champions Shortlisted by DivY: {}", data_shortlisted_dy);

                    let data_shortlisted_dy_dp = analyze_dividend_payout_rate(
                        &data_shortlisted_dy,
                        args.max_div_payout_rate / 100.0,
                    )?;

                    log::info!(
                        "Champions Shortlisted by DivY and Div Pay-Out: {}",
                        data_shortlisted_dy_dp
                    );

                    let data_shortlisted_dy_dp_dg =
                        analyze_div_growth(&data_shortlisted_dy_dp, args.min_div_growth_rate)?;

                    print_summary(&data_shortlisted_dy_dp_dg, None)?;
                }
                None => {
                    let companies = investments_forecasting::get_polygon_companies_list()?;

                    let mut symbols: Vec<String> = vec![];

                    companies.into_iter().for_each(|(s, _)| {
                        symbols.push(s);
                    });
                    get_polygon_companies_data(&symbols, args.database)?;
                }
            }
        }
    } else {
        match data {
            Some(data) => {
                companies
                    .iter()
                    .try_for_each(|symbol| print_summary(&data, Some(&symbol)))?;
            }
            None => {
                // If we to continue then we get list of all companies
                // and start execution from company being a value of argument "company"
                let companies = if args.cont {
                    let company_to_start = companies[0].clone();
                    let companies = investments_forecasting::get_polygon_companies_list()?;
                    let companies: Vec<String> = companies.iter().map(|(s, _)| s.clone()).collect();
                    let company_to_start_index = companies
                        .iter()
                        .position(|x| x == &company_to_start)
                        .ok_or("Error creating filter of min_growth_rate")?;

                    let companies = companies
                        .iter()
                        .enumerate()
                        .filter(|&(index, _)| index >= company_to_start_index)
                        .map(|(_, company)| company.clone())
                        .collect();
                    companies
                } else {
                    companies
                };
                get_polygon_companies_data(&companies, args.database)?;
            }
        }
    }
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
