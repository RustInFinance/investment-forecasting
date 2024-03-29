use calamine::{Reader, Xlsx};
use polars::prelude::*;
use std::fmt;

use chrono::prelude::*;

use polygon_client::rest::RESTClient;
use std::collections::BTreeMap;
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
        df = DataFrame::new(df_series).map_err(|msg| {
            log::error!("DF error: {msg}");
            "Error: Could not create DataFrame"
        })?;
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
fn should_try_again<T>(maybe_resp: Result<T, reqwest::Error>, dummy: T) -> (T, bool) {
    match maybe_resp {
        Ok(r) => (r, false),
        Err(e) => {
            log::info!("Error: {:?}", e.status());
            let repeat = if let Some(status) = e.status() {
                if status == 429 {
                    println!("Waiting for 30 s and rerunning query");
                    let thirty_secs = std::time::Duration::new(30, 0);
                    std::thread::sleep(thirty_secs);
                    true
                } else {
                    panic!("POLYGON API: failed to query tickers");
                }
            } else {
                panic!("POLYGON API: failed to query tickers");
            };
            (dummy, repeat)
        }
    }
}

pub fn get_polygon_companies_list() -> Result<Vec<(String, String)>, &'static str> {
    let mut query_params = HashMap::new();
    query_params.insert("active", "true");

    let client = RESTClient::new(None, None);
    // Get all dividend data we can have
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut run = true;
            let mut resp = polygon_client::types::ReferenceTickersResponse {
                next_url: None,
                results: vec![],
                status: "OK".to_owned(),
                count: 0,
                request_id: "".to_owned(),
            };

            while run {
                let maybe_resp = client.reference_tickers(&query_params).await;
                log::info!("RESPONSE(LIST COMPANIES): {maybe_resp:#?}");
                (resp, run) = should_try_again(maybe_resp, resp);
            }

            let tickers_results_to_vec =
                |results: &Vec<polygon_client::types::ReferenceTickersResponseTickerV3>| {
                    let mut companies: Vec<(String, String)> = results
                        .iter()
                        .map(|x| {
                            log::info!("{}: name: {}, type: {}", x.ticker, x.name, x.market);
                            (x.ticker.clone(), x.name.clone())
                        })
                        .collect();
                    companies
                };

            let mut companies: Vec<(String, String)> = tickers_results_to_vec(&resp.results);

            while resp.next_url.clone().is_some() {
                if let Some(url) = &resp.next_url.clone() {
                    run = true;
                    while run {
                        let maybe_resp = client.fetch_next_page(url).await;
                        log::info!("RESPONSE NEXT PAGE (LIST COMPANIES): {maybe_resp:#?}");
                        (resp, run) = should_try_again(maybe_resp, resp);
                    }
                    // Here let's attach
                    companies.append(&mut tickers_results_to_vec(&resp.results));
                }
            }

            return Ok::<Vec<(String, String)>, &'static str>(companies);
        })
}

async fn get_dividiend_data(
    client: &RESTClient,
    query_params: &HashMap<&str, &str>,
) -> Result<(f64, f64, u32, Vec<(String, f64)>), &'static str> {
    let dividends_results_to_vec =
        |results: &Vec<polygon_client::types::ReferenceStockDividendsResultV3>| {
            let div_history: Vec<(String, f64)> = results
                .iter()
                .map(|x| {
                    log::info!(
                        "{}: ex date: {}, payment date: {}, frequency: {}, div type: {} amount: {}",
                        x.ticker,
                        x.ex_dividend_date,
                        x.pay_date,
                        x.frequency,
                        x.dividend_type,
                        x.cash_amount
                    );
                    (x.pay_date.clone(), x.cash_amount)
                })
                .collect();
            div_history
        };

    let mut run = true;
    let mut resp = polygon_client::types::ReferenceStockDividendsResponse {
        next_url: None,
        results: vec![],
        status: "OK".to_owned(),
    };

    while run {
        let maybe_resp = client.reference_stock_dividends(&query_params).await;
        log::info!("RESPONSE(DIVIDENDS): {maybe_resp:#?}");
        (resp, run) = should_try_again(maybe_resp, resp);
    }

    let mut div_history: Vec<(String, f64)> = dividends_results_to_vec(&resp.results);
    while resp.next_url.clone().is_some() {
        if let Some(url) = &resp.next_url.clone() {
            run = true;
            while run {
                let maybe_resp = client.fetch_next_page(url).await;
                log::info!("RESPONSE NEXT PAGE (DIVIDENDS): {maybe_resp:#?}");
                (resp, run) = should_try_again(maybe_resp, resp);
            }
            // Here let's attach
            div_history.append(&mut dividends_results_to_vec(&resp.results));
        }
    }

    div_history.sort_by(|a, b| {
        let a_date = NaiveDate::parse_from_str(&a.0, "%Y-%m-%d").expect("unable to parse date");
        let b_date = NaiveDate::parse_from_str(&b.0, "%Y-%m-%d").expect("unable to parse date");
        a_date.cmp(&b_date)
    });

    log::info!("Ordered dividends: {div_history:#?}");

    let current_year = Utc::now().year();
    let num_years_of_interest = 5;
    let div_history = div_history
        .into_iter()
        .filter(|x| {
            let x_date_year = NaiveDate::parse_from_str(&x.0, "%Y-%m-%d")
                .expect("unable to parse date")
                .year();
            if (current_year - x_date_year) <= num_years_of_interest {
                true
            } else {
                false
            }
        })
        .collect::<Vec<_>>();

    // Curr Dividend  and corressponding date
    let (curr_div, curr_div_date) = match div_history.iter().rev().next() {
        Some((pay_date, cash_amount)) => (
            cash_amount,
            NaiveDate::parse_from_str(&pay_date, "%Y-%m-%d").expect("Wrong payout date format"),
        ),
        None => panic!("No dividend Data!"),
    };
    let (currency, frequency) = if resp.results.len() > 0 {
        (resp.results[0].currency.clone(), resp.results[0].frequency)
    } else {
        panic!("No dividend Data!");
    };

    let dgr = calculate_dgr(&div_history, Utc::now().year().to_string().as_ref())?;
    log::info!("Current Div: {curr_div} {currency}, Paid date: {curr_div_date},  Frequency: {frequency}, Average DGR(samples: {}): {dgr}",
            div_history.len());

    Ok((*curr_div, dgr, frequency, div_history))
}

pub fn get_polygon_data(company: &str) -> Result<(f64, f64, f64, u32, Option<f64>), &'static str> {
    let mut query_params = HashMap::new();
    query_params.insert("ticker", company);

    let client = RESTClient::new(None, None);
    // Get all dividend data we can have
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let (curr_div, dgr, frequency, div_history) =
                get_dividiend_data(&client, &query_params).await?;

            let mut close_query_params = HashMap::new();
            close_query_params.insert("adjusted", "true");

            let mut run = true;
            let mut resp = polygon_client::types::StockEquitiesPreviousCloseResponse {
                ticker: "".to_owned(),
                results: vec![],
                count: 0,
                query_count: 0,
                results_count: 0,
                status: "OK".to_owned(),
                adjusted: false,
            };
            while run {
                let maybe_resp = client
                    .stock_equities_previous_close(company, &HashMap::new())
                    .await;
                log::info!("RESPONSE(STOCK EQUITIES): {resp:#?}");
                (resp, run) = should_try_again(maybe_resp, resp);
            }

            let prev_day_share_data = resp
                .results
                .iter()
                .next()
                .ok_or("Error reading previous date share price")?;
            let share_price = prev_day_share_data.c;

            let divy = calculate_divy(
                &div_history,
                share_price,
                Utc::now().year().to_string().as_ref(),
            )?;
            log::info!("Stock price: {share_price}, Div Yield[%]: {divy:.2}");

            let years_of_growth = calculate_consecutive_years_of_growth(
                &div_history,
                Utc::now().year().to_string().as_ref(),
            )?;
            log::info!("Consecutive years of dividend growth: {years_of_growth}");

            run = true;
            let mut resp = polygon_client::types::ReferenceStockFinancialsVXResponse {
                next_url: None,
                results: vec![],
                status: "OK".to_owned(),
                request_id: None,
            };
            while run {
                let maybe_resp = client.reference_stock_financials_vx(&query_params).await;
                log::info!("RESPONSE(STOCK FINANCIALS): {resp:#?}");
                (resp, run) = should_try_again(maybe_resp, resp);
            }

            let payout_rate = match get_quaterly_payout_rate(&resp, &div_history) {
                Ok(payout_rate) => Some(payout_rate),
                Err(_) => get_annual_payout_rate(&resp, &div_history)?,
            };

            return Ok::<(f64, f64, f64, u32, Option<f64>), &'static str>((
                curr_div,
                divy,
                dgr,
                years_of_growth,
                payout_rate,
            ));
        })
}

fn get_net_cash_flow(
    fd: &polygon_client::types::FinancialDimensions,
    company_name: &str,
    fiscal_year: &str,
    fiscal_period: &str,
) -> Result<f64, &'static str> {
    let net_value = if let Some(ismap) = &fd.cash_flow_statement {
        let net_value = if ismap.contains_key("net_cash_flow_from_operating_activities") {
            let net_cash_flow = ismap
                .get("net_cash_flow_from_operating_activities")
                .expect("Error getting net_cash_flow_from_operating_activities");
            let net_value = net_cash_flow.value.clone().unwrap();
            let net_unit = net_cash_flow.unit.clone().unwrap();
            let net_label = net_cash_flow.label.clone().unwrap();
            log::info!(
                "{}: {} {} net cash flow: {} of {}, labeled as {}",
                company_name,
                fiscal_year,
                fiscal_period,
                net_value,
                net_unit,
                net_label
            );

            // curr_div * num_shares  / net_value
            net_value
        } else {
            return Err("Missing net_cash_flow_from_operating_activities");
        };
        net_value
    } else {
        return Err("Implement missing cash flow statement");
    };
    Ok(net_value)
}

fn get_basic_average_shares(
    fd: &polygon_client::types::FinancialDimensions,
    company_name: &str,
    fiscal_year: &str,
    fiscal_period: &str,
) -> Result<Option<f64>, &'static str> {
    let basic_average_shares = if let Some(ismap) = &fd.income_statement {
        let basic_average_shares = if ismap.contains_key("basic_average_shares") {
            let basic_average_shares = ismap
                .get("basic_average_shares")
                .expect("Error getting basic_average_shares");
            let value = basic_average_shares.value.clone().unwrap();
            let unit = basic_average_shares.unit.clone().unwrap();
            let label = basic_average_shares.label.clone().unwrap();
            log::info!(
                "{}: {} {} basic average shares: {} of {}, labeled as {}",
                company_name,
                fiscal_year,
                fiscal_period,
                value,
                unit,
                label
            );
            Some(value)
        } else {
            None
        };
        basic_average_shares
    } else {
        todo!("Implement missing net_cash_flow_continuing");
    };
    Ok(basic_average_shares)
}

fn calculate_annualized_div(
    div_history: &Vec<(String, f64)>,
    fiscal_year: &str,
) -> Result<f64, &'static str> {
    let annuallized_div = div_history
        .iter()
        .filter(|x| {
            NaiveDate::parse_from_str(&x.0, "%Y-%m-%d")
                .expect("Dividend date parsing error")
                .year()
                == fiscal_year
                    .parse::<i32>()
                    .expect("Unable to parse fiscal year")
        })
        .fold(0.0, |mut acc, num| {
            acc += num.1;
            acc
        });
    Ok(annuallized_div)
}

/// Calculate consecutive years of growing dividend, not including current year
fn calculate_consecutive_years_of_growth(
    div_history: &Vec<(String, f64)>,
    current_year: &str,
) -> Result<u32, &'static str> {
    let current_year = current_year
        .parse::<i32>()
        .expect("Unable to parse currrent year");
    let mut annual_div: BTreeMap<i32, f64> = BTreeMap::new();

    div_history.iter().try_for_each(|x| {
        let year = NaiveDate::parse_from_str(&x.0, "%Y-%m-%d")
            .map_err(|_| "Error parsing dividend year")?
            .year();
        // Skip current year (no full data yet)
        if year != current_year {
            let possible_sum = annual_div.get_mut(&year);
            match possible_sum {
                Some(s) => *s += x.1,
                None => {
                    annual_div.insert(year, x.1);
                    ()
                }
            }
        }
        Ok::<(), &str>(())
    })?;

    let mut num_consecutive_years = 0;
    let mut from_newer_to_older = annual_div.iter().rev();
    let (next_year, mut next_year_div) = from_newer_to_older
        .next()
        .ok_or("Error: unable to get devidend")?;
    log::info!("Annual dividend year: {next_year} annual_div: {next_year_div}");
    'petla: for (year, sum) in from_newer_to_older {
        log::info!("Annual dividend year: {year} annual_div: {sum}");
        if sum < next_year_div {
            num_consecutive_years += 1;
            next_year_div = sum;
        } else {
            break 'petla;
        }
    }

    Ok(num_consecutive_years)
}

fn get_annual_payout_rate(
    resp: &polygon_client::types::ReferenceStockFinancialsVXResponse,
    div_history: &Vec<(String, f64)>,
) -> Result<Option<f64>, &'static str> {
    // Pick the most recent annual report
    let res = resp
        .results
        .iter()
        .filter(|x| x.timeframe == "annual")
        .max_by(|x, y| {
            let x_date = NaiveDate::parse_from_str(
                &x.end_date.clone().expect("Missing end date"),
                "%Y-%m-%d",
            )
            .expect("Wrong end date format");
            let y_date = NaiveDate::parse_from_str(
                &y.end_date.clone().expect("Missing end date"),
                "%Y-%m-%d",
            )
            .expect("Wrong end date format");
            x_date.cmp(&y_date)
        });

    if let Some(r) = res {
        log::info!(
            "{:?}: start date: {:?}, end date: {:?}, fiscal_year: {}, timeframe: {} fiscal_period: {}",
            r.tickers,
            r.start_date,
            r.end_date,
            r.fiscal_year,
            r.timeframe,
            r.fiscal_period
        );
        // Div payout dates must come from chosen fiscal year
        let annuallized_div = calculate_annualized_div(div_history, &r.fiscal_year)?;

        let net_value = get_net_cash_flow(
            &r.financials,
            r.company_name.as_ref(),
            r.fiscal_year.as_ref(),
            r.fiscal_period.as_ref(),
        )?;
        let basic_average_shares = get_basic_average_shares(
            &r.financials,
            r.company_name.as_ref(),
            r.fiscal_year.as_ref(),
            r.fiscal_period.as_ref(),
        )?;
        let payout_rate = match basic_average_shares {
            Some(num_shares) => Some(calculate_payout_ratio(
                annuallized_div,
                num_shares,
                net_value,
            )?),
            None => None,
        };
        Ok(payout_rate)
    } else {
        log::info!("No annual financial report found");
        return Ok(None);
    }
}

fn get_quaterly_payout_rate(
    resp: &polygon_client::types::ReferenceStockFinancialsVXResponse,
    div_history: &Vec<(String, f64)>,
) -> Result<f64, &'static str> {
    // Pick the most recent finished period
    let res = resp
        .results
        .iter()
        .filter(|x| x.timeframe == "quarterly")
        .max_by(|x, y| {
            let x_date = NaiveDate::parse_from_str(
                &x.end_date.clone().expect("Missing end date"),
                "%Y-%m-%d",
            )
            .expect("Wrong end date format");
            let y_date = NaiveDate::parse_from_str(
                &y.end_date.clone().expect("Missing end date"),
                "%Y-%m-%d",
            )
            .expect("Wrong end date format");
            x_date.cmp(&y_date)
        })
        .ok_or("Unable to get most recent financial period")?;

    log::info!(
        "{:?}: start date: {:?}, end date: {:?}, fiscal_year: {}, timeframe: {} fiscal_period: {}",
        res.tickers,
        res.start_date,
        res.end_date,
        res.fiscal_year,
        res.timeframe,
        res.fiscal_period
    );

    let start_date = NaiveDate::parse_from_str(
        &res.start_date.clone().expect("Missing start date"),
        "%Y-%m-%d",
    )
    .expect("Wrong start date format");
    let end_date =
        NaiveDate::parse_from_str(&res.end_date.clone().expect("Missing end date"), "%Y-%m-%d")
            .expect("Wrong end date format");

    // Div payout date must be within start and end of quarter
    let div = div_history
        .iter()
        .filter(|x| {
            let x_date =
                NaiveDate::parse_from_str(&x.0, "%Y-%m-%d").expect("Wrong end date format");
            (start_date < x_date) && (x_date < end_date)
        })
        .next()
        .ok_or("Unable to get dividend from recent financial period")?;

    let net_value = get_net_cash_flow(
        &res.financials,
        res.company_name.as_ref(),
        res.fiscal_year.as_ref(),
        res.fiscal_period.as_ref(),
    )?;
    let basic_average_shares = get_basic_average_shares(
        &res.financials,
        res.company_name.as_ref(),
        res.fiscal_year.as_ref(),
        res.fiscal_period.as_ref(),
    )?;

    let payout_rate = match basic_average_shares {
        Some(num_shares) => calculate_payout_ratio(div.1, num_shares, net_value)?,
        None => return Err("Unable to get basic shares value from quaterly financial report"),
    };
    Ok(payout_rate)
}

/// DGR On quaterly basis calculate(make UT)
fn calculate_payout_ratio(div: f64, num_shares: f64, net_value: f64) -> Result<f64, &'static str> {
    let payout_rate = div * num_shares as f64 / net_value * 100.0;
    Ok(payout_rate)
}

/// Calculate dividend yield
/// Formula : get historical data e.g. from
fn calculate_divy(
    div_history: &Vec<(String, f64)>,
    share_price: f64,
    current_year: &str,
) -> Result<f64, &'static str> {
    let dhiter = div_history.iter();

    let mut average = 0.0;
    let mut annual_div = 0.0;

    let current_year = current_year
        .parse::<i32>()
        .expect("Unable to parse currrent year");
    let mut annual_div: BTreeMap<i32, f64> = BTreeMap::new();

    div_history.iter().try_for_each(|x| {
        let year = NaiveDate::parse_from_str(&x.0, "%Y-%m-%d")
            .map_err(|_| "Error parsing dividend year")?
            .year();
        // Skip current year (no full data yet)
        if year != current_year {
            let possible_sum = annual_div.get_mut(&year);
            match possible_sum {
                Some(s) => *s += x.1,
                None => {
                    annual_div.insert(year, x.1);
                    ()
                }
            }
        }
        Ok::<(), &str>(())
    })?;

    let mut from_newer_to_older = annual_div.iter().rev();
    let annual_div = from_newer_to_older
        .next()
        .ok_or("Error: unable to get devidend")?
        .1;

    Ok(annual_div / share_price * 100.0)
}

/// DGR On quaterly basis calculate
fn calculate_dgr(
    div_history: &Vec<(String, f64)>,
    current_year: &str,
) -> Result<f64, &'static str> {
    let dhiter = div_history.iter();

    let mut average = 0.0;
    let mut annual_div = 0.0;

    let current_year = current_year
        .parse::<i32>()
        .expect("Unable to parse currrent year");
    let mut annual_div: BTreeMap<i32, f64> = BTreeMap::new();

    div_history.iter().try_for_each(|x| {
        let year = NaiveDate::parse_from_str(&x.0, "%Y-%m-%d")
            .map_err(|_| "Error parsing dividend year")?
            .year();
        // Skip current year (no full data yet)
        if year != current_year {
            let possible_sum = annual_div.get_mut(&year);
            match possible_sum {
                Some(s) => *s += x.1,
                None => {
                    annual_div.insert(year, x.1);
                    ()
                }
            }
        }
        Ok::<(), &str>(())
    })?;

    // Update data with zeros when there was no dividends in a given historical period
    let oldest_year = annual_div
        .iter()
        .next()
        .ok_or("Error: unable to get devidend")?
        .0;
    for y in *oldest_year..current_year {
        if annual_div.contains_key(&y) == false {
            annual_div.insert(y, 0.0);
            log::info!("Company was having a gap in paying divdends at {y}");
        }
    }

    let mut from_newer_to_older = annual_div.iter().rev();
    let (next_year, mut next_year_div) = from_newer_to_older
        .next()
        .ok_or("Error: unable to get devidend")?;

    log::info!("DGR: Annual dividend year: {next_year} annual_div: {next_year_div}");
    let mut num_averages = 0;
    for (year, sum) in from_newer_to_older {
        log::info!("DGR: Annual dividend year: {year} annual_div: {sum}");
        if *sum > 0.0 {
            average += (next_year_div / sum - 1.0) * 100.0;
        } else {
            // If next year dividend is positive and previous one is zero then
            // increase was by 100%
            if *next_year_div > 0.0 {
                average += 100.0;
            } else {
                average += 0.0;
            }
        }
        next_year_div = sum;
        num_averages += 1;
    }

    if num_averages == 0 {
        Ok(0.0)
    } else {
        Ok(average / num_averages as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round2(val: f64) -> f64 {
        (val * 100.0).round() / 100.0
    }

    #[test]
    fn test_calulate_divy() -> Result<(), String> {
        let div_hists: Vec<(String, f64)> = vec![
            ("2023-01-01".to_owned(), 0.5),
            ("2023-04-01".to_owned(), 0.5),
            ("2023-07-01".to_owned(), 0.5),
            ("2023-11-01".to_owned(), 0.5),
        ];
        assert_eq!(calculate_divy(&div_hists, 100.0, "2024"), Ok(2.0));

        let div_hists: Vec<(String, f64)> = vec![
            ("2023-01-01".to_owned(), 1.0),
            ("2023-04-01".to_owned(), 1.0),
            ("2023-07-01".to_owned(), 2.0),
            ("2023-11-01".to_owned(), 4.0),
        ];
        assert_eq!(calculate_divy(&div_hists, 100.0, "2024"), Ok(8.0));
        Ok(())
    }

    #[test]
    fn test_calculate_dgr() -> Result<(), String> {
        let div_hists: Vec<(String, f64)> = vec![
            ("2023-01-01".to_owned(), 0.5),
            ("2023-04-01".to_owned(), 0.5),
            ("2023-07-01".to_owned(), 0.5),
            ("2023-11-01".to_owned(), 0.5),
        ];
        assert_eq!(calculate_dgr(&div_hists, "2024"), Ok(0.0));

        let div_hists: Vec<(String, f64)> = vec![
            ("2023-01-01".to_owned(), 0.5),
            ("2023-04-01".to_owned(), 0.5),
            ("2023-07-01".to_owned(), 0.5),
            ("2023-11-01".to_owned(), 0.5),
            ("2022-01-01".to_owned(), 0.5),
            ("2022-04-01".to_owned(), 0.5),
            ("2022-07-01".to_owned(), 0.5),
            ("2022-11-01".to_owned(), 0.5),
        ];
        assert_eq!(calculate_dgr(&div_hists, "2024"), Ok(0.0));

        let div_hists: Vec<(String, f64)> = vec![
            ("2022-01-01".to_owned(), 0.1),
            ("2022-04-01".to_owned(), 0.9),
            ("2022-07-01".to_owned(), 1.0),
            ("2022-11-01".to_owned(), 1.0),
            ("2023-01-01".to_owned(), 0.5),
            ("2023-04-01".to_owned(), 0.5),
            ("2023-07-01".to_owned(), 2.0),
            ("2023-11-01".to_owned(), 3.0),
        ];
        assert_eq!(calculate_dgr(&div_hists, "2024"), Ok(100.0));

        let div_hists: Vec<(String, f64)> = vec![
            ("2022-03-01".to_owned(), 0.365),
            ("2022-06-01".to_owned(), 0.365),
            ("2022-09-01".to_owned(), 0.365),
            ("2022-12-01".to_owned(), 0.365),
            ("2023-03-01".to_owned(), 0.365),
            ("2023-06-01".to_owned(), 0.125),
            ("2023-09-01".to_owned(), 0.125),
            ("2023-12-01".to_owned(), 0.125),
            ("2024-03-01".to_owned(), 0.125),
        ];

        //0.125*3.0+0.365 = 0.74
        //0.365*4.0 = 1.46
        // DGR: (0.74/1.46 - 1.0)*100.0 = -49.315068
        assert_eq!(
            Ok::<f64, &str>(round2(calculate_dgr(&div_hists, "2024").unwrap())),
            Ok(-49.32)
        );

        // 0.3475
        // 2.0*0.365 = 0.73
        // 0.0
        // DGR = (0.0 -1.0)*100.0 + (0.73/0.3475 - 1.0)*100.0 = 10.071942 / 2.0 = 5.04
        let div_hists: Vec<(String, f64)> = vec![
            ("2021-12-01".to_owned(), 0.3475),
            ("2022-03-01".to_owned(), 0.365),
            ("2022-06-01".to_owned(), 0.365),
        ];

        assert_eq!(
            Ok::<f64, &str>(round2(calculate_dgr(&div_hists, "2024").unwrap())),
            Ok(5.04)
        );

        // ABEV as of 28th of March 2024
        let div_hists: Vec<(String, f64)> = vec![
            ("1970-01-01".to_owned(), 0.0550986),
            ("2005-10-14".to_owned(), 0.005492),
            ("2006-01-05".to_owned(), 0.001936),
            ("2006-04-10".to_owned(), 0.01076),
            ("2006-07-10".to_owned(), 0.011932),
            ("2006-11-09".to_owned(), 0.0121),
            ("2007-01-08".to_owned(), 0.014732),
            ("2007-04-12".to_owned(), 0.014256),
            ("2007-06-18".to_owned(), 0.001926),
            ("2007-07-09".to_owned(), 0.0068092),
            ("2007-10-26".to_owned(), 0.0325832),
            ("2007-12-28".to_owned(), 0.010568),
            ("2008-05-08".to_owned(), 0.031102),
            ("2008-06-16".to_owned(), 0.0002936),
            ("2008-08-13".to_owned(), 0.0296124),
            ("2008-10-24".to_owned(), 0.0173388),
            ("2009-02-09".to_owned(), 0.0056056),
            ("2009-06-08".to_owned(), 0.0069884),
            ("2009-06-22".to_owned(), 0.0010552),
            ("2009-08-11".to_owned(), 0.0068696),
            ("2009-10-14".to_owned(), 0.0046288),
            ("2009-12-29".to_owned(), 0.0102084),
            ("2010-04-08".to_owned(), 0.0085252),
            ("2010-10-25".to_owned(), 0.0191172),
            ("2010-12-22".to_owned(), 0.0299664),
            ("2011-03-29".to_owned(), 0.0670016),
            ("2011-08-12".to_owned(), 0.0314662),
            ("2011-11-28".to_owned(), 0.0112554),
            ("2012-04-17".to_owned(), 0.0655808),
            ("2012-08-03".to_owned(), 0.0117658),
            ("2012-10-22".to_owned(), 0.0108748),
            ("2013-01-29".to_owned(), 0.0793902),
            ("2013-04-05".to_owned(), 0.0079676),
            ("2014-01-30".to_owned(), 0.065248),
            ("2014-05-02".to_owned(), 0.057983),
            ("2014-09-05".to_owned(), 0.026945),
            ("2014-11-24".to_owned(), 0.092538),
            ("2015-01-21".to_owned(), 0.048092),
            ("2015-02-06".to_owned(), 0.037219),
            ("2015-04-07".to_owned(), 0.027902),
            ("2015-07-09".to_owned(), 0.032246),
            ("2015-10-08".to_owned(), 0.035761),
            ("2016-01-07".to_owned(), 0.038278),
            ("2016-03-07".to_owned(), 0.032999),
            ("2016-08-05".to_owned(), 0.039616),
            ("2016-12-05".to_owned(), 0.047804),
            ("2017-01-05".to_owned(), 0.067134),
            ("2017-03-02".to_owned(), 0.022606),
            ("2017-07-24".to_owned(), 0.049309),
            ("2018-01-08".to_owned(), 0.093373),
            ("2018-03-05".to_owned(), 0.021539),
            ("2018-08-06".to_owned(), 0.042972),
            ("2019-01-07".to_owned(), 0.081518),
            ("2020-01-07".to_owned(), 0.120835),
            ("2021-01-11".to_owned(), 0.078966),
            ("2021-02-04".to_owned(), 0.01424),
            ("2022-01-06".to_owned(), 0.107707),
            ("2023-01-05".to_owned(), 0.144287),
            ("2024-01-08".to_owned(), 0.150969),
        ];

        assert_eq!(
            Ok::<f64, &str>(round2(calculate_dgr(&div_hists, "2024").unwrap())),
            Ok(17.58)
        );

        Ok(())
    }

    #[test]
    fn test_calulate_payout_rate() -> Result<(), String> {
        assert_eq!(calculate_payout_ratio(0.5, 100.0, 200.0), Ok(25.0));
        Ok(())
    }

    #[test]
    fn test_calculate_annualized_div() -> Result<(), String> {
        let div_hists: Vec<(String, f64)> = vec![
            ("2023-01-01".to_owned(), 0.5),
            ("2023-04-01".to_owned(), 1.0),
            ("2023-07-01".to_owned(), 2.0),
            ("2023-11-01".to_owned(), 4.0),
            ("2022-04-01".to_owned(), 0.3),
            ("2022-07-01".to_owned(), 0.3),
            ("2022-11-01".to_owned(), 0.2),
            ("2022-01-01".to_owned(), 0.1),
        ];

        assert_eq!(calculate_annualized_div(&div_hists, "2023"), Ok(7.5));
        assert_eq!(calculate_annualized_div(&div_hists, "2022"), Ok(0.9));
        Ok(())
    }

    #[test]
    fn test_calculate_consecutive_years_of_growth() -> Result<(), String> {
        let div_hists: Vec<(String, f64)> = vec![
            ("2023-01-01".to_owned(), 0.5),
            ("2023-04-01".to_owned(), 1.0),
            ("2023-07-01".to_owned(), 2.0),
            ("2023-11-01".to_owned(), 4.0),
            ("2022-04-01".to_owned(), 0.3),
            ("2022-07-01".to_owned(), 0.3),
            ("2022-11-01".to_owned(), 0.2),
            ("2022-01-01".to_owned(), 0.1),
        ];
        assert_eq!(
            calculate_consecutive_years_of_growth(&div_hists, "2024"),
            Ok(1)
        );

        let div_hists: Vec<(String, f64)> = vec![
            ("2024-01-01".to_owned(), 0.5),
            ("2023-01-01".to_owned(), 0.5),
            ("2023-04-01".to_owned(), 1.0),
            ("2023-07-01".to_owned(), 2.0),
            ("2023-11-01".to_owned(), 4.0),
            ("2022-04-01".to_owned(), 0.3),
            ("2022-07-01".to_owned(), 0.3),
            ("2022-11-01".to_owned(), 0.2),
            ("2022-01-01".to_owned(), 0.1),
        ];
        assert_eq!(
            calculate_consecutive_years_of_growth(&div_hists, "2024"),
            Ok(1)
        );

        let div_hists: Vec<(String, f64)> = vec![
            ("2024-01-01".to_owned(), 0.5),
            ("2023-01-01".to_owned(), 0.5),
            ("2023-04-01".to_owned(), 1.0),
            ("2023-07-01".to_owned(), 2.0),
            ("2023-11-01".to_owned(), 3.0),
            ("2022-04-01".to_owned(), 3.3),
            ("2022-07-01".to_owned(), 3.3),
            ("2022-11-01".to_owned(), 0.2),
            ("2022-01-01".to_owned(), 0.1),
        ];
        assert_eq!(
            calculate_consecutive_years_of_growth(&div_hists, "2024"),
            Ok(0)
        );

        let div_hists: Vec<(String, f64)> = vec![
            ("2024-01-01".to_owned(), 0.5),
            ("2023-01-01".to_owned(), 2.5),
            ("2023-04-01".to_owned(), 1.0),
            ("2023-07-01".to_owned(), 2.0),
            ("2023-11-01".to_owned(), 3.0),
            ("2022-04-01".to_owned(), 2.3),
            ("2022-07-01".to_owned(), 3.3),
            ("2022-11-01".to_owned(), 0.2),
            ("2022-01-01".to_owned(), 0.1),
            ("2021-04-01".to_owned(), 3.3),
            ("2021-07-01".to_owned(), 3.3),
            ("2021-11-01".to_owned(), 0.2),
            ("2021-01-01".to_owned(), 0.1),
        ];
        assert_eq!(
            calculate_consecutive_years_of_growth(&div_hists, "2024"),
            Ok(1)
        );

        Ok(())
    }
}
