use calamine::{Reader, Xlsx};
use polars::prelude::*;
use std::fmt;

use chrono::prelude::*;
use chrono::Duration;

use std::collections::HashMap;
use polygon_client::rest::RESTClient;

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


//pub struct ReferenceStockDividendsResultV3 {
//    pub cash_amount: f64,
//    pub currency: String,
//    pub declaration_date: String,
//    pub dividend_type: DividendType,
//    pub ex_dividend_date: String,
//    pub frequency: u32,
//    pub pay_date: String,
//    pub record_date: String,
//    pub ticker: String,
//}

pub fn get_polygon_data(company : &str) -> Result<(f64,f64,f64,f64),&'static str>{
    let mut query_params = HashMap::new();
    query_params.insert("ticker", company);
    
    let client = RESTClient::new(None, None);
    // Get all dividend data we can have
    tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                 let resp = client.reference_stock_dividends(&query_params)
        .await
        .expect("POLYGON API: failed to query tickers");

        let mut div_history : Vec<(String,f64)> = resp.results.iter().map(|x| {
            log::info!("{}: ex date: {}, payment date: {}, frequency: {}, div type: {} amount: {}", x.ticker,x.ex_dividend_date,x.pay_date,x.frequency,x.dividend_type,x.cash_amount);
            (x.pay_date.clone(),x.cash_amount)
        }).collect();

        div_history.sort_by(|a,b| {
           let a_date = NaiveDate::parse_from_str(&a.0, "%Y-%m-%d").expect( "unable to parse date");
           let b_date = NaiveDate::parse_from_str(&b.0, "%Y-%m-%d").expect( "unable to parse date"); 
           a_date.cmp(&b_date)
        });

        // Curr Dividend  and corressponding date 
        let (curr_div, _curr_div_date) = match div_history.iter().rev().next() {
            Some((pay_date,cash_amount)) => (cash_amount,NaiveDate::parse_from_str(&pay_date, "%Y-%m-%d").expect("Wrong payout date format")),
            None => panic!("No dividend Data!"),
        };
        let (currency, frequency) = if resp.results.len() > 0 {
            (resp.results[0].currency.clone(),resp.results[0].frequency)
        } else {
            panic!("No dividend Data!");
        };

        let dgr = calculate_dgr(&div_history)?;
        log::info!("Current Div: {curr_div} {currency}, Frequency: {frequency}, Average DGR(samples: {}): {dgr}",
            div_history.len());

        let mut close_query_params = HashMap::new();
        close_query_params.insert("adjusted", "true");
        let resp = client.stock_equities_previous_close(company,&HashMap::new()).await.expect("Unable to get stock price");
        let prev_day_share_data = resp.results.iter().next().ok_or("Error reading previous date share price")?;
        let share_price = prev_day_share_data.c;

        let divy = calculate_divy(&div_history,share_price,frequency)?;
        log::info!("Stock price: {share_price}, Div Yield[%]: {divy:.2}");

        let resp = client.reference_stock_financials_vx(&query_params)
            .await
            .expect("failed to query tickers");
  
        let payout_rate = match get_quaterly_payout_rate(&resp,&div_history) {
            Ok(payout_rate) => payout_rate,
            Err(_) => get_annual_payout_rate(&resp,&div_history)?,
        };

        return Ok::<(f64,f64,f64,f64), &'static str>((*curr_div,divy,dgr,payout_rate))
    })?;
    Err("Unable to get comapny data")
}

fn get_net_cash_flow( fd : &polygon_client::types::FinancialDimensions, company_name : &str, fiscal_year : &str, fiscal_period : &str) ->Result<f64,&'static str> {

        let net_value = if let Some(ismap) = &fd.cash_flow_statement {
            let net_value = if ismap.contains_key("net_cash_flow_continuing") {
                let net_cash_flow = ismap.get("net_cash_flow_continuing").expect("Error getting net_cash_flow_continuing");
                let net_value = net_cash_flow.value.clone().unwrap();
                let net_unit =  net_cash_flow.unit.clone().unwrap();
                let net_label =  net_cash_flow.label.clone().unwrap();
                log::info!("{}: {} {} net cash flow: {} of {}, labeled as {}",company_name,fiscal_year,fiscal_period,net_value,net_unit,net_label);

                // curr_div * num_shares  / net_value
                net_value
            } else {
                return Err("Missing net_cash_flow_continuing");
            };
            net_value
        } else {
            return Err("Implement missing cash flow statement");
        };
    Ok(net_value)
}

fn get_basic_average_shares( fd : &polygon_client::types::FinancialDimensions, company_name : &str, fiscal_year : &str, fiscal_period : &str) ->Result<f64,&'static str> {

        let basic_average_shares = if let Some(ismap) = &fd.income_statement {

            let basic_average_shares = if ismap.contains_key("basic_average_shares") {
                let basic_average_shares = ismap.get("basic_average_shares").expect("Error getting basic_average_shares");
                let value = basic_average_shares.value.clone().unwrap();
                let unit =  basic_average_shares.unit.clone().unwrap();
                let label = basic_average_shares.label.clone().unwrap();
                log::info!("{}: {} {} basic average shares: {} of {}, labeled as {}",company_name,fiscal_year,fiscal_period,value,unit,label);
                value
            } else {
                todo!("Implement missing net_cash_flow_continuing");
            };
            basic_average_shares
        } else {
            todo!("Implement missing net_cash_flow_continuing");
        };
        Ok(basic_average_shares)
}

fn get_annual_payout_rate( resp : &polygon_client::types::ReferenceStockFinancialsVXResponse, div_history: &Vec<(String,f64)>) ->Result<f64,&'static str> {

        // Pick the most recent annual report 
        let res = resp.results.iter().filter(|x| x.timeframe == "annual").max_by(|x,y| {
            let x_date =  NaiveDate::parse_from_str(&x.end_date.clone().expect("Missing end date"), "%Y-%m-%d").expect("Wrong end date format");
            let y_date =  NaiveDate::parse_from_str(&y.end_date.clone().expect("Missing end date"), "%Y-%m-%d").expect("Wrong end date format");
            x_date.cmp(&y_date)
        }).ok_or("Unable to get most recent annual financial period")?;

        log::info!("{:?}: start date: {:?}, end date: {:?}, fiscal_year: {}, timeframe: {} fiscal_period: {}", res.tickers,res.start_date,res.end_date,res.fiscal_year,res.timeframe,res.fiscal_period);

        println!("FISCAL _YEAR: \"{}\"",res.fiscal_year);
        let fiscal_year =  NaiveDate::parse_from_str(&res.fiscal_year, "%Y").expect("Wrong fiscal year format");

        // Div payout dates must come from chosen fiscal year
        let annuallized_div = div_history.iter().filter(|x| {
            let (year, _) =  NaiveDate::parse_and_remainder(&x.0, "%Y").expect("Wrong div date format");
            year == fiscal_year
        }).fold(0.0, |mut acc, num| {acc += num.1; acc});

        let net_value = get_net_cash_flow(&res.financials,res.company_name.as_ref(),res.fiscal_year.as_ref(),res.fiscal_period.as_ref())?;
        let basic_average_shares = get_basic_average_shares(&res.financials,res.company_name.as_ref(),res.fiscal_year.as_ref(),res.fiscal_period.as_ref())?;
        let payout_rate = calculate_payout_ratio(annuallized_div,basic_average_shares,net_value)?;
        Ok(payout_rate)
}

fn get_quaterly_payout_rate( resp : &polygon_client::types::ReferenceStockFinancialsVXResponse, div_history: &Vec<(String,f64)>) ->Result<f64,&'static str> {

        // Pick the most recent finished period
        let res = resp.results.iter().filter(|x| x.timeframe == "quarterly").max_by(|x,y| {
            let x_date =  NaiveDate::parse_from_str(&x.end_date.clone().expect("Missing end date"), "%Y-%m-%d").expect("Wrong end date format");
            let y_date =  NaiveDate::parse_from_str(&y.end_date.clone().expect("Missing end date"), "%Y-%m-%d").expect("Wrong end date format");
            x_date.cmp(&y_date)
        }).ok_or("Unable to get most recent financial period")?;

        log::info!("{:?}: start date: {:?}, end date: {:?}, fiscal_year: {}, timeframe: {} fiscal_period: {}", res.tickers,res.start_date,res.end_date,res.fiscal_year,res.timeframe,res.fiscal_period);
        
        let start_date =  NaiveDate::parse_from_str(&res.start_date.clone().expect("Missing start date"), "%Y-%m-%d").expect("Wrong start date format");
        let end_date =  NaiveDate::parse_from_str(&res.end_date.clone().expect("Missing end date"), "%Y-%m-%d").expect("Wrong end date format");

        // Div payout date must be within start and end of quarter
        let div = div_history.iter().filter(|x| {
            let x_date =  NaiveDate::parse_from_str(&x.0, "%Y-%m-%d").expect("Wrong end date format");
            (start_date < x_date) && (x_date< end_date)
        }).next().ok_or("Unable to get dividend from recent financial period")?;

        let net_value = get_net_cash_flow(&res.financials,res.company_name.as_ref(),res.fiscal_year.as_ref(),res.fiscal_period.as_ref())?;
        let basic_average_shares = get_basic_average_shares(&res.financials,res.company_name.as_ref(),res.fiscal_year.as_ref(),res.fiscal_period.as_ref())?;
        let payout_rate = calculate_payout_ratio(div.1,basic_average_shares,net_value)?;
        Ok(payout_rate)
}



/// DGR On quaterly basis calculate(make UT)
fn calculate_payout_ratio(div : f64, num_shares : f64, net_value : f64) -> Result<f64,&'static str>{

    let payout_rate = div * num_shares as f64 / net_value * 100.0;
    Ok(payout_rate)
}



/// Calculate dividend yield
/// Formula : get historical data e.g. from 
fn calculate_divy(div_history: &Vec<(String,f64)>,share_price : f64, frequency : u32) -> Result<f64,&'static str>{
   if div_history.len() < frequency as usize {
       let div_info = div_history.iter().rev().next().ok_or("Unable to get dividend value")?;
       Ok(div_info.1/share_price *frequency as f64*100.0)
   } else {
       let end = div_history.len();
       let annual_dividends = div_history.get(end - frequency as usize..end).ok_or("Error getting anuallized dividend")?;
       let annualized_div = annual_dividends.iter().fold(0.0, |mut acc, num| {acc += num.1; acc});
       Ok(annualized_div/share_price*100.0)
   }
}


/// DGR On quaterly basis calculate
fn calculate_dgr(div_history: &Vec<(String,f64)>) -> Result<f64,&'static str>{
  
    let mut dhiter = div_history.iter();

    let mut prev_val = match dhiter.next() {
        Some((_,value)) => value,
        None => return Err("No dividends samples!"),
    };

    let mut average = 0.0;
    let mut count = 0;
    dhiter.for_each(|(_,new_val)|{
       average += (new_val/prev_val - 1.0)* 100.0;
       count +=1;
       prev_val = new_val;
    });

    Ok(average/count as f64)
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calulate_divy() -> Result<(), String> {
        let div_hists : Vec<(String,f64)> = vec![("2023-01-01".to_owned(),0.5),
            ("2023-04-01".to_owned(),0.5),
            ("2023-07-01".to_owned(),0.5),
            ("2023-11-01".to_owned(),0.5)
        ]; 
        assert_eq!(calculate_divy(&div_hists,100.0,4),Ok(2.0));

        let div_hists : Vec<(String,f64)> = vec![("2023-01-01".to_owned(),1.0),
            ("2023-04-01".to_owned(),1.0),
            ("2023-07-01".to_owned(),2.0),
            ("2023-11-01".to_owned(),4.0)
        ]; 
        assert_eq!(calculate_divy(&div_hists,100.0,4),Ok(8.0));
        Ok(())
    }

    #[test]
    fn test_calulate_dgr() -> Result<(), String> {
        let div_hists : Vec<(String,f64)> = vec![("2023-01-01".to_owned(),0.5),
            ("2023-04-01".to_owned(),0.5),
            ("2023-07-01".to_owned(),0.5),
            ("2023-11-01".to_owned(),0.5)
        ]; 
        assert_eq!(calculate_dgr(&div_hists),Ok(0.0));

        let div_hists : Vec<(String,f64)> = vec![("2023-01-01".to_owned(),0.5),
            ("2023-04-01".to_owned(),1.0),
            ("2023-07-01".to_owned(),2.0),
            ("2023-11-01".to_owned(),4.0)
        ]; 
        assert_eq!(calculate_dgr(&div_hists),Ok(100.0));
        Ok(())
    }

    #[test]
    fn test_calulate_payout_rate() -> Result<(), String> {
        assert_eq!(calculate_payout_ratio(0.5,100.0,200.0),Ok(25.0));
        Ok(())
    }
}
