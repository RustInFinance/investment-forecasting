use clap::Parser;
use polars::prelude::*;
use std::collections::BTreeMap;
use time::OffsetDateTime;
use yahoo_finance_api as yahoo;

// // TODO: Print dividend data and then add in summary the dividend per month
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Exchange {
    EUR(String),
    PLN(String),
    USD(String),
}

#[derive(Debug, PartialEq, PartialOrd, Copy, Clone)]
pub enum Currency {
    PLN(f64),
    EUR(f64),
    USD(f64),
}

impl Currency {
    fn value(&self) -> f64 {
        match self {
            Currency::EUR(val) => *val,
            Currency::PLN(val) => *val,
            Currency::USD(val) => *val,
        }
    }
    fn derive(&self, val: f64) -> Currency {
        match self {
            Currency::EUR(_) => Currency::EUR(val),
            Currency::PLN(_) => Currency::PLN(val),
            Currency::USD(_) => Currency::USD(val),
        }
    }

    pub fn derive_exchange(&self, date: String) -> Exchange {
        match self {
            Currency::EUR(_) => Exchange::EUR(date),
            Currency::PLN(_) => Exchange::PLN(date),
            Currency::USD(_) => Exchange::USD(date),
        }
    }

    fn print(&self) -> String {
        match self {
            Currency::EUR(val) => format!("{:.2} EUR", val),
            Currency::PLN(val) => format!("{:.2} PLN", val),
            Currency::USD(val) => format!("{:.2} USD", val),
        }
    }
}

fn print_monthly_dividends_distribution(stocks: &[Stock]) {
    println!("Dividend distribution per month:");
    let mut monthly_distribution = BTreeMap::new();

    // USD / PLN
    let mut exchange_rate_usd_pln = 0.0;
    let provider = yahoo::YahooConnector::new().unwrap();
    match provider.get_latest_quotes("USDPLN=X", "1d") {
        Ok(response) => {
            if let Ok(quotes) = response.quotes() {
                println!("Kurs USD/PLN: {:?}", quotes.last().unwrap().close);
                exchange_rate_usd_pln = quotes.last().unwrap().close;
            }
        }
        Err(e) => eprintln!("Error: {:?}", e),
    }
    // EUR / PLN
    let mut exchange_rate_eur_pln = 0.0;
    let provider = yahoo::YahooConnector::new().unwrap();
    match provider.get_latest_quotes("EURPLN=X", "1d") {
        Ok(response) => {
            if let Ok(quotes) = response.quotes() {
                println!("Kurs USD/PLN: {:?}", quotes.last().unwrap().close);
                exchange_rate_eur_pln = quotes.last().unwrap().close;
            }
        }
        Err(e) => eprintln!("Error: {:?}", e),
    }

    // convert all dividends into PLN
    for stock in stocks {
        for (month, amount) in &stock.monthly_dividends {
            let amount_pln = match stock.current_value {
                Currency::USD(_) => amount * exchange_rate_usd_pln,
                Currency::EUR(_) => amount * exchange_rate_eur_pln,
                Currency::PLN(_) => *amount,
            };
            *monthly_distribution.entry(month.clone()).or_insert(0.0) += amount_pln;
        }
    }

    monthly_distribution.iter().for_each(|(m, v)| {
        println!("{m} : {v:0.2} PLN ");
    });
}

fn print_summary(data: &[Stock]) {
    let mut total_investement = 0.0;
    let mut annual_dividend = 0.0;
    let mut portfolio_value = 0.0;
    let mut total_investement_eur = 0.0;
    let mut annual_dividend_eur = 0.0;
    let mut portfolio_value_eur = 0.0;
    let mut total_investement_pln = 0.0;
    let mut annual_dividend_pln = 0.0;
    let mut portfolio_value_pln = 0.0;

    data.iter().for_each(|e| {
        match e.invested_value {
            Currency::USD(val) => total_investement += val,
            Currency::EUR(val) => total_investement_eur += val,
            Currency::PLN(val) => total_investement_pln += val,
        }
        match e.current_value {
            Currency::USD(val) => portfolio_value += val,
            Currency::EUR(val) => portfolio_value_eur += val,
            Currency::PLN(val) => portfolio_value_pln += val,
        }
        match e.annualized_dividend {
            Currency::USD(val) => annual_dividend += val,
            Currency::EUR(val) => annual_dividend_eur += val,
            Currency::PLN(val) => annual_dividend_pln += val,
        }
    });
    if total_investement > 0.0 {
        println!("Total investement[$]: {:.2}", total_investement);
        println!("Total portfolio value [$]: {:.2}", portfolio_value);
        println!("Total annual dividend[$]: {:.2}", annual_dividend);
        println!(
            "Portoflio yield[%]: {:.2}\n",
            annual_dividend / total_investement * 100.0
        )
    }
    if total_investement_eur > 0.0 {
        println!("Total investement[EUR]: {:.2}", total_investement_eur);
        println!("Total portfolio value [EUR]: {:.2}", portfolio_value_eur);
        println!("Total annual dividend[EUR]: {:.2}", annual_dividend_eur);
        println!(
            "Portoflio yield[%]: {:.2}\n",
            annual_dividend_eur / total_investement_eur * 100.0
        )
    }
    if total_investement_pln > 0.0 {
        println!("Total investement[PLN]: {:.2}", total_investement_pln);
        println!("Total portfolio value [PLN]: {:.2}", portfolio_value_pln);
        println!("Total annual dividend[PLN]: {:.2}", annual_dividend_pln);
        println!(
            "Portoflio yield[%]: {:.2}\n",
            annual_dividend_pln / total_investement_pln * 100.0
        )
    }

    let mut exchange_rate_usd_pln = 0.0;
    let provider = yahoo::YahooConnector::new().unwrap();
    match provider.get_latest_quotes("USDPLN=X", "1d") {
        Ok(response) => {
            if let Ok(quotes) = response.quotes() {
                println!("Kurs USD/PLN: {:?}", quotes.last().unwrap().close);
                exchange_rate_usd_pln = quotes.last().unwrap().close;
            }
        }
        Err(e) => eprintln!("Error: {:?}", e),
    }

    if total_investement_eur > 0.0 && total_investement > 0.0 {
        // Get current USD exchange rate and EUR exchange rate
        let provider = yahoo::YahooConnector::new().unwrap();
        match provider.get_latest_quotes("EURUSD=X", "1d") {
            Ok(response) => {
                if let Ok(quotes) = response.quotes() {
                    println!("Kurs EUR/USD: {:?}", quotes.last().unwrap().close);
                    let exchange_rate = quotes.last().unwrap().close;
                    let eur = total_investement_eur * exchange_rate;
                    let usd = total_investement;
                    let total = eur + usd;
                    let combined_yield = eur / total * annual_dividend_eur / total_investement_eur
                        * 100.0
                        + usd / total * annual_dividend / total_investement * 100.0;
                    println!("Combined portfolio yield [%]: {:.2}", combined_yield);
                    let combined_annual_dividend =
                        annual_dividend_eur * exchange_rate + annual_dividend;
                    println!(
                        "Combined portfolio annual income [$]: {:.2}",
                        combined_annual_dividend
                    );
                    println!(
                        "Combined portfolio annual income [PLN]: {:.2}",
                        combined_annual_dividend * exchange_rate_usd_pln
                    );
                }
            }
            Err(e) => eprintln!("Error: {:?}", e),
        }
    }
}

fn print_data_frame(data: &[Stock]) {
    let mut symbols: Vec<&str> = vec![];
    let mut invested_values: Vec<String> = vec![];
    let mut current_values: Vec<String> = vec![];
    let mut current_yields: Vec<f64> = vec![];
    let mut yields_on_invested: Vec<f64> = vec![];
    let mut annualized_dividends: Vec<String> = vec![];
    data.iter().for_each(|e| {
        symbols.push(e.symbol);
        invested_values.push(e.invested_value.print());
        current_values.push(e.current_value.print());
        current_yields.push(e.current_yield * 100.0);
        yields_on_invested.push(e.yield_on_invested * 100.0);
        annualized_dividends.push(e.annualized_dividend.print());
    });
    let symbol_series = Series::new("Company", symbols);
    let invested_values_series = Series::new("Investment", &invested_values);
    let current_values_series = Series::new("Current Value", &current_values);
    let current_yields_series = Series::new("Yield[%]", &current_yields);
    let yields_series = Series::new("Yield on investment[%]", &yields_on_invested);
    let annualized_dividends_series = Series::new("Annual dividend", &annualized_dividends);

    let df = DataFrame::new(vec![
        symbol_series,
        invested_values_series,
        current_values_series,
        current_yields_series,
        yields_series,
        annualized_dividends_series,
    ])
    .expect("Unable to create DataFrame")
    .sort(["Company"], false, true)
    .map_err(|_| "Unable to sort per company report dataframe")
    .expect("Unable to sort DataFrame");
    println!("{df}");
}

fn compute_yield_on_investment(invested_value: f64, current_value: f64, current_yield: f64) -> f64 {
    current_value * current_yield / invested_value
}

struct Stock<'a> {
    symbol: &'a str,
    invested_value: Currency,
    current_value: Currency,
    current_yield: f64,
    yield_on_invested: f64,
    annualized_dividend: Currency,
    monthly_dividends: BTreeMap<String, f64>,
}

impl<'a> Stock<'a> {
    fn new(
        symbol: &'a str,
        invested_value: Currency,
        current_value: Currency,
        current_yield: f64,
        monthly_dividends: BTreeMap<String, f64>,
    ) -> Self {
        // compute yield on invested
        Self {
            symbol,
            invested_value,
            current_value,
            current_yield,
            yield_on_invested: compute_yield_on_investment(
                invested_value.value(),
                current_value.value(),
                current_yield,
            ),
            annualized_dividend: current_value.derive(current_yield * current_value.value()),
            monthly_dividends,
        }
    }
}

fn get_data(
    symbol: &str,
    investement: Currency,
    num_shares: f64,
    div_yield: Option<f64>,
) -> Result<Stock, Box<dyn std::error::Error>> {
    // Tworzenie providera Yahoo Finance
    let mut provider = yahoo::YahooConnector::new()?;

    // Pobieranie bieżącej ceny akcji
    let response = provider.get_latest_quotes(symbol, "1d")?;

    // Wyświetlenie danych o cenie
    match response.last_quote() {
        Ok(quote) => {
            //         println!("=== current stock price {} ===", symbol);

            let datetime = OffsetDateTime::from_unix_timestamp(quote.timestamp as i64)?;
            //println!("Date: {}", datetime.date());
            //println!();
        }
        Err(e) => {
            println!("⚠️  Error fetching the data: {}", e);
            println!();
        }
    }

    // Pobieranie metadanych (zawierają dodatkowe informacje o akcji)
    match response.metadata() {
        Ok(metadata) => {
            //if let Some(currency) = &metadata.currency {
            //    println!("Currency: {}", currency);
            //}

            //if let Some(price) = metadata.regular_market_price {
            //    println!("Market price: ${:.2}", price);
            //}

            //println!();
        }
        Err(e) => {
            println!("⚠️  Error getting metadata of stock: {}", e);
            println!();
        }
    }

    let dividend_yield = match div_yield {
        Some(yield_value) => {
            //println!(
            //    "Given dividend yield (from arg): {:.2}%",
            //    yield_value * 100.0
            //);
            yield_value
        }
        None => {
            let response_info = provider.get_ticker_info(symbol)?;
            let mut yield_value = 0.0;
            match response_info.quote_summary {
                Some(info) => {
                    // TODO: iterate
                    if let Some(summary) = info.result {
                        summary.iter().for_each(|item| {
                            if let Some(dividend_yield) =
                                item.summary_detail.as_ref().and_then(|d| d.dividend_yield)
                            {
                                // println!("Dividend yeild from metadata : {:.2}%", dividend_yield);
                                yield_value = dividend_yield;
                            }
                        });
                    } else {
                        println!("⚠️ No info about stock");
                    }
                }
                None => {
                    println!("⚠️ No info about stock");
                }
            }
            yield_value
        }
    };

    let value = investement.derive(response.last_quote()?.close * num_shares);

    let monthly_dividends = get_dividend_history(symbol, num_shares)?;

    Ok(Stock::new(
        symbol,
        investement,
        value,
        dividend_yield,
        monthly_dividends,
    ))
}

fn get_dividend_history(
    symbol: &str,
    num_shares: f64,
) -> Result<BTreeMap<String, f64>, Box<dyn std::error::Error>> {
    let provider = yahoo::YahooConnector::new()?;

    // Get scope of for dividends (previous year)
    let now = OffsetDateTime::now_utc();
    let prev_year = now.year() - 1;

    let start = time::Date::from_calendar_date(prev_year, time::Month::January, 1)?
        .with_hms(0, 0, 0)?
        .assume_utc();
    let end = time::Date::from_calendar_date(prev_year, time::Month::December, 31)?
        .with_hms(23, 59, 59)?
        .assume_utc();

    let resp = provider.get_quote_history(symbol, start, end)?;

    // Zbierz dywidendy per miesiac
    let mut monthly_dividends: BTreeMap<String, f64> = BTreeMap::new();

    if let Ok(quotes) = resp.quotes() {
        // quotes to ceny — szukamy eventow dywidendowych
        // Yahoo finance zwraca dividendy w polu adjclose vs close diff
        // Ale lepiej uzyc get_quote_history i sprawdzic eventy
    }

    // Probujemy wyciagnac dywidendy z odpowiedzi (pole events.dividends w Yahoo API)
    // yahoo_finance_api parsuje to w strukt YResponse
    if let Some(result) = resp.chart.result.as_ref() {
        for r in result {
            if let Some(events) = &r.events {
                if let Some(dividends) = &events.dividends {
                    for (_timestamp_str, div) in dividends {
                        let dt = OffsetDateTime::from_unix_timestamp(div.date as i64)?;
                        let month_key = format!("{:02}", dt.month() as u8);
                        let dividend_income = div.amount * num_shares;
                        *monthly_dividends.entry(month_key).or_insert(0.0) += dividend_income;
                    }
                }
            }
        }
    }

    //    println!("Monthly dividends for {}: {:?}", symbol, monthly_dividends);

    Ok(monthly_dividends)
}

fn main() -> Result<(), String> {
    if std::env::var("POLARS_FMT_MAX_ROWS").is_err() {
        std::env::set_var("POLARS_FMT_MAX_ROWS", "-1")
    }

    // Get stock prices , get dividends data and get EUR/USD

    // List of companies in a format (symbol, invested financial resources, current value, current
    // Yield)
    let ania = vec![
        //Stock::new("ABEV", Currency::USD(121+11.91),  Currency::USD(240.84), 0.0789),
        get_data(
            "ABEV",
            Currency::USD(121.0 + 11.20 + 80.0 + 1.91),
            82.09,
            None,
        )
        .unwrap(),
        get_data(
            "BBY",
            Currency::USD(1000.0 + 92.0 + 93.62 + 94.73),
            11.63,
            None,
        )
        .unwrap(),
        get_data("GOOGL", Currency::USD(58.68 + 60.0), 0.81, None).unwrap(),
        get_data("GSL", Currency::USD(276.0 + 44.36 + 2.92), 7.19, None).unwrap(),
        get_data("KO", Currency::USD(23.06 + 70.0 + 5.88 + 5.92), 1.75, None).unwrap(),
        get_data("LX", Currency::USD(3.49 + 9.72), 1.9, None).unwrap(),
        get_data("SM", Currency::USD(27.74), 2.0, None).unwrap(),
        get_data(
            "TGT",
            Currency::USD(26.9 + 744.63 + 2.0 + 8.78 + 1.0 + 8.88),
            9.25,
            None,
        )
        .unwrap(),
        //Stock::new("VZ",   Currency::USD(19.85),    Currency::USD(24.98),   0.0671),
        get_data("VZ", Currency::USD(19.85), 0.49, None).unwrap(),
    ];

    print_data_frame(&ania);

    let jacek = vec![
        get_data("AHOG.DE", Currency::EUR(5980.74), 179.11, None).unwrap(),
        get_data("BMO", Currency::USD(2100.0), 28.23, None).unwrap(),
        get_data("CNQ", Currency::USD(300.0 + 298.51), 138.65, None).unwrap(),
        get_data("CVX", Currency::USD(100.0), 6.43, None).unwrap(),
        get_data("EIX", Currency::USD(200.0), 6.23, None).unwrap(),
        get_data(
            "EPR",
            Currency::USD(500.0 + 500.0 + 987.80 + 496.86 + 5150.48 + 100.0 + 100.0 + 5340.12),
            79.24,
            None,
        )
        .unwrap(),
        get_data("NVS", Currency::USD(201.0), 1.89, None).unwrap(),
        get_data("O", Currency::USD(336.99), 5.2, None).unwrap(),
        get_data("SNY", Currency::USD(200.0), 4.02, None).unwrap(),
        get_data("TW10.F", Currency::EUR(200.66 + 52.0), 12.02, None).unwrap(),
        get_data("TRN.MI", Currency::EUR(92.29), 18.86, None).unwrap(),
        get_data(
            "UPS",
            Currency::USD(1999.99 + 200.0 + 1000.0 + 1000.0),
            15.02,
            None,
        )
        .unwrap(),
        get_data("VVD.DE", Currency::EUR(569.23), 15.77, None).unwrap(),
        //    Stock::new("DE000A289XJ2",Currency::USD(2594.37),   Currency::EUR(2627.66),  0.0501),
    ];
    print_data_frame(&jacek);

    println!("ANIA:");
    print_monthly_dividends_distribution(&ania);
    print_summary(&ania);

    println!("JACEK:");
    print_monthly_dividends_distribution(&jacek);
    print_summary(&jacek);

    // Compute summary in PLN

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_compute_yeild_on_investement() -> Result<(), String> {
        assert_eq!(compute_yield_on_investment(1000.0, 1200.0, 0.05), 0.06);
        Ok(())
    }
}
