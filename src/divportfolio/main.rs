use clap::Parser;
use polars::prelude::*;

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
            Currency::EUR(val) => format!("{:.2} EUR",val),
            Currency::PLN(val) => format!("{:.2} PLN",val),
            Currency::USD(val) => format!("{:.2} USD",val),
        }
    }
}

fn print_summary(data : &[Stock]) {
    let mut total_investement = 0.0;
    let mut annual_dividend  = 0.0;
    let mut portfolio_value = 0.0;
    let mut total_investement_eur = 0.0;
    let mut annual_dividend_eur = 0.0;
    let mut portfolio_value_eur = 0.0;
    let mut total_investement_pln = 0.0;
    let mut annual_dividend_pln = 0.0;
    let mut portfolio_value_pln = 0.0;
    
    data.iter().for_each(|e|
        {
            match e.invested_value {
                Currency::USD(val) => {
                    total_investement+=val
                },
                Currency::EUR(val) => {
                    total_investement_eur+=val
                },
                Currency::PLN(val) => {
                    total_investement_pln+=val
                }, 
            }
            match e.current_value {
                Currency::USD(val) => {
                    portfolio_value+=val
                },
                Currency::EUR(val) => {
                    portfolio_value_eur+=val
                },
                Currency::PLN(val) => {
                    portfolio_value_pln+=val
                }, 
            }
            match e.annualized_dividend {
                Currency::USD(val) => {
                    annual_dividend+=val
                },
                Currency::EUR(val) => {
                    annual_dividend_eur+=val
                },
                Currency::PLN(val) => {
                    annual_dividend_pln+=val
                }, 

            }
        });
    if total_investement > 0.0 {
        println!("Total investement[$]: {:.2}",total_investement);
        println!("Total portfolio value [$]: {:.2}",portfolio_value);
        println!("Total annual dividend[$]: {:.2}",annual_dividend);
        println!("Portoflio yield[%]: {:.2}\n",annual_dividend/total_investement*100.0)

    }
    if total_investement_eur > 0.0 {
        println!("Total investement[EUR]: {:.2}",total_investement_eur);
        println!("Total portfolio value [EUR]: {:.2}",portfolio_value_eur);
        println!("Total annual dividend[EUR]: {:.2}",annual_dividend_eur);
        println!("Portoflio yield[%]: {:.2}\n",annual_dividend_eur/total_investement_eur*100.0)
    }
    if total_investement_pln > 0.0 {
        println!("Total investement[PLN]: {:.2}",total_investement_pln);
        println!("Total portfolio value [PLN]: {:.2}",portfolio_value_pln);
        println!("Total annual dividend[PLN]: {:.2}",annual_dividend_pln);
        println!("Portoflio yield[%]: {:.2}\n",annual_dividend_pln/total_investement_pln*100.0)
    }
}

fn print_data_frame(data : &[Stock])
{
   let mut symbols : Vec<&str> = vec![];
   let mut invested_values: Vec<String> = vec![];
   let mut current_values : Vec<String> = vec![];
   let mut current_yields : Vec<f64> = vec![];
   let mut yields_on_invested : Vec<f64> = vec![];
   let mut annualized_dividends : Vec<String> = vec![];
   data.iter().for_each(|e| {
        symbols.push(e.symbol);
        invested_values.push(e.invested_value.print());
        current_values.push(e.current_value.print());
        current_yields.push(e.current_yield*100.0);
        yields_on_invested.push(e.yield_on_invested*100.0);
        annualized_dividends.push(e.annualized_dividend.print());
   }); 
   let symbol_series = Series::new("Company",symbols); 
   let invested_values_series = Series::new("Investment", &invested_values);
   let current_values_series = Series::new("Current Value", &current_values);
   let current_yields_series = Series::new("Yield[%]", &current_yields);
   let yields_series = Series::new("Yield on investment[%]", &yields_on_invested);
   let annualized_dividends_series = Series::new("Annual dividend", &annualized_dividends);

   let df = DataFrame::new(vec![symbol_series, invested_values_series, current_values_series,
   current_yields_series, yields_series, annualized_dividends_series]).expect("Unable to create DataFrame")
        .sort(["Company"], false, true)
        .map_err(|_| "Unable to sort per company report dataframe").expect("Unable to sort DataFrame");
    println!("{df}");
}



fn compute_yield_on_investment(invested_value : f64, current_value : f64, current_yield : f64 ) -> f64 {
    current_value * current_yield / invested_value
}


struct Stock<'a> {
    symbol : &'a str,
    invested_value : Currency,
    current_value : Currency,
    current_yield : f64,
    yield_on_invested : f64,
    annualized_dividend : Currency,
}

impl<'a> Stock<'a> {
    fn new(symbol : &'a str, invested_value : Currency, current_value : Currency, current_yield : f64) -> Self {
        // compute yield on invested
        Self {
            symbol,
            invested_value,
            current_value,
            current_yield,
            yield_on_invested: compute_yield_on_investment(invested_value.value(), current_value.value(), current_yield),
            annualized_dividend: current_value.derive(current_yield*current_value.value()),

        }
    }
}

fn main() -> Result<(), String> {

    if std::env::var("POLARS_FMT_MAX_ROWS").is_err() {
        std::env::set_var("POLARS_FMT_MAX_ROWS", "-1")
    }
    // List of companies in a format (symbol, invested financial resources, current value, current
    // Yield)
    let ania = vec![
    Stock::new("ABEV", Currency::USD(3.11),  Currency::USD(4.95), 0.0609),
    Stock::new("BBY",  Currency::USD(80.35), Currency::USD(68.81), 0.0538),
    Stock::new("GOOGL",Currency::USD(58.68),   Currency::USD(56.13), 0.00252),
    Stock::new("KO",   Currency::USD(74.86),   Currency::USD(69.69),  0.02893),
    Stock::new("TGT",  Currency::USD(35.49),   Currency::USD(66.40),  0.04283),
    Stock::new("VZ",   Currency::USD(19.85),    Currency::USD(20.08),   0.0673),
    ];

    print_data_frame(&ania);

    let jacek = vec![
    Stock::new("BMO", Currency::USD(500.0),  Currency::USD(364.13), 0.0347),
    Stock::new("CAG", Currency::USD(481.99), Currency::USD(918.33), 0.0826),
    Stock::new("CNQ", Currency::USD(2.0),   Currency::USD(2.71),  0.0529),
    Stock::new("CVX", Currency::USD(1.0),   Currency::USD(1.43),  0.0422),
    Stock::new("EIX", Currency::USD(2.00),  Currency::USD(2.59),  0.0542),
    Stock::new("EPR", Currency::USD(975.26), Currency::USD(844.26), 0.0672),
    Stock::new("TW10",Currency::EUR(10.66),  Currency::EUR(22.53),  0.0437),
    Stock::new("UEI", Currency::EUR(21.29),   Currency::EUR(86.45),  0.0423),
    Stock::new("UPS", Currency::USD(99.99), Currency::USD(169.66), 0.0607),
    Stock::new("XRAY",Currency::USD(30.0),   Currency::USD(38.46),  0.0501),
    ];
    print_data_frame(&jacek);

    println!("ANIA:");
    print_summary(&ania);

    println!("JACEK:");
    print_summary(&jacek);

    // Get current USD exchange rate and EUR exchange rate
    
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

