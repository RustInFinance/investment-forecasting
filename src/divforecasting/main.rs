use calamine::{open_workbook, Xlsx};
use clap::Parser;
use gnuplot::{AxesCommon, Caption, Color, Coordinate, Figure};
use polars::prelude::*;

// TODO: frequency of div paid should be yield based on historical data not fixed to four

/// Program to predict gains from Dividend companies (Fetch XLSX list from: https://moneyzine.com/investments/dividend-champions/)
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Symbol names of companies from dividend list as provided with "data" argument
    #[arg(long, default_value = "dividend-investment-gains.png")]
    output: String,

    /// Data in XLSX format (Fetch from https://moneyzine.com/investments/dividend-champions/)
    #[arg(long)]
    data: Option<String>,

    /// Symbol names of companies from dividend list as provided with "data" argument
    #[arg(long, default_values_t = &[] )]
    company: Vec<String>,

    /// Custom (not taken from the list) company name
    #[arg(long, required = false, requires_all = &["custom_price","custom_div_yield","custom_div_growth"])]
    custom_name: Option<String>,

    /// Custom company share price[$]
    #[arg(long, required = false)]
    custom_price: Option<f64>,

    /// Custom company dividend yield[%]
    #[arg(long)]
    custom_div_yield: Option<f64>,

    /// Custom company dividend growth[%]
    #[arg(long)]
    custom_div_growth: Option<f64>,

    /// Capital to be invested[$]
    #[arg(long, default_value_t = 10000.0)]
    capital: f64,

    /// An Average shares price annual growth rate[%]
    #[arg(long, default_value_t = 7.4)]
    share_price_growth_rate: f64,

    /// Length of investment [years]
    #[arg(long, default_value_t = 4)]
    years: u32,

    #[arg(long, default_value_t = 15.0)]
    tax_rate: f64,
}

enum Target<'a> {
    manual(&'a str, f64, f64, f64),
    symbol(&'a str),
}

fn compute_dividend_gain(
    num_shares: f64,
    curr_div: f64,
    num_capitalizations: u32,
    transaction_tax: f64,
) -> f64 {
    let gains: f64 = num_shares * curr_div / (num_capitalizations as f64) * (1.0 - transaction_tax);
    gains
}

fn compute_gain(
    capital: f64,
    investment_rate: f64,
    num_capitalizations: u32,
    transaction_tax: f64,
) -> (f64, f64) {
    let gains: f64 =
        capital * investment_rate / (num_capitalizations as f64) * (1.0 - transaction_tax);
    (capital + gains, gains)
}

//TODO: gains should be added to have total gain tracing
fn generate_revolut_gains(
    capital: f64,
    investment_rate: f64,
    time_line: &Vec<u32>,
) -> (f64, Vec<f64>) {
    let mut gains: Vec<f64> = vec![];

    let mut curr_capital = capital;
    let mut curr_gain: f64 = 0.0;

    time_line.iter().for_each(|_| {
        let g: f64;
        (curr_capital, g) = compute_gain(curr_capital, investment_rate, 365, 0.0);
        curr_gain += g;
        gains.push(curr_gain);
    });

    curr_capital = capital + (curr_capital - capital) * (1.0 - 0.19);

    let ng = match gains.pop() {
        Some(g) => g * (1.0 - 0.19),
        None => panic!("This is wrong!"),
    };

    gains.push(ng);

    (curr_capital, gains)
}

fn generate_bonds_gains(
    capital: f64,
    investment_rate: f64,
    time_line: &Vec<u32>,
) -> (f64, Vec<f64>) {
    let mut gains: Vec<f64> = vec![];
    let mut curr_capital = capital;
    let mut curr_gain = 0.0;

    time_line.iter().for_each(|x| {
        let mut g: f64 = 0.0;
        if x % 365 == 0 {
            (curr_capital, g) = compute_gain(curr_capital, investment_rate, 1, 0.0);
        }
        curr_gain += g;
        gains.push(curr_gain);
    });
    let ng = match gains.pop() {
        Some(g) => g * (1.0 - 0.19),
        None => panic!("This is wrong!"),
    };
    gains.push(ng);

    (curr_capital, gains)
}

fn generate_santander_gains(
    capital: f64,
    investment_rate: f64,
    time_line: &Vec<u32>,
) -> (f64, Vec<f64>) {
    let mut gains: Vec<f64> = vec![];
    let mut curr_capital = capital;
    let mut curr_gain = 0.0;

    time_line.iter().for_each(|x| {
        let mut g: f64 = 0.0;
        if x % 30 == 0 {
            (curr_capital, g) = compute_gain(curr_capital, investment_rate, 12, 0.19);
        }
        curr_gain += g;
        gains.push(curr_gain);
    });

    (curr_capital, gains)
}

fn forecast_low_risk_instruments(base_capital: f64) {
    let time_data: Vec<u32> = (1u32..365u32).collect();

    // Data of Revolut
    let (total_revolut, revolut_gains) = generate_revolut_gains(base_capital, 0.0405, &time_data);
    let (total_obligacje, obligacje_gains) =
        generate_revolut_gains(base_capital, 0.075, &time_data);
    // Data of santander
    let (total_santander, santander_gains) =
        generate_santander_gains(base_capital, 0.04, &time_data);
    let (total_toyota, toyota_gains) = generate_santander_gains(base_capital, 0.07, &time_data);
    let max_range = if total_santander > total_revolut {
        total_santander - base_capital
    } else {
        total_revolut - base_capital
    };

    let revolut_caption = format!(
        "Revolut (Total: {}, Gains: {})",
        total_revolut,
        (total_revolut - base_capital)
    );
    let santander_caption = format!(
        "Santander (Total: {}, Gains: {})",
        total_santander,
        (total_santander - base_capital)
    );
    let toyota_caption = format!(
        "Toyota (Total: {}, Gains: {})",
        total_toyota,
        (total_toyota - base_capital)
    );
    let obligacje_caption = format!(
        "Obligacje (Total: {}, Gains: {})",
        total_obligacje,
        (total_obligacje - base_capital)
    );

    // make actual plot
    let colors: Vec<&str> = vec!["blue", "green", "navy", "web-green", "#127cc1", "#76B900"];
    let mut fg = Figure::new();
    fg.set_terminal("pngcairo size 1280,960", "investment-gains.png");

    let axes = fg
        .axes2d()
        .set_title(
            &format!("Investment forecasting (input capital: {base_capital} zl)"),
            &[gnuplot::LabelOption::<&str>::Font("Arial", 15.0)],
        )
        .set_x_label(
            "time[days]",
            &[gnuplot::LabelOption::<&str>::Font("Arial", 12.0)],
        )
        .set_y_label("Gain", &[gnuplot::LabelOption::<&str>::Font("Arial", 12.0)])
        .set_y_range(
            gnuplot::AutoOption::Fix(0.0),
            gnuplot::AutoOption::Fix(max_range as f64 * 3.0 as f64),
        );

    axes.lines(
        &time_data,
        &revolut_gains,
        &[Caption(&revolut_caption), Color(colors[0])],
    );
    axes.lines(
        &time_data,
        &santander_gains,
        &[Caption(&santander_caption), Color(colors[1])],
    );
    axes.lines(
        &time_data,
        &toyota_gains,
        &[Caption(&toyota_caption), Color(colors[2])],
    );
    axes.lines(
        &time_data,
        &obligacje_gains,
        &[Caption(&obligacje_caption), Color(colors[3])],
    );
    fg.show().expect("Error plotting");
}

fn forecast_dividend_stocks(
    output_file_name: &str,
    base_capital: f64,
    data: Option<String>,
    companies: Vec<Target>,
    investment_years: u32,
    shares_price_growth_rate: f64,
    tax_rate: f64,
) {
    let time_data: Vec<u32> = (1u32..365 * investment_years + 1).collect();

    let tax_rate = tax_rate / 100.0;
    let mut num_capitalizations: u32 = 4;
    let shares_price_growth_rate = shares_price_growth_rate / 100.0;

    // make actual plot
    let colors: Vec<&str> = vec!["blue", "green", "navy", "web-green", "#127cc1", "#76B900"];
    let mut fg = Figure::new();
    let mut max_y = 0.0;
    fg.set_terminal("pngcairo size 1280,960", output_file_name);

    let axes = fg
        .axes2d()
        .set_title(
            &format!("Dividend {investment_years} years long investment forecasting (starting capital[$]: {base_capital:.2}, share price growth rate[%]: {:.2} ) ",shares_price_growth_rate*100.0),
            &[gnuplot::LabelOption::<&str>::Font("Arial", 15.0)],
        )
        .set_x_label(
            "time[days]",
            &[gnuplot::LabelOption::<&str>::Font("Arial", 12.0)],
        )
        .set_y_label(
            "Total Dividends",
            &[gnuplot::LabelOption::<&str>::Font("Arial", 12.0)],
        );

    companies.iter().enumerate().for_each(|(i, x)| {

        match x {
            Target::manual(name,dy,dyg,sp) => {

                // Get Dividend prediction
                let (capital, final_payout, gains) = forecast_dividend_gains(
                    base_capital,
                    *dy/100.0,
                    *dyg/100.0,
                    *sp,
                    shares_price_growth_rate,
                    tax_rate,
                    &time_data,
                    num_capitalizations,
                );

                let caption = match gains.last() {
                    Some(x) => {
                        if *x > max_y {
                            max_y = *x;
                        }
                        format!(
                        "{name}(DIVY[%]: {:.2}, DYG 5G[%]: {:.2}, Price[$]: {:.2}) (Stock[$]: {:.2}, Payout[$]: {:.2},Payout2Investment[%]: {:.2}, Total Dividends Gains[$]: {:.2} )",*dy,*dyg,*sp, capital, final_payout, (final_payout/base_capital)*100.0,x
                    )},
                    None => panic!("Error: No dividend data to plot!"),
                };
                axes.lines(&time_data, &gains, &[Caption(&caption), Color(colors[i])]);

            },
            Target::symbol(name) => {
                let name_str : &str = &name;
                let company = Series::new("", vec![name_str]);


                let (share_price, dy, dyg) = match data.clone() {
                    Some(database) => {

                        let mut excel: Xlsx<_> = open_workbook(database)
                            .map_err(|_| "Error: opening XLSX")
                            .expect("Could not open Dividends data file");
                        let all = investments_forecasting::load_list(&mut excel, "All").expect("Unable to load Data");

                        let mask = all.column("Symbol").unwrap().equal(&company).unwrap();
                        let company_data = all.filter(&mask).expect("Unable to filter loaded data");

                        let price_series = company_data.column("Price").unwrap();
                        let price = price_series.get(0).expect("Unable to get Price of selected company");

                        let dy_series = company_data.column("Div Yield").unwrap();
                        let dy = dy_series.get(0).expect("Unable to get Div Yield of selected company");

                        let dyg_series = company_data.column("DGR 5Y").unwrap();
                        let dyg = dyg_series.get(0).expect("Unable to get DGR 5Y of selected company");

                        // Dividend list has percentages of values so we need to convert them from e.g. 1%
                        // to 0.01 etc.
                        let (price,dy,dyg) = match (price,dy,dyg) {
                            (AnyValue::Float64(valp),AnyValue::Float64(vald),AnyValue::Float64(valg)) => (valp,vald/100.0,valg/100.0),
                            _ => panic!("Unable to get price value"),
                        };
                        (price, dy, dyg)
                    }
                    None => {

                        let (share_price, _, divy, frequency,  dgr, _, _, _) =
                            investments_forecasting::get_polygon_data(&name).expect("Error: unable to get Data from polygon IO for forecasting");
                        num_capitalizations = frequency;
                        log::info!("Forcasting stock: {name} with params: share price({share_price}), Frequency(frequency), Div yield[%]({divy}), DGR5Y[%]({dgr})");
                        (share_price, divy/100.0, dgr/100.0)
                    },
                };

                // Get Dividend prediction
                let (capital, final_payout, gains) = forecast_dividend_gains(
                    base_capital,
                    dy,
                    dyg,
                    share_price,
                    shares_price_growth_rate,
                    tax_rate,
                    &time_data,
                    num_capitalizations,
                );
                let caption = match gains.last() {
                    Some(x) => {
                        if *x > max_y {
                            max_y = *x;
                        }
                        format!(
                        "{name}(DIV Yield[%]: {:.2}, DYG 5G[%]: {:.2}, Price[$]: {:.2}) (Capital in Stock[$]: {:.2}, Payout[$]: {:.2}, Total Dividends Gains[$]: {:.2} )",dy*100.0,dyg*100.0,share_price, capital, final_payout,x
                    )},
                    None => panic!("Error: No dividend data to plot!"),
                };
                axes.lines(&time_data, &gains, &[Caption(&caption), Color(colors[i])]);
            },
        }
    });

    // Extend Y range to fit plot titles
    axes.set_y_range(
        gnuplot::AutoOption::Fix(0.0),
        gnuplot::AutoOption::Fix(max_y * 1.2 as f64),
    );

    let info = format!(
        "Notes:\n   * {}% of Tax is applied to every dividend pay-out\n",
        tax_rate * 100.0
    );
    axes.label(
        &info,
        Coordinate::Graph(0.02),
        Coordinate::Graph(0.75),
        &[gnuplot::LabelOption::<&str>::Font("Arial", 15.0)],
    );

    fg.show().expect("Error plotting");
}

fn forecast_dividend_gains(
    base_capital: f64,
    div_yield: f64,
    div_yield_growth_5y: f64,
    share_price: f64,
    share_price_growth_rate: f64,
    tax_rate: f64,
    time_line: &Vec<u32>,
    num_capitalizations: u32,
) -> (f64, f64, Vec<f64>) {
    let mut gains: Vec<f64> = vec![];

    let mut curr_gain: f64 = 0.0;
    let mut share_price = share_price;
    let mut curr_div = div_yield * share_price;

    let mut last_gain = 0.0;

    let capitalization_period = 365 / num_capitalizations;

    let num_shares = base_capital / share_price;
    log::info!("Company: Price[$]: {share_price},  Num Shares: {num_shares} , ANNUAL DIV PER SHARE[$]: {curr_div}");

    time_line.iter().for_each(|x| {
        if x % capitalization_period == 0 {
            let g: f64;
            g = compute_dividend_gain(num_shares, curr_div, num_capitalizations, tax_rate);
            curr_gain += g;
            last_gain = g;
            log::info!(
                "Company: Price[$]: {share_price},  Num Shares: {num_shares} ,PAYED DIV[$]: {g}"
            );
        }
        if x % 365 == 0 {
            // Share price and div yeild update
            // Compute new share price
            share_price = share_price * (1.0 + share_price_growth_rate);
            // Compute new Div Yield
            curr_div = curr_div * (1.0 + div_yield_growth_5y);
        }
        gains.push(curr_gain);
    });

    (num_shares * share_price, last_gain, gains)
}

fn main() {
    println!("Hello, investment forecasting world!");

    investments_forecasting::init_logging_infrastructure();
    let args = Args::parse();

    forecast_low_risk_instruments(args.capital);

    let mut targets: Vec<Target> = vec![];
    args.company
        .iter()
        .for_each(|symbol| targets.push(Target::symbol(&symbol)));

    if let Some(name) = args.custom_name {
        match (
            args.custom_div_yield,
            args.custom_div_growth,
            args.custom_price,
        ) {
            (Some(dy), Some(dg), Some(p)) => {
                targets.push(Target::manual(&name, dy, dg, p));
                forecast_dividend_stocks(
                    args.output.as_ref(),
                    args.capital,
                    args.data,
                    targets,
                    args.years,
                    args.share_price_growth_rate,
                    args.tax_rate,
                );
            }
            _ => panic!("\nError: Missing some custom arguments"),
        }
    } else {
        forecast_dividend_stocks(
            args.output.as_ref(),
            args.capital,
            args.data,
            targets,
            args.years,
            args.share_price_growth_rate,
            args.tax_rate,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ww_calculator() -> Result<(), String> {
        // 10000.0 * 0.04/365.0  = 1.0958904
        assert_eq!(
            compute_gain(10000.0, 0.04, 365, 0.0),
            (10000.0 + 1.09589041096, 1.095890410958904)
        );

        // 10000.0 * 0.04/12.0 * (1.0-0.19)= 27.0
        assert_eq!(
            compute_gain(10000.0, 0.04, 12, 0.19),
            (10000.0 + 27.0, 27.000000000000004)
        );
        Ok(())
    }

    #[test]
    fn test_compute_dividend_gains() -> Result<(), String> {
        let num_shares = 10.0;
        let div_yield_rate = 0.1;
        let share_price = 100.0;
        let num_capitalizations = 4;
        let transaction_tax = 0.1;

        // num_shares*share_price*div_yield_rate / (num_capitalizations as f64) * (1.0 - transaction_tax);
        //1000.0*0.1/(4.0)*0.9 = 25.0*0.9 = 22.5;
        let ref_gain = 22.5;
        let curr_div = share_price * div_yield_rate;

        assert_eq!(
            ref_gain,
            compute_dividend_gain(num_shares, curr_div, num_capitalizations, transaction_tax)
        );

        Ok(())
    }

    #[test]
    fn test_dividend_gains() -> Result<(), String> {
        let time_data: Vec<u32> = (1u32..366).collect();

        let base_capital = 1000.0;
        let tax_rate = 0.15;
        let num_capitalizations: u32 = 1;
        let div_yield: f64 = 0.5;
        let div_yield_growth_5y: f64 = 0.10;
        let share_price: f64 = 100.0;
        let share_price_growth_rate: f64 = 0.1;

        // final capital : (1000.0 * (1.0 + 0.1)) = 1100.0
        let ref_final_capital = 1100.00;

        // total dividend payout : 1000.0*(0.5)*(1.0-0.15)
        let ref_total_payout: f64 = 425.0;

        // final payout : 1000.0*(0.5)/1.0*(1.0-0.15)
        let ref_final_payout: f64 = 425.0;

        // Compute dividend gains and value of stock
        let (final_capital, final_payout, gains) = forecast_dividend_gains(
            base_capital,
            div_yield,
            div_yield_growth_5y,
            share_price,
            share_price_growth_rate,
            tax_rate,
            &time_data,
            num_capitalizations,
        );

        assert_eq!(ref_final_capital, ((final_capital * 100.0).round() / 100.0));
        assert_eq!(ref_final_payout, ((final_payout * 100.0).round() / 100.0));
        match gains.last() {
            Some(total_payout) => {
                assert_eq!(*total_payout, ref_total_payout);
                return Ok(());
            }
            None => return Err(format!("Error: No dividend gains found!")),
        }
    }

    #[test]
    fn test_dividend_gains_2() -> Result<(), String> {
        let time_data: Vec<u32> = (1u32..366).collect();

        let base_capital = 1000.0;
        let tax_rate = 0.15;
        let num_capitalizations: u32 = 4;
        let div_yield: f64 = 0.5;
        let div_yield_growth_5y: f64 = 0.10;
        let share_price: f64 = 100.0;
        let share_price_growth_rate: f64 = 0.1;

        // final capital : (1000.0 * (1.0 + 0.1/4.0) )*(1.025)*(1.025)*(1.025) = 1103.812891
        let ref_final_capital = 1100.00;

        // final payout : 1000.0*(0.5)/4.0*(1.0-0.15)

        // total dividend payout :
        // 1000.0*0.5/4.0*(1.0-0.15) = 106.25 <- c1
        // 1000.0*(0.5/4.0)*(1.0-0.15)= 106.25 <- c2
        // 1000.0*0.5/4.0*(1.0-0.15) = 106.25 <- c3
        // (1000.0*(0.5/4.0)*(1.0-0.15)= 106.25 <- c4 (final payout)
        // c1 + c2 + c3 + c4 = 106.25*4.0 = 425.0
        let ref_final_payout: f64 = 106.25;
        let ref_total_payout: f64 = 425.0;

        // Compute dividend gains and value of stock
        let (final_capital, final_payout, gains) = forecast_dividend_gains(
            base_capital,
            div_yield,
            div_yield_growth_5y,
            share_price,
            share_price_growth_rate,
            tax_rate,
            &time_data,
            num_capitalizations,
        );

        assert_eq!(ref_final_payout, ((final_payout * 100.0).round() / 100.0));
        assert_eq!(ref_final_capital, ((final_capital * 100.0).round() / 100.0));
        match gains.last() {
            Some(total_payout) => {
                assert_eq!(((*total_payout * 100.0).round() / 100.0), ref_total_payout);
                return Ok(());
            }
            None => return Err(format!("Error: No dividend gains found!")),
        }
    }

    #[test]
    fn test_dividend_gains_3() -> Result<(), String> {
        let num_years = 2;
        let time_data: Vec<u32> = (1u32..365 * num_years + 1).collect();

        let base_capital = 1000.0;
        let tax_rate = 0.15;
        let num_capitalizations: u32 = 4;
        let div_yield: f64 = 0.5;
        let div_yield_growth_5y: f64 = 0.10;
        let share_price: f64 = 100.0;
        let share_price_growth_rate: f64 = 0.1;

        // final capital : (1000.0 * (1.0 + 0.1) )*(1.0+ 0.1) =1210.0
        let ref_final_capital = 1210.00;

        // total dividend payout :
        // 1000.0*0.5/4.0*(1.0-0.15) = 106.25 <- c1
        // (1000.0*(0.5/4.0)*(1.0-0.15)= 106.25 <- c2
        // 1000.0*0.5/4.0*(1.0-0.15) = 106.25 <- c3
        // (1000.0*(0.5/4.0)*(1.0-0.15)= 106.25 <- c4
        // ((1000.0*0.5)*(1.0+0.1))/4.0*(1.0-0.15) = 116.875 <- c5
        // ((1000.0*0.5)*(1.0+0.1))/4.0*(1.0-0.15) = 116.875 <- c6
        // ((1000.0*0.5)*(1.0+0.1))/4.0*(1.0-0.15) = 116.875 <- c7
        // ((1000.0*0.5)*(1.0+0.1))/4.0*(1.0-0.15) = 116.875 <- c8 (final payout)
        // c1 + c2 + c3 + c4 + c5 +c6 +c7 +c8 = 106.25*4.0 + 116.875*4.0 = 892.5
        let ref_total_payout: f64 = 892.5;
        let ref_final_payout: f64 = 116.88;

        // Compute dividend gains and value of stock
        let (final_capital, final_payout, gains) = forecast_dividend_gains(
            base_capital,
            div_yield,
            div_yield_growth_5y,
            share_price,
            share_price_growth_rate,
            tax_rate,
            &time_data,
            num_capitalizations,
        );

        assert_eq!(ref_final_payout, ((final_payout * 100.0).round() / 100.0));
        assert_eq!(ref_final_capital, ((final_capital * 100.0).round() / 100.0));
        match gains.last() {
            Some(total_payout) => {
                assert_eq!(((*total_payout * 100.0).round() / 100.0), ref_total_payout);
                return Ok(());
            }
            None => return Err(format!("Error: No dividend gains found!")),
        }
    }
}
