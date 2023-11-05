use chrono::{Datelike, NaiveDate, NaiveDateTime};
use gnuplot::{AxesCommon, Caption, Color, Coordinate, Figure, Tick};

enum Target {
    manual(&'static str,f64,f64,f64),
    symbol(String),
}

fn compute_dividend_gain(
    num_shares: f64,
    div_yield_rate: f64,
    share_price: f64,
    num_capitalizations: u32,
    transaction_tax: f64,
) -> f64 {
    let gains: f64 = num_shares * div_yield_rate * share_price / (num_capitalizations as f64)
        * (1.0 - transaction_tax);
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

    time_line.iter().for_each(|x| {
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
    let colors: Vec<&str> = vec!["blue", "green", "navy", "web-green"];
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
    base_capital: f64,
    companies: Vec<Target>,
    investment_years: u32,
) {
    let time_data: Vec<u32> = (1u32..365 * investment_years + 1).collect();

    let tax_rate = 0.15;
    let num_capitalizations: u32 = 4;
    let share_price_growth_rate: f64 = 0.034;

    // make actual plot
    let colors: Vec<&str> = vec!["blue", "green", "navy", "web-green"];
    let mut fg = Figure::new();
    fg.set_terminal("pngcairo size 1280,960", "dividend-investment-gains.png");

    let axes = fg
        .axes2d()
        .set_title(
            &format!("Dividend Investment Forecasting (starting capital[$]: {base_capital:.2} )"),
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
    //        .set_y_range(
    //            gnuplot::AutoOption::Fix(0.0),
    //            gnuplot::AutoOption::Fix(max_range as f64 * 3.0 as f64),
    //        );

    companies.iter().enumerate().for_each(|(i, x)| {

        match x {
            Target::manual(name,dy,dyg,sp) => {

                // Get Dividend prediction
                let (capital, gains) = forecast_dividend_gains(
                    base_capital,
                    *dy,
                    *dyg,
                    *sp,
                    share_price_growth_rate,
                    tax_rate,
                    &time_data,
                    num_capitalizations,
                );

                let caption = match gains.last() {
                    Some(x) => format!(
                        "{name}(DIV Yield: {}, DYG 5G: {}, Price[$]: {}) (Capital in Stock[$]: {:.2}, Total Dividends Gains[$]: {:.2} )",*dy,*dyg,*sp, capital, x
                    ),
                    None => panic!("Error: No dividend data to plot!"),
                };
                axes.lines(&time_data, &gains, &[Caption(&caption), Color(colors[i])]);

            },
            Target::symbol(name) => {
                // TODO: Load Data
            },
        }
    }); 
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
) -> (f64, Vec<f64>) {
    let mut gains: Vec<f64> = vec![];

    let mut curr_gain: f64 = 0.0;
    let mut share_price = share_price;
    let mut div_yield = div_yield;

    let capitalization_period = 365 / num_capitalizations;

    let num_shares = base_capital / share_price;
    time_line.iter().for_each(|x| {
        if x % capitalization_period == 0 {
            let g: f64;
            g = compute_dividend_gain(
                num_shares,
                div_yield,
                share_price,
                num_capitalizations,
                tax_rate,
            );
            curr_gain += g;
        }
        if x % 365 == 0 {
            // Share price and div yeild update
            // Compute new share price
            share_price = share_price * (1.0 + share_price_growth_rate);
            // Compute new Div Yield
            div_yield = div_yield * (1.0 + div_yield_growth_5y);
        }
        gains.push(curr_gain);
    });

    (num_shares * share_price, gains)
}

fn main() {
    println!("Hello, investment forecasting world!");

    let base_capital: f64 = 10000.0;

    forecast_low_risk_instruments(base_capital);

    let div_yield: f64 = 0.05;
    let div_yield_growth_5y: f64 = 0.10;
    let share_price: f64 = 100.0;

    forecast_dividend_stocks(base_capital, vec![Target::manual("REFERENCE",div_yield,div_yield_growth_5y,share_price)], 1);
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

        assert_eq!(
            ref_gain,
            compute_dividend_gain(
                num_shares,
                div_yield_rate,
                share_price,
                num_capitalizations,
                transaction_tax
            )
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

        // Compute dividend gains and value of stock
        let (final_capital, gains) = forecast_dividend_gains(
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

        // total dividend payout :
        // 1000.0*0.5/4.0*(1.0-0.15) = 106.25 <- c1
        // (1000.0*(0.5/4.0)*(1.0-0.15)= 106.25 <- c2
        // 1000.0*0.5/4.0*(1.0-0.15) = 106.25 <- c3
        // (1000.0*(0.5/4.0)*(1.0-0.15)= 106.25 <- c4
        // c1 + c2 + c3 + c4 = 106.25*4.0 = 425.0

        let ref_total_payout: f64 = 425.0;

        // Compute dividend gains and value of stock
        let (final_capital, gains) = forecast_dividend_gains(
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
        // (1000.0*(1.0+0.1))*(0.5*(1.0+0.1))/4.0*(1.0-0.15) = 128.5625 <- c5
        // (1000.0*(1.0+0.1))*(0.5*(1.0+0.1))/4.0*(1.0-0.15) = 128.5625 <- c6
        // (1000.0*(1.0+0.1))*(0.5*(1.0+0.1))/4.0*(1.0-0.15) = 128.5625 <- c7
        // (1000.0*(1.0+0.1))*(0.5*(1.0+0.1))/4.0*(1.0-0.15) = 128.5625 <- c8
        // c1 + c2 + c3 + c4 + c5 +c6 +c7 +c8 = 106.25*4.0 + 128.5625*4.0 = 939.25
        let ref_total_payout: f64 = 939.25;

        // Compute dividend gains and value of stock
        let (final_capital, gains) = forecast_dividend_gains(
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
        match gains.last() {
            Some(total_payout) => {
                assert_eq!(((*total_payout * 100.0).round() / 100.0), ref_total_payout);
                return Ok(());
            }
            None => return Err(format!("Error: No dividend gains found!")),
        }
    }
}
