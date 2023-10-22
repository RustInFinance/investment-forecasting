use gnuplot::{AxesCommon, Caption, Color, Coordinate, Figure, Tick};
use chrono::{Datelike, NaiveDate, NaiveDateTime};

fn compute_gain(capital : f32, investment_rate : f32, num_capitalizations : u32, transaction_tax : f32) -> (f32,f32) {
        let gains : f32 = capital*investment_rate/(num_capitalizations as f32)*(1.0 - transaction_tax);
        (capital+gains,gains)
}


//TODO: gains should be added to have total gain tracing
fn generate_revolut_gains(capital : f32, investment_rate : f32,time_line : &Vec<u32>) -> (f32,Vec<f32>)
{
    let mut gains : Vec<f32> = vec![];

    let mut curr_capital = capital;
    let mut curr_gain :f32 = 0.0;

    time_line.iter().for_each(|x| {
        let g : f32;
        (curr_capital,g) = compute_gain(curr_capital, investment_rate, 365, 0.0); 
        curr_gain+=g;
        gains.push(curr_gain);
    });

    curr_capital = capital + (curr_capital - capital)*(1.0 - 0.19);

    let ng = match gains.pop() {
       Some(g) => g*(1.0-0.19),
       None => panic!("This is wrong!"),
    };

    gains.push(ng);

    (curr_capital,gains)
}

fn generate_obligacje_gains(capital : f32, investment_rate : f32,time_line : &Vec<u32>) -> (f32,Vec<f32>)
{

    let mut gains : Vec<f32> = vec![];
    let mut curr_capital = capital;
    let mut curr_gain = 0.0f32;

    time_line.iter().for_each(|x| {
        let mut g : f32 = 0.0f32; 
        if x % 365 == 0 {
            (curr_capital,g) = compute_gain(curr_capital, investment_rate, 1, 0.0); 
        }
        curr_gain+=g;
        gains.push(curr_gain as f32);
    });
    let ng = match gains.pop() {
       Some(g) => g*(1.0-0.19),
       None => panic!("This is wrong!"),
    };
    gains.push(ng);

    (curr_capital,gains)
}

fn generate_santander_gains(capital : f32, investment_rate : f32,time_line : &Vec<u32>) -> (f32,Vec<f32>)
{
    let mut gains : Vec<f32> = vec![];
    let mut curr_capital = capital;
    let mut curr_gain = 0.0f32;

    time_line.iter().for_each(|x| {
        let mut g : f32 = 0.0f32; 
        if x % 30 == 0 {
            (curr_capital,g) = compute_gain(curr_capital, investment_rate, 12, 0.19); 
        }
        curr_gain+=g;
        gains.push(curr_gain as f32);
    });

    (curr_capital,gains)
}


fn main() {
    println!("Hello, investment forecasting world!");

    let time_data : Vec<u32> = (1u32..365u32).collect();

    let base_capital : f32 = 20000.0;

    // Data of Revolut
    let (total_revolut, revolut_gains) = generate_revolut_gains(base_capital,0.0405,&time_data);
    let (total_obligacje, obligacje_gains) = generate_revolut_gains(base_capital,0.075,&time_data);
    // Data of santander
    let (total_santander, santander_gains) = generate_santander_gains(base_capital,0.04,&time_data);
    let (total_toyota, toyota_gains) = generate_santander_gains(base_capital,0.07,&time_data);
    let max_range = if total_santander > total_revolut { (total_santander-base_capital) } else { (total_revolut-base_capital)}; 


    let revolut_caption = format!("Revolut (Total: {}, Gains: {})",total_revolut,(total_revolut-base_capital));
    let santander_caption = format!("Santander (Total: {}, Gains: {})",total_santander,(total_santander-base_capital));
    let toyota_caption = format!("Toyota (Total: {}, Gains: {})",total_toyota,(total_toyota-base_capital));
    let obligacje_caption = format!("Obligacje (Total: {}, Gains: {})",total_obligacje,(total_obligacje-base_capital));

    // make actual plot
    let colors: Vec<&str> = vec!["blue", "green", "navy", "web-green"];
    let mut fg = Figure::new();
    fg.set_terminal("pngcairo size 1280,960", "investment-gains.png");

    let axes = fg
        .axes2d()
        .set_title(&format!("Investment forecasting (input capital: {base_capital} zl)"), &[gnuplot::LabelOption::<&str>::Font("Arial", 15.0)])
        .set_x_label(
            "time[days]",
            &[gnuplot::LabelOption::<&str>::Font("Arial", 12.0)],
        )
        .set_y_label(
            "Gain",
            &[gnuplot::LabelOption::<&str>::Font("Arial", 12.0)],
        )
        .set_y_range(
            gnuplot::AutoOption::Fix(0.0),
            gnuplot::AutoOption::Fix( max_range as f64 *3.0 as f64), 
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ww_calculator() -> Result<(), String> {
        // 10000.0 * 0.04/365.0  = 1.0958904
        assert_eq!(compute_gain(10000.0,0.04,365, 0.0), (10000.0 + 1.0958904,1.0958904));
        // 10000.0 * 0.04/12.0 * (1.0-0.19)= 27.0
        assert_eq!(compute_gain(10000.0,0.04,12, 0.19), (10000.0 + 27.0,27.0));
        Ok(())
    }
}
