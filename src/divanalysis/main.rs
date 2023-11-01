use calamine::{open_workbook, Reader, Xlsx};
use polars::prelude::*;
use std::collections::HashMap;

mod logging;
use logging::ResultExt;


//1) Stopa dywidendy (dywidenda / cena_akcji * 100%)
// - 1.5 - 2 x wzgledem S&P 500 stopy dywidendy
// - wyzsza niz inflacja (srednia inflacja historyczna 3.4% w USA)
// - 10% i wiecej to trzeba mocno sprawdzic stopa wyplaty dywidendy
// - zaleca 4.7%
//2) Stopa wyplat dywidendy (wyplycona dywidenda / zysk netto - licozny od przeplywow)
//  - nie wiecej niz 75% (chyba ze spolki REIT, komandytowo-akcyjne) 
//3) stopa wzrostu dywidendy (http://dripinvesting.org)
//  - zaleca 10%

fn analyze<R>(excel : &mut Xlsx<R>,category : &str) -> Result<(),&'static str> 
    where
        R: std::io::BufRead, 
        R: std::io::Read,
        R: std::io::Seek,
{
    log::info!("Processing category: {}", category);
    let names = excel
        .sheet_names();
    let name_sheet = names.iter().find(|x| *x == category).ok_or("Error: Category not found")?;

//let df: DataFrame = df!(
//    "integer" => &[1, 2, 3, 4, 5],
//    "date" => &[
//        "12-10-2023",
//        "12-10-2023",
//        "12-10-2023",
//        "12-10-2023",
//        "12-10-2023"
//    ],
//    "float" => &[4.0, 5.0, 6.0, 7.0, 8.0],
//)
//.unwrap();

//let out = df.clone().select(["integer", "float"]).expect_and_log("Error: Select failed!");

//log::info!("{out}");

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

        let mut columns : Vec<&str> = Vec::default();
        let mut sseries: HashMap<usize, Vec<&str>> = HashMap::new(); 
        let mut fseries : HashMap<usize, Vec<f64>> = HashMap::new(); 
        for c in categories {

            // Find indices of interesting collumns
            if let Some(v) = c.get_string() {
                columns.push(v); 
            }
        }
        log::info!("Columns: {:?}",columns);

        // Iterate through rows of actual sold transactions
        for row in rows {
            //log::info!("{:?}",row);

            for (i,cell) in row.iter().enumerate() {

                match cell {
                    calamine::DataType::Float(f) => {
                        if fseries.contains_key(&i) {
                            let vf = fseries.get_mut(&i).ok_or("Error: accessing invalid category")?;
                            vf.push(*f);
                        } else {
                            fseries.insert(i,vec![*f]);
                        } },
                    calamine::DataType::String(s) => {
                        if sseries.contains_key(&i) {
                            let vf = sseries.get_mut(&i).ok_or("Error: accessing invalid category")?;
                            vf.push(s);
                        } else {
                            sseries.insert(i,vec![s]);
                        } 
                    },
                    _ => (),
                }


            }
        }

        // Build DataFrame
        let mut df_series : Vec<Series> = vec![]; 
        fseries.iter().for_each(|(k,v)| {
            let s = Series::new(columns[*k], v.into_iter().collect::<f64>());
            df_series.push(s);
        }); 
        log::info!("f32 DF {}", df.head(Some(2)));
        sseries.iter().for_each(|(k,v)| {
            let s = Series::new(columns[*k], v);
            df_series.push(s);
        });
        df = DataFrame::new(df_series).map_err(|_| "Error: Could not create DataFrame")?; 
    }

    log::info!("{}", df.head(Some(2)));
     

    Ok(())
}


fn main() -> Result<(),&'static str>
{
    println!("Hello financial analysis world!");
    logging::init_logging_infrastructure();

    let mut excel: Xlsx<_> = open_workbook("data/U.S.DividendChampions-LIVE.xlsx").map_err(|_| "Error: opening XLSX")?;

    // Champions
    analyze(&mut excel, "Champions")?;
    // Contenders


    Ok(())
}
