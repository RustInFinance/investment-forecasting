# investment-forecasting
Tools to help in investin in divdend companies based on data from [Dividend Champions list](https://moneyzine.com/investments/dividend-champions/) and 10-11-12 method of investing as described in this [book](https://getrichwithdividends.com/)

### Examples:
##### Find a company from Dividend champions that is worth investing for based on 10-11-12 system:
cargo run --bin divanalysis -- --data data/U.S.DividendChampions-LIVE.xlsx 

###### Output:
```bash
shape: (1, 5)
┌────────┬───────────────────────────────┬─────────────┬───────────┬───────┐
│ Symbol ┆ Company                       ┆ Current Div ┆ Div Yield ┆ Price │
│ ---    ┆ ---                           ┆ ---         ┆ ---       ┆ ---   │
│ str    ┆ str                           ┆ f64         ┆ f64       ┆ f64   │
╞════════╪═══════════════════════════════╪═════════════╪═══════════╪═══════╡
│ CTBI   ┆ Community Trust Bancorp, Inc. ┆ 0.46        ┆ 5.17      ┆ 35.61 │
└────────┴───────────────────────────────┴─────────────┴───────────┴───────┘
```
##### Predict Dividend gains for ABM Industries Inc. (ABM) for 4 years investment period
cargo run --bin divforecasting -- --company ABM --data data/U.S.DividendChampions-LIVE.xlsx  --years 4

##### Predict Dividend gains for Apple company (parameters defined manually) for 5 years investment period:
cargo run --bin divforecasting -- --custom-name Apple --custom-price 218.86 --custom-div-yield 1.33 --custom-div-growth 7.27  --tax-rate 0.0 --share-price-growth-rate=-19.4 --years 5 --capital 218.86 

##### Predict Dividend gains for Apple company (parameters defined manually) and ABM and CTBI for 5 years investment period:
cargo run --bin divforecasting -- --custom-name Apple --custom-price 218.86 --custom-div-yield 1.33 --custom-div-growth 7.27  --tax-rate 0.0 --share-price-growth-rate=-19.4 --years 5 --capital 1000.0 --company ABM --company CTBI --data data/U.S.DividendChampions-LIVE.xlsx 
###### Output:

