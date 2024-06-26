# investment-forecasting
Tools to help in investing in dividend companies based on data from [Dividend Champions list](https://moneyzine.com/investments/dividend-champions/) and 10-11-12 method of investing as described in this [book](https://getrichwithdividends.com/)
### How to use it?
1. Download [Dividend Champions list](https://moneyzine.com/investments/dividend-champions/)
2. Use divanalysis tool to find companies worth investing for
3. Use divforecasting tool to predict gains of chosen companies for your expected investment time. You can also manually type "custom" company parameters as it maybe useful for reference
      
### Examples:
##### Find a company from Dividend champions that is worth investing for based on 10-11-12 system:
cargo run --bin divanalysis -- --data data/U.S.DividendChampions-LIVE.xlsx 

##### List all companies which data is available via Polygon.io API
POLARS_FMT_MAX_ROWS=200 POLYGON_AUTH_KEY=<Your API Key>  cargo run --bin divanalysis --  --list-all

##### Get data according to 10-11-12 system for ABR (Arbor Realty Trust):
POLARS_FMT_MAX_COLS=9  POLYGON_AUTH_KEY=<your API key>  cargo run --bin divanalysis -- --company ABR

###### Output:
```bash
shape: (1, 9)
┌────────┬─────────────┬────────────┬──────────────────┬──────────────┬───────────┬─────────────────────────────────┬─────────────────┬───────────────────────────────┐
│ Symbol ┆ Share Price ┆ Recent Div ┆ Annual Frequency ┆ Div Yield[%] ┆ DGR5G[%]  ┆ Years of consecutive Div growth ┆ Payout ratio[%] ┆ Industry Desc                 │
│ ---    ┆ ---         ┆ ---        ┆ ---              ┆ ---          ┆ ---       ┆ ---                             ┆ ---             ┆ ---                           │
│ str    ┆ f64         ┆ f64        ┆ u32              ┆ f64          ┆ f64       ┆ u32                             ┆ f64             ┆ str                           │
╞════════╪═════════════╪════════════╪══════════════════╪══════════════╪═══════════╪═════════════════════════════════╪═════════════════╪═══════════════════════════════╡
│ ABR    ┆ 12.96       ┆ 0.43       ┆ 4                ┆ 12.962963    ┆ 10.193743 ┆ 11                              ┆ 131.519505      ┆ REAL ESTATE INVESTMENT TRUSTS │
└────────┴─────────────┴────────────┴──────────────────┴──────────────┴───────────┴─────────────────────────────────┴─────────────────┴───────────────────────────────┘
```

##### List all companies which data is available via DripInvesting XLSX documents
POLARS_FMT_MAX_ROWS=200 cargo run --bin divanalysis --  --list-all --data data/U.S.DividendChampions-JAN.xlsx

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
![image](https://github.com/jczaja/investment-forecasting/assets/15085062/0f9327b2-a3b5-4838-b538-6b7b93bc37bc)

