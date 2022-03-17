CREATE DATABASE ark_db

CREATE TABLE stocks (
    id SERIAL PRIMARY KEY,
    company TEXT NOT NULL,
    ticker TEXT NOT NULL,
    cusip TEXT NOT NULL
);


CREATE TABLE etf_holdings (
    dt DATE NOT NULL, 
    fund TEXT NOT NULL,
    stock_id INTEGER NOT NULL,
    shares INTEGER NOT NULL,
    market_value NUMERIC,
    weight NUMERIC,
    PRIMARY KEY (dt, fund, stock_id),
    CONSTRAINT fk_stock FOREIGN KEY (stock_id) REFERENCES stocks (id)
);

CREATE INDEX ON etf_holdings (fund, dt DESC);

SELECT create_hypertable('etf_holdings', 'dt');