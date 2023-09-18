use chrono::NaiveDateTime;
use entity::auction::Entity as Auction;
use entity::{bid, bid::Entity as Bid};
use figment::{
    providers::{Format, Toml},
    Figment,
};
use migration::{Migrator, MigratorTrait};
use polars::df;
use polars::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ColumnTrait, DbConn, EntityTrait, QueryFilter};
use std::env;
use tracing::debug;

use crate::setup;

pub async fn load_data(raw_adf: &str, raw_bdf: &str) {
    // Set up DB
    let db_full_url = match env::var("DB_URL") {
        Ok(v) => v,
        // If env var is not set, get the url from the Rocket.toml file.
        Err(_e) => Figment::from(Toml::file("Penny.toml"))
            .extract_inner::<String>("default.databases.penny.url")
            .unwrap(),
    };

    let name = db_full_url.split('/').last().unwrap();
    let url = db_full_url.trim_end_matches(&format!("/{}", name));

    let db = match setup::set_up_db(url, name).await {
        Ok(db) => db,
        Err(err) => panic!("{}", err),
    };

    // Apply Database Migrations, panic on error
    Migrator::up(&db, None).await.unwrap();

    debug!("Connected to database");

    load_aucs(&db, raw_adf).await.unwrap();
    load_bids(&db, raw_bdf).await.unwrap();
}

fn dollar_to_penny(d: Decimal) -> u64 {
    let pennies = d * dec!(100);
    let i: i64 = pennies.round().try_into().expect("Too big.");
    i as u64
}

async fn load_aucs(db: &DbConn, path: &str) -> Result<(), PolarsError> {
    debug!("Loading aucs from database");
    let aucs = Auction::find().all(db).await.unwrap();
    let mut adf = df! [
      "auction_id"      => aucs.iter().map(|a| a.id).collect::<Vec<i32>>(),
      "start_time"      => aucs.iter().map(|a| a.start_time).collect::<Vec<NaiveDateTime>>(),
      "bin_price"       => aucs.iter().map(|a| dollar_to_penny(a.buy_it_now_price)).collect::<Vec<u64>>(),
      "no_jumper_limit" => aucs.iter().map(|a| dollar_to_penny(a.no_jumper_limit)).collect::<Vec<u64>>(),
      "exchangeable"    => aucs.iter().map(|a| a.exchangeable).collect::<Vec<bool>>(),
      "one_per_user"    => aucs.iter().map(|a| a.one_per_user).collect::<Vec<bool>>(),
      "no_re_entry"     => aucs.iter().map(|a| a.no_re_entry).collect::<Vec<bool>>(),
      "is_bindolence"   => aucs.iter().map(|a| a.is_bindolence).collect::<Vec<Option<bool>>>(),
      "percent_off"     => aucs.iter().map(|a| a.percent_off).collect::<Vec<Option<i32>>>(),
      "end_time"        => aucs.iter().map(|a| a.auction_end).collect::<Vec<Option<NaiveDateTime>>>(),
    ]?;

    debug!("Saving aucs to file");
    let mut new_adf_file = std::fs::File::create(path).unwrap();
    ParquetWriter::new(&mut new_adf_file)
        .finish(&mut adf)
        .unwrap();
    Ok(())
}

async fn load_bids(db: &DbConn, path: &str) -> Result<(), PolarsError> {
    debug!("Loading bids from database");
    let bids =
        Bid::find()
            .filter(bid::Column::Timestamp.gt(
                NaiveDateTime::parse_from_str("2023-01-01 07:00 AM", "%Y-%m-%d %H:%M %p").unwrap(),
            ))
            .all(db)
            .await
            .unwrap();
    let mut bdf = df! [
      "auction_id" => bids.iter().map(|b| b.auction_id).collect::<Vec<i32>>(),
      "price"      => bids.iter().map(|b| dollar_to_penny(b.price)).collect::<Vec<u64>>(),
      "timestamp"  => bids.iter().map(|b| b.timestamp).collect::<Vec<NaiveDateTime>>(),
      "username"   => bids.into_iter().map(|b| b.username).collect::<Vec<String>>(),
    ]?;

    debug!("Saving bids to file");
    let mut new_bdf_file = std::fs::File::create(path).unwrap();
    ParquetWriter::new(&mut new_bdf_file)
        .finish(&mut bdf)
        .unwrap();
    Ok(())
}
