use polars::prelude::*;
use std::fs::File;
use tracing::debug;

mod load;
mod ops;
mod setup;

#[tokio::main]
async fn main() {
    // Set up logging
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("error,processor=debug")
        .with_writer(non_blocking)
        .compact()
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let raw_adf = "./data/raw/adf.parquet";
    let raw_bdf = "./data/raw/bdf.parquet";

    // Uncomment if your data is in a postgres database.
    // load::load_data(raw_adf, raw_bdf).await;
    process_data(raw_adf, raw_bdf).await.unwrap();
}

async fn process_data(raw_adf_path: &str, raw_bdf_path: &str) -> Result<(), PolarsError> {
    debug!("Loading auctions from file");
    let adf_file = File::open(raw_adf_path).expect("could not open file");
    let adf = ParquetReader::new(adf_file).finish().unwrap();
    debug!("Loaded auctions");

    debug!("Loading bids from file");
    let bdf_file = File::open(raw_bdf_path).expect("could not open file");
    let bdf = ParquetReader::new(bdf_file).finish().unwrap();
    debug!("Loaded bids");

    let (adf, bdf) = ops::adf_bdf_remove_incomplete_auctions(adf, bdf);

    let adf = ops::adf_sin_cos_start_time(adf);
    let adf = ops::adf_handle_nulls(adf);

    let bdf = ops::adf_bdf_calculate_bid_deltas(&adf, bdf);

    // Calculate distance to user's last bid
    let bdf = ops::bdf_distance_to_prior_bid(bdf);

    let bdf = ops::bdf_mark_final_bid(bdf);

    let bdf = ops::bdf_mark_timestamp_index(bdf);

    debug!("Collecting bdf");
    let bdf = bdf.collect().unwrap();
    debug!("Collected bdf");

    debug!("Collecting user stats");
    let user_stats = ops::bdf_user_historical_stats(&bdf);
    let mut user_stats = user_stats.collect().unwrap();
    debug!("Collected user stats");

    debug!("Writing user info");
    let mut user_file = std::fs::File::create("./data/user_stats.parquet").unwrap();
    ParquetWriter::new(&mut user_file)
        .finish(&mut user_stats)
        .unwrap();
    debug!("Wrote user info");

    let bdf = ops::bdf_user_stats_join(bdf, &user_stats);

    let adf = adf.collect().unwrap();
    let bdf = bdf.collect().unwrap();

    let df = ops::adf_bdf_make_df(adf, bdf);

    debug!("Collecting df");
    let mut df = df.collect().unwrap();
    debug!("Collected df");

    debug!("Writing df");
    let mut df_file = std::fs::File::create("./data/df.parquet").unwrap();
    ParquetWriter::new(&mut df_file).finish(&mut df).unwrap();
    debug!("Wrote df");

    let (train_df, val_df, test_df) = ops::train_test_split(&df);
    let train_df = train_df.collect().unwrap();
    let val_df = val_df.collect().unwrap();
    let test_df = test_df.collect().unwrap();

    debug!("Writing train/test frames");
    write_frames(train_df, "train");
    write_frames(val_df, "val");
    write_frames(test_df, "test");
    debug!("Wrote train/test frames");

    Ok(())
}

fn write_frames(df: DataFrame, id: &str) {
    let tdf = ops::time_series_data(&df);
    let mut tdf = tdf.collect().unwrap();

    let mut tdf_file = std::fs::File::create(format!("./data/split/tdf_{id}.parquet")).unwrap();
    ParquetWriter::new(&mut tdf_file).finish(&mut tdf).unwrap();

    let mdf = ops::meta_data(&df);
    let mut mdf = mdf.collect().unwrap();

    let mut mdf_file = std::fs::File::create(format!("./data/split/mdf_{id}.parquet")).unwrap();
    ParquetWriter::new(&mut mdf_file).finish(&mut mdf).unwrap();

    let y = ops::y_data(&df);
    let mut y = y.collect().unwrap();

    let mut y_file = std::fs::File::create(format!("./data/split/y_{id}.parquet")).unwrap();
    ParquetWriter::new(&mut y_file).finish(&mut y).unwrap();
}
