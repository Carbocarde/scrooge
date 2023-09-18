use std::f64::consts::PI;

use polars::lazy::dsl::col;
use polars::prelude::*;
use tracing::debug;

pub fn adf_handle_nulls(adf: LazyFrame) -> LazyFrame {
    adf.with_columns([
        col("is_bindolence").fill_null(false).alias("is_bindolence"),
        col("percent_off").fill_null(0).alias("percent_off"),
    ])
}

pub fn adf_sin_cos_start_time(adf: LazyFrame) -> LazyFrame {
    adf.with_columns([
        col("start_time").dt().hour().alias("start_hour"),
        col("start_time").dt().minute().alias("start_minute"),
    ])
    .with_columns([
        col("start_hour")
            .cast(DataType::Float64)
            .map(
                |x| Ok(Some(x / 23. * PI * 2.)),
                GetOutput::from_type(DataType::Float64),
            )
            .sin()
            .alias("start_hour_sin"),
        col("start_hour")
            .cast(DataType::Float64)
            .map(
                |x| Ok(Some(x / 23. * PI * 2.)),
                GetOutput::from_type(DataType::Float64),
            )
            .cos()
            .alias("start_hour_cos"),
        col("start_minute")
            .cast(DataType::Float64)
            .map(
                |x| Ok(Some(x / 23. * PI * 2.)),
                GetOutput::from_type(DataType::Float64),
            )
            .sin()
            .alias("start_minute_sin"),
        col("start_minute")
            .cast(DataType::Float64)
            .map(
                |x| Ok(Some(x / 23. * PI * 2.)),
                GetOutput::from_type(DataType::Float64),
            )
            .cos()
            .alias("start_minute_cos"),
    ])
    .drop_columns(["start_hour", "start_minute"])
}

pub fn adf_bdf_remove_incomplete_auctions(
    adf: DataFrame,
    bdf: DataFrame,
) -> (LazyFrame, LazyFrame) {
    let filtered_auctions = bdf
        .clone()
        .lazy()
        .groupby([col("auction_id")])
        .agg([
            col("price").count().alias("entries"),
            col("price").max().alias("max_price"),
        ])
        .join(
            adf.clone().lazy(),
            [col("auction_id")],
            [col("auction_id")],
            JoinArgs::new(JoinType::Inner),
        )
        .filter(
            col("entries")
                .eq(col("max_price"))
                .and(col("end_time").is_not_null()),
        )
        .select([col("auction_id")])
        .collect()
        .unwrap();

    let adf = adf
        .lazy()
        .inner_join(filtered_auctions.clone().lazy(), "auction_id", "auction_id");

    let bdf = bdf
        .lazy()
        .inner_join(filtered_auctions.lazy(), "auction_id", "auction_id");

    (adf, bdf)
}

pub fn adf_bdf_calculate_bid_deltas(adf: &LazyFrame, bdf: LazyFrame) -> LazyFrame {
    bdf.inner_join(
        adf.clone().select([col("auction_id"), col("start_time")]),
        "auction_id",
        "auction_id",
    )
    .sort("price", Default::default())
    .with_column(
        col("timestamp")
            .shift(1)
            .over(["auction_id"])
            .alias("prior_bid_timestamp"),
    )
    .with_column(
        when(col("price").eq(1))
            .then((col("start_time") - col("timestamp")) / lit(1000000))
            .otherwise((col("timestamp") - col("prior_bid_timestamp")) / lit(1000000))
            .cast(DataType::Int64)
            .alias("delta"),
    )
    .drop_columns(["prior_bid_timestamp", "start_time"])
}

pub fn bdf_distance_to_prior_bid(bdf: LazyFrame) -> LazyFrame {
    bdf.sort("price", Default::default()).with_column(
        ((col("price") - col("price").shift(1))
            .cast(DataType::Int64)
            .fill_null(-1))
        .over(["auction_id", "username"])
        .alias("prior_bid_dist"),
    )
}

pub fn bdf_mark_final_bid(bdf: LazyFrame) -> LazyFrame {
    bdf.with_column(
        col("price")
            .eq(col("price").max())
            .over(["auction_id"])
            .alias("final_bid"),
    )
}

pub fn bdf_mark_timestamp_index(bdf: LazyFrame) -> LazyFrame {
    bdf.sort("timestamp", Default::default())
        .with_row_count("idx", None)
}

pub fn bdf_user_historical_stats(bdf: &DataFrame) -> LazyFrame {
    bdf.clone()
        .lazy()
        .groupby_dynamic(
            col("timestamp"),
            [col("username")],
            DynamicGroupOptions {
                every: Duration::parse("30m"),
                period: Duration::parse("4d"),
                offset: Duration::parse("30m"),
                ..Default::default()
            },
        )
        .agg([
            min("idx").alias("idx"),
            avg("delta").alias("avg_delta"),
            col("delta").std(1).alias("std_delta"),
            count().alias("total_spend"),
            avg("price").alias("avg_bid_price"),
            col("price").std(1).alias("std_bid_price"),
            col("final_bid")
                .filter(col("final_bid"))
                .count()
                .alias("wins"),
        ])
        .drop_columns(["timestamp"])
        .unique(None, UniqueKeepStrategy::First)
        .sort("username", Default::default())
}

pub fn bdf_user_stats_join(bdf: DataFrame, user_stats: &DataFrame) -> LazyFrame {
    bdf.lazy()
        .join(
            user_stats.clone().lazy(),
            [col("idx")],
            [col("idx")],
            JoinArgs::new(JoinType::AsOf(AsOfOptions {
                left_by: Some(vec!["username".into()]),
                right_by: Some(vec!["username".into()]),
                ..Default::default()
            })),
        )
        .drop_columns(["idx"])
}

pub fn adf_bdf_make_df(adf: DataFrame, bdf: DataFrame) -> LazyFrame {
    bdf.lazy()
        .inner_join(adf.lazy(), "auction_id", "auction_id")
}

pub fn time_series_data(df: &DataFrame) -> LazyFrame {
    df.clone()
        .lazy()
        .select([
            col("auction_id"),
            col("prior_bid_dist").alias("prior_bid_dist_0"),
            col("delta").alias("delta_0"),
            col("avg_delta").alias("avg_delta_0"),
            col("std_delta").alias("std_delta_0"),
            col("total_spend").alias("total_spend_0"),
            col("avg_bid_price").alias("avg_bid_price_0"),
            col("std_bid_price").alias("std_bid_price_0"),
            col("wins").alias("wins_0"),
        ])
        .with_columns(
            (1..9)
                .flat_map(|index| {
                    [
                        col("prior_bid_dist_0")
                            .shift(index)
                            .over(["auction_id"])
                            .alias(format!("prior_bid_dist_{index}").as_str()),
                        col("delta_0")
                            .shift(index)
                            .over(["auction_id"])
                            .alias(format!("delta_{index}").as_str()),
                        col("avg_delta_0")
                            .shift(index)
                            .over(["auction_id"])
                            .alias(format!("avg_delta_{index}").as_str()),
                        col("std_delta_0")
                            .shift(index)
                            .over(["auction_id"])
                            .alias(format!("std_delta_{index}").as_str()),
                        col("total_spend_0")
                            .shift(index)
                            .over(["auction_id"])
                            .alias(format!("total_spend_{index}").as_str()),
                        col("avg_bid_price_0")
                            .shift(index)
                            .over(["auction_id"])
                            .alias(format!("avg_bid_price_{index}").as_str()),
                        col("std_bid_price_0")
                            .shift(index)
                            .over(["auction_id"])
                            .alias(format!("std_bid_price_{index}").as_str()),
                        col("wins_0")
                            .shift(index)
                            .over(["auction_id"])
                            .alias(format!("wins_{index}").as_str()),
                    ]
                })
                .collect::<Vec<Expr>>(),
        )
        .drop_columns(["auction_id"])
}

pub fn meta_data(df: &DataFrame) -> LazyFrame {
    df.clone().lazy().select([
        col("price"),
        col("bin_price"),
        col("no_jumper_limit"),
        col("exchangeable"),
        col("one_per_user"),
        col("no_re_entry"),
        col("is_bindolence"),
        col("percent_off"),
        col("start_hour_sin"),
        col("start_hour_cos"),
        col("start_minute_sin"),
        col("start_minute_cos"),
    ])
}

pub fn y_data(df: &DataFrame) -> LazyFrame {
    df.clone().lazy().select([col("final_bid")])
}

pub fn train_test_split(df: &DataFrame) -> (LazyFrame, LazyFrame, LazyFrame) {
    let valid_aucs = df
        .clone()
        .lazy()
        .select([col("auction_id"), col("start_time")])
        // Remove historical starting buffer (first 4 days 1 hour)
        .filter(
            (col("start_time") - col("start_time").min()).gt(lit(chrono::Duration::days(4)
                .checked_add(&chrono::Duration::hours(1))
                .unwrap())),
        )
        .unique(None, UniqueKeepStrategy::First)
        .sort("start_time", Default::default())
        .with_row_count("idx", None)
        .collect()
        .unwrap();

    let (row_count, _) = valid_aucs.shape();
    let row_count = row_count as u64;

    let train_val_split_idx = row_count * 6 / 10;
    let val_test_split_idx = row_count * 8 / 10;

    debug!(
        "Split sizes: (train {}, val {}, test {})",
        train_val_split_idx,
        val_test_split_idx - train_val_split_idx,
        row_count - val_test_split_idx
    );

    let train_aucs = valid_aucs
        .clone()
        .lazy()
        .filter(col("idx").lt_eq(lit(train_val_split_idx)))
        .select([col("auction_id")]);

    let val_aucs = valid_aucs
        .clone()
        .lazy()
        .filter(
            col("idx")
                .gt(lit(train_val_split_idx))
                .and(col("idx").lt_eq(lit(val_test_split_idx))),
        )
        .select([col("auction_id")]);

    let test_aucs = valid_aucs
        .lazy()
        .filter(col("idx").gt(lit(val_test_split_idx)))
        .select([col("auction_id")]);

    let train = df
        .clone()
        .lazy()
        .inner_join(train_aucs, col("auction_id"), col("auction_id"));
    let val = df
        .clone()
        .lazy()
        .inner_join(val_aucs, col("auction_id"), col("auction_id"));
    let test = df
        .clone()
        .lazy()
        .inner_join(test_aucs, col("auction_id"), col("auction_id"));

    (train, val, test)
}
