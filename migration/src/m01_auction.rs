use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Auction::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Auction::Id)
                            .integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Auction::CategoryId).not_null().integer())
                    .col(ColumnDef::new(Auction::CategoryName).not_null().string())
                    .col(ColumnDef::new(Auction::Name).not_null().string())
                    .col(ColumnDef::new(Auction::BuyItNowPrice).not_null().decimal())
                    .col(
                        ColumnDef::new(Auction::BuyItNowOldPrice)
                            .not_null()
                            .decimal(),
                    )
                    .col(ColumnDef::new(Auction::ProductId).not_null().integer())
                    .col(ColumnDef::new(Auction::OnePerUser).not_null().boolean())
                    .col(
                        ColumnDef::new(Auction::IsDummyAuctionForSearch)
                            .not_null()
                            .boolean(),
                    )
                    .col(ColumnDef::new(Auction::NoReEntry).not_null().boolean())
                    .col(ColumnDef::new(Auction::NoJumperLimit).not_null().decimal())
                    .col(ColumnDef::new(Auction::Exchangeable).not_null().boolean())
                    .col(ColumnDef::new(Auction::StartTime).timestamp().not_null())
                    .col(ColumnDef::new(Auction::EstimatedTotalCost).decimal())
                    .col(ColumnDef::new(Auction::BidsPlacedByWinner).string())
                    .col(ColumnDef::new(Auction::PriceAfterPromoDiscount).decimal())
                    .col(ColumnDef::new(Auction::PercentOffPromo).string())
                    .col(ColumnDef::new(Auction::PercentOff).integer())
                    .col(ColumnDef::new(Auction::IsBindolence).boolean())
                    .col(ColumnDef::new(Auction::PennyStartedRecording).boolean())
                    .col(ColumnDef::new(Auction::PennyRecordedFull).boolean())
                    .col(ColumnDef::new(Auction::AuctionEnd).timestamp())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Auction::Table).to_owned())
            .await
    }
}

/// Learn more at https://docs.rs/sea-query#iden
#[derive(Iden)]
enum Auction {
    Table,
    Id,
    CategoryId,
    CategoryName,
    Name,
    BuyItNowPrice,
    BuyItNowOldPrice,
    OnePerUser,
    ProductId,
    IsDummyAuctionForSearch,
    NoReEntry,
    NoJumperLimit,
    Exchangeable,
    StartTime,
    EstimatedTotalCost,
    BidsPlacedByWinner,
    PriceAfterPromoDiscount,
    PercentOffPromo,
    PercentOff,
    IsBindolence,
    PennyStartedRecording,
    PennyRecordedFull,
    AuctionEnd,
}
