use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Bid::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Bid::Timestamp).timestamp().not_null())
                    .col(ColumnDef::new(Bid::Username).string().not_null())
                    .col(ColumnDef::new(Bid::Price).decimal().not_null())
                    .col(ColumnDef::new(Bid::AuctionId).integer().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .from(Bid::Table, Bid::AuctionId)
                            .to(Auction::Table, Auction::Id),
                    )
                    .primary_key(Index::create().col(Bid::AuctionId).col(Bid::Price))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Bid::Table).to_owned())
            .await
    }
}

/// Learn more at https://docs.rs/sea-query#iden
#[derive(Iden)]
enum Bid {
    Table,
    Username,
    Price,
    Timestamp,
    AuctionId,
}

#[derive(Iden)]
enum Auction {
    Table,
    Id,
}
