pub use sea_orm_migration::prelude::*;

mod m01_auction;
mod m02_bid;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m01_auction::Migration),
            Box::new(m02_bid::Migration),
        ]
    }
}
