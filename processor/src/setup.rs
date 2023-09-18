use sea_orm::*;

pub(super) async fn set_up_db(db_url: &str, db_name: &str) -> Result<DatabaseConnection, DbErr> {
    // Attempt to directly connect to the database
    let db_table = Database::connect(format!("{}/{}", db_url, db_name)).await;
    if db_table.is_ok() {
        return db_table;
    }

    // Directly connecting failed, construct the database then connect to it
    let db_root = Database::connect(format!("{}/postgres", db_url)).await?;

    let db = match db_root.get_database_backend() {
        DbBackend::MySql => {
            db_root
                .execute(Statement::from_string(
                    db_root.get_database_backend(),
                    format!("CREATE DATABASE IF NOT EXISTS `{}`;", db_name),
                ))
                .await?;

            let url = format!("{}/{}", db_url, db_name);
            Database::connect(&url).await?
        }
        DbBackend::Postgres => {
            db_root
                .execute(Statement::from_string(
                    db_root.get_database_backend(),
                    format!("DROP DATABASE IF EXISTS \"{}\";", db_name),
                ))
                .await?;
            db_root
                .execute(Statement::from_string(
                    db_root.get_database_backend(),
                    format!("CREATE DATABASE \"{}\";", db_name),
                ))
                .await?;

            let url = format!("{}/{}", db_url, db_name);
            Database::connect(&url).await?
        }
        DbBackend::Sqlite => db_root,
    };

    Ok(db)
}
