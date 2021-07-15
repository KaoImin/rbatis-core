use rbatis_core::db::DBPool;
use rbatis_core::Error;

#[macro_use]
pub mod bencher;

#[tokio::main]
async fn main() -> Result<(), Error> {
    //Automatic judgment of database type or  postgres://postgres:123456@localhost:5432/postgres
    let pool = DBPool::new("mysql://root:123456@localhost:3306/test").await?;
    let mut conn = pool.acquire().await?;
    let data: (serde_json::Value, usize) = conn
        .fetch("SELECT * FROM biz_activity;")
        .await.unwrap();
    println!("count: {:?}", data);
    return Ok(());
}

#[cfg(test)]
mod test {
    use rbatis_core::convert::StmtConvert;
    use rbatis_core::db::{DriverType, DBPool};
    use rbatis_core::Error;

    #[test]
    pub fn test_convert() {
        let mut s = String::new();
        DriverType::Postgres.stmt_convert(1000, &mut s);
        println!("stmt:{}", s);
    }

    #[test]
    pub fn bench_convert() {
        let mut s = String::with_capacity(200000);
        DriverType::Postgres.stmt_convert(0, &mut s);
        println!("stmt:{}", s);
        bench!(100000,{
            DriverType::Postgres.stmt_convert(1,&mut s);
        });
    }

    #[tokio::test]
    async fn test_tx() -> Result<(), Error> {
        //Automatic judgment of database type or  postgres://postgres:123456@localhost:5432/postgres
        let pool = DBPool::new("mysql://root:123456@localhost:3306/test").await?;
        let mut tx = pool.begin().await?;
        let data = tx
            .exec("UPDATE `biz_activity` SET `name` = 'test2' WHERE (`id` = '222');")
            .await.unwrap();
        println!("count: {:?}", data);
        tx.commit().await.unwrap();
        return Ok(());
    }

    #[tokio::test]
    async fn test_conn_tx() -> Result<(), Error> {
        //Automatic judgment of database type or  postgres://postgres:123456@localhost:5432/postgres
        let pool = DBPool::new("mysql://root:123456@localhost:3306/test").await?;
        let mut conn = pool.acquire().await?;
        let mut tx = conn.begin().await?;
        let data = tx
            .exec("UPDATE `biz_activity` SET `name` = 'test2' WHERE (`id` = '222');")
            .await.unwrap();
        println!("count: {:?}", data);
        tx.commit().await.unwrap();
        return Ok(());
    }

    #[tokio::test]
    async fn test_conn_tx_rollback() -> Result<(), Error> {
        //Automatic judgment of database type or  postgres://postgres:123456@localhost:5432/postgres
        let pool = DBPool::new("mysql://root:123456@localhost:3306/test").await?;
        let mut conn = pool.acquire().await?;
        let mut tx = conn.begin().await?;
        let data = tx
            .exec("UPDATE `biz_activity` SET `name` = 'test2' WHERE (`id` = '222');")
            .await.unwrap();
        println!("count: {:?}", data);
        tx.rollback().await.unwrap();
        return Ok(());
    }
}
