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
    use rbatis_core::db::DriverType;

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
}
