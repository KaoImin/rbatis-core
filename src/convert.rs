use serde_json::Value;

use crate::db::DriverType;
use crate::Result;
use std::fmt::Write;

///the stmt replace str convert
pub trait StmtConvert {
    fn stmt_convert(&self, index: usize, item: &mut String);
}

impl StmtConvert for DriverType {
    fn stmt_convert(&self, index: usize, item: &mut String) {
        match &self {
            DriverType::Postgres => {
                item.push('$');
                item.write_fmt(format_args!("{}", index))
                    .expect("a Display implementation returned an error unexpectedly");
            }
            DriverType::Mysql => {
                item.push('?');
            }
            DriverType::Sqlite => {
                item.push('?');
            }
            DriverType::Mssql => {
                item.push('@');
                item.push('p');
                item.write_fmt(format_args!("{}", (index + 1)))
                    .expect("a Display implementation returned an error unexpectedly");
            }
            DriverType::None => {
                panic!("[rbatis] un support none for driver type!")
            }
        }
    }
}

///json convert
pub trait JsonCodec {
    /// to an json value
    fn try_to_json(self) -> Result<Value>;
}

///json convert
pub trait RefJsonCodec {
    /// to an json value
    fn try_to_json(&self) -> Result<Value>;
}

///result convert
pub trait ResultCodec<T> {
    fn into_result(self) -> Result<T>;
}
