use bson::{to_bson, Array, Bson, Document};
use serde_json::{json, Value};
use sqlx_core::column::Column;
use sqlx_core::decode::Decode;
use sqlx_core::error::BoxDynError;
use sqlx_core::mssql::{Mssql, MssqlRow, MssqlValue, MssqlValueRef};
use sqlx_core::row::Row;
use sqlx_core::type_info::TypeInfo;
use sqlx_core::types::BigDecimal;
use sqlx_core::value::ValueRef;

use crate::convert::{BsonCodec, RefBsonCodec, ResultCodec};
use crate::new_bson_option_into;

impl<'r> BsonCodec for sqlx_core::mssql::MssqlValueRef<'r> {
    fn try_to_bson(self) -> crate::Result<Bson> {
        //TODO batter way to match type replace use string match
        match self.type_info().name() {
            "NULL" => {
                return Ok(Bson::Null);
            }
            "TINYINT" => {
                let r: Option<i8> = Decode::<'_, Mssql>::decode(self)?;
                let ret = if let Some(i) = r {
                    Bson::Int32(i as i32)
                } else {
                    Bson::Null
                };

                return Ok(ret);
            }
            "SMALLINT" => {
                let r: Option<i16> = Decode::<'_, Mssql>::decode(self)?;
                let ret = if let Some(i) = r {
                    Bson::Int32(i as i32)
                } else {
                    Bson::Null
                };

                return Ok(ret);
            }
            "INT" => {
                let r: Option<i32> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "BIGINT" => {
                let r: Option<i64> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "REAL" => {
                let r: Option<f32> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "FLOAT" => {
                let r: Option<f64> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }

            "VARCHAR" | "NVARCHAR" | "BIGVARCHAR" | "CHAR" | "BIGCHAR" | "NCHAR" => {
                let r: Option<String> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }

            //TODO  "DATE" | "TIME" | "DATETIME" | "TIMESTAMP" => {
            //     let r: Option<String> = Decode::<'_, Mssql>::decode(self)?;
            //     return Ok(json!(r));
            // }

            //TODO "NEWDECIMAL" => {
            //     let r: Option<String> = Decode::<'_, Mssql>::decode(self)?;
            //     return Ok(json!(r));
            // }

            // you can use already supported types to decode this
            _ => {
                return Err(crate::Error::from(format!(
                    "un support database type for:{:?}!",
                    self.type_info().name()
                )));
            }
        }
    }
}

impl RefBsonCodec for Vec<MssqlRow> {
    fn try_to_bson(&self) -> crate::Result<Array> {
        let mut arr = Vec::with_capacity(self.len());
        for row in self {
            let mut d = Document::new();
            let columns = row.columns();
            for x in columns {
                let key = x.name();
                let v: MssqlValueRef = row.try_get_raw(key)?;
                d.insert(key.to_owned(), v.try_to_bson()?);
            }
            arr.push(Bson::Document(d));
        }
        Ok(arr)
    }
}
