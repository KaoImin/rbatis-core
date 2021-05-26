use serde_json::{json, Value};
use sqlx_core::column::Column;
use sqlx_core::decode::Decode;
use sqlx_core::error::BoxDynError;
use sqlx_core::mssql::{Mssql, MssqlRow, MssqlValue, MssqlValueRef};
use sqlx_core::row::Row;
use sqlx_core::type_info::TypeInfo;
use sqlx_core::types::BigDecimal;
use sqlx_core::value::ValueRef;

use crate::convert::{JsonCodec, RefJsonCodec, ResultCodec};

impl<'r> JsonCodec for sqlx_core::mssql::MssqlValueRef<'r> {
    fn try_to_json(self) -> crate::Result<serde_json::Value> {
        //TODO batter way to match type replace use string match
        match self.type_info().name() {
            "NULL" => {
                return Ok(serde_json::Value::Null);
            }
            "TINYINT" => {
                let r: Option<i8> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(json!(r));
            }
            "SMALLINT" => {
                let r: Option<i16> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(json!(r));
            }
            "INT" => {
                let r: Option<i32> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(json!(r));
            }
            "BIGINT" => {
                let r: Option<i64> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(json!(r));
            }
            "REAL" => {
                let r: Option<f32> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(json!(r));
            }
            "FLOAT" => {
                let r: Option<f64> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(json!(r));
            }

            "VARCHAR" | "NVARCHAR" | "BIGVARCHAR" | "CHAR" | "BIGCHAR" | "NCHAR" => {
                let r: Option<String> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(json!(r));
            }

            "DATE" | "TIME" | "DATETIME" | "TIMESTAMP" => {
                let r: Option<String> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(json!(r));
            }

            "NEWDECIMAL" => {
                let r: Option<String> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(json!(r));
            }
            // you can use already supported types to decode this
            _ => {
                let r: Option<String> = Decode::<'_, Mssql>::decode(self)?;
                return Ok(json!(r));
            }
        }
    }
}

impl RefJsonCodec for Vec<MssqlRow> {
    fn try_to_json(&self) -> crate::Result<serde_json::Value> {
        let mut arr = Vec::with_capacity(self.len());
        for row in self {
            let mut m = serde_json::Map::new();
            let columns = row.columns();
            for x in columns {
                let key = x.name();
                let v: MssqlValueRef = row.try_get_raw(key)?;
                m.insert(key.to_owned(), v.try_to_json()?);
            }
            arr.push(serde_json::Value::Object(m));
        }
        Ok(serde_json::Value::from(arr))
    }
}
