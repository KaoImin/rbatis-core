use bson::{spec::BinarySubtype, to_bson, Array, Bson, Document};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx_core::column::Column;
use sqlx_core::decode::Decode;
use sqlx_core::error::BoxDynError;
use sqlx_core::mysql::{MySql, MySqlRow, MySqlValue, MySqlValueRef};
use sqlx_core::row::Row;
use sqlx_core::type_info::TypeInfo;
use sqlx_core::types::{BigDecimal, Json};
use sqlx_core::value::ValueRef;

use crate::convert::{BsonCodec, RefBsonCodec, ResultCodec};
use crate::new_bson_option_into;

impl<'r> BsonCodec for sqlx_core::mysql::MySqlValueRef<'r> {
    fn try_to_bson(self) -> crate::Result<Bson> {
        match self.type_info().name() {
            "NULL" => {
                return Ok(Bson::Null);
            }
            "DECIMAL" => {
                let r: Option<String> = Decode::<'_, MySql>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "BIGINT UNSIGNED" => {
                let r: Option<u64> = Decode::<'_, MySql>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "BIGINT" => {
                let r: Option<i64> = Decode::<'_, MySql>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "INT UNSIGNED" | "MEDIUMINT UNSIGNED" => {
                let r: Option<u32> = Decode::<'_, MySql>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "INT" | "MEDIUMINT" => {
                let r: Option<i32> = Decode::<'_, MySql>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "SMALLINT" => {
                let r: Option<i16> = Decode::<'_, MySql>::decode(self)?;
                let ret = if let Some(i) = r {
                    Bson::Int32(i as i32)
                } else {
                    Bson::Null
                };

                return Ok(ret);
            }
            "SMALLINT UNSIGNED" => {
                let r: Option<u16> = Decode::<'_, MySql>::decode(self)?;
                let ret = if let Some(i) = r {
                    Bson::Int32(i as i32)
                } else {
                    Bson::Null
                };

                return Ok(ret);
            }
            "TINYINT UNSIGNED" => {
                let r: Option<u8> = Decode::<'_, MySql>::decode(self)?;
                let ret = if let Some(i) = r {
                    Bson::Int32(i as i32)
                } else {
                    Bson::Null
                };

                return Ok(ret);
            }
            "TINYINT" => {
                let r: Option<i8> = Decode::<'_, MySql>::decode(self)?;
                let ret = if let Some(i) = r {
                    Bson::Int32(i as i32)
                } else {
                    Bson::Null
                };

                return Ok(ret);
            }
            "FLOAT" => {
                let r: Option<f32> = Decode::<'_, MySql>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "DOUBLE" => {
                let r: Option<f64> = Decode::<'_, MySql>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "BINARY" | "VARBINARY" | "CHAR" | "VARCHAR" | "TEXT" | "ENUM" => {
                let r: Option<String> = Decode::<'_, MySql>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "TINYTEXT" | "MEDIUMTEXT"
            | "LONGTEXT" => {
                let r: Option<Vec<u8>> = Decode::<'_, MySql>::decode(self)?;
                return Ok(new_bson_option_into!(r, BinarySubtype::Generic));
            }
            "BIT" | "BOOLEAN" => {
                let r: Option<u8> = Decode::<'_, MySql>::decode(self)?;
                let ret = if let Some(i) = r {
                    Bson::Boolean(i == 1)
                } else {
                    Bson::Null
                };

                return Ok(ret);
            }

            "DATE" => {
                let r: Option<chrono::NaiveDate> = Decode::<'_, MySql>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "TIME" | "YEAR" => {
                let r: Option<chrono::NaiveTime> = Decode::<'_, MySql>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "DATETIME" => {
                let r: Option<chrono::NaiveDateTime> = Decode::<'_, MySql>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "TIMESTAMP" => {
                let r: Option<chrono::NaiveDateTime> = Decode::<'_, MySql>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "JSON" => {
                let r: Option<Json<serde_json::Value>> = Decode::<'_, MySql>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            _ => {
                //TODO "GEOMETRY" support. for now you can use already supported types to decode this
                let r: Option<Vec<u8>> = Decode::<'_, MySql>::decode(self)?;
                return Ok(new_bson_option_into!(r, BinarySubtype::Generic));
            }
        }
    }
}

impl RefBsonCodec for Vec<MySqlRow> {
    fn try_to_bson(&self) -> crate::Result<Array> {
        let mut arr = Vec::with_capacity(self.len());
        for row in self {
            let mut d = Document::new();
            let columns = row.columns();
            for x in columns {
                let key = x.name();
                let v: MySqlValueRef = row.try_get_raw(key)?;
                d.insert(key.to_owned(), v.try_to_bson()?);
            }
            arr.push(Bson::Document(d));
        }
        Ok(arr)
    }
}
