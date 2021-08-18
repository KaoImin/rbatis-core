use bson::{spec::BinarySubtype, to_bson, Array, Bson, Document};
use serde_json::{json, Value};
use sqlx_core::column::Column;
use sqlx_core::decode::Decode;
use sqlx_core::error::BoxDynError;
use sqlx_core::row::Row;
use sqlx_core::sqlite::SqliteRow;
use sqlx_core::sqlite::{Sqlite, SqliteValue, SqliteValueRef};
use sqlx_core::type_info::TypeInfo;
use sqlx_core::value::ValueRef;

use crate::convert::{BsonCodec, RefBsonCodec, ResultCodec};
use crate::new_bson_option_into;

impl<'c> BsonCodec for SqliteValueRef<'c> {
    fn try_to_bson(self) -> crate::Result<Bson> {
        let type_string = self.type_info().name().to_owned();
        return match type_string.as_str() {
            "NULL" => Ok(Bson::Null),
            "TEXT" => {
                let r: Option<String> = Decode::<'_, Sqlite>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "BOOLEAN" => {
                let r: Option<bool> = Decode::<'_, Sqlite>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "INTEGER" => {
                let r: Option<i64> = Decode::<'_, Sqlite>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "REAL" => {
                let r: Option<f64> = Decode::<'_, Sqlite>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "BLOB" => {
                let r: Option<Vec<u8>> = Decode::<'_, Sqlite>::decode(self)?;
                return Ok(new_bson_option_into!(r, BinarySubtype::Generic));
            }
            _ => {
                //TODO "NUMERIC" |"DATE" | "TIME" | "DATETIME"
                //you can use already supported types to decode this
                let r: Option<Vec<u8>> = Decode::<'_, Sqlite>::decode(self)?;
                return Ok(new_bson_option_into!(r, BinarySubtype::Generic));
            }
        };
    }
}

impl RefBsonCodec for Vec<SqliteRow> {
    fn try_to_bson(&self) -> crate::Result<Array> {
        let mut arr = Vec::with_capacity(self.len());
        for row in self {
            let mut d = Document::new();
            let columns = row.columns();
            for x in columns {
                let key = x.name();
                let v: SqliteValueRef = row.try_get_raw(key)?;
                d.insert(key.to_owned(), v.try_to_bson()?);
            }
            arr.push(Bson::Document(d));
        }
        Ok(arr)
    }
}
