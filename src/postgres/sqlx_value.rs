use serde_json::{json, Value};
use sqlx_core::column::Column;
use sqlx_core::decode::Decode;
use sqlx_core::error::BoxDynError;
use sqlx_core::postgres::types::{PgMoney, PgTimeTz};
use sqlx_core::postgres::PgRow;
use sqlx_core::postgres::{PgValue, PgValueRef, Postgres};
use sqlx_core::row::Row;
use sqlx_core::type_info::TypeInfo;
use sqlx_core::types::chrono::{FixedOffset, NaiveTime};
use sqlx_core::types::ipnetwork::IpNetwork;
use sqlx_core::types::time::Time;
use sqlx_core::types::{BigDecimal, Json, Uuid};
use sqlx_core::value::ValueRef;

use crate::convert::{JsonCodec, RefJsonCodec, ResultCodec};
use crate::postgres::PgInterval;
use chrono::Utc;

use crate::{new_json_option_into};

impl<'c> JsonCodec for PgValueRef<'c> {
    fn try_to_json(self) -> crate::Result<serde_json::Value> {
        match self.type_info().name() {
            "VOID" => {
                return Ok(serde_json::Value::Null);
            }
            "NUMERIC" => {
                //decimal
                let r: Option<String> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "NUMERIC[]" => {
                //decimal
                let r: Option<Vec<String>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "MONEY" => {
                //decimal
                let r: Option<String> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "MONEY[]" => {
                //decimal
                let r: Option<Vec<String>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "BOOL" => {
                let r: Option<bool> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "BOOL[]" => {
                let r: Vec<bool> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(r.into());
            }
            "BYTEA" | "BYTEA[]" => {
                let r: Option<Vec<u8>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "FLOAT4" => {
                let r: Option<f32> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "FLOAT4[]" => {
                let r: Option<Vec<f32>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "FLOAT8" => {
                let r: Option<f64> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "FLOAT8[]" => {
                let r: Option<Vec<f64>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "INT2" => {
                let r: Option<i16> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "INT2[]" => {
                let r: Option<Vec<i16>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "INT4" => {
                let r: Option<i32> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "INT4[]" => {
                let r: Option<Vec<i32>> = Decode::<'_, Postgres>::decode(self)?;

                return Ok(new_json_option_into!(r));
            }
            "INT8" => {
                let r: Option<i64> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "INT8[]" => {
                let r: Option<Vec<i64>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "OID" => {
                let r: Option<u32> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "OID[]" => {
                let r: Option<Vec<u32>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "TEXT" | "NAME" | "VARCHAR" | "BPCHAR" | "CHAR" | "\"CHAR\"" | "UNKNOWN" => {
                let r: Option<String> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "TEXT[]" | "CHAR[]" | "VARCHAR[]" | "\"CHAR\"[]" | "NAME[]" => {
                let r: Option<Vec<String>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
            "UUID" => {
                let r: Option<Uuid> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "UUID[]" => {
                let r: Option<Vec<Uuid>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "JSON" | "JSONB" => {
                let r: Option<Json<serde_json::Value>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "JSON[]" | "JSONB[]" => {
                let r: Option<Vec<Json<serde_json::Value>>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "TIME" => {
                let r: Option<Time> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "TIME[]" => {
                let r: Option<Vec<Time>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "DATE" => {
                let r: Option<chrono::NaiveDate> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "DATE[]" => {
                let r: Option<Vec<chrono::NaiveDate>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "TIMESTAMP" => {
                let r: Option<chrono::NaiveDateTime> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "TIMESTAMP[]" => {
                let r: Option<Vec<chrono::NaiveDateTime>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "TIMESTAMPTZ" => {
                let r: Option<chrono::DateTime<Utc>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "TIMESTAMPTZ[]" => {
                let r: Option<Vec<chrono::DateTime<Utc>>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "CIDR" | "INET" => {
                let r: Option<IpNetwork> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "CIDR[]" | "INET[]" => {
                let r: Option<Vec<IpNetwork>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }

            "INTERVAL" => {
                let r: Option<sqlx_core::postgres::types::PgInterval> =
                    Decode::<'_, Postgres>::decode(self)?;
                if r.is_none() {
                    return Ok(serde_json::Value::Null);
                }
                return Ok(json!(PgInterval::from(r.unwrap())));
            }
            "VARBIT" | "BIT" => {
                let r: Option<bit_vec::BitVec> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            "VARBIT[]" | "BIT[]" => {
                let r: Option<Vec<bit_vec::BitVec>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(json!(r));
            }
            _ => {
                //TODO
                // "JSONPATH","JSONPATH[]",
                // "INT8RANGE","INT8RANGE[]",
                // "DATERANGE","DATERANGE[]",
                // "TSTZRANGE","TSTZRANGE[]",
                // "TSRANGE","TSRANGE[]",
                // "NUMRANGE","NUMRANGE[]",
                // "INT4RANGE","INT4RANGE[]",
                // "RECORD","RECORD[]"
                // "TIMETZ" "TIMETZ[]"
                // "INTERVAL[]"
                // "POINT","POINT[],"
                // LSEG","LSEG[]",
                // "PATH","PATH[]",
                // "BOX","BOX[]",
                // "POLYGON","POLYGON[]",
                // "LINE","LINE[]",
                // "CIRCLE", "CIRCLE[]",
                // "MACADDR8","MACADDR8[]",
                // "MACADDR","MACADDR[]",
                //  you can use already Vec<u8> types to decode this
                let r: Option<Vec<u8>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_json_option_into!(r));
            }
        }
    }
}

impl RefJsonCodec for Vec<PgRow> {
    fn try_to_json(&self) -> crate::Result<serde_json::Value> {
        let mut arr = Vec::with_capacity(self.len());
        for row in self {
            let mut m = serde_json::Map::new();
            let columns = row.columns();
            for x in columns {
                let key = x.name();
                let v: PgValueRef = row.try_get_raw(key)?;
                m.insert(key.to_owned(), v.try_to_json()?);
            }
            arr.push(serde_json::Value::Object(m));
        }
        Ok(serde_json::Value::from(arr))
    }
}
