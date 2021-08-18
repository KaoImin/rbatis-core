use bson::{spec::BinarySubtype, to_bson, Array, Bson, Document};
use chrono::Utc;
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

use crate::convert::{BsonCodec, RefBsonCodec, ResultCodec};
use crate::new_bson_option_into;
use crate::postgres::PgInterval;

impl<'c> BsonCodec for PgValueRef<'c> {
    fn try_to_bson(self) -> crate::Result<Bson> {
        match self.type_info().name() {
            "VOID" => {
                return Ok(Bson::Null);
            }
            "NUMERIC" => {
                //decimal
                let r: Option<String> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "NUMERIC[]" => {
                //decimal
                let r: Option<Vec<String>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "MONEY" => {
                //decimal
                let r: Option<String> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "MONEY[]" => {
                //decimal
                let r: Option<Vec<String>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "BOOL" => {
                let r: Option<bool> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "BOOL[]" => {
                let r: Vec<bool> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(r.into());
            }
            "BYTEA" | "BYTEA[]" => {
                let r: Option<Vec<u8>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r, BinarySubtype::Generic));
            }
            "FLOAT4" => {
                let r: Option<f32> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "FLOAT4[]" => {
                let r: Option<Vec<f32>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "FLOAT8" => {
                let r: Option<f64> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "FLOAT8[]" => {
                let r: Option<Vec<f64>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "INT2" => {
                let r: Option<i16> = Decode::<'_, Postgres>::decode(self)?;
                let ret = if let Some(i) = r {
                    Bson::Int32(i as i32)
                } else {
                    Bson::Null
                };

                return Ok(ret);
            }
            "INT2[]" => {
                let r: Option<Vec<i16>> = Decode::<'_, Postgres>::decode(self)?;
                let ret = if let Some(array) = r {
                    let a = array.into_iter().map(|i| i as i32).collect::<Vec<_>>();
                    a.into()
                } else {
                    Bson::Null
                };

                return Ok(ret);
            }
            "INT4" => {
                let r: Option<i32> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "INT4[]" => {
                let r: Option<Vec<i32>> = Decode::<'_, Postgres>::decode(self)?;

                return Ok(new_bson_option_into!(r));
            }
            "INT8" => {
                let r: Option<i64> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "INT8[]" => {
                let r: Option<Vec<i64>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "OID" => {
                let r: Option<u32> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "OID[]" => {
                let r: Option<Vec<u32>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "TEXT" | "NAME" | "VARCHAR" | "BPCHAR" | "CHAR" | "\"CHAR\"" | "UNKNOWN" => {
                let r: Option<String> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "TEXT[]" | "CHAR[]" | "VARCHAR[]" | "\"CHAR\"[]" | "NAME[]" => {
                let r: Option<Vec<String>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(new_bson_option_into!(r));
            }
            "UUID" => {
                let r: Option<Uuid> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "UUID[]" => {
                let r: Option<Vec<Uuid>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "JSON" | "JSONB" => {
                let r: Option<Json<serde_json::Value>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "JSON[]" | "JSONB[]" => {
                let r: Option<Vec<Json<serde_json::Value>>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "TIME" => {
                let r: Option<Time> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "TIME[]" => {
                let r: Option<Vec<Time>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "DATE" => {
                let r: Option<chrono::NaiveDate> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "DATE[]" => {
                let r: Option<Vec<chrono::NaiveDate>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "TIMESTAMP" => {
                let r: Option<chrono::NaiveDateTime> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "TIMESTAMP[]" => {
                let r: Option<Vec<chrono::NaiveDateTime>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "TIMESTAMPTZ" => {
                let r: Option<chrono::DateTime<Utc>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "TIMESTAMPTZ[]" => {
                let r: Option<Vec<chrono::DateTime<Utc>>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "CIDR" | "INET" => {
                let r: Option<IpNetwork> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "CIDR[]" | "INET[]" => {
                let r: Option<Vec<IpNetwork>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }

            "INTERVAL" => {
                let r: Option<sqlx_core::postgres::types::PgInterval> =
                    Decode::<'_, Postgres>::decode(self)?;
                if r.is_none() {
                    return Ok(Bson::Null);
                }
                return Ok(to_bson(&PgInterval::from(r.unwrap())).unwrap());
            }
            "VARBIT" | "BIT" => {
                let r: Option<bit_vec::BitVec> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
            }
            "VARBIT[]" | "BIT[]" => {
                let r: Option<Vec<bit_vec::BitVec>> = Decode::<'_, Postgres>::decode(self)?;
                return Ok(to_bson(&r).unwrap());
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
                return Ok(new_bson_option_into!(r, BinarySubtype::Generic));
            }
        }
    }
}

impl RefBsonCodec for Vec<PgRow> {
    fn try_to_bson(&self) -> crate::Result<Array> {
        let mut arr = Vec::with_capacity(self.len());
        for row in self {
            let mut d = Document::new();
            let columns = row.columns();
            for x in columns {
                let key = x.name();
                let v: PgValueRef = row.try_get_raw(key)?;
                d.insert(key.to_owned(), v.try_to_bson()?);
            }
            arr.push(Bson::Document(d));
        }
        Ok(arr)
    }
}
