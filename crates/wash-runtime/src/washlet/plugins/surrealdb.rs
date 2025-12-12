//! # SurrealDB Plugin
//!
//! This module implements the `seamlezz:surrealdb@0.1.0` interface using
//! the SurrealDB Rust SDK as the backend.

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context as _;
use serde_content::{Number, Serializer, Value as Content};
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::{Database, Namespace, Root};
use surrealdb::sql;
use surrealdb::Surreal;
use tokio::sync::RwLock;
use wasmtime::component::HasSelf;

use crate::engine::ctx::Ctx;
use crate::engine::workload::WorkloadComponent;
use crate::plugin::HostPlugin;
use crate::wit::{WitInterface, WitWorld};

const PLUGIN_SURREALDB_ID: &str = "seamlezz-surrealdb";

mod bindings {
    wasmtime::component::bindgen!({
        world: "surrealdb",
        imports: { default: async | trappable },
    });
}

#[derive(Debug, Clone, PartialEq)]
pub enum Auth {
    Root { username: String, password: String },
    Namespace { username: String, password: String },
    Database { username: String, password: String },
}

impl Default for Auth {
    fn default() -> Self {
        Auth::Root {
            username: "root".to_string(),
            password: "root".to_string(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SurrealdbConfig {
    pub url: String,
    pub namespace: String,
    pub database: String,
    pub auth: Auth,
}

#[derive(Clone)]
pub struct WasiSurrealdb {
    db: Arc<RwLock<Surreal<Any>>>,
}

impl WasiSurrealdb {
    pub async fn new(config: SurrealdbConfig) -> anyhow::Result<Self> {
        let db: Surreal<Any> = Surreal::init();

        db.connect(&config.url)
            .await
            .context("failed to connect to SurrealDB")?;

        match &config.auth {
            Auth::Root { username, password } => {
                db.signin(Root { username, password })
                    .await
                    .context("failed to sign in as root")?;
            }
            Auth::Namespace { username, password } => {
                db.signin(Namespace {
                    username,
                    password,
                    namespace: &config.namespace,
                })
                .await
                .context("failed to sign in as namespace user")?;
            }
            Auth::Database { username, password } => {
                db.signin(Database {
                    username,
                    password,
                    namespace: &config.namespace,
                    database: &config.database,
                })
                .await
                .context("failed to sign in as database user")?;
            }
        }

        db.use_ns(&config.namespace)
            .use_db(&config.database)
            .await
            .context("failed to select namespace/database")?;

        tracing::info!(
            url = %config.url,
            namespace = %config.namespace,
            database = %config.database,
            "SurrealDB plugin connected successfully"
        );

        Ok(Self {
            db: Arc::new(RwLock::new(db)),
        })
    }
}

fn into_content(value: sql::Value) -> anyhow::Result<Content<'static>> {
    let serializer = Serializer::new();
    match value {
        sql::Value::None => Ok(Content::Option(None)),
        sql::Value::Null => Ok(Content::Option(None)),
        sql::Value::Bool(v) => Ok(Content::Bool(v)),
        sql::Value::Number(v) => match v {
            sql::Number::Int(v) => Ok(Content::Number(Number::I64(v))),
            sql::Number::Float(v) => Ok(Content::Number(Number::F64(v))),
            sql::Number::Decimal(v) => serializer
                .serialize(v)
                .context("Could not serialize decimal"),
            _ => anyhow::bail!("Could not serialize number"),
        },
        sql::Value::Strand(v) => Ok(Content::String(Cow::Owned(v.0))),
        sql::Value::Duration(v) => serializer
            .serialize(v.0)
            .context("Could not serialize duration"),
        sql::Value::Datetime(v) => serializer
            .serialize(v.0)
            .context("Could not serialize datetime"),
        sql::Value::Uuid(v) => serializer
            .serialize(v.0)
            .context("Could not serialize uuid"),
        sql::Value::Array(v) => {
            let mut vec = Vec::with_capacity(v.0.len());
            for value in v.0 {
                vec.push(into_content(value)?);
            }
            Ok(Content::Seq(vec))
        }
        sql::Value::Object(v) => {
            let mut vec = Vec::with_capacity(v.0.len());
            for (key, value) in v.0 {
                let key = Content::String(Cow::Owned(key));
                let value = into_content(value)?;
                vec.push((key, value));
            }
            Ok(Content::Map(vec))
        }
        sql::Value::Geometry(v) => match v {
            sql::Geometry::Point(v) => serializer.serialize(v).context("Could not serialize point"),
            sql::Geometry::Line(v) => serializer.serialize(v).context("Could not serialize line"),
            sql::Geometry::Polygon(v) => {
                serializer.serialize(v).context("Could not serialize polygon")
            }
            sql::Geometry::MultiPoint(v) => serializer
                .serialize(v)
                .context("Could not serialize multipoint"),
            sql::Geometry::MultiLine(v) => serializer
                .serialize(v)
                .context("Could not serialize multiline"),
            sql::Geometry::MultiPolygon(v) => serializer
                .serialize(v)
                .context("Could not serialize multipolygon"),
            sql::Geometry::Collection(v) => serializer
                .serialize(v)
                .context("Could not serialize geometry collection"),
            _ => anyhow::bail!("Could not serialize geometry"),
        },
        sql::Value::Bytes(v) => Ok(Content::Bytes(Cow::Owned(v.into()))),
        sql::Value::Thing(v) => serializer
            .serialize(v)
            .context("Could not serialize thing"),
        sql::Value::Param(v) => serializer
            .serialize(v.0)
            .context("Could not serialize param"),
        sql::Value::Idiom(v) => serializer
            .serialize(v.0)
            .context("Could not serialize idiom"),
        sql::Value::Table(v) => serializer
            .serialize(v.0)
            .context("Could not serialize table"),
        sql::Value::Mock(v) => serializer
            .serialize(v)
            .context("Could not serialize mock"),
        sql::Value::Regex(v) => serializer
            .serialize(v)
            .context("Could not serialize regex"),
        sql::Value::Cast(v) => serializer
            .serialize(v)
            .context("Could not serialize cast"),
        sql::Value::Block(v) => serializer
            .serialize(v)
            .context("Could not serialize block"),
        sql::Value::Range(v) => serializer
            .serialize(v)
            .context("Could not serialize range"),
        sql::Value::Edges(v) => serializer
            .serialize(v)
            .context("Could not serialize edges"),
        sql::Value::Future(v) => serializer
            .serialize(v)
            .context("Could not serialize future"),
        sql::Value::Constant(v) => serializer
            .serialize(v)
            .context("Could not serialize constant"),
        sql::Value::Function(v) => serializer
            .serialize(v)
            .context("Could not serialize function"),
        sql::Value::Subquery(v) => serializer
            .serialize(v)
            .context("Could not serialize subquery"),
        sql::Value::Expression(v) => serializer
            .serialize(v)
            .context("Could not serialize expression"),
        sql::Value::Query(v) => serializer
            .serialize(v)
            .context("Could not serialize query"),
        sql::Value::Model(v) => serializer
            .serialize(v)
            .context("Could not serialize model"),
        sql::Value::Closure(v) => serializer
            .serialize(v)
            .context("Could not serialize closure"),
        sql::Value::Refs(_) => Ok(Content::Seq(vec![])),
        _ => anyhow::bail!("Could not serialize value"),
    }
}

fn cbor_to_json(cbor: serde_cbor::Value) -> serde_json::Value {
    match cbor {
        serde_cbor::Value::Null => serde_json::Value::Null,
        serde_cbor::Value::Bool(b) => serde_json::Value::Bool(b),
        serde_cbor::Value::Integer(i) => {
            serde_json::Value::Number(serde_json::Number::from(i as i64))
        }
        serde_cbor::Value::Float(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        serde_cbor::Value::Bytes(b) => {
            use base64::Engine;
            serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        serde_cbor::Value::Text(s) => serde_json::Value::String(s),
        serde_cbor::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(cbor_to_json).collect())
        }
        serde_cbor::Value::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .filter_map(|(k, v)| {
                    let key = match k {
                        serde_cbor::Value::Text(s) => s,
                        serde_cbor::Value::Integer(i) => i.to_string(),
                        _ => return None,
                    };
                    Some((key, cbor_to_json(v)))
                })
                .collect();
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::Null,
    }
}

impl bindings::seamlezz::surrealdb::call::Host for Ctx {
    async fn query(
        &mut self,
        query: String,
        params: Vec<(String, Vec<u8>)>,
    ) -> anyhow::Result<Vec<Result<Vec<u8>, String>>> {
        let Some(plugin) = self.get_plugin::<WasiSurrealdb>(PLUGIN_SURREALDB_ID) else {
            return Ok(vec![Err("SurrealDB plugin not available".to_string())]);
        };

        let db = plugin.db.read().await;

        let mut query_builder = db.query(&query);

        for (key, value) in params {
            let cbor_value: serde_cbor::Value = match serde_cbor::from_slice(&value) {
                Ok(v) => v,
                Err(e) => {
                    return Ok(vec![Err(format!(
                        "Failed to deserialize CBOR parameter '{}': {}",
                        key, e
                    ))]);
                }
            };

            let json_value = cbor_to_json(cbor_value);
            query_builder = query_builder.bind((key, json_value));
        }

        let mut result = match query_builder.await {
            Ok(r) => r,
            Err(e) => {
                return Ok(vec![Err(format!("Query execution failed: {}", e))]);
            }
        };

        let mut res = Vec::new();
        let num_statements = result.num_statements();

        for i in 0..num_statements {
            match result.take::<surrealdb::Value>(i) {
                Ok(response) => {
                    let inner = response.into_inner();
                    match into_content(inner) {
                        Ok(content) => match serde_cbor::to_vec(&content) {
                            Ok(bytes) => res.push(Ok(bytes)),
                            Err(e) => {
                                tracing::error!("CBOR serialization failed: {:?}", e);
                                res.push(Err(e.to_string()));
                            }
                        },
                        Err(e) => {
                            tracing::error!("Content conversion failed: {:?}", e);
                            res.push(Err(e.to_string()));
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!("Error taking result at index {}: {:?}", i, e);
                    res.push(Err(e.to_string()));
                }
            };
        }

        Ok(res)
    }
}

#[async_trait::async_trait]
impl HostPlugin for WasiSurrealdb {
    fn id(&self) -> &'static str {
        PLUGIN_SURREALDB_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from("seamlezz:surrealdb/call@0.1.0")]),
            ..Default::default()
        }
    }

    async fn on_component_bind(
        &self,
        component: &mut WorkloadComponent,
        interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let has_surrealdb = interfaces
            .iter()
            .any(|i| i.namespace == "seamlezz" && i.package == "surrealdb");

        if !has_surrealdb {
            tracing::warn!(
                "WasiSurrealdb plugin requested for non-seamlezz:surrealdb interface(s): {:?}",
                interfaces
            );
            return Ok(());
        }

        tracing::debug!(
            workload_id = component.workload_id(),
            component_id = component.id(),
            "Adding SurrealDB interfaces to linker"
        );

        bindings::seamlezz::surrealdb::call::add_to_linker::<_, HasSelf<Ctx>>(
            component.linker(),
            |ctx| ctx,
        )?;

        tracing::debug!(
            component_id = component.id(),
            "Successfully added SurrealDB interfaces to linker"
        );

        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        tracing::debug!(
            workload_id = workload_id,
            "SurrealDB plugin unbound from workload"
        );
        Ok(())
    }
}