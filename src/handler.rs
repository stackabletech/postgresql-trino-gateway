use std::sync::Arc;

use pgwire::api::PgWireServerHandlers;
use pgwire::api::auth::StartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};

use crate::query_extended::GatewayExtendedQueryHandler;
use crate::query_simple::GatewayQueryHandler;
use crate::startup::GatewayStartupHandler;

/// Factory that provides handler implementations to pgwire.
pub struct GatewayHandlerFactory {
    pub(crate) startup: Arc<GatewayStartupHandler>,
    pub(crate) query: Arc<GatewayQueryHandler>,
    pub(crate) extended_query: Arc<GatewayExtendedQueryHandler>,
}

impl GatewayHandlerFactory {
    pub fn new(
        startup: Arc<GatewayStartupHandler>,
        query: Arc<GatewayQueryHandler>,
        extended_query: Arc<GatewayExtendedQueryHandler>,
    ) -> Self {
        Self {
            startup,
            query,
            extended_query,
        }
    }
}

impl PgWireServerHandlers for GatewayHandlerFactory {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup.clone()
    }

    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.query.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.extended_query.clone()
    }
}
