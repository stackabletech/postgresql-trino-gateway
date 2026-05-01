// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use std::sync::Arc;

use pgwire::api::PgWireServerHandlers;
use pgwire::api::auth::StartupHandler;
use pgwire::api::cancel::CancelHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};

use crate::cancel::GatewayCancelHandler;
use crate::query_extended::GatewayExtendedQueryHandler;
use crate::query_simple::GatewayQueryHandler;
use crate::startup::GatewayStartupHandler;

pub struct GatewayHandlerFactory {
    pub(crate) startup: Arc<GatewayStartupHandler>,
    pub(crate) query: Arc<GatewayQueryHandler>,
    pub(crate) extended_query: Arc<GatewayExtendedQueryHandler>,
    pub(crate) cancel: Arc<GatewayCancelHandler>,
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
            cancel: Arc::new(GatewayCancelHandler),
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

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        self.cancel.clone()
    }
}
