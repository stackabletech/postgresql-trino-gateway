// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.

//! Startup-time security-policy validation.
//!
//! `--auth` enables a cleartext-password challenge on each PG connection.
//! Cleartext-over-plaintext is unsafe on any network, so the gateway refuses
//! to start that configuration unless the listener is bound to a loopback
//! address. SCRAM-SHA-256 is not implemented in this gateway because the
//! current architecture forwards the cleartext password to Trino as HTTP
//! Basic auth — a future architecture (gateway-side password store + a
//! Trino service-account credential) could support SCRAM, but that is out
//! of scope here.
//!
//! Per-connection enforcement (refusing plaintext clients when auth+TLS are
//! configured) lives in `startup.rs` because pgwire does not refuse
//! clients that skip the SslRequest upgrade — the gateway must check
//! `ClientInfo::is_secure()` itself.

use std::net::{SocketAddr, ToSocketAddrs};

use anyhow::{Context, Result, bail};

use crate::config::Config;

/// Result of inspecting the (auth, TLS, listen-addr) triple. Returned for
/// logging/diagnostic use; the policy decision is enforced by `validate`'s
/// `Result`.
///
/// `CleartextRequiresTls` does **not** by itself guarantee a TLS-only
/// listener — pgwire accepts plaintext clients regardless of whether a
/// `TlsAcceptor` is configured, because the SslRequest upgrade is opt-in
/// per connection. The startup handler enforces TLS-only when this posture
/// is in effect; see `startup.rs`.
#[derive(Debug, PartialEq, Eq)]
pub enum AuthPosture {
    /// `--auth` is off. The gateway connects to Trino with `--trino-user`
    /// and no per-client credentials.
    Disabled,
    /// `--auth` is on and TLS is configured. The startup handler refuses
    /// plaintext clients on this posture.
    CleartextRequiresTls,
    /// `--auth` is on, no TLS, but the listener is a loopback address.
    /// Acceptable for local development only.
    CleartextLoopback,
}

/// Validate the gateway's security policy and emit a startup log line. The
/// returned `AuthPosture` is informational; the `Err` arm is the only
/// blocking outcome.
pub fn validate(config: &Config) -> Result<AuthPosture> {
    let posture = classify(config)?;
    match posture {
        AuthPosture::Disabled => {} // default state, no log line needed
        AuthPosture::CleartextRequiresTls => {
            tracing::info!(
                "auth enabled — cleartext password over TLS \
                 (plaintext clients will be refused; password forwarded to Trino as HTTP Basic)"
            );
        }
        AuthPosture::CleartextLoopback => {
            tracing::warn!(
                addr = %config.listen_addr,
                "auth enabled on a plaintext loopback listener — dev only, NOT for production. \
                 Configure --tls-cert and --tls-key for any non-loopback deployment."
            );
        }
    }
    Ok(posture)
}

/// Pure classification helper, separated from logging so it can be tested
/// without a tracing subscriber.
pub fn classify(config: &Config) -> Result<AuthPosture> {
    if !config.auth {
        return Ok(AuthPosture::Disabled);
    }

    let has_tls = config.tls_cert.is_some() && config.tls_key.is_some();
    if has_tls {
        return Ok(AuthPosture::CleartextRequiresTls);
    }

    if listen_addr_is_loopback(&config.listen_addr)? {
        return Ok(AuthPosture::CleartextLoopback);
    }

    bail!(
        "--auth on non-loopback bind ({}) requires TLS. \
         Set --tls-cert and --tls-key, or bind to 127.0.0.1 / [::1] / localhost for dev.",
        config.listen_addr
    )
}

/// Return true iff every address that `addr` resolves to is a loopback IP.
///
/// We accept hostname forms like `localhost:5432` because `TcpListener::bind`
/// also accepts them; rejecting them on the parser path alone would block a
/// common safe configuration. If any resolved address is non-loopback the
/// host is considered non-loopback (conservative: a hostname pointing to
/// both `127.0.0.1` and a public IP is treated as public).
fn listen_addr_is_loopback(addr: &str) -> Result<bool> {
    if let Ok(parsed) = addr.parse::<SocketAddr>() {
        return Ok(parsed.ip().is_loopback());
    }
    let resolved: Vec<_> = addr
        .to_socket_addrs()
        .with_context(|| format!("invalid --listen-addr: {addr}"))?
        .collect();
    if resolved.is_empty() {
        bail!("--listen-addr {addr} did not resolve to any address");
    }
    Ok(resolved.iter().all(|s| s.ip().is_loopback()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn cfg(auth: bool, listen: &str, tls: bool) -> Config {
        Config {
            listen_addr: listen.to_owned(),
            tls_cert: tls.then(|| PathBuf::from("/tmp/cert")),
            tls_key: tls.then(|| PathBuf::from("/tmp/key")),
            trino_host: "h".to_owned(),
            trino_port: 8080,
            trino_catalog: "c".to_owned(),
            trino_schema: "s".to_owned(),
            trino_user: "u".to_owned(),
            trino_ssl: false,
            trino_ssl_insecure: false,
            auth,
        }
    }

    #[test]
    fn auth_disabled_is_always_ok() {
        assert_eq!(
            classify(&cfg(false, "0.0.0.0:5432", false)).unwrap(),
            AuthPosture::Disabled
        );
        assert_eq!(
            classify(&cfg(false, "127.0.0.1:5432", false)).unwrap(),
            AuthPosture::Disabled
        );
    }

    #[test]
    fn auth_with_tls_passes_on_any_address() {
        assert_eq!(
            classify(&cfg(true, "0.0.0.0:5432", true)).unwrap(),
            AuthPosture::CleartextRequiresTls
        );
        assert_eq!(
            classify(&cfg(true, "127.0.0.1:5432", true)).unwrap(),
            AuthPosture::CleartextRequiresTls
        );
        assert_eq!(
            classify(&cfg(true, "[::]:5432", true)).unwrap(),
            AuthPosture::CleartextRequiresTls
        );
    }

    #[test]
    fn auth_without_tls_on_loopback_warns_but_passes() {
        assert_eq!(
            classify(&cfg(true, "127.0.0.1:5432", false)).unwrap(),
            AuthPosture::CleartextLoopback
        );
        assert_eq!(
            classify(&cfg(true, "[::1]:5432", false)).unwrap(),
            AuthPosture::CleartextLoopback
        );
    }

    /// `IpAddr::is_loopback` accepts the entire 127.0.0.0/8 range (RFC 5735),
    /// not just 127.0.0.1. Document and pin that behaviour.
    #[test]
    fn auth_without_tls_on_non_canonical_loopback_passes() {
        assert_eq!(
            classify(&cfg(true, "127.0.0.2:5432", false)).unwrap(),
            AuthPosture::CleartextLoopback
        );
    }

    /// `localhost` resolves to a loopback IP on every reasonable system.
    /// Accept it rather than rejecting a common safe configuration.
    #[test]
    fn auth_without_tls_on_localhost_hostname_passes() {
        assert_eq!(
            classify(&cfg(true, "localhost:5432", false)).unwrap(),
            AuthPosture::CleartextLoopback
        );
    }

    #[test]
    fn auth_without_tls_on_non_loopback_is_refused() {
        let err = classify(&cfg(true, "0.0.0.0:5432", false)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("requires TLS"), "actionable message: {msg}");
        assert!(msg.contains("0.0.0.0:5432"), "echo bind addr: {msg}");
    }

    #[test]
    fn auth_without_tls_on_public_ip_is_refused() {
        let err = classify(&cfg(true, "10.20.30.40:5432", false)).unwrap_err();
        assert!(format!("{err:#}").contains("requires TLS"));
    }

    #[test]
    fn unparseable_listen_addr_with_auth_no_tls_surfaces_resolve_error() {
        // Use a name that DNS will not resolve. The error must mention the
        // input verbatim so the operator can fix it.
        let err = classify(&cfg(true, "not.a.real.host.invalid:5432", false)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("invalid --listen-addr") || msg.contains("not.a.real.host.invalid"),
            "should surface the bad input: {msg}"
        );
    }

    /// Asymmetric tls_cert without tls_key is treated as no-TLS by the
    /// has_tls check. A non-loopback bind in that state is refused, which
    /// matches the main.rs warning that already fires.
    #[test]
    fn asymmetric_tls_flags_are_treated_as_no_tls() {
        let mut c = cfg(true, "0.0.0.0:5432", false);
        c.tls_cert = Some(PathBuf::from("/tmp/cert"));
        c.tls_key = None;
        assert!(classify(&c).is_err());
    }

    /// Disabled posture skips listen-addr validation by design — without
    /// auth, there are no credentials to protect. `TcpListener::bind` will
    /// surface its own error if the address is unusable.
    #[test]
    fn auth_disabled_does_not_validate_listen_addr() {
        assert_eq!(
            classify(&cfg(false, "garbage", false)).unwrap(),
            AuthPosture::Disabled
        );
    }
}
