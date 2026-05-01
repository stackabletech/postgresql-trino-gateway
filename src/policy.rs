// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0

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
    /// `--auth` is off and the listener is loopback. The gateway connects
    /// to Trino with `--trino-user` and no per-client credentials. Safe
    /// for local development.
    DisabledLoopback,
    /// `--auth` is off, the listener is non-loopback, and the operator
    /// explicitly set `--allow-insecure-listener`. Every network-reachable
    /// client gets unauthenticated access; only acceptable when Trino
    /// itself authenticates or the network is otherwise trusted.
    DisabledOpenBind,
    /// `--auth` is on and TLS is configured. The startup handler refuses
    /// plaintext clients on this posture.
    CleartextRequiresTls,
    /// `--auth` is on, no TLS, but the listener is a loopback address.
    /// Acceptable for local development only.
    CleartextLoopback,
}

/// Validate the gateway's security policy and emit startup log lines. The
/// returned `AuthPosture` is informational; the `Err` arm is the only
/// blocking outcome.
pub fn validate(config: &Config) -> Result<AuthPosture> {
    let posture = classify(config)?;
    validate_trino_posture(config)?;
    match posture {
        AuthPosture::DisabledLoopback => {} // default safe state, no log noise
        AuthPosture::DisabledOpenBind => {
            tracing::warn!(
                addr = %config.listen_addr,
                "auth disabled on a non-loopback bind — every network-reachable \
                 client gets unauthenticated access to Trino as --trino-user. \
                 This was opted into via --allow-insecure-listener."
            );
        }
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
        if listen_addr_is_loopback(&config.listen_addr)? {
            return Ok(AuthPosture::DisabledLoopback);
        }
        if config.allow_insecure_listener {
            return Ok(AuthPosture::DisabledOpenBind);
        }
        bail!(
            "--auth=false on a non-loopback bind ({}) is not allowed by default. \
             Either pass --auth (and --tls-cert/--tls-key for password protection) \
             to require authentication, or pass --allow-insecure-listener to \
             explicitly allow unauthenticated network access.",
            config.listen_addr
        )
    }

    // `--allow-insecure-listener` only relaxes the auth-off path. With auth
    // on it has no effect; warn so an operator who later drops --auth
    // realises the flag is now actively granting open network access.
    if config.allow_insecure_listener {
        tracing::warn!(
            "--allow-insecure-listener has no effect while --auth is enabled; \
             remove the flag unless you intend to drop --auth later"
        );
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

/// Check the Trino-side TLS / auth-forwarding posture and emit warnings
/// for each insecure knob. Refuses the one combination that would only
/// surface as a per-connection runtime error: `--auth` on with Trino
/// over plain HTTP and `--trino-allow-plaintext-auth` not set.
pub fn validate_trino_posture(config: &Config) -> Result<()> {
    if config.trino_tls_no_verify {
        if !config.trino_ssl {
            tracing::warn!(
                "--trino-tls-no-verify has no effect without --trino-ssl; remove the flag"
            );
        } else {
            tracing::warn!(
                "Trino TLS certificate verification is disabled — \
                 server identity is not authenticated. Use only on trusted networks \
                 with self-signed certs."
            );
        }
    }
    if config.trino_allow_plaintext_auth {
        if config.trino_ssl {
            tracing::warn!(
                "--trino-allow-plaintext-auth has no effect with --trino-ssl; \
                 credentials are sent over the TLS connection regardless"
            );
        } else if config.auth {
            tracing::warn!(
                trino_host = %config.trino_host,
                "forwarding client passwords to Trino over plain HTTP. \
                 Use only with a loopback or otherwise-trusted Trino endpoint."
            );
        } else {
            tracing::warn!(
                "--trino-allow-plaintext-auth has no effect when --auth is disabled; \
                 the gateway forwards no credentials"
            );
        }
    }
    if config.auth && !config.trino_ssl && !config.trino_allow_plaintext_auth {
        bail!(
            "--auth requires either --trino-ssl (HTTPS to Trino) or \
             --trino-allow-plaintext-auth (forward credentials over plain HTTP). \
             Without one, the Trino client will refuse to send the password and \
             every authenticated PG connection will fail."
        );
    }
    Ok(())
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
            trino_ssl: true, // safe default; tests that need plain HTTP override
            trino_tls_no_verify: false,
            trino_allow_plaintext_auth: false,
            auth,
            allow_insecure_listener: false,
            max_connections: 256,
        }
    }

    #[test]
    fn auth_disabled_on_loopback_is_ok() {
        assert_eq!(
            classify(&cfg(false, "127.0.0.1:5432", false)).unwrap(),
            AuthPosture::DisabledLoopback
        );
        assert_eq!(
            classify(&cfg(false, "[::1]:5432", false)).unwrap(),
            AuthPosture::DisabledLoopback
        );
    }

    #[test]
    fn auth_disabled_on_non_loopback_is_refused_without_opt_in() {
        let err = classify(&cfg(false, "0.0.0.0:5432", false)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("--allow-insecure-listener"),
            "must mention the opt-in flag: {msg}"
        );
        assert!(msg.contains("0.0.0.0:5432"), "echo bind addr: {msg}");
    }

    #[test]
    fn auth_disabled_on_non_loopback_passes_with_opt_in() {
        let mut c = cfg(false, "0.0.0.0:5432", false);
        c.allow_insecure_listener = true;
        assert_eq!(classify(&c).unwrap(), AuthPosture::DisabledOpenBind);
    }

    /// The opt-in flag must not flip a loopback bind into the open-bind
    /// posture. The loopback check runs first; the flag is a no-op on
    /// loopback.
    #[test]
    fn allow_insecure_listener_on_loopback_stays_disabled_loopback() {
        let mut c = cfg(false, "127.0.0.1:5432", false);
        c.allow_insecure_listener = true;
        assert_eq!(classify(&c).unwrap(), AuthPosture::DisabledLoopback);
    }

    /// Setting both `--auth` and `--allow-insecure-listener` is a footgun
    /// (the flag is silently ignored while auth is on, then activates if
    /// the operator later drops --auth). `classify` warns but accepts.
    #[test]
    fn allow_insecure_listener_with_auth_on_is_accepted_with_warning() {
        let mut c = cfg(true, "127.0.0.1:5432", true);
        c.allow_insecure_listener = true;
        // Warn is logged; classify still returns the auth-driven posture.
        assert_eq!(classify(&c).unwrap(), AuthPosture::CleartextRequiresTls);
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

    /// Disabled posture now validates listen-addr (it must be loopback or
    /// opted in via --allow-insecure-listener), so an unparseable address
    /// surfaces an error here.
    #[test]
    fn auth_disabled_with_unparseable_addr_surfaces_error() {
        let err = classify(&cfg(false, "garbage", false)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("invalid --listen-addr") || msg.contains("garbage"));
    }

    // -- validate_trino_posture --

    #[test]
    fn trino_posture_default_https_is_ok() {
        let c = cfg(true, "127.0.0.1:5432", false);
        assert!(validate_trino_posture(&c).is_ok());
    }

    /// Auth on + Trino HTTP + no plaintext-auth opt-in: the trino client
    /// would refuse to send the password and every authenticated PG
    /// connection would fail. Surface that at startup.
    #[test]
    fn trino_posture_auth_over_http_without_opt_in_is_refused() {
        let mut c = cfg(true, "127.0.0.1:5432", false);
        c.trino_ssl = false;
        let err = validate_trino_posture(&c).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("--trino-ssl") && msg.contains("--trino-allow-plaintext-auth"),
            "should mention both escape hatches: {msg}"
        );
    }

    #[test]
    fn trino_posture_auth_over_http_with_opt_in_passes() {
        let mut c = cfg(true, "127.0.0.1:5432", false);
        c.trino_ssl = false;
        c.trino_allow_plaintext_auth = true;
        assert!(validate_trino_posture(&c).is_ok());
    }

    /// Tls-no-verify with auth disabled and Trino over HTTPS: warn but
    /// pass. The flag is a useful self-signed-cert escape hatch.
    #[test]
    fn trino_posture_tls_no_verify_with_https_passes() {
        let mut c = cfg(false, "127.0.0.1:5432", false);
        c.trino_tls_no_verify = true;
        assert!(validate_trino_posture(&c).is_ok());
    }

    /// Tls-no-verify without --trino-ssl: the flag has no effect because
    /// verification only runs over TLS. Warn, but don't refuse.
    #[test]
    fn trino_posture_tls_no_verify_without_ssl_warns_but_passes() {
        let mut c = cfg(false, "127.0.0.1:5432", false);
        c.trino_ssl = false;
        c.trino_tls_no_verify = true;
        // auth is off here so the auth+http refusal doesn't apply.
        assert!(validate_trino_posture(&c).is_ok());
    }

    /// Plaintext-auth flag set with --trino-ssl on: redundant (TLS already
    /// protects the credentials in transit). Warn but pass.
    #[test]
    fn trino_posture_plaintext_auth_with_https_warns_but_passes() {
        let mut c = cfg(true, "127.0.0.1:5432", true);
        c.trino_allow_plaintext_auth = true;
        assert!(validate_trino_posture(&c).is_ok());
    }

    /// Plaintext-auth flag set with --auth off and Trino over HTTP: the
    /// flag would only matter if credentials were being forwarded, but
    /// there are none. Warn but pass.
    #[test]
    fn trino_posture_plaintext_auth_without_auth_warns_but_passes() {
        let mut c = cfg(false, "127.0.0.1:5432", false);
        c.trino_ssl = false;
        c.trino_allow_plaintext_auth = true;
        assert!(validate_trino_posture(&c).is_ok());
    }

    /// Defence in depth: if validate_trino_posture is ever bypassed, the
    /// trino-rust-client itself rejects the broken combo at build time
    /// (BasicAuthWithHttp). Pin that contract so a future trino-rust-client
    /// upgrade that loosens it doesn't silently let credentials leak over
    /// plain HTTP without the operator's explicit opt-in.
    #[test]
    fn trino_client_builder_rejects_basic_auth_over_http_when_not_opted_in() {
        use trino_rust_client::ClientBuilder;
        use trino_rust_client::auth::Auth;
        // secure(false) is the default, auth_http_insecure(false) is the
        // default; setting Basic auth in this state must error at build().
        let result = ClientBuilder::new("u", "h")
            .auth(Auth::new_basic("u", Some("pw")))
            .build();
        assert!(
            result.is_err(),
            "trino-rust-client must reject Basic auth over HTTP when not explicitly allowed"
        );
    }
}
