// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0

//! TLS termination for the PostgreSQL listening socket.
//!
//! Loads a PEM-encoded certificate chain and private key from disk and
//! returns a `TlsAcceptor` ready to hand to `pgwire::tokio::process_socket`.
//!
//! `aws-lc-rs` is used as the rustls crypto provider, attached explicitly
//!
//! # Implementation notes
//!
//! The `ServerConfig` is built with `ServerConfig::builder_with_provider`
//! rather than `ServerConfig::builder` because the latter requires a
//! process-level provider to be installed, which is not always the case
//! when running in a container.
//!
//! The `ServerConfig` is built with `with_safe_default_protocol_versions`
//! per `ServerConfig` rather than via the global default. This avoids the
//! panic that `ServerConfig::builder()` raises when no process-level
//! provider has been installed and removes the ordering coupling between
//! `build_acceptor` and any global init step.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use pgwire::tokio::TlsAcceptor;
use pgwire::tokio::tokio_rustls::rustls::ServerConfig;
use pgwire::tokio::tokio_rustls::rustls::crypto::aws_lc_rs;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// Build a `TlsAcceptor` from PEM-encoded certificate chain and private key
/// files on disk. Supports PKCS#8, RSA (PKCS#1), and SEC1 EC private keys.
pub fn build_acceptor(cert_path: &Path, key_path: &Path) -> Result<TlsAcceptor> {
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let provider = Arc::new(aws_lc_rs::default_provider());
    let cfg = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("rustls: invalid protocol-version configuration")?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .with_context(|| {
            format!(
                "TLS keypair invalid (cert {}, key {})",
                cert_path.display(),
                key_path.display()
            )
        })?;

    Ok(TlsAcceptor::from(Arc::new(cfg)))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)
        .with_context(|| format!("opening TLS certificate file {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("reading TLS certificate PEM {}", path.display()))?;
    if certs.is_empty() {
        return Err(anyhow!("no PEM certificates found in {}", path.display()));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file =
        File::open(path).with_context(|| format!("opening TLS key file {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let key = rustls_pemfile::private_key(&mut reader)
        .with_context(|| format!("reading TLS key PEM {}", path.display()))?
        .ok_or_else(|| anyhow!("no PEM private key found in {}", path.display()))?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// `TlsAcceptor` doesn't implement `Debug`, so `unwrap_err` on the
    /// `Result<TlsAcceptor, _>` doesn't compile. Match instead.
    fn err_msg(r: Result<TlsAcceptor>) -> String {
        match r {
            Ok(_) => panic!("expected error"),
            Err(e) => format!("{e:#}"),
        }
    }

    /// Generate a throwaway self-signed cert+key as PEM strings.
    fn self_signed_pem() -> (String, String) {
        let cert = rcgen::generate_simple_self_signed(vec!["gateway.test".to_owned()]).unwrap();
        (cert.cert.pem(), cert.signing_key.serialize_pem())
    }

    #[test]
    fn build_acceptor_succeeds_with_valid_self_signed_pair() {
        let (cert_pem, key_pem) = self_signed_pem();
        let mut cert_file = tempfile::NamedTempFile::new().unwrap();
        let mut key_file = tempfile::NamedTempFile::new().unwrap();
        cert_file.write_all(cert_pem.as_bytes()).unwrap();
        key_file.write_all(key_pem.as_bytes()).unwrap();

        let result = build_acceptor(cert_file.path(), key_file.path());
        assert!(
            result.is_ok(),
            "expected acceptor to build cleanly with rcgen-generated PEM pair: {:?}",
            result.as_ref().err().map(|e| format!("{e:#}"))
        );
    }

    #[test]
    fn build_acceptor_rejects_missing_cert_file() {
        let msg = err_msg(build_acceptor(
            Path::new("/nonexistent/cert.pem"),
            Path::new("/nonexistent/key.pem"),
        ));
        assert!(
            msg.contains("certificate"),
            "error should mention certificate: {msg}"
        );
    }

    #[test]
    fn build_acceptor_rejects_empty_cert_file() {
        let mut cert = tempfile::NamedTempFile::new().unwrap();
        let mut key = tempfile::NamedTempFile::new().unwrap();
        cert.write_all(b"").unwrap();
        key.write_all(b"").unwrap();
        let msg = err_msg(build_acceptor(cert.path(), key.path()));
        assert!(
            msg.contains("no PEM certificates"),
            "error should mention missing PEM certs: {msg}"
        );
    }

    #[test]
    fn build_acceptor_rejects_garbage_cert() {
        let mut cert = tempfile::NamedTempFile::new().unwrap();
        let mut key = tempfile::NamedTempFile::new().unwrap();
        cert.write_all(b"this is not a PEM file").unwrap();
        key.write_all(b"neither is this").unwrap();
        let msg = err_msg(build_acceptor(cert.path(), key.path()));
        assert!(
            msg.contains("PEM") || msg.contains("certificate"),
            "error should mention PEM/certificate parsing: {msg}"
        );
    }

    /// Mismatched cert and key (cert from one self-signed pair, key from
    /// another) must be rejected by `with_single_cert`.
    #[test]
    fn build_acceptor_rejects_mismatched_cert_and_key() {
        let (cert_pem, _) = self_signed_pem();
        let (_, key_pem) = self_signed_pem();
        let mut cert_file = tempfile::NamedTempFile::new().unwrap();
        let mut key_file = tempfile::NamedTempFile::new().unwrap();
        cert_file.write_all(cert_pem.as_bytes()).unwrap();
        key_file.write_all(key_pem.as_bytes()).unwrap();

        let msg = err_msg(build_acceptor(cert_file.path(), key_file.path()));
        assert!(
            msg.contains("TLS keypair invalid"),
            "error should mention keypair mismatch: {msg}"
        );
    }
}
