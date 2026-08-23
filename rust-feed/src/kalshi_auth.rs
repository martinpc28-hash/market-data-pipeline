//! Firma RSA-PSS/SHA-256 para autenticar contra Kalshi -- EXACTAMENTE la
//! misma receta que ya usa este proyecto en Python y que ya está
//! funcionando en producción contra la cuenta real del usuario (ver
//! ingestion/kalshi_ingest.py::build_auth_headers y
//! common/kalshi_rest.py::_sign, con la librería `cryptography`). Portada
//! acá 1:1 al equivalente en Rust (RustCrypto: rsa + sha2), no
//! reinventada -- si el Python ya firma bien contra la cuenta real, esta
//! misma receta debería andar igual.
//!
//! Mensaje a firmar: "<timestamp_ms><METHOD><path>" (concatenado, sin
//! separadores) -- ej. "1703123456789GET/trade-api/ws/v2".
//! Padding: PSS con MGF1(SHA-256), salt_length = tamaño del digest de
//! SHA-256 (32 bytes). Firma resultante en base64.

use anyhow::{Context, Result};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;
use sha2::Sha256;

pub struct KalshiCredentials {
    pub api_key_id: String,
    private_key: RsaPrivateKey,
}

impl KalshiCredentials {
    /// Carga las credenciales desde variables de entorno -- mismos
    /// nombres que ya usa el resto del proyecto (KALSHI_API_KEY_ID,
    /// KALSHI_PRIVATE_KEY_PATH), así que el mismo .env / mismo volumen
    /// montado (./secrets:/app/secrets:ro) sirve sin cambios.
    ///
    /// La clave PEM puede venir en formato PKCS8 ("BEGIN PRIVATE KEY")
    /// o PKCS1 ("BEGIN RSA PRIVATE KEY") -- Python's
    /// `serialization.load_pem_private_key` detecta el formato solo, acá
    /// se prueban los dos a mano por lo mismo (no se puede confirmar de
    /// antemano cuál generó el usuario).
    pub fn from_env() -> Result<Self> {
        let api_key_id = std::env::var("KALSHI_API_KEY_ID")
            .context("falta KALSHI_API_KEY_ID en el entorno")?;
        let key_path = std::env::var("KALSHI_PRIVATE_KEY_PATH")
            .context("falta KALSHI_PRIVATE_KEY_PATH en el entorno")?;

        // Mismo criterio que Python (PROJECT_ROOT / key_path): si no es
        // una ruta absoluta, es relativa a la raíz del proyecto, que
        // dentro del contenedor es /app (ver Dockerfile.rust-feed).
        let full_path = if std::path::Path::new(&key_path).is_absolute() {
            std::path::PathBuf::from(&key_path)
        } else {
            std::path::PathBuf::from("/app").join(&key_path)
        };
        let pem = std::fs::read_to_string(&full_path)
            .with_context(|| format!("no se pudo leer la clave privada en {full_path:?}"))?;

        let private_key = RsaPrivateKey::from_pkcs8_pem(&pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(&pem))
            .context("no se pudo parsear la clave privada de Kalshi (ni como PKCS8 ni PKCS1)")?;

        Ok(Self {
            api_key_id,
            private_key,
        })
    }

    /// Headers de autenticación para UNA conexión/request -- hay que
    /// generarlos de nuevo en cada reconexión (el timestamp tiene una
    /// ventana de validez corta, igual que en Python).
    pub fn auth_headers(&self, method: &str, path: &str) -> Vec<(String, String)> {
        let timestamp_ms = crate::state::now_unix_millis().to_string();
        let message = format!("{timestamp_ms}{method}{path}");

        let mut rng = rand::thread_rng();
        let signing_key = rsa::pss::SigningKey::<Sha256>::new(self.private_key.clone());
        let signature = signing_key.sign_with_rng(&mut rng, message.as_bytes());
        let signature_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            signature.to_bytes(),
        );

        vec![
            ("KALSHI-ACCESS-KEY".to_string(), self.api_key_id.clone()),
            ("KALSHI-ACCESS-SIGNATURE".to_string(), signature_b64),
            ("KALSHI-ACCESS-TIMESTAMP".to_string(), timestamp_ms),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs8::EncodePrivateKey;

    #[test]
    fn signs_and_produces_expected_header_names() {
        // Clave de prueba generada en memoria (NO es una clave real) --
        // solo para confirmar que el flujo de firma no explota y que los
        // headers tienen el nombre/forma correcta, no para validar
        // contra la API real de Kalshi (eso solo se puede probar en la
        // máquina del usuario).
        let mut rng = rand::thread_rng();
        let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pem = key
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .to_string();

        let dir = std::env::temp_dir();
        let key_path = dir.join("test_kalshi_key.pem");
        std::fs::write(&key_path, pem).unwrap();

        std::env::set_var("KALSHI_API_KEY_ID", "test-key-id");
        std::env::set_var("KALSHI_PRIVATE_KEY_PATH", key_path.to_str().unwrap());

        let creds = KalshiCredentials::from_env().unwrap();
        let headers = creds.auth_headers("GET", "/trade-api/ws/v2");

        let names: Vec<_> = headers.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "KALSHI-ACCESS-KEY",
                "KALSHI-ACCESS-SIGNATURE",
                "KALSHI-ACCESS-TIMESTAMP",
            ]
        );
        assert_eq!(headers[0].1, "test-key-id");
        assert!(!headers[1].1.is_empty());

        std::fs::remove_file(&key_path).ok();
    }
}
