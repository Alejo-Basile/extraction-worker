//! Adaptador real de extraccion de texto con `pdf-extract` (S2-P3-01).
//!
//! La extraccion es CPU-bound y sincrona: el pipeline la ejecuta en
//! `spawn_blocking` (SDD 3.3). El PDF llega como archivo temporal ya
//! descargado, nunca como buffer completo en memoria.

use std::path::Path;

use extraction_core::error_code;
use extraction_core::{ErrorKind, ExtractorError, TextExtractor};

/// Clasifica un error de `pdf-extract`. Un PDF cifrado se detecta por el
/// mensaje (lopdf no es dependencia directa y el tipo exacto no expone el
/// variant Decryption desde aca).
fn pdf_error(e: pdf_extract::OutputError, path: &Path) -> ExtractorError {
    let message = e.to_string();
    let (code, kind) = if message.contains("password") || message.contains("encrypt") {
        (error_code::PDF_ENCRYPTED, ErrorKind::Business)
    } else {
        (error_code::PDF_CORRUPT, ErrorKind::Business)
    };
    ExtractorError {
        code: code.to_string(),
        message: format!("{message} ({})", path.display()),
        kind,
    }
}

pub struct PdfExtractor;

impl TextExtractor for PdfExtractor {
    fn extract(&self, pdf_path: &Path) -> Result<String, ExtractorError> {
        let text = pdf_extract::extract_text(pdf_path).map_err(|e| pdf_error(e, pdf_path))?;
        if text.trim().is_empty() {
            return Err(ExtractorError {
                code: error_code::PDF_NO_TEXT.to_string(),
                message: format!("sin texto extraible: {}", pdf_path.display()),
                kind: ErrorKind::Business,
            });
        }
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/minimal.pdf")
    }

    #[test]
    fn extrae_texto_del_fixture() {
        let ext = PdfExtractor;
        let path = fixture_path();
        if !path.exists() {
            panic!("fixture ausente: {}", path.display());
        }
        match ext.extract(&path) {
            Ok(text) => assert!(!text.trim().is_empty(), "el fixture debe tener texto"),
            Err(e) => panic!("extraccion del fixture fallo: {e}"),
        }
    }

    #[test]
    fn pdf_corrupto_es_error_de_negocio() {
        let dir = std::env::temp_dir().join(format!(
            "pdf-corrupto-{}-{}.pdf",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::write(&dir, b"esto no es un pdf").unwrap_or_else(|e| {
            panic!("no se pudo escribir el temporal: {e}");
        });
        let ext = PdfExtractor;
        let res = ext.extract(&dir);
        let _ = std::fs::remove_file(&dir);
        match res {
            Err(e) => {
                assert_eq!(e.code, error_code::PDF_CORRUPT);
                assert_eq!(e.kind, ErrorKind::Business);
            }
            Ok(_) => panic!("un archivo no-PDF debe fallar"),
        }
    }
}
