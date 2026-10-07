//! Implementacion fake de `TextExtractor` para tests (S0-P3-06).

use crate::{ErrorKind, ExtractorError, TextExtractor};

/// Extractor fake deterministico. No lee PDFs reales: devuelve contenido
/// estable segun el input para facilitar tests.
#[derive(Default, Debug, Clone)]
pub struct FakeTextExtractor {
    /// Texto a devolver cuando el input es reconocido como valido.
    pub text: String,
    /// Codigo de error a devolver (si Some), en lugar de extraer texto.
    pub error_code: Option<String>,
    /// Mensaje a devolver en caso de error.
    pub error_message: String,
}

impl FakeTextExtractor {
    /// Crea un extractor fake que siempre devuelve `text`.
    pub fn returning(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            error_code: None,
            error_message: String::new(),
        }
    }

    /// Crea un extractor fake que siempre devuelve un error de negocio.
    pub fn failing(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            text: String::new(),
            error_code: Some(code.into()),
            error_message: message.into(),
        }
    }
}

impl TextExtractor for FakeTextExtractor {
    fn extract(&self, _pdf: &[u8]) -> Result<String, ExtractorError> {
        if let Some(code) = self.error_code.clone() {
            return Err(ExtractorError {
                code,
                message: self.error_message.clone(),
                kind: ErrorKind::Business,
            });
        }
        Ok(self.text.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_devuelve_texto_deterministico() {
        let ext = FakeTextExtractor::returning("hola mundo");
        let res = ext.extract(b"%PDF-1.4\nfake\n");
        assert!(matches!(res, Ok(s) if s == "hola mundo"));
    }

    #[test]
    fn fake_devuelve_error_de_negocio() {
        let ext = FakeTextExtractor::failing("PDF_CORRUPT", "xref roto");
        let res = ext.extract(b"not pdf");
        match res {
            Err(err) => {
                assert_eq!(err.code, "PDF_CORRUPT");
                assert_eq!(err.kind, ErrorKind::Business);
            }
            Ok(_) => panic!("debe fallar"),
        }
    }
}
