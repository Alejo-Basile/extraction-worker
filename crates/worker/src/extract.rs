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

pub struct PdfExtractor {
    /// Tope de paginas por documento (defensa ante PDF hostil, §11.5).
    /// Contar paginas antes de extraer evita parsear texto de PDFs patologicos.
    pub max_pages: u32,
}

impl PdfExtractor {
    pub fn new(max_pages: u32) -> Self {
        Self { max_pages }
    }
}

impl TextExtractor for PdfExtractor {
    fn extract(&self, pdf_path: &Path) -> Result<String, ExtractorError> {
        // Conteo previo con lopdf: un PDF de miles de paginas muere aqui y no
        // en la extraccion de texto. El parseo doble es el costo aceptado.
        let pages = lopdf::Document::load(pdf_path)
            .map_err(|e| ExtractorError {
                code: error_code::PDF_CORRUPT.to_string(),
                message: format!("no se pudo abrir el PDF: {e} ({})", pdf_path.display()),
                kind: ErrorKind::Business,
            })?
            .get_pages()
            .len();
        if pages > self.max_pages as usize {
            return Err(ExtractorError {
                code: error_code::PDF_TOO_MANY_PAGES.to_string(),
                message: format!("{pages} paginas superan el tope de {}", self.max_pages),
                kind: ErrorKind::Business,
            });
        }
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
        let ext = PdfExtractor::new(2000);
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
    fn pdf_con_demasiadas_paginas_es_error_de_negocio() {
        let ext = PdfExtractor::new(1);
        let path = three_pages_fixture();
        let res = ext.extract(&path);
        let _ = std::fs::remove_file(&path);
        match res {
            Err(e) => {
                assert_eq!(e.code, error_code::PDF_TOO_MANY_PAGES);
                assert_eq!(e.kind, ErrorKind::Business);
            }
            Ok(_) => panic!("un PDF de 3 paginas con tope 1 debe fallar"),
        }
    }

    /// PDF de 3 paginas generado al vuelo con xref correcto.
    fn three_pages_fixture() -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "f3b-tres-paginas-{}-{}.pdf",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let page = |n: usize| {
            format!(
                "BT /F1 12 Tf 20 {} Td (Pagina {n}) Tj ET\n",
                792 - 20 * n as i32
            )
        };
        let mut out: Vec<u8> = b"%PDF-1.4\n".to_vec();
        let mut offsets: Vec<usize> = Vec::new();
        let mut push = |out: &mut Vec<u8>, body: &[u8]| {
            offsets.push(out.len());
            out.extend_from_slice(body);
        };
        push(
            &mut out,
            b"1 0 obj\n<</Type/Catalog/Pages 2 0 R>>\nendobj\n",
        );
        push(
            &mut out,
            b"2 0 obj\n<</Type/Pages/Count 3/Kids[4 0 R 5 0 R 6 0 R]>>\nendobj\n",
        );
        push(
            &mut out,
            b"3 0 obj\n<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>\nendobj\n",
        );
        for i in 0..3usize {
            let body = format!(
                "{} 0 obj\n<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]/Contents {} 0 R/Resources<</Font<</F1 3 0 R>>>>>>\nendobj\n",
                i + 4,
                i + 7
            );
            push(&mut out, body.as_bytes());
        }
        for i in 0..3usize {
            let content = page(i + 1);
            let body = format!(
                "{} 0 obj\n<</Length {}>>\nstream\n{}endstream\nendobj\n",
                i + 7,
                content.len(),
                content
            );
            push(&mut out, body.as_bytes());
        }
        let xref_pos = out.len();
        out.extend_from_slice(b"xref\n0 10\n0000000000 65535 f \n");
        for off in &offsets {
            out.extend_from_slice(format!("{off:010} 00000 n \n").as_bytes());
        }
        out.extend_from_slice(
            format!("trailer<</Size 10/Root 1 0 R>>\nstartxref\n{xref_pos}\n%%EOF\n").as_bytes(),
        );
        std::fs::write(&path, &out).unwrap_or_else(|e| panic!("fixture temporal: {e}"));
        path
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
        let ext = PdfExtractor::new(2000);
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
