# ADR-OC-01: ocr-worker separado con Tesseract para Fase B

Estado: **Propuesto**
Fecha: 2025-11-16

## Contexto
Fase B requiere extraer texto de documentos escaneados PDF sin metadatos de texto. `extraction-worker` actual usa `pdf-parse` para texto embebido y rechaza escaneados.

## Decisión
Crear `ocr-worker` independiente en mismo repo, con cola `stream:ocr-processing` y DLQ `stream:ocr-processing-dlq`, handoff atómico `XADD + XACK`.

Tesseract como baseline OCR:
- Idiomas spa+eng
- `tessdata_fast`
- OEM 1, PSM 3
- DPI inicial 150-200

Rasterizador: evaluar Poppler `pdftoppm` vs Ghostscript. Provisionalmente Poppler por performance.

## Consecuencias
Positivo:
- Separación de responsabilidades, escala independiente.
- Evidencia basada en POC antes de implementación.

Negativo:
- Aumenta latencia de extracción para escaneados.
- Gestión de recursos CPU/RSS por página.

## Gates de aceptación
- CER medio < 0.05 en corpus sintético limpio DPI 150.
- P95 tiempo/página < 8 s en CPU serie 2 vCPU.
- RSS pico < 300 MB por página 3500px límite.

## Evidencia
POC preliminar `labs/ocr-poc/`:
- Ghostscript DPI150 ~3.8 s/página, CER ~0.03
- Poppler DPI150 1 página 3.79 s, 5 páginas media 5.57 s/página
- CER 0.024-0.041 en texto sintético limpio
Se requiere completar métricas RSS y límite de dimensiones 3500px.

## Próximos pasos
Completar POC, documentar métricas finales, implementar worker con los parámetros validados.
