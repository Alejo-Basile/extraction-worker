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

Rasterizador: Poppler `pdftoppm` validado con limitador de dimensiones. Ghostscript queda como comparación pendiente antes de cerrar el ADR.

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
POC `labs/ocr-poc/` con corpus A4 sintético a 300 DPI y Tesseract spa+eng, OEM 1, PSM 3:

- Limitador `convert -resize '3500x3500>' -resize '12000000@>'` validado con página sobredimensionada:
  - DPI150: 5167x7309, RSS 483 MB, 5.9 s -> 2474x3500, RSS 85.9 MB, 1.4 s, CER 0.0063.
  - DPI200: 6889x9745, RSS 823 MB, 8.6 s -> 2474x3500, RSS 85.7 MB, 1.4 s, CER 0.0063.
- Corpus A4 con limitador (1, 5 y 20 páginas):
  - DPI150: 715-776 ms/página, CER 0.0056-0.0067, RSS 55-56 MB.
  - DPI200: 796-889 ms/página, CER 0.0056-0.0067, RSS 62-63 MB.
- Gates: CER <0.05 cumple; P95 tiempo/página <8 s cumple; RSS pico <300 MB cumple.
- Ghostscript: medición preliminar con fixtures sobredimensionadas (~3.8 s/página DPI150, CER ~0.03), pendiente de re-ejecutar con corpus A4 corregido.

## Próximos pasos
Completar POC, documentar métricas finales, implementar worker con los parámetros validados.
