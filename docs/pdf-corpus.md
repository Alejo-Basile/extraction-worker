# Corpus de PDFs patologicos y hostiles (S5-P3-05)

Los fixtures se generan en `crates/worker/tests/pdf_corpus.rs` para que el
corpus sea reproducible y no versionar binarios. Cada caso tiene un test que
fija la clasificación y el destino.

| Caso | Fixture generado | Clasificación / destino | Test |
|---|---|---|---|
| 0 bytes | archivo vacío | `PDF_CORRUPT`, negocio → DLQ | `pdf_vacio_y_no_pdf_son_corruptos` |
| No-PDF con cabecera faltante | texto plano | `PDF_CORRUPT`, negocio → DLQ | `pdf_vacio_y_no_pdf_son_corruptos` |
| Escaneado sin capa de texto | página con content stream vacío | `PDF_NO_TEXT`, negocio → DLQ | `pdf_escaneado_sin_texto_es_error_de_negocio` |
| Cifrado | PDF real cifrado con lopdf | `PDF_ENCRYPTED`, negocio → DLQ | `pdf_cifrado_es_error_de_negocio` |
| Gigante | objeto mayor a `WORKER_MAX_PDF_BYTES` | `PDF_TOO_LARGE`, negocio → DLQ antes de extraer | `objeto_gigante_corta_por_tamano_antes_de_extraer` |
| Miles de páginas | 1200 páginas con tope 1000 | `PDF_TOO_MANY_PAGES`, negocio → DLQ | `pdf_con_miles_de_paginas_muere_en_el_conteo_previo` |
| JavaScript embebido | `/OpenAction` con `/S /JavaScript` | se ignora; se extrae el texto normal | `pdf_con_javascript_embebido_no_se_ejecuta_ni_cuelga` |
| Fuente malformada | fuente con referencia ausente | `PDF_CORRUPT` si falla; no panic ni cuelgue | `pdf_con_fuente_malformada_no_cuelga_ni_paniquea` |
| Referencias cíclicas | árbol `/Pages` que se apunta a sí mismo | `PDF_NO_TEXT` (negocio → DLQ); no panic ni cuelgue | `pdf_con_referencias_ciclicas_no_cuelga_ni_paniquea` |
| Truncado sin `xref` | PDF recortado antes de la tabla | `PDF_CORRUPT`/`PDF_NO_TEXT` (negocio → DLQ) o reparación tolerante; no panic ni cuelgue | `pdf_truncado_sin_xref_no_cuelga_ni_paniquea` |
| `startxref` inválido | offset fuera del archivo | `PDF_CORRUPT`/`PDF_NO_TEXT` o reparación; no cuelgue | `pdf_con_startxref_fuera_de_rango_no_cuelga` |
| `/Count` gigante | `100000000` páginas declaradas con un solo `/Kids` | no preasigna por el número declarado; extrae el kid real o falla acotado | `pdf_con_count_de_paginas_gigante_no_preasigna` |
| `/Length` de stream mentiroso | stream declara 10 MB y entrega pocos bytes | `PDF_CORRUPT`/`PDF_NO_TEXT` o tolerancia; no cuelgue | `pdf_con_stream_declarado_gigante_pero_corto_no_cuelga` |
| Archivo adjunto embebido | `/EmbeddedFiles` con payload ejecutable | se ignora; solo se extrae el texto visible | `pdf_con_archivo_adjunto_no_extrae_el_payload` |
| Acción `/Launch` | `/OpenAction` con `calc.exe`/`cmd.exe` | se ignora; no se ejecuta ni se filtra | `pdf_con_accion_launch_no_se_ejecuta` |
| Hostil con trabajo lento | extractor lento en el pipeline | `JOB_TIMEOUT`, transient → mensaje queda pendiente | `job_timeout_devuelve_transitorio` |
| Liveness con hostil | job en timeout | `/healthz` sigue respondiendo | `healthz_responde_durante_timeout_de_job` |

Notas:

- `WORKER_JOB_TIMEOUT_SECS` es la última línea de defensa: no mata el hilo
  CPU-bound, pero devuelve el fallo a tiempo, suelta el permiso del bulkhead y
  deja el mensaje pendiente para `XAUTOCLAIM`.
- Un panic del parser se convierte en `PDF_CORRUPT` para no convertir un PDF
  malformado en un fallo de infraestructura reintentable.
