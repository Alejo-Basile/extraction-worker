//! Lectura de los gauges de pids del cgroup (H-03).
//!
//! El panel de recursos compara `worker_pids_current` contra
//! `worker_pids_limit`; ambos salen de los archivos `pids.current` y
//! `pids.max` del cgroup del contenedor. Se soportan cgroup v2 (raiz
//! plana) y v1 (subdirectorio `pids/`). Cualquier archivo ausente,
//! ilegible o no numerico se traduce en `None`: el gauge nunca debe
//! inventar un valor.

use std::path::{Path, PathBuf};

/// Uso de pids del cgroup: valor actual y limite (`None` = sin limite).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PidsUsage {
    pub current: u64,
    pub limit: Option<u64>,
}

/// Raiz del cgroup: `WORKER_CGROUP_ROOT` si esta presente y no vacia; si
/// no, `/sys/fs/cgroup`.
pub fn cgroup_root() -> PathBuf {
    match std::env::var("WORKER_CGROUP_ROOT") {
        Ok(v) if !v.trim().is_empty() => PathBuf::from(v),
        _ => PathBuf::from("/sys/fs/cgroup"),
    }
}

/// Lee el usage de pids de la raiz dada. `None` si no se puede leer un
/// par v2 ni v1 completo.
pub fn read_pids_usage(root: &Path) -> Option<PidsUsage> {
    read_v2(root).or_else(|| read_v1(root))
}

/// cgroup v2: `pids.max` y `pids.current` en la raiz.
fn read_v2(root: &Path) -> Option<PidsUsage> {
    read_pair(&root.join("pids.max"), &root.join("pids.current"))
}

/// cgroup v1: los mismos archivos dentro del subdirectorio `pids/`.
fn read_v1(root: &Path) -> Option<PidsUsage> {
    read_pair(
        &root.join("pids").join("pids.max"),
        &root.join("pids").join("pids.current"),
    )
}

/// Completa el par solo si `pids.current` es numerico y `pids.max` esta
/// presente y es numerico o el literal `max`.
fn read_pair(max_path: &Path, current_path: &Path) -> Option<PidsUsage> {
    let current = parse_u64(current_path)?;
    let limit = parse_limit(max_path)?;
    Some(PidsUsage { current, limit })
}

fn parse_u64(path: &Path) -> Option<u64> {
    let raw = std::fs::read_to_string(path).ok()?;
    raw.trim().parse::<u64>().ok()
}

/// `None` externo: archivo ausente o no numerico. `Some(None)`: limite
/// `max` (sin limite). `Some(Some(n))`: limite numerico.
fn parse_limit(path: &Path) -> Option<Option<u64>> {
    let raw = std::fs::read_to_string(path).ok()?;
    let raw = raw.trim();
    if raw == "max" {
        return Some(None);
    }
    raw.parse::<u64>().ok().map(Some)
}

/// Usage de pids de la raiz configurada (`WORKER_CGROUP_ROOT` o
/// `/sys/fs/cgroup`).
pub fn pids_usage() -> Option<PidsUsage> {
    read_pids_usage(&cgroup_root())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Directorio fixture unico bajo `temp_dir()`, nunca dentro del
    /// worktree; cada prueba lo borra al terminar.
    fn fixture_dir(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "worker-cgroup-{tag}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("no se pudo crear {dir:?}: {e}"));
        dir
    }

    fn write_file(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("no se pudo crear {parent:?}: {e}"));
        }
        std::fs::write(path, contents)
            .unwrap_or_else(|e| panic!("no se pudo escribir {path:?}: {e}"));
    }

    fn cleanup(dir: &Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn v2_con_limite_numerico_devuelve_ambos() {
        let dir = fixture_dir("v2-numerico");
        write_file(&dir.join("pids.max"), "200\n");
        write_file(&dir.join("pids.current"), "7\n");
        assert_eq!(
            read_pids_usage(&dir),
            Some(PidsUsage {
                current: 7,
                limit: Some(200)
            })
        );
        cleanup(&dir);
    }

    #[test]
    fn v2_con_limite_max_devuelve_none() {
        let dir = fixture_dir("v2-max");
        write_file(&dir.join("pids.max"), "max\n");
        write_file(&dir.join("pids.current"), "3\n");
        assert_eq!(
            read_pids_usage(&dir),
            Some(PidsUsage {
                current: 3,
                limit: None
            })
        );
        cleanup(&dir);
    }

    #[test]
    fn v1_con_directorio_pids_devuelve_ambos() {
        let dir = fixture_dir("v1");
        write_file(&dir.join("pids").join("pids.max"), "500\n");
        write_file(&dir.join("pids").join("pids.current"), "12\n");
        assert_eq!(
            read_pids_usage(&dir),
            Some(PidsUsage {
                current: 12,
                limit: Some(500)
            })
        );
        cleanup(&dir);
    }

    #[test]
    fn archivos_ausentes_devuelven_none_sin_paniquear() {
        let dir = fixture_dir("ausentes");
        assert_eq!(read_pids_usage(&dir), None);
        // Solo `pids.current`, sin `pids.max`: tampoco se inventa el limite.
        write_file(&dir.join("pids.current"), "1\n");
        assert_eq!(read_pids_usage(&dir), None);
        cleanup(&dir);
    }

    #[test]
    fn contenido_no_numerico_devuelve_none_sin_paniquear() {
        let dir = fixture_dir("no-numerico");
        write_file(&dir.join("pids.max"), "200\n");
        write_file(&dir.join("pids.current"), "muchos\n");
        assert_eq!(read_pids_usage(&dir), None);
        write_file(&dir.join("pids.current"), "7\n");
        write_file(&dir.join("pids.max"), "ilimitado\n");
        assert_eq!(read_pids_usage(&dir), None);
        cleanup(&dir);
    }

    #[test]
    fn los_gauges_de_pids_aparecen_en_el_exporter() -> Result<(), String> {
        let handle = crate::telemetry::prometheus_handle()?;
        metrics::gauge!("worker_pids_current").set(7.0);
        metrics::gauge!("worker_pids_limit").set(200.0);
        let rendered = handle.render();
        assert!(
            rendered.contains("worker_pids_current"),
            "el exporter debe contener worker_pids_current: {rendered}"
        );
        assert!(
            rendered.contains("worker_pids_limit"),
            "el exporter debe contener worker_pids_limit: {rendered}"
        );
        Ok(())
    }
}
