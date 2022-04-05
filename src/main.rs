use std::{
    collections::{HashMap, HashSet},
    fs::{copy, create_dir_all},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::exit,
    thread
};

use anyhow::{bail, Result};
use chrono::Local;
use clap::Parser;
use cron::Schedule;
use log::log_enabled;
use rayon::{prelude::*, ThreadPoolBuilder};
use walkdir::WalkDir;

fn main() {
    let cli = Cli::parse();
    exit(cli.run().map_or_else(
        |e| {
            eprintln!("failed to run: {}", e);
            1
        },
        |_| 0,
    ));
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(short, long)]
    from: Vec<PathBuf>,

    #[clap(short, long)]
    to: PathBuf,

    #[clap(short, long, parse(from_occurrences))]
    verbose: u8,

    #[clap(short, long)]
    parallel_threads: Option<NonZeroUsize>,

    #[clap(short, long, parse(try_from_str = parse_cron))]
    cron_expr: Option<Schedule>,
}

fn parse_cron(s: &str) -> Result<Schedule> {
    s.parse().map_err(Into::into)
}

impl Cli {
    fn run(&self) -> Result<()> {
        self.check()?;

        if let Some(num) = self.parallel_threads {
            ThreadPoolBuilder::new()
                .num_threads(usize::from(num))
                .build_global()?;
        }

        if let Some(s) = self.cron_expr.as_ref() {
            log::debug!("parsing cron expression: {}", s);

            for date in s.upcoming(Local) {
                let now = Local::now();
                if date > now {
                    let dura = date - now;
                    log::info!("waiting {} for next date time: {}", dura, date);
                    println!("等待 {} 秒直到 {} 开始执行", dura.num_seconds(), date);
                    thread::sleep(dura.to_std()?);
                }
                let start = Local::now();
                println!(
                    "开始从 {} 复制到 {}",
                    self.from
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    self.to.display()
                );

                try_copy(&self.from, &self.to)?;

                println!("复制完成，用时：{}", Local::now() - start);
            }
        } else {
            try_copy(&self.from, &self.to)?;
        }
        Ok(())
    }

    fn check(&self) -> Result<()> {
        let verbose = self.verbose;
        if verbose > 4 {
            bail!("invalid arg: 4 < {} number of verbose", verbose);
        }
        let level: log::LevelFilter = unsafe { std::mem::transmute((verbose + 1) as usize) };
        env_logger::builder()
            .filter_level(log::LevelFilter::Error)
            .filter_module(env!("CARGO_CRATE_NAME"), level)
            .init();

        if self.from.iter().collect::<HashSet<_>>().len() != self.from.len() {
            bail!("duplicated paths: {:?}", self.from);
        }
        for p in &self.from {
            if !p.exists() {
                bail!("path {} does not exist", p.display());
            }
        }
        if !self.to.exists() {
            log::info!("creating to target path: {}", self.to.display());
            create_dir_all(&self.to)?;
        } else if !self.to.is_dir() {
            bail!(
                "directory {} does not exist, please create a directory",
                self.to.display()
            );
        }
        Ok(())
    }
}

fn try_copy<P: AsRef<Path>>(from: &[P], to: &P) -> Result<()> {
    let (from, to) = (
        from.iter()
            .map(|p| p.as_ref().canonicalize())
            .collect::<Result<Vec<_>, _>>()?,
        to.as_ref().canonicalize()?,
    );
    log::trace!("try copy from {:?} to {}", from, to.display());

    // find all items in from and to folders
    let from_items = from
        .iter()
        .filter_map(|p| {
            walk_items(p)
                .map(|paths| (p.to_path_buf(), paths))
                .map_err(|e| log::warn!("failed to walk path `{}`: {}", p.display(), e))
                .ok()
        })
        .collect::<HashMap<_, _>>();

    let froms = from_items
        .values()
        .flat_map(|set| set.iter())
        .collect::<HashSet<_>>();
    if log_enabled!(log::Level::Info) {
        let size = froms
            .iter()
            .flat_map(|p| p.metadata().map(|data| data.len()))
            .sum::<u64>();
        log::info!(
            "found {} items in from: {:?}. size: {}MB",
            froms.len(),
            from,
            size as f64 / (1024 * 1024) as f64
        );
    }

    let to_items = walk_items(&to)?;
    log::debug!("found {} items in to: {}", to_items.len(), to.display());

    // compare from and find items that dont exist in to
    let from_tos = from_items
        .iter()
        // get pair of base,from_item
        .flat_map(|(base, items)| items.iter().map(move |item| (base, item)))
        // get to_item
        .map(|(base, from)| {
            from.strip_prefix(base)
                .map(|suffix| (from, to.join(suffix)))
        })
        // filter items
        .filter(|res| {
            res.as_ref()
                .map_or(true, |(_, to_target)| !to_items.contains(to_target))
        })
        .collect::<Result<Vec<_>, _>>()?;

    if log_enabled!(log::Level::Info) {
        let len = from_tos
            .iter()
            .flat_map(|(from, _)| from.metadata().map(|data| data.len()))
            .sum::<u64>();
        log::info!(
            "trying parallel copy {} items {}MB from `{:?}` to {}",
            from_tos.len(),
            len as f64 / (1024 * 1024) as f64,
            from,
            to.display()
        );
    }

    // copy parallel
    // let files = from_tos.iter().try_fold(0, |acc, (from, to)| {
    //     if to.exists() {
    //         log::debug!("skipped existing file {}", to.display());
    //         return Ok(acc);
    //     } else if let Some(p) = to.parent().filter(|p| !p.exists()) {
    //         log::info!("creating directories {} for {}", p.display(), to.display());
    //         create_dir_all(p)?;
    //     }

    //     log::trace!("copying from `{}` to `{}`", from.display(), to.display());
    //     copy(from, to).map(|_| acc + 1)
    // })?;
    from_tos.par_iter().try_for_each(|(from, to)| {
        if to.exists() {
            log::warn!("skipped existing file {}", to.display());
            return Ok(());
        } else if let Some(p) = to.parent().filter(|p| !p.exists()) {
            log::debug!("creating directories {} for {}", p.display(), to.display());
            create_dir_all(p)?;
        }
        log::trace!("copying from `{}` to `{}`", from.display(), to.display());
        copy(from, to).map(|_| ())
    })?;
    Ok(())
}

fn walk_items(path: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
    WalkDir::new(path)
        .into_iter()
        .par_bridge()
        .map(|dir| dir.map(|p| p.path().to_path_buf()))
        .filter(|dir| dir.as_ref().map_or(true, |en| en.is_file()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use once_cell::sync::Lazy;
    use tempfile::{tempdir, TempDir};

    use rand::prelude::*;

    use super::*;
    use std::{
        env,
        fs::{create_dir, write},
        sync::Once,
    };

    static INIT: Once = Once::new();
    static TEMP_DIRS: Lazy<(TempDir, Vec<PathBuf>)> = Lazy::new(|| {
        let dir = tempdir().unwrap();
        let paths = temp(dir.path()).unwrap();
        (dir, paths)
    });

    #[ctor::ctor]
    fn init() {
        INIT.call_once(|| {
            env_logger::builder()
                .is_test(true)
                .filter_level(log::LevelFilter::Error)
                .filter_module(env!("CARGO_CRATE_NAME"), log::LevelFilter::Trace)
                .init();
        });
    }

    fn temp(dir: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
        let mut rng = rand::thread_rng();

        let root = dir.as_ref().to_path_buf();
        log::debug!("test temp walk dir: {}", root.display());

        let (loop_count, per_files, per_dirs) = (6, 8, 10);
        let (mut file_count, mut dir_count, mut empty_dir_count) = (0, 0, 0);
        let mut dirs = vec![root];
        let mut file_paths = vec![];
        for i in 0..loop_count {
            let mut new_dirs = vec![];

            for path in &dirs {
                for j in 0..per_files {
                    let file = path.join(format!("test_{}_{}.txt", i, j));
                    write(&file, format!("test file for {}_{}", i, j))?;
                    file_paths.push(file);
                    file_count += 1;
                }

                for j in 0..per_dirs {
                    let path = path.join(format!("test_dir_{}_{}", i, j));
                    create_dir(&path)?;
                    dir_count += 1;
                    // empty
                    if random() {
                        new_dirs.push(path);
                    } else {
                        empty_dir_count += 1;
                    }
                }
            }
            if new_dirs.is_empty() {
                new_dirs.push(dirs[rng.gen_range(0..dirs.len())].clone());
            }
            dirs = new_dirs;
        }
        log::debug!(
            "create {} files and {} dirs, {} empty dirs",
            file_count,
            dir_count,
            empty_dir_count
        );
        Ok(file_paths)
    }

    #[test]
    fn test_walk() -> Result<()> {
        let (dir, file_paths) = &*TEMP_DIRS;
        let items = walk_items(dir.path())?;
        assert_eq!(items.len(), file_paths.len());
        assert_eq!(
            items.iter().collect::<HashSet<_>>(),
            file_paths.iter().collect::<HashSet<_>>()
        );
        Ok(())
    }

    #[test]
    fn test_try_copy() -> Result<()> {
        let to_dir = tempdir()?;
        let (from_dir, from_files) = &*TEMP_DIRS;

        try_copy(&[from_dir.path()], &to_dir.path())?;

        let to_files = walk_items(to_dir.path())?;
        assert_eq!(from_files.len(), to_files.len());

        assert_eq!(
            from_files
                .iter()
                .map(|p| p.strip_prefix(from_dir.path()))
                .collect::<Result<HashSet<_>, _>>(),
            to_files
                .iter()
                .map(|p| p.strip_prefix(to_dir.path()))
                .collect::<Result<HashSet<_>, _>>()
        );
        Ok(())
    }
}
