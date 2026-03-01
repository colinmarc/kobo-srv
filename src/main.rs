use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, mpsc};

use anyhow::{Context as _, bail};
use base64::Engine;
use clap::Parser;
use epub::doc::EpubDoc;
use notify::{RecursiveMode, Watcher};
use serde::Serialize;
use tiny_http::{Header, Request, Response, SslConfig};
use uuid::Uuid;

// Uuid::new_v5(&Uuid::NAMESPACE_URL, b"colinmarc/kobo-srv")
const NAMESPACE: Uuid = uuid::uuid!("32536825-6dfc-5597-b4cb-4c896cdd8c4e");

const KOBO_SYNC_TOKEN: http::HeaderName = http::HeaderName::from_static("x-kobo-synctoken");
const CONTENT_TYPE_JSON: http::HeaderValue =
    http::HeaderValue::from_static("application/json; charset=utf-8");
const CONTENT_TYPE_EPUB: http::HeaderValue = http::HeaderValue::from_static("application/epub+zip");

#[derive(Debug, Parser)]
#[command(name = "kobo-srv")]
#[command(about = "Kobo store emulator")]
struct Cli {
    /// Directory of epub files to serve.
    dir: PathBuf,
    /// The address to bind. Defaults to 0.0.0.0:8080.
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: SocketAddr,
    /// The address to use in responses. This will be used to e.g. construct
    /// download URLs for books, and should match the api_endpoint you
    /// configured on your kobo.
    #[arg(long)]
    external_url: Option<http::Uri>,
    /// Path to TLS certificate (PEM).
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<PathBuf>,
    /// Path to TLS private key (PEM).
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,
}

struct Book {
    path: PathBuf,
    size: u64,
    created: chrono::DateTime<chrono::Utc>,
    modified: chrono::DateTime<chrono::Utc>,
    title: String,
    author: String,
    language: Option<String>,
    publisher: Option<String>,
    date: Option<String>,
    description: Option<String>,
    cover: Option<(Vec<u8>, String)>,
}

struct Server {
    books: RwLock<BTreeMap<Uuid, Book>>,
    salt: String,
    external_uri: http::Uri,
}

impl Server {
    fn new(external_url: http::Uri) -> Self {
        // We "salt" our continuation tokens so that we treat a continuation
        // token from another instance of the server as a fresh sync.
        let salt = format!("{:08x}", rand::random::<u32>());

        Self {
            books: RwLock::new(BTreeMap::new()),
            salt,
            external_uri: external_url,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
enum SyncItem<'a> {
    NewEntitlement(Entitlement<'a>),
    ChangedEntitlement(Entitlement<'a>),
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct Entitlement<'a> {
    book_entitlement: BookEntitlement<'a>,
    book_metadata: BookMetadata<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct BookEntitlement<'a> {
    accessibility: &'a str,
    created: chrono::DateTime<chrono::Utc>,
    cross_revision_id: Uuid,
    id: Uuid,
    is_removed: bool,
    last_modified: chrono::DateTime<chrono::Utc>,
    revision_id: Uuid,
    status: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct BookMetadata<'a> {
    #[serde(serialize_with = "serialize_one")]
    contributor_roles: ContributorRole<'a>,
    cover_image_id: Uuid,
    cross_revision_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<&'a str>,
    #[serde(serialize_with = "serialize_one")]
    download_urls: DownloadUrl<'a>,
    entitlement_id: Uuid,
    language: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    publication_date: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    publisher: Option<Publisher<'a>>,
    revision_id: Uuid,
    title: &'a str,
    work_id: Uuid,
}

impl<'a> BookMetadata<'a> {
    fn new(id: Uuid, book: &'a Book, external_url: &http::Uri) -> Self {
        let base = external_url.to_string();
        let base = base.trim_end_matches('/');
        let download_url = format!("{base}/download/{id}");

        BookMetadata {
            contributor_roles: ContributorRole { name: &book.author },
            cover_image_id: id,
            cross_revision_id: id,
            description: book.description.as_deref(),
            download_urls: DownloadUrl {
                format: "EPUB3",
                size: book.size,
                url: download_url,
                platform: "Generic",
            },
            entitlement_id: id,
            language: book.language.as_deref().unwrap_or("en"),
            publication_date: book.date.as_deref(),
            publisher: book.publisher.as_deref().map(|name| Publisher { name }),
            revision_id: id,
            title: &book.title,
            work_id: id,
        }
    }
}

fn serialize_one<T: Serialize, S: serde::Serializer>(val: &T, s: S) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq;
    let mut seq = s.serialize_seq(Some(1))?;
    seq.serialize_element(val)?;
    seq.end()
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct Publisher<'a> {
    name: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct ContributorRole<'a> {
    name: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct DownloadUrl<'a> {
    format: &'a str,
    size: u64,
    url: String,
    platform: &'a str,
}

fn find_header<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    req.headers()
        .iter()
        .find(|h| h.field.as_str().as_str().eq_ignore_ascii_case(name))
        .map(|h| h.value.as_str())
}

fn json_response(value: impl Serialize) -> anyhow::Result<Response<io::Cursor<Vec<u8>>>> {
    let body = serde_json::to_vec(&value).expect("serialization failed");
    Ok(Response::from_data(body).with_header(
        Header::from_bytes(
            http::header::CONTENT_TYPE.as_str(),
            CONTENT_TYPE_JSON.as_bytes(),
        )
        .unwrap(),
    ))
}

fn err_response(status: u16, msg: &str) -> Response<io::Cursor<Vec<u8>>> {
    Response::from_string(msg).with_status_code(status)
}

type Handler = fn(&Server, Request, matchit::Params) -> anyhow::Result<()>;

fn decode_sync_token(req: &Request, salt: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let raw = find_header(req, KOBO_SYNC_TOKEN.as_str())?;
    let bytes = base64::engine::general_purpose::STANDARD.decode(raw).ok()?;

    let ts = std::str::from_utf8(&bytes)
        .ok()?
        .strip_prefix(salt)?
        .strip_prefix(':')?;
    ts.parse::<chrono::DateTime<chrono::Utc>>().ok()
}

fn encode_sync_token(salt: &str, t: &chrono::DateTime<chrono::Utc>) -> String {
    let payload = format!("{salt}:{}", t.to_rfc3339());
    base64::engine::general_purpose::STANDARD.encode(payload.as_bytes())
}

fn handle_sync(server: &Server, req: Request, _params: matchit::Params) -> anyhow::Result<()> {
    let since = decode_sync_token(&req, &server.salt);
    let now = chrono::Utc::now();

    let books = server.books.read().unwrap();
    let mut items: Vec<SyncItem> = Vec::new();
    let mut count_new = 0;
    let mut count_changed = 0;
    for (&id, book) in books.iter() {
        let is_new = match since {
            None => true,
            Some(since) if book.created > since => true,
            Some(since) if book.modified > since => false,
            _ => continue,
        };

        let entitlement = Entitlement {
            book_entitlement: BookEntitlement {
                accessibility: "Full",
                created: book.created,
                cross_revision_id: id,
                id,
                is_removed: false,
                last_modified: book.modified,
                revision_id: id,
                status: "Active",
            },
            book_metadata: BookMetadata::new(id, book, &server.external_uri),
        };

        if is_new {
            items.push(SyncItem::NewEntitlement(entitlement));
            count_new += 1;
        } else {
            items.push(SyncItem::ChangedEntitlement(entitlement));
            count_changed += 1;
        }
    }

    tracing::debug!(new = count_new, changed = count_changed, "syncing");

    let token = encode_sync_token(&server.salt, &now);
    let resp = json_response(items)?
        .with_header(Header::from_bytes(KOBO_SYNC_TOKEN.as_str(), token.as_bytes()).unwrap());
    Ok(req.respond(resp)?)
}

fn handle_metadata(server: &Server, req: Request, params: matchit::Params) -> anyhow::Result<()> {
    let id: Uuid = params.get("id").unwrap().parse()?;

    let books = server.books.read().unwrap();
    let resp = match books.get(&id) {
        Some(book) => {
            let metadata = BookMetadata::new(id, book, &server.external_uri);
            json_response([metadata])?
        }
        None => err_response(404, "not found"),
    };

    Ok(req.respond(resp)?)
}

fn handle_image(server: &Server, req: Request, params: matchit::Params) -> anyhow::Result<()> {
    let id: Uuid = params.get("id").unwrap().parse()?;

    let books = server.books.read().unwrap();
    let cover = books.get(&id).and_then(|book| book.cover.as_ref());

    let resp = match cover {
        Some((data, mime)) => Response::from_data(data.clone()).with_header(
            Header::from_bytes(http::header::CONTENT_TYPE.as_str(), mime.as_bytes()).unwrap(),
        ),
        None => err_response(404, "not found"),
    };
    Ok(req.respond(resp)?)
}

fn handle_download(server: &Server, req: Request, params: matchit::Params) -> anyhow::Result<()> {
    let id: Uuid = params.get("id").unwrap().parse()?;

    let books = server.books.read().unwrap();
    let book = match books.get(&id) {
        Some(b) => b,
        None => {
            req.respond(err_response(404, "not found"))?;
            return Ok(());
        }
    };

    let file = File::open(&book.path)?;
    tracing::info!(id = %id, title = %book.title, "serving download");

    req.respond(
        Response::from_file(file).with_header(
            Header::from_bytes(
                http::header::CONTENT_TYPE.as_str(),
                CONTENT_TYPE_EPUB.as_bytes(),
            )
            .unwrap(),
        ),
    )?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    if !cli.dir.is_dir() {
        bail!("{} is not a directory", cli.dir.display());
    }

    let scheme = if cli.tls_cert.is_some() && cli.tls_key.is_some() {
        "https"
    } else {
        "http"
    };

    let external_uri = match cli.external_url {
        Some(url) => url,
        None => format!("{scheme}://{}", cli.bind).parse()?,
    };

    let server = Arc::new(Server::new(external_uri.clone()));
    let dir = cli.dir.clone();
    let server_clone = server.clone();
    std::thread::spawn(move || {
        if let Err(err) = watch_directory(dir, &server_clone) {
            tracing::error!(%err, "watcher failed");
            std::process::exit(1);
        }
    });

    let http = if let Some(cert_path) = cli.tls_cert
        && let Some(key_path) = cli.tls_key
    {
        let cert = std::fs::read(&cert_path).context("Failed to read TLS certificate")?;
        let key = std::fs::read(&key_path).context("Failed to read TLS private key")?;

        tiny_http::Server::https(
            cli.bind,
            SslConfig {
                certificate: cert,
                private_key: key,
            },
        )
    } else {
        tiny_http::Server::http(cli.bind)
    }
    .map_err(|e| anyhow::anyhow!("Failed to start server: {}", e))?;

    let base_path = external_uri.path();

    let mut get = matchit::Router::new();
    get.insert("/v1/library/sync", handle_sync as Handler)?;
    get.insert("/v1/library/{id}/metadata", handle_metadata as Handler)?;
    get.insert("/image/{id}/{*rest}", handle_image as Handler)?;
    get.insert("/download/{id}", handle_download as Handler)?;

    tracing::info!("listening on {scheme}://{}", cli.bind);

    for req in http.incoming_requests() {
        let Ok(uri) = req.url().parse::<http::Uri>() else {
            tracing::warn!("failed to parse url: {}", req.url());
            continue;
        };

        let path = uri.path();
        tracing::info!("{} {}", req.method(), path);

        let path = path
            .strip_prefix(base_path.strip_suffix("/").unwrap_or(base_path))
            .unwrap_or(path);

        let result = match get.at(path).ok() {
            Some(m) => (m.value)(&server, req, m.params),
            None => {
                tracing::debug!(path = req.url(), "unhandled path");
                let _ = req.respond(json_response(serde_json::json!({}))?);
                Ok(())
            }
        };

        if let Err(err) = result {
            tracing::error!(%err, "handler error");
        }
    }

    Ok(())
}

fn watch_directory(dir: PathBuf, server: &Server) -> anyhow::Result<()> {
    let books = scan_directory(&dir);
    tracing::info!(count = books.len(), "initial scan complete");
    *server.books.write().unwrap() = books;

    let lib_books = &server.books;
    let d = dir.clone();

    let (tx, rx) = mpsc::channel();
    let mut watcher = notify::recommended_watcher(tx).context("failed to create watcher")?;

    watcher
        .watch(&dir, RecursiveMode::NonRecursive)
        .context(format!("failed to watch {}", dir.display()))?;

    for _ in rx {
        let books = scan_directory(&d);
        tracing::info!(count = books.len(), "rescan complete");

        *lib_books.write().unwrap() = books;
    }

    Ok(())
}

fn scan_directory(dir: &Path) -> BTreeMap<Uuid, Book> {
    let mut books = BTreeMap::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(err) => {
            tracing::error!(%err, "failed to read directory");
            return books;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("epub"))
        {
            match scan_epub(&path) {
                Ok((id, book)) => {
                    books.insert(id, book);
                }
                Err(err) => tracing::error!(%err, "failed to parse epub"),
            }
        }
    }

    books
}

fn scan_epub(path: &Path) -> anyhow::Result<(Uuid, Book)> {
    let meta = std::fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    let size = meta.len();
    let created: chrono::DateTime<chrono::Utc> =
        meta.created().unwrap_or(std::time::UNIX_EPOCH).into();
    let modified: chrono::DateTime<chrono::Utc> =
        meta.modified().unwrap_or(std::time::UNIX_EPOCH).into();
    let mut doc = EpubDoc::new(path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;

    // Use the epub's dc:identifier for a stable UUID, falling back to
    // the relative path if absent.
    let id = match doc.mdata("identifier") {
        Some(m) => &m.value,
        None => {
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                bail!("empty or invalid filename");
            };

            stem
        }
    };
    let id = Uuid::new_v5(&NAMESPACE, id.as_bytes());

    let title = doc
        .mdata("title")
        .map(|m| m.value.clone())
        .unwrap_or_else(|| {
            path.file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned()
        });

    let author = doc
        .mdata("creator")
        .map(|m| m.value.clone())
        .unwrap_or_default();

    let language = doc.mdata("language").map(|m| m.value.clone());
    let publisher = doc.mdata("publisher").map(|m| m.value.clone());
    let date = doc.mdata("date").map(|m| m.value.clone());
    let description = doc.mdata("description").map(|m| m.value.clone());
    let cover = doc.get_cover();

    tracing::info!(%id, %title, %author, "found book");

    Ok((
        id,
        Book {
            path: path.to_owned(),
            size,
            created,
            modified,
            title,
            author,
            language,
            publisher,
            date,
            description,
            cover,
        },
    ))
}
