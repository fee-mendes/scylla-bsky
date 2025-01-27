use std::sync::Arc;
use tokio::time::Duration;
use futures::{pin_mut, StreamExt};
use std::env;
use std::collections::HashMap;
use chrono::{DateTime, Utc}; 
use skystreamer::{stream::EventStream, RepoSubscription};
use skystreamer::types::Post;
use skystreamer::types::feed::LikeEvent;
use skystreamer::types::actor::Profile;
use skystreamer::types::Embed::Media;
use skystreamer::types::Embed::External;
use skystreamer::types::Embed::Record;
use scylla::SerializeCql;
use scylla::SerializeRow;
use scylla::prepared_statement::PreparedStatement;
use scylla::statement::Consistency;
use scylla::transport::ExecutionProfile;
use scylla::transport::Compression;
use scylla::load_balancing::DefaultPolicy;
use scylla::{Session, SessionBuilder};

#[derive(Debug, SerializeCql)]
struct AvatarUDT {
    cid: Option<String>,
    mime_type: Option<String>,
    size: Option<i64>,
}

#[derive(Debug, SerializeCql)]
struct Reply {
    parent: Option<String>,
    root: Option<String>,
}

#[derive(Debug, SerializeCql)]
struct EmbedBlob {
    cid: String,
    mime_type: String,
    size: Option<i64>,
}

#[derive(Debug, SerializeCql)]
struct EmbedMedia {
    kind: String,
    alt: Option<String>,
    blob: EmbedBlob,
    aspect_ratio: Option<HashMap<String, i32>>
}

#[derive(Debug, SerializeCql)]
struct ExternalRef {
    description: String,
    thumb: Option<EmbedBlob>,
    title: String,
    uri: String,
}

#[derive(Debug, SerializeCql)]
struct Embeddings {
    media: Option<Vec<EmbedMedia>>,
    external: Option<ExternalRef>,
    record: Option<String>,
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
   // Connect to a ScyllaDB cluster
   let args: Vec<String> = env::args().collect();

   let mut host = "127.0.0.1";
   let dc = "datacenter1";
   let usr = "scylla";
   let pwd = "scylla";
   let ks = "social";

   match args.len() {
       1 => {
           println!("Using default values. Host: {}, DC: {}, Username: {}, Password: ********", host, dc, usr);
       }
       _ => {
           host = &args[1];
       }
   }

   println!("Connecting to {} ...", host);
   let default_policy = DefaultPolicy::builder()
       .prefer_datacenter(dc.to_string())
       .token_aware(true)
       .permit_dc_failover(false)
       .build();

   let profile = ExecutionProfile::builder()
       .load_balancing_policy(default_policy)
       .build();

   let handle = profile.into_handle();

   let session: Session = SessionBuilder::new()
       .known_node(host)
       .schema_agreement_interval(Duration::from_secs(5))
       .auto_await_schema_agreement(true)
       .default_execution_profile_handle(handle)
       .compression(Some(Compression::Lz4))
       .user(usr, pwd)
       .use_keyspace(ks, false)
       .build()
       .await?;
   let session = Arc::new(session);

   println!("Connected successfully! Policy: TokenAware(DCAware())");

   // Create KS and Table
   let ks_stmt = format!("CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'NetworkTopologyStrategy', '{}': 1}}", ks, dc);
   session.query(ks_stmt, &[]).await?;

   // Create subscription to bsky.network
   let subscription = RepoSubscription::new("bsky.network").await.unwrap();

   let mut binding = EventStream::new(subscription);
   let event_stream = binding.stream().await?;

   // let commit_stream = subscription.stream_commits().await;
   pin_mut!(event_stream);

   while let Some(record) = event_stream.next().await {
       match record {
           skystreamer::types::commit::Record::Profile(val) => 
               handle_profile(val, session.clone()).await,
           skystreamer::types::commit::Record::Like(val) =>
               handle_like(val, session.clone()).await,
           skystreamer::types::commit::Record::Post(val) =>
               handle_post(val, session.clone()).await,
           // For the purpose of this exercise we do not care much about
           // the rest, see https://docs.rs/skystreamer/0.2.2/skystreamer/types/commit/enum.Record.html
           // to implement other functionality.

           _ => (), // This typically would be todo!() but it panics.
       }
   }

   // Just exit if the stream stops reporting
   println!("Exitting normally");

   Ok(())
}

async fn handle_profile(val: Box<Profile>, session: Arc<Session>) {
    println!("{:?}", val);
    let avatar = if let Some(x) = val.avatar {
        AvatarUDT{ 
            cid: Some(x.clone().cid), 
            mime_type: Some(x.clone().mime_type), 
            size: Some(x.size.unwrap() as i64) }
    } else {
        AvatarUDT{
            cid: None,
            mime_type: None,
            size: None }
    };
    
    let created_at: Option<DateTime<Utc>> = if let Some(x) = val.created_at {
        Some(x.with_timezone(&Utc))
    } else {
        None
    };
    

    let pinned_post = if let Some(x) = val.pinned_post {
        Some(x.to_string())
    } else {
        None
    };

    let stmt = format!(
        "INSERT INTO profile (
        did, avatar, created_at, description, display_name, labels, pinned_post
        ) VALUES (?, ?, ?, ?, ?, ?, ?)"
    );
     
    let mut ps: PreparedStatement = session.prepare(stmt).await.expect("Prepared statement failed");
    ps.set_consistency(Consistency::LocalQuorum);
    session.execute(&ps, 
        (val.did.as_str(), avatar, created_at, 
         val.description, val.display_name, val.labels,
         pinned_post))
        .await.unwrap();
}

async fn handle_like(val: Box<LikeEvent>, session: Arc<Session>) {
    println!("{:?}", val);

    let author_stmt = format!(
        "INSERT INTO likes_by_author (
        author, subject, created_at, cid
        ) VALUES (?, ?, ?, ?)"
    );

    let counter_stmt = format!(
        "UPDATE post_likes SET likes = likes + 1 WHERE subject = ?"
    );

    let mut author_ps: PreparedStatement = session.prepare(author_stmt).await.expect("Prepared statement failed");
    let mut counter_ps: PreparedStatement = session.prepare(counter_stmt).await.expect("Prepared statement failed");

    author_ps.set_consistency(Consistency::LocalQuorum);
    counter_ps.set_consistency(Consistency::LocalQuorum);

    let cid = if let Some(x) = val.cid {
        match x {
            atrium_api::types::CidLink(cid) =>
                Some(cid.to_string())
        }
    } else {
        None
    };

    let created_at = val.created_at.with_timezone(&Utc);

    let _ = session.execute(&author_ps,
        (val.author.as_str(), val.subject.to_string(), created_at, cid)
    ).await;

    #[derive(SerializeCql, SerializeRow)]
    struct CounterUpdate {
        subject: String,
    }

    let _ = session.execute(&counter_ps, CounterUpdate{ subject: val.subject.to_string() }).await;
}


async fn handle_post(val: Box<Post>, session: Arc<Session>) {
    println!("{:?}", val);

    let reply = if let Some(x) = val.reply {
        Reply{
            parent: Some(x.parent.to_string()),
            root: Some(x.root.to_string()),
        }
    } else {
        Reply {
            parent: None,
            root: None,
        }
    };

    let mut vec: Vec<EmbedMedia> = vec![];
    if let Some(x) = val.embed.clone() {
        match x {
            Media(media) =>
                for content in media {
                    match content {
                        skystreamer::types::Media::Image(img) => {
                            let aspect = if let Some(ratio) = img.aspect_ratio {
                                let mut map: HashMap<String, i32> = HashMap::new();
                                map.insert("width".to_string(), ratio.0 as i32);
                                map.insert("height".to_string(), ratio.1 as i32);
                                Some(map)
                                // Some((ratio.0 as i32, ratio.1 as i32))
                            } else {
                                None
                            };
                            vec.push(EmbedMedia{ kind: "Image".to_string(),
                                     alt: Some(img.alt),
                                     blob: EmbedBlob{ cid: img.blob.cid, mime_type: img.blob.mime_type,
                                                      size: Some(img.blob.size.unwrap() as i64)},
                                     aspect_ratio: aspect});
                        }
                        skystreamer::types::Media::Video(video) => {
                            let aspect = if let Some(ratio) = video.aspect_ratio {
                                let mut map: HashMap<String, i32> = HashMap::new();
                                map.insert("width".to_string(), ratio.0 as i32);
                                map.insert("height".to_string(), ratio.1 as i32);
                                Some(map)
                                // Some((ratio.0 as i32, ratio.1 as i32))
                            } else {
                                None
                            };
                            vec.push(EmbedMedia { kind: "Video".to_string(),
                                     alt: video.alt,
                                     blob: EmbedBlob{ cid: video.blob.cid, mime_type: video.blob.mime_type,
                                                      size: Some(video.blob.size.unwrap() as i64)},
                                     aspect_ratio: aspect});
                        }
                    }
                }
            _ => (),
        }
    }

    let external = if let Some(x) = val.embed.clone() {
        match x {
            External(reference) =>
                if let Some(thumb) = reference.thumb {
                    Some(ExternalRef{
                         description: reference.description,
                         thumb: Some(EmbedBlob{ cid: thumb.clone().cid, mime_type: thumb.clone().mime_type,
                                                size: Some(thumb.size.unwrap() as i64)}),
                         title: reference.title,
                         uri: reference.uri,
                    })
                } else {
                    Some(ExternalRef{
                         description: reference.description,
                         thumb: None,
                         title: reference.title,
                         uri: reference.uri,
                    })
                }
            _ => None,
        }
    } else {
        None
    };

    let record_cid = if let Some(x) = val.embed.clone() {
        match x {
            Record(cid) =>
                Some(cid.to_string()),
            _ => None,
        }
    } else {
        None
    };

    let embed = Embeddings {
        media: Some(vec),
        external: external,
        record: record_cid,
    };

    let created_at: Option<DateTime<Utc>> = Some(val.created_at.with_timezone(&Utc));

    let stmt = format!(
        "INSERT INTO post (
        author, created_at, content, id, language, reply, tags, labels, embed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );

    session.query(stmt, 
        (val.author.as_str(), created_at, val.text, 
         val.id.to_string(), val.language, reply, val.tags,
         val.labels, embed))
        .await.unwrap();
}

