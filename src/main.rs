use std::{collections::BTreeMap, fs::File, io::{BufRead, BufReader, Write}};
use serde::{Deserialize, Serialize};

use tar::Archive;
use xz::read::XzDecoder;

#[derive(Deserialize)]
pub struct WDInput {
    pub item: String,
    pub imdb: String
}

#[derive(Deserialize)]
pub struct ImdbArtist {
    pub relations: Vec<Relation>,
    pub id: String,
    #[serde(rename = "life-span")]
    pub life_span: LifeSpan,
    pub name: Option<String>
}

#[derive(Deserialize)]
pub struct LifeSpan {
    pub begin: Option<String>,
    pub end: Option<String>
}

#[derive(Deserialize)]
pub struct Relation {
    pub r#type: String,
    pub url: Option<UrlRessource>
}

#[derive(Deserialize)]
pub struct UrlRessource {
    pub resource: String
}

#[derive(Serialize)]
pub struct Result {
    pub imdb_id: String,
    pub wikidata_id: String,
    pub musicbrainz_id: String,
    pub musicbrainz_name: Option<String>,
    pub musicbrainz_birth: Option<String>,
    pub musicbrainz_death: Option<String>,
}

fn main() {
    let mut csv_output = csv::Writer::from_path("./reconciled.csv").unwrap();
    let wd_inputs: Vec<WDInput> = serde_json::from_reader(File::open("./composerimdbnobrainz.json").unwrap()).unwrap();
    let mut item_by_imdb: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for input in wd_inputs {
        if let Some(present) = item_by_imdb.get_mut(&input.imdb) {
            present.push(input.item)
        } else {
            item_by_imdb.insert(input.imdb, vec![input.item]);
        }
    }
    
    let musicbrainz_artist_packed = BufReader::new(File::open("../artist.tar.xz").unwrap());
    let musicbrainz_artist_decompressed = XzDecoder::new(musicbrainz_artist_packed);
    let mut musicbrainz_artist_archive = Archive::new(musicbrainz_artist_decompressed);

    for entry in musicbrainz_artist_archive.entries().unwrap() {
        let entry = entry.unwrap();
        let path = entry.path().unwrap();
        if path.to_string_lossy() != "mbdump/artist" {
            continue
        }
        
        let mut reader = BufReader::with_capacity(10_000_000, entry);

        let mut line = Vec::with_capacity(10_000);
        let mut nb_read = 0;
        while {
            line = Vec::with_capacity(10_000);
            nb_read += 1;
            reader.read_until(0x0A, &mut line).unwrap()
        } != 0 {
            if nb_read % 1000 == 0 {
                println!("{}", nb_read);
            }

            let artist: ImdbArtist = serde_json::from_slice(&line).unwrap();
            
            let mut imdb_ids: Vec<String> = Vec::new();
            for relation in &artist.relations {
                if relation.r#type == "IMDb" {
                    let url = relation.url.as_ref().unwrap();
                    let url_parts: Vec<&str> = url.resource.split('/').collect();
                    if let Some(Some(imdb_id)) =  url_parts.len().checked_sub(2).map(|x| url_parts.get(x)) {
                        imdb_ids.push(imdb_id.to_string());
                    }
                }
            }

            if imdb_ids.len() > 1 {
                println!("error : 2 or more imdb ids listed for artist {} : {:?}", artist.id, imdb_ids);
                continue
            };

            for imdb_id in imdb_ids {
                if let Some(wikidata_artist_id) = item_by_imdb.get(&imdb_id) {
                    let result = Result {
                        imdb_id,
                        wikidata_id: wikidata_artist_id[0].clone(),
                        musicbrainz_id: artist.id.clone(),
                        musicbrainz_name: artist.name.clone(),
                        musicbrainz_birth: artist.life_span.begin.clone(),
                        musicbrainz_death: artist.life_span.end.clone(),
                    };
                    csv_output.serialize(result).unwrap();
                }
            }
        }   
    }
}