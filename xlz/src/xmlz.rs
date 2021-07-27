use failure::Error;
use quick_xml::Reader;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// Returns an XML stream either from a file or a URL.
pub fn get_xml_stream(source: &str) -> Result<Reader<Box<dyn BufRead>>, Error> {
    let local_path = Path::new(source);

    if local_path.is_file() {
        let file = File::open(local_path)?;
        let reader = BufReader::new(file);

        Ok(Reader::from_reader(Box::new(reader)))
    } else {
        panic!("{:?} is not a file", &source);
    }
}
