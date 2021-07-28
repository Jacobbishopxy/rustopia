use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    path::Path,
    str::FromStr,
};

use anyhow::Result;
use quick_xml::Reader;

pub fn read_all<T: FromStr>(file_name: &str) -> Vec<Result<T, <T as FromStr>::Err>> {
    fs::read_to_string(file_name)
        .expect("file not found!")
        .lines()
        .map(|x| x.parse())
        .collect()
}

pub fn read_iter(file_name: &str, func: fn(&str)) {
    let file = File::open(file_name).expect("file not found!");
    let reader = BufReader::new(file);

    for line in reader.lines() {
        func(&line.unwrap());
    }
}

pub fn read_line(file_name: &str, func: fn(&str)) -> Result<(), std::io::Error> {
    let file = File::open(&file_name)?;

    let mut reader = BufReader::new(file);
    let mut line = String::new();

    loop {
        match reader.read_line(&mut line) {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    break;
                }

                func(&line);

                line.clear();
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    Ok(())
}

pub fn read_spilt(file_name: &str, func: fn(&[u8])) -> Result<(), std::io::Error> {
    let file = File::open(&file_name)?;

    let reader = BufReader::new(file);

    for line in reader.split(0x10) {
        func(&line?);
    }

    Ok(())
}

pub fn get_xml_stream(source: &str) -> Result<Reader<Box<dyn BufRead>>> {
    let local_path = Path::new(source);

    if local_path.is_file() {
        let file = File::open(local_path)?;
        let reader = BufReader::new(file);

        Ok(Reader::from_reader(Box::new(reader)))
    } else {
        panic!("{:?} is not a file", &source);
    }
}
