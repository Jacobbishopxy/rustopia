use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    str::FromStr,
};

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
