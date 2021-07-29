fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (file, sheet) = (&args[1], &args[2]);

    println!("{:?}:{:?}", file, sheet);

    let workbook = xlz::Source::Path(file).read();

    match workbook {
        Ok(wb) => {
            let mut wb = wb;
            let sheets = wb.sheets();
            let sheet = sheets.get(sheet).unwrap();
            for row in sheet.rows(&mut wb) {
                for cell in row.0 {
                    print!("{:?}, ", cell.value);
                }
                println!("")
            }
        }
        Err(e) => println!("{:?}", e),
    }
}
