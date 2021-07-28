fn main() {
    let mut wb = xl::Workbook::open("test.xlsx").unwrap();
    let sheets = wb.sheets();
    let sheet = sheets.get("Dev").unwrap();
    for row in sheet.rows(&mut wb) {
        for cell in row.0 {
            println!("{:?} >>> {:?}", cell.cell_type, cell.value);
        }
    }
}
