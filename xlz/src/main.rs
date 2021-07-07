use xlz::{self, WriteExtension};

fn main() -> std::io::Result<()> {
    let file = "./test.xlsx";
    let sheet = "Sheet 1";

    xlz::read_then_write(file, sheet, &WriteExtension::JSON)
}
