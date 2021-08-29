use polars::prelude::*;

fn main() {
    let ca: UInt32Chunked = (0..10).map(Some).collect();

    // let mut builder = PrimitiveChunkedBuilder::<UInt32Type>::new("foo", 10);
    // for value in 0..10 {
    //     builder.append_value(value);
    // }
    // let ca = builder.finish();

    println!("{:?}", ca);
}
