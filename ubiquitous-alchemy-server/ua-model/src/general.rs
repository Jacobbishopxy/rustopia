use std::any::Any;

#[allow(dead_code)]
struct Header {
    uuid: u64,
    protocol: String,
}

// statically typed, no pointer dereference
#[allow(dead_code)]
struct GenericPacket<T> {
    header: Header,
    data: T,
}

// uses the "Any" type to have dynamic typing
#[allow(dead_code)]
struct AnyPacket {
    header: Header,
    data: dyn Any,
}

// uses an enum to capture the different possible types
#[allow(dead_code)]
enum DataEnum {
    Integer(i32),
    Float(f32),
    String(String),
}
#[allow(dead_code)]
struct EnumPacket {
    header: Header,
    data: DataEnum,
}

#[allow(dead_code)]
trait DataTrait {
    // interface your data conforms to
}

#[allow(dead_code)]
struct TraitPacket<'a> {
    header: Header,
    data: &'a dyn DataTrait, // uses a pointer dereference to something that implements DataTrait
}

// statically typed, but will accept any type that conforms to DataTrait
#[allow(dead_code)]
struct StaticTraitPacket<T: DataTrait> {
    header: Header,
    data: T,
}
