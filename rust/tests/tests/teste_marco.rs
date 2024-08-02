use fury_derive::Fury;

#[test]
fn complex_struct() {
    #[derive(Fury, Debug, PartialEq)]
    #[tag("example.foo2")]
    struct Animal {
        category: String,
    }
}
