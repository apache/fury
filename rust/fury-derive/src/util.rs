use syn::{Field, Fields};

pub fn sorted_fields(fields: &Fields) -> Vec<&Field> {
    let mut fields = fields.iter().collect::<Vec<&Field>>();
    fields.sort_by(|a, b| a.ident.cmp(&b.ident));
    fields
}
