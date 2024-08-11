use std::any::{Any, TypeId};
use fury_core::error::Error;
use fury_core::fury::Fury;
use fury_derive::{impl_polymorph, Fury};

#[impl_polymorph]
trait Animal {
    fn get_name(&self) -> String;
}

#[derive(Fury, Debug)]
#[polymorphic_traits("Animal")]
struct Dog {
    name: String
}

impl Animal for Dog {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}


#[test]
fn test_custom_trait_object_work() {
    let mut fury = Fury::default();

    #[derive(Fury)]
    struct Person {
        pet: Box<dyn Animal>
    }

    let p = Person {
        pet: Box::new(Dog {
            name: String::from("puppy")
        })
    };
    fury.register::<Person>(501);
    fury.register::<Dog>(500);
    let bin = fury.serialize(&p);
    let obj: Person = fury.deserialize(&bin).unwrap();
    assert_eq!(obj.pet.get_name(), "puppy");
}



#[test]
fn test_any_trait_object_work() {
    let mut fury = Fury::default();

    #[derive(Fury)]
    struct Person {
        pet: Box<dyn Any>
    }

    let p = Person {
        pet: Box::new(Dog {
            name: String::from("puppy")
        })
    };
    fury.register::<Person>(501);
    fury.register::<Dog>(500);
    let bin = fury.serialize(&p);
    let obj: Person = fury.deserialize(&bin).unwrap();
    let pet = obj.pet.downcast_ref::<Dog>().unwrap();
    assert_eq!(pet.get_name(), "puppy");
}
