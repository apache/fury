// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use fury_core::fury::Fury;
use fury_derive::Fury;
use std::collections::HashMap;
use fury_core::types::Mode;

#[test]
fn complex_struct() {
    #[derive(Fury, Debug, PartialEq, Default)]
    #[tag("example.foo2")]
    struct Animal {
        category: String,
    }

    #[derive(Fury, Debug, PartialEq, Default)]
    #[tag("example.foo")]
    struct Person {

        animal: Vec<Animal>,

    }
    let person: Person = Person {

        animal: vec![Animal {
            category: "Dog".to_string(),
        }],

    };
    let fury = Fury::default()
        .mode(Mode::Compatible);
    let bin: Vec<u8> = fury.serialize(&person);
    let obj: Person = fury.deserialize(&bin).expect("should success");
    assert_eq!(person, obj);
}

#[test]
fn encode_to_obin() {
    #[derive(Fury, Debug, PartialEq, Default)]
    #[tag("example.foo2")]
    struct Animal {
        category: String,
    }

    #[derive(Fury, Debug, PartialEq, Default)]
    #[tag("example.ComplexObject")]
    struct Person {
        f1: String,
        f2: HashMap<String, i8>,
        f3: i8,
        f4: i16,
        f5: i32,
        f6: i64,
        f7: f32,
        f8: f64,
        f10: HashMap<i32, f64>,
    }
    let fury = Fury::default();
    let bin: Vec<u8> = fury.serialize(&Person {
        f1: "Hello".to_string(),
        f2: HashMap::from([("hello1".to_string(), 1), ("hello2".to_string(), 2)]),
        f3: 1,
        f4: 2,
        f5: 3,
        f6: 4,
        f7: 5.0,
        f8: 6.0,
        f10: HashMap::from([(1, 1.0), (2, 2.0)]),
    });

    print!("{:?}", bin);
}
