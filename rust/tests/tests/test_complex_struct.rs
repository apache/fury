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

#[test]
fn complex_struct() {
    #[derive(Fury, Debug, PartialEq)]
    #[tag("example.foo2")]
    struct Animal {
        category: String,
    }

    #[derive(Fury, Debug, PartialEq)]
    #[tag("example.foo")]
    struct Person {
        c1: Vec<u8>,  // binary
        c2: Vec<i16>, // primitive array
        animal: Vec<Animal>,
        c3: Vec<Vec<u8>>,
        name: String,
        c4: HashMap<String, String>,
        age: u16,
        op: Option<String>,
        op2: Option<String>,
        date: NaiveDate,
        time: NaiveDateTime,
        c5: f32,
        c6: f64,
    }
    let person: Person = Person {
        c1: vec![1, 2, 3],
        c2: vec![5, 6, 7],
        c3: vec![vec![1, 2], vec![1, 3]],
        animal: vec![Animal {
            category: "Dog".to_string(),
        }],
        c4: HashMap::from([
            ("hello1".to_string(), "hello2".to_string()),
            ("hello2".to_string(), "hello3".to_string()),
        ]),
        age: 12,
        name: "hello".to_string(),
        op: Some("option".to_string()),
        op2: None,
        date: NaiveDate::from_ymd_opt(2025, 12, 12).unwrap(),
        time: DateTime::from_timestamp(1689912359, 0).unwrap().naive_utc(),
        c5: 2.0,
        c6: 4.0,
    };
    let fury = Fury::new();
    let bin: Vec<u8> = fury.serialize(&person);
    let obj: Person = fury.deserialize(&bin).expect("should success");
    assert_eq!(person, obj);
}

#[test]
fn encode_to_obin() {
    #[derive(Fury, Debug, PartialEq)]
    #[tag("example.foo2")]
    struct Animal {
        category: String,
    }

    #[derive(Fury, Debug, PartialEq)]
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
    let fury = Fury::new();
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
