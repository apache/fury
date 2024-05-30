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
use fury::{from_buffer, to_buffer, Fury};
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

    let bin: Vec<u8> = to_buffer(&person);
    let obj: Person = from_buffer(&bin).expect("should success");
    assert_eq!(person, obj);
}

#[test]
fn decode_py_struct() {
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
        f9: Vec<i16>,
        f10: HashMap<i32, f64>,
    }

    let bin = [
        134, 0, 179, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 81, 159, 160, 124, 69, 240, 2, 120, 21, 0,
        101, 120, 97, 109, 112, 108, 101, 46, 67, 111, 109, 112, 108, 101, 120, 79, 98, 106, 101,
        99, 116, 71, 168, 32, 21, 0, 13, 0, 3, 115, 116, 114, 0, 30, 0, 2, 255, 7, 0, 1, 0, 0, 0,
        255, 12, 0, 85, 85, 85, 85, 85, 85, 213, 63, 255, 7, 0, 100, 0, 0, 0, 255, 12, 0, 146, 36,
        73, 146, 36, 73, 210, 63, 0, 30, 0, 2, 0, 13, 0, 2, 107, 49, 255, 3, 0, 255, 0, 13, 0, 2,
        107, 50, 255, 3, 0, 2, 255, 3, 0, 127, 255, 5, 0, 255, 127, 255, 7, 0, 255, 255, 255, 127,
        255, 9, 0, 255, 255, 255, 255, 255, 255, 255, 127, 255, 11, 0, 0, 0, 0, 63, 255, 12, 0, 85,
        85, 85, 85, 85, 85, 229, 63, 0, 25, 0, 2, 255, 5, 0, 1, 0, 255, 5, 0, 2, 0, 134, 2, 98, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 81, 159, 160, 124, 69, 240, 2, 120, 21, 0, 101, 120, 97, 109,
        112, 108, 101, 46, 67, 111, 109, 112, 108, 101, 120, 79, 98, 106, 101, 99, 116, 71, 168,
        32, 21, 253, 253, 253, 255, 3, 0, 0, 255, 5, 0, 0, 0, 255, 7, 0, 0, 0, 0, 0, 255, 9, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 255, 11, 0, 171, 170, 170, 62, 255, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        253,
    ];

    let obj: Person = from_buffer(&bin).expect("should some");
    print!("{:?}", obj);
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

    let bin: Vec<u8> = to_buffer(&Person {
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
