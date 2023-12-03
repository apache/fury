// Copyright 2023 The Fury Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use fury::row::{from_row, to_row};
use fury_derive::FuryRow;

#[test]
fn row() {
    #[derive(FuryRow)]
    struct Foo {
        f1: String,
        f2: i8,
    }

    #[derive(FuryRow)]
    struct Bar {
        f3: Foo,
    }

    let row = to_row(&Bar {
        f3: Foo {
            f1: String::from("hello"),
            f2: 1,
        },
    });

    let obj = from_row::<Bar>(&row);
    let f1: &str = obj.get_f3().get_f1();
    assert_eq!(f1, "hello");
    let f2: i8 = obj.get_f3().get_f2();
    assert_eq!(f2, 1);
}
