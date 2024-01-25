/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#[link(wasm_import_module = "pulsar")]
extern "C" {
    fn get_args(arg_id: i64, addr: i64, len: i32) -> i32;

    fn put_result(arg_id: i64, addr: i64, len: i32) -> i32;
}

#[no_mangle]
pub unsafe extern "C" fn process(arg_id: i64) {
    let mut buf = [0u8; 32];
    let buf_ptr = buf.as_mut_ptr() as i64;
    eprintln!("rust side-> buffer base address: {}", buf_ptr);
    // get arg from java
    let len = get_args(arg_id, buf_ptr, buf.len() as i32);
    let java_arg = std::str::from_utf8(&buf[..len as usize]).unwrap();
    eprintln!("rust side-> recv:{}", java_arg);
    // pass rust result to java
    let rust_result = "rust result".as_bytes();
    let result_ptr = rust_result.as_ptr() as i64;
    _ = put_result(arg_id, result_ptr, rust_result.len() as i32);
}
