use std::{
    ffi::{c_char, c_int, c_void},
    io::stdin,
};

use crate::ffi::sqlite3;

pub mod ffi {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(improper_ctypes)]
    include!(concat!(env!("OUT_DIR"), "/sqliteInt.rs"));
}

extern "C" fn frame_callback(user_data: *mut c_void, data: *const c_char, len: c_int) -> c_int {
    dbg!("here");
    let db: *mut sqlite3 = user_data as _;
    unsafe {
        ffi::replicate(db, data, len);
    }

    let conn = unsafe { rusqlite::Connection::from_handle(db as _).unwrap() };
    let mut stmt = conn.prepare("select * from test").unwrap();
    let mut rows = stmt.query(()).unwrap();
    while let Some(row) = rows.next().unwrap() {
        dbg!(row);
    }

    0
}

fn main() {
    unsafe {
        let mut db1: *mut sqlite3 = std::ptr::null_mut() as _;
        let name = b":memory:\0";
        ffi::sqlite3_open(name.as_ptr() as _, &mut db1 as _);

        let mut db2: *mut sqlite3 = std::ptr::null_mut() as _;
        ffi::sqlite3_open(name.as_ptr() as _, &mut db2 as _);

        ffi::init_replication_callback(db1, Some(frame_callback), db2 as _);

        let stdout = stdin();
        for line in stdout.lines() {
            let mut bytes = line.unwrap().as_bytes().to_vec();
            bytes.push(0);
            ffi::sqlite3_exec(
                db1,
                bytes.as_ptr() as _,
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
        }
    }
}
