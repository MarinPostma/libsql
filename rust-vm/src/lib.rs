#![allow(dead_code)]

use core::fmt::{self, Display};
use std::collections::HashMap;
use std::ffi::{c_int, c_void, CStr};

use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use ffi::{sqlite3, Mem, Vdbe, VdbeOp};

const SQLITE_INTERNAL: usize = 2;

pub mod ffi {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(improper_ctypes)]
    include!(concat!(env!("OUT_DIR"), "/sqliteInt.rs"));
}

#[derive(Debug)]
enum Value {
    Integer(c_int),
    Null,
}

enum ReturnCode {
    SqliteRow,
}

enum RowValue {
    String(String),
    Integer(u64),
    Blob(Vec<u8>),
    Float(f64),
    Null,
}

impl fmt::Debug for RowValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(s) => write!(f, "'{s}'"),
            Self::Integer(n) => write!(f, "{n}"),
            Self::Blob(b) => {
                #[allow(deprecated)]
                let b64 = base64::encode(b);
                write!(f, "b64:{b64}")
            }
            Self::Float(x) => write!(f, "{x}"),
            Self::Null => write!(f, "Null"),
        }
    }
}

#[derive(Debug)]
struct Row {
    values: Vec<RowValue>,
}

#[derive(Debug, Serialize, Deserialize)]
enum TableRoot {
    System,
    User(i32),
}

impl Display for TableRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableRoot::System => write!(f, "system"),
            TableRoot::User(i) => write!(f, "{i}"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Op {
    Transaction,
    Record {
        id: u32,
        /// A number representing the root of the tree into which to insert the row.
        /// Since there is no guarantee over what will be the actual root page returned by the
        /// execution of OP_CreateBtree, we store an offset instead: the first call to CreateBTree
        /// has offset 0, etc...
        /// When restoring from a log, one has to convert this offset into an actual root offset.
        root: TableRoot,
        data: Vec<i8>,
    },
    SetCookie {
        db: i32,
        value: i32,
        cookie: i32,
    },
    CreateBTree(usize),
}

#[derive(Default)]
struct Trace {
    ops: Vec<Op>,
}

impl Trace {
    fn new() -> Self {
        Self { ops: Vec::new() }
    }
    fn clear(&mut self) {
        self.ops.clear();
    }

    fn push(&mut self, op: Op) {
        self.ops.push(op);
    }
}

impl fmt::Debug for Trace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for op in self.ops.iter() {
            match op {
                Op::Transaction => writeln!(f, "TX_BEGIN")?,
                Op::Record { id, root, data } => {
                    let row = decode_record(&data);
                    writeln!(f, "INSERT root={root} id={id} row={row:?}")?;
                }
                Op::CreateBTree(i) => writeln!(f, "CREATE_BTREE root={i}")?,
                Op::SetCookie { db, value, cookie } => {
                    writeln!(f, "SET_COOKIE db={db}, value={value}, cookie={cookie}")?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
struct Context {
    cursor_to_root: HashMap<i32, i32>,
    trace: Trace,
}

#[derive(Debug)]
struct ReplicationState {
    /// maps a register to a node in the current context
    btree_root_remap: HashMap<i32, i32>,
    contexts: HashMap<i32, Context>,
    context_stack: Vec<i32>,
    context_ids: i32,
    logger: mpsc::Sender<Op>,
}

impl ReplicationState {
    fn in_context(&self) -> bool {
        !self.context_stack.is_empty()
    }
    fn current_context_mut(&mut self) -> &mut Context {
        let ctx_id = self
            .context_stack
            .last()
            .expect("replication method called out of any context");
        self.contexts
            .get_mut(dbg!(ctx_id))
            .expect("invalid replication state")
    }
}

async fn update_conn(conn: &mut (usize, TcpStream), log: &[Op]) -> bool {
    let to_send = &log[conn.0..];
    let mut sent = 0;
    for op in to_send {
        let data = bincode::serialize(op).unwrap();
        if conn.1.write_all(&data).await.is_err() {
            return false;
        }

        sent += 1;
    }

    conn.0 += sent;

    true
}

fn serve_log(mut rcv: mpsc::Receiver<Op>) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut log = Vec::new();
        let listener = TcpListener::bind("127.0.0.1:7890").await.unwrap();
        dbg!();
        let mut conns = Vec::new();
        loop {
            tokio::select! {
                Some(e) = rcv.recv() => {
                    log.push(e);
                    let mut to_remove = Vec::new();
                    for (i, conn) in conns.iter_mut().enumerate() {
                        if !update_conn(conn, &log).await {
                            to_remove.push(i);
                        }
                    }

                    for i in to_remove {
                        conns.remove(i);
                    }
                },
                Ok((s, _)) = listener.accept() => {
                    dbg!();
                    let mut conn = (0, s);
                    if update_conn(&mut conn, &log).await {
                        conns.push(conn);
                    }
                },
                else => break
            }
        }
    });
}

#[no_mangle]
pub extern "C" fn replication_state_init() -> *mut c_void {
    let (sender, receiver) = mpsc::channel(512);
    let state = Box::new(ReplicationState {
        btree_root_remap: HashMap::new(),
        contexts: HashMap::new(),
        context_stack: Vec::new(),
        context_ids: 0,
        logger: sender,
    });

    std::thread::spawn(move || serve_log(receiver));

    let ptr = Box::leak(state) as *mut _;

    ptr as _
}

/// Enter a vdbe context. Returns a new context_id
#[no_mangle]
pub extern "C" fn replication_enter_context(vdbe: *const Vdbe) {
    println!("entering context");
    let vdbe = unsafe { &*vdbe };
    // we are only interested in writes
    let state = unsafe { &mut *((*vdbe.db).replication_context as *mut ReplicationState) };
    // this is not good, we need a proper id
    let ctx_id = state.context_ids;
    state.context_ids = state.context_ids.wrapping_add(1);

    let ctx = Context::default();
    state.contexts.insert(ctx_id, ctx);
    state.context_stack.push(dbg!(ctx_id));
}

#[no_mangle]
pub extern "C" fn replication_exit_context(vdbe: *const Vdbe) {
    println!("exiting context");
    let vdbe = unsafe { &*vdbe };
    let state = unsafe { &mut *((*vdbe.db).replication_context as *mut ReplicationState) };
    if state.in_context() {
        let last = state.context_stack.pop().unwrap();
        state.contexts.remove(dbg!(&last));
    }
}

#[no_mangle]
pub extern "C" fn replication_state_destroy(state: *mut c_void) {
    let _state: Box<ReplicationState> = unsafe { Box::from_raw(state as *mut ReplicationState) };
}

#[no_mangle]
pub extern "C" fn replication_post_commit_cleanup(vdbe: *mut Vdbe) {
    let vdbe = unsafe { &*vdbe };
    let _state = unsafe { &mut *((*vdbe.db).replication_context as *mut ReplicationState) };
    println!("post commit called");
}

#[no_mangle]
pub extern "C" fn replication_pre_commit(vdbe: *const Vdbe) -> c_int {
    let vdbe = unsafe { &*vdbe };
    let state = unsafe { &mut *((*vdbe.db).replication_context as *mut ReplicationState) };
    if state.in_context() {
        let ctx = state.current_context_mut();
        println!("Trace:\n {:?}", ctx.trace);
        let trace = std::mem::take(&mut ctx.trace);
        for op in trace.ops {
            state.logger.blocking_send(op).unwrap();
        }
    }
    0
}

// takes a db and apply a stream of logical frames to it.
pub fn replicate(_db: *mut sqlite3) {
    todo!()
}

#[derive(Debug)]
enum RecordType {
    Null,
    Int8,
    Int16,
    Int24,
    Int32,
    Int48,
    Int64,
    F64,
    Zero,
    One,
    Reserved,
    Blob(usize),
    Text(usize),
}

impl RecordType {
    fn from_u64(d: u64) -> Self {
        match d {
            0 => Self::Null,
            1 => Self::Int8,
            2 => Self::Int16,
            3 => Self::Int24,
            4 => Self::Int32,
            5 => Self::Int48,
            6 => Self::Int64,
            7 => Self::F64,
            8 => Self::Zero,
            9 => Self::One,
            10 | 11 => Self::Reserved,
            n if n >= 12 && n % 2 == 0 => Self::Blob((n as usize - 12) / 2),
            n if n >= 13 && n % 2 != 0 => Self::Blob((n as usize - 13) / 2),
            _ => unreachable!(),
        }
    }

    fn size(&self) -> usize {
        match self {
            RecordType::Null => 0,
            RecordType::Int8 => 1,
            RecordType::Int16 => 2,
            RecordType::Int24 => 3,
            RecordType::Int32 => 4,
            RecordType::Int48 => 6,
            RecordType::Int64 => 8,
            RecordType::F64 => 8,
            RecordType::Zero => 0,
            RecordType::One => 0,
            RecordType::Reserved => 0,
            RecordType::Blob(s) => *s,
            RecordType::Text(s) => *s,
        }
    }
}

fn decode_record(data: &[i8]) -> Vec<RowValue> {
    let (data_offset, ty_offset) = sqlite_get_var_int(data);
    let mut ty_slice = &data[ty_offset as usize..data_offset as usize];
    let mut data_slice =
        unsafe { &*((&data[data_offset as usize..]) as *const [i8] as *const [u8]) };
    let mut row = Vec::new();
    while !ty_slice.is_empty() {
        let (ty, size) = sqlite_get_var_int(ty_slice);
        ty_slice = &ty_slice[size as usize..];
        let ty = RecordType::from_u64(ty);
        let data_len = ty.size();
        let value = match ty {
            RecordType::Null => RowValue::Null,
            RecordType::Int8 => RowValue::Integer(u8::from_ne_bytes(
                data_slice[..data_len].try_into().unwrap(),
            ) as u64),
            RecordType::Int16 => RowValue::Integer(u16::from_ne_bytes(
                data_slice[..data_len].try_into().unwrap(),
            ) as u64),
            RecordType::Int24 => {
                let mut bytes = [0; 4];
                bytes[..data_len].copy_from_slice(&data_slice[..data_len]);
                RowValue::Integer(u32::from_ne_bytes(bytes) as u64)
            }
            RecordType::Int32 => RowValue::Integer(u32::from_ne_bytes(
                data_slice[..data_len].try_into().unwrap(),
            ) as u64),
            RecordType::Int48 => {
                let mut bytes = [0; 8];
                bytes[..data_len].copy_from_slice(&data_slice[..data_len]);
                RowValue::Integer(u64::from_ne_bytes(bytes))
            }
            RecordType::Int64 => RowValue::Integer(u64::from_ne_bytes(
                data_slice[..data_len].try_into().unwrap(),
            )),
            RecordType::F64 => RowValue::Float(f64::from_ne_bytes(
                data_slice[..data_len].try_into().unwrap(),
            )),
            RecordType::Zero => RowValue::Integer(0),
            RecordType::One => RowValue::Integer(1),
            RecordType::Reserved => RowValue::Null,
            RecordType::Blob(_) => {
                let data = data_slice[..data_len].to_vec();
                RowValue::Blob(data)
            }
            RecordType::Text(_) => {
                let s = unsafe { String::from_utf8_unchecked(data_slice[..data_len].to_vec()) };
                RowValue::String(s)
            }
        };
        row.push(value);

        data_slice = &data_slice[data_len..];
    }

    row
}

#[no_mangle]
pub extern "C" fn replication_step(vdbe: *const Vdbe, op: *const VdbeOp) -> c_int {
    let vdbe = unsafe { &*vdbe };
    let state = unsafe { &mut *((*vdbe.db).replication_context as *mut ReplicationState) };
    if !state.in_context() {
        return 0;
    }
    let op = unsafe { &*op };
    let code: OpCode = op.opcode.try_into().unwrap();
    match code {
        OpCode::OpTransaction => {
            state.current_context_mut().trace.push(Op::Transaction);
        }
        OpCode::OpCreateBtree => {
            let root = unsafe { vdbe.vdbe_get_reg(op.p2).u.i };
            let root_remap_id = state.btree_root_remap.len();
            state
                .btree_root_remap
                .insert(root as i32, root_remap_id as i32);
            state
                .current_context_mut()
                .trace
                .push(Op::CreateBTree(root_remap_id))
        }
        OpCode::OpSetCookie => {
            let db = op.p1;
            let value = op.p3;
            let cookie = op.p2;
            state
                .current_context_mut()
                .trace
                .push(Op::SetCookie { db, value, cookie });
        }
        OpCode::OpOpenWrite => {
            // p1 contains a cursor for the table/index whose root is in p2. We create the mapping
            state
                .current_context_mut()
                .cursor_to_root
                .insert(op.p1, op.p2);
        }
        OpCode::OpInsert => {
            // p1 contains the cursor we want to insert to, we get the corresponding table root
            // mapping
            let root = *state
                .current_context_mut()
                .cursor_to_root
                .get(&op.p1)
                .expect("unknown cursor root!");
            let root = if root == 1 {
                // This is an insert to the system table, we need to patch it.
                TableRoot::System
            } else {
                let mapped_root = state.btree_root_remap.get(&root).expect("unkown root!");
                TableRoot::User(*mapped_root)
            };
            if op.p4type == P4_TABLE {
                let table = unsafe { &op.p4.pTab };
                let name = unsafe { CStr::from_ptr((**table).zName) };
                println!("inserting into {name:?}");
            }

            let id = unsafe { vdbe.vdbe_get_reg(op.p3).u.i as u32 };

            let data_reg = vdbe.vdbe_get_reg(op.p2);
            let data = unsafe { std::slice::from_raw_parts(data_reg.z, data_reg.n as usize) };
            state.current_context_mut().trace.push(Op::Record {
                id,
                root,
                data: data.to_vec(),
            })
        }
        _ => (),
    }

    0
}

impl Vdbe {
    fn vdbe_get_reg(&self, index: i32) -> &Mem {
        if index >= self.nMem {
            panic!("register {index} out of bound")
        }
        unsafe { &(*self.aMem.offset(index as isize)) }
    }
}

const P4_NOTUSED: i8 = 0; /* The P4 parameter is not used */
const P4_TRANSIENT: i8 = 0; /* P4 is a pointer to a transient string */
const P4_STATIC: i8 = -1; /* Pointer to a static string */
const P4_COLLSEQ: i8 = -2; /* P4 is a pointer to a CollSeq structure */
const P4_INT32: i8 = -3; /* P4 is a 32-bit signed integer */
const P4_SUBPROGRAM: i8 = -4; /* P4 is a pointer to a SubProgram structure */
const P4_TABLE: i8 = -5; /* P4 is a pointer to a Table structure */
const P4_FREE_IF_LE: i8 = -6;
const P4_DYNAMIC: i8 = -6; /* Pointer to memory from sqliteMalloc() */
const P4_FUNCDEF: i8 = -7; /* P4 is a pointer to a FuncDef structure */
const P4_KEYINFO: i8 = -8; /* P4 is a pointer to a KeyInfo structure */
const P4_EXPR: i8 = -9; /* P4 is a pointer to an Expr tree */
const P4_MEM: i8 = -10; /* P4 is a pointer to a Mem*    structure */
const P4_VTAB: i8 = -11; /* P4 is a pointer to an sqlite3_vtab structure */
const P4_REAL: i8 = -12; /* P4 is a 64-bit floating point value */
const P4_INT64: i8 = -13; /* P4 is a 64-bit signed integer */
const P4_INTARRAY: i8 = -14; /* P4 is a vector of 32-bit integers */
const P4_FUNCCTX: i8 = -15; /* P4 is a pointer to an sqlite3_context object */

#[derive(Debug, TryFromPrimitive, Clone, Copy)]
#[repr(u8)]
enum OpCode {
    OpSavepoint = 0,
    OpAutoCommit = 1,
    OpTransaction = 2,
    OpCheckpoint = 3,
    OpJournalMode = 4,
    OpVacuum = 5,
    OpVFilter = 6,
    OpVUpdate = 7,
    OpInit = 8,
    OpGoto = 9,
    OpGosub = 10,
    OpInitCoroutine = 11,
    OpYield = 12,
    OpMustBeInt = 13,
    OpJump = 14,
    OpOnce = 15,
    OpIf = 16,
    OpIfNot = 17,
    OpIsType = 18,
    OpNot = 19,
    OpIfNullRow = 20,
    OpSeekLT = 21,
    OpSeekLE = 22,
    OpSeekGE = 23,
    OpSeekGT = 24,
    OpIfNotOpen = 25,
    OpIfNoHope = 26,
    OpNoConflict = 27,
    OpNotFound = 28,
    OpFound = 29,
    OpSeekRowid = 30,
    OpNotExists = 31,
    OpLast = 32,
    OpIfSmaller = 33,
    OpSorterSort = 34,
    OpSort = 35,
    OpRewind = 36,
    OpSorterNext = 37,
    OpPrev = 38,
    OpNext = 39,
    OpIdxLE = 40,
    OpIdxGT = 41,
    OpIdxLT = 42,
    OpIdxGE = 43,
    OpRowSetRead = 44,
    OpRowSetTest = 45,
    OpOr = 46,
    OpAnd = 47,
    OpProgram = 48,
    OpFkIfZero = 49,
    OpIfPos = 50,
    OpIfNotZero = 51,
    OpDecrJumpZero = 52,
    OpIsNull = 53,
    OpNotNull = 54,
    OpNe = 55,
    OpEq = 56,
    OpGt = 57,
    OpLe = 58,
    OpLt = 59,
    OpGe = 60,
    OpElseEq = 61,
    OpIncrVacuum = 62,
    OpVNext = 63,
    OpFilter = 64,
    OpPureFunc = 65,
    OpFunction = 66,
    OpReturn = 67,
    OpEndCoroutine = 68,
    OpHaltIfNull = 69,
    OpHalt = 70,
    OpInteger = 71,
    OpInt64 = 72,
    OpString = 73,
    OpBeginSubrtn = 74,
    OpNull = 75,
    OpSoftNull = 76,
    OpBlob = 77,
    OpVariable = 78,
    OpMove = 79,
    OpCopy = 80,
    OpSCopy = 81,
    OpIntCopy = 82,
    OpFkCheck = 83,
    OpResultRow = 84,
    OpCollSeq = 85,
    OpAddImm = 86,
    OpRealAffinity = 87,
    OpCast = 88,
    OpPermutation = 89,
    OpCompare = 90,
    OpIsTrue = 91,
    OpZeroOrNull = 92,
    OpOffset = 93,
    OpColumn = 94,
    OpTypeCheck = 95,
    OpAffinity = 96,
    OpMakeRecord = 97,
    OpCount = 98,
    OpReadCookie = 99,
    OpSetCookie = 100,
    OpReopenIdx = 101,
    OpOpenRead = 102,
    OpOpenWrite = 103,
    OpOpenDup = 104,
    OpBitAnd = 105,
    OpBitOr = 106,
    OpShiftLeft = 107,
    OpShiftRight = 108,
    OpAdd = 109,
    OpSubtract = 110,
    OpMultiply = 111,
    OpDivide = 112,
    OpRemainder = 113,
    OpConcat = 114,
    OpOpenAutoindex = 115,
    OpOpenEphemeral = 116,
    OpBitNot = 117,
    OpSorterOpen = 118,
    OpSequenceTest = 119,
    OpString8 = 120,
    OpOpenPseudo = 121,
    OpClose = 122,
    OpColumnsUsed = 123,
    OpSeekScan = 124,
    OpSeekHit = 125,
    OpSequence = 126,
    OpCreateWasmFunc = 127,
    OpDropWasmFunc = 128,
    OpNewRowid = 129,
    OpInsert = 130,
    OpRowCell = 131,
    OpDelete = 132,
    OpResetCount = 133,
    OpSorterCompare = 134,
    OpSorterData = 135,
    OpRowData = 136,
    OpRowid = 137,
    OpNullRow = 138,
    OpSeekEnd = 139,
    OpIdxInsert = 140,
    OpSorterInsert = 141,
    OpIdxDelete = 142,
    OpDeferredSeek = 143,
    OpIdxRowid = 144,
    OpFinishSeek = 145,
    OpDestroy = 146,
    OpClear = 147,
    OpResetSorter = 148,
    OpCreateBtree = 149,
    OpSqlExec = 150,
    OpParseSchema = 151,
    OpLoadAnalysis = 152,
    OpDropTable = 153,
    OpDropIndex = 154,
    OpDropTrigger = 155,
    OpReal = 156,
    OpIntegrityCk = 157,
    OpRowSetAdd = 158,
    OpParam = 159,
    OpFkCounter = 160,
    OpMemMax = 161,
    OpOffsetLimit = 162,
    OpAggInverse = 163,
    OpAggStep = 164,
    OpAggStep1 = 165,
    OpAggValue = 166,
    OpAggFinal = 167,
    OpExpire = 168,
    OpCursorLock = 169,
    OpCursorUnlock = 170,
    OpTableLock = 171,
    OpVBegin = 172,
    OpVCreate = 173,
    OpVDestroy = 174,
    OpVOpen = 175,
    OpVInitIn = 176,
    OpVPreparedSql = 177,
    OpVColumn = 178,
    OpVRename = 179,
    OpPagecount = 180,
    OpMaxPgcnt = 181,
    OpClrSubtype = 182,
    OpFilterAdd = 183,
    OpTrace = 184,
    OpCursorHint = 185,
    OpReleaseReg = 186,
    OpNoop = 187,
    OpExplain = 188,
    OpAbortable = 189,
}

const SLOT_2_0: u32 = 0x001fc07f;
const SLOT_4_2_0: u32 = 0xf01fc07f;

/// translated from C
///
/// This will need some fuzzing with the c version as oracle.
fn sqlite_get_var_int(data: &[i8]) -> (u64, u8) {
    let mut temp = data;
    let (mut a, mut b, mut s);

    if temp[0] >= 0 {
        return (data[0] as u64, 1);
    }

    if temp[1] >= 0 {
        let val = (((data[0] & 0x7f) as u32) << 7) | (data[1] as u32);
        return (val as u64, 2);
    }

    a = (temp[0] as u32) << 14;
    b = temp[1] as u32;
    temp = &temp[2..];
    a |= temp[0] as u32;
    /* a: p0<<14 | p2 (unmasked) */
    if a & 0x80 == 0 {
        a &= SLOT_2_0;
        b &= 0x7f;
        b = b << 7;
        a |= b;
        return (a as u64, 3);
    }

    /* CSE1 from below */
    a &= SLOT_2_0;
    temp = &temp[1..];
    b = b << 14;
    b |= temp[0] as u32;
    /* b: p1<<14 | p3 (unmasked) */
    if b & 0x80 == 0 {
        b &= SLOT_2_0;
        /* moved CSE1 up */
        /* a &= (0x7f<<14)|(0x7f); */
        a = a << 7;
        a |= b;
        return (a as u64, 4);
    }

    /* a: p0<<14 | p2 (masked) */
    /* b: p1<<14 | p3 (unmasked) */
    /* 1:save off p0<<21 | p1<<14 | p2<<7 | p3 (masked) */
    /* moved CSE1 up */
    /* a &= (0x7f<<14)|(0x7f); */
    b &= SLOT_2_0;
    s = a;
    /* s: p0<<14 | p2 (masked) */

    temp = &temp[1..];
    a = a << 14;
    a |= temp[0] as u32;
    /* a: p0<<28 | p2<<14 | p4 (unmasked) */
    if a & 0x80 == 0 {
        /* we can skip these cause they were (effectively) done above
         ** while calculating s */
        /* a &= (0x7f<<28)|(0x7f<<14)|(0x7f); */
        /* b &= (0x7f<<14)|(0x7f); */
        b = b << 7;
        a |= b;
        s = s >> 18;
        let val = (s as u64) << 32 | a as u64;
        return (val, 5);
    }

    /* 2:save off p0<<21 | p1<<14 | p2<<7 | p3 (masked) */
    s = s << 7;
    s |= b;
    /* s: p0<<21 | p1<<14 | p2<<7 | p3 (masked) */

    temp = &temp[1..];
    b = b << 14;
    b |= temp[0] as u32;
    /* b: p1<<28 | p3<<14 | p5 (unmasked) */
    if b & 0x80 == 0 {
        /* we can skip this cause it was (effectively) done above in calc'ing s */
        /* b &= (0x7f<<28)|(0x7f<<14)|(0x7f); */
        a &= SLOT_2_0;
        a = a << 7;
        a |= b;
        s = s >> 18;
        let val = (s as u64) << 32 | a as u64;
        return (val, 6);
    }

    temp = &temp[1..];
    a = a << 14;
    a |= temp[0] as u32;
    /* a: p2<<28 | p4<<14 | p6 (unmasked) */
    if a & 0x80 == 0 {
        a &= SLOT_4_2_0;
        b &= SLOT_2_0;
        b = b << 7;
        a |= b;
        s = s >> 11;
        let val = (s as u64) << 32 | a as u64;
        return (val, 7);
    }

    /* CSE2 from below */
    a &= SLOT_2_0;
    temp = &temp[1..];
    b = b << 14;
    b |= temp[0] as u32;
    /* b: p3<<28 | p5<<14 | p7 (unmasked) */
    if b & 0x80 == 0 {
        b &= SLOT_4_2_0;
        /* moved CSE2 up */
        /* a &= (0x7f<<14)|(0x7f); */
        a = a << 7;
        a |= b;
        s = s >> 4;
        let val = (s as u64) << 32 | a as u64;
        return (val, 8);
    }

    temp = &temp[1..];
    a = a << 15;
    a |= temp[0] as u32;
    /* a: p4<<29 | p6<<15 | p8 (unmasked) */

    /* moved CSE2 up */
    /* a &= (0x7f<<29)|(0x7f<<15)|(0xff); */
    b &= SLOT_2_0;
    b = b << 8;
    a |= b;

    s = s << 4;
    b = data[4] as _;
    b &= 0x7f;
    b = b >> 3;
    s |= b;

    let val = (s as u64) << 32 | a as u64;

    return (val, 9);
}
