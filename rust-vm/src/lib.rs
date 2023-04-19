use core::fmt::{self, Display};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, c_int, c_void, CStr};
use std::mem::MaybeUninit;

use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};

use ffi::{MEM_Undefined, Mem, Vdbe, VdbeOp, _sqlite3GetVarint};

use crate::ffi::{MEM_Blob, MEM_Zero, MEM_Int};

macro_rules! state_or_return {
    ($vdbe:expr, $ret:expr) => {
        unsafe {
            let db = &mut *$vdbe.db;
            if db.replication_state.is_null() {
                return $ret;
            }
            &mut *(db.replication_state as *mut ReplicationState)
        }
    };
}

pub mod ffi {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(improper_ctypes)]
    include!(concat!(env!("OUT_DIR"), "/sqliteInt.rs"));
}

#[derive(Serialize, Deserialize)]
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
                if let Ok(s) = std::str::from_utf8(&b) {
                    write!(f, "{s}")
                } else {
                    #[allow(deprecated)]
                    let b64 = base64::encode(b);
                    write!(f, "b64:{b64}")
                }
            }
            Self::Float(x) => write!(f, "{x}"),
            Self::Null => write!(f, "Null"),
        }
    }
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
    Transaction {
        p1: i32,
        p2: i32,
    },
    Insert {
        id: u32,
        root: u32,
        /// sqlite record,
        data: Vec<u8>,
        /// number of trailing 0 in data, taken from mem.u.nZero
        n_zero: u32,
    },
    SetCookie {
        db: i32,
        value: i32,
        cookie: i32,
    },
    CreateBTree {
        p1: i32,
        p3: i32,
    },
}

#[derive(Default, Serialize, Deserialize)]
struct Trace {
    ops: Vec<Op>,
}

impl Trace {
    fn push(&mut self, op: Op) {
        self.ops.push(op);
    }

    /// return the number of registers to allocate for this trace
    fn count_registers(&self) -> usize {
        let mut roots = HashSet::new();
         self
            .ops
            .iter()
            .map(|r| match r {
                Op::Transaction { .. } => 0,
                Op::Insert { root, .. } => {
                    // we need:
                    // - 1 reg for data
                    // - 1 reg for row id
                    // - 1 reg if we need to open a cursor for this row
                    if !roots.contains(&root) {
                        roots.insert(root);
                        3
                    } else {
                        2
                    }
                },
                Op::SetCookie { .. } => 0,
                Op::CreateBTree { .. } => 1,
            })
            .sum()
    }

    fn gen_code(self, vdbe: *mut Vdbe) {
        dbg!(&self);
        let num_regs = self.count_registers();
        dbg!(num_regs);
        let mut regs: Vec<ffi::Mem> = Vec::with_capacity(num_regs);
        // perform some partial initialization of the registers
        let db = unsafe { (&*vdbe).db };
        for _ in 0..num_regs {
            let mut mem: Mem = unsafe { MaybeUninit::zeroed().assume_init() };
            mem.db = db;
            mem.flags = MEM_Undefined as _;

            regs.push(mem);
        }

        let mut cursors = HashMap::new();
        // the register 0 is reserved for the cursor
        let mut current_reg = self.ops.iter().any(|op| matches!(op, Op::Insert { .. })) as usize;
        for op in self.ops.into_iter() {
            match op {
                Op::Transaction { p1, p2 } => unsafe {
                    ffi::sqlite3VdbeAddOp2(vdbe, OpCode::OpTransaction as _, p1, p2);
                },
                Op::Insert {
                    id,
                    root,
                    data,
                    n_zero,
                } => {
                    // 1) make row
                    let mut flags = MEM_Blob;
                    if n_zero != 0 {
                        flags |= MEM_Zero;
                    }
                    let reg = &mut regs[current_reg];
                    reg.u = ffi::sqlite3_value_MemValue { nZero: n_zero as _ };
                    reg.z = data.as_ptr() as _;
                    reg.n = data.len() as _;
                    reg.flags = flags as _;
                    let data_reg = current_reg;
                    current_reg += 1;

                    let id_reg = current_reg;
                    regs[id_reg].u.i = id as _;
                    regs[id_reg].flags |= MEM_Int as u16;
                    current_reg += 1;

                    // TODO: don't leak!!
                    std::mem::forget(data);
                    // 2) put row in available register
                    let next_cursor = cursors.len();
                    let cursor = match cursors.entry(root) {
                        Entry::Occupied(e) => *e.get(),
                        Entry::Vacant(e) => {
                            e.insert(next_cursor);
                            unsafe {
                                ffi::sqlite3VdbeAddOp2(
                                    vdbe,
                                    OpCode::OpOpenWrite as _,
                                    next_cursor as _,
                                    root as _,
                                );
                            }
                            next_cursor
                        }
                    };
                    // 3) add insert intruction to insert row stored at index
                    unsafe {
                        ffi::sqlite3VdbeAddOp3(
                            vdbe,
                            OpCode::OpInsert as _,
                            cursor as _,
                            data_reg as _,
                            id_reg as _,
                        );
                    }

                }
                Op::SetCookie { db, value, cookie } => unsafe {
                    ffi::sqlite3VdbeAddOp3(
                        vdbe,
                        OpCode::OpSetCookie as _,
                        db as _,
                        cookie as _,
                        value as _,
                    );
                },
                Op::CreateBTree { p1, p3 } => {
                    unsafe {
                        ffi::sqlite3VdbeAddOp3(
                            vdbe,
                            OpCode::OpCreateBtree as _,
                            p1 as _,
                            current_reg as _,
                            p3 as _,
                        );
                    }
                    current_reg += 1;
                }
            }
        }

        unsafe {
            ffi::sqlite3VdbeAddOp0(vdbe, OpCode::OpHalt as _);
        }

        let vdbe = unsafe { &mut *vdbe };
        vdbe.nMem = regs.len() as _;
        vdbe.aMem = regs.as_mut_ptr();
        vdbe.nCursor = cursors.len() as _;
        let mut ap_csr = vec![std::ptr::null_mut(); cursors.len()];
        vdbe.apCsr = ap_csr.as_mut_ptr() as _;
        vdbe.pc = 0;
        // TODO handle leak!!
        std::mem::forget(regs);
        std::mem::forget(ap_csr);
    }
}

impl fmt::Debug for Trace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for op in self.ops.iter() {
            match op {
                Op::Transaction { .. } => writeln!(f, "TX_BEGIN")?,
                Op::Insert { id, root, data, .. } => unsafe {
                    let row = decode_record(std::mem::transmute(data.as_slice()));
                    writeln!(f, "INSERT root={root} id={id} row={row:?}")?;
                },
                Op::CreateBTree { .. } => writeln!(f, "CREATE_BTREE")?,
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
    trace: Trace,
}

struct ReplicationState {
    contexts: HashMap<i32, Context>,
    context_stack: Vec<i32>,
    context_ids: i32,
    frame_cb: extern "C" fn(*mut c_void, *const c_char, c_int) -> c_int,
    user_data: *mut c_void,
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
            .get_mut(ctx_id)
            .expect("invalid replication state")
    }
}

#[no_mangle]
pub extern "C" fn replication_state_init(
    cb: extern "C" fn(*mut c_void, *const c_char, c_int) -> c_int,
    user_data: *mut c_void,
) -> *mut c_void {
    let state = Box::new(ReplicationState {
        contexts: HashMap::new(),
        context_stack: Vec::new(),
        context_ids: 0,
        frame_cb: cb,
        user_data,
    });

    let ptr = Box::leak(state) as *mut _;

    ptr as _
}

#[no_mangle]
pub extern "C" fn prepare_program(trace: *const u8, len: u32, vdbe: *mut Vdbe) {
    let bytes = unsafe { std::slice::from_raw_parts(trace, len as usize) };
    let trace: Trace = bincode::deserialize(bytes).unwrap();
    trace.gen_code(vdbe);
}

/// Enter a vdbe context. Returns a new context_id
#[no_mangle]
pub extern "C" fn replication_enter_context(vdbe: *const Vdbe) {
    println!("entering context");
    let vdbe = unsafe { &*vdbe };
    // we are only interested in writes
    let state = state_or_return!(vdbe, ());
    // this is not good, we need a proper id
    let ctx_id = state.context_ids;
    state.context_ids = state.context_ids.wrapping_add(1);

    let ctx = Context::default();
    state.contexts.insert(ctx_id, ctx);
    state.context_stack.push(ctx_id);
}

#[no_mangle]
pub extern "C" fn replication_exit_context(vdbe: *const Vdbe) {
    println!("exiting context");
    let vdbe = unsafe { &*vdbe };
    let state = state_or_return!(vdbe, ());
    if state.in_context() {
        let last = state.context_stack.pop().unwrap();
        state.contexts.remove(&last);
    }
}

#[no_mangle]
pub extern "C" fn replication_state_destroy(state: *mut c_void) {
    let _state: Box<ReplicationState> = unsafe { Box::from_raw(state as *mut ReplicationState) };
}

#[no_mangle]
pub extern "C" fn replication_post_commit_cleanup(vdbe: *mut Vdbe) {
    let vdbe = unsafe { &*vdbe };
    let _state = state_or_return!(vdbe, ());
    println!("post commit called");
}

#[no_mangle]
pub extern "C" fn replication_pre_commit(vdbe: *const Vdbe) -> c_int {
    let vdbe = unsafe { &*vdbe };
    let state = state_or_return!(vdbe, 0);
    if vdbe.readOnly() != 0 {
        return 0;
    }
    if state.in_context() {
        let ctx = state.current_context_mut();
        println!("Trace:\n {:?}", ctx.trace);
        let trace = std::mem::take(&mut ctx.trace);
        dbg!(&trace);
        let data = bincode::serialize(&trace).unwrap();
        (state.frame_cb)(state.user_data, data.as_ptr() as _, data.len() as _);
    }
    0
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
            n if n >= 13 && n % 2 != 0 => Self::Text((n as usize - 13) / 2),
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

    let state = state_or_return!(vdbe, 0);

    if !state.in_context() {
        return 0;
    }
    if vdbe.readOnly() == 1 {
        return 0;
    }

    let op = unsafe { &*op };
    let code: OpCode = op.opcode.try_into().unwrap();
    dbg!(&code);
    match code {
        OpCode::OpTransaction => {
            state.current_context_mut().trace.push(Op::Transaction {
                p1: op.p1,
                p2: op.p2,
            });
        }
        OpCode::OpCreateBtree => state.current_context_mut().trace.push(Op::CreateBTree {
            p1: op.p1,
            p3: op.p3,
        }),
        OpCode::OpSetCookie => {
            let db = op.p1;
            let value = op.p3;
            let cookie = op.p2;
            state
                .current_context_mut()
                .trace
                .push(Op::SetCookie { db, value, cookie });
        }
        OpCode::OpInsert => {
            // p1 contains the cursor we want to insert to, we get the corresponding table root
            if op.p4type == P4_TABLE {
                let table = unsafe { &op.p4.pTab };
                let name = unsafe { CStr::from_ptr((**table).zName) };
                println!("inserting into {name:?}");
            }

            let id = unsafe { vdbe.vdbe_get_reg(op.p3).u.i as u32 };

            let data_reg = vdbe.vdbe_get_reg(op.p2);
            let data =
                unsafe { std::slice::from_raw_parts(data_reg.z as *const u8, data_reg.n as usize) };
            let root = unsafe { (**vdbe.apCsr.offset(op.p1 as isize)).pgnoRoot };
            // mapping
            state.current_context_mut().trace.push(Op::Insert {
                id,
                root,
                data: data.to_vec(),
                n_zero: unsafe { data_reg.u.nZero as _ },
            });
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

const P4_TABLE: i8 = -5; /* P4 is a pointer to a Table structure */

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

/// translated from C
///
/// This will need some fuzzing with the c version as oracle.
fn sqlite_get_var_int(data: &[i8]) -> (u64, u8) {
    let mut out = 0;
    let count = unsafe { _sqlite3GetVarint(data.as_ptr() as *const _, &mut out as *mut _) };
    (out, count)
}
