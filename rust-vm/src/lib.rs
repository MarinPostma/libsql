#![allow(dead_code)]
use std::ffi::{c_char, c_double, c_int, c_void, CStr};
use std::fmt;

use num_enum::TryFromPrimitive;
const SQLITE_INTERNAL: usize = 2;

#[derive(Debug)]
enum Value {
    Integer(c_int),
    Null,
}

enum ReturnCode {
    SqliteRow,
}

#[derive(Debug)]
enum RowValue {
    String(String),
    Integer(u64),
    Blob(Vec<u8>),
    Float(f64),
    Null,
}

#[derive(Debug)]
struct Row {
    values: Vec<RowValue>,
}

#[derive(Debug)]
enum Op {
    Transaction,
    Record { id: u32, table: String, row: Row },
}

#[derive(Debug)]
struct ReplicationState {
    stack: Vec<Op>,
}

#[no_mangle]
pub extern "C" fn replication_state_init() -> *mut c_void {
    dbg!(std::mem::size_of::<Mem>());
    let state = Box::new(ReplicationState { stack: Vec::new() });

    println!("created state with value: {state:?}");

    let ptr = Box::leak(state) as *mut _;

    ptr as _
}

#[no_mangle]
pub extern "C" fn replication_state_destroy(state: *mut c_void) {
    let state: Box<ReplicationState> = unsafe { Box::from_raw(state as *mut ReplicationState) };
    println!("destroying state: {state:?}");
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

fn decode_record(data: &[i8]) {
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

    dbg!(row);
}

#[no_mangle]
pub extern "C" fn replication_step(
    state: *mut c_void,
    op: *const VdbeOp,
    vdbe: *const Vdbe,
) -> c_int {
    let state = unsafe { &mut *(state as *mut ReplicationState) };
    let op = unsafe { &*op };
    let code: OpCode = op.opcode.try_into().unwrap();
    match code {
        OpCode::OpTransaction => {
            state.stack.push(Op::Transaction);
        }
        OpCode::OpInsert => {
            if op.p4type == P4_TABLE {
                let table = unsafe { &op.p4.pTab };
                let name = unsafe { CStr::from_ptr((**table).zName) };
                println!("inserting into {name:?}");
            }

            let reg_id = op.p2 as isize;
            let reg = unsafe { &(*(*vdbe).aMem.offset(reg_id)) };
            let data = unsafe { std::slice::from_raw_parts(reg.z, reg.n as usize) };
            decode_record(data);
        }
        _ => (),
    }

    0
}

#[allow(dead_code, non_snake_case)]
#[repr(C)]
pub struct Vdbe {
    db: *const c_void,
    ppVPrev: *const *const Vdbe,
    pVNext: *const Vdbe,
    pParse: *const c_void,
    nVar: c_int,
    nMem: c_int,
    nCursor: c_int,
    cacheCstr: u32,
    pc: c_int,
    rc: c_int,
    nChange: i64,
    iStatement: c_int,
    iCurrentTime: i64,
    nFkConstraint: i64,
    nStmtDefCons: i64,
    nStmtDefImmCons: i64,
    aMem: *const Mem,
    apArg: *const *const Mem,
    // there are more fields I don't care about.
}

#[repr(C)]
union MemValue {
    r: c_double,
    i: i64,
    nZero: c_int,
    zPType: *const c_char,
    pDef: *const c_void,
}

#[allow(dead_code, non_snake_case)]
#[repr(C)]
struct Mem {
    u: MemValue,
    z: *const c_char,
    n: c_int,
    flags: u16,
    enc: u8,
    eSubtype: u8,
    db: *const c_void,
    szMalloc: c_int,
    uTemp: u32,
    zMalloc: *const c_char,
    xDel: *const c_void,
    // #ifdef SQLITE_DEBUG
    //   Mem *pScopyFrom;    /* This Mem is a shallow copy of pScopyFrom */
    //   u16 mScopyFlags;    /* flags value immediately after the shallow copy */
    // #endif
}

#[repr(C)]
struct Table {
    zName: *const c_char,
}

#[repr(C)]
union P4union {
    /* fourth parameter */
    i: c_int,                /* Integer value if p4type==P4_INT32 */
    p: *const c_void,        /* Generic pointer */
    z: *const c_char,        /* Pointer to data for string (char array) types */
    pI64: *const i64,        /* Used when p4type is P4_INT64 */
    pReal: *const c_double,  /* Used when p4type is P4_REAL */
    pFunc: *const c_void,    /* Used when p4type is P4_FUNCDEF */
    pCtx: *const c_void,     /* Used when p4type is P4_FUNCCTX */
    pColl: *const c_void,    /* Used when p4type is P4_COLLSEQ */
    pMem: *const c_void,     /* Used when p4type is P4_MEM */
    pVtab: *const c_void,    /* Used when p4type is P4_VTAB */
    pKeyInfo: *const c_void, /* Used when p4type is P4_KEYINFO */
    ai: *const u32,          /* Used when p4type is P4_INTARRAY */
    pProgram: *const c_void, /* Used when p4type is P4_SUBPROGRAM */
    pTab: *const Table,      /* Used when p4type is P4_TABLE */
}

impl fmt::Debug for P4union {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("p4")
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

#[repr(C)]
#[derive(Debug)]
pub struct VdbeOp {
    opcode: u8,  /* What operation to perform */
    p4type: i8,  /* One of the P4_xxx constants for p4 */
    p5: u16,     /* Fifth parameter is an unsigned 16-bit integer */
    p1: c_int,   /* First operand */
    p2: c_int,   /* Second parameter (often the jump destination) */
    p3: c_int,   /* The third parameter */
    p4: P4union, /* The 4th parameter */
    _pad: u64,   // dunno what this is
}

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
