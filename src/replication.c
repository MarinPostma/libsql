#include <string.h>

#include "sqliteInt.h"
#include "vdbeInt.h"
#include "vdbe.h"

int sqlite3_parse_object_init(Parse* sParse, sqlite3* db) {
  int rc = SQLITE_OK;       /* Result code */

  /* sqlite3ParseObjectInit(&sParse, db); // inlined for performance */
  memset(PARSE_HDR(sParse), 0, PARSE_HDR_SZ);
  memset(PARSE_TAIL(sParse), 0, PARSE_TAIL_SZ);
  sParse->pOuterParse = db->pParse;
  db->pParse =sParse;
  sParse->db = db;
  sParse->pReprepare = NULL;
  assert( ppStmt && *ppStmt==0 );
  if( db->mallocFailed ) sqlite3ErrorMsg(sParse, "out of memory");
  assert( sqlite3_mutex_held(db->mutex) );

  return rc;
}

int replicate(sqlite3* db) {
    Parse sParse;
    // acquire mutex
    sqlite3_mutex_enter(db->mutex);
    sqlite3BtreeEnterAll(db);

    sqlite3_parse_object_init(&sParse, db);
    Vdbe* p = sqlite3VdbeCreate(&sParse);

    sqlite3VdbeAddOp2(p, OP_Integer, 17, 2);
    sqlite3VdbeAddOp2(p, OP_Halt, 0, 0);
    sqlite3VdbeExec(p);

    // release
    sqlite3BtreeLeaveAll(db);
    sqlite3_mutex_leave(db->mutex);
    return 0;
}
