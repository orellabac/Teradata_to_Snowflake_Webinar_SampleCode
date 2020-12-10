var hlp = require('@mobilize/snowflake-instrumentation-helpers');
hlp.metadatadir='.snowqm';
hlp.category = 'procedure';
hlp.Logger.info('starting tests');

function ok(expr, msg) {
  if (!expr) throw new Error(msg);
  return true;
}

suite('DEMO_DB');
test('MACROSAMPLE01',()=> {
    var res = hlp.go('procedure', 'DEMO_DB','MACROSAMPLE01',1,0);
    return ok(!res.lastError);
});

test('MACRO_1',()=> {
    var res = hlp.go('procedure', 'DEMO_DB','MACRO_1',3,0,0,0);
    return ok(!res.lastError);
});

test('CREATE_INSERTS',()=> {
    var res = hlp.go('procedure', 'DEMO_DB','CREATE_INSERTS',2,'DB_NAME','TBL_NAME');
    return ok(!res.lastError);
});

test('DYNAMIC_RESULT_SETS',()=> {
    var res = hlp.go('procedure', 'DEMO_DB','DYNAMIC_RESULT_SETS',0);
    return ok(!res.lastError);
});

