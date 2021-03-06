var hlp = require('@mobilize/snowflake-instrumentation-helpers');
hlp.metadatadir='.snowqm';
hlp.category = 'procedure';
hlp.Logger.info('starting tests');

function ok(expr, msg) {
  if (!expr) throw new Error(msg);
  return true;
}

suite('DEMODB');
test('PROCEDURESAMPLE01',()=> {
    var res = hlp.go('procedure', 'DEMODB','PROCEDURESAMPLE01',2,0,0);
    return ok(!res.lastError);
});

test('PROCEDURESAMPLE02',()=> {
    var res = hlp.go('procedure', 'DEMODB','PROCEDURESAMPLE02',2,0,0);
    return ok(!res.lastError);
});

test('PROCEDURESAMPLE03',()=> {
    var res = hlp.go('procedure', 'DEMODB','PROCEDURESAMPLE03',2,0,0);
    return ok(!res.lastError);
});

