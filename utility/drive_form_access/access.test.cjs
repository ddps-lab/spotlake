const {test} = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const crypto = require('node:crypto');
const code = fs.readFileSync(__dirname + '/Code.gs', 'utf8');
const NOW = new Date('2026-09-17T09:00:00Z');
const END = '2026-10-01T00:00:00.000Z';
function harness() {
  const values = new Map([['activatedAt', '2026-09-17T08:00:00Z'], ['enabled', 'true']]);
  const permissions = [{id:'owner',type:'user',role:'owner',emailAddress:'spotlake@hanyang.ac.kr'}];
  const calls = [];
  const props = {
    getProperty:k => values.get(k) || null,
    setProperty:(k,v) => values.set(k,v),
    deleteProperty:k => values.delete(k),
    getProperties:() => Object.fromEntries(values),
  };
  const context = vm.createContext({console, Date,
    PropertiesService: {getScriptProperties:() => props},
    Utilities: {DigestAlgorithm:{SHA_256:'sha256'},
      computeDigest:(_,s) => crypto.createHash('sha256').update(s).digest(),
      base64EncodeWebSafe:b => Buffer.from(b).toString('base64url')},
    Drive: {Permissions: {
      list:() => ({permissions:structuredClone(permissions)}),
      create:(body,folder,options) => {
        assert.equal(options.sendNotificationEmail,false);
        calls.push('create'); const p={id:'p'+permissions.length,...body};
        permissions.push(p); return {...p};
      },
      update:(body,folder,id) => {
        calls.push('update'); const p=permissions.find(x => x.id===id);
        Object.assign(p,body); return {...p};
      },
      get:(folder,id) => ({...permissions.find(x => x.id===id)}),
      remove:(folder,id) => {
        calls.push('remove'); const i=permissions.findIndex(x=>x.id===id);
        if (i>=0) permissions.splice(i,1);
      },
    }},
  });
  vm.runInContext(code,context);
  function response(opts={}) {
    return {
      getTimestamp:() => new Date(opts.at || '2026-09-17T08:30:00Z'),
      getId:() => opts.id || 'response-1',
      getRespondentEmail:() => opts.email === undefined ? 'researcher@example.org' : opts.email,
      getItemResponses:() => [
        {getItem:()=>({getTitle:()=> 'SpotLake Dataset — Terms of Use'}),
          getResponse:()=> opts.agree === false ? [] : ['Agree']},
        {getItem:()=>({getTitle:()=> 'Email address'}),getResponse:()=> 'attacker@example.org'},
      ],
    };
  }
  return {context,props,values,permissions,calls,response};
}
test('UTC month boundaries include December and leap February',()=>{
  const {context:c}=harness();
  assert.equal(c.nextMonthUtc_(new Date('2026-12-31T23:59:59Z')).toISOString(),'2027-01-01T00:00:00.000Z');
  assert.equal(c.nextMonthUtc_(new Date('2028-02-29T23:59:59Z')).toISOString(),'2028-03-01T00:00:00.000Z');
});
test('grant uses collected identity, reader role, exact expiry and no email; replay is a no-op',()=>{
  const h=harness(); h.context.processResponse_(h.response(),NOW);
  assert.deepEqual(h.calls,['create']);
  assert.equal(h.permissions[1].emailAddress,'researcher@example.org');
  assert.equal(h.permissions[1].role,'reader');
  assert.equal(h.permissions[1].expirationTime,END);
  h.context.processResponse_(h.response(),NOW);
  assert.deepEqual(h.calls,['create']);
});
test('pre-cutover, previous-month and nonconsenting submissions cannot grant',()=>{
  const h=harness();
  h.context.processResponse_(h.response({at:'2026-09-17T07:00:00Z'}),NOW);
  h.context.processResponse_(h.response(),new Date(END));
  h.context.processResponse_(h.response({agree:false}),NOW);
  assert.deepEqual(h.calls,[]);
});
test('collected email is required; typed email is never a fallback',()=>{
  const h=harness();
  assert.throws(()=>h.context.processResponse_(h.response({email:''}),NOW),/collected account email/);
  assert.deepEqual(h.calls,[]);
});
test('lab owner/admin access is never downgraded or given expiration',()=>{
  const h=harness(); h.context.processResponse_(h.response({email:'spotlake@hanyang.ac.kr'}),NOW);
  assert.deepEqual(h.calls,[]);
  assert.equal(h.permissions[0].role,'owner');
});
test('manual collaborator permissions cannot be adopted or downgraded',()=>{
  for (const role of ['reader','writer']) {
    const h=harness();
    h.permissions.push({id:'manual',type:'user',role,emailAddress:'researcher@example.org'});
    assert.throws(()=>h.context.processResponse_(h.response(),NOW),/not managed|non-reader/);
    assert.deepEqual(h.calls,[]);
  }
});
test('lost create response is recovered without creating duplicate grants',()=>{
  const h=harness(), create=h.context.Drive.Permissions.create;
  h.context.Drive.Permissions.create=(...args)=>{create(...args); throw new Error('lost response');};
  assert.throws(()=>h.context.processResponse_(h.response(),NOW),/lost response/);
  h.context.processResponse_(h.response(),NOW);
  assert.deepEqual(h.calls,['create','update']);
  assert.equal(h.permissions.length,2);
});
test('a new submission next month renews access; old submission does not',()=>{
  const h=harness(); h.context.processResponse_(h.response(),NOW);
  const next=new Date('2026-10-01T01:00:00Z');
  h.context.processResponse_(h.response(),next);
  assert.deepEqual(h.calls,['create']);
  h.context.processResponse_(h.response({id:'response-2',at:'2026-10-01T00:30:00Z'}),next);
  assert.equal(h.permissions[1].expirationTime,'2026-11-01T00:00:00.000Z');
  h.context.cleanupExpired_(next);
  assert.equal(h.permissions.length,2);
});
test('cleanup removes only expired managed readers',()=>{
  const h=harness(); h.context.processResponse_(h.response(),NOW);
  h.context.cleanupExpired_(new Date(END));
  assert.equal(h.permissions.length,1);
  assert.equal(h.permissions[0].role,'owner');
  assert.equal(h.values.has('done:response-1'),false);
});
test('cleanup refuses a manually elevated permission',()=>{
  const h=harness(); h.context.processResponse_(h.response(),NOW);
  h.permissions[1].role='writer';
  assert.throws(()=>h.context.cleanupExpired_(new Date(END)),/manual review/);
  assert.equal(h.permissions.length,2);
});
test('public root fails closed',()=>{
  const h=harness(); h.permissions.push({id:'public',type:'anyone',role:'reader'});
  assert.throws(()=>h.context.assertRestricted_(),/general access/);
});
test('a trigger from a different Form is rejected before any grant',()=>{
  const h=harness();
  assert.throws(()=>h.context.onDatasetRequest({source:{getId:()=> 'wrong-form'},
    response:h.response()}),/installed Google Form/);
  assert.deepEqual(h.calls,[]);
});
test('missing activation cutoff cannot import historical responses',()=>{
  const h=harness(); h.values.delete('activatedAt');
  assert.throws(()=>h.context.processResponse_(h.response(),NOW),/activation cutoff/);
  assert.deepEqual(h.calls,[]);
});
test('an incomplete consent or collection configuration blocks processing',()=>{
  for (const bad of ['email','one-response','terms']) {
    const h=harness();
    h.context.Session={getEffectiveUser:()=>({getEmail:()=> 'spotlake@hanyang.ac.kr'})};
    h.context.FormApp={ItemType:{CHECKBOX:'checkbox'},openById:()=>({
      collectsEmail:()=> bad!=='email',hasLimitOneResponsePerUser:()=> bad==='one-response',
      getItems:()=>[{getTitle:()=> 'SpotLake Dataset — Terms of Use',
        asCheckboxItem:()=>({isRequired:()=> bad!=='terms'})}],
    })};
    assert.throws(()=>h.context.form_(),/email collection|Terms of Use/);
  }
});
test('activation audits all collaborators before revoking general access',()=>{
  const h=harness(); h.values.delete('activatedAt'); h.values.delete('enabled');
  h.context.LockService={getScriptLock:()=>({tryLock:()=>true,releaseLock:()=>{}})};
  h.context.form_=()=>({}); h.context.tree_=()=>[{id:'root'}];
  h.permissions.push({id:'public',type:'anyone',role:'reader'},
    {id:'manual',type:'user',role:'writer',emailAddress:'external@example.org'});
  assert.throws(()=>h.context.activate(),/Unexpected existing collaborator/);
  assert.deepEqual(h.calls,[]);
  assert.equal(h.values.has('activatedAt'),false);
});
test('trigger installation failure leaves current public access unchanged',()=>{
  const h=harness(); h.values.delete('activatedAt'); h.values.delete('enabled');
  h.context.LockService={getScriptLock:()=>({tryLock:()=>true,releaseLock:()=>{}})};
  h.context.form_=()=>({}); h.context.tree_=()=>[{id:'root'}];
  h.context.installTriggers_=()=>{throw new Error('install failed');};
  h.permissions.push({id:'public',type:'anyone',role:'reader'});
  assert.throws(()=>h.context.activate(),/install failed/);
  assert.deepEqual(h.calls,[]);
  assert.equal(h.values.has('activatedAt'),false);
});
