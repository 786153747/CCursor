import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { BASE_REDIRECT, DEFAULT_PROVIDERS, DEFAULT_ROUTES } from './defaults.js';

const bundledCatalog = '{"models":["test-release-snapshot"]}\n';
const releaseDefaultsModuleUrl = new URL('./release-defaults.js', import.meta.url).href;

function createTemporaryHome(testContext) {
  const homeDirectory = mkdtempSync(join(tmpdir(), 'ccursor-release-defaults-'));
  testContext.after(() => rmSync(homeDirectory, { recursive: true, force: true }));
  mkdirSync(join(homeDirectory, 'bundle'));
  writeFileSync(join(homeDirectory, 'bundle', 'models-catalog.json'), bundledCatalog);
  return homeDirectory;
}

function runReleaseDefaults(homeDirectory) {
  // A new process isolates routes.js's import-time homedir() and the mocks.
  const subprocessScript = `
    import assert from 'node:assert/strict';
    import childProcess from 'node:child_process';
    import filesystem from 'node:fs';
    import { syncBuiltinESMExports } from 'node:module';
    import { homedir } from 'node:os';
    import { join } from 'node:path';

    assert.equal(homedir(), process.env.HOME);
    const databaseAccesses = [];
    for (const methodName of ['existsSync', 'readFileSync', 'openSync']) {
      const originalMethod = filesystem[methodName];
      filesystem[methodName] = (...argumentsList) => {
        if (String(argumentsList[0]).includes('state.vscdb')) {
          databaseAccesses.push(methodName);
          throw new Error('Cursor state database access is forbidden');
        }
        return originalMethod(...argumentsList);
      };
    }

    const subprocessCalls = [];
    for (const methodName of ['exec', 'execSync', 'execFile', 'execFileSync', 'spawn', 'spawnSync']) {
      childProcess[methodName] = () => {
        subprocessCalls.push(methodName);
        throw new Error('Release defaults must not invoke sqlite3 or other subprocesses');
      };
    }
    syncBuiltinESMExports();

    // Supply the bundled asset location without building or invoking the CLI.
    globalThis.__dirname = join(process.env.HOME, 'bundle');
    const { releaseDefaults } = await import(${JSON.stringify(releaseDefaultsModuleUrl)});
    const logMessages = [];
    releaseDefaults(message => logMessages.push(message));
    process.stdout.write(JSON.stringify({ logMessages, databaseAccesses, subprocessCalls }));
  `;

  const subprocessResult = spawnSync(process.execPath, ['--input-type=module', '--eval', subprocessScript], {
    cwd: homeDirectory,
    env: {
      ...process.env,
      HOME: homeDirectory,
      USERPROFILE: homeDirectory,
      APPDATA: join(homeDirectory, 'AppData'),
      XDG_CONFIG_HOME: join(homeDirectory, '.config'),
      NODE_OPTIONS: '',
    },
    encoding: 'utf-8',
    timeout: 10_000,
  });
  assert.ifError(subprocessResult.error);
  assert.equal(subprocessResult.status, 0, subprocessResult.stderr);
  const result = JSON.parse(subprocessResult.stdout);
  assert.deepEqual(result.databaseAccesses, [], 'must not inspect state.vscdb');
  assert.deepEqual(result.subprocessCalls, [], 'must not query sqlite3, even if errors are caught');
  return result.logMessages;
}

test('keeps existing routes and providers byte-for-byte while refreshing the catalog', testContext => {
  const homeDirectory = createTemporaryHome(testContext);
  const configurationDirectory = join(homeDirectory, '.ccursor');
  mkdirSync(configurationDirectory);

  const existingRoutes = Buffer.from(
    '{\r\n\t"byokMode": 1, "server": {"host":"custom.test","port":45678},\r\n'
    + '\t"redirect": ["custom.Service/Method"], "customSetting": true\r\n}\r\n',
  );
  const existingProviders = Buffer.from(
    '{ "providers": [{"id":"custom","apiKey":"synthetic-test-key"}], "customSetting": 17 }  ',
  );
  writeFileSync(join(configurationDirectory, 'routes.json'), existingRoutes);
  writeFileSync(join(configurationDirectory, 'providers.json'), existingProviders);
  writeFileSync(join(configurationDirectory, 'models-catalog.json'), 'old catalog');

  const logMessages = runReleaseDefaults(homeDirectory);

  assert.deepEqual(readFileSync(join(configurationDirectory, 'routes.json')), existingRoutes);
  assert.deepEqual(readFileSync(join(configurationDirectory, 'providers.json')), existingProviders);
  assert.equal(readFileSync(join(configurationDirectory, 'models-catalog.json'), 'utf-8'), bundledCatalog);
  assert.ok(logMessages.includes('  routes.json already exists, keep'));
  assert.ok(logMessages.includes('  providers.json already exists, keep'));
  assert.ok(!logMessages.some(message => message.includes('BYOK is OFF by default')));
});

test('creates new configuration with BYOK OFF and explicit post-login guidance', testContext => {
  const homeDirectory = createTemporaryHome(testContext);

  const logMessages = runReleaseDefaults(homeDirectory);
  const configurationDirectory = join(homeDirectory, '.ccursor');
  const routes = JSON.parse(readFileSync(join(configurationDirectory, 'routes.json'), 'utf-8'));
  const providers = JSON.parse(readFileSync(join(configurationDirectory, 'providers.json'), 'utf-8'));

  assert.deepEqual(routes, { ...DEFAULT_ROUTES, byokMode: 0, redirect: BASE_REDIRECT });
  assert.deepEqual(providers, DEFAULT_PROVIDERS);
  assert.equal(readFileSync(join(configurationDirectory, 'models-catalog.json'), 'utf-8'), bundledCatalog);
  assert.ok(logMessages.some(message => /BYOK is OFF.*enable BYOK after.*login.*onboarding/.test(message)));
});
